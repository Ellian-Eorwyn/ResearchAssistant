from __future__ import annotations

import csv
import io
import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.models.citation_metadata import CitationMetadata
from backend.models.settings import AppSettings, LLMBackendConfig
from backend.models.sources import SourceManifestRow
from backend.pipeline.source_downloader import (
    _apply_oa_citation_helper,
    _build_citation_metadata,
    SourceDownloadOrchestrator,
)
from backend.routers import search
from backend.search.searxng_client import SearXNGClient
from backend.storage.attached_repository import AttachedRepositoryService, _load_source_rows
from backend.storage.file_store import FileStore


def _build_csv_bytes(rows: list[dict[str, str]]) -> bytes:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


class SearXNGClientTests(unittest.TestCase):
    def test_build_search_params_serializes_filters(self) -> None:
        client = SearXNGClient("http://example.test/searxng")
        self.addCleanup(client.close)

        params = client.build_search_params(
            "climate policy",
            page=2,
            categories=["science", "news"],
            language="en-US",
            time_range="month",
            enabled_plugins=["oa_doi_rewrite"],
        )

        self.assertEqual(
            params,
            {
                "q": "climate policy",
                "format": "json",
                "pageno": 2,
                "categories": "science,news",
                "language": "en-US",
                "time_range": "month",
                "enabled_plugins": "oa_doi_rewrite",
            },
        )

    def test_get_config_normalizes_categories_languages_and_time_ranges(self) -> None:
        client = SearXNGClient("http://example.test/searxng")
        self.addCleanup(client.close)
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "categories": ["general", "science"],
            "locales": {"en-US": "English (United States)"},
            "default_doi_resolver": "oadoi.org",
            "plugins": [{"name": "oa_doi_rewrite", "enabled": False}],
            "engines": [
                {"enabled": True, "time_range_support": True},
                {"enabled": False, "time_range_support": False},
            ],
        }

        with patch.object(client._client, "get", return_value=response):
            config = client.get_config(force_refresh=True)

        self.assertEqual(config["categories"], ["general", "science"])
        self.assertEqual(config["languages"][0]["value"], "auto")
        self.assertEqual(config["languages"][1]["value"], "all")
        self.assertIn({"value": "en-US", "label": "English (United States)"}, config["languages"])
        self.assertEqual(config["time_ranges"], ["day", "month", "year"])
        self.assertTrue(config["supports_oa_doi_helper"])
        self.assertEqual(config["defaults"]["categories"], ["general"])

    def test_search_paginated_normalizes_published_date_and_authors(self) -> None:
        client = SearXNGClient("http://example.test/searxng")
        self.addCleanup(client.close)
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "results": [
                {
                    "title": "Example",
                    "url": "https://example.com/article",
                    "content": "Snippet",
                    "engine": "semantic scholar",
                    "engines": ["semantic scholar"],
                    "category": "science",
                    "publishedDate": "2026-04-10T00:00:00",
                    "authors": ["Ada Lovelace"],
                    "doi": "10.1234/example",
                    "html_url": "https://example.com/article",
                    "pdf_url": "https://example.com/article.pdf",
                    "score": 2.0,
                }
            ]
        }

        with patch.object(client._client, "get", return_value=response):
            results = client.search_paginated("example", target_results=1, max_pages=1)

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["published_date"], "2026-04-10T00:00:00")
        self.assertEqual(results[0]["authors"], ["Ada Lovelace"])
        self.assertEqual(results[0]["doi"], "10.1234/example")


class SearchRouterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="search-router-test-")
        root = Path(self.temp_dir.name)
        self.store = FileStore(base_dir=root / "data", sync_project_profiles=False)
        self.repository = AttachedRepositoryService(store=self.store)
        self.repository.create(str(root / "repo"))
        self.store.save_app_settings(
            AppSettings(
                use_llm=True,
                searxng_base_url="http://example.test/searxng",
                llm_backend=LLMBackendConfig(model="demo"),
            )
        )
        self.app = FastAPI()
        self.app.state.repository_service = self.repository
        self.app.state.search_jobs = {}
        self.app.state.search_jobs_lock = threading.Lock()
        self.app.include_router(search.router, prefix="/api")
        self.client = TestClient(self.app)

    def tearDown(self) -> None:
        self.client.close()
        self.temp_dir.cleanup()

    def test_get_search_options_returns_normalized_config(self) -> None:
        with patch(
            "backend.routers.search.SearXNGClient.get_config",
            return_value={
                "categories": ["general", "science"],
                "languages": [{"value": "auto", "label": "Auto-detect"}],
                "time_ranges": ["day", "month", "year"],
                "supports_oa_doi_helper": True,
                "defaults": {"categories": ["general"], "language": "auto", "time_range": ""},
            },
        ):
            response = self.client.get("/api/search/options")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["categories"], ["general", "science"])
        self.assertTrue(response.json()["supports_oa_doi_helper"])

    def test_start_search_accepts_filters_and_echoes_them_in_status(self) -> None:
        with patch("backend.routers.search.threading.Thread") as thread_cls:
            response = self.client.post(
                "/api/search/start",
                json={
                    "prompt": "climate policy",
                    "target_count": 100,
                    "categories": ["news"],
                    "language": "en-US",
                    "time_range": "month",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["categories"], ["news"])
        self.assertEqual(payload["language"], "en-US")
        self.assertEqual(payload["time_range"], "month")
        thread_cls.return_value.start.assert_called_once()


class SearchImportAndCitationHintTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="search-import-test-")
        root = Path(self.temp_dir.name)
        self.store = FileStore(base_dir=root / "data", sync_project_profiles=False)
        self.repository = AttachedRepositoryService(store=self.store)
        self.repository.create(str(root / "repo"))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_search_seed_import_backfills_title_author_year_and_seed_doi(self) -> None:
        self.repository.import_source_list(
            "search.csv",
            _build_csv_bytes(
                [
                    {
                        "URL": "https://example.com/article",
                        "Title": "Transformer Interpretability",
                        "Authors": "Ada Lovelace; Grace Hopper",
                        "Year": "2024",
                        "DOI": "10.1234/example",
                    }
                ]
            ),
        )

        with self.repository._writer_lock():
            state = self.repository._load_state_locked()
        rows = _load_source_rows(state.get("sources", []))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "Transformer Interpretability")
        self.assertEqual(rows[0].author_names, "Ada Lovelace; Grace Hopper")
        self.assertEqual(rows[0].publication_year, "2024")
        self.assertEqual(rows[0].seed_doi, "10.1234/example")

    def test_duplicate_search_seed_import_backfills_missing_fields(self) -> None:
        self.repository.import_source_list(
            "initial.csv",
            _build_csv_bytes([{"URL": "https://example.com/article"}]),
        )
        self.repository.import_source_list(
            "search.csv",
            _build_csv_bytes(
                [
                    {
                        "URL": "https://example.com/article",
                        "Title": "Recovered Title",
                        "Authors": "Ada Lovelace",
                        "Year": "2025",
                        "DOI": "10.5555/recovered",
                    }
                ]
            ),
        )

        with self.repository._writer_lock():
            state = self.repository._load_state_locked()
        rows = _load_source_rows(state.get("sources", []))

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].title, "Recovered Title")
        self.assertEqual(rows[0].author_names, "Ada Lovelace")
        self.assertEqual(rows[0].publication_year, "2025")
        self.assertEqual(rows[0].seed_doi, "10.5555/recovered")

    def test_seed_doi_is_used_before_weaker_doi_heuristics(self) -> None:
        row = SourceManifestRow(
            id="000001",
            repository_source_id="000001",
            original_url="https://example.com/paper",
            title="Visible title without DOI",
            seed_doi="10.1234/seeded",
        )

        citation = _build_citation_metadata(
            row=row,
            title="Visible title without DOI",
            author_names="",
            publication_date="",
            document_type="report",
            organization_name="",
            html_metadata={},
        )

        self.assertEqual(citation.doi, "10.1234/seeded")

    def test_oa_helper_only_updates_url_when_current_url_is_blank_or_doi_resolver(self) -> None:
        direct = CitationMetadata(
            doi="10.1234/example",
            title="Authoritative Title",
            url="https://doi.org/10.1234/example",
            evidence=["direct"],
        )
        helper = CitationMetadata(
            doi="10.1234/example",
            url="https://publisher.example/article",
            evidence=["helper"],
        )

        updated = _apply_oa_citation_helper(direct, helper)
        self.assertEqual(updated.title, "Authoritative Title")
        self.assertEqual(updated.url, "https://publisher.example/article")
        self.assertIn("direct", updated.evidence)
        self.assertIn("helper", updated.evidence)

        preserved = _apply_oa_citation_helper(
            CitationMetadata(
                doi="10.1234/example",
                title="Authoritative Title",
                url="https://publisher.example/original",
                evidence=["direct"],
            ),
            helper,
        )
        self.assertEqual(preserved.url, "https://publisher.example/original")
        self.assertEqual(preserved.title, "Authoritative Title")


class SourceDownloaderDoiAutoVerifyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="source-downloader-doi-test-")
        root = Path(self.temp_dir.name)
        self.store = FileStore(base_dir=root / "data", sync_project_profiles=False)
        self.output_dir = root / "output"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_markdown(self, row: SourceManifestRow, text: str) -> None:
        path = self.output_dir / row.markdown_file
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")

    def _build_orchestrator(
        self,
        row: SourceManifestRow,
        *,
        use_llm: bool = False,
        run_catalog: bool = False,
        run_citation_verify: bool = False,
    ) -> SourceDownloadOrchestrator:
        return SourceDownloadOrchestrator(
            job_id="job-001",
            store=self.store,
            use_llm=use_llm,
            llm_backend=LLMBackendConfig(model="demo"),
            run_download=False,
            run_convert=False,
            run_catalog=run_catalog,
            run_citation_verify=run_citation_verify,
            run_llm_cleanup=False,
            run_llm_title=False,
            run_llm_summary=False,
            run_llm_rating=False,
            target_rows=[row],
            output_dir=self.output_dir,
        )

    def test_catalog_persists_authoritative_doi_metadata_without_waiting_for_llm(self) -> None:
        row = SourceManifestRow(
            id="000001",
            repository_source_id="000001",
            source_kind="url",
            original_url="https://example.com/article",
            seed_doi="10.1234/example",
            markdown_file="sources/000001/000001.md",
        )
        self._write_markdown(row, "# Placeholder title\n\nBody text.")
        orchestrator = self._build_orchestrator(row, run_catalog=True)
        doi_citation = CitationMetadata(
            doi="10.1234/example",
            title="Authoritative DOI Title",
            authors=[{"literal": "Ada Lovelace"}, {"literal": "Grace Hopper"}],
            issued="2024-03-01",
            publisher="OpenAI Press",
            url="https://publisher.example/article",
            confidence=0.95,
            ready_for_ris=True,
        )

        with patch(
            "backend.pipeline.source_downloader._resolve_doi_citation_metadata",
            return_value=doi_citation,
        ):
            with patch(
                "backend.pipeline.source_downloader._resolve_searxng_oa_citation_metadata",
                return_value=CitationMetadata(doi="10.1234/example"),
            ):
                orchestrator._generate_source_catalog(row, notes=[])

        self.assertEqual(row.title, "Authoritative DOI Title")
        self.assertEqual(row.author_names, "Ada Lovelace; Grace Hopper")
        self.assertEqual(row.publication_date, "2024-03-01")
        self.assertTrue(row.catalog_file)

        payload = json.loads((self.output_dir / row.catalog_file).read_text(encoding="utf-8"))
        self.assertEqual(payload["title"], "Authoritative DOI Title")
        self.assertEqual(payload["author_names"], "Ada Lovelace; Grace Hopper")
        self.assertEqual(payload["publication_date"], "2024-03-01")
        self.assertEqual(payload["citation"]["title"], "Authoritative DOI Title")
        self.assertEqual(payload["citation"]["verification_status"], "verified")

    def test_citation_verification_skips_llm_for_authoritative_doi_metadata(self) -> None:
        row = SourceManifestRow(
            id="000001",
            repository_source_id="000001",
            source_kind="url",
            original_url="https://example.com/article",
            seed_doi="10.1234/example",
            markdown_file="sources/000001/000001.md",
        )
        self._write_markdown(row, "# Placeholder title\n\nBody text.")
        orchestrator = self._build_orchestrator(
            row,
            use_llm=True,
            run_citation_verify=True,
        )
        doi_citation = CitationMetadata(
            doi="10.1234/example",
            title="Authoritative DOI Title",
            authors=[{"literal": "Ada Lovelace"}, {"literal": "Grace Hopper"}],
            issued="2024-03-01",
            publisher="OpenAI Press",
            url="https://publisher.example/article",
            confidence=0.95,
            ready_for_ris=True,
        )

        with patch(
            "backend.pipeline.source_downloader._resolve_doi_citation_metadata",
            return_value=doi_citation,
        ):
            with patch(
                "backend.pipeline.source_downloader._resolve_searxng_oa_citation_metadata",
                return_value=CitationMetadata(doi="10.1234/example"),
            ):
                with patch.object(
                    orchestrator,
                    "_verify_citation_with_llm",
                    side_effect=AssertionError("LLM verification should be skipped"),
                ):
                    orchestrator._generate_source_citation_verification(row, notes=[])

        payload = json.loads((self.output_dir / row.catalog_file).read_text(encoding="utf-8"))
        self.assertEqual(payload["citation"]["verification_status"], "verified")
        self.assertIn("trusted_doi_registry_metadata", payload["citation"]["notes"])


if __name__ == "__main__":
    unittest.main()
