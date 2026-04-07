from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.models.settings import LLMBackendConfig
from backend.models.sources import SourceManifestRow, SourcePhaseMetadata
from backend.pipeline.source_downloader import SourceDownloadOrchestrator
from backend.storage.file_store import FileStore


class DummyLLMClient:
    def __init__(self, config):
        self.config = config

    def sync_chat_completion(self, *, system_prompt: str, user_prompt: str, response_format=None):
        if response_format == "json":
            return json.dumps(
                {
                    "overall_score": 0.81,
                    "confidence": 0.72,
                    "rationale": "Strong enough for the research question.",
                }
            )
        return "Concise summary of the source."

    def sync_close(self):
        return None


class SourceDownloaderAgentPhaseTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="source-downloader-agent-phase-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")

    def tearDown(self):
        self._tmp.cleanup()

    def test_convert_phase_can_run_without_download_when_raw_html_exists(self):
        job_id = self.store.create_job()
        output_dir = self.store.get_sources_output_dir(job_id)
        raw_path = output_dir / "originals" / "000001_source.html"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(
            "<html><main><h1>Alpha</h1><p>Independent convert phase text.</p></main></html>",
            encoding="utf-8",
        )

        row = SourceManifestRow(
            id="000001",
            original_url="https://example.com/a",
            final_url="https://example.com/a",
            fetch_status="success",
            detected_type="html",
            raw_file="originals/000001_source.html",
        )

        orchestrator = SourceDownloadOrchestrator(
            job_id=job_id,
            store=self.store,
            run_download=False,
            run_convert=True,
            run_llm_cleanup=False,
            run_llm_title=False,
            run_llm_summary=False,
            run_llm_rating=False,
            target_rows=[row],
        )

        orchestrator.run()
        manifest = self.store.load_artifact(job_id, "06_sources_manifest")
        self.assertIsNotNone(manifest)
        result_row = manifest["rows"][0]
        self.assertTrue(result_row["markdown_file"])
        self.assertEqual(result_row["phase_metadata"]["convert"]["status"], "completed")
        self.assertTrue(result_row["phase_metadata"]["convert"]["content_digest"])

    def test_convert_phase_marks_summary_and_tag_stale_when_markdown_changes(self):
        job_id = self.store.create_job()
        output_dir = self.store.get_sources_output_dir(job_id)
        raw_path = output_dir / "originals" / "000001_source.html"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(
            "<html><main><h1>Updated</h1><p>Updated markdown content.</p></main></html>",
            encoding="utf-8",
        )

        row = SourceManifestRow(
            id="000001",
            original_url="https://example.com/a",
            final_url="https://example.com/a",
            fetch_status="success",
            detected_type="html",
            raw_file="originals/000001_source.html",
            summary_status="generated",
            rating_status="generated",
            phase_metadata={
                "summarize": SourcePhaseMetadata(
                    phase="summarize",
                    status="completed",
                    content_digest="old-summary-digest",
                ),
                "tag": SourcePhaseMetadata(
                    phase="tag",
                    status="completed",
                    content_digest="old-rating-digest",
                ),
            },
        )

        orchestrator = SourceDownloadOrchestrator(
            job_id=job_id,
            store=self.store,
            run_download=False,
            run_convert=True,
            run_llm_cleanup=False,
            run_llm_title=False,
            run_llm_summary=False,
            run_llm_rating=False,
            target_rows=[row],
        )

        orchestrator.run()
        manifest = self.store.load_artifact(job_id, "06_sources_manifest")
        result_row = manifest["rows"][0]
        self.assertEqual(result_row["summary_status"], "stale")
        self.assertEqual(result_row["rating_status"], "stale")
        self.assertTrue(result_row["phase_metadata"]["summary"]["stale"])
        self.assertTrue(result_row["phase_metadata"]["rating"]["stale"])

    def test_summary_and_tag_can_run_from_existing_markdown(self):
        job_id = self.store.create_job()
        output_dir = self.store.get_sources_output_dir(job_id)
        markdown_path = output_dir / "markdown" / "000001_clean.md"
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            "# Alpha\n\nEvidence for apprenticeship curriculum updates.\n",
            encoding="utf-8",
        )

        row = SourceManifestRow(
            id="000001",
            original_url="https://example.com/a",
            final_url="https://example.com/a",
            fetch_status="success",
            detected_type="html",
            markdown_file="markdown/000001_clean.md",
        )

        with patch("backend.pipeline.source_downloader.llm_backend_ready_for_chat", return_value=True), patch(
            "backend.pipeline.source_downloader.UnifiedLLMClient",
            DummyLLMClient,
        ):
            orchestrator = SourceDownloadOrchestrator(
                job_id=job_id,
                store=self.store,
                use_llm=True,
                llm_backend=LLMBackendConfig(model="test-model"),
                project_profile_name="custom_profile.yaml",
                project_profile_yaml="name: Test Profile\ndescription: Score relevance.\n",
                run_download=False,
                run_convert=False,
                run_llm_cleanup=False,
                run_llm_title=False,
                run_llm_summary=True,
                run_llm_rating=True,
                target_rows=[row],
            )
            orchestrator.run()

        manifest = self.store.load_artifact(job_id, "06_sources_manifest")
        result_row = manifest["rows"][0]
        self.assertTrue(result_row["summary_file"])
        self.assertTrue(result_row["rating_file"])
        self.assertEqual(result_row["phase_metadata"]["summary"]["status"], "completed")
        self.assertEqual(result_row["phase_metadata"]["rating"]["status"], "completed")

    def test_title_only_phase_does_not_generate_catalog_artifact(self):
        job_id = self.store.create_job()
        output_dir = self.store.get_sources_output_dir(job_id)
        markdown_path = output_dir / "markdown" / "000001_clean.md"
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            "# Existing Source Title\n\nBody text.\n",
            encoding="utf-8",
        )

        row = SourceManifestRow(
            id="000001",
            original_url="https://example.com/title-only",
            final_url="https://example.com/title-only",
            fetch_status="success",
            detected_type="html",
            markdown_file="markdown/000001_clean.md",
        )

        orchestrator = SourceDownloadOrchestrator(
            job_id=job_id,
            store=self.store,
            use_llm=False,
            run_download=False,
            run_convert=False,
            run_catalog=False,
            run_citation_verify=False,
            run_llm_cleanup=False,
            run_llm_title=True,
            run_llm_summary=False,
            run_llm_rating=False,
            target_rows=[row],
        )

        orchestrator.run()
        manifest = self.store.load_artifact(job_id, "06_sources_manifest")
        result_row = manifest["rows"][0]
        self.assertEqual(result_row["title"], "Existing Source Title")
        self.assertEqual(result_row["title_status"], "extracted")
        self.assertEqual(result_row["catalog_status"], "")
        self.assertEqual(result_row["phase_metadata"]["title"]["status"], "completed")

    def test_citation_verify_only_preserves_existing_catalog_metadata(self):
        job_id = self.store.create_job()
        output_dir = self.store.get_sources_output_dir(job_id)
        markdown_path = output_dir / "markdown" / "000001_clean.md"
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            "# Alpha\n\nBody text.\n",
            encoding="utf-8",
        )
        metadata_dir = output_dir / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        catalog_path = metadata_dir / "000001_catalog.json"
        catalog_path.write_text(
            json.dumps(
                {
                    "title": "Pinned Title",
                    "author_names": "Pinned Author",
                    "publication_date": "2024-01-02",
                    "publication_year": "2024",
                    "document_type": "report",
                    "organization_name": "Pinned Org",
                    "organization_type": "agency",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        row = SourceManifestRow(
            id="000001",
            original_url="https://example.com/citation-only",
            final_url="https://example.com/citation-only",
            fetch_status="success",
            detected_type="html",
            markdown_file="markdown/000001_clean.md",
            catalog_file="metadata/000001_catalog.json",
            title="Pinned Title",
            author_names="Pinned Author",
            publication_date="2024-01-02",
            publication_year="2024",
            document_type="report",
            organization_name="Pinned Org",
            organization_type="agency",
        )

        with patch("backend.pipeline.source_downloader.llm_backend_ready_for_chat", return_value=False):
            orchestrator = SourceDownloadOrchestrator(
                job_id=job_id,
                store=self.store,
                use_llm=False,
                run_download=False,
                run_convert=False,
                run_catalog=False,
                run_citation_verify=True,
                run_llm_cleanup=False,
                run_llm_title=False,
                run_llm_summary=False,
                run_llm_rating=False,
                target_rows=[row],
            )
            orchestrator.run()

        manifest = self.store.load_artifact(job_id, "06_sources_manifest")
        result_row = manifest["rows"][0]
        self.assertEqual(result_row["title"], "Pinned Title")
        self.assertEqual(result_row["author_names"], "Pinned Author")
        self.assertEqual(result_row["catalog_status"], "")
        self.assertEqual(result_row["phase_metadata"]["citation_verify"]["status"], "skipped")

    def test_catalog_phase_persists_deterministic_metadata_from_markdown(self):
        job_id = self.store.create_job()
        output_dir = self.store.get_sources_output_dir(job_id)
        markdown_path = output_dir / "markdown" / "000001_clean.md"
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            "---\n"
            "title: State Housing Retrofit Report\n"
            "authors:\n"
            "  - Jane Doe\n"
            "  - John Roe\n"
            "date: 2024-03-15\n"
            "---\n\n"
            "# State Housing Retrofit Report\n\n"
            "Body text.\n",
            encoding="utf-8",
        )

        row = SourceManifestRow(
            id="000001",
            original_url="https://example.com/report",
            final_url="https://example.com/report",
            fetch_status="success",
            detected_type="html",
            markdown_file="markdown/000001_clean.md",
        )

        orchestrator = SourceDownloadOrchestrator(
            job_id=job_id,
            store=self.store,
            use_llm=False,
            run_download=False,
            run_convert=False,
            run_catalog=True,
            run_llm_cleanup=False,
            run_llm_title=False,
            run_llm_summary=False,
            run_llm_rating=False,
            target_rows=[row],
        )

        orchestrator.run()
        manifest = self.store.load_artifact(job_id, "06_sources_manifest")
        result_row = manifest["rows"][0]
        self.assertEqual(result_row["catalog_status"], "generated")
        self.assertTrue(result_row["catalog_file"])
        self.assertEqual(result_row["author_names"], "Jane Doe; John Roe")
        self.assertEqual(result_row["publication_date"], "2024-03-15")
        self.assertEqual(result_row["publication_year"], "2024")
        self.assertEqual(result_row["document_type"], "report")
        self.assertEqual(result_row["phase_metadata"]["catalog"]["status"], "completed")

    def test_uploaded_document_skips_fetch_and_runs_convert(self):
        job_id = self.store.create_job()
        output_dir = self.store.get_sources_output_dir(job_id)
        raw_path = output_dir / "sources" / "000002" / "000002_note.md"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text("# Uploaded Note\n\nEvidence captured locally.\n", encoding="utf-8")

        row = SourceManifestRow(
            id="000002",
            source_kind="uploaded_document",
            source_document_name="note.md",
            original_url="",
            final_url="",
            fetch_status="not_applicable",
            detected_type="document",
            raw_file="sources/000002/000002_note.md",
        )

        orchestrator = SourceDownloadOrchestrator(
            job_id=job_id,
            store=self.store,
            run_download=True,
            run_convert=True,
            run_catalog=False,
            run_llm_cleanup=False,
            run_llm_title=False,
            run_llm_summary=False,
            run_llm_rating=False,
            target_rows=[row],
        )

        orchestrator.run()
        manifest = self.store.load_artifact(job_id, "06_sources_manifest")
        result_row = manifest["rows"][0]
        self.assertEqual(result_row["source_kind"], "uploaded_document")
        self.assertEqual(result_row["fetch_status"], "not_applicable")
        self.assertEqual(result_row["phase_metadata"]["fetch"]["status"], "skipped")
        self.assertEqual(result_row["phase_metadata"]["fetch"]["error_code"], "not_applicable")
        self.assertTrue(result_row["markdown_file"])
        self.assertEqual(result_row["phase_metadata"]["convert"]["status"], "completed")


if __name__ == "__main__":
    unittest.main()
