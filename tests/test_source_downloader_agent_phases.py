from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx

from backend.models.settings import LLMBackendConfig
from backend.models.sources import SourceManifestRow, SourceOutputOptions, SourcePhaseMetadata
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

    def test_html_sources_can_write_visual_pdf_without_markdown_conversion(self):
        job_id = self.store.create_job()
        orchestrator = SourceDownloadOrchestrator(
            job_id=job_id,
            store=self.store,
            run_download=True,
            run_convert=False,
            run_llm_cleanup=False,
            run_llm_title=False,
            run_llm_summary=False,
            run_llm_rating=False,
            output_options=SourceOutputOptions(
                include_raw_file=False,
                include_rendered_html=False,
                include_rendered_pdf=True,
                include_markdown=False,
            ),
        )
        orchestrator._ensure_output_dirs()

        row = SourceManifestRow(
            id="000001",
            original_url="https://example.com/visual-pdf",
            final_url="https://example.com/visual-pdf",
            detected_type="html",
        )
        response = httpx.Response(
            200,
            headers={"content-type": "text/html; charset=utf-8"},
            content=(
                b"<html><head><title>Visual PDF</title></head>"
                b"<body><main>Rendered PDF only.</main></body></html>"
            ),
            request=httpx.Request("GET", "https://example.com/visual-pdf"),
        )
        notes: list[str] = []

        class DummyRenderer:
            def capture_visual_pdf(self, url: str):
                return b"%PDF-1.4 visual\n", "", ["visual_capture_mock"]

            def render(self, url: str):
                raise AssertionError("Rendered HTML should not be requested for PDF-only capture")

        orchestrator._handle_html_response(
            row,
            response,
            "https://example.com/visual-pdf",
            DummyRenderer(),
            notes,
        )

        pdf_path = self.store.get_sources_output_dir(job_id) / "rendered" / "000001_rendered.pdf"
        self.assertEqual(row.fetch_status, "success")
        self.assertEqual(row.rendered_pdf_file, "rendered/000001_rendered.pdf")
        self.assertEqual(pdf_path.read_bytes(), b"%PDF-1.4 visual\n")
        self.assertFalse(row.raw_file)
        self.assertFalse(row.rendered_file)
        self.assertFalse(row.markdown_file)
        self.assertIn("visual_capture_mock", notes)

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

    def test_citation_verify_revisits_blocked_uploaded_documents_when_repository_reference_is_available(self):
        job_id = self.store.create_job()
        output_dir = self.store.get_sources_output_dir(job_id)
        markdown_path = output_dir / "markdown" / "000001_clean.md"
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            "# Uploaded Source\n\nBody text.\n",
            encoding="utf-8",
        )
        raw_path = output_dir / "sources" / "000001" / "Uploaded Source.pdf"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_bytes(b"%PDF-1.4 uploaded\n")
        metadata_dir = output_dir / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        catalog_path = metadata_dir / "000001_catalog.json"
        source_digest = hashlib.sha256(markdown_path.read_bytes()).hexdigest()
        catalog_path.write_text(
            json.dumps(
                {
                    "citation": {
                        "item_type": "report",
                        "title": "Uploaded Source",
                        "authors": [{"family": "Doe", "given": "Jane", "literal": ""}],
                        "issued": "2024",
                        "publisher": "Example Org",
                        "url": "",
                        "verification_status": "blocked",
                        "verification_content_digest": source_digest,
                        "missing_fields": ["url"],
                        "ready_for_ris": False,
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        row = SourceManifestRow(
            id="000001",
            source_kind="uploaded_document",
            source_document_name="Uploaded Source.pdf",
            fetch_status="not_applicable",
            detected_type="pdf",
            raw_file="sources/000001/Uploaded Source.pdf",
            markdown_file="markdown/000001_clean.md",
            catalog_file="metadata/000001_catalog.json",
            title="Uploaded Source",
            author_names="Jane Doe",
            publication_date="2024",
            publication_year="2024",
            document_type="report",
            organization_name="Example Org",
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
        updated_catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        citation = updated_catalog["citation"]
        self.assertEqual(result_row["phase_metadata"]["citation_verify"]["status"], "skipped")
        self.assertEqual(citation["url"], "repository:///sources/000001/Uploaded%20Source.pdf")
        self.assertTrue(citation["ready_for_ris"])
        self.assertEqual(citation["missing_fields"], [])

    def test_citation_verify_reprocesses_non_ready_catalogs_and_keeps_existing_citation_fields(self):
        job_id = self.store.create_job()
        output_dir = self.store.get_sources_output_dir(job_id)
        markdown_path = output_dir / "markdown" / "000001_clean.md"
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            "# Sparse Markdown\n\nBody without explicit citation fields.\n",
            encoding="utf-8",
        )
        metadata_dir = output_dir / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        catalog_path = metadata_dir / "000001_catalog.json"
        source_digest = hashlib.sha256(markdown_path.read_bytes()).hexdigest()
        catalog_path.write_text(
            json.dumps(
                {
                    "citation": {
                        "item_type": "report",
                        "title": "Existing Catalog Title",
                        "authors": [{"family": "Doe", "given": "Jane", "literal": ""}],
                        "issued": "2024",
                        "publisher": "Existing Org",
                        "verification_status": "blocked",
                        "verification_content_digest": source_digest,
                        "missing_fields": ["url"],
                        "ready_for_ris": False,
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        row = SourceManifestRow(
            id="000001",
            original_url="",
            final_url="",
            fetch_status="success",
            detected_type="document",
            markdown_file="markdown/000001_clean.md",
            catalog_file="metadata/000001_catalog.json",
            title="",
            author_names="",
            publication_date="",
            publication_year="",
            document_type="",
            organization_name="",
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

        updated_catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        citation = updated_catalog["citation"]
        self.assertTrue(citation["title"])
        self.assertEqual(citation["authors"][0]["family"], "Doe")
        self.assertEqual(citation["issued"], "2024")
        self.assertTrue(citation["ready_for_ris"])
        self.assertEqual(citation["verification_status"], "skipped_llm_disabled")

    def test_citation_verify_uses_publication_year_when_publication_date_is_blank(self):
        job_id = self.store.create_job()
        output_dir = self.store.get_sources_output_dir(job_id)
        markdown_path = output_dir / "markdown" / "000001_clean.md"
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(
            "# Alpha Source\n\nBody text.\n",
            encoding="utf-8",
        )
        metadata_dir = output_dir / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        catalog_path = metadata_dir / "000001_catalog.json"
        catalog_path.write_text("{}", encoding="utf-8")

        row = SourceManifestRow(
            id="000001",
            original_url="",
            final_url="",
            fetch_status="success",
            detected_type="document",
            markdown_file="markdown/000001_clean.md",
            catalog_file="metadata/000001_catalog.json",
            title="Alpha Source",
            author_names="Jane Doe",
            publication_date="",
            publication_year="2024",
            document_type="report",
            organization_name="Example Org",
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

        updated_catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        citation = updated_catalog["citation"]
        self.assertEqual(citation["issued"], "2024")
        self.assertTrue(citation["ready_for_ris"])
        self.assertEqual(citation["verification_status"], "skipped_llm_disabled")

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
