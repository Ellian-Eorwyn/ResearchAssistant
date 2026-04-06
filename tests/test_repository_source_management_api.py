from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import repository
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


class RepositorySourceManagementApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="repo-source-management-api-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.service = AttachedRepositoryService(store=self.store)

        self.repo_dir = self.tmp_path / "repo"
        self.repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(self.repo_dir))
        self.service.import_source_list(
            filename="sources.csv",
            content=(
                "URL,Title\n"
                "https://example.com/a,Alpha Source\n"
                "https://example.com/b,Beta Source\n"
            ).encode("utf-8"),
        )

        self.external_markdown = self.tmp_path / "external-cleanup.md"
        self.external_markdown.write_text("# External cleanup\n", encoding="utf-8")

        state_path = self.repo_dir / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        rows = state["sources"]

        rows[0]["title"] = "Alpha Source"
        rows[0]["fetch_status"] = "success"
        rows[0]["raw_file"] = "sources/000001/000001_source.pdf"
        rows[0]["markdown_file"] = "sources/000001/000001_clean.md"
        rows[0]["metadata_file"] = "sources/000001/000001_metadata.json"

        rows[1]["title"] = "Beta Source"
        rows[1]["fetch_status"] = "success"
        rows[1]["raw_file"] = "sources/000002/000002_source.html"
        rows[1]["rendered_file"] = "sources/000002/000002_rendered.html"
        rows[1]["rendered_pdf_file"] = "sources/000002/000002_rendered.pdf"
        rows[1]["markdown_file"] = "sources/000002/000002_clean.md"
        rows[1]["llm_cleanup_file"] = str(self.external_markdown)
        rows[1]["metadata_file"] = "sources/000002/000002_metadata.json"

        state["citations"] = [
            {
                "repository_source_id": "000001",
                "cited_url": "https://example.com/a",
                "cited_title": "Alpha Citation",
                "match_confidence": 0.9,
            },
            {
                "repository_source_id": "000002",
                "cited_url": "https://example.com/b",
                "cited_title": "Beta Citation",
                "match_confidence": 0.7,
            },
        ]
        state_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        (self.repo_dir / "sources" / "000001").mkdir(parents=True, exist_ok=True)
        (self.repo_dir / "sources" / "000002").mkdir(parents=True, exist_ok=True)
        (self.repo_dir / "sources" / "000001" / "000001_source.pdf").write_bytes(b"%PDF-1.4\n")
        (self.repo_dir / "sources" / "000001" / "000001_clean.md").write_text(
            "# Alpha markdown\n",
            encoding="utf-8",
        )
        (self.repo_dir / "sources" / "000001" / "000001_metadata.json").write_text(
            json.dumps({"id": "000001"}, ensure_ascii=False),
            encoding="utf-8",
        )
        (self.repo_dir / "sources" / "000002" / "000002_source.html").write_text(
            "<html><body>Raw HTML</body></html>",
            encoding="utf-8",
        )
        (self.repo_dir / "sources" / "000002" / "000002_rendered.html").write_text(
            "<html><body><script>alert(1)</script>Rendered</body></html>",
            encoding="utf-8",
        )
        (self.repo_dir / "sources" / "000002" / "000002_rendered.pdf").write_bytes(b"%PDF-1.4 rendered\n")
        (self.repo_dir / "sources" / "000002" / "000002_clean.md").write_text(
            "# Beta markdown\n",
            encoding="utf-8",
        )
        (self.repo_dir / "sources" / "000002" / "000002_metadata.json").write_text(
            json.dumps({"id": "000002"}, ensure_ascii=False),
            encoding="utf-8",
        )

        self.service.rebuild()

        app = FastAPI()
        app.state.file_store = self.store
        app.state.repository_service = self.service
        app.include_router(repository.router, prefix="/api")
        self.client = TestClient(app)

    def tearDown(self):
        self._tmp.cleanup()

    def test_open_repository_source_file_serves_expected_artifact(self):
        pdf_response = self.client.get("/api/repository/sources/000001/files/pdf")
        self.assertEqual(pdf_response.status_code, 200)
        self.assertEqual(pdf_response.headers["content-type"], "application/pdf")

        html_response = self.client.get("/api/repository/sources/000002/files/html")
        self.assertEqual(html_response.status_code, 200)
        self.assertIn("text/html", html_response.headers["content-type"])
        self.assertIn("sandbox", html_response.headers["content-security-policy"])

        rendered_response = self.client.get("/api/repository/sources/000002/files/rendered")
        self.assertEqual(rendered_response.status_code, 200)
        self.assertIn("text/html", rendered_response.headers["content-type"])

        markdown_response = self.client.get("/api/repository/sources/000002/files/md")
        self.assertEqual(markdown_response.status_code, 200)
        self.assertIn("text/plain", markdown_response.headers["content-type"])

    def test_open_repository_source_file_returns_404_when_kind_missing(self):
        response = self.client.get("/api/repository/sources/000001/files/html")
        self.assertEqual(response.status_code, 404)
        self.assertIn("No file available", response.json()["detail"])

    def test_bulk_delete_removes_rows_repo_files_and_linked_citations(self):
        response = self.client.post(
            "/api/repository/sources/bulk-delete",
            json={"source_ids": ["000002"]},
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["deleted_sources"], 1)
        self.assertEqual(payload["deleted_citations"], 1)
        self.assertTrue((self.repo_dir / "sources" / "000001" / "000001_source.pdf").exists())
        self.assertFalse((self.repo_dir / "sources" / "000002" / "000002_source.html").exists())
        self.assertFalse((self.repo_dir / "sources" / "000002" / "000002_rendered.html").exists())
        self.assertFalse((self.repo_dir / "sources" / "000002" / "000002_rendered.pdf").exists())
        self.assertFalse((self.repo_dir / "sources" / "000002" / "000002_clean.md").exists())
        self.assertTrue(self.external_markdown.exists())

        state = json.loads(
            (self.repo_dir / ".ra_repo" / "repository_state.json").read_text(encoding="utf-8")
        )
        self.assertEqual([row["id"] for row in state["sources"]], ["000001"])
        self.assertEqual(len(state["citations"]), 1)
        self.assertEqual(state["citations"][0]["repository_source_id"], "000001")

    def test_export_source_files_flattens_names_and_suffixes_collisions(self):
        export_dir = self.tmp_path / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        (export_dir / "000001 - Alpha Source.md").write_text("existing\n", encoding="utf-8")

        response = self.client.post(
            "/api/repository/sources/export-files",
            json={
                "source_ids": ["000001", "000002"],
                "file_kinds": ["md", "html"],
                "destination_path": str(export_dir),
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["requested_sources"], 2)
        self.assertEqual(payload["exported_files"], 3)
        self.assertEqual(payload["missing_files"], 1)
        self.assertTrue((export_dir / "000001 - Alpha Source (2).md").exists())
        self.assertTrue((export_dir / "000002 - Beta Source.html").exists())
        self.assertTrue((export_dir / "000002 - Beta Source.md").exists())
        self.assertEqual(
            (export_dir / "000002 - Beta Source.md").read_text(encoding="utf-8"),
            "# External cleanup\n",
        )

    def test_patch_source_updates_manifest_catalog_summary_and_rating_artifacts(self):
        response = self.client.patch(
            "/api/repository/sources/000001",
            json={
                "title": "Alpha Source Revised",
                "author_names": "Jane Doe; John Roe",
                "publication_date": "2025-03-15",
                "document_type": "report",
                "organization_name": "Alpha Agency",
                "organization_type": "government",
                "tags_text": "housing, retrofit",
                "notes": "Reviewed by analyst",
                "summary_text": "This report summarizes retrofit pilots.",
                "overall_relevance": 0.9,
                "depth_score": 0.75,
                "relevant_detail_score": 0.8,
                "rating_rationale": "Directly addresses retrofit implementation evidence.",
                "relevant_sections": "Executive summary\nAppendix B",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["title"], "Alpha Source Revised")
        self.assertEqual(payload["author_names"], "Jane Doe; John Roe")
        self.assertEqual(payload["summary_text"], "This report summarizes retrofit pilots.")
        self.assertEqual(payload["rating_overall_relevance"], 0.9)
        self.assertEqual(payload["rating_depth_score"], 0.75)
        self.assertEqual(payload["rating_relevant_detail_score"], 0.8)
        self.assertEqual(
            payload["rating_rationale"],
            "Directly addresses retrofit implementation evidence.",
        )
        self.assertEqual(payload["relevant_sections"], "Executive summary\n\nAppendix B")

        summary_path = self.repo_dir / "sources" / "000001" / "000001_summary.md"
        rating_path = self.repo_dir / "sources" / "000001" / "000001_rating.json"
        catalog_path = self.repo_dir / "sources" / "000001" / "000001_catalog.json"
        metadata_path = self.repo_dir / "sources" / "000001" / "000001_metadata.json"

        self.assertTrue(summary_path.exists())
        self.assertTrue(rating_path.exists())
        self.assertTrue(catalog_path.exists())
        self.assertTrue(metadata_path.exists())

        catalog_payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        self.assertEqual(catalog_payload["title"], "Alpha Source Revised")
        self.assertEqual(catalog_payload["author_names"], "Jane Doe; John Roe")
        self.assertEqual(catalog_payload["citation"]["title"], "")
        self.assertEqual(catalog_payload["citation"]["publisher"], "")

        rating_payload = json.loads(rating_path.read_text(encoding="utf-8"))
        self.assertEqual(rating_payload["ratings"]["overall_relevance"], 0.9)
        self.assertEqual(rating_payload["ratings"]["depth_score"], 0.75)
        self.assertEqual(rating_payload["ratings"]["relevant_detail_score"], 0.8)
        self.assertEqual(
            rating_payload["relevant_sections"],
            ["Executive summary", "Appendix B"],
        )

        self.assertIn("This report summarizes retrofit pilots.", summary_path.read_text(encoding="utf-8"))

        state = json.loads(
            (self.repo_dir / ".ra_repo" / "repository_state.json").read_text(encoding="utf-8")
        )
        source_row = next(row for row in state["sources"] if row["id"] == "000001")
        self.assertEqual(source_row["summary_status"], "existing")
        self.assertEqual(source_row["rating_status"], "existing")
        self.assertEqual(source_row["catalog_status"], "existing")
        self.assertEqual(source_row["publication_year"], "2025")

    def test_patch_source_updates_verified_citation_metadata_separately(self):
        response = self.client.patch(
            "/api/repository/sources/000001",
            json={
                "citation_title": "Alpha Citation Title",
                "citation_authors": "Jane Doe; John Roe",
                "citation_issued": "2025-03-15",
                "citation_type": "report",
                "citation_publisher": "Alpha Agency",
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["citation_title"], "Alpha Citation Title")
        self.assertEqual(payload["citation_authors"], "Jane Doe; John Roe")
        self.assertEqual(payload["citation_issued"], "2025-03-15")
        self.assertEqual(payload["citation_type"], "report")
        self.assertEqual(payload["citation_verification_status"], "verified")
        self.assertTrue(payload["citation_ready"])

        catalog_path = self.repo_dir / "sources" / "000001" / "000001_catalog.json"
        catalog_payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        citation = catalog_payload["citation"]

        self.assertEqual(citation["title"], "Alpha Citation Title")
        self.assertEqual(citation["issued"], "2025-03-15")
        self.assertEqual(citation["item_type"], "report")
        self.assertEqual(citation["publisher"], "Alpha Agency")
        self.assertEqual(citation["verification_status"], "verified")
        self.assertTrue(citation["ready_for_ris"])
        self.assertIn("title", citation["manual_override_fields"])
        self.assertIn("authors", citation["manual_override_fields"])
        self.assertIn("issued", citation["manual_override_fields"])
        self.assertIn("item_type", citation["manual_override_fields"])
        self.assertTrue(citation["field_evidence"]["title"]["manual_override"])
        self.assertEqual(citation["field_evidence"]["title"]["source_type"], "manual_override")


if __name__ == "__main__":
    unittest.main()
