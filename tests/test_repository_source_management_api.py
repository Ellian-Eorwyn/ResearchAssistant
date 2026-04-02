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


if __name__ == "__main__":
    unittest.main()
