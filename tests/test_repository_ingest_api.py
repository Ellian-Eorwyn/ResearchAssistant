from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.models.export import EXPORT_COLUMNS
from backend.routers import repository
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


class RepositoryIngestApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="repo-ingest-api-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.service = AttachedRepositoryService(store=self.store)

        self.repo_dir = self.tmp_path / "repo"
        self.repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(self.repo_dir))

        app = FastAPI()
        app.state.file_store = self.store
        app.state.repository_service = self.service
        app.include_router(repository.router, prefix="/api")
        self.client = TestClient(app)

    def tearDown(self):
        self._tmp.cleanup()

    def test_seed_ingest_endpoint_harvests_links_without_creating_citation_rows(self):
        response = self.client.post(
            "/api/repository/ingest/seed-files",
            files=[
                (
                    "files",
                    (
                        "report.md",
                        (
                            "# Deep Research Report\n\n"
                            "- [Alpha](https://example.com/a)\n"
                            "- Beta https://example.com/b\n"
                        ).encode("utf-8"),
                        "text/markdown",
                    ),
                )
            ],
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["import_type"], "source_seed")
        self.assertEqual(payload["accepted_new"], 2)

        state = json.loads(
            (self.repo_dir / ".ra_repo" / "repository_state.json").read_text(encoding="utf-8")
        )
        self.assertEqual(len(state["sources"]), 2)
        self.assertEqual(state["citations"], [])
        citations_csv = (self.repo_dir / "citations.csv").read_text(encoding="utf-8-sig").strip()
        self.assertEqual(citations_csv, ",".join(EXPORT_COLUMNS))

    def test_manual_document_ingest_endpoint_creates_uploaded_document_source(self):
        response = self.client.post(
            "/api/repository/ingest/documents",
            files=[
                (
                    "files",
                    (
                        "note.md",
                        b"# Uploaded Note\n\nLocal evidence.\n",
                        "text/markdown",
                    ),
                )
            ],
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["import_type"], "document_source")
        self.assertEqual(payload["accepted_new"], 1)

        state = json.loads(
            (self.repo_dir / ".ra_repo" / "repository_state.json").read_text(encoding="utf-8")
        )
        self.assertEqual(len(state["sources"]), 1)
        row = state["sources"][0]
        self.assertEqual(row["source_kind"], "uploaded_document")
        self.assertEqual(row["fetch_status"], "not_applicable")
        self.assertTrue(row["raw_file"].startswith("sources/000001/"))
        self.assertTrue((self.repo_dir / row["raw_file"]).is_file())


if __name__ == "__main__":
    unittest.main()
