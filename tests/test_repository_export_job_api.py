from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import repository
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


class RepositoryExportJobApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="repo-export-api-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.service = AttachedRepositoryService(store=self.store)

        repo_dir = self.tmp_path / "repo"
        repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(repo_dir))
        self.first_import = self.service.import_source_list(
            filename="sources.csv",
            content=("URL\nhttps://example.com/a\n").encode("utf-8"),
        )
        self.second_import = self.service.import_source_list(
            filename="sources2.csv",
            content=("URL\nhttps://example.com/b\n").encode("utf-8"),
        )

        app = FastAPI()
        app.state.file_store = self.store
        app.state.repository_service = self.service
        app.include_router(repository.router, prefix="/api")
        self.client = TestClient(app)

    def tearDown(self):
        self._tmp.cleanup()

    def test_export_job_scope_all_success(self):
        resp = self.client.post("/api/repository/export-job", json={"scope": "all"})
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["scope"], "all")
        self.assertEqual(payload["total_urls"], 2)
        self.assertTrue(payload["job_id"])

    def test_export_job_scope_import_success(self):
        resp = self.client.post(
            "/api/repository/export-job",
            json={"scope": "import", "import_id": self.second_import.import_id},
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["scope"], "import")
        self.assertEqual(payload["import_id"], self.second_import.import_id)
        self.assertEqual(payload["total_urls"], 1)

    def test_export_job_invalid_scope_returns_400(self):
        resp = self.client.post("/api/repository/export-job", json={"scope": "unsupported"})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("Invalid scope", resp.json()["detail"])

    def test_export_job_missing_import_id_returns_400(self):
        resp = self.client.post("/api/repository/export-job", json={"scope": "import"})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("import_id", resp.json()["detail"])

    def test_export_job_empty_import_scope_returns_409(self):
        dup = self.service.import_source_list(
            filename="sources-dup.csv",
            content=("URL\nhttps://example.com/a\n").encode("utf-8"),
        )
        self.assertEqual(dup.accepted_new, 0)

        resp = self.client.post(
            "/api/repository/export-job",
            json={"scope": "import", "import_id": dup.import_id},
        )
        self.assertEqual(resp.status_code, 409)
        self.assertIn("No URLs available", resp.json()["detail"])


if __name__ == "__main__":
    unittest.main()
