from __future__ import annotations

import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import repository
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


class RepositoryDocumentReprocessApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="repo-document-reprocess-api-tests-")
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

    def test_document_imports_list_and_reprocess_start(self):
        document_dir = self.repo_dir / "documents" / "import123"
        document_dir.mkdir(parents=True, exist_ok=True)
        document_path = document_dir / "report.md"
        document_bytes = b"# Report\n\nAlpha claim.\n"
        document_path.write_bytes(document_bytes)

        with self.service._writer_lock():
            self.service._save_state_locked(
                sources=[],
                citations=[],
                imports=[
                    {
                        "import_id": "import123",
                        "import_type": "document_process",
                        "imported_at": "2026-01-01T00:00:00+00:00",
                        "documents": [
                            {
                                "filename": "report.md",
                                "source_document_name": "report.md",
                                "repository_path": "documents/import123/report.md",
                                "sha256": hashlib.sha256(document_bytes).hexdigest(),
                                "document_import_id": "import123",
                            }
                        ],
                        "selected_profile_id": "generic_numeric_academic",
                    }
                ],
            )

        list_response = self.client.get("/api/repository/document-imports")
        self.assertEqual(list_response.status_code, 200)
        list_payload = list_response.json()
        self.assertEqual(len(list_payload["imports"]), 1)
        self.assertEqual(list_payload["imports"][0]["import_id"], "import123")
        self.assertTrue(list_payload["imports"][0]["rerunnable"])
        self.assertEqual(list_payload["imports"][0]["documents"][0]["filename"], "report.md")

        with patch.object(
            AttachedRepositoryService,
            "_reprocess_documents_worker",
            autospec=True,
        ) as mock_worker:
            response = self.client.post(
                "/api/repository/reprocess-documents",
                json={
                    "target_import_ids": ["import123"],
                    "profile_override": "generic_author_year_academic",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["accepted_documents"], 1)
        self.assertEqual(payload["target_import_ids"], ["import123"])
        self.assertEqual(payload["selected_profile_id"], "generic_author_year_academic")

        job_store = self.service.job_store_for(payload["job_id"])
        context = job_store.load_artifact(payload["job_id"], "repo_processing_context")
        self.assertIsNotNone(context)
        self.assertEqual(context["processing_mode"], "reprocess_documents")
        self.assertEqual(context["target_import_ids"], ["import123"])
        self.assertEqual(context["documents"][0]["source_document_name"], "report.md")

        status = job_store.get_job_status(payload["job_id"])
        self.assertIsNotNone(status)
        self.assertEqual(status["processing_mode"], "reprocess_documents")
        self.assertEqual(status["target_import_ids"], ["import123"])
        self.assertEqual(status["repository_preprocess_state"], "pending")
        self.assertTrue(mock_worker.called)


if __name__ == "__main__":
    unittest.main()
