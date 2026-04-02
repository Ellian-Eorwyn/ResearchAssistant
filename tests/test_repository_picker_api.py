from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import repository
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


class RepositoryPickDirectoryApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="repo-picker-api-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.service = AttachedRepositoryService(store=self.store)

        app = FastAPI()
        app.state.file_store = self.store
        app.state.repository_service = self.service
        app.include_router(repository.router, prefix="/api")
        self.client = TestClient(app)

    def tearDown(self):
        self._tmp.cleanup()

    @patch("backend.routers.repository._pick_directory_dialog")
    def test_pick_directory_returns_selected_path(self, mock_pick):
        mock_pick.return_value = "/tmp/selected-repo"

        response = self.client.get(
            "/api/repository/pick-directory?mode=open&initial_path=/tmp"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["path"], "/tmp/selected-repo")
        mock_pick.assert_called_once_with("open", initial_path="/tmp")

    @patch("backend.routers.repository._pick_directory_dialog")
    def test_pick_directory_supports_export_mode(self, mock_pick):
        mock_pick.return_value = "/tmp/export-destination"

        response = self.client.get(
            "/api/repository/pick-directory?mode=export&initial_path=/tmp"
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["path"], "/tmp/export-destination")
        mock_pick.assert_called_once_with("export", initial_path="/tmp")

    def test_pick_directory_rejects_invalid_mode(self):
        response = self.client.get("/api/repository/pick-directory?mode=invalid")
        self.assertEqual(response.status_code, 400)
        self.assertIn("Invalid mode", response.json()["detail"])

    @patch("backend.routers.repository._pick_directory_dialog")
    def test_pick_directory_returns_501_when_picker_unavailable(self, mock_pick):
        mock_pick.side_effect = RuntimeError("Native folder picker is unavailable in this runtime.")

        response = self.client.get("/api/repository/pick-directory?mode=create")

        self.assertEqual(response.status_code, 501)
        self.assertIn("Native folder picker is unavailable", response.json()["detail"])


if __name__ == "__main__":
    unittest.main()
