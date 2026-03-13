from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from backend.models.sources import SourceManifestRow
from backend.storage.attached_repository import AttachedRepositoryService, repository_dedupe_key
from backend.storage.file_store import FileStore


class RepositoryServiceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="repo-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.service = AttachedRepositoryService(store=self.store)

    def tearDown(self):
        self._tmp.cleanup()

    def test_repository_dedupe_key_strips_tracking_params(self):
        a = "https://example.com/path?a=1&utm_source=test#section"
        b = "https://example.com/path?a=1"
        c = "https://example.com/path?a=1&fbclid=abc"

        self.assertEqual(repository_dedupe_key(a), repository_dedupe_key(b))
        self.assertEqual(repository_dedupe_key(b), repository_dedupe_key(c))

    def test_merge_source_rows_continues_numeric_ids(self):
        rows = [
            SourceManifestRow(id="000010", original_url="https://example.com/a", fetch_status="success"),
            SourceManifestRow(id="", original_url="https://example.com/b", fetch_status="queued"),
        ]

        merged = self.service._merge_source_rows(rows)
        ids = [row.id for row in merged.rows]

        self.assertEqual(ids, ["000010", "000011"])
        self.assertEqual(merged.next_source_id, 12)

    def test_attach_scans_legacy_manifest_and_dedupes(self):
        repo_dir = self.tmp_path / "repo"
        repo_dir.mkdir(parents=True, exist_ok=True)
        manifest = repo_dir / "manifest.csv"

        with manifest.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "id",
                    "original_url",
                    "fetch_status",
                    "fetched_at",
                ],
            )
            writer.writeheader()
            writer.writerow(
                {
                    "id": "000001",
                    "original_url": "https://example.com/a?utm_source=one",
                    "fetch_status": "success",
                    "fetched_at": "2026-01-01T00:00:00+00:00",
                }
            )
            writer.writerow(
                {
                    "id": "000099",
                    "original_url": "https://example.com/a",
                    "fetch_status": "failed",
                    "fetched_at": "2026-01-01T00:00:00+00:00",
                }
            )

        status = self.service.attach(str(repo_dir))
        self.assertTrue(status.attached)
        self.assertEqual(status.total_sources, 1)
        self.assertEqual(status.next_source_id, 2)


if __name__ == "__main__":
    unittest.main()
