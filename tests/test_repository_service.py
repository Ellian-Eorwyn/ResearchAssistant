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

    def test_create_export_job_scope_all_creates_job_bibliography(self):
        repo_dir = self.tmp_path / "repo_all"
        repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(repo_dir))
        self.service.import_source_list(
            filename="sources.csv",
            content=(
                "URL\n"
                "https://example.com/a\n"
                "https://example.com/b\n"
            ).encode("utf-8"),
        )

        result = self.service.create_export_job(scope="all")
        self.assertEqual(result.scope, "all")
        self.assertEqual(result.total_urls, 2)
        self.assertTrue(result.job_id)

        bib = self.store.load_artifact(result.job_id, "03_bibliography")
        self.assertIsNotNone(bib)
        urls = [entry.get("url") for entry in bib.get("entries", [])]
        self.assertEqual(urls, ["https://example.com/a", "https://example.com/b"])

    def test_create_export_job_scope_import_selects_only_that_import(self):
        repo_dir = self.tmp_path / "repo_import"
        repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(repo_dir))

        first = self.service.import_source_list(
            filename="sources.csv",
            content=("URL\nhttps://example.com/a\n").encode("utf-8"),
        )
        second = self.service.import_source_list(
            filename="sources2.csv",
            content=("URL\nhttps://example.com/b\n").encode("utf-8"),
        )
        self.assertNotEqual(first.import_id, second.import_id)

        result = self.service.create_export_job(scope="import", import_id=second.import_id)
        self.assertEqual(result.scope, "import")
        self.assertEqual(result.import_id, second.import_id)
        self.assertEqual(result.total_urls, 1)

        bib = self.store.load_artifact(result.job_id, "03_bibliography")
        self.assertIsNotNone(bib)
        urls = [entry.get("url") for entry in bib.get("entries", [])]
        self.assertEqual(urls, ["https://example.com/b"])

    def test_create_export_job_scope_import_rejects_unknown_import_id(self):
        repo_dir = self.tmp_path / "repo_unknown_import"
        repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(repo_dir))
        self.service.import_source_list(
            filename="sources.csv",
            content=("URL\nhttps://example.com/a\n").encode("utf-8"),
        )

        with self.assertRaises(ValueError):
            self.service.create_export_job(scope="import", import_id="does-not-exist")

    def test_create_export_job_scope_import_empty_selection_raises_runtime_error(self):
        repo_dir = self.tmp_path / "repo_empty_import"
        repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(repo_dir))
        self.service.import_source_list(
            filename="sources.csv",
            content=("URL\nhttps://example.com/a\n").encode("utf-8"),
        )
        duplicate_import = self.service.import_source_list(
            filename="sources_dup.csv",
            content=("URL\nhttps://example.com/a\n").encode("utf-8"),
        )
        self.assertEqual(duplicate_import.accepted_new, 0)

        artifacts_dir = self.store.artifacts_dir
        before = len(list(artifacts_dir.iterdir())) if artifacts_dir.exists() else 0
        with self.assertRaises(RuntimeError):
            self.service.create_export_job(scope="import", import_id=duplicate_import.import_id)
        after = len(list(artifacts_dir.iterdir())) if artifacts_dir.exists() else 0
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
