"""Blocked fetches must be visible to `ra where`.

A source that stored a bot wall instead of the document needs the user to act,
exactly as a failed one does. Counting only `failed` made a repository whose
only problem was blocked fetches look finished, and kept `ra triage` out of the
suggested next actions.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from backend.models.sources import SourceManifestRow, SourcePhaseMetadata
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore
from backend.workflow.orientation import orientation
from backend.workflow.triage import triage_failures


class OrientationBlockedFetchTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="orientation-blocked-")
        root = Path(self._tmp.name)
        self.store = FileStore(base_dir=root / "data", sync_project_profiles=False)
        self.service = AttachedRepositoryService(store=self.store)
        self.service.create(str(root / "repo"))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _commit(self, rows: list[SourceManifestRow]) -> None:
        with self.service._writer_lock():
            self.service._save_state_locked(sources=rows, citations=[], imports=[])
            self.service._rebuild_outputs_locked(rows, [])

    def _blocked_row(self, source_id: str = "000001") -> SourceManifestRow:
        row = SourceManifestRow(
            id=source_id,
            original_url=f"https://example.com/{source_id}",
            fetch_status="blocked",
            fetch_verification="blocked_challenge",
            error_message="blocked_challenge: Challenge or interstitial page detected",
        )
        row.phase_metadata["fetch"] = SourcePhaseMetadata(
            phase="fetch",
            status="failed",
            error="blocked_challenge: Challenge or interstitial page detected",
            error_code="blocked_challenge",
        )
        return row

    def test_a_blocked_row_is_counted_as_a_failure(self) -> None:
        self._commit([self._blocked_row()])
        report = orientation(self.service, include_column_stats=False)
        self.assertEqual(report.failures_by_code, {"blocked_challenge": 1})
        self.assertEqual(report.failure_examples, {"blocked_challenge": ["000001"]})

    def test_the_summary_names_blocked_sources(self) -> None:
        self._commit([self._blocked_row()])
        report = orientation(self.service, include_column_stats=False)
        self.assertIn("1 blocked", report.summary)

    def test_a_successful_row_is_not_counted(self) -> None:
        good = SourceManifestRow(
            id="000002",
            original_url="https://example.com/good",
            fetch_status="success",
        )
        self._commit([self._blocked_row(), good])
        report = orientation(self.service, include_column_stats=False)
        self.assertEqual(sum(report.failures_by_code.values()), 1)

    def test_triage_groups_a_blocked_row_with_a_hand_collection_remedy(self) -> None:
        self._commit([self._blocked_row()])
        report = triage_failures(self.service, phase="fetch")
        self.assertEqual(report.total_failed, 1)
        group = report.groups[0]
        self.assertEqual(group.error_code, "blocked_challenge")
        self.assertEqual(group.classification, "needs_manual_document")


if __name__ == "__main__":
    unittest.main()
