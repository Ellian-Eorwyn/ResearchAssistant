"""Backend support for the operator CLI commands (set-cell, set-fetch-status)
and the stale-cleanup fix behind full-run.

Reuses the service-backed harness from test_repo_operations.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from backend.models.sources import SourceManifestRow, SourcePhaseMetadata
from backend.pipeline.source_downloader import PHASE_CLEANUP, effective_markdown_rel_path

from test_repo_operations import _OperationsTestCase


class SetColumnValuesConstraintTests(_OperationsTestCase):
    def _seed_enum_column(self):
        self.commit([self.seed_source("000003", "https://example.com/a")])
        column = self.service.create_column("Sector")
        self.service.update_column(
            column.id,
            patch={
                "instruction_prompt": "Classify the sector.",
                "output_constraint": {
                    "allowed_values": ["residential", "commercial", "not sure"],
                    "fallback_value": "not sure",
                },
            },
        )
        return column.id

    def test_value_outside_allowed_is_warned(self) -> None:
        column_id = self._seed_enum_column()
        plan = self.plan(
            "set_column_values",
            {"column_id": column_id, "values": {"000003": "industrial"}, "overwrite": True},
        )
        codes = [w.code for w in plan.warnings]
        self.assertIn("value_not_in_allowed", codes)
        self.assertEqual(plan.blockers, [])  # a warning, not a hard block

    def test_allowed_value_is_not_warned(self) -> None:
        column_id = self._seed_enum_column()
        plan = self.plan(
            "set_column_values",
            {"column_id": column_id, "values": {"000003": "residential"}, "overwrite": True},
        )
        self.assertNotIn("value_not_in_allowed", [w.code for w in plan.warnings])


class UpdateSourceFetchFieldsTests(_OperationsTestCase):
    def test_force_status_and_reason_then_clear(self) -> None:
        self.commit([self.seed_source("000003", "https://example.com/a", fetch_status="success")])

        self.service.update_source(
            "000003", patch={"fetch_status": "partial", "error_message": "PDF added; re-run"}
        )
        row = self.rows_by_id()["000003"]
        self.assertEqual(row["fetch_status"], "partial")
        self.assertEqual(row["error_message"], "PDF added; re-run")

        # Marking it resolved and clearing the stale reason, all through the API.
        self.service.update_source("000003", patch={"fetch_status": "success", "error_message": ""})
        row = self.rows_by_id()["000003"]
        self.assertEqual(row["fetch_status"], "success")
        self.assertEqual(row["error_message"], "")

    def test_invalid_status_is_rejected(self) -> None:
        self.commit([self.seed_source("000003", "https://example.com/a")])
        with self.assertRaises(ValueError):
            self.service.update_source("000003", patch={"fetch_status": "nonsense"})


class EffectiveMarkdownStalenessTests(unittest.TestCase):
    """The contract the column-run source-text fix relies on: a stale cleaned
    copy (left by a re-attach/reconvert) must not shadow the fresh markdown."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.base = Path(self._tmp.name)
        d = self.base / "sources" / "000009"
        d.mkdir(parents=True)
        (d / "000009_clean.md").write_text("fresh PDF text", encoding="utf-8")
        (d / "000009_llm_clean.md").write_text("stale thin text", encoding="utf-8")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _row(self) -> SourceManifestRow:
        return SourceManifestRow(
            id="000009",
            detected_type="pdf",
            markdown_file="sources/000009/000009_clean.md",
            llm_cleanup_file="sources/000009/000009_llm_clean.md",
        )

    def test_stale_cleanup_falls_back_to_markdown(self) -> None:
        row = self._row()
        row.phase_metadata[PHASE_CLEANUP] = SourcePhaseMetadata(
            phase=PHASE_CLEANUP, status="stale", stale=True
        )
        self.assertEqual(
            effective_markdown_rel_path(row, self.base), "sources/000009/000009_clean.md"
        )

    def test_current_cleanup_is_preferred(self) -> None:
        row = self._row()
        row.phase_metadata[PHASE_CLEANUP] = SourcePhaseMetadata(
            phase=PHASE_CLEANUP, status="cleaned", stale=False
        )
        self.assertEqual(
            effective_markdown_rel_path(row, self.base), "sources/000009/000009_llm_clean.md"
        )


if __name__ == "__main__":
    unittest.main()
