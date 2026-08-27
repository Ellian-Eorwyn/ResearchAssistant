"""Tests for the ``resignal`` phase: regenerate fetch-time signals from disk.

Hermetic and network-free. The phase reads the raw HTML already saved on disk
and recomputes deterministic signals (today: ``date_signals``) in place, so a
repository fetched before a signal existed can be backfilled without a
re-download. Modeled on ``test_image_phase.py``.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from backend.models.settings import LLMBackendConfig
from backend.models.sources import SourceManifestRow, SourceOutputOptions
from backend.pipeline.source_downloader import SourceDownloadOrchestrator
from backend.storage.file_store import FileStore

SOURCE_ID = "000042"
PAGE_URL = "https://example.com/vpp"

# Raw HTML carrying an authoritative published date the fetch path would have
# captured, plus filler so byline/copyright heuristics have room to run.
RAW_HTML = (
    "<html><head><title>VPP explainer</title>"
    '<meta property="article:published_time" content="2023-04-13T10:00:00Z">'
    "</head><body><h1>Virtual Power Plants</h1>"
    "<p>A virtual power plant aggregates home batteries.</p>"
    + "filler " * 50
    + "</body></html>"
)

DUMMY_BACKEND = LLMBackendConfig(kind="openai", base_url="http://llms.local", model="chat")


class ResignalPhaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.out = Path(self._tmp.name)
        (self.out / "sources" / SOURCE_ID).mkdir(parents=True)
        (self.out / "sources" / SOURCE_ID / f"{SOURCE_ID}_source.html").write_text(
            RAW_HTML, encoding="utf-8"
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _row(self, *, detected_type: str = "html", raw: bool = True) -> SourceManifestRow:
        return SourceManifestRow(
            id=SOURCE_ID,
            detected_type=detected_type,
            fetch_status="success",
            raw_file=(f"sources/{SOURCE_ID}/{SOURCE_ID}_source.html" if raw else ""),
            final_url=PAGE_URL,
            title="VPP explainer",
            date_signals="",
        )

    def _orchestrator(self, **flags) -> SourceDownloadOrchestrator:
        return SourceDownloadOrchestrator(
            job_id="resignaltest",
            store=FileStore(self.out / ".jobs"),
            use_llm=False,
            llm_backend=DUMMY_BACKEND,
            output_options=SourceOutputOptions(),
            output_dir=self.out,
            writes_to_repository=True,
            **flags,
        )

    # -- tests ------------------------------------------------------------

    def test_regenerates_date_signals_from_saved_html(self):
        row = self._row()
        notes: list[str] = []
        self._orchestrator(run_download=False, run_resignal=True)._regenerate_fetch_signals(row, notes)

        self.assertIn("meta_published=2023-04-13", row.date_signals)
        self.assertEqual(row.phase_metadata["resignal"].status, "completed")
        self.assertTrue(any("resignal_regenerated" in n for n in notes))

    def test_noop_when_phase_not_requested(self):
        row = self._row()
        self._orchestrator(run_download=False, run_convert=True, run_resignal=False)._regenerate_fetch_signals(
            row, []
        )
        # Phase never ran: no signal written, no phase metadata recorded.
        self.assertEqual(row.date_signals, "")
        self.assertNotIn("resignal", row.phase_metadata)

    def test_is_idempotent(self):
        first = self._row()
        self._orchestrator(run_resignal=True)._regenerate_fetch_signals(first, [])
        signals_after_first = first.date_signals
        self.assertIn("meta_published=2023-04-13", signals_after_first)

        # A second pass regenerates the same value and stays completed. No note
        # is emitted the second time because nothing changed.
        second_notes: list[str] = []
        self._orchestrator(run_resignal=True)._regenerate_fetch_signals(first, second_notes)
        self.assertEqual(first.date_signals, signals_after_first)
        self.assertEqual(first.phase_metadata["resignal"].status, "completed")
        self.assertFalse(second_notes)

    def test_non_html_row_is_skipped_cleanly(self):
        row = self._row(detected_type="pdf")
        self._orchestrator(run_resignal=True)._regenerate_fetch_signals(row, [])
        self.assertEqual(row.date_signals, "")
        self.assertEqual(row.phase_metadata["resignal"].status, "skipped")

    def test_missing_raw_file_is_skipped_with_reason(self):
        row = self._row(raw=False)
        self._orchestrator(run_resignal=True)._regenerate_fetch_signals(row, [])
        self.assertEqual(row.phase_metadata["resignal"].status, "skipped")
        self.assertEqual(row.phase_metadata["resignal"].error_code, "resignal_missing_prerequisite")


if __name__ == "__main__":
    unittest.main()
