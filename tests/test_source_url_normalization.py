from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.pipeline.source_downloader import (
    RuntimeCapabilities,
    SourceDownloadOrchestrator,
    SourceTarget,
    dedupe_url_key,
    normalize_url,
)
from backend.pipeline.source_list_parser import parse_source_list_upload
from backend.storage.file_store import FileStore


class SourceUrlNormalizationTests(unittest.TestCase):
    def test_normalize_url_strips_trailing_wrappers(self):
        normalized, err = normalize_url("https://example.com/report.pdf`")
        self.assertEqual(err, "")
        self.assertEqual(normalized, "https://example.com/report.pdf")

    def test_dedupe_key_matches_encoded_and_plain_path(self):
        a = (
            "https://www.cal-smacna.org/wp-content/uploads/2025/12/"
            "CalNEXT-ET25SWE0024-smoutcault@ucdavis.edu_.pdf"
        )
        b = (
            "https://www.cal-smacna.org/wp-content/uploads/2025/12/"
            "CalNEXT-ET25SWE0024-smoutcault%40ucdavis.edu_.pdf"
        )
        self.assertEqual(dedupe_url_key(a), dedupe_url_key(b))

    def test_source_list_parser_cleans_urls_and_detects_duplicates(self):
        csv_text = (
            "URL\n"
            "https://www.cal-smacna.org/wp-content/uploads/2025/12/"
            "CalNEXT-ET25SWE0024-smoutcault@ucdavis.edu_.pdf`\n"
            "https://www.cal-smacna.org/wp-content/uploads/2025/12/"
            "CalNEXT-ET25SWE0024-smoutcault%40ucdavis.edu_.pdf\n"
        )
        parsed = parse_source_list_upload("sources.csv", csv_text.encode("utf-8"))
        self.assertEqual(parsed.accepted_rows, 2)
        self.assertEqual(parsed.estimated_duplicate_urls, 1)
        self.assertEqual(
            parsed.entries[0].url,
            "https://www.cal-smacna.org/wp-content/uploads/2025/12/"
            "CalNEXT-ET25SWE0024-smoutcault@ucdavis.edu_.pdf",
        )


class SourceDownloadCancellationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="source-cancel-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")

    def tearDown(self):
        self._tmp.cleanup()

    def test_cancel_requested_marks_status_cancelled(self):
        job_id = self.store.create_job()
        orchestrator = SourceDownloadOrchestrator(job_id=job_id, store=self.store)
        orchestrator.request_cancel()
        targets = [
            SourceTarget(
                id="000001",
                source_document_name="doc-a.md",
                citation_number="1",
                original_url="https://example.com/a",
            ),
            SourceTarget(
                id="000002",
                source_document_name="doc-a.md",
                citation_number="2",
                original_url="https://example.com/b",
            ),
        ]
        runtime_caps = RuntimeCapabilities(
            trafilatura_available=False,
            playwright_python_available=False,
            playwright_browser_available=False,
            textutil_available=False,
            tesseract_available=False,
            llm_vision_enabled=False,
            runtime_notes=[],
            runtime_guidance=[],
        )

        with (
            patch.object(orchestrator, "_build_targets", return_value=targets),
            patch.object(orchestrator, "_load_previous_rows", return_value=[]),
            patch(
                "backend.pipeline.source_downloader.detect_runtime_capabilities",
                return_value=runtime_caps,
            ),
        ):
            orchestrator.run()

        status = self.store.get_source_status(job_id)
        self.assertEqual(status["state"], "cancelled")
        self.assertEqual(status["total_urls"], 2)
        self.assertEqual(status["processed_urls"], 0)
        self.assertTrue(status["cancel_requested"])

    def test_request_cancel_marks_live_status_cancelling(self):
        job_id = self.store.create_job()
        orchestrator = SourceDownloadOrchestrator(job_id=job_id, store=self.store)
        targets = [
            SourceTarget(
                id="000001",
                source_document_name="doc-a.md",
                citation_number="1",
                original_url="https://example.com/a",
            ),
        ]
        runtime_caps = RuntimeCapabilities(
            trafilatura_available=False,
            playwright_python_available=False,
            playwright_browser_available=False,
            textutil_available=False,
            tesseract_available=False,
            llm_vision_enabled=False,
            runtime_notes=[],
            runtime_guidance=[],
        )

        orchestrator._initialize_status(
            targets=targets,
            runtime_capabilities=runtime_caps,
            existing_rows=[],
        )
        orchestrator._mark_item_running(targets[0])
        orchestrator.request_cancel()

        status = self.store.get_source_status(job_id)
        self.assertEqual(status["state"], "cancelling")
        self.assertTrue(status["cancel_requested"])
        self.assertTrue(status["stop_after_current_item"])
        self.assertIn("finishing current item before stopping", status["message"])


if __name__ == "__main__":
    unittest.main()
