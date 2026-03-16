from __future__ import annotations

import unittest

from backend.models.sources import SourceManifestRow
from backend.pipeline.source_downloader import parse_cleanup_response, summarize_output_rows


class SourceOutputSummaryTests(unittest.TestCase):
    def test_summary_counts_outputs_and_missing_summaries(self):
        rows = [
            SourceManifestRow(
                id="000001",
                original_url="https://example.com/a",
                fetch_status="success",
                raw_file="originals/000001_source.pdf",
                markdown_file="markdown/000001_clean.md",
                summary_file="summaries/000001_summary.md",
                summary_status="generated",
            ),
            SourceManifestRow(
                id="000002",
                original_url="https://example.com/b",
                fetch_status="success",
                rendered_pdf_file="rendered/000002_rendered.pdf",
                markdown_file="markdown/000002_clean.md",
                llm_cleanup_needed=True,
                llm_cleanup_status="failed",
                summary_status="failed",
            ),
        ]

        summary = summarize_output_rows(rows)

        self.assertEqual(summary.total_rows, 2)
        self.assertEqual(summary.raw_file_count, 1)
        self.assertEqual(summary.rendered_pdf_count, 1)
        self.assertEqual(summary.markdown_count, 2)
        self.assertEqual(summary.summary_file_count, 1)
        self.assertEqual(summary.summary_missing_count, 1)
        self.assertEqual(summary.summary_failed_count, 1)
        self.assertEqual(summary.llm_cleanup_needed_count, 1)
        self.assertEqual(summary.llm_cleanup_failed_count, 1)

    def test_parse_cleanup_response(self):
        needs_cleanup, cleaned_markdown = parse_cleanup_response(
            "NEEDS_CLEANUP: yes\nCLEANED_MARKDOWN:\n# Title\nParagraph"
        )
        self.assertTrue(needs_cleanup)
        self.assertIn("Paragraph", cleaned_markdown)

        needs_cleanup, cleaned_markdown = parse_cleanup_response(
            "NEEDS_CLEANUP: no\nCLEANED_MARKDOWN:\n"
        )
        self.assertFalse(needs_cleanup)
        self.assertEqual(cleaned_markdown, "")


if __name__ == "__main__":
    unittest.main()
