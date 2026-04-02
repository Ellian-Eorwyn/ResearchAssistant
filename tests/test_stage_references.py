from __future__ import annotations

import unittest

from backend.models.ingestion import IngestedDocument, TextBlock
from backend.pipeline.stage_references import detect_references_section


class StageReferencesTests(unittest.TestCase):
    def test_prefers_works_cited_and_stops_before_separator_and_later_heading(self):
        doc = IngestedDocument(
            filename="report.md",
            file_type="md",
            blocks=[
                TextBlock(text="Report", block_index=0, is_heading=True, heading_level=1),
                TextBlock(text="Body paragraph.", block_index=1),
                TextBlock(text="Works Cited", block_index=2, is_heading=True, heading_level=2),
                TextBlock(text='1. Agency. "Alpha Study." 2024. https://example.com/alpha', block_index=3),
                TextBlock(text='2. Agency. "Beta Study." 2025. https://example.com/beta', block_index=4),
                TextBlock(text="------------------------------------------------------------------------", block_index=5),
                TextBlock(text="References", block_index=6, is_heading=True, heading_level=2),
                TextBlock(text="Appendix references not part of works cited.", block_index=7),
            ],
            full_text="",
        )

        section = detect_references_section(doc)

        self.assertIsNotNone(section)
        assert section is not None
        self.assertEqual(section.heading_text, "Works Cited")
        self.assertEqual(section.start_block_index, 2)
        self.assertEqual(section.end_block_index, 4)
        self.assertIn("Alpha Study", section.raw_text)
        self.assertNotIn("Appendix references", section.raw_text)


if __name__ == "__main__":
    unittest.main()
