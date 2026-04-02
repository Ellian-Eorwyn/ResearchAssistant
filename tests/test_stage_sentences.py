from __future__ import annotations

import unittest

from backend.models.citations import InTextCitation
from backend.models.ingestion import IngestedDocument, TextBlock
from backend.pipeline.stage_sentences import extract_citing_sentences


class CitationContextTests(unittest.TestCase):
    def test_extracts_paragraph_context_from_block(self):
        doc = IngestedDocument(
            filename="alpha.md",
            file_type="md",
            blocks=[
                TextBlock(
                    text="Opening paragraph without citations.",
                    block_index=0,
                    char_offset_start=0,
                    char_offset_end=34,
                ),
                TextBlock(
                    text="Important claim supported by prior work [1, 2].",
                    block_index=1,
                    char_offset_start=35,
                    char_offset_end=82,
                ),
            ],
            full_text="Opening paragraph without citations.\nImportant claim supported by prior work [1, 2].",
        )
        citations = [
            InTextCitation(
                citation_id="alpha.md_cit_1",
                document_filename="alpha.md",
                raw_marker="[1, 2]",
                ref_numbers=[1, 2],
                block_index=1,
                char_offset_start=76,
                char_offset_end=82,
            )
        ]

        result = extract_citing_sentences(doc, citations)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].text, "")
        self.assertEqual(
            result[0].paragraph,
            "Important claim supported by prior work [1, 2].",
        )
        self.assertEqual(result[0].citation_ids, ["alpha.md_cit_1"])


if __name__ == "__main__":
    unittest.main()
