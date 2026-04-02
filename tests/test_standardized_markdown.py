from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from backend.models.bibliography import BibliographyEntry, ReferencesSection
from backend.models.ingestion import IngestedDocument, TextBlock
from backend.models.settings import LLMBackendConfig
from backend.pipeline.standardized_markdown import (
    normalize_document_to_standardized_markdown,
)


def _make_document(filename: str, blocks: list[TextBlock]) -> IngestedDocument:
    return IngestedDocument(
        filename=filename,
        file_type="md",
        blocks=blocks,
        full_text="\n\n".join(block.text for block in blocks),
    )


class StandardizedMarkdownTests(unittest.TestCase):
    def test_numeric_citations_render_sentence_end_numbers_and_doi_links(self):
        document = _make_document(
            "alpha.md",
            [
                TextBlock(text="Policy Memo", block_index=0, is_heading=True, heading_level=1),
                TextBlock(text="Alpha claim is important [1].", block_index=1),
                TextBlock(
                    text="References\n1. Agency. Alpha report. 2024. DOI: 10.1000/alpha",
                    block_index=2,
                ),
            ],
        )
        references_section = ReferencesSection(
            document_filename="alpha.md",
            start_block_index=2,
            end_block_index=2,
            heading_text="References",
            raw_text="1. Agency. Alpha report. 2024. DOI: 10.1000/alpha",
            detection_method="heading_match",
        )
        entries = [
            BibliographyEntry(
                ref_number=1,
                raw_text="Agency. Alpha report. 2024. DOI: 10.1000/alpha",
                source_document_name="alpha.md",
                authors=["Agency"],
                title="Alpha report",
                year="2024",
                doi="10.1000/alpha",
            )
        ]

        output = normalize_document_to_standardized_markdown(
            document=document,
            bibliography_entries=entries,
            references_section=references_section,
        )

        self.assertEqual(output.result.status, "normalized")
        self.assertIn("# Policy Memo", output.markdown_text)
        self.assertIn("Alpha claim is important. [1]", output.markdown_text)
        self.assertIn("## Works Cited", output.markdown_text)
        self.assertIn("[Source](https://doi.org/10.1000/alpha)", output.markdown_text)
        self.assertEqual(output.result.works_cited_linked_entries, 1)

    def test_author_year_citations_are_mapped_to_numeric_references(self):
        document = _make_document(
            "brief.md",
            [
                TextBlock(text="Agency findings improved outcomes (Smith, 2024).", block_index=0),
            ],
        )
        entries = [
            BibliographyEntry(
                ref_number=1,
                raw_text="Smith, Jane. Agency Findings. 2024. https://example.com/findings",
                source_document_name="brief.md",
                authors=["Smith, Jane"],
                title="Agency Findings",
                year="2024",
                url="https://example.com/findings",
            )
        ]

        output = normalize_document_to_standardized_markdown(
            document=document,
            bibliography_entries=entries,
            custom_profiles=[],
            profile_override="generic_author_year_academic",
        )

        self.assertIn("Agency findings improved outcomes. [1]", output.markdown_text)
        self.assertNotIn("(Smith, 2024)", output.markdown_text)
        self.assertEqual(output.result.matched_citation_markers, 1)

    def test_footnote_markers_are_normalized_to_sentence_end_numbers(self):
        document = _make_document(
            "report.md",
            [
                TextBlock(text="Important result.^1", block_index=0),
            ],
        )
        entries = [
            BibliographyEntry(
                ref_number=1,
                raw_text="Reporter. Important Result. 2025. https://example.com/result",
                source_document_name="report.md",
                authors=["Reporter"],
                title="Important Result",
                year="2025",
                url="https://example.com/result",
            )
        ]

        output = normalize_document_to_standardized_markdown(
            document=document,
            bibliography_entries=entries,
            custom_profiles=[],
            profile_override="footnote_endnote_report",
        )

        self.assertIn("Important result. [1]", output.markdown_text)
        self.assertEqual(output.result.matched_citation_markers, 1)

    @patch("backend.pipeline.standardized_markdown._run_llm_normalization")
    def test_low_confidence_documents_can_use_llm_fallback_and_emit_suggestions(self, mock_llm):
        mock_llm.return_value = {
            "blocks": [
                {
                    "kind": "paragraph",
                    "text": "Messy report cleaned.",
                    "citations": [1],
                }
            ],
            "works_cited": [
                {
                    "number": 1,
                    "text": "Agency. Cleaned report.",
                    "url": "https://example.com/cleaned-report",
                }
            ],
            "warnings": ["Fallback repaired unsupported formatting."],
            "unresolved_markers": [],
            "profile_suggestion": {
                "label": "Vendor report cleanup",
                "description": "Rules for vendor research report exports.",
                "reference_heading_patterns": ["^Sources$"],
                "citation_marker_patterns": [r"\[\d+\]"],
                "bibliography_split_patterns": [r"^\d+\."],
                "llm_guidance": "Repair numbered source blocks from vendor reports.",
            },
        }

        document = _make_document(
            "messy.md",
            [
                TextBlock(text="Messy vendor export with unsupported inline formatting", block_index=0),
            ],
        )

        output = normalize_document_to_standardized_markdown(
            document=document,
            bibliography_entries=[],
            use_llm=True,
            llm_backend=LLMBackendConfig(
                kind="ollama",
                base_url="http://localhost:11434",
                model="test-model",
            ),
            research_purpose="Normalize research report exports.",
        )

        self.assertTrue(output.result.used_llm_fallback)
        self.assertEqual(output.result.status, "normalized")
        self.assertIn("Messy report cleaned. [1]", output.markdown_text)
        self.assertIn("[Source](https://example.com/cleaned-report)", output.markdown_text)
        self.assertIsNotNone(output.suggestion)
        self.assertEqual(len(output.result.suggestion_ids), 1)

    @patch("backend.pipeline.standardized_markdown.UnifiedLLMClient")
    def test_llm_normalization_chunks_large_body_without_truncation_and_preserves_works_cited(
        self,
        mock_client_cls,
    ):
        mock_client = mock_client_cls.return_value
        mock_client.sync_chat_completion.side_effect = [
            json.dumps(
                {
                    "blocks": [
                        {
                            "kind": "paragraph",
                            "text": "Chunk one normalized.",
                            "citations": [],
                        }
                    ],
                    "works_cited": [],
                    "warnings": [],
                    "unresolved_markers": [],
                    "profile_suggestion": {},
                }
            ),
            json.dumps(
                {
                    "blocks": [
                        {
                            "kind": "paragraph",
                            "text": "Chunk two normalized.",
                            "citations": [],
                        }
                    ],
                    "works_cited": [],
                    "warnings": [],
                    "unresolved_markers": [],
                    "profile_suggestion": {},
                }
            ),
        ]

        long_paragraph = " ".join(["Long body content for chunking regression."] * 12)
        blocks = [
            TextBlock(text="Chunked Report", block_index=0, is_heading=True, heading_level=1),
            *[
                TextBlock(
                    text=f"{long_paragraph} Segment {index}.",
                    block_index=index,
                )
                for index in range(1, 6)
            ],
            TextBlock(text="Unmatched citation marker appears here [99].", block_index=6),
        ]
        document = _make_document("chunked.md", blocks)
        entries = [
            BibliographyEntry(
                ref_number=1,
                raw_text="Agency. Stable source. 2024. https://example.com/source",
                source_document_name="chunked.md",
                authors=["Agency"],
                title="Stable source",
                year="2024",
                url="https://example.com/source",
            )
        ]

        output = normalize_document_to_standardized_markdown(
            document=document,
            bibliography_entries=entries,
            use_llm=True,
            llm_backend=LLMBackendConfig(
                kind="ollama",
                base_url="http://localhost:11434",
                model="test-model",
                max_source_chars=4500,
            ),
        )

        self.assertTrue(output.result.used_llm_fallback)
        self.assertIn("Chunk one normalized.", output.markdown_text)
        self.assertIn("Chunk two normalized.", output.markdown_text)
        self.assertIn("## Works Cited", output.markdown_text)
        self.assertIn("[Source](https://example.com/source)", output.markdown_text)

        prompts = [
            call.kwargs["user_prompt"]
            for call in mock_client.sync_chat_completion.call_args_list
        ]
        self.assertGreaterEqual(len(prompts), 2)
        self.assertTrue(any("Document scope:\nbody chunk 1 of" in prompt for prompt in prompts))
        self.assertTrue(any("Document scope:\nbody chunk 2 of" in prompt for prompt in prompts))
        self.assertTrue(all("[... truncated ...]" not in prompt for prompt in prompts))
        mock_client.sync_close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
