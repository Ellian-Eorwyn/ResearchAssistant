from __future__ import annotations

import json
import unittest
from unittest.mock import patch

from backend.models.bibliography import ReferencesSection
from backend.models.settings import LLMBackendConfig
from backend.pipeline.stage_bibliography import parse_bibliography


class StageBibliographyTests(unittest.TestCase):
    def test_parses_simple_author_title_year_entry(self):
        artifact = parse_bibliography(
            [
                ReferencesSection(
                    document_filename="alpha.md",
                    start_block_index=0,
                    end_block_index=0,
                    heading_text="References",
                    raw_text="[1] Smith, Jane. Agency Findings. 2024. https://example.com/findings\n\n[2] Example Org. Annual Report. 2023. https://example.com/report",
                    detection_method="heading_match",
                )
            ]
        )

        self.assertEqual(len(artifact.entries), 2)
        self.assertEqual(artifact.entries[0].authors, ["Smith, Jane"])
        self.assertEqual(artifact.entries[0].title, "Agency Findings")
        self.assertEqual(artifact.entries[0].year, "2024")

    @patch("backend.pipeline.stage_bibliography.UnifiedLLMClient")
    def test_llm_repair_can_correct_bibliography_fields(self, mock_client_cls):
        mock_client = mock_client_cls.return_value
        mock_client.sync_chat_completion.return_value = json.dumps(
            {
                "authors": ["S. Finnegan", "C. Jones", "S. Sharples"],
                "title": "The embodied CO2e of sustainable energy technologies used in buildings: a review article",
                "year": "2018",
                "journal_or_source": "Energy Build.",
                "volume": "181",
                "issue": "",
                "pages": "50-61",
                "doi": "10.1016/j.enbuild.2018.09.037",
                "url": "",
            }
        )

        artifact = parse_bibliography(
            [
                ReferencesSection(
                    document_filename="alpha.md",
                    start_block_index=0,
                    end_block_index=0,
                    heading_text="References",
                    raw_text="[59] S. Finnegan, C. Jones, S. Sharples, The embodied CO2e of sustainable energy technologies used in buildings: a review article, Energy Build. 181 (2018) 50-61. https://doi.org/10.1016/j.enbuild.2018.09.037.",
                    detection_method="heading_match",
                )
            ],
            use_llm=True,
            llm_backend=LLMBackendConfig(
                kind="ollama",
                base_url="http://localhost:11434",
                model="test-model",
            ),
        )

        self.assertEqual(len(artifact.entries), 1)
        entry = artifact.entries[0]
        self.assertEqual(entry.authors, ["S. Finnegan", "C. Jones", "S. Sharples"])
        self.assertEqual(
            entry.title,
            "The embodied CO2e of sustainable energy technologies used in buildings: a review article",
        )
        self.assertEqual(entry.repair_method, "llm_bibliography_repair")
        mock_client.sync_close.assert_called_once()

    def test_parses_title_first_entries_with_org_authors_and_year_in_title(self):
        artifact = parse_bibliography(
            [
                ReferencesSection(
                    document_filename="alpha.md",
                    start_block_index=0,
                    end_block_index=0,
                    heading_text="Works Cited",
                    raw_text=(
                        "[5] 2025 Building Energy Efficiency Standards (webpage). "
                        "California Energy Commission, 2025 "
                        "(effective for permits on/after 2026-01-01). "
                        "Link: `https://example.com/standards`\n"
                        "[6] Single-family Buildings: What's New in 2025 (fact sheet). "
                        "Energy Code Ace, 2025. "
                        "Link: `https://example.com/fact-sheet`"
                    ),
                    detection_method="heading_match",
                )
            ]
        )

        self.assertEqual(len(artifact.entries), 2)

        first = artifact.entries[0]
        self.assertEqual(first.ref_number, 5)
        self.assertEqual(first.authors, ["California Energy Commission"])
        self.assertEqual(first.title, "2025 Building Energy Efficiency Standards (webpage)")
        self.assertEqual(first.year, "2025")
        self.assertEqual(first.url, "https://example.com/standards")

        second = artifact.entries[1]
        self.assertEqual(second.ref_number, 6)
        self.assertEqual(second.authors, ["Energy Code Ace"])
        self.assertEqual(second.title, "Single-family Buildings: What's New in 2025 (fact sheet)")
        self.assertEqual(second.year, "2025")
        self.assertEqual(second.url, "https://example.com/fact-sheet")

    def test_line_entries_capture_trailing_footnote_numbers_and_clean_urls(self):
        artifact = parse_bibliography(
            [
                ReferencesSection(
                    document_filename="alpha.md",
                    start_block_index=0,
                    end_block_index=0,
                    heading_text="Works Cited",
                    raw_text=(
                        "Works cited are provided as inline citations throughout this document. Key sources include:\n"
                        "UC Davis Western Cooling Efficiency Center, "
                        "\"Multifunction Heat Pump Lab Test - Variable Speed,\" "
                        "CalNEXT Project ET23SWE0066, 2025. "
                        "https://ca-etp.com/node/13519[^61]\n"
                        "Association for Energy Affordability, RMI, Emanant Systems, LBNL, and SmithGroup, "
                        "\"Low-GWP Mechanical Modules for Rapid Deployment Project,\" "
                        "California Energy Commission Publication CEC-500-2025-029, June 2025. "
                        "https://www.energy.ca.gov/sites/default/files/2025-06/CEC-500-2025-029.pdf[^62]"
                    ),
                    detection_method="heading_match",
                )
            ]
        )

        self.assertEqual(len(artifact.entries), 2)
        self.assertEqual(artifact.entries[0].ref_number, 61)
        self.assertEqual(artifact.entries[0].authors, ["UC Davis Western Cooling Efficiency Center"])
        self.assertEqual(
            artifact.entries[0].url,
            "https://ca-etp.com/node/13519",
        )
        self.assertEqual(artifact.entries[1].ref_number, 62)
        self.assertEqual(
            artifact.entries[1].url,
            "https://www.energy.ca.gov/sites/default/files/2025-06/CEC-500-2025-029.pdf",
        )

    def test_parses_leading_given_family_author_before_org(self):
        artifact = parse_bibliography(
            [
                ReferencesSection(
                    document_filename="alpha.md",
                    start_block_index=0,
                    end_block_index=0,
                    heading_text="Works Cited",
                    raw_text=(
                        'David Vernon, UC Davis, "Residential Multi-Function Heat Pumps: '
                        'Product Search Final Report," CalNEXT ET22SWE0021, December 2022. '
                        "https://example.com/product-search[^65]"
                    ),
                    detection_method="heading_match",
                )
            ]
        )

        self.assertEqual(len(artifact.entries), 1)
        entry = artifact.entries[0]
        self.assertEqual(entry.authors, ["David Vernon"])
        self.assertEqual(entry.title, "Residential Multi-Function Heat Pumps: Product Search Final Report")
        self.assertEqual(entry.year, "2022")
        self.assertEqual(entry.url, "https://example.com/product-search")


if __name__ == "__main__":
    unittest.main()
