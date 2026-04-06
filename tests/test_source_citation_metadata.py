from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from backend.models.citation_metadata import CitationMetadata
from backend.models.settings import LLMBackendConfig
from backend.models.sources import SourceManifestRow
from backend.pipeline.source_downloader import (
    _build_citation_metadata,
    _build_deterministic_catalog_metadata,
    _finalize_citation_metadata,
    SourceDownloadOrchestrator,
    build_ris_record,
    extract_html_citation_metadata,
    normalize_citation_authors,
)
from backend.storage.file_store import FileStore


class SourceCitationMetadataTests(unittest.TestCase):
    def test_extracts_html_citation_metadata_from_citation_meta_tags(self):
        html = """
        <html>
          <head>
            <meta name="citation_title" content="Heat Pump Performance Study" />
            <meta name="citation_author" content="Doe, Jane" />
            <meta name="citation_author" content="Roe, John" />
            <meta name="citation_publication_date" content="2024-03-15" />
            <meta name="citation_doi" content="10.1234/example" />
            <meta name="citation_journal_title" content="Journal of Efficient Buildings" />
            <meta name="citation_volume" content="12" />
            <meta name="citation_issue" content="4" />
            <meta name="citation_firstpage" content="50" />
            <meta name="citation_lastpage" content="61" />
            <meta name="citation_public_url" content="https://example.com/article" />
          </head>
          <body></body>
        </html>
        """

        metadata = extract_html_citation_metadata(html, base_url="https://example.com/article")

        self.assertEqual(metadata["title"], "Heat Pump Performance Study")
        self.assertEqual(metadata["authors"], ["Doe, Jane", "Roe, John"])
        self.assertEqual(metadata["issued"], "2024-03-15")
        self.assertEqual(metadata["doi"], "10.1234/example")
        self.assertEqual(metadata["container_title"], "Journal of Efficient Buildings")
        self.assertEqual(metadata["volume"], "12")
        self.assertEqual(metadata["issue"], "4")
        self.assertEqual(metadata["pages"], "50-61")
        self.assertEqual(metadata["url"], "https://example.com/article")

    def test_extracts_html_citation_metadata_from_json_ld(self):
        html = """
        <html>
          <head>
            <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "ScholarlyArticle",
                "headline": "Grid Flexibility Findings",
                "author": [
                  {"@type": "Person", "givenName": "Ava", "familyName": "Smith"}
                ],
                "datePublished": "2025-01-07",
                "publisher": {"@type": "Organization", "name": "Gamma University"},
                "isPartOf": {"@type": "Periodical", "name": "Journal of Buildings"},
                "identifier": "https://doi.org/10.5555/example",
                "url": "https://example.com/journal-article",
                "volumeNumber": "7",
                "issueNumber": "2",
                "pageStart": "101",
                "pageEnd": "118",
                "inLanguage": "en"
              }
            </script>
          </head>
          <body></body>
        </html>
        """

        metadata = extract_html_citation_metadata(html, base_url="https://example.com/journal-article")

        self.assertEqual(metadata["title"], "Grid Flexibility Findings")
        self.assertEqual(metadata["authors"], ["Smith, Ava"])
        self.assertEqual(metadata["issued"], "2025-01-07")
        self.assertEqual(metadata["publisher"], "Gamma University")
        self.assertEqual(metadata["container_title"], "Journal of Buildings")
        self.assertEqual(metadata["doi"], "10.5555/example")
        self.assertEqual(metadata["pages"], "101-118")
        self.assertEqual(metadata["language"], "en")

    def test_builds_deterministic_catalog_metadata_from_markdown_front_matter(self):
        row = SourceManifestRow(
            id="000001",
            source_kind="uploaded_document",
            original_url="https://example.com/report",
            detected_type="document",
        )
        markdown = """---
title: Alpha Agency Findings
authors:
  - Jane Doe
  - John Roe
date: 2024-05-10
publisher: Alpha Agency
---

# Alpha Agency Findings

By Jane Doe and John Roe
"""

        payload = _build_deterministic_catalog_metadata(row=row, markdown_text=markdown)
        citation = payload["citation"]

        self.assertEqual(payload["title"], "Alpha Agency Findings")
        self.assertEqual(payload["author_names"], "Jane Doe; John Roe")
        self.assertEqual(payload["publication_date"], "2024-05-10")
        self.assertEqual(payload["organization_name"], "Alpha Agency")
        self.assertEqual(citation["title"], "Alpha Agency Findings")
        self.assertEqual(citation["issued"], "2024-05-10")
        self.assertEqual(citation["authors"][0]["family"], "Doe")
        self.assertEqual(citation["authors"][0]["given"], "Jane")

    def test_builds_citation_metadata_from_doi_only_url(self):
        row = SourceManifestRow(
            id="000001",
            source_kind="url",
            original_url="https://doi.org/10.1234/example",
        )

        citation = _build_citation_metadata(
            row=row,
            title="Sample Report",
            author_names="Jane Doe",
            publication_date="2024",
            document_type="report",
            organization_name="Example Org",
            html_metadata={},
        )

        self.assertEqual(citation.doi, "10.1234/example")
        self.assertEqual(citation.url, "https://doi.org/10.1234/example")
        self.assertFalse(citation.ready_for_ris)
        self.assertNotEqual(citation.verification_status, "verified")

    def test_normalizes_personal_and_corporate_authors(self):
        authors = normalize_citation_authors(
            ["Jane Doe", "Roe, John", "California Energy Commission", "Madonna"]
        )

        self.assertEqual(authors[0].family, "Doe")
        self.assertEqual(authors[0].given, "Jane")
        self.assertEqual(authors[1].family, "Roe")
        self.assertEqual(authors[1].given, "John")
        self.assertEqual(authors[2].literal, "California Energy Commission")
        self.assertEqual(authors[3].literal, "Madonna")

    def test_builds_ris_record_for_standard(self):
        citation = CitationMetadata(
            item_type="standard",
            title="High-Performance Building Standard",
            authors=normalize_citation_authors(["ASHRAE"]),
            issued="2025-01-07",
            publisher="ASHRAE",
            url="https://example.com/standard",
            standard_number="ASHRAE 90.1",
            ready_for_ris=True,
        )

        record = build_ris_record(citation)

        self.assertIn("TY  - RPRT", record)
        self.assertIn("AU  - ASHRAE", record)
        self.assertIn("TI  - High-Performance Building Standard", record)
        self.assertIn("PY  - 2025", record)
        self.assertIn("DA  - 2025/01/07", record)
        self.assertIn("UR  - https://example.com/standard", record)
        self.assertIn("M3  - Standard", record)
        self.assertIn("VO  - ASHRAE 90.1", record)
        self.assertTrue(record.endswith("ER  -"))

    def test_ris_readiness_requires_only_title_authors_publication_year_and_url(self):
        citation = CitationMetadata(
            item_type="",
            title="Minimal Citation",
            authors=normalize_citation_authors(["Jane Doe"]),
            issued="2024",
            url="https://example.com/minimal",
            verification_status="verified",
        )

        finalized = _finalize_citation_metadata(citation)

        self.assertTrue(finalized.ready_for_ris)
        self.assertEqual(finalized.missing_fields, [])
        self.assertEqual(finalized.blocked_reasons, [])
        self.assertEqual(finalized.verification_status, "verified")

    def test_finalized_citation_uses_organization_as_corporate_author_when_authors_missing(self):
        citation = CitationMetadata(
            item_type="report",
            title="Statewide Grid Planning Update",
            issued="2025",
            publisher="California Energy Commission",
            url="https://example.com/grid-update",
            verification_status="verified",
        )

        finalized = _finalize_citation_metadata(citation)

        self.assertTrue(finalized.ready_for_ris)
        self.assertEqual(len(finalized.authors), 1)
        self.assertEqual(finalized.authors[0].literal, "California Energy Commission")
        self.assertIn("organization_author_fallback", finalized.notes)
        self.assertEqual(finalized.field_evidence["authors"].source_type, "publisher_fallback")
        self.assertEqual(finalized.field_evidence["authors"].value, "California Energy Commission")

    def test_catalog_llm_uses_pdf_markdown_for_direct_pdf_downloads(self):
        with TemporaryDirectory(prefix="source-citation-pdf-source-") as tmp_dir:
            tmp_path = Path(tmp_dir)
            store = FileStore(base_dir=tmp_path / "app_data")
            job_id = store.create_job()
            output_dir = store.get_sources_output_dir(job_id)

            pdf_rel = Path("originals") / "000001_source.pdf"
            pdf_abs = output_dir / pdf_rel
            pdf_abs.parent.mkdir(parents=True, exist_ok=True)
            pdf_abs.write_bytes(b"%PDF-1.4 test pdf")

            rendered_html_rel = Path("rendered") / "000001_rendered.html"
            rendered_html_abs = output_dir / rendered_html_rel
            rendered_html_abs.parent.mkdir(parents=True, exist_ok=True)
            rendered_html_abs.write_text(
                "<html><body><h1>Rendered HTML Title</h1><p>HTML body.</p></body></html>",
                encoding="utf-8",
            )

            row = SourceManifestRow(
                id="000001",
                original_url="https://example.com/report.pdf",
                final_url="https://example.com/report.pdf",
                detected_type="pdf",
                raw_file=pdf_rel.as_posix(),
                rendered_file=rendered_html_rel.as_posix(),
            )
            orchestrator = SourceDownloadOrchestrator(
                job_id=job_id,
                store=store,
                use_llm=True,
                llm_backend=LLMBackendConfig(model="test-model"),
                run_download=False,
                run_convert=True,
                run_catalog=False,
                run_llm_cleanup=False,
                run_llm_title=False,
                run_llm_summary=False,
                run_llm_rating=False,
                target_rows=[row],
            )

            with patch.object(
                orchestrator,
                "_convert_pdf_to_markdown",
                return_value=("# PDF-native title\n\nPDF body.\n", "pdf_text", []),
            ) as convert_pdf:
                orchestrator._generate_markdown_from_existing_artifacts(row, [])

            self.assertTrue(convert_pdf.called)
            self.assertEqual(row.markdown_file, "markdown/000001_clean.md")
            markdown_text = (output_dir / row.markdown_file).read_text(encoding="utf-8")
            self.assertIn("PDF-native title", markdown_text)
            self.assertNotIn("Rendered HTML Title", markdown_text)


if __name__ == "__main__":
    unittest.main()
