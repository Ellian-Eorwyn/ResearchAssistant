"""Stage 7: Build the CSV export from matched citations."""

from __future__ import annotations

import csv
import io

from backend.models.bibliography import BibliographyEntry
from backend.models.citations import CitationMatch, CitingSentence, InTextCitation
from backend.models.export import EXPORT_COLUMNS, ExportArtifact, ExportRow


def build_export(
    matches: list[CitationMatch],
    citations: list[InTextCitation],
    sentences: list[CitingSentence],
    bib_entries: list[BibliographyEntry],
    research_purpose: str = "",
) -> ExportArtifact:
    """Assemble export rows from matched citations."""
    # Build lookups
    cit_by_id: dict[str, InTextCitation] = {c.citation_id: c for c in citations}
    sent_by_id: dict[str, CitingSentence] = {s.sentence_id: s for s in sentences}

    rows: list[ExportRow] = []
    matched_count = 0
    unmatched_count = 0

    for match in matches:
        cit = cit_by_id.get(match.citation_id)
        sent = sent_by_id.get(match.sentence_id)
        bib: BibliographyEntry | None = None
        if match.matched_bib_entry_index is not None and match.matched_bib_entry_index < len(
            bib_entries
        ):
            bib = bib_entries[match.matched_bib_entry_index]
            matched_count += 1
        else:
            unmatched_count += 1

        warnings_parts: list[str] = []
        if match.match_confidence < 1.0:
            warnings_parts.append(f"Low match confidence: {match.match_confidence}")
        if match.unmatched_reason:
            warnings_parts.append(match.unmatched_reason)
        if bib and bib.parse_confidence < 0.5:
            warnings_parts.append(f"Low parse confidence: {bib.parse_confidence}")
        if bib and bib.parse_warnings:
            warnings_parts.extend(bib.parse_warnings)

        row = ExportRow(
            source_document=cit.document_filename if cit else "",
            page_in_source=str(cit.page_number or "") if cit else "",
            citing_sentence="",
            citing_paragraph=sent.paragraph if sent else "",
            context_before="",
            context_after="",
            citation_raw=cit.raw_marker if cit else "",
            citation_ref_numbers=str(match.ref_number),
            cited_authors="; ".join(bib.authors) if bib else "",
            cited_title=bib.title if bib else "",
            cited_year=bib.year if bib else "",
            cited_source=bib.journal_or_source if bib else "",
            cited_volume=bib.volume if bib else "",
            cited_issue=bib.issue if bib else "",
            cited_pages=bib.pages if bib else "",
            cited_doi=bib.doi if bib else "",
            cited_url=bib.url if bib else "",
            cited_raw_entry=bib.raw_text if bib else "",
            match_confidence=match.match_confidence,
            match_method=match.match_method,
            warnings="; ".join(warnings_parts),
            research_purpose=research_purpose,
        )
        rows.append(row)

    return ExportArtifact(
        rows=rows,
        total_citations_found=len(citations),
        total_bib_entries=len(bib_entries),
        matched_count=matched_count,
        unmatched_count=unmatched_count,
    )


def write_csv(artifact: ExportArtifact) -> str:
    """Write the export artifact to CSV string (UTF-8 with BOM for Excel)."""
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=EXPORT_COLUMNS)
    writer.writeheader()

    for row in artifact.rows:
        payload = row.model_dump(mode="json")
        writer.writerow({column: payload.get(column, "") for column in EXPORT_COLUMNS})

    return output.getvalue()
