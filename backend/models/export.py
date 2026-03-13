"""Models for the CSV export stage."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ExportRow(BaseModel):
    repository_source_id: str = ""
    import_type: str = ""
    imported_at: str = ""
    provenance_ref: str = ""
    source_document: str = ""
    page_in_source: str = ""
    citing_sentence: str = ""
    citing_paragraph: str = ""
    context_before: str = ""
    context_after: str = ""
    citation_raw: str = ""
    citation_ref_numbers: str = ""
    cited_authors: str = ""
    cited_title: str = ""
    cited_year: str = ""
    cited_source: str = ""
    cited_volume: str = ""
    cited_issue: str = ""
    cited_pages: str = ""
    cited_doi: str = ""
    cited_url: str = ""
    cited_raw_entry: str = ""
    match_confidence: float = 0.0
    match_method: str = ""
    warnings: str = ""
    # Phase 3 scaffolding
    cited_abstract: str = ""
    cited_summary: str = ""
    research_purpose: str = ""


EXPORT_COLUMNS = [
    "repository_source_id",
    "import_type",
    "imported_at",
    "provenance_ref",
    "source_document",
    "page_in_source",
    "citing_sentence",
    "citing_paragraph",
    "context_before",
    "context_after",
    "citation_raw",
    "citation_ref_numbers",
    "cited_authors",
    "cited_title",
    "cited_year",
    "cited_source",
    "cited_volume",
    "cited_issue",
    "cited_pages",
    "cited_doi",
    "cited_url",
    "cited_raw_entry",
    "match_confidence",
    "match_method",
    "warnings",
    "cited_abstract",
    "cited_summary",
    "research_purpose",
]


class ExportArtifact(BaseModel):
    rows: list[ExportRow]
    total_citations_found: int = 0
    total_bib_entries: int = 0
    matched_count: int = 0
    unmatched_count: int = 0
