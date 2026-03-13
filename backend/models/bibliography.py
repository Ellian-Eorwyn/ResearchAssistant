"""Models for references detection and bibliography parsing."""

from __future__ import annotations

from pydantic import BaseModel, Field


class ReferencesSection(BaseModel):
    document_filename: str
    start_block_index: int
    end_block_index: int
    heading_text: str
    raw_text: str
    detection_method: str  # heading_match | pattern_cluster | llm_assisted
    confidence: float = 1.0


class BibliographyEntry(BaseModel):
    ref_number: int | None = None
    raw_text: str
    source_document_name: str = ""
    authors: list[str] = Field(default_factory=list)
    title: str = ""
    year: str = ""
    journal_or_source: str = ""
    volume: str = ""
    issue: str = ""
    pages: str = ""
    doi: str = ""
    url: str = ""
    parse_confidence: float = 1.0
    parse_warnings: list[str] = Field(default_factory=list)
    repair_method: str = ""


class BibliographyArtifact(BaseModel):
    sections: list[ReferencesSection]
    entries: list[BibliographyEntry]
    total_raw_entries: int = 0
    parse_failures: int = 0
