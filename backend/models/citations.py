"""Models for in-text citation detection, sentence extraction, and matching."""

from __future__ import annotations

from pydantic import BaseModel, Field


class InTextCitation(BaseModel):
    citation_id: str
    document_filename: str
    raw_marker: str  # e.g. "[1]", "[2, 3]", "[4-7]"
    ref_numbers: list[int]
    page_number: int | None = None
    char_offset_start: int
    char_offset_end: int
    style: str = "bracket"  # bracket | superscript


class CitingSentence(BaseModel):
    sentence_id: str
    document_filename: str
    text: str
    paragraph: str = ""
    page_number: int | None = None
    citation_ids: list[str] = Field(default_factory=list)
    context_before: str = ""
    context_after: str = ""


class CitationMatch(BaseModel):
    citation_id: str
    ref_number: int
    sentence_id: str = ""
    matched_bib_entry_index: int | None = None
    match_confidence: float = 1.0
    match_method: str = "ref_number"
    unmatched_reason: str = ""


class CitationArtifact(BaseModel):
    citations: list[InTextCitation]
    sentences: list[CitingSentence]
    matches: list[CitationMatch]
    unmatched_citations: int = 0
    unmatched_bib_entries: int = 0
