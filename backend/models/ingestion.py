"""Models for the document ingestion stage."""

from __future__ import annotations

from pydantic import BaseModel, Field


class TextBlock(BaseModel):
    text: str
    page_number: int | None = None
    block_index: int
    is_heading: bool = False
    heading_level: int | None = None
    char_offset_start: int = 0  # offset in full_text
    char_offset_end: int = 0


class IngestedDocument(BaseModel):
    filename: str
    file_type: str
    total_pages: int | None = None
    blocks: list[TextBlock]
    full_text: str
    warnings: list[str] = Field(default_factory=list)
    # Inline citation URLs extracted from Markdown links: {ref_number: url}
    inline_citation_urls: dict[int, str] = Field(default_factory=dict)


class IngestionArtifact(BaseModel):
    documents: list[IngestedDocument]
