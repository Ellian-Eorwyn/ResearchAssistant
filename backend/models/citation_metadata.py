"""Canonical citation metadata models for repository sources."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CitationAuthor(BaseModel):
    family: str = ""
    given: str = ""
    literal: str = ""


class CitationFieldEvidence(BaseModel):
    value: str = ""
    source_type: str = ""
    source_label: str = ""
    evidence: str = ""
    confidence: float = 0.0
    manual_override: bool = False


class CitationMetadata(BaseModel):
    item_type: str = ""
    title: str = ""
    authors: list[CitationAuthor] = Field(default_factory=list)
    issued: str = ""
    publisher: str = ""
    container_title: str = ""
    volume: str = ""
    issue: str = ""
    pages: str = ""
    doi: str = ""
    url: str = ""
    report_number: str = ""
    standard_number: str = ""
    language: str = ""
    accessed: str = ""
    evidence: list[str] = Field(default_factory=list)
    confidence: float = 0.0
    missing_fields: list[str] = Field(default_factory=list)
    ready_for_ris: bool = False
    verification_status: str = ""
    verification_confidence: float = 0.0
    verification_model: str = ""
    verification_content_digest: str = ""
    verified_at: str = ""
    blocked_reasons: list[str] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)
    manual_override_fields: list[str] = Field(default_factory=list)
    field_evidence: dict[str, CitationFieldEvidence] = Field(default_factory=dict)
