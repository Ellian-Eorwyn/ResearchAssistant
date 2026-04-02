"""Models for repository-scoped ingestion profiles and normalization results."""

from __future__ import annotations

from pydantic import BaseModel, Field


class IngestionProfile(BaseModel):
    profile_id: str
    label: str
    description: str = ""
    built_in: bool = False
    file_type_hints: list[str] = Field(default_factory=list)
    reference_heading_patterns: list[str] = Field(default_factory=list)
    citation_marker_patterns: list[str] = Field(default_factory=list)
    bibliography_split_patterns: list[str] = Field(default_factory=list)
    llm_guidance: str = ""
    confidence_threshold: float = 0.6
    notes: list[str] = Field(default_factory=list)


class IngestionProfileSuggestion(BaseModel):
    suggestion_id: str
    created_at: str = ""
    source_profile_id: str = ""
    proposed_profile: IngestionProfile
    reason: str = ""
    example_filename: str = ""
    example_excerpt: str = ""
    status: str = "pending"  # pending | accepted | rejected


class DocumentNormalizationResult(BaseModel):
    filename: str
    source_document_path: str = ""
    standardized_markdown_path: str = ""
    metadata_path: str = ""
    selected_profile_id: str = ""
    selected_profile_label: str = ""
    status: str = "pending"  # pending | normalized | partial | failed
    confidence_score: float = 0.0
    used_llm_fallback: bool = False
    bibliography_entry_count: int = 0
    total_citation_markers: int = 0
    matched_citation_markers: int = 0
    unresolved_citation_markers: int = 0
    reference_section_detected: bool = False
    works_cited_linked_entries: int = 0
    suggestion_ids: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    error_message: str = ""


class IngestionProfileListResponse(BaseModel):
    default_profile_id: str = ""
    profiles: list[IngestionProfile] = Field(default_factory=list)


class IngestionProfileSuggestionListResponse(BaseModel):
    suggestions: list[IngestionProfileSuggestion] = Field(default_factory=list)


class IngestionProfileActionResponse(BaseModel):
    status: str = "completed"
    message: str = ""
    profile: IngestionProfile | None = None


class IngestionProfileSuggestionActionResponse(BaseModel):
    status: str = "completed"
    message: str = ""
    suggestion: IngestionProfileSuggestion | None = None
    accepted_profile: IngestionProfile | None = None
