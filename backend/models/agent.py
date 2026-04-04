"""Agent-facing request and response models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from backend.models.sources import SourcePhaseMetadata

AgentPhaseName = Literal["fetch", "convert", "tag", "summarize"]


class AgentErrorPayload(BaseModel):
    code: str
    message: str
    retryable: bool = False


class AgentRunSourcePhasesRequest(BaseModel):
    scope: str = "queued"
    import_id: str = ""
    source_ids: list[str] = Field(default_factory=list)
    phases: list[AgentPhaseName] = Field(default_factory=list)
    project_profile_name: str = ""
    force: bool = False
    limit: int | None = Field(default=None, ge=1, le=500)
    idempotency_key: str = ""


class AgentRunCounts(BaseModel):
    total: int = 0
    processed: int = 0
    success: int = 0
    failed: int = 0
    partial: int = 0
    skipped: int = 0


class AgentRunCurrentItem(BaseModel):
    source_id: str = ""
    url: str = ""


class AgentRunSnapshot(BaseModel):
    manifest_csv: str = ""
    manifest_xlsx: str = ""
    bundle_file: str = ""
    repository_path: str = ""
    output_summary: dict = Field(default_factory=dict)


class AgentRunRecord(BaseModel):
    run_id: str
    scope: str = ""
    import_id: str = ""
    phase_states: dict[str, SourcePhaseMetadata] = Field(default_factory=dict)
    counts: AgentRunCounts = Field(default_factory=AgentRunCounts)
    current_item: AgentRunCurrentItem = Field(default_factory=AgentRunCurrentItem)
    selected_source_ids: list[str] = Field(default_factory=list)
    started_at: str = ""
    completed_at: str = ""
    cancel_requested: bool = False
    result_snapshot: AgentRunSnapshot = Field(default_factory=AgentRunSnapshot)


class AgentSourceArtifactUris(BaseModel):
    markdown: str = ""
    clean_markdown: str = ""
    summary: str = ""
    rating: str = ""
    metadata: str = ""


class AgentSourceFreshness(BaseModel):
    summary_stale: bool = False
    rating_stale: bool = False


class AgentSourceProvenance(BaseModel):
    import_id: str = ""
    import_type: str = ""
    imported_at: str = ""
    provenance_ref: str = ""
    repository_path: str = ""
    repository_source_id: str = ""
    source_document_name: str = ""
    citation_number: str = ""


class AgentSourceRecord(BaseModel):
    source_id: str
    original_url: str = ""
    final_url: str = ""
    title: str = ""
    detected_type: str = ""
    fetch_status: str = ""
    convert_status: str = ""
    tag_status: str = ""
    summarize_status: str = ""
    rating_overall: float | None = None
    rating_confidence: float | None = None
    summary_present: bool = False
    rating_present: bool = False
    content_digests: dict[str, str] = Field(default_factory=dict)
    artifact_uris: AgentSourceArtifactUris = Field(default_factory=AgentSourceArtifactUris)
    provenance: AgentSourceProvenance = Field(default_factory=AgentSourceProvenance)
    freshness: AgentSourceFreshness = Field(default_factory=AgentSourceFreshness)
    phase_metadata: dict[str, SourcePhaseMetadata] = Field(default_factory=dict)
    updated_at: str = ""


class AgentResourceRecord(BaseModel):
    resource_id: str
    kind: str = ""
    path: str = ""
    title: str = ""
    tags: list[str] = Field(default_factory=list)
    last_modified_at: str = ""
    short_description: str = ""
    content_hash: str = ""
    mime_type: str = "text/markdown"


class AgentResourceContent(BaseModel):
    resource: AgentResourceRecord
    content: str = ""


class AgentSourceContentChunk(BaseModel):
    source_id: str
    kind: str
    mime_type: str = "text/plain"
    artifact_uri: str = ""
    cursor: str = ""
    next_cursor: str = ""
    total_length: int = 0
    offset_start: int = 0
    offset_end: int = 0
    content: str = ""
