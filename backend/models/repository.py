"""Models for attached repository management and imports."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from backend.models.ingestion_profiles import DocumentNormalizationResult
from backend.models.sources import SourceDownloadRequest, SourceOutputSummary


class AttachRepositoryRequest(BaseModel):
    path: str


class CreateRepositoryRequest(BaseModel):
    path: str


class RepositoryScanSummary(BaseModel):
    scanned_at: str = ""
    total_sources: int = 0
    total_citations: int = 0
    next_source_id: int = 1
    manifests_scanned: int = 0
    artifacts_scanned: int = 0
    citations_scanned: int = 0
    duplicate_urls_removed: int = 0


class RepositoryHealth(BaseModel):
    missing_files: int = 0
    orphaned_citation_rows: int = 0


class RepositoryStatusResponse(BaseModel):
    attached: bool = False
    path: str = ""
    schema_version: int = 4
    next_source_id: int = 1
    total_sources: int = 0
    total_citations: int = 0
    queued_count: int = 0
    download_state: str = "idle"  # idle | running | cancelling | completed | cancelled | failed
    message: str = ""
    last_scan_at: str = ""
    last_updated_at: str = ""
    health: RepositoryHealth = Field(default_factory=RepositoryHealth)
    output_summary: SourceOutputSummary = Field(default_factory=SourceOutputSummary)
    scan: RepositoryScanSummary | None = None


class RepositoryImportResponse(BaseModel):
    import_id: str
    import_type: str
    total_candidates: int = 0
    accepted_new: int = 0
    duplicates_skipped: int = 0
    total_sources: int = 0
    queued_count: int = 0
    message: str = ""


class RepositoryActionResponse(BaseModel):
    status: str
    message: str = ""
    queued_count: int = 0
    total_sources: int = 0
    total_citations: int = 0


class RepositoryExportJobRequest(BaseModel):
    scope: str = "all"
    import_id: str = ""


class RepositoryExportJobResponse(BaseModel):
    job_id: str
    total_urls: int = 0
    scope: str = "all"
    import_id: str = ""
    message: str = ""


class RepositoryMergeRequest(BaseModel):
    source_paths: list[str]  # external repos to merge into current


class RepositoryMergeResponse(BaseModel):
    status: str  # "started" | "completed" | "failed"
    message: str = ""
    sources_merged: int = 0
    duplicates_removed: int = 0
    total_merged_sources: int = 0
    total_merged_citations: int = 0


class RepositoryProcessDocumentsResponse(BaseModel):
    job_id: str
    import_id: str
    accepted_documents: int = 0
    total_sources: int = 0
    total_citations: int = 0
    selected_profile_id: str = ""
    document_normalization: list[DocumentNormalizationResult] = Field(default_factory=list)
    message: str = ""


class RepositoryDocumentImportDocument(BaseModel):
    filename: str
    repository_path: str
    sha256: str = ""


class RepositoryDocumentImportRecord(BaseModel):
    import_id: str
    import_type: str
    imported_at: str = ""
    provenance: str = ""
    selected_profile_id: str = ""
    processing_job_id: str = ""
    document_count: int = 0
    rerunnable: bool = False
    documents: list[RepositoryDocumentImportDocument] = Field(default_factory=list)


class RepositoryDocumentImportListResponse(BaseModel):
    imports: list[RepositoryDocumentImportRecord] = Field(default_factory=list)


class RepositoryReprocessDocumentsRequest(BaseModel):
    target_import_ids: list[str] = Field(default_factory=list)
    profile_override: str = ""


class RepositoryReprocessDocumentsResponse(BaseModel):
    job_id: str
    reprocess_id: str
    target_import_ids: list[str] = Field(default_factory=list)
    accepted_documents: int = 0
    total_sources: int = 0
    total_citations: int = 0
    selected_profile_id: str = ""
    document_normalization: list[DocumentNormalizationResult] = Field(default_factory=list)
    message: str = ""


class RepositorySourceTaskRequest(SourceDownloadRequest):
    scope: str = "queued"
    import_id: str = ""
    source_ids: list[str] = Field(default_factory=list)
    limit: int | None = Field(default=None, ge=1, le=500)
    selected_phases: list[str] = Field(default_factory=list)


class RepositorySourceTaskResponse(BaseModel):
    job_id: str
    status: str = "started"
    scope: str = "queued"
    import_id: str = ""
    total_urls: int = 0
    message: str = ""


class RepositorySourceDeleteRequest(BaseModel):
    source_ids: list[str] = Field(default_factory=list)


class RepositorySourceDeleteResponse(BaseModel):
    status: str = "completed"
    deleted_sources: int = 0
    deleted_citations: int = 0
    deleted_files: int = 0
    total_sources: int = 0
    total_citations: int = 0
    message: str = ""


class RepositorySourceExportRequest(BaseModel):
    source_ids: list[str] = Field(default_factory=list)
    file_kinds: list[str] = Field(default_factory=list)
    destination_path: str = ""


class RepositorySourceExportResponse(BaseModel):
    status: str = "completed"
    requested_sources: int = 0
    exported_files: int = 0
    missing_files: int = 0
    destination_path: str = ""
    message: str = ""


class RepositorySourcePatchRequest(BaseModel):
    title: str | None = None
    author_names: str | None = None
    publication_date: str | None = None
    document_type: str | None = None
    organization_name: str | None = None
    organization_type: str | None = None
    tags_text: str | None = None
    notes: str | None = None
    summary_text: str | None = None
    overall_relevance: float | None = Field(default=None, ge=0.0, le=1.0)
    depth_score: float | None = Field(default=None, ge=0.0, le=1.0)
    relevant_detail_score: float | None = Field(default=None, ge=0.0, le=1.0)
    rating_rationale: str | None = None
    relevant_sections: str | None = None
    citation_title: str | None = None
    citation_authors: str | None = None
    citation_issued: str | None = None
    citation_type: str | None = None
    citation_url: str | None = None
    citation_publisher: str | None = None
    citation_container_title: str | None = None
    citation_volume: str | None = None
    citation_issue: str | None = None
    citation_pages: str | None = None
    citation_doi: str | None = None
    citation_report_number: str | None = None
    citation_standard_number: str | None = None
    citation_language: str | None = None
    citation_accessed: str | None = None
    citation_override_fields: list[str] = Field(default_factory=list)
    custom_fields: dict[str, str | None] = Field(default_factory=dict)


class RepositoryColumnOutputConstraint(BaseModel):
    kind: Literal["text", "yes_no", "integer", "number", "date"] = "text"
    allowed_values: list[str] = Field(default_factory=list)
    max_words: int | None = Field(default=None, ge=1, le=100)
    fallback_value: str = ""
    format_hint: str = ""


class RepositoryColumnConfig(BaseModel):
    id: str
    label: str
    kind: Literal["builtin", "custom"] = "builtin"
    builtin_key: str = ""
    instruction_prompt: str = ""
    output_constraint: RepositoryColumnOutputConstraint | None = None
    last_run_at: str = ""
    last_run_status: str = ""


class RepositoryColumnCreateRequest(BaseModel):
    label: str = ""


class RepositoryColumnUpdateRequest(BaseModel):
    label: str | None = None
    instruction_prompt: str | None = None
    output_constraint: RepositoryColumnOutputConstraint | None = None


class RepositoryColumnPromptFixRequest(BaseModel):
    draft_prompt: str = ""


class RepositoryColumnPromptFixResponse(BaseModel):
    status: str = "completed"
    column_id: str
    prompt: str = ""
    output_constraint: RepositoryColumnOutputConstraint | None = None
    notes: list[str] = Field(default_factory=list)


class RepositoryColumnRunRowError(BaseModel):
    source_id: str
    message: str = ""


class RepositoryColumnRunRequest(BaseModel):
    filters: "RepositoryManifestFilterRequest" = Field(default_factory=lambda: RepositoryManifestFilterRequest())
    scope: Literal["filtered", "all", "empty_only", "selected"] = "filtered"
    source_ids: list[str] = Field(default_factory=list)
    confirm_overwrite: bool = False


class RepositoryColumnRunStartResponse(BaseModel):
    job_id: str = ""
    status: str = "started"  # started | confirmation_required
    column_id: str
    total_rows: int = 0
    populated_rows: int = 0
    message: str = ""


class RepositoryColumnRunStatus(BaseModel):
    job_id: str
    column_id: str
    column_label: str = ""
    state: Literal["pending", "running", "completed", "failed", "cancelled"] = "pending"
    total_rows: int = 0
    processed_rows: int = 0
    succeeded_rows: int = 0
    failed_rows: int = 0
    current_source_id: str = ""
    current_source_title: str = ""
    message: str = ""
    started_at: str = ""
    completed_at: str = ""
    row_errors: list[RepositoryColumnRunRowError] = Field(default_factory=list)


class RepositoryManifestFilterRequest(BaseModel):
    q: str = ""
    fetch_status: str = ""
    detected_type: str = ""
    source_kind: str = ""
    document_type: str = ""
    organization_type: str = ""
    organization_name: str = ""
    author_names: str = ""
    publication_date: str = ""
    tags_text: str = ""
    has_summary: bool | None = None
    has_rating: bool | None = None
    rating_overall_min: float | None = None
    rating_overall_max: float | None = None
    rating_overall_relevance_min: float | None = None
    rating_overall_relevance_max: float | None = None
    rating_depth_score_min: float | None = None
    rating_depth_score_max: float | None = None
    rating_relevant_detail_score_min: float | None = None
    rating_relevant_detail_score_max: float | None = None
    citation_type: str = ""
    citation_doi: str = ""
    citation_report_number: str = ""
    citation_standard_number: str = ""
    citation_missing_fields: str = ""
    citation_ready: bool | None = None
    citation_confidence_min: float | None = None
    citation_confidence_max: float | None = None


class RepositoryCitationRisExportRequest(BaseModel):
    scope: str = "all"  # all | filtered | selected
    source_ids: list[str] = Field(default_factory=list)
    filters: RepositoryManifestFilterRequest = Field(default_factory=RepositoryManifestFilterRequest)


RepositoryColumnRunRequest.model_rebuild()
