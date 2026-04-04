"""Models for attached repository management and imports."""

from __future__ import annotations

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
    schema_version: int = 3
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
