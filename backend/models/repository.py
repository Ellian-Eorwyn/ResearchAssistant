"""Models for attached repository management and imports."""

from __future__ import annotations

from pydantic import BaseModel, Field

from backend.models.sources import SourceOutputSummary


class AttachRepositoryRequest(BaseModel):
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
    schema_version: int = 2
    next_source_id: int = 1
    total_sources: int = 0
    total_citations: int = 0
    queued_count: int = 0
    download_state: str = "idle"  # idle | running | completed | failed
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
    primary_path: str
    secondary_path: str
    output_mode: str = "new"  # "new" | "into_primary"
    output_path: str = ""


class RepositoryMergeResponse(BaseModel):
    status: str  # "started" | "completed" | "failed"
    message: str = ""
    primary_sources: int = 0
    secondary_sources: int = 0
    duplicates_removed: int = 0
    total_merged_sources: int = 0
    total_merged_citations: int = 0
    output_path: str = ""
