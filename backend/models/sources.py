"""Models for source download manifests and status tracking."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SourceManifestRow(BaseModel):
    id: str
    repository_source_id: str = ""
    import_type: str = ""
    imported_at: str = ""
    provenance_ref: str = ""
    source_document_name: str = ""
    citation_number: str = ""
    original_url: str = ""
    final_url: str = ""
    fetch_status: str = ""
    http_status: int | None = None
    content_type: str = ""
    detected_type: str = ""  # pdf | html | document | unsupported
    fetch_method: str = ""  # http | playwright
    title: str = ""
    raw_file: str = ""
    rendered_file: str = ""
    rendered_pdf_file: str = ""
    markdown_file: str = ""
    summary_file: str = ""
    metadata_file: str = ""
    notes: str = ""
    error_message: str = ""
    fetched_at: str = ""
    canonical_url: str = ""
    sha256: str = ""
    extraction_method: str = ""  # raw_html | rendered_html
    markdown_char_count: int = 0


SOURCE_MANIFEST_COLUMNS = [
    "id",
    "repository_source_id",
    "import_type",
    "imported_at",
    "provenance_ref",
    "source_document_name",
    "citation_number",
    "original_url",
    "final_url",
    "fetch_status",
    "http_status",
    "content_type",
    "detected_type",
    "fetch_method",
    "title",
    "raw_file",
    "rendered_file",
    "rendered_pdf_file",
    "markdown_file",
    "summary_file",
    "metadata_file",
    "notes",
    "error_message",
    "fetched_at",
    "canonical_url",
    "sha256",
    "extraction_method",
    "markdown_char_count",
]


class SourceManifestArtifact(BaseModel):
    rows: list[SourceManifestRow]
    total_urls: int = 0
    success_count: int = 0
    failed_count: int = 0
    partial_count: int = 0


class SourceItemStatus(BaseModel):
    id: str
    original_url: str
    citation_number: str = ""
    status: str = "pending"  # pending | running | completed | failed | skipped | cancelled
    fetch_status: str = ""
    error_message: str = ""


class RuntimeGuidance(BaseModel):
    code: str = ""
    title: str = ""
    detail: str = ""
    command: str = ""


class SourceDownloadStatus(BaseModel):
    job_id: str
    state: str = "pending"  # pending | running | completed | failed | cancelled
    total_urls: int = 0
    processed_urls: int = 0
    success_count: int = 0
    failed_count: int = 0
    partial_count: int = 0
    skipped_count: int = 0
    duplicate_urls_removed: int = 0
    started_at: str | None = None
    completed_at: str | None = None
    current_item_id: str = ""
    current_url: str = ""
    message: str = ""
    runtime_notes: list[str] = Field(default_factory=list)
    runtime_guidance: list[RuntimeGuidance] = Field(default_factory=list)
    rerun_failed_only: bool = False
    output_dir: str = ""
    manifest_csv: str = ""
    manifest_xlsx: str = ""
    bundle_file: str = ""
    items: list[SourceItemStatus] = Field(default_factory=list)


class SourceListUploadResponse(BaseModel):
    job_id: str
    filename: str
    total_rows: int = 0
    accepted_rows: int = 0
    missing_url_rows: int = 0
    estimated_duplicate_urls: int = 0
    merged_with_existing_job: bool = False
    total_urls_in_job: int = 0
