"""Models for source download manifests and status tracking."""

from __future__ import annotations

from pydantic import BaseModel, Field


class SourcePhaseMetadata(BaseModel):
    phase: str = ""
    status: str = "pending"  # pending | running | completed | failed | skipped | stale
    error: str = ""
    error_code: str = ""
    started_at: str = ""
    completed_at: str = ""
    content_digest: str = ""
    model: str = ""
    profile_name: str = ""
    prompt_version: str = ""
    stale: bool = False


class SourceManifestRow(BaseModel):
    id: str
    repository_source_id: str = ""
    source_kind: str = "url"  # url | uploaded_document
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
    title_status: str = ""  # not_requested | existing | extracted | generated | failed | skipped_*
    author_names: str = ""
    publication_date: str = ""
    publication_year: str = ""
    seed_doi: str = ""
    document_type: str = ""
    organization_name: str = ""
    organization_type: str = ""
    raw_file: str = ""
    rendered_file: str = ""
    rendered_pdf_file: str = ""
    markdown_file: str = ""
    llm_cleanup_needed: bool = False
    llm_cleanup_file: str = ""
    llm_cleanup_status: str = ""  # not_requested | not_needed | cleaned | failed | skipped_*
    catalog_file: str = ""
    catalog_status: str = ""  # not_requested | existing | generated | failed | stale | skipped_*
    summary_file: str = ""
    summary_status: str = ""  # not_requested | existing | generated | failed | skipped_*
    rating_file: str = ""
    rating_status: str = ""  # not_requested | existing | generated | failed | skipped_*
    metadata_file: str = ""
    tags_text: str = ""
    notes: str = ""
    error_message: str = ""
    fetched_at: str = ""
    canonical_url: str = ""
    sha256: str = ""
    extraction_method: str = ""  # raw_html | rendered_html
    markdown_char_count: int = 0
    custom_fields: dict[str, str] = Field(default_factory=dict)
    phase_metadata: dict[str, SourcePhaseMetadata] = Field(default_factory=dict)


SOURCE_MANIFEST_COLUMNS = [
    "id",
    "repository_source_id",
    "source_kind",
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
    "title_status",
    "author_names",
    "publication_date",
    "publication_year",
    "document_type",
    "organization_name",
    "organization_type",
    "raw_file",
    "rendered_file",
    "rendered_pdf_file",
    "markdown_file",
    "llm_cleanup_needed",
    "llm_cleanup_file",
    "llm_cleanup_status",
    "catalog_file",
    "catalog_status",
    "summary_file",
    "summary_status",
    "rating_file",
    "rating_status",
    "metadata_file",
    "tags_text",
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
    source_kind: str = "url"
    status: str = "pending"  # pending | running | completed | failed | skipped | cancelled
    fetch_status: str = ""
    catalog_status: str = ""
    citation_verification_status: str = ""
    title_status: str = ""
    llm_cleanup_status: str = ""
    summary_status: str = ""
    rating_status: str = ""
    error_message: str = ""


class RuntimeGuidance(BaseModel):
    code: str = ""
    title: str = ""
    detail: str = ""
    command: str = ""


class SourceOutputOptions(BaseModel):
    include_raw_file: bool = True
    include_rendered_html: bool = True
    include_rendered_pdf: bool = True
    include_markdown: bool = True


class SourceDownloadRequest(BaseModel):
    rerun_failed_only: bool = False
    run_download: bool = True
    run_convert: bool = False
    run_catalog: bool = False
    run_citation_verify: bool = False
    run_llm_cleanup: bool = False
    run_llm_title: bool = False
    run_llm_summary: bool = True
    run_llm_rating: bool = False
    force_redownload: bool = False
    force_convert: bool = False
    force_catalog: bool = False
    force_citation_verify: bool = False
    force_llm_cleanup: bool = False
    force_title: bool = False
    force_summary: bool = False
    force_rating: bool = False
    project_profile_name: str = ""
    include_raw_file: bool = True
    include_rendered_html: bool = True
    include_rendered_pdf: bool = True
    include_markdown: bool = True


class SourceOutputSummary(BaseModel):
    total_rows: int = 0
    raw_file_count: int = 0
    rendered_html_count: int = 0
    rendered_pdf_count: int = 0
    markdown_count: int = 0
    llm_cleanup_file_count: int = 0
    llm_cleanup_needed_count: int = 0
    llm_cleanup_failed_count: int = 0
    catalog_file_count: int = 0
    catalog_missing_count: int = 0
    catalog_failed_count: int = 0
    summary_file_count: int = 0
    summary_missing_count: int = 0
    summary_failed_count: int = 0
    rating_file_count: int = 0
    rating_missing_count: int = 0
    rating_failed_count: int = 0


class SourceDownloadStatus(BaseModel):
    job_id: str
    state: str = "pending"  # pending | running | cancelling | completed | failed | cancelled
    total_urls: int = 0
    processed_urls: int = 0
    success_count: int = 0
    failed_count: int = 0
    partial_count: int = 0
    skipped_count: int = 0
    duplicate_urls_removed: int = 0
    started_at: str | None = None
    completed_at: str | None = None
    cancel_requested: bool = False
    cancel_requested_at: str | None = None
    stop_after_current_item: bool = False
    current_item_id: str = ""
    current_url: str = ""
    message: str = ""
    runtime_notes: list[str] = Field(default_factory=list)
    runtime_guidance: list[RuntimeGuidance] = Field(default_factory=list)
    rerun_failed_only: bool = False
    run_download: bool = True
    run_convert: bool = False
    run_catalog: bool = False
    run_citation_verify: bool = False
    run_llm_cleanup: bool = False
    run_llm_title: bool = False
    run_llm_summary: bool = True
    run_llm_rating: bool = False
    force_redownload: bool = False
    force_convert: bool = False
    force_catalog: bool = False
    force_citation_verify: bool = False
    force_llm_cleanup: bool = False
    force_title: bool = False
    force_summary: bool = False
    force_rating: bool = False
    project_profile_name: str = ""
    output_options: SourceOutputOptions = Field(default_factory=SourceOutputOptions)
    output_summary: SourceOutputSummary = Field(default_factory=SourceOutputSummary)
    output_dir: str = ""
    manifest_csv: str = ""
    manifest_xlsx: str = ""
    bundle_file: str = ""
    writes_to_repository: bool = False
    repository_path: str = ""
    selected_scope: str = ""
    selected_import_id: str = ""
    selected_phases: list[str] = Field(default_factory=list)
    phase_states: dict[str, SourcePhaseMetadata] = Field(default_factory=dict)
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
