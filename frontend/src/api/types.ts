export type StageKey =
  | "ingesting"
  | "detecting_references"
  | "parsing_bibliography"
  | "detecting_citations"
  | "extracting_sentences"
  | "matching_citations"
  | "exporting";

export interface StageStatus {
  stage: StageKey;
  status: "pending" | "running" | "completed" | "failed";
  item_count: number;
  warnings: string[];
  errors: string[];
}

export interface JobStatusResponse {
  job_id: string;
  current_stage: "pending" | StageKey | "completed" | "failed";
  stages: StageStatus[];
  progress_pct: number;
  created_at: string;
  completed_at: string | null;
  selected_profile_id?: string;
  processing_mode?: string;
  target_import_ids?: string[];
  repository_preprocess_state?: string;
  repository_preprocess_message?: string;
  repository_finalize_state?: string;
  repository_finalize_message?: string;
  document_normalization?: DocumentNormalizationResult[];
  document_replacements?: RepositoryDocumentReplacement[];
}

export interface RepositoryHealth {
  missing_files: number;
  orphaned_citation_rows: number;
}

export interface SourceOutputSummary {
  total_rows: number;
  raw_file_count: number;
  rendered_html_count: number;
  rendered_pdf_count: number;
  markdown_count: number;
  llm_cleanup_file_count: number;
  llm_cleanup_needed_count: number;
  llm_cleanup_failed_count: number;
  catalog_file_count: number;
  catalog_missing_count: number;
  catalog_failed_count: number;
  summary_file_count: number;
  summary_missing_count: number;
  summary_failed_count: number;
  rating_file_count: number;
  rating_missing_count: number;
  rating_failed_count: number;
}

export interface RepositoryScanSummary {
  scanned_at: string;
  total_sources: number;
  total_citations: number;
  next_source_id: number;
  manifests_scanned: number;
  artifacts_scanned: number;
  citations_scanned: number;
  duplicate_urls_removed: number;
}

export interface RepositoryStatusResponse {
  attached: boolean;
  path: string;
  schema_version: number;
  next_source_id: number;
  total_sources: number;
  total_citations: number;
  queued_count: number;
  download_state: "idle" | "running" | "cancelling" | "completed" | "cancelled" | "failed";
  message: string;
  last_scan_at: string;
  last_updated_at: string;
  health: RepositoryHealth;
  output_summary: SourceOutputSummary;
  scan: RepositoryScanSummary | null;
}

export interface RepositoryImportResponse {
  import_id: string;
  import_type: string;
  total_candidates: number;
  accepted_new: number;
  duplicates_skipped: number;
  total_sources: number;
  queued_count: number;
  message: string;
}

export interface RepositoryActionResponse {
  status: string;
  message: string;
  queued_count: number;
  total_sources: number;
  total_citations: number;
}

export interface RepositoryMergeResponse {
  status: string;
  message: string;
  sources_merged: number;
  duplicates_removed: number;
  total_merged_sources: number;
  total_merged_citations: number;
}

export interface RepositoryProcessDocumentsResponse {
  job_id: string;
  import_id: string;
  accepted_documents: number;
  total_sources: number;
  total_citations: number;
  selected_profile_id: string;
  document_normalization: DocumentNormalizationResult[];
  message: string;
}

export interface RepositoryDocumentImportDocument {
  filename: string;
  repository_path: string;
  sha256: string;
}

export interface RepositoryDocumentImportRecord {
  import_id: string;
  import_type: string;
  imported_at: string;
  provenance: string;
  selected_profile_id: string;
  processing_job_id: string;
  document_count: number;
  rerunnable: boolean;
  documents: RepositoryDocumentImportDocument[];
}

export interface RepositoryDocumentImportListResponse {
  imports: RepositoryDocumentImportRecord[];
}

export interface RepositoryReprocessDocumentsRequest {
  target_import_ids: string[];
  profile_override: string;
}

export interface RepositoryReprocessDocumentsResponse {
  job_id: string;
  reprocess_id: string;
  target_import_ids: string[];
  accepted_documents: number;
  total_sources: number;
  total_citations: number;
  selected_profile_id: string;
  document_normalization: DocumentNormalizationResult[];
  message: string;
}

export interface RepositoryDocumentReplacement {
  filename: string;
  repository_path: string;
  status: string;
  replaced_existing_rows: number;
  preserved_existing_rows: number;
  new_rows: number;
}

export interface IngestionProfile {
  profile_id: string;
  label: string;
  description: string;
  built_in: boolean;
  file_type_hints: string[];
  reference_heading_patterns: string[];
  citation_marker_patterns: string[];
  bibliography_split_patterns: string[];
  llm_guidance: string;
  confidence_threshold: number;
  notes: string[];
}

export interface IngestionProfileSuggestion {
  suggestion_id: string;
  created_at: string;
  source_profile_id: string;
  proposed_profile: IngestionProfile;
  reason: string;
  example_filename: string;
  example_excerpt: string;
  status: "pending" | "accepted" | "rejected" | string;
}

export interface DocumentNormalizationResult {
  filename: string;
  source_document_path: string;
  standardized_markdown_path: string;
  metadata_path: string;
  selected_profile_id: string;
  selected_profile_label: string;
  status: "pending" | "normalized" | "partial" | "failed" | string;
  confidence_score: number;
  used_llm_fallback: boolean;
  bibliography_entry_count: number;
  total_citation_markers: number;
  matched_citation_markers: number;
  unresolved_citation_markers: number;
  reference_section_detected: boolean;
  works_cited_linked_entries: number;
  suggestion_ids: string[];
  warnings: string[];
  error_message: string;
}

export interface IngestionProfileListResponse {
  default_profile_id: string;
  profiles: IngestionProfile[];
}

export interface IngestionProfileSuggestionListResponse {
  suggestions: IngestionProfileSuggestion[];
}

export interface IngestionProfileActionResponse {
  status: string;
  message: string;
  profile: IngestionProfile | null;
}

export interface IngestionProfileSuggestionActionResponse {
  status: string;
  message: string;
  suggestion: IngestionProfileSuggestion | null;
  accepted_profile: IngestionProfile | null;
}

export interface RepositorySourceTaskResponse {
  job_id: string;
  status: string;
  scope: string;
  import_id: string;
  total_urls: number;
  message: string;
}

export interface SourceCancelResponse {
  job_id: string;
  status: "cancelling" | "not_running" | "running_no_handle";
  message?: string;
}

export interface PickDirectoryResponse {
  path: string;
}

export interface LLMBackendConfig {
  kind: "ollama" | "openai" | string;
  base_url: string;
  api_key: string;
  model: string;
  temperature: number;
  think_mode: "default" | "think" | "no_think";
  num_ctx: number;
  max_source_chars: number;
  llm_timeout: number;
}

export interface RepoSettings {
  llm_backend: LLMBackendConfig;
  use_llm: boolean;
  research_purpose: string;
  fetch_delay: number;
}

export interface AppSettings {
  last_repository_path: string;
}

export interface ModelsResponse {
  models: string[];
  error: string;
}

export interface BibliographyEntry {
  ref_number: number;
  authors: string[];
  title: string;
  year: string;
  url: string;
  doi: string;
  raw_text: string;
  parse_confidence: number;
}

export interface BibliographyResult {
  entries: BibliographyEntry[];
}

export interface CitationRow {
  raw_marker: string;
  ref_numbers: number[];
  page_number: number | null;
  style: string;
}

export interface SentenceRow {
  page_number: number | null;
  text: string;
  paragraph: string;
  citation_ids: string[];
}

export interface MatchRow {
  ref_number: number;
  matched_bib_entry_index: number | null;
  match_confidence: number;
  match_method: string;
}

export interface CitationResult {
  citations: CitationRow[];
  sentences: SentenceRow[];
  matches: MatchRow[];
}

export interface ExportResult {
  rows: Array<Record<string, string | number | null>>;
  matched_count: number;
  unmatched_count: number;
  total_bib_entries: number;
}

export interface SourceItemStatus {
  id: string;
  original_url: string;
  citation_number: string;
  source_kind: string;
  status: "pending" | "running" | "completed" | "failed" | "skipped" | "cancelled";
  fetch_status: string;
  catalog_status: string;
  title_status: string;
  llm_cleanup_status: string;
  summary_status: string;
  rating_status: string;
  error_message: string;
}

export interface RuntimeGuidance {
  code: string;
  title: string;
  detail: string;
  command: string;
}

export interface SourceDownloadStatus {
  job_id: string;
  state: "pending" | "running" | "cancelling" | "completed" | "failed" | "cancelled";
  total_urls: number;
  processed_urls: number;
  success_count: number;
  failed_count: number;
  partial_count: number;
  skipped_count: number;
  duplicate_urls_removed: number;
  cancel_requested: boolean;
  cancel_requested_at?: string | null;
  stop_after_current_item: boolean;
  message: string;
  runtime_notes: string[];
  runtime_guidance: RuntimeGuidance[];
  run_download: boolean;
  run_convert?: boolean;
  run_catalog: boolean;
  run_llm_cleanup: boolean;
  run_llm_title: boolean;
  run_llm_summary: boolean;
  run_llm_rating: boolean;
  force_redownload?: boolean;
  force_convert?: boolean;
  force_catalog?: boolean;
  force_llm_cleanup?: boolean;
  force_title?: boolean;
  force_summary?: boolean;
  force_rating?: boolean;
  output_summary: SourceOutputSummary;
  writes_to_repository: boolean;
  repository_path: string;
  selected_scope: string;
  selected_import_id: string;
  items: SourceItemStatus[];
}

export interface SourceDownloadRequest {
  rerun_failed_only: boolean;
  run_download: boolean;
  run_convert?: boolean;
  run_catalog: boolean;
  run_llm_cleanup: boolean;
  run_llm_title: boolean;
  run_llm_summary: boolean;
  run_llm_rating: boolean;
  force_redownload: boolean;
  force_convert?: boolean;
  force_catalog: boolean;
  force_llm_cleanup: boolean;
  force_title: boolean;
  force_summary: boolean;
  force_rating: boolean;
  project_profile_name: string;
  include_raw_file: boolean;
  include_rendered_html: boolean;
  include_rendered_pdf: boolean;
  include_markdown: boolean;
}

export interface RepositorySourceTaskRequest extends SourceDownloadRequest {
  scope: "all" | "queued" | "import" | "latest_import";
  import_id: string;
  source_ids?: string[];
  selected_phases?: string[];
}

export interface ProjectProfile {
  name: string;
  filename: string;
}

export interface RepositoryDashboardJob {
  job_id: string;
  kind: "citation_extraction" | "source_capture";
  state: string;
  updated_at: string;
  message: string;
  progress_pct?: number;
  processed_urls?: number;
  total_urls?: number;
}

export interface RepositoryDashboardImport {
  import_id: string;
  import_type: string;
  provenance: string;
  imported_at: string;
  total_candidates: number;
  accepted_new: number;
  duplicates_skipped: number;
}

export interface RepositoryDashboardResponse {
  status: RepositoryStatusResponse;
  metrics: {
    total_sources: number;
    total_citations: number;
    queued_count: number;
    next_source_id: number;
  };
  output_formats: {
    raw: number;
    rendered_html: number;
    rendered_pdf: number;
    markdown: number;
    catalogs: number;
    summaries: number;
    ratings: number;
  };
  warning_aggregates: {
    missing_files: number;
    orphaned_citation_rows: number;
    incomplete_summaries: number;
    failed_catalogs: number;
    failed_ratings: number;
    failed_fetches: number;
  };
  recent_imports: RepositoryDashboardImport[];
  recent_jobs: RepositoryDashboardJob[];
}

export interface RepositoryCitationDataResponse {
  bibliography: BibliographyResult;
  citations: CitationResult;
}

export interface RepositoryManifestRow {
  id: string;
  repository_source_id: string;
  source_kind: string;
  import_type: string;
  imported_at: string;
  provenance_ref: string;
  source_document_name: string;
  citation_number: string;
  original_url: string;
  final_url: string;
  fetch_status: string;
  http_status: number | null;
  content_type: string;
  detected_type: string;
  fetch_method: string;
  title: string;
  title_status: string;
  author_names: string;
  publication_date: string;
  publication_year: string;
  document_type: string;
  organization_name: string;
  organization_type: string;
  raw_file: string;
  rendered_file: string;
  rendered_pdf_file: string;
  markdown_file: string;
  llm_cleanup_needed: boolean;
  llm_cleanup_file: string;
  llm_cleanup_status: string;
  catalog_file: string;
  catalog_status: string;
  summary_file: string;
  summary_status: string;
  rating_file: string;
  rating_status: string;
  metadata_file: string;
  tags_text: string;
  notes: string;
  error_message: string;
  fetched_at: string;
  canonical_url: string;
  sha256: string;
  extraction_method: string;
  markdown_char_count: number;
  summary_text?: string;
  rating_overall?: string | number | boolean;
  rating_confidence?: string | number | boolean;
  rating_depth_score?: string | number | boolean;
  rating_overall_relevance?: string | number | boolean;
  rating_relevant_detail_score?: string | number | boolean;
  rating_rationale?: string;
  relevant_sections?: string;
  rating_dimensions_json?: string;
  flag_scores_json?: string;
  rating_raw_json?: string;
  [key: string]: string | number | boolean | null | undefined;
}

export interface RepositoryManifestColumn {
  key: string;
  label: string;
  sortable: boolean;
  type: "text" | "number";
}

export interface RepositoryManifestResponse {
  rows: RepositoryManifestRow[];
  total: number;
  limit: number;
  offset: number;
  sort_by: string;
  sort_dir: "asc" | "desc";
  columns: RepositoryManifestColumn[];
  filters: {
    q: string;
    fetch_status: string;
    detected_type: string;
    source_kind: string;
    document_type: string;
    organization_type: string;
    organization_name: string;
    author_names: string;
    publication_date: string;
    tags_text: string;
    has_summary: boolean | null;
    has_rating: boolean | null;
    rating_overall_min: number | null;
    rating_overall_max: number | null;
    rating_overall_relevance_min: number | null;
    rating_overall_relevance_max: number | null;
    rating_depth_score_min: number | null;
    rating_depth_score_max: number | null;
    rating_relevant_detail_score_min: number | null;
    rating_relevant_detail_score_max: number | null;
  };
}

export type RepositorySourceFileKind = "pdf" | "html" | "rendered" | "md";

export interface RepositorySourceDeleteResponse {
  status: string;
  deleted_sources: number;
  deleted_citations: number;
  deleted_files: number;
  total_sources: number;
  total_citations: number;
  message: string;
}

export interface RepositorySourceExportRequest {
  source_ids: string[];
  file_kinds: RepositorySourceFileKind[];
  destination_path: string;
}

export interface RepositorySourceExportResponse {
  status: string;
  requested_sources: number;
  exported_files: number;
  missing_files: number;
  destination_path: string;
  message: string;
}

export interface UploadResponse {
  job_id: string;
  files: Array<{ filename: string; file_type: string; size_bytes: number }>;
}
