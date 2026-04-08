import type {
  RepositoryManifestColumn,
  RepositoryManifestFilterPayload,
  RepositorySourceFileKind,
  RepositorySourceTaskRequest,
} from "../api/types";

export interface RepositoryBrowserFilters {
  q: string;
  fetchStatus: string;
  detectedType: string;
  sourceKind: string;
  documentType: string;
  organizationType: string;
  organizationName: string;
  authorNames: string;
  publicationDate: string;
  tagsText: string;
  hasSummary: string;
  hasRating: string;
  ratingOverallRelevanceMin: string;
  ratingOverallRelevanceMax: string;
  ratingDepthScoreMin: string;
  ratingDepthScoreMax: string;
  ratingRelevantDetailScoreMin: string;
  ratingRelevantDetailScoreMax: string;
  citationType: string;
  citationDoi: string;
  citationReportNumber: string;
  citationStandardNumber: string;
  citationMissingFields: string;
  citationReady: string;
  citationConfidenceMin: string;
  citationConfidenceMax: string;
  sortBy: string;
  sortDir: "asc" | "desc" | "";
  limit: number;
  offset: number;
}

export interface RepositoryBrowserSelectionInput {
  orderedIds: string[];
  currentSelectedIds: Set<string>;
  targetId: string;
  checked: boolean;
  lastAnchorId: string | null;
  shiftKey: boolean;
}

export interface RepositoryBrowserSelectionResult {
  selectedIds: Set<string>;
  anchorId: string;
}

export interface RepositoryBrowserStoredState {
  visibleColumns: string[];
  columnWidths: Record<string, number>;
}

export interface RepositoryBrowserColumnCategory {
  id: string;
  label: string;
  columnKeys: string[];
}

export type RepositoryBrowserTaskScope = "all" | "selected" | "empty_only";
export type RepositoryBrowserDownloadScope = "all" | "selected" | "failed_fetch";

export interface RepositoryBrowserQueuedSourceTask {
  id: "convert" | "cleanup" | "title" | "catalog" | "citation_verify" | "summary" | "rating";
  label: string;
  payload: RepositorySourceTaskRequest;
}

export interface RepositoryBrowserSourceTaskQueueInput {
  draft: RepositorySourceTaskRequest;
  scope: RepositoryBrowserTaskScope;
  selectedSourceIds: string[];
  defaultProjectProfileName: string;
}

export interface RepositoryBrowserDownloadTaskPayloadInput {
  draft: RepositorySourceTaskRequest;
  scope: RepositoryBrowserDownloadScope;
  selectedSourceIds: string[];
  defaultProjectProfileName: string;
  runCleanup: boolean;
}

export const REPOSITORY_BROWSER_FILE_COLUMNS: Array<{
  id: `file_${RepositorySourceFileKind}`;
  kind: RepositorySourceFileKind;
  label: string;
}> = [
  { id: "file_pdf", kind: "pdf", label: "PDF" },
  { id: "file_html", kind: "html", label: "HTML" },
  { id: "file_rendered", kind: "rendered", label: "Rendered" },
  { id: "file_md", kind: "md", label: "MD" },
];

export const REPOSITORY_BROWSER_BIBLIOGRAPHY_COLUMNS = [
  "title",
  "author_names",
  "publication_year",
  "publication_date",
  "document_type",
  "organization_name",
  "organization_type",
  "citation_type",
  "citation_doi",
  "citation_report_number",
  "citation_standard_number",
];

export const REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS = [
  "title",
  "author_names",
  "publication_year",
  "organization_type",
  "organization_name",
  "rating_overall_relevance",
];

const REPOSITORY_BROWSER_LEGACY_DEFAULT_VISIBLE_COLUMNS = [...REPOSITORY_BROWSER_BIBLIOGRAPHY_COLUMNS];

export const REPOSITORY_BROWSER_COLUMN_CATEGORIES: RepositoryBrowserColumnCategory[] = [
  {
    id: "bibliography",
    label: "Bibliography",
    columnKeys: REPOSITORY_BROWSER_BIBLIOGRAPHY_COLUMNS,
  },
  {
    id: "source",
    label: "Source",
    columnKeys: [
      "id",
      "source_kind",
      "source_document_name",
      "original_url",
      "final_url",
      "detected_type",
      "fetch_status",
      "fetched_at",
      "markdown_char_count",
    ],
  },
  {
    id: "analysis",
    label: "Analysis",
    columnKeys: [
      "summary_text",
      "rating_overall_relevance",
      "rating_depth_score",
      "rating_relevant_detail_score",
      "rating_rationale",
      "relevant_sections",
      "tags_text",
      "notes",
    ],
  },
  {
    id: "citation",
    label: "Citation",
    columnKeys: [
      "citation_title",
      "citation_authors",
      "citation_issued",
      "citation_url",
      "citation_publisher",
      "citation_container_title",
      "citation_volume",
      "citation_issue",
      "citation_pages",
      "citation_language",
      "citation_accessed",
      "citation_ready",
      "citation_verification_status",
      "citation_verified_at",
      "citation_missing_fields",
      "citation_blocked_reasons",
      "citation_manual_override_fields",
      "citation_confidence",
      "citation_field_evidence_json",
    ],
  },
  {
    id: "status",
    label: "Status",
    columnKeys: [
      "title_status",
      "catalog_status",
      "llm_cleanup_status",
      "summary_status",
      "rating_status",
      "error_message",
    ],
  },
  {
    id: "files",
    label: "Files",
    columnKeys: REPOSITORY_BROWSER_FILE_COLUMNS.map((item) => item.id),
  },
];

export const REPOSITORY_BROWSER_PAGE_SIZE = 250;

export function buildRepositoryBrowserStorageKey(repositoryPath: string): string {
  const normalizedPath = repositoryPath.trim() || "detached";
  return `repository-browser:v6:${encodeURIComponent(normalizedPath)}`;
}

export function clampRepositoryBrowserColumnWidth(width: number): number {
  if (!Number.isFinite(width)) return 180;
  return Math.max(120, Math.min(720, Math.round(width)));
}

function sameRepositoryBrowserColumns(left: string[], right: string[]): boolean {
  if (left.length !== right.length) return false;
  return left.every((value, index) => value === right[index]);
}

export function reorderRepositoryBrowserColumns(
  visibleColumns: string[],
  draggedColumnId: string,
  targetColumnId: string,
): string[] {
  if (draggedColumnId === targetColumnId) return visibleColumns;
  const fromIndex = visibleColumns.indexOf(draggedColumnId);
  const toIndex = visibleColumns.indexOf(targetColumnId);
  if (fromIndex < 0 || toIndex < 0) return visibleColumns;
  const next = [...visibleColumns];
  const [dragged] = next.splice(fromIndex, 1);
  next.splice(toIndex, 0, dragged);
  return sameRepositoryBrowserColumns(next, visibleColumns) ? visibleColumns : next;
}

export function moveRepositoryBrowserColumnToEnd(
  visibleColumns: string[],
  draggedColumnId: string,
): string[] {
  const fromIndex = visibleColumns.indexOf(draggedColumnId);
  if (fromIndex < 0 || fromIndex === visibleColumns.length - 1) return visibleColumns;
  const next = [...visibleColumns];
  const [dragged] = next.splice(fromIndex, 1);
  next.push(dragged);
  return sameRepositoryBrowserColumns(next, visibleColumns) ? visibleColumns : next;
}

export function buildRepositoryBrowserSourceTaskQueue({
  draft,
  scope,
  selectedSourceIds,
  defaultProjectProfileName,
}: RepositoryBrowserSourceTaskQueueInput): RepositoryBrowserQueuedSourceTask[] {
  const selectedRows = scope === "selected" ? selectedSourceIds.map((value) => value.trim()).filter(Boolean) : [];
  const taskScope = scope === "selected" ? "all" : scope;
  const sourceIds = selectedRows.length > 0 ? selectedRows : undefined;
  const basePayload: RepositorySourceTaskRequest = {
    ...draft,
    scope: taskScope,
    import_id: "",
    source_ids: sourceIds,
    selected_phases: [],
    rerun_failed_only: false,
    run_download: false,
    run_convert: false,
    run_catalog: false,
    run_citation_verify: false,
    run_llm_cleanup: false,
    run_llm_title: false,
    run_llm_summary: false,
    run_llm_rating: false,
    force_redownload: false,
    force_convert: false,
    force_catalog: false,
    force_citation_verify: false,
    force_llm_cleanup: false,
    force_title: false,
    force_summary: false,
    force_rating: false,
    include_raw_file: true,
    include_rendered_html: true,
    include_rendered_pdf: true,
    include_markdown: true,
    project_profile_name: draft.project_profile_name || defaultProjectProfileName,
  };

  const tasks: RepositoryBrowserQueuedSourceTask[] = [];
  const selectedTaskIds: Array<RepositoryBrowserQueuedSourceTask["id"]> = [];
  if (draft.run_llm_cleanup) selectedTaskIds.push("cleanup");
  if (draft.run_llm_title) selectedTaskIds.push("title");
  if (draft.run_catalog) selectedTaskIds.push("catalog");
  if (draft.run_citation_verify) selectedTaskIds.push("citation_verify");
  if (draft.run_llm_summary) selectedTaskIds.push("summary");
  if (draft.run_llm_rating) selectedTaskIds.push("rating");

  if (selectedTaskIds.length === 0) {
    return tasks;
  }

  selectedTaskIds.forEach((taskId) => {
    if (taskId === "cleanup") {
      tasks.push({
        id: taskId,
        label: "LLM Markdown Cleanup",
        payload: {
          ...basePayload,
          selected_phases: ["cleanup"],
          run_llm_cleanup: true,
        },
      });
      return;
    }
    if (taskId === "title") {
      tasks.push({
        id: taskId,
        label: "Title Resolution",
        payload: {
          ...basePayload,
          selected_phases: ["title"],
          run_llm_title: true,
        },
      });
      return;
    }
    if (taskId === "catalog") {
      tasks.push({
        id: taskId,
        label: "Catalog Metadata",
        payload: {
          ...basePayload,
          selected_phases: ["catalog"],
          run_catalog: true,
        },
      });
      return;
    }
    if (taskId === "citation_verify") {
      tasks.push({
        id: taskId,
        label: "Citation Verification",
        payload: {
          ...basePayload,
          selected_phases: ["citation_verify"],
          run_citation_verify: true,
        },
      });
      return;
    }
    if (taskId === "summary") {
      tasks.push({
        id: taskId,
        label: "LLM Summaries",
        payload: {
          ...basePayload,
          selected_phases: ["summary"],
          run_llm_summary: true,
        },
      });
      return;
    }
    tasks.push({
      id: taskId,
      label: "Rating Sources",
      payload: {
        ...basePayload,
        selected_phases: ["rating"],
        run_llm_rating: true,
      },
    });
  });

  return tasks;
}

export function buildRepositoryBrowserDownloadTaskPayload({
  draft,
  scope,
  selectedSourceIds,
  defaultProjectProfileName,
  runCleanup,
}: RepositoryBrowserDownloadTaskPayloadInput): RepositorySourceTaskRequest {
  const normalizedSelectedIds = selectedSourceIds.map((value) => value.trim()).filter(Boolean);
  const forceRedownload = scope !== "failed_fetch";
  const includeMarkdown = Boolean(draft.include_markdown || runCleanup);
  const runConvert = includeMarkdown;
  const forceConvert = forceRedownload && runConvert;

  return {
    ...draft,
    scope: "all",
    import_id: "",
    source_ids: scope === "selected" ? normalizedSelectedIds : [],
    selected_phases: [],
    rerun_failed_only: scope === "failed_fetch",
    run_download: true,
    run_convert: runConvert,
    run_catalog: false,
    run_citation_verify: false,
    run_llm_cleanup: runCleanup,
    run_llm_title: false,
    run_llm_summary: false,
    run_llm_rating: false,
    force_redownload: forceRedownload,
    force_convert: forceConvert,
    force_catalog: false,
    force_citation_verify: false,
    force_llm_cleanup: false,
    force_title: false,
    force_summary: false,
    force_rating: false,
    include_raw_file: Boolean(draft.include_raw_file),
    include_rendered_html: Boolean(draft.include_rendered_html),
    include_rendered_pdf: Boolean(draft.include_rendered_pdf),
    include_markdown: includeMarkdown,
    project_profile_name: draft.project_profile_name || defaultProjectProfileName,
  };
}

export function defaultRepositoryBrowserColumnWidth(columnKey: string): number {
  const widthByColumn: Record<string, number> = {
    id: 120,
    source_kind: 150,
    source_document_name: 220,
    title: 320,
    author_names: 240,
    publication_year: 140,
    publication_date: 150,
    document_type: 170,
    organization_name: 220,
    organization_type: 180,
    original_url: 300,
    final_url: 300,
    detected_type: 140,
    fetch_status: 140,
    fetched_at: 180,
    markdown_char_count: 150,
    summary_text: 380,
    rating_overall_relevance: 160,
    rating_depth_score: 140,
    rating_relevant_detail_score: 150,
    rating_rationale: 360,
    relevant_sections: 320,
    tags_text: 220,
    notes: 260,
    citation_type: 170,
    citation_title: 320,
    citation_authors: 240,
    citation_issued: 160,
    citation_url: 300,
    citation_publisher: 220,
    citation_container_title: 220,
    citation_volume: 120,
    citation_issue: 120,
    citation_pages: 150,
    citation_language: 140,
    citation_accessed: 160,
    citation_doi: 220,
    citation_report_number: 200,
    citation_standard_number: 200,
    citation_ready: 130,
    citation_verification_status: 170,
    citation_verified_at: 180,
    citation_missing_fields: 220,
    citation_blocked_reasons: 260,
    citation_manual_override_fields: 240,
    citation_confidence: 160,
    citation_field_evidence_json: 320,
    title_status: 130,
    catalog_status: 130,
    llm_cleanup_status: 150,
    summary_status: 140,
    rating_status: 130,
    error_message: 280,
    file_pdf: 110,
    file_html: 110,
    file_rendered: 120,
    file_md: 100,
  };
  return widthByColumn[columnKey] || 180;
}

export function migrateRepositoryBrowserVisibleColumns(visibleColumns: string[]): string[] {
  const normalized = visibleColumns.map((value) => String(value));
  if (
    sameRepositoryBrowserColumns(
      normalized,
      REPOSITORY_BROWSER_LEGACY_DEFAULT_VISIBLE_COLUMNS,
    )
  ) {
    return [...REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS];
  }
  return normalized;
}

export function resolveRepositoryBrowserColumnWidth(
  columnWidths: Record<string, number>,
  columnKey: string,
): number {
  return clampRepositoryBrowserColumnWidth(
    columnWidths[columnKey] ?? defaultRepositoryBrowserColumnWidth(columnKey),
  );
}

export function buildRepositoryBrowserQuery(filters: RepositoryBrowserFilters): URLSearchParams {
  const params = new URLSearchParams({
    q: filters.q,
    fetch_status: filters.fetchStatus,
    detected_type: filters.detectedType,
    source_kind: filters.sourceKind,
    document_type: filters.documentType,
    organization_type: filters.organizationType,
    organization_name: filters.organizationName,
    author_names: filters.authorNames,
    publication_date: filters.publicationDate,
    tags_text: filters.tagsText,
    limit: String(filters.limit),
    offset: String(filters.offset),
  });
  if (filters.sortBy.trim()) {
    params.set("sort_by", filters.sortBy);
    if (filters.sortDir) {
      params.set("sort_dir", filters.sortDir);
    }
  }

  if (filters.hasSummary === "true" || filters.hasSummary === "false") {
    params.set("has_summary", filters.hasSummary);
  }
  if (filters.hasRating === "true" || filters.hasRating === "false") {
    params.set("has_rating", filters.hasRating);
  }

  const numericFilters: Array<[string, string]> = [
    ["rating_overall_relevance_min", filters.ratingOverallRelevanceMin],
    ["rating_overall_relevance_max", filters.ratingOverallRelevanceMax],
    ["rating_depth_score_min", filters.ratingDepthScoreMin],
    ["rating_depth_score_max", filters.ratingDepthScoreMax],
    ["rating_relevant_detail_score_min", filters.ratingRelevantDetailScoreMin],
    ["rating_relevant_detail_score_max", filters.ratingRelevantDetailScoreMax],
    ["citation_confidence_min", filters.citationConfidenceMin],
    ["citation_confidence_max", filters.citationConfidenceMax],
  ];
  numericFilters.forEach(([key, value]) => {
    const normalized = String(value || "").trim();
    if (!normalized) return;
    params.set(key, normalized);
  });

  const textFilters: Array<[string, string]> = [
    ["citation_type", filters.citationType],
    ["citation_doi", filters.citationDoi],
    ["citation_report_number", filters.citationReportNumber],
    ["citation_standard_number", filters.citationStandardNumber],
    ["citation_missing_fields", filters.citationMissingFields],
  ];
  textFilters.forEach(([key, value]) => {
    const normalized = String(value || "").trim();
    if (!normalized) return;
    params.set(key, normalized);
  });
  if (filters.citationReady === "true" || filters.citationReady === "false") {
    params.set("citation_ready", filters.citationReady);
  }

  return params;
}

export function nextRepositoryBrowserSort(
  currentSortBy: string,
  currentSortDir: RepositoryBrowserFilters["sortDir"],
  columnKey: string,
): Pick<RepositoryBrowserFilters, "sortBy" | "sortDir"> {
  if (currentSortBy !== columnKey) {
    return { sortBy: columnKey, sortDir: "asc" };
  }
  if (currentSortDir === "asc") {
    return { sortBy: columnKey, sortDir: "desc" };
  }
  return { sortBy: "", sortDir: "" };
}

function parseOptionalFloat(value: string): number | null {
  const normalized = value.trim();
  if (!normalized) return null;
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function parseOptionalBoolean(value: string): boolean | null {
  if (value === "true") return true;
  if (value === "false") return false;
  return null;
}

export function buildRepositoryManifestFilterPayload(
  filters: RepositoryBrowserFilters,
): RepositoryManifestFilterPayload {
  return {
    q: filters.q,
    fetch_status: filters.fetchStatus,
    detected_type: filters.detectedType,
    source_kind: filters.sourceKind,
    document_type: filters.documentType,
    organization_type: filters.organizationType,
    organization_name: filters.organizationName,
    author_names: filters.authorNames,
    publication_date: filters.publicationDate,
    tags_text: filters.tagsText,
    has_summary: parseOptionalBoolean(filters.hasSummary),
    has_rating: parseOptionalBoolean(filters.hasRating),
    rating_overall_min: null,
    rating_overall_max: null,
    rating_overall_relevance_min: parseOptionalFloat(filters.ratingOverallRelevanceMin),
    rating_overall_relevance_max: parseOptionalFloat(filters.ratingOverallRelevanceMax),
    rating_depth_score_min: parseOptionalFloat(filters.ratingDepthScoreMin),
    rating_depth_score_max: parseOptionalFloat(filters.ratingDepthScoreMax),
    rating_relevant_detail_score_min: parseOptionalFloat(filters.ratingRelevantDetailScoreMin),
    rating_relevant_detail_score_max: parseOptionalFloat(filters.ratingRelevantDetailScoreMax),
    citation_type: filters.citationType.trim(),
    citation_doi: filters.citationDoi.trim(),
    citation_report_number: filters.citationReportNumber.trim(),
    citation_standard_number: filters.citationStandardNumber.trim(),
    citation_missing_fields: filters.citationMissingFields.trim(),
    citation_ready: parseOptionalBoolean(filters.citationReady),
    citation_confidence_min: parseOptionalFloat(filters.citationConfidenceMin),
    citation_confidence_max: parseOptionalFloat(filters.citationConfidenceMax),
  };
}

export function toggleRepositoryBrowserSelection(
  input: RepositoryBrowserSelectionInput,
): RepositoryBrowserSelectionResult {
  const next = new Set(input.currentSelectedIds);
  const normalizedTarget = input.targetId.trim();
  if (!normalizedTarget) {
    return { selectedIds: next, anchorId: input.lastAnchorId || "" };
  }

  if (input.shiftKey && input.lastAnchorId) {
    const startIndex = input.orderedIds.indexOf(input.lastAnchorId);
    const endIndex = input.orderedIds.indexOf(normalizedTarget);
    if (startIndex >= 0 && endIndex >= 0) {
      const [from, to] = startIndex <= endIndex ? [startIndex, endIndex] : [endIndex, startIndex];
      input.orderedIds.slice(from, to + 1).forEach((id) => {
        if (input.checked) {
          next.add(id);
        } else {
          next.delete(id);
        }
      });
      return { selectedIds: next, anchorId: normalizedTarget };
    }
  }

  if (input.checked) {
    next.add(normalizedTarget);
  } else {
    next.delete(normalizedTarget);
  }
  return { selectedIds: next, anchorId: normalizedTarget };
}

export function sanitizeRepositoryBrowserExportTitle(title: string): string {
  const cleaned = title
    .replace(/[<>:"/\\|?*\u0000-\u001f]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/[. ]+$/g, "");
  return cleaned || "Untitled";
}

export function formatRepositoryBrowserExportFilename(
  sourceId: string,
  title: string,
  extension: string,
  usedNames: Set<string>,
): string {
  const normalizedExt =
    extension && extension.startsWith(".") ? extension : extension ? `.${extension}` : "";
  const base = `${sourceId} - ${sanitizeRepositoryBrowserExportTitle(title)}`;
  let candidate = `${base}${normalizedExt}`;
  let counter = 2;
  while (usedNames.has(candidate.toLowerCase())) {
    candidate = `${base} (${counter})${normalizedExt}`;
    counter += 1;
  }
  usedNames.add(candidate.toLowerCase());
  return candidate;
}

export function mergeRepositoryBrowserColumns(
  manifestColumns: RepositoryManifestColumn[],
): RepositoryManifestColumn[] {
  const synthetic = REPOSITORY_BROWSER_FILE_COLUMNS.map((column) => ({
    key: column.id,
    label: column.label,
    sortable: false,
    type: "text" as const,
    kind: "builtin" as const,
    renamable: false,
    processable: false,
    requires_llm: false,
    sort_type: "text" as const,
    instruction_prompt: "",
    output_constraint: null,
    include_row_context: false,
    include_source_text: false,
    last_run_at: "",
    last_run_status: "",
  }));
  return [...manifestColumns, ...synthetic];
}

export function labelRepositoryBrowserColumn(columnKey: string, fallbackLabel?: string): string {
  const overrides: Record<string, string> = {
    id: "Source ID",
    source_kind: "Source Kind",
    source_document_name: "Source Document",
    author_names: "Authors",
    publication_year: "Publication Year",
    publication_date: "Publication Date",
    document_type: "Document Type",
    organization_name: "Organization",
    organization_type: "Organization Type",
    tags_text: "Tags",
    citation_type: "Citation Type",
    citation_title: "Citation Title",
    citation_authors: "Citation Authors",
    citation_issued: "Citation Issued",
    citation_url: "Citation URL",
    citation_publisher: "Citation Publisher",
    citation_container_title: "Container Title",
    citation_volume: "Volume",
    citation_issue: "Issue",
    citation_pages: "Pages",
    citation_language: "Language",
    citation_accessed: "Accessed",
    citation_doi: "Citation DOI",
    citation_report_number: "Report Number",
    citation_standard_number: "Standard Number",
    citation_missing_fields: "Missing Citation Fields",
    citation_verification_status: "Citation Status",
    citation_verified_at: "Citation Verified At",
    citation_blocked_reasons: "Citation Blocked Reasons",
    citation_manual_override_fields: "Citation Override Fields",
    citation_field_evidence_json: "Citation Field Evidence",
    citation_ready: "RIS Ready",
    citation_confidence: "Citation Confidence",
    rating_relevant_detail_score: "Detail Score",
  };
  return overrides[columnKey] || fallbackLabel || columnKey;
}
