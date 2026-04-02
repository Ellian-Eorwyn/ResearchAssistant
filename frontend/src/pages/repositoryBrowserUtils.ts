import type { RepositoryManifestColumn, RepositorySourceFileKind } from "../api/types";

export interface RepositoryBrowserFilters {
  q: string;
  fetchStatus: string;
  detectedType: string;
  hasSummary: string;
  hasRating: string;
  ratingOverallRelevanceMin: string;
  ratingOverallRelevanceMax: string;
  ratingDepthScoreMin: string;
  ratingDepthScoreMax: string;
  ratingRelevantDetailScoreMin: string;
  ratingRelevantDetailScoreMax: string;
  sortBy: string;
  sortDir: "asc" | "desc";
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

export const REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS = [
  "id",
  "source_document_name",
  "title",
  "markdown_char_count",
  "summary_text",
  "rating_overall_relevance",
  "rating_depth_score",
  "rating_relevant_detail_score",
  "rating_rationale",
  ...REPOSITORY_BROWSER_FILE_COLUMNS.map((item) => item.id),
];

export function buildRepositoryBrowserStorageKey(): string {
  return "repository-browser:v3";
}

export function buildRepositoryBrowserQuery(filters: RepositoryBrowserFilters): URLSearchParams {
  const params = new URLSearchParams({
    q: filters.q,
    fetch_status: filters.fetchStatus,
    detected_type: filters.detectedType,
    sort_by: filters.sortBy,
    sort_dir: filters.sortDir,
    limit: String(filters.limit),
    offset: String(filters.offset),
  });

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
  ];
  numericFilters.forEach(([key, value]) => {
    const normalized = value.trim();
    if (!normalized) return;
    params.set(key, normalized);
  });

  return params;
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
  }));
  return [...manifestColumns, ...synthetic];
}

export function labelRepositoryBrowserColumn(columnKey: string, fallbackLabel?: string): string {
  const overrides: Record<string, string> = {
    id: "Source ID",
    source_document_name: "Source Document",
    rating_relevant_detail_score: "Detail Score",
  };
  return overrides[columnKey] || fallbackLabel || columnKey;
}
