export interface SpreadsheetFilters {
  q: string;
  sortBy: string;
  sortDir: "asc" | "desc" | "";
  limit: number;
  offset: number;
}

export interface SpreadsheetStoredState {
  visibleColumns: string[];
  columnWidths: Record<string, number>;
}

export const SPREADSHEET_PAGE_SIZE = 50;

export function buildSpreadsheetManifestQuery(filters: SpreadsheetFilters): URLSearchParams {
  const params = new URLSearchParams({
    q: filters.q,
    limit: String(filters.limit),
    offset: String(filters.offset),
  });
  if (filters.sortBy) {
    params.set("sort_by", filters.sortBy);
  }
  if (filters.sortDir) {
    params.set("sort_dir", filters.sortDir);
  }
  return params;
}

export function buildSpreadsheetStorageKey(sessionId: string, targetId: string): string {
  return `spreadsheets:v1:${sessionId || "none"}:${targetId || "none"}`;
}

export function clampSpreadsheetColumnWidth(width: number): number {
  if (!Number.isFinite(width)) return 180;
  return Math.max(120, Math.min(640, Math.round(width)));
}

export function nextSpreadsheetSort(
  currentSortBy: string,
  currentSortDir: SpreadsheetFilters["sortDir"],
  columnId: string,
): Pick<SpreadsheetFilters, "sortBy" | "sortDir"> {
  if (currentSortBy !== columnId) {
    return { sortBy: columnId, sortDir: "asc" };
  }
  if (currentSortDir === "asc") {
    return { sortBy: columnId, sortDir: "desc" };
  }
  if (currentSortDir === "desc") {
    return { sortBy: "", sortDir: "" };
  }
  return { sortBy: columnId, sortDir: "asc" };
}
