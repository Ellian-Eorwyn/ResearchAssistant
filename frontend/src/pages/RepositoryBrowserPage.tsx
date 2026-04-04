import { useEffect, useMemo, useRef, useState, type ChangeEvent } from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type {
  RepositoryManifestColumn,
  RepositoryManifestRow,
  RepositorySourceFileKind,
} from "../api/types";
import {
  Button,
  EmptyState,
  InputField,
  SectionHeader,
  SelectField,
  SurfaceCard,
} from "../components/primitives";
import { useAppState } from "../state/AppState";
import {
  buildRepositoryBrowserQuery,
  buildRepositoryBrowserStorageKey,
  labelRepositoryBrowserColumn,
  mergeRepositoryBrowserColumns,
  REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS,
  REPOSITORY_BROWSER_FILE_COLUMNS,
  toggleRepositoryBrowserSelection,
  type RepositoryBrowserFilters,
} from "./repositoryBrowserUtils";

const DEFAULT_FILTERS: RepositoryBrowserFilters = {
  q: "",
  fetchStatus: "",
  detectedType: "",
  sourceKind: "",
  documentType: "",
  organizationType: "",
  organizationName: "",
  authorNames: "",
  publicationDate: "",
  tagsText: "",
  hasSummary: "",
  hasRating: "",
  ratingOverallRelevanceMin: "",
  ratingOverallRelevanceMax: "",
  ratingDepthScoreMin: "",
  ratingDepthScoreMax: "",
  ratingRelevantDetailScoreMin: "",
  ratingRelevantDetailScoreMax: "",
  sortBy: "id",
  sortDir: "asc",
  limit: 250,
  offset: 0,
};

const DEFAULT_EXPORT_SELECTION: Record<RepositorySourceFileKind, boolean> = {
  pdf: false,
  html: false,
  rendered: false,
  md: true,
};

function hasRawFileWithSuffix(row: RepositoryManifestRow, suffixes: string[]): boolean {
  const rawFile = String(row.raw_file || "").trim().toLowerCase();
  return suffixes.some((suffix) => rawFile.endsWith(suffix));
}

function hasFileForKind(row: RepositoryManifestRow, kind: RepositorySourceFileKind): boolean {
  if (kind === "pdf") return hasRawFileWithSuffix(row, [".pdf"]);
  if (kind === "html") return hasRawFileWithSuffix(row, [".html", ".htm"]);
  if (kind === "rendered") {
    return Boolean(String(row.rendered_file || "").trim() || String(row.rendered_pdf_file || "").trim());
  }
  return Boolean(String(row.llm_cleanup_file || "").trim() || String(row.markdown_file || "").trim());
}

function buildFileHref(row: RepositoryManifestRow, kind: RepositorySourceFileKind): string {
  return `/api/repository/sources/${encodeURIComponent(row.id)}/files/${kind}`;
}

function formatCellValue(value: string | number | boolean | null | undefined): string {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  return String(value);
}

export function RepositoryBrowserPage() {
  const {
    getRepositoryManifest,
    pickRepositoryDirectory,
    refreshDashboard,
    repositoryStatus,
    lastRepositoryPath,
  } = useAppState();

  const [filters, setFilters] = useState<RepositoryBrowserFilters>(DEFAULT_FILTERS);
  const [visibleColumns, setVisibleColumns] = useState<string[]>(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [lastAnchorId, setLastAnchorId] = useState<string | null>(null);
  const [showColumnChooser, setShowColumnChooser] = useState(false);
  const [showExportPanel, setShowExportPanel] = useState(false);
  const [exportPath, setExportPath] = useState("");
  const [exportSelection, setExportSelection] =
    useState<Record<RepositorySourceFileKind, boolean>>(DEFAULT_EXPORT_SELECTION);
  const [actionMessage, setActionMessage] = useState("");
  const [actionError, setActionError] = useState("");
  const [deletePending, setDeletePending] = useState(false);
  const [exportPending, setExportPending] = useState(false);

  const storageKey = useMemo(() => buildRepositoryBrowserStorageKey(), []);

  useEffect(() => {
    try {
      const legacyKeys: string[] = [];
      for (let index = 0; index < window.localStorage.length; index += 1) {
        const key = window.localStorage.key(index);
        if (key && key.startsWith("repository-browser:v2:")) {
          legacyKeys.push(key);
        }
      }
      legacyKeys.forEach((key) => window.localStorage.removeItem(key));
    } catch {
      // Ignore storage failures.
    }
  }, []);

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(storageKey);
      if (!raw) {
        setVisibleColumns(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS);
        setFilters((prev) => ({ ...prev, limit: DEFAULT_FILTERS.limit, offset: 0 }));
        return;
      }
      const parsed = JSON.parse(raw) as {
        visibleColumns?: string[];
        pageSize?: number;
      };
      if (Array.isArray(parsed.visibleColumns) && parsed.visibleColumns.length > 0) {
        setVisibleColumns(parsed.visibleColumns.map((item) => String(item)));
      } else {
        setVisibleColumns(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS);
      }
      if (Number.isFinite(parsed.pageSize) && Number(parsed.pageSize) > 0) {
        setFilters((prev) => ({
          ...prev,
          limit: Math.max(25, Math.min(500, Number(parsed.pageSize))),
          offset: 0,
        }));
      } else {
        setFilters((prev) => ({ ...prev, limit: DEFAULT_FILTERS.limit, offset: 0 }));
      }
    } catch {
      setVisibleColumns(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS);
      setFilters((prev) => ({ ...prev, limit: DEFAULT_FILTERS.limit, offset: 0 }));
    }
  }, [storageKey]);

  const queryParams = useMemo(() => buildRepositoryBrowserQuery(filters), [filters]);
  const queryString = queryParams.toString();

  const manifestQuery = useQuery({
    queryKey: ["repository-browser-manifest", queryString],
    queryFn: () => getRepositoryManifest(queryParams),
    staleTime: 1000,
  });

  const allColumns = useMemo(
    () => mergeRepositoryBrowserColumns(manifestQuery.data?.columns || []),
    [manifestQuery.data?.columns],
  );
  const sortableColumns = useMemo(
    () => allColumns.filter((column) => column.sortable),
    [allColumns],
  );

  useEffect(() => {
    if (!manifestQuery.data?.columns) return;
    const available = new Set(allColumns.map((column) => column.key));
    setVisibleColumns((prev) => {
      const filtered = prev.filter((column) => available.has(column));
      if (filtered.length > 0) return filtered;
      return REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS.filter((column) => available.has(column));
    });
  }, [allColumns, manifestQuery.data?.columns]);

  useEffect(() => {
    try {
      window.localStorage.setItem(
        storageKey,
        JSON.stringify({
          visibleColumns,
          pageSize: filters.limit,
        }),
      );
    } catch {
      // Ignore storage failures.
    }
  }, [filters.limit, storageKey, visibleColumns]);

  useEffect(() => {
    setSelectedIds(new Set());
    setLastAnchorId(null);
    setActionError("");
    setActionMessage("");
  }, [repositoryStatus?.path]);

  useEffect(() => {
    const total = manifestQuery.data?.total || 0;
    if (total === 0 && filters.offset !== 0) {
      setFilters((prev) => ({ ...prev, offset: 0 }));
      return;
    }
    if (total > 0 && filters.offset >= total) {
      const maxOffset = Math.floor((total - 1) / filters.limit) * filters.limit;
      if (maxOffset !== filters.offset) {
        setFilters((prev) => ({ ...prev, offset: maxOffset }));
      }
    }
  }, [filters.limit, filters.offset, manifestQuery.data?.total]);

  const rows = manifestQuery.data?.rows || [];
  const totalRows = manifestQuery.data?.total || 0;
  const columnById = useMemo(() => {
    const entries = new Map<string, RepositoryManifestColumn>();
    allColumns.forEach((column) => entries.set(column.key, column));
    return entries;
  }, [allColumns]);

  const renderedColumns = useMemo(
    () => visibleColumns.map((columnId) => columnById.get(columnId)).filter(Boolean) as RepositoryManifestColumn[],
    [columnById, visibleColumns],
  );

  const visibleIds = useMemo(() => rows.map((row) => row.id), [rows]);
  const selectedVisibleCount = useMemo(
    () => visibleIds.filter((id) => selectedIds.has(id)).length,
    [selectedIds, visibleIds],
  );
  const allVisibleSelected = rows.length > 0 && selectedVisibleCount === rows.length;
  const someVisibleSelected = selectedVisibleCount > 0 && selectedVisibleCount < rows.length;

  const headerCheckboxRef = useRef<HTMLInputElement | null>(null);
  useEffect(() => {
    if (!headerCheckboxRef.current) return;
    headerCheckboxRef.current.indeterminate = someVisibleSelected;
  }, [someVisibleSelected]);

  const selectedExportKinds = useMemo(
    () =>
      (Object.entries(exportSelection) as Array<[RepositorySourceFileKind, boolean]>)
        .filter(([, enabled]) => enabled)
        .map(([kind]) => kind),
    [exportSelection],
  );

  const patchFilters = (patch: Partial<RepositoryBrowserFilters>, resetOffset = true) => {
    setFilters((prev) => ({
      ...prev,
      ...patch,
      offset: resetOffset ? 0 : patch.offset ?? prev.offset,
    }));
  };

  const toggleVisibleColumn = (columnId: string) => {
    setVisibleColumns((prev) => {
      if (prev.includes(columnId)) {
        if (prev.length === 1) return prev;
        return prev.filter((item) => item !== columnId);
      }
      return [...prev, columnId];
    });
  };

  const toggleSelectAllVisible = (checked: boolean) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      visibleIds.forEach((id) => {
        if (checked) {
          next.add(id);
        } else {
          next.delete(id);
        }
      });
      return next;
    });
    setLastAnchorId(visibleIds[0] || null);
  };

  const handleRowCheckbox = (event: ChangeEvent<HTMLInputElement>, rowId: string) => {
    const nativeEvent = event.nativeEvent as MouseEvent | Event;
    const result = toggleRepositoryBrowserSelection({
      orderedIds: visibleIds,
      currentSelectedIds: selectedIds,
      targetId: rowId,
      checked: event.target.checked,
      lastAnchorId,
      shiftKey: "shiftKey" in nativeEvent ? Boolean(nativeEvent.shiftKey) : false,
    });
    setSelectedIds(result.selectedIds);
    setLastAnchorId(result.anchorId);
  };

  const handleSort = (column: RepositoryManifestColumn) => {
    if (!column.sortable) return;
    patchFilters(
      {
        sortBy: column.key,
        sortDir:
          filters.sortBy === column.key && filters.sortDir === "asc" ? "desc" : "asc",
      },
      true,
    );
  };

  const handleBrowseExportPath = async () => {
    const picked = await pickRepositoryDirectory(
      "export",
      exportPath.trim() || repositoryStatus?.path || lastRepositoryPath,
    );
    if (picked) {
      setExportPath(picked);
      setActionError("");
    }
  };

  const handleDeleteSelected = async () => {
    const ids = Array.from(selectedIds);
    if (ids.length === 0 || deletePending) return;
    const confirmed = window.confirm(
      `Delete ${ids.length} selected source(s) and their linked citations and files?`,
    );
    if (!confirmed) return;

    setDeletePending(true);
    setActionError("");
    setActionMessage("");
    try {
      const response = await api.deleteRepositorySources(ids);
      setSelectedIds((prev) => {
        const next = new Set(prev);
        ids.forEach((id) => next.delete(id));
        return next;
      });
      setLastAnchorId(null);
      await refreshDashboard();
      await manifestQuery.refetch();
      setActionMessage(response.message || "Selected sources deleted.");
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to delete sources"));
    } finally {
      setDeletePending(false);
    }
  };

  const handleExportSelected = async () => {
    const ids = Array.from(selectedIds);
    if (ids.length === 0 || exportPending) return;
    if (!exportPath.trim()) {
      setActionError("Choose or enter an export destination first.");
      return;
    }
    if (selectedExportKinds.length === 0) {
      setActionError("Select at least one file type to export.");
      return;
    }

    setExportPending(true);
    setActionError("");
    setActionMessage("");
    try {
      const response = await api.exportRepositorySourceFiles({
        source_ids: ids,
        file_kinds: selectedExportKinds,
        destination_path: exportPath.trim(),
      });
      setActionMessage(response.message || "Selected files exported.");
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to export source files"));
    } finally {
      setExportPending(false);
    }
  };

  return (
    <div>
      <SectionHeader
        title="Repository Browser"
        description="Browse repository sources, tune visible columns, open artifacts, and run bulk export or delete actions."
      />

      {(actionMessage || actionError) && (
        <SurfaceCard className={actionError ? "mb-4 border border-error/30 bg-error/10" : "mb-4"}>
          <div className={actionError ? "text-body-md text-error" : "text-body-md text-on-surface"}>
            {actionError || actionMessage}
          </div>
        </SurfaceCard>
      )}

      <SurfaceCard className="mb-4">
        <div className="grid gap-3 lg:grid-cols-4">
          <InputField
            label="Search"
            placeholder="Title, URL, summary, rationale"
            value={filters.q}
            onChange={(event) => patchFilters({ q: event.target.value })}
          />
          <InputField
            label="Fetch Status"
            placeholder="success / failed / queued"
            value={filters.fetchStatus}
            onChange={(event) => patchFilters({ fetchStatus: event.target.value })}
          />
          <InputField
            label="Detected Type"
            placeholder="pdf / html / document"
            value={filters.detectedType}
            onChange={(event) => patchFilters({ detectedType: event.target.value })}
          />
          <InputField
            label="Source Kind"
            placeholder="url / uploaded_document"
            value={filters.sourceKind}
            onChange={(event) => patchFilters({ sourceKind: event.target.value })}
          />
        </div>

        <div className="mt-3 grid gap-3 lg:grid-cols-4">
          <InputField
            label="Document Type"
            placeholder="report / journal article / web page"
            value={filters.documentType}
            onChange={(event) => patchFilters({ documentType: event.target.value })}
          />
          <InputField
            label="Organization Type"
            placeholder="agency / company / university"
            value={filters.organizationType}
            onChange={(event) => patchFilters({ organizationType: event.target.value })}
          />
          <InputField
            label="Organization"
            placeholder="publisher or institution"
            value={filters.organizationName}
            onChange={(event) => patchFilters({ organizationName: event.target.value })}
          />
          <InputField
            label="Authors"
            placeholder="author names"
            value={filters.authorNames}
            onChange={(event) => patchFilters({ authorNames: event.target.value })}
          />
        </div>

        <div className="mt-3 grid gap-3 lg:grid-cols-4">
          <InputField
            label="Publication Date"
            placeholder="2024 or 2024-03-15"
            value={filters.publicationDate}
            onChange={(event) => patchFilters({ publicationDate: event.target.value })}
          />
          <InputField
            label="Tags"
            placeholder="policy, housing, retrofit"
            value={filters.tagsText}
            onChange={(event) => patchFilters({ tagsText: event.target.value })}
          />
          <SelectField
            label="Page Size"
            value={String(filters.limit)}
            onChange={(event) =>
              patchFilters({ limit: Math.max(25, Math.min(500, Number(event.target.value) || 250)) })
            }
          >
            <option value="50">50</option>
            <option value="100">100</option>
            <option value="250">250</option>
            <option value="500">500</option>
          </SelectField>
        </div>

        <div className="mt-3 grid gap-3 md:grid-cols-2 lg:grid-cols-4">
          <SelectField
            label="Has Summary"
            value={filters.hasSummary}
            onChange={(event) => patchFilters({ hasSummary: event.target.value })}
          >
            <option value="">Any</option>
            <option value="true">Yes</option>
            <option value="false">No</option>
          </SelectField>
          <SelectField
            label="Has Rating"
            value={filters.hasRating}
            onChange={(event) => patchFilters({ hasRating: event.target.value })}
          >
            <option value="">Any</option>
            <option value="true">Yes</option>
            <option value="false">No</option>
          </SelectField>
          <SelectField
            label="Sort By"
            value={filters.sortBy}
            onChange={(event) => patchFilters({ sortBy: event.target.value })}
          >
            {sortableColumns.map((column) => (
              <option key={column.key} value={column.key}>
                {labelRepositoryBrowserColumn(column.key, column.label)}
              </option>
            ))}
          </SelectField>
          <SelectField
            label="Sort Dir"
            value={filters.sortDir}
            onChange={(event) => patchFilters({ sortDir: event.target.value as "asc" | "desc" })}
          >
            <option value="asc">Ascending</option>
            <option value="desc">Descending</option>
          </SelectField>
        </div>

        <div className="mt-4 grid gap-3 xl:grid-cols-3">
          <div className="rounded-md bg-surface-container-low p-3">
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
              Relevance
            </div>
            <div className="grid gap-2 md:grid-cols-2">
              <InputField
                label="Min"
                placeholder="0.0"
                value={filters.ratingOverallRelevanceMin}
                onChange={(event) => patchFilters({ ratingOverallRelevanceMin: event.target.value })}
              />
              <InputField
                label="Max"
                placeholder="1.0"
                value={filters.ratingOverallRelevanceMax}
                onChange={(event) => patchFilters({ ratingOverallRelevanceMax: event.target.value })}
              />
            </div>
          </div>

          <div className="rounded-md bg-surface-container-low p-3">
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
              Depth
            </div>
            <div className="grid gap-2 md:grid-cols-2">
              <InputField
                label="Min"
                placeholder="0.0"
                value={filters.ratingDepthScoreMin}
                onChange={(event) => patchFilters({ ratingDepthScoreMin: event.target.value })}
              />
              <InputField
                label="Max"
                placeholder="1.0"
                value={filters.ratingDepthScoreMax}
                onChange={(event) => patchFilters({ ratingDepthScoreMax: event.target.value })}
              />
            </div>
          </div>

          <div className="rounded-md bg-surface-container-low p-3">
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
              Detail
            </div>
            <div className="grid gap-2 md:grid-cols-2">
              <InputField
                label="Min"
                placeholder="0.0"
                value={filters.ratingRelevantDetailScoreMin}
                onChange={(event) => patchFilters({ ratingRelevantDetailScoreMin: event.target.value })}
              />
              <InputField
                label="Max"
                placeholder="1.0"
                value={filters.ratingRelevantDetailScoreMax}
                onChange={(event) => patchFilters({ ratingRelevantDetailScoreMax: event.target.value })}
              />
            </div>
          </div>
        </div>

        <div className="mt-4 flex flex-wrap items-center gap-2">
          <Button onClick={() => setShowColumnChooser((prev) => !prev)}>
            {showColumnChooser ? "Hide Columns" : "Choose Columns"}
          </Button>
          <Button onClick={() => setShowExportPanel((prev) => !prev)} disabled={selectedIds.size === 0}>
            Export Selected
          </Button>
          <Button variant="danger" onClick={() => void handleDeleteSelected()} disabled={selectedIds.size === 0 || deletePending}>
            {deletePending ? "Deleting..." : "Delete Selected"}
          </Button>
          <Button
            variant="ghost"
            onClick={() => {
              setFilters({ ...DEFAULT_FILTERS, limit: filters.limit, sortBy: filters.sortBy, sortDir: filters.sortDir });
            }}
          >
            Reset Filters
          </Button>
          {selectedIds.size > 0 && (
            <Button
              variant="ghost"
              onClick={() => {
                setSelectedIds(new Set());
                setLastAnchorId(null);
              }}
            >
              Clear Selection ({selectedIds.size})
            </Button>
          )}
        </div>

        {showColumnChooser && (
          <div className="mt-4 rounded-md border border-outline-variant/30 bg-surface-container-low p-4">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div className="text-title-sm font-semibold">Visible Columns</div>
              <Button
                variant="ghost"
                onClick={() =>
                  setVisibleColumns(
                    REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS.filter((columnId) =>
                      allColumns.some((column) => column.key === columnId),
                    ),
                  )
                }
              >
                Reset Columns
              </Button>
            </div>
            <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
              {allColumns.map((column) => (
                <label key={column.key} className="flex items-center gap-2 text-body-md">
                  <input
                    checked={visibleColumns.includes(column.key)}
                    onChange={() => toggleVisibleColumn(column.key)}
                    type="checkbox"
                  />
                  {labelRepositoryBrowserColumn(column.key, column.label)}
                </label>
              ))}
            </div>
          </div>
        )}

        {showExportPanel && (
          <div className="mt-4 rounded-md border border-outline-variant/30 bg-surface-container-low p-4">
            <div className="mb-3 text-title-sm font-semibold">Export Selected Files</div>
            <div className="grid gap-3 md:grid-cols-[1fr_auto]">
              <InputField
                label="Destination Folder"
                placeholder="/absolute/path/to/export/folder"
                value={exportPath}
                onChange={(event) => setExportPath(event.target.value)}
              />
              <Button className="md:mt-6" onClick={() => void handleBrowseExportPath()}>
                Browse Folder
              </Button>
            </div>
            <div className="mt-3 flex flex-wrap gap-3">
              {REPOSITORY_BROWSER_FILE_COLUMNS.map((column) => (
                <label key={column.id} className="flex items-center gap-2 text-body-md">
                  <input
                    checked={exportSelection[column.kind]}
                    onChange={(event) =>
                      setExportSelection((prev) => ({
                        ...prev,
                        [column.kind]: event.target.checked,
                      }))
                    }
                    type="checkbox"
                  />
                  {column.label}
                </label>
              ))}
            </div>
            <div className="mt-4 flex flex-wrap gap-2">
              <Button variant="primary" onClick={() => void handleExportSelected()} disabled={exportPending}>
                {exportPending ? "Exporting..." : "Run Export"}
              </Button>
              <Button
                onClick={() => {
                  setShowExportPanel(false);
                  setExportSelection(DEFAULT_EXPORT_SELECTION);
                }}
              >
                Close
              </Button>
            </div>
          </div>
        )}
      </SurfaceCard>

      <SurfaceCard className="p-0">
        {manifestQuery.isLoading ? (
          <div className="p-6 text-body-md text-on-surface-variant">Loading repository sources...</div>
        ) : manifestQuery.isError ? (
          <div className="p-6 text-body-md text-error">
            {String((manifestQuery.error as Error)?.message || "Failed to load repository sources")}
          </div>
        ) : rows.length === 0 ? (
          <div className="p-4">
            <EmptyState
              title="No matching sources"
              detail="Adjust filters or import additional sources into the repository."
            />
          </div>
        ) : (
          <>
            <div className="max-h-[68vh] overflow-auto thin-scrollbar">
              <table className="data-table">
                <thead>
                  <tr>
                    <th className="w-12">
                      <input
                        ref={headerCheckboxRef}
                        checked={allVisibleSelected}
                        onChange={(event) => toggleSelectAllVisible(event.target.checked)}
                        type="checkbox"
                      />
                    </th>
                    {renderedColumns.map((column) => {
                      const active = filters.sortBy === column.key;
                      const directionLabel = active ? (filters.sortDir === "asc" ? "↑" : "↓") : "";
                      return (
                        <th key={column.key}>
                          {column.sortable ? (
                            <button
                              className="flex items-center gap-2 text-left text-label-sm uppercase tracking-[0.08em] text-on-surface-variant"
                              onClick={() => handleSort(column)}
                              type="button"
                            >
                              <span>{labelRepositoryBrowserColumn(column.key, column.label)}</span>
                              <span>{directionLabel}</span>
                            </button>
                          ) : (
                            <span>{labelRepositoryBrowserColumn(column.key, column.label)}</span>
                          )}
                        </th>
                      );
                    })}
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row) => {
                    const isSelected = selectedIds.has(row.id);
                    return (
                      <tr key={`${row.id}-${row.original_url}`} className={isSelected ? "bg-surface-container-highest" : ""}>
                        <td>
                          <input
                            checked={isSelected}
                            onChange={(event) => handleRowCheckbox(event, row.id)}
                            type="checkbox"
                          />
                        </td>
                        {renderedColumns.map((column) => {
                          if (column.key.startsWith("file_")) {
                            const kind = column.key.replace("file_", "") as RepositorySourceFileKind;
                            return (
                              <td key={`${row.id}-${column.key}`}>
                                {hasFileForKind(row, kind) ? (
                                  <a
                                    className="text-primary hover:underline"
                                    href={buildFileHref(row, kind)}
                                    rel="noreferrer"
                                    target="_blank"
                                  >
                                    Open
                                  </a>
                                ) : (
                                  <span className="text-on-surface-variant/60">—</span>
                                )}
                              </td>
                            );
                          }

                          const value = row[column.key];
                          const formatted = formatCellValue(value);
                          const isLongText =
                            column.key === "summary_text" ||
                            column.key === "rating_rationale" ||
                            column.key === "relevant_sections";
                          return (
                            <td
                              key={`${row.id}-${column.key}`}
                              className={isLongText ? "data-cell-wrap repository-browser-long-cell" : ""}
                              title={formatted === "—" ? "" : formatted}
                            >
                              {formatted}
                            </td>
                          );
                        })}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>

            <div className="flex items-center justify-between border-t border-outline-variant/30 bg-surface-container px-4 py-3">
              <div className="text-body-md text-on-surface-variant">
                Showing {rows.length} of {totalRows} rows
              </div>
              <div className="flex gap-2">
                <Button
                  disabled={filters.offset <= 0}
                  onClick={() => patchFilters({ offset: Math.max(0, filters.offset - filters.limit) }, false)}
                >
                  Prev
                </Button>
                <Button
                  disabled={filters.offset + filters.limit >= totalRows}
                  onClick={() => patchFilters({ offset: filters.offset + filters.limit }, false)}
                >
                  Next
                </Button>
              </div>
            </div>
          </>
        )}
      </SurfaceCard>
    </div>
  );
}
