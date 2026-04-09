import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
} from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type {
  SpreadsheetColumnConfig,
  SpreadsheetColumnRunStatus,
} from "../api/types";
import {
  Button,
  EmptyState,
  InputField,
  SectionHeader,
  StatusBadge,
  SurfaceCard,
  TextAreaField,
} from "../components/primitives";
import { useAppState } from "../state/AppState";
import {
  buildSpreadsheetManifestQuery,
  buildSpreadsheetStorageKey,
  clampSpreadsheetColumnWidth,
  nextSpreadsheetSort,
  SPREADSHEET_PAGE_SIZE,
  type SpreadsheetFilters,
} from "./spreadsheetUtils";

const POLL_INTERVAL_MS = 2000;

interface ColumnPromptDraftState {
  columnId: string;
  label: string;
  prompt: string;
  inputColumnIds: string[];
  outputConstraint: SpreadsheetColumnConfig["output_constraint"];
}

type ColumnRunScope = "all" | "empty_only" | "selected";

interface ColumnRunScopeDraftState {
  columnId: string;
  label: string;
  scope: ColumnRunScope;
}

interface EditingCellState {
  rowId: string;
  columnId: string;
  value: string;
}

function toneForRunState(state: string): "neutral" | "active" | "warning" | "error" | "success" {
  if (state === "running" || state === "pending") return "active";
  if (state === "failed") return "error";
  if (state === "completed") return "success";
  return "neutral";
}

function formatCellValue(value: unknown): string {
  if (value === null || value === undefined || value === "") return "";
  if (typeof value === "boolean") return value ? "true" : "false";
  return String(value);
}

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  document.body.removeChild(anchor);
  URL.revokeObjectURL(url);
}

function ColumnPromptModal({
  draft,
  columns,
  error,
  llmReady,
  fixingPrompt,
  savePending,
  onChange,
  onCancel,
  onFixPrompt,
  onSave,
}: {
  draft: ColumnPromptDraftState;
  columns: SpreadsheetColumnConfig[];
  error: string;
  llmReady: boolean;
  fixingPrompt: boolean;
  savePending: boolean;
  onChange: (patch: Partial<ColumnPromptDraftState>) => void;
  onCancel: () => void;
  onFixPrompt: () => void;
  onSave: () => void;
}) {
  const selectableColumns = columns.filter((column) => column.id !== draft.columnId);

  const toggleInputColumn = (columnId: string) => {
    const selected = new Set(draft.inputColumnIds);
    if (selected.has(columnId)) {
      selected.delete(columnId);
    } else {
      selected.add(columnId);
    }
    onChange({ inputColumnIds: Array.from(selected) });
  };

  return (
    <div className="fixed inset-0 z-40 flex items-center justify-center bg-surface/80 p-4 backdrop-blur-sm">
      <div className="w-full max-w-4xl rounded-xl border border-outline-variant/40 bg-surface-container p-5 shadow-2xl">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-title-sm font-semibold">Column Instructions</div>
            <div className="mt-1 text-body-md text-on-surface-variant">{draft.label}</div>
          </div>
          <button
            className="rounded-sm px-2 py-1 text-label-sm text-on-surface-variant hover:text-on-surface"
            onClick={onCancel}
            type="button"
          >
            Close
          </button>
        </div>

        <TextAreaField
          className="mt-4"
          label="Prompt"
          rows={10}
          value={draft.prompt}
          onChange={(event) => onChange({ prompt: event.target.value })}
        />

        <div className="mt-4 rounded-lg border border-outline-variant/30 bg-surface-container-low p-4">
          <div className="text-title-sm font-semibold">Input Columns</div>
          <div className="mt-1 text-body-md text-on-surface-variant">
            Choose exactly which columns are sent to the model for this output column.
          </div>
          <div className="mt-4 grid max-h-64 gap-2 overflow-auto md:grid-cols-2">
            {selectableColumns.map((column) => (
              <label
                key={column.id}
                className="flex items-start gap-3 rounded-md border border-outline-variant/20 bg-surface px-3 py-3"
              >
                <input
                  checked={draft.inputColumnIds.includes(column.id)}
                  type="checkbox"
                  onChange={() => toggleInputColumn(column.id)}
                />
                <span>
                  <span className="block text-body-md text-on-surface">{column.label}</span>
                  <span className="block text-label-sm text-on-surface-variant">
                    {column.kind === "custom" ? "Custom" : column.data_type}
                  </span>
                </span>
              </label>
            ))}
          </div>
        </div>

        {!llmReady && (
          <div className="mt-3 rounded-md bg-warning/10 px-3 py-2 text-body-md text-warning">
            Configure and enable the repository LLM backend to fix prompts or run spreadsheet columns.
          </div>
        )}
        {error && (
          <div className="mt-3 rounded-md bg-error/10 px-3 py-2 text-body-md text-error">
            {error}
          </div>
        )}

        <div className="mt-5 flex flex-wrap justify-end gap-2">
          <Button onClick={onCancel}>Cancel</Button>
          <Button
            disabled={!llmReady || !draft.prompt.trim() || fixingPrompt || savePending}
            onClick={onFixPrompt}
          >
            {fixingPrompt ? "Fixing..." : "Fix Up Prompt"}
          </Button>
          <Button
            disabled={savePending || fixingPrompt}
            variant="primary"
            onClick={onSave}
          >
            {savePending ? "Saving..." : "Save"}
          </Button>
        </div>
      </div>
    </div>
  );
}

function ColumnRunScopeModal({
  draft,
  selectedCount,
  startPending,
  onCancel,
  onChangeScope,
  onConfirm,
}: {
  draft: ColumnRunScopeDraftState;
  selectedCount: number;
  startPending: boolean;
  onCancel: () => void;
  onChangeScope: (scope: ColumnRunScope) => void;
  onConfirm: () => void;
}) {
  const selectedDisabled = selectedCount === 0;
  return (
    <div className="fixed inset-0 z-40 flex items-center justify-center bg-surface/80 p-4 backdrop-blur-sm">
      <div className="w-full max-w-xl rounded-xl border border-outline-variant/40 bg-surface-container p-5 shadow-2xl">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-title-sm font-semibold">Run Column</div>
            <div className="mt-1 text-body-md text-on-surface-variant">{draft.label}</div>
          </div>
          <button
            className="rounded-sm px-2 py-1 text-label-sm text-on-surface-variant hover:text-on-surface"
            disabled={startPending}
            onClick={onCancel}
            type="button"
          >
            Close
          </button>
        </div>

        <div className="mt-4 grid gap-2">
          {(
            [
              {
                scope: "all",
                label: "Whole Table",
                detail: "Run this column across every row in the active table.",
                disabled: false,
              },
              {
                scope: "empty_only",
                label: "Blank Rows Only",
                detail: "Fill only rows where this column is currently empty.",
                disabled: false,
              },
              {
                scope: "selected",
                label: "Selected Rows",
                detail: selectedDisabled
                  ? "Select one or more rows in the table to use this scope."
                  : `Run only the ${selectedCount} selected row${selectedCount === 1 ? "" : "s"}.`,
                disabled: selectedDisabled,
              },
            ] as Array<{ scope: ColumnRunScope; label: string; detail: string; disabled: boolean }>
          ).map((option) => (
            <button
              key={option.scope}
              className={[
                "rounded-lg border px-4 py-4 text-left transition",
                draft.scope === option.scope
                  ? "border-primary bg-primary/10"
                  : "border-outline-variant/30 bg-surface-container-low hover:bg-surface-container",
                option.disabled ? "cursor-not-allowed opacity-50" : "",
              ].join(" ")}
              disabled={startPending || option.disabled}
              onClick={() => onChangeScope(option.scope)}
              type="button"
            >
              <div className="font-semibold text-on-surface">{option.label}</div>
              <div className="mt-1 text-body-md text-on-surface-variant">{option.detail}</div>
            </button>
          ))}
        </div>

        <div className="mt-5 flex justify-end gap-2">
          <Button disabled={startPending} onClick={onCancel}>
            Cancel
          </Button>
          <Button disabled={startPending} variant="primary" onClick={onConfirm}>
            {startPending ? "Starting..." : "Start Run"}
          </Button>
        </div>
      </div>
    </div>
  );
}

export function SpreadsheetsPage() {
  const { appSettings, repositoryStatus } = useAppState();
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [selectedSessionId, setSelectedSessionId] = useState("");
  const [uploading, setUploading] = useState(false);
  const [actionError, setActionError] = useState("");
  const [actionMessage, setActionMessage] = useState("");
  const [filters, setFilters] = useState<SpreadsheetFilters>({
    q: "",
    sortBy: "",
    sortDir: "",
    limit: SPREADSHEET_PAGE_SIZE,
    offset: 0,
  });
  const [visibleColumns, setVisibleColumns] = useState<string[]>([]);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const [selectedRowIds, setSelectedRowIds] = useState<Set<string>>(new Set());
  const [activeRowId, setActiveRowId] = useState("");
  const [detailDraft, setDetailDraft] = useState<Record<string, string>>({});
  const [detailSaving, setDetailSaving] = useState(false);
  const [editingCell, setEditingCell] = useState<EditingCellState | null>(null);
  const [promptDraft, setPromptDraft] = useState<ColumnPromptDraftState | null>(null);
  const [promptSaving, setPromptSaving] = useState(false);
  const [promptFixing, setPromptFixing] = useState(false);
  const [promptError, setPromptError] = useState("");
  const [runDraft, setRunDraft] = useState<ColumnRunScopeDraftState | null>(null);
  const [runStarting, setRunStarting] = useState(false);
  const [columnRunJobId, setColumnRunJobId] = useState<string | null>(null);
  const [activeColumnRun, setActiveColumnRun] = useState<SpreadsheetColumnRunStatus | null>(null);
  const [exporting, setExporting] = useState(false);

  const llmReady = Boolean(appSettings.use_llm && appSettings.llm_backend.model.trim());

  const workspaceQuery = useQuery({
    queryKey: ["spreadsheet-workspace", repositoryStatus?.path || ""],
    queryFn: () => api.getSpreadsheetWorkspaceStatus(),
  });

  useEffect(() => {
    if (!selectedSessionId && workspaceQuery.data?.current_session_id) {
      setSelectedSessionId(workspaceQuery.data.current_session_id);
    }
  }, [selectedSessionId, workspaceQuery.data?.current_session_id]);

  const sessionQuery = useQuery({
    queryKey: ["spreadsheet-session", selectedSessionId],
    enabled: selectedSessionId.trim().length > 0,
    queryFn: () => api.getSpreadsheetSession(selectedSessionId),
  });

  const activeTargetId = sessionQuery.data?.active_target?.id || "";
  const storageKey = useMemo(
    () => buildSpreadsheetStorageKey(selectedSessionId, activeTargetId),
    [selectedSessionId, activeTargetId],
  );

  useEffect(() => {
    if (!activeTargetId) return;
    const raw = localStorage.getItem(storageKey);
    if (!raw) {
      setVisibleColumns([]);
      setColumnWidths({});
      return;
    }
    try {
      const parsed = JSON.parse(raw) as { visibleColumns?: string[]; columnWidths?: Record<string, number> };
      setVisibleColumns(parsed.visibleColumns || []);
      setColumnWidths(parsed.columnWidths || {});
    } catch {
      setVisibleColumns([]);
      setColumnWidths({});
    }
  }, [storageKey, activeTargetId]);

  const manifestQuery = useQuery({
    queryKey: ["spreadsheet-manifest", selectedSessionId, activeTargetId, filters],
    enabled: selectedSessionId.trim().length > 0 && activeTargetId.trim().length > 0,
    queryFn: () =>
      api.getSpreadsheetManifest(selectedSessionId, buildSpreadsheetManifestQuery(filters)),
  });

  useEffect(() => {
    if (!activeTargetId || !manifestQuery.data) return;
    const availableIds = manifestQuery.data.columns.map((column) => column.id);
    setVisibleColumns((prev) => {
      if (prev.length === 0) return availableIds;
      const next = prev.filter((columnId) => availableIds.includes(columnId));
      return next.length > 0 ? next : availableIds;
    });
  }, [activeTargetId, manifestQuery.data]);

  useEffect(() => {
    if (!activeTargetId) return;
    localStorage.setItem(
      storageKey,
      JSON.stringify({ visibleColumns, columnWidths }),
    );
  }, [storageKey, visibleColumns, columnWidths, activeTargetId]);

  useEffect(() => {
    setSelectedRowIds(new Set());
    setActiveRowId("");
    setEditingCell(null);
    setDetailDraft({});
  }, [selectedSessionId, activeTargetId]);

  useEffect(() => {
    if (!columnRunJobId || !selectedSessionId) return;
    let active = true;
    const interval = setInterval(async () => {
      try {
        const status = await api.getSpreadsheetColumnRunStatus(selectedSessionId, columnRunJobId);
        if (!active) return;
        setActiveColumnRun(status);
        if (status.state === "completed" || status.state === "failed" || status.state === "cancelled") {
          clearInterval(interval);
          setColumnRunJobId(null);
          void manifestQuery.refetch();
          void sessionQuery.refetch();
          if (status.state === "failed") {
            setActionError(status.message || "Spreadsheet column run failed");
          } else {
            setActionMessage(status.message || "Spreadsheet column run completed");
          }
        }
      } catch (error) {
        if (!active) return;
        clearInterval(interval);
        setColumnRunJobId(null);
        setActionError(String((error as Error).message || "Failed to read spreadsheet run status"));
      }
    }, POLL_INTERVAL_MS);
    return () => {
      active = false;
      clearInterval(interval);
    };
  }, [columnRunJobId, manifestQuery, selectedSessionId, sessionQuery]);

  const rows = manifestQuery.data?.rows || [];
  const columns = sessionQuery.data?.columns || manifestQuery.data?.columns || [];
  const renderedColumns = useMemo(
    () => columns.filter((column) => visibleColumns.includes(column.id)),
    [columns, visibleColumns],
  );
  const activeRow = rows.find((row) => String(row.id || "") === activeRowId) || null;
  const sessionSummaries = workspaceQuery.data?.sessions || [];

  useEffect(() => {
    if (!activeRow) {
      setDetailDraft({});
      return;
    }
    const nextDraft: Record<string, string> = {};
    columns.forEach((column) => {
      nextDraft[column.id] = formatCellValue(activeRow[column.id]);
    });
    setDetailDraft(nextDraft);
  }, [activeRow, columns]);

  const handleUploadSpreadsheet = async (file: File | null) => {
    if (!file) return;
    setUploading(true);
    setActionError("");
    setActionMessage("");
    try {
      const response = await api.uploadSpreadsheetSession(file);
      const sessionId = response.session.session.session_id;
      setSelectedSessionId(sessionId);
      setFilters((prev) => ({ ...prev, offset: 0 }));
      await workspaceQuery.refetch();
      setActionMessage(response.message || `Opened ${file.name}.`);
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to open spreadsheet"));
    } finally {
      setUploading(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = "";
      }
    }
  };

  const handleTargetChange = async (targetId: string) => {
    if (!selectedSessionId) return;
    setActionError("");
    try {
      await api.activateSpreadsheetTarget(selectedSessionId, targetId);
      setFilters((prev) => ({ ...prev, offset: 0, sortBy: "", sortDir: "" }));
      await sessionQuery.refetch();
      await manifestQuery.refetch();
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to switch spreadsheet target"));
    }
  };

  const handleSelectSession = (event: ChangeEvent<HTMLSelectElement>) => {
    setSelectedSessionId(event.target.value);
    setFilters((prev) => ({ ...prev, offset: 0, sortBy: "", sortDir: "" }));
    setActionError("");
    setActionMessage("");
  };

  const handleOpenPrompt = (column: SpreadsheetColumnConfig) => {
    setPromptDraft({
      columnId: column.id,
      label: column.label,
      prompt: column.instruction_prompt,
      inputColumnIds: column.input_column_ids,
      outputConstraint: column.output_constraint,
    });
    setPromptError("");
  };

  const handleSavePrompt = async () => {
    if (!selectedSessionId || !promptDraft) return;
    setPromptSaving(true);
    setPromptError("");
    try {
      await api.updateSpreadsheetColumn(selectedSessionId, promptDraft.columnId, {
        instruction_prompt: promptDraft.prompt,
        output_constraint: promptDraft.outputConstraint,
        input_column_ids: promptDraft.inputColumnIds,
      });
      await sessionQuery.refetch();
      await manifestQuery.refetch();
      setPromptDraft(null);
      setActionMessage(`Saved settings for ${promptDraft.label}.`);
    } catch (error) {
      setPromptError(String((error as Error).message || "Failed to save spreadsheet column"));
    } finally {
      setPromptSaving(false);
    }
  };

  const handleFixPrompt = async () => {
    if (!selectedSessionId || !promptDraft) return;
    setPromptFixing(true);
    setPromptError("");
    try {
      const response = await api.fixSpreadsheetColumnPrompt(
        selectedSessionId,
        promptDraft.columnId,
        promptDraft.prompt,
      );
      setPromptDraft((prev) =>
        prev
          ? {
              ...prev,
              prompt: response.prompt,
              outputConstraint: response.output_constraint,
            }
          : prev,
      );
    } catch (error) {
      setPromptError(String((error as Error).message || "Failed to improve the prompt"));
    } finally {
      setPromptFixing(false);
    }
  };

  const handleCreateColumn = async () => {
    if (!selectedSessionId) return;
    setActionError("");
    setActionMessage("");
    try {
      const created = await api.createSpreadsheetColumn(selectedSessionId, "New Column");
      setVisibleColumns((prev) => (prev.includes(created.id) ? prev : [...prev, created.id]));
      await sessionQuery.refetch();
      await manifestQuery.refetch();
      setActionMessage(`Added ${created.label}.`);
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to create spreadsheet column"));
    }
  };

  const handleRenameColumn = async (column: SpreadsheetColumnConfig) => {
    if (!selectedSessionId || column.kind !== "custom") return;
    const label = window.prompt("Rename column", column.label);
    if (label == null) return;
    setActionError("");
    try {
      await api.updateSpreadsheetColumn(selectedSessionId, column.id, { label });
      await sessionQuery.refetch();
      await manifestQuery.refetch();
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to rename spreadsheet column"));
    }
  };

  const handleRunColumn = async (
    draft: ColumnRunScopeDraftState,
    confirmOverwrite = false,
  ) => {
    if (!selectedSessionId) return;
    setRunStarting(true);
    setActionError("");
    setActionMessage("");
    try {
      const response = await api.startSpreadsheetColumnRun(selectedSessionId, draft.columnId, {
        q: filters.q,
        scope: draft.scope,
        row_ids: draft.scope === "selected" ? Array.from(selectedRowIds) : [],
        confirm_overwrite: confirmOverwrite,
      });
      if (response.status === "confirmation_required") {
        const confirmed = window.confirm(
          response.message || `Overwrite ${response.populated_rows} populated cell(s)?`,
        );
        if (confirmed) {
          await handleRunColumn(draft, true);
        }
        return;
      }
      setColumnRunJobId(response.job_id);
      setRunDraft(null);
      setActionMessage(response.message || "Started spreadsheet column run.");
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to start spreadsheet column run"));
    } finally {
      setRunStarting(false);
    }
  };

  const commitCellEdit = async (cell: EditingCellState | null) => {
    if (!selectedSessionId || !cell) return;
    setActionError("");
    try {
      const updated = await api.patchSpreadsheetRow(selectedSessionId, cell.rowId, {
        [cell.columnId]: cell.value,
      });
      setEditingCell(null);
      await manifestQuery.refetch();
      if (activeRowId === cell.rowId) {
        setDetailDraft((prev) => ({ ...prev, [cell.columnId]: formatCellValue(updated[cell.columnId]) }));
      }
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to save spreadsheet cell"));
    }
  };

  const saveActiveRow = async () => {
    if (!selectedSessionId || !activeRowId) return;
    setDetailSaving(true);
    setActionError("");
    try {
      await api.patchSpreadsheetRow(selectedSessionId, activeRowId, detailDraft);
      await manifestQuery.refetch();
      setActionMessage("Saved row changes.");
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to save row changes"));
    } finally {
      setDetailSaving(false);
    }
  };

  const handleToggleRow = (rowId: string, checked: boolean) => {
    setSelectedRowIds((prev) => {
      const next = new Set(prev);
      if (checked) {
        next.add(rowId);
      } else {
        next.delete(rowId);
      }
      return next;
    });
  };

  const handleExport = async () => {
    if (!selectedSessionId) return;
    setExporting(true);
    setActionError("");
    try {
      const response = await api.exportSpreadsheetSession(selectedSessionId);
      downloadBlob(response.blob, response.filename);
      setActionMessage(`Exported ${response.filename}.`);
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to export spreadsheet"));
    } finally {
      setExporting(false);
    }
  };

  return (
    <div className="space-y-4">
      <SectionHeader
        title="Spreadsheets"
        description="Open CSV, XLSX, JSON, JSONL, NDJSON, Parquet, or SQLite tables in a spreadsheet-focused workspace with direct cell editing and LLM-assisted column runs."
        right={
          <div className="flex flex-wrap items-center gap-2">
            {activeColumnRun && (
              <StatusBadge
                text={`${activeColumnRun.column_label}: ${activeColumnRun.processed_rows}/${activeColumnRun.total_rows}`}
                tone={toneForRunState(activeColumnRun.state)}
              />
            )}
            <Button
              disabled={uploading}
              variant="primary"
              onClick={() => fileInputRef.current?.click()}
            >
              {uploading ? "Opening..." : "Open File"}
            </Button>
            <input
              ref={fileInputRef}
              accept=".csv,.xlsx,.json,.jsonl,.ndjson,.parquet,.sqlite,.db"
              className="hidden"
              type="file"
              onChange={(event) => void handleUploadSpreadsheet(event.target.files?.[0] || null)}
            />
          </div>
        }
      />

      {actionMessage && (
        <SurfaceCard className="border border-success/20 bg-success/10">
          <div className="text-body-md text-success">{actionMessage}</div>
        </SurfaceCard>
      )}
      {actionError && (
        <SurfaceCard className="border border-error/30 bg-error/10">
          <div className="text-body-md text-error">{actionError}</div>
        </SurfaceCard>
      )}

      <SurfaceCard>
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
          <div className="grid gap-4 md:grid-cols-2">
            <label className="grid gap-1 text-body-md">
              <span className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
                Open Session
              </span>
              <select
                className="rounded-md border border-outline-variant bg-surface-container-lowest px-3 py-2 text-body-md text-on-surface"
                value={selectedSessionId}
                onChange={handleSelectSession}
              >
                <option value="">Choose a session</option>
                {sessionSummaries.map((session) => (
                  <option key={session.session_id} value={session.session_id}>
                    {session.filename}
                  </option>
                ))}
              </select>
            </label>
            <label className="grid gap-1 text-body-md">
              <span className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
                Target
              </span>
              <select
                className="rounded-md border border-outline-variant bg-surface-container-lowest px-3 py-2 text-body-md text-on-surface"
                disabled={!sessionQuery.data}
                value={sessionQuery.data?.active_target?.id || ""}
                onChange={(event) => void handleTargetChange(event.target.value)}
              >
                {(sessionQuery.data?.targets || []).map((target) => (
                  <option key={target.id} value={target.id}>
                    {target.label}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <div className="rounded-lg border border-outline-variant/30 bg-surface-container-low p-4">
            {sessionQuery.data ? (
              <div className="space-y-2 text-body-md text-on-surface">
                <div className="font-semibold">{sessionQuery.data.session.filename}</div>
                <div className="text-on-surface-variant">
                  Format: {sessionQuery.data.session.source_format.toUpperCase()}
                </div>
                <div className="text-on-surface-variant">
                  Targets: {sessionQuery.data.session.target_count}
                </div>
                <div className="text-on-surface-variant">
                  Updated: {new Date(sessionQuery.data.session.updated_at).toLocaleString()}
                </div>
              </div>
            ) : (
              <div className="text-body-md text-on-surface-variant">
                Open a file to start a spreadsheet session.
              </div>
            )}
          </div>
        </div>
      </SurfaceCard>

      {!selectedSessionId || !sessionQuery.data ? (
        <SurfaceCard>
          <EmptyState
            title="No Spreadsheet Open"
            detail="Open a supported table file to edit rows, add custom columns, and run LLM prompts against selected input columns."
          />
        </SurfaceCard>
      ) : (
        <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_360px]">
          <div className="space-y-4">
            <SurfaceCard>
              <div className="flex flex-wrap items-end gap-3">
                <InputField
                  className="min-w-[260px] flex-1"
                  id="spreadsheet-search"
                  label="Search"
                  placeholder="Search all visible cell values"
                  value={filters.q}
                  onChange={(event) =>
                    setFilters((prev) => ({ ...prev, q: event.target.value, offset: 0 }))
                  }
                />
                <Button onClick={() => void handleCreateColumn()}>Add Custom Column</Button>
                <Button disabled={exporting} variant="primary" onClick={() => void handleExport()}>
                  {exporting ? "Exporting..." : "Export Original Format"}
                </Button>
              </div>
              <div className="mt-4 grid gap-2 md:grid-cols-2 xl:grid-cols-3">
                {columns.map((column) => {
                  const checked = visibleColumns.includes(column.id);
                  return (
                    <div
                      key={column.id}
                      className="rounded-md border border-outline-variant/20 bg-surface-container-low px-3 py-3"
                    >
                      <label className="flex items-center gap-2 text-body-md text-on-surface">
                        <input
                          checked={checked}
                          type="checkbox"
                          onChange={(event) =>
                            setVisibleColumns((prev) =>
                              event.target.checked
                                ? [...prev, column.id]
                                : prev.filter((value) => value !== column.id),
                            )
                          }
                        />
                        {column.label}
                      </label>
                      <div className="mt-2 flex items-center gap-2">
                        <label className="text-label-sm text-on-surface-variant">Width</label>
                        <input
                          className="w-24 rounded-md border border-outline-variant bg-surface-container-lowest px-2 py-1 text-body-md"
                          type="number"
                          value={columnWidths[column.id] || 180}
                          onChange={(event) =>
                            setColumnWidths((prev) => ({
                              ...prev,
                              [column.id]: clampSpreadsheetColumnWidth(Number(event.target.value)),
                            }))
                          }
                        />
                      </div>
                    </div>
                  );
                })}
              </div>
            </SurfaceCard>

            <SurfaceCard className="overflow-hidden p-0">
              <div className="overflow-auto">
                <table className="data-table min-w-full">
                  <colgroup>
                    <col style={{ width: "44px" }} />
                    {renderedColumns.map((column) => (
                      <col
                        key={column.id}
                        style={{ width: `${columnWidths[column.id] || 180}px` }}
                      />
                    ))}
                  </colgroup>
                  <thead>
                    <tr>
                      <th>
                        <input
                          checked={rows.length > 0 && rows.every((row) => selectedRowIds.has(String(row.id || "")))}
                          type="checkbox"
                          onChange={(event) =>
                            setSelectedRowIds(
                              event.target.checked
                                ? new Set(rows.map((row) => String(row.id || "")))
                                : new Set(),
                            )
                          }
                        />
                      </th>
                      {renderedColumns.map((column) => {
                        const sorting = filters.sortBy === column.id ? filters.sortDir : "";
                        return (
                          <th key={column.id} style={{ verticalAlign: "top" }}>
                            <button
                              className="w-full text-left"
                              onClick={() =>
                                setFilters((prev) => ({
                                  ...prev,
                                  ...nextSpreadsheetSort(prev.sortBy, prev.sortDir, column.id),
                                  offset: 0,
                                }))
                              }
                              type="button"
                            >
                              <div className="font-semibold text-on-surface">
                                {column.label}
                                {sorting === "asc" ? " ↑" : sorting === "desc" ? " ↓" : ""}
                              </div>
                            </button>
                            <div className="mt-2 flex flex-wrap gap-1">
                              <button
                                className="rounded-md bg-surface px-2 py-1 text-label-sm text-primary hover:underline"
                                onClick={() => handleOpenPrompt(column)}
                                type="button"
                              >
                                Prompt
                              </button>
                              <button
                                className="rounded-md bg-surface px-2 py-1 text-label-sm text-primary hover:underline"
                                onClick={() =>
                                  setRunDraft({
                                    columnId: column.id,
                                    label: column.label,
                                    scope: selectedRowIds.size > 0 ? "selected" : "empty_only",
                                  })
                                }
                                type="button"
                              >
                                Run
                              </button>
                              {column.kind === "custom" && (
                                <button
                                  className="rounded-md bg-surface px-2 py-1 text-label-sm text-primary hover:underline"
                                  onClick={() => void handleRenameColumn(column)}
                                  type="button"
                                >
                                  Rename
                                </button>
                              )}
                            </div>
                          </th>
                        );
                      })}
                    </tr>
                  </thead>
                  <tbody>
                    {rows.map((row) => {
                      const rowId = String(row.id || "");
                      return (
                        <tr
                          key={rowId}
                          className={activeRowId === rowId ? "repository-browser-row-active" : ""}
                          onClick={() => setActiveRowId(rowId)}
                        >
                          <td onClick={(event) => event.stopPropagation()}>
                            <input
                              checked={selectedRowIds.has(rowId)}
                              type="checkbox"
                              onChange={(event) => handleToggleRow(rowId, event.target.checked)}
                            />
                          </td>
                          {renderedColumns.map((column) => {
                            const isEditing =
                              editingCell?.rowId === rowId && editingCell?.columnId === column.id;
                            return (
                              <td
                                key={`${rowId}-${column.id}`}
                                className="cursor-text"
                                onDoubleClick={() =>
                                  setEditingCell({
                                    rowId,
                                    columnId: column.id,
                                    value: formatCellValue(row[column.id]),
                                  })
                                }
                              >
                                {isEditing ? (
                                  <input
                                    autoFocus
                                    className="w-full rounded-md border border-primary bg-surface-container-lowest px-2 py-1 text-body-md"
                                    value={editingCell.value}
                                    onBlur={() => void commitCellEdit(editingCell)}
                                    onChange={(event) =>
                                      setEditingCell((prev) =>
                                        prev ? { ...prev, value: event.target.value } : prev,
                                      )
                                    }
                                    onKeyDown={(event) => {
                                      if (event.key === "Enter") {
                                        event.preventDefault();
                                        void commitCellEdit(editingCell);
                                      }
                                      if (event.key === "Escape") {
                                        event.preventDefault();
                                        setEditingCell(null);
                                      }
                                    }}
                                  />
                                ) : (
                                  formatCellValue(row[column.id]) || "—"
                                )}
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
                  Showing {rows.length} rows out of {manifestQuery.data?.total || 0}
                </div>
                <div className="flex gap-2">
                  <Button
                    disabled={filters.offset <= 0}
                    onClick={() =>
                      setFilters((prev) => ({
                        ...prev,
                        offset: Math.max(0, prev.offset - SPREADSHEET_PAGE_SIZE),
                      }))
                    }
                  >
                    Prev
                  </Button>
                  <Button
                    disabled={filters.offset + SPREADSHEET_PAGE_SIZE >= (manifestQuery.data?.total || 0)}
                    onClick={() =>
                      setFilters((prev) => ({
                        ...prev,
                        offset: prev.offset + SPREADSHEET_PAGE_SIZE,
                      }))
                    }
                  >
                    Next
                  </Button>
                </div>
              </div>
            </SurfaceCard>
          </div>

          <SurfaceCard className="min-h-[480px]">
            {activeRow ? (
              <>
                <div className="flex items-center justify-between gap-2">
                  <div>
                    <div className="text-title-sm font-semibold">Row {activeRowId}</div>
                    <div className="mt-1 text-body-md text-on-surface-variant">
                      Edit the active row directly. Double-click any table cell for inline editing.
                    </div>
                  </div>
                  <Button disabled={detailSaving} variant="primary" onClick={() => void saveActiveRow()}>
                    {detailSaving ? "Saving..." : "Save Row"}
                  </Button>
                </div>

                <div className="mt-4 grid max-h-[70vh] gap-3 overflow-auto pr-1">
                  {columns.map((column) => (
                    <InputField
                      key={column.id}
                      label={column.label}
                      value={detailDraft[column.id] || ""}
                      onChange={(event) =>
                        setDetailDraft((prev) => ({ ...prev, [column.id]: event.target.value }))
                      }
                    />
                  ))}
                </div>
              </>
            ) : (
              <EmptyState
                title="Select A Row"
                detail="Choose a row to inspect and edit it in the side panel."
              />
            )}
          </SurfaceCard>
        </div>
      )}

      {promptDraft && (
        <ColumnPromptModal
          columns={columns}
          draft={promptDraft}
          error={promptError}
          fixingPrompt={promptFixing}
          llmReady={llmReady}
          savePending={promptSaving}
          onCancel={() => {
            if (promptSaving || promptFixing) return;
            setPromptDraft(null);
            setPromptError("");
          }}
          onChange={(patch) => setPromptDraft((prev) => (prev ? { ...prev, ...patch } : prev))}
          onFixPrompt={() => void handleFixPrompt()}
          onSave={() => void handleSavePrompt()}
        />
      )}

      {runDraft && (
        <ColumnRunScopeModal
          draft={runDraft}
          selectedCount={selectedRowIds.size}
          startPending={runStarting}
          onCancel={() => {
            if (runStarting) return;
            setRunDraft(null);
          }}
          onChangeScope={(scope) => setRunDraft((prev) => (prev ? { ...prev, scope } : prev))}
          onConfirm={() => void handleRunColumn(runDraft)}
        />
      )}
    </div>
  );
}
