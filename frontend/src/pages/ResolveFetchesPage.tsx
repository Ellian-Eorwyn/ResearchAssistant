import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState } from "react";

import { api } from "../api/client";
import type { CaptureSessionInfo, RepositoryCaptureResponse, ResolveSourceRow } from "../api/types";
import { AttachPanel } from "../components/AttachPanel";
import { Button, EmptyState, StatusBadge, SurfaceCard } from "../components/primitives";
import { RemoteBrowserCanvas } from "../components/RemoteBrowserCanvas";
import { useAppState } from "../state/AppState";
import { labelFetchVerification, statusTone } from "./repositoryBrowserUtils";
import {
  RESOLVE_FILTER_INCLUDE,
  buildResolveProcessingQueue,
  filterResolveRows,
  hostFromUrl,
  nextUnresolvedId,
  type ResolveFilter,
} from "./resolveFetchesUtils";

const FILTERS: Array<{ id: ResolveFilter; label: string }> = [
  { id: "blocked", label: "Blocked" },
  { id: "failed", label: "Failed" },
  { id: "partial", label: "Partial" },
  { id: "all", label: "All" },
];

export function ResolveFetchesPage() {
  const queryClient = useQueryClient();
  const {
    repositoryStatus,
    settingsDraft,
    sourceTaskDraft,
    sourceRunning,
    sourceStatus,
    sourceTaskQueueActiveLabel,
    sourceTaskQueueCompletedCount,
    sourceTaskQueueTotalCount,
    startSourceTaskQueue,
    cancelSourceTasks,
  } = useAppState();

  const [filter, setFilter] = useState<ResolveFilter>("blocked");
  const [query, setQuery] = useState("");
  const [activeId, setActiveId] = useState("");
  const [session, setSession] = useState<CaptureSessionInfo | null>(null);
  const [addressBar, setAddressBar] = useState("");
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [captureResult, setCaptureResult] = useState<RepositoryCaptureResponse | null>(null);
  const [sinceMs, setSinceMs] = useState(0);
  const [autoAdvance, setAutoAdvance] = useState(true);
  const [overwriteExisting, setOverwriteExisting] = useState(false);
  const [pendingIds, setPendingIds] = useState<string[]>([]);
  const [pendingTitles, setPendingTitles] = useState<Record<string, string>>({});

  const listQuery = useQuery({
    queryKey: ["resolve-fetches"],
    queryFn: () => api.listBlockedSources(RESOLVE_FILTER_INCLUDE.all),
  });
  const availabilityQuery = useQuery({
    queryKey: ["capture-availability"],
    queryFn: () => api.getCaptureAvailability(),
    staleTime: 60_000,
  });

  const rows = useMemo(
    () => filterResolveRows(listQuery.data?.rows ?? [], filter, query),
    [listQuery.data, filter, query],
  );
  const activeRow = useMemo(
    () => rows.find((row) => row.id === activeId) ?? null,
    [rows, activeId],
  );

  // Close the browser when the page unmounts so a headful Chromium is never
  // left running invisibly on the server.
  useEffect(() => {
    return () => {
      if (session) void api.closeCaptureSession(session.session_id);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [session?.session_id]);

  // Survive a refresh: the tray is session intent, but losing it mid-pass and
  // silently skipping the processing would be worse than a little persistence.
  const trayKey = `ra:resolve-pending:${repositoryStatus?.path ?? ""}`;
  useEffect(() => {
    try {
      const stored = window.sessionStorage.getItem(trayKey);
      if (stored) {
        const parsed = JSON.parse(stored) as { ids: string[]; titles: Record<string, string> };
        setPendingIds(parsed.ids ?? []);
        setPendingTitles(parsed.titles ?? {});
      }
    } catch {
      // A malformed entry is not worth surfacing; start with an empty tray.
    }
  }, [trayKey]);
  useEffect(() => {
    try {
      window.sessionStorage.setItem(
        trayKey,
        JSON.stringify({ ids: pendingIds, titles: pendingTitles }),
      );
    } catch {
      // Private-mode storage failures must not break attaching.
    }
  }, [trayKey, pendingIds, pendingTitles]);

  const refreshList = () => {
    void queryClient.invalidateQueries({ queryKey: ["resolve-fetches"] });
    void queryClient.invalidateQueries({ queryKey: ["repository-manifest"] });
  };

  const startProcessing = () => {
    if (sourceRunning || pendingIds.length === 0) return;
    const tasks = buildResolveProcessingQueue({
      sourceIds: pendingIds,
      draft: sourceTaskDraft,
      defaultProjectProfileName: settingsDraft?.default_project_profile_name ?? "",
      overwriteExisting,
    });
    if (tasks.length === 0) return;
    void startSourceTaskQueue(
      tasks.map((task) => ({ label: task.label, payload: task.payload })),
      `Processing ${pendingIds.length} resolved source(s).`,
    );
    setPendingIds([]);
    setPendingTitles({});
  };

  // The in-app browser is keyed on the capture session, not the source, so
  // changing sources never moves it on its own. When a session is open, point it
  // at the given source's page so its live view — and the next Capture — are the
  // right page, sparing a close/re-open just to load the URL.
  const loadSessionToRow = (row?: ResolveSourceRow) => {
    const url = row?.final_url || row?.original_url || "";
    if (session && url) {
      navigateMutation.mutate({ url, action: "goto" });
    }
  };

  const handleResolved = (result: RepositoryCaptureResponse) => {
    setCaptureResult(result);
    setError("");
    if (result.status !== "captured") {
      // Still a wall. Stay on this source — advancing would hide the warning.
      refreshList();
      return;
    }

    const resolvedId = result.source_id || activeId;
    setPendingIds((prev) => (prev.includes(resolvedId) ? prev : [...prev, resolvedId]));
    setPendingTitles((prev) => ({
      ...prev,
      [resolvedId]: result.title || activeRow?.title || resolvedId,
    }));

    // Work out the next source from the list as it is *now*, before the
    // refetch drops the row we just resolved and leaves no anchor to count from.
    const nextId = autoAdvance
      ? nextUnresolvedId(rows, resolvedId, [...pendingIds, resolvedId])
      : "";
    const nextRow = nextId ? rows.find((row) => row.id === nextId) : undefined;
    refreshList();
    if (nextId) {
      setActiveId(nextId);
      setCaptureResult(null);
      setSinceMs(0);
      // Move the open browser onto the new source so the next Capture targets it.
      loadSessionToRow(nextRow);
    }
  };

  const reverifyMutation = useMutation({
    mutationFn: () => api.reverifyFetches({ scope: "all", force: true }),
    onSuccess: (result) => {
      setNotice(result.message);
      setError("");
      refreshList();
    },
    onError: (mutationError) => setError(String(mutationError)),
  });

  const startSessionMutation = useMutation({
    mutationFn: (row: ResolveSourceRow) =>
      api.createCaptureSession({
        source_id: row.id,
        url: row.final_url || row.original_url,
      }),
    onSuccess: (info) => {
      setSession(info);
      setAddressBar(info.current_url);
      setCaptureResult(null);
      setError("");
      setNotice(
        info.headless
          ? "Running headless — some sites detect this. Open the page in your own browser instead and attach the file."
          : "",
      );
    },
    onError: (mutationError) => setError(String(mutationError)),
  });

  const navigateMutation = useMutation({
    mutationFn: (payload: { url?: string; action?: "goto" | "back" | "forward" | "reload" }) =>
      api.navigateCaptureSession(session!.session_id, payload),
    onSuccess: (info) => {
      setSession(info);
      setAddressBar(info.current_url);
      setError("");
    },
    onError: (mutationError) => setError(String(mutationError)),
  });

  const captureMutation = useMutation({
    mutationFn: () =>
      api.captureSessionIntoSource(session!.session_id, {
        source_id: activeId,
        include_raw_html: true,
        include_rendered_html: true,
        include_rendered_pdf: true,
        include_markdown: true,
      }),
    onSuccess: handleResolved,
    onError: (mutationError) => setError(String(mutationError)),
  });

  const uploadMutation = useMutation({
    mutationFn: (file: File) =>
      api.manualUploadIntoSource(
        activeId,
        file,
        activeRow?.final_url || activeRow?.original_url || "",
      ),
    onSuccess: handleResolved,
    onError: (mutationError) => setError(String(mutationError)),
  });

  const attachPathMutation = useMutation({
    mutationFn: (path: string) =>
      api.attachWatchedFile(
        activeId,
        path,
        activeRow?.final_url || activeRow?.original_url || "",
      ),
    onSuccess: handleResolved,
    onError: (mutationError) => setError(String(mutationError)),
  });

  const selectRow = (row: ResolveSourceRow) => {
    if (row.id !== activeId) loadSessionToRow(row);
    setActiveId(row.id);
    setCaptureResult(null);
    setNotice("");
    setError("");
    setSinceMs(0);
  };

  const openInOwnBrowser = (row: ResolveSourceRow) => {
    // Arm the watch folder first: everything that lands from here on is almost
    // certainly what the user just saved, and gets flagged as such.
    setSinceMs(Date.now());
    window.open(row.original_url, "_blank", "noopener,noreferrer");
  };

  const availability = availabilityQuery.data;
  const counts = listQuery.data;
  const attaching =
    uploadMutation.isPending || attachPathMutation.isPending || captureMutation.isPending;

  return (
    <div className="flex h-full min-h-0 gap-4">
      <aside className="flex w-96 min-w-[20rem] flex-col gap-3 overflow-hidden">
        <SurfaceCard className="flex flex-col gap-3">
          <div>
            <h1 className="text-title-md font-semibold text-on-surface">Resolve Fetches</h1>
            <p className="mt-1 text-body-md text-on-surface-variant">
              Sources where a bot wall, sign-in or error page was stored instead of the
              real document.
            </p>
          </div>
          <div className="flex flex-wrap gap-1">
            {FILTERS.map((entry) => {
              const count =
                entry.id === "blocked"
                  ? counts?.blocked_count
                  : entry.id === "failed"
                    ? counts?.failed_count
                    : entry.id === "partial"
                      ? counts?.partial_count
                      : counts?.total;
              return (
                <button
                  key={entry.id}
                  className={
                    filter === entry.id
                      ? "rounded-md bg-surface-container-highest px-3 py-1 text-body-sm font-semibold text-primary"
                      : "rounded-md px-3 py-1 text-body-sm text-on-surface-variant hover:bg-surface-variant"
                  }
                  type="button"
                  onClick={() => setFilter(entry.id)}
                >
                  {entry.label}
                  {count === undefined ? "" : ` (${count})`}
                </button>
              );
            })}
          </div>
          <input
            className="w-full rounded-md bg-surface-variant px-3 py-2 text-body-md text-on-surface outline-none"
            placeholder="Filter by id, title or URL"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
          <Button
            disabled={reverifyMutation.isPending}
            variant="secondary"
            onClick={() => reverifyMutation.mutate()}
          >
            {reverifyMutation.isPending ? "Re-checking…" : "Re-check all fetches"}
          </Button>
        </SurfaceCard>

        <div className="flex-1 overflow-y-auto pr-1">
          {listQuery.isLoading ? (
            <div className="p-4 text-body-md text-on-surface-variant">Loading…</div>
          ) : rows.length === 0 ? (
            <EmptyState
              title="Nothing to resolve"
              detail="Every source in this repository fetched real content."
            />
          ) : (
            <ul className="flex flex-col gap-2">
              {rows.map((row) => (
                <li key={row.id}>
                  <button
                    className={
                      activeId === row.id
                        ? "w-full rounded-lg bg-surface-container-highest p-3 text-left ghost-border"
                        : "w-full rounded-lg bg-surface-container p-3 text-left ghost-border hover:bg-surface-container-high"
                    }
                    type="button"
                    onClick={() => selectRow(row)}
                    onDragOver={(event) => event.preventDefault()}
                    onDrop={(event) => {
                      event.preventDefault();
                      if (sourceRunning) return;
                      const file = event.dataTransfer.files?.[0];
                      if (!file) return;
                      // Dropping onto a row means "this one", even if another
                      // is currently selected.
                      setActiveId(row.id);
                      uploadMutation.mutate(file);
                    }}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <span className="font-mono text-body-sm text-on-surface-variant">
                        {row.id}
                      </span>
                      <StatusBadge text={row.fetch_status} tone={statusTone(row.fetch_status)} />
                    </div>
                    <div className="mt-1 truncate text-body-md font-semibold text-on-surface">
                      {row.title || hostFromUrl(row.original_url)}
                    </div>
                    <div className="mt-1 truncate text-body-sm text-on-surface-variant">
                      {hostFromUrl(row.original_url)}
                      {row.http_status ? ` · HTTP ${row.http_status}` : ""}
                      {row.markdown_char_count ? ` · ${row.markdown_char_count} chars` : ""}
                    </div>
                    {row.fetch_verification && row.fetch_verification !== "ok" && (
                      <div className="mt-2">
                        <StatusBadge
                          text={labelFetchVerification(row.fetch_verification)}
                          tone={statusTone(row.fetch_verification)}
                        />
                      </div>
                    )}
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      </aside>

      <section className="flex min-h-0 flex-1 flex-col gap-3">
        {!activeRow ? (
          <EmptyState
            title="Select a source"
            detail="Pick a blocked source on the left, then open it in your own browser or the in-app one."
          />
        ) : (
          <>
            <SurfaceCard className="flex flex-col gap-3">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="min-w-0">
                  <div className="truncate text-title-sm font-semibold text-on-surface">
                    {activeRow.id} · {activeRow.title || hostFromUrl(activeRow.original_url)}
                  </div>
                  <a
                    className="text-body-sm text-primary hover:underline"
                    href={activeRow.original_url}
                    rel="noreferrer"
                    target="_blank"
                  >
                    {activeRow.original_url}
                  </a>
                </div>
                <div className="flex items-center gap-2">
                  {session && (
                    <StatusBadge
                      text={
                        session.headless
                          ? "headless"
                          : `headful${session.channel ? ` · ${session.channel}` : ""}`
                      }
                      tone={session.headless ? "warning" : "success"}
                    />
                  )}
                  <Button variant="primary" onClick={() => openInOwnBrowser(activeRow)}>
                    Open in my browser
                  </Button>
                  {!session ? (
                    <Button
                      disabled={
                        startSessionMutation.isPending || availability?.available === false
                      }
                      variant="secondary"
                      onClick={() => startSessionMutation.mutate(activeRow)}
                    >
                      {startSessionMutation.isPending ? "Starting…" : "Open in-app browser"}
                    </Button>
                  ) : (
                    <Button
                      variant="secondary"
                      onClick={() => {
                        void api.closeCaptureSession(session.session_id);
                        setSession(null);
                      }}
                    >
                      Close in-app browser
                    </Button>
                  )}
                </div>
              </div>

              {availability && !availability.available && (
                <div className="rounded-md bg-error/10 p-3 text-body-md text-error">
                  The in-app browser is unavailable: {availability.error}
                  {availability.guidance && (
                    <div className="mt-1 font-mono text-body-sm">{availability.guidance}</div>
                  )}
                  <div className="mt-1 text-on-surface-variant">
                    Open the page in your own browser and attach the saved file instead.
                  </div>
                </div>
              )}

              {session && (
                <div className="flex flex-wrap items-center gap-2">
                  <Button
                    variant="secondary"
                    onClick={() => navigateMutation.mutate({ action: "back" })}
                  >
                    ←
                  </Button>
                  <Button
                    variant="secondary"
                    onClick={() => navigateMutation.mutate({ action: "forward" })}
                  >
                    →
                  </Button>
                  <Button
                    variant="secondary"
                    onClick={() => navigateMutation.mutate({ action: "reload" })}
                  >
                    ⟳
                  </Button>
                  <Button variant="secondary" onClick={() => loadSessionToRow(activeRow)}>
                    Load source page
                  </Button>
                  <input
                    className="min-w-[16rem] flex-1 rounded-md bg-surface-variant px-3 py-2 font-mono text-body-sm text-on-surface outline-none"
                    value={addressBar}
                    onChange={(event) => setAddressBar(event.target.value)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter") {
                        navigateMutation.mutate({ url: addressBar, action: "goto" });
                      }
                    }}
                  />
                  <Button
                    disabled={captureMutation.isPending || sourceRunning}
                    variant="primary"
                    onClick={() => captureMutation.mutate()}
                  >
                    {captureMutation.isPending ? "Capturing…" : `Capture into ${activeRow.id}`}
                  </Button>
                </div>
              )}

              <label className="flex items-center gap-2 text-body-sm text-on-surface-variant">
                <input
                  checked={autoAdvance}
                  type="checkbox"
                  onChange={(event) => setAutoAdvance(event.target.checked)}
                />
                Advance to the next source after a successful attach
              </label>

              {error && (
                <div className="rounded-md bg-error/10 p-3 text-body-md text-error">{error}</div>
              )}
              {notice && !error && (
                <div className="rounded-md bg-surface-variant p-3 text-body-md text-on-surface-variant">
                  {notice}
                </div>
              )}
              {captureResult && (
                <div
                  className={
                    captureResult.status === "captured"
                      ? "rounded-md bg-success/10 p-3 text-body-md text-success"
                      : "rounded-md bg-warning/10 p-3 text-body-md text-warning"
                  }
                >
                  {captureResult.message}
                  {captureResult.written_files.length > 0 && (
                    <ul className="mt-2 font-mono text-body-sm">
                      {captureResult.written_files.map((path) => (
                        <li key={path}>{path}</li>
                      ))}
                    </ul>
                  )}
                </div>
              )}
            </SurfaceCard>

            <div className="grid min-h-0 flex-1 gap-3 lg:grid-cols-[minmax(0,1fr)_24rem]">
              <div className="min-h-0 overflow-hidden">
                {session ? (
                  <div className="h-full overflow-hidden rounded-lg ghost-border">
                    <RemoteBrowserCanvas
                      sessionId={session.session_id}
                      viewportHeight={session.viewport_height}
                      viewportWidth={session.viewport_width}
                      onError={setError}
                      onUrlChange={setAddressBar}
                    />
                  </div>
                ) : (
                  <SurfaceCard className="h-full overflow-y-auto">
                    <div className="text-title-sm font-semibold text-on-surface">
                      What we currently have
                    </div>
                    <p className="mt-1 text-body-md text-on-surface-variant">
                      {activeRow.error_message || "No error was recorded for this source."}
                    </p>
                    <pre className="mt-3 whitespace-pre-wrap rounded-md bg-surface-container-low p-3 font-mono text-body-sm text-on-surface-variant">
                      {activeRow.current_content_preview || "(nothing was stored)"}
                    </pre>
                  </SurfaceCard>
                )}
              </div>

              <div className="flex min-h-0 flex-col gap-3 overflow-y-auto">
                <AttachPanel
                  busy={attaching}
                  disabled={sourceRunning}
                  finalUrl={activeRow.final_url || activeRow.original_url}
                  sinceMs={sinceMs}
                  sourceId={activeRow.id}
                  onAttachFile={(file) => uploadMutation.mutate(file)}
                  onAttachPath={(path) => attachPathMutation.mutate(path)}
                />

                <SurfaceCard className="flex flex-col gap-2">
                  <div className="text-title-sm font-semibold text-on-surface">
                    Resolved, waiting to process
                    {pendingIds.length > 0 ? ` (${pendingIds.length})` : ""}
                  </div>
                  {pendingIds.length === 0 ? (
                    <p className="text-body-md text-on-surface-variant">
                      Attached sources collect here. Only one repository job can run at a
                      time, so they are processed together rather than one at a time.
                    </p>
                  ) : (
                    <ul className="max-h-40 overflow-y-auto text-body-sm text-on-surface-variant">
                      {pendingIds.map((id) => (
                        <li key={id} className="truncate">
                          <span className="font-mono">{id}</span> · {pendingTitles[id] || ""}
                        </li>
                      ))}
                    </ul>
                  )}

                  <label className="flex items-center gap-2 text-body-sm text-on-surface-variant">
                    <input
                      checked={overwriteExisting}
                      type="checkbox"
                      onChange={(event) => setOverwriteExisting(event.target.checked)}
                    />
                    Overwrite existing catalog, summary and rating
                  </label>

                  {sourceRunning ? (
                    <div className="flex flex-col gap-2">
                      <div className="text-body-sm text-on-surface-variant">
                        {sourceTaskQueueTotalCount > 0
                          ? `${sourceTaskQueueCompletedCount}/${sourceTaskQueueTotalCount} · `
                          : ""}
                        {sourceTaskQueueActiveLabel || "Running"}
                        {sourceStatus
                          ? ` · ${sourceStatus.processed_urls}/${sourceStatus.total_urls}`
                          : ""}
                      </div>
                      <Button variant="secondary" onClick={() => void cancelSourceTasks()}>
                        Stop
                      </Button>
                    </div>
                  ) : (
                    <Button
                      disabled={pendingIds.length === 0}
                      variant="primary"
                      onClick={startProcessing}
                    >
                      {pendingIds.length === 0
                        ? "Nothing to process"
                        : `Process ${pendingIds.length} resolved source(s)`}
                    </Button>
                  )}
                </SurfaceCard>
              </div>
            </div>
          </>
        )}
      </section>
    </div>
  );
}
