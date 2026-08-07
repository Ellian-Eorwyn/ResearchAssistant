import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";

import { api } from "../api/client";
import type { CaptureSessionInfo, RepositoryCaptureResponse, ResolveSourceRow } from "../api/types";
import { Button, EmptyState, StatusBadge, SurfaceCard } from "../components/primitives";
import { RemoteBrowserCanvas } from "../components/RemoteBrowserCanvas";
import { labelFetchVerification, statusTone } from "./repositoryBrowserUtils";
import {
  RESOLVE_FILTER_INCLUDE,
  filterResolveRows,
  hostFromUrl,
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
  const [filter, setFilter] = useState<ResolveFilter>("blocked");
  const [query, setQuery] = useState("");
  const [activeId, setActiveId] = useState("");
  const [session, setSession] = useState<CaptureSessionInfo | null>(null);
  const [addressBar, setAddressBar] = useState("");
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [captureResult, setCaptureResult] = useState<RepositoryCaptureResponse | null>(null);
  const uploadInputRef = useRef<HTMLInputElement>(null);

  const listQuery = useQuery({
    queryKey: ["resolve-fetches", filter],
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

  const refreshList = () => {
    void queryClient.invalidateQueries({ queryKey: ["resolve-fetches"] });
    void queryClient.invalidateQueries({ queryKey: ["repository-manifest"] });
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
          ? "Running headless — some sites detect this. If the page refuses, use Upload a saved page."
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
    onSuccess: (result) => {
      setCaptureResult(result);
      setError("");
      refreshList();
    },
    onError: (mutationError) => setError(String(mutationError)),
  });

  const uploadMutation = useMutation({
    mutationFn: (file: File) =>
      api.manualUploadIntoSource(
        activeId,
        file,
        activeRow?.final_url || activeRow?.original_url || "",
      ),
    onSuccess: (result) => {
      setCaptureResult(result);
      setError("");
      refreshList();
    },
    onError: (mutationError) => setError(String(mutationError)),
  });

  const selectRow = (row: ResolveSourceRow) => {
    setActiveId(row.id);
    setCaptureResult(null);
    setNotice("");
    setError("");
  };

  const availability = availabilityQuery.data;
  const counts = listQuery.data;

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
            detail="Pick a blocked source on the left to open it in a browser you can drive."
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
                      text={session.headless ? "headless" : "headful"}
                      tone={session.headless ? "warning" : "success"}
                    />
                  )}
                  {!session ? (
                    <Button
                      disabled={
                        startSessionMutation.isPending || availability?.available === false
                      }
                      variant="primary"
                      onClick={() => startSessionMutation.mutate(activeRow)}
                    >
                      {startSessionMutation.isPending ? "Starting…" : "Open in browser"}
                    </Button>
                  ) : (
                    <Button
                      variant="secondary"
                      onClick={() => {
                        void api.closeCaptureSession(session.session_id);
                        setSession(null);
                      }}
                    >
                      Close browser
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
                    You can still upload a saved copy of the page below.
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
                    disabled={captureMutation.isPending}
                    variant="primary"
                    onClick={() => captureMutation.mutate()}
                  >
                    {captureMutation.isPending ? "Capturing…" : `Capture into ${activeRow.id}`}
                  </Button>
                </div>
              )}

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

            {session ? (
              <div className="min-h-0 flex-1 overflow-hidden rounded-lg ghost-border">
                <RemoteBrowserCanvas
                  sessionId={session.session_id}
                  viewportHeight={session.viewport_height}
                  viewportWidth={session.viewport_width}
                  onError={setError}
                  onUrlChange={setAddressBar}
                />
              </div>
            ) : (
              <SurfaceCard className="min-h-0 flex-1 overflow-y-auto">
                <div className="text-title-sm font-semibold text-on-surface">
                  What we currently have
                </div>
                <p className="mt-1 text-body-md text-on-surface-variant">
                  {activeRow.error_message || "No error was recorded for this source."}
                </p>
                <pre className="mt-3 whitespace-pre-wrap rounded-md bg-surface-container-low p-3 font-mono text-body-sm text-on-surface-variant">
                  {activeRow.current_content_preview || "(nothing was stored)"}
                </pre>

                <div className="mt-4 border-t border-outline-variant/30 pt-4">
                  <div className="text-title-sm font-semibold text-on-surface">
                    Upload a saved page
                  </div>
                  <p className="mt-1 text-body-md text-on-surface-variant">
                    If the site cannot be worked past in the browser, save it yourself
                    (File → Save Page As, or print to PDF) and upload it here. It is written
                    into source {activeRow.id} in place. Complete-page saves leave a{" "}
                    <code>_files/</code> folder that is ignored, and <code>.mhtml</code>{" "}
                    archives are not supported.
                  </p>
                  <input
                    ref={uploadInputRef}
                    accept=".html,.htm,.xhtml,.pdf,.md,.markdown,.txt"
                    className="hidden"
                    type="file"
                    onChange={(event) => {
                      const file = event.target.files?.[0];
                      if (file) uploadMutation.mutate(file);
                      event.target.value = "";
                    }}
                  />
                  <Button
                    className="mt-3"
                    disabled={uploadMutation.isPending}
                    variant="secondary"
                    onClick={() => uploadInputRef.current?.click()}
                  >
                    {uploadMutation.isPending ? "Uploading…" : "Choose a file"}
                  </Button>
                </div>
              </SurfaceCard>
            )}
          </>
        )}
      </section>
    </div>
  );
}
