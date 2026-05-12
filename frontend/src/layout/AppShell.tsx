import { useCallback, useEffect, useMemo, useState } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";

import { api } from "../api/client";
import type { LLMCallLogEntry, LLMCallLogSummary } from "../api/types";
import { Button, StatusBadge } from "../components/primitives";
import { useAppState } from "../state/AppState";

interface NavEntry {
  label: string;
  to: string;
}

export const PRIMARY_NAV: NavEntry[] = [
  { label: "Browser", to: "/browser" },
  { label: "Search", to: "/search" },
  { label: "Spreadsheets", to: "/spreadsheets" },
  { label: "AI Guidance", to: "/ai-guidance" },
  { label: "Settings", to: "/settings" },
];

export const LEGACY_NAV_GROUPS: Array<{ label: string; items: NavEntry[] }> = [
  {
    label: "Legacy Project",
    items: [{ label: "Overview", to: "/project/overview" }],
  },
  {
    label: "Legacy Processing",
    items: [
      { label: "Citation Extraction", to: "/processing/citation-extraction" },
      { label: "Job History", to: "/processing/job-history" },
    ],
  },
  {
    label: "Legacy Data",
    items: [
      { label: "Citations", to: "/data/citations" },
      { label: "Bibliography", to: "/data/bibliography" },
      { label: "Contexts", to: "/data/sentences" },
      { label: "Matches", to: "/data/matches" },
    ],
  },
  {
    label: "Legacy Settings",
    items: [
      { label: "Ingestion Profiles", to: "/settings/ingestion-profiles" },
      { label: "Advanced", to: "/settings/advanced" },
    ],
  },
];

function statusTone(state: string | undefined): "neutral" | "active" | "warning" | "error" {
  if (state === "running") return "active";
  if (state === "cancelling") return "warning";
  if (state === "failed") return "error";
  if (state === "completed") return "neutral";
  return "neutral";
}

function formatTokens(value: number | null | undefined): string {
  if (!value) return "0";
  if (value >= 1000) return `${(value / 1000).toFixed(value >= 10000 ? 0 : 1)}k`;
  return String(value);
}

function formatLocalDate(value: string): string {
  if (!value) return "In progress";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function promptLabel(call: LLMCallLogEntry): string {
  const promptTokens = call.prompt_tokens ?? call.estimated_context_tokens;
  const source = call.prompt_tokens == null ? "est." : "reported";
  return `${formatTokens(promptTokens)} context (${source})`;
}

export function AppShell() {
  const navigate = useNavigate();
  const location = useLocation();
  const {
    repositoryStatus,
    dashboard,
    lastRepositoryPath,
    openRepository,
    createRepository,
    pickRepositoryDirectory,
    switchRepository,
    processingRunning,
    sourceRunning,
    sourceStopping,
    sourceTaskJobId,
    sourceTaskQueueActiveLabel,
    sourceTaskQueueCompletedCount,
    sourceTaskQueueTotalCount,
    processingStatus,
    sourceStatus,
    cancelSourceTasks,
  } = useAppState();

  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [legacyOpen, setLegacyOpen] = useState(false);
  const [llmLogOpen, setLlmLogOpen] = useState(false);
  const [llmLog, setLlmLog] = useState<LLMCallLogSummary | null>(null);
  const [llmLogError, setLlmLogError] = useState("");

  const repoName = useMemo(() => {
    if (!repositoryStatus?.path) return "No Repository";
    const chunks = repositoryStatus.path.split("/").filter(Boolean);
    return chunks[chunks.length - 1] || repositoryStatus.path;
  }, [repositoryStatus?.path]);

  const jobLabel = useMemo(() => {
    if (sourceRunning) {
      if (!sourceStatus) {
        return sourceStopping ? "Repository Processing stopping" : "Repository Processing running";
      }
      const processed = sourceStatus?.processed_urls || 0;
      const total = sourceStatus?.total_urls || 0;
      const queuePrefix =
        sourceTaskQueueTotalCount > 0 && sourceTaskQueueActiveLabel
          ? `Task ${Math.min(sourceTaskQueueCompletedCount + 1, sourceTaskQueueTotalCount)}/${sourceTaskQueueTotalCount}: ${sourceTaskQueueActiveLabel} · `
          : "";
      return sourceStopping
        ? `${queuePrefix}Repository Processing stopping ${processed}/${total}`
        : `${queuePrefix}Repository Processing ${processed}/${total}`;
    }
    if (processingRunning) {
      const preprocessState = String(
        processingStatus?.repository_preprocess_state || "",
      ).toLowerCase();
      if (preprocessState === "pending" || preprocessState === "running") {
        return (
          processingStatus?.repository_preprocess_message ||
          "Legacy Citation Extraction preprocessing"
        );
      }
      const finalizeState = String(
        processingStatus?.repository_finalize_state || "",
      ).toLowerCase();
      if (finalizeState === "pending" || finalizeState === "running") {
        return (
          processingStatus?.repository_finalize_message ||
          "Legacy Citation Extraction finalizing"
        );
      }
      const pct = Math.round(processingStatus?.progress_pct || 0);
      return `Legacy Citation Extraction ${pct}%`;
    }
    return "Idle";
  }, [
    processingRunning,
    processingStatus?.progress_pct,
    processingStatus?.repository_finalize_message,
    processingStatus?.repository_finalize_state,
    processingStatus?.repository_preprocess_message,
    processingStatus?.repository_preprocess_state,
    sourceRunning,
    sourceStatus?.processed_urls,
    sourceStatus?.total_urls,
    sourceStopping,
    sourceTaskQueueActiveLabel,
    sourceTaskQueueCompletedCount,
    sourceTaskQueueTotalCount,
  ]);

  const repoState = repositoryStatus?.download_state || "idle";
  const isBrowserRoute = location.pathname === "/browser";

  const refreshLlmLog = useCallback(async () => {
    try {
      setLlmLog(await api.getLlmCallLog(20));
      setLlmLogError("");
    } catch (error) {
      setLlmLogError(error instanceof Error ? error.message : "Could not load LLM log");
    }
  }, []);

  useEffect(() => {
    void refreshLlmLog();
    const timer = window.setInterval(() => void refreshLlmLog(), 5000);
    return () => window.clearInterval(timer);
  }, [refreshLlmLog]);

  const handleOpenProject = useCallback(async () => {
    const seedPath = repositoryStatus?.path || lastRepositoryPath;
    const selectedPath = await pickRepositoryDirectory("open", seedPath);
    if (!selectedPath) return;
    const opened = await openRepository(selectedPath);
    if (opened) {
      navigate("/browser");
    }
  }, [
    lastRepositoryPath,
    navigate,
    openRepository,
    pickRepositoryDirectory,
    repositoryStatus?.path,
  ]);

  const handleCreateProject = useCallback(async () => {
    const seedPath = repositoryStatus?.path || lastRepositoryPath;
    const selectedPath = await pickRepositoryDirectory("create", seedPath);
    if (!selectedPath) return;
    const created = await createRepository(selectedPath);
    if (created) {
      navigate("/browser");
    }
  }, [
    createRepository,
    lastRepositoryPath,
    navigate,
    pickRepositoryDirectory,
    repositoryStatus?.path,
  ]);

  const handleSwitchRepository = useCallback(() => {
    switchRepository();
    navigate("/");
  }, [navigate, switchRepository]);

  return (
    <div className="flex h-screen overflow-hidden flex-col bg-surface">
      <header className="sticky top-0 z-40 flex h-14 items-center justify-between border-b border-outline-variant/30 bg-surface px-4 md:px-6">
        <div className="flex items-center gap-4">
          <button
            className="rounded-md border border-outline-variant/40 px-2 py-1 text-body-md md:hidden"
            onClick={() => setSidebarOpen((prev) => !prev)}
            type="button"
          >
            Menu
          </button>
          <button className="text-xl font-bold tracking-tight" onClick={() => navigate("/browser")} type="button">
            ResearchAssistant
          </button>
          <div className="hidden items-center gap-2 lg:flex">
            <Button variant="ghost" onClick={() => void handleCreateProject()}>New Project</Button>
            <Button variant="ghost" onClick={() => void handleOpenProject()}>Open Project</Button>
            <Button variant="ghost" onClick={() => navigate("/processing/job-history")}>Recent</Button>
            <Button variant="ghost" onClick={() => navigate("/settings/advanced")}>Help</Button>
          </div>
          <div className="hidden items-center gap-3 md:flex">
            <StatusBadge
              text={repoState}
              tone={statusTone(repoState)}
            />
            <div className="max-w-[460px] truncate font-mono text-label-sm text-on-surface-variant" title={repositoryStatus?.path || ""}>
              {repositoryStatus?.path || "No repository loaded"}
            </div>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Button variant="secondary" onClick={() => navigate("/browser")}>Home</Button>
          <Button variant="secondary" onClick={handleSwitchRepository}>Switch Repo</Button>
        </div>
      </header>

      <div className="flex min-h-0 flex-1">
        <aside
          className={`
            thin-scrollbar z-30 flex h-[calc(100vh-3.5rem)] w-72 shrink-0 flex-col overflow-y-auto border-r border-outline-variant/20 bg-surface-container-low p-4
            ${sidebarOpen ? "fixed left-0 top-14" : "hidden"}
            md:static md:flex
          `}
        >
          <div className="mb-4 rounded-lg bg-surface-container p-3 ghost-border">
            <div className="text-title-sm font-bold">{repoName}</div>
            <div className="mt-1 truncate font-mono text-label-sm text-on-surface-variant" title={repositoryStatus?.path || ""}>
              {repositoryStatus?.path || "No active path"}
            </div>
          </div>

          <nav className="space-y-4 pb-6">
            <section>
              <div className="px-2 pb-2 text-label-sm uppercase tracking-[0.09em] text-on-surface-variant/60">
                Workspace
              </div>
              <div className="space-y-1">
                {PRIMARY_NAV.map((item) => (
                  <NavLink
                    key={item.to}
                    className={({ isActive }) =>
                      [
                        "block rounded-md px-3 py-2 text-body-md transition",
                        isActive
                          ? "bg-surface-container-highest text-primary"
                          : "text-on-surface-variant hover:bg-surface-container hover:text-on-surface",
                      ].join(" ")
                    }
                    onClick={() => setSidebarOpen(false)}
                    to={item.to}
                  >
                    {item.label}
                  </NavLink>
                ))}
              </div>
            </section>

            <section>
              <button
                className="flex w-full items-center justify-between rounded-md px-2 py-2 text-left text-label-sm uppercase tracking-[0.09em] text-on-surface-variant/70 hover:bg-surface-container"
                onClick={() => setLegacyOpen((prev) => !prev)}
                type="button"
              >
                <span>Legacy</span>
                <span>{legacyOpen ? "−" : "+"}</span>
              </button>
              {legacyOpen && (
                <div className="mt-2 space-y-4">
                  {LEGACY_NAV_GROUPS.map((group) => (
                    <section key={group.label}>
                      <div className="px-2 pb-1 text-label-sm uppercase tracking-[0.09em] text-on-surface-variant/50">
                        {group.label}
                      </div>
                      <div className="space-y-1">
                        {group.items.map((item) => (
                          <NavLink
                            key={item.to}
                            className={({ isActive }) =>
                              [
                                "block rounded-md px-3 py-2 text-body-md transition",
                                isActive
                                  ? "bg-surface-container-highest text-primary"
                                  : "text-on-surface-variant hover:bg-surface-container hover:text-on-surface",
                              ].join(" ")
                            }
                            onClick={() => setSidebarOpen(false)}
                            to={item.to}
                          >
                            {item.label}
                          </NavLink>
                        ))}
                      </div>
                    </section>
                  ))}
                </div>
              )}
            </section>
          </nav>
        </aside>

        <main
          className={[
            "thin-scrollbar flex min-h-0 flex-1 flex-col bg-surface-container-lowest p-4 pb-24 md:p-6 md:pb-24",
            isBrowserRoute ? "overflow-hidden" : "overflow-y-auto",
          ].join(" ")}
        >
          <div className="mb-4 font-mono text-label-sm uppercase tracking-[0.08em] text-on-surface-variant/70">
            {location.pathname.replace(/^\//, "").replace(/\//g, " > ") || "browser"}
          </div>
          <div className="flex h-full min-h-0 flex-1 flex-col overflow-hidden">
            <Outlet />
          </div>
        </main>
      </div>

      <footer className="fixed bottom-0 right-0 z-40 flex w-full items-center justify-between border-t border-outline-variant/30 bg-surface-container/90 px-4 py-2 backdrop-blur md:w-[540px] md:rounded-tl-lg md:border-l">
        <div className="flex items-center gap-3">
          <StatusBadge
            text={sourceStopping ? "stopping" : sourceRunning || processingRunning ? "running" : "idle"}
            tone={sourceStopping ? "warning" : sourceRunning || processingRunning ? "active" : "neutral"}
          />
          <div className="font-mono text-label-sm text-on-surface-variant">{jobLabel}</div>
        </div>

        <div className="ml-4 flex min-w-0 items-center justify-end gap-3">
          <div className="truncate text-right font-mono text-label-sm text-on-surface-variant">
            {sourceStatus?.message || dashboard?.recent_jobs?.[0]?.message || "No active jobs"}
          </div>
          <button
            className="shrink-0 rounded-md bg-surface-variant px-3 py-2 font-mono text-label-sm text-on-surface-variant hover:bg-surface-container-highest"
            onClick={() => {
              setLlmLogOpen(true);
              void refreshLlmLog();
            }}
            title="Show LLM call context log"
            type="button"
          >
            LLM {formatTokens(llmLog?.largest_context_tokens || 0)}
          </button>
          {sourceRunning && sourceTaskJobId && (
            <Button
              disabled={sourceStopping}
              variant="danger"
              onClick={() => void cancelSourceTasks()}
            >
              {sourceStopping ? "Stopping..." : "Stop Run"}
            </Button>
          )}
        </div>
      </footer>

      {llmLogOpen && (
        <div className="fixed inset-0 z-50 flex items-end justify-end bg-surface/80 p-4 backdrop-blur-sm" role="dialog" aria-modal="true">
          <div className="thin-scrollbar max-h-[82vh] w-full max-w-3xl overflow-y-auto rounded-lg border border-outline-variant/40 bg-surface p-4 shadow-xl">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 className="text-title-lg font-bold">LLM Call Log</h2>
                <p className="mt-1 text-body-sm text-on-surface-variant">
                  Largest completed context this session:{" "}
                  <span className="font-mono text-on-surface">
                    {formatTokens(llmLog?.largest_context_tokens || 0)} tokens
                  </span>
                </p>
              </div>
              <Button variant="secondary" onClick={() => setLlmLogOpen(false)}>Close</Button>
            </div>

            {llmLogError && (
              <div className="mt-4 rounded-md border border-error/40 bg-error/10 p-3 text-body-sm text-error">
                {llmLogError}
              </div>
            )}

            <div className="mt-4 grid gap-3 text-body-sm md:grid-cols-4">
              <div>
                <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Calls</div>
                <div className="font-mono text-title-md">{llmLog?.total_calls || 0}</div>
              </div>
              <div>
                <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Completed</div>
                <div className="font-mono text-title-md">{llmLog?.completed_calls || 0}</div>
              </div>
              <div>
                <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Failed</div>
                <div className="font-mono text-title-md">{llmLog?.failed_calls || 0}</div>
              </div>
              <div>
                <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Log File</div>
                <div className="truncate font-mono text-label-sm" title={llmLog?.log_file || ""}>
                  {llmLog?.log_file || "Not started"}
                </div>
              </div>
            </div>

            <div className="mt-5 space-y-3">
              {(llmLog?.recent_calls || []).length === 0 ? (
                <div className="rounded-md bg-surface-container p-4 text-body-md text-on-surface-variant">
                  No LLM calls have been recorded in this app session yet.
                </div>
              ) : (
                llmLog?.recent_calls.map((call) => (
                  <details key={call.id} className="rounded-md bg-surface-container p-3 ghost-border">
                    <summary className="cursor-pointer list-none">
                      <div className="flex flex-wrap items-center justify-between gap-3">
                        <div className="min-w-0">
                          <div className="font-mono text-label-sm text-on-surface-variant">
                            {formatLocalDate(call.completed_at || call.started_at)} · {call.call_type} · {call.status}
                          </div>
                          <div className="truncate text-body-md" title={call.prompt_preview}>
                            {call.prompt_preview || "No prompt text"}
                          </div>
                        </div>
                        <div className="shrink-0 text-right font-mono text-label-sm">
                          <div>{promptLabel(call)}</div>
                          <div className="text-on-surface-variant">
                            {formatTokens(call.prompt_chars)} chars · {call.duration_ms} ms
                          </div>
                        </div>
                      </div>
                    </summary>
                    <div className="mt-3 grid gap-3">
                      <div className="grid gap-1">
                        <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
                          System Prompt
                        </div>
                        <pre className="thin-scrollbar max-h-48 overflow-auto rounded-md bg-surface-container-lowest p-3 text-label-sm">
                          {call.system_prompt || "(empty)"}
                        </pre>
                      </div>
                      <div className="grid gap-1">
                        <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
                          User Prompt
                        </div>
                        <pre className="thin-scrollbar max-h-72 overflow-auto rounded-md bg-surface-container-lowest p-3 text-label-sm">
                          {call.user_prompt || "(empty)"}
                        </pre>
                      </div>
                      {call.error && (
                        <div className="rounded-md border border-error/40 bg-error/10 p-3 text-body-sm text-error">
                          {call.error}
                        </div>
                      )}
                    </div>
                  </details>
                ))
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
