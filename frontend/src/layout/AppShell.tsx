import { useCallback, useMemo, useState } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";

import { Button, StatusBadge } from "../components/primitives";
import { useAppState } from "../state/AppState";

interface NavEntry {
  label: string;
  to: string;
}

export const PRIMARY_NAV: NavEntry[] = [
  { label: "Browser", to: "/browser" },
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
    processingStatus,
    sourceStatus,
    cancelSourceTasks,
  } = useAppState();

  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [legacyOpen, setLegacyOpen] = useState(false);

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
      return sourceStopping
        ? `Repository Processing stopping ${processed}/${total}`
        : `Repository Processing ${processed}/${total}`;
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
  ]);

  const repoState = repositoryStatus?.download_state || "idle";
  const isBrowserRoute = location.pathname === "/browser";

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
            {dashboard?.recent_jobs?.[0]?.message || "No active jobs"}
          </div>
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
    </div>
  );
}
