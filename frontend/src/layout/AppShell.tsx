import { useCallback, useMemo, useState } from "react";
import { NavLink, Outlet, useLocation, useNavigate } from "react-router-dom";

import { Button, StatusBadge } from "../components/primitives";
import { useAppState } from "../state/AppState";

interface NavEntry {
  label: string;
  to: string;
}

const NAV_GROUPS: Array<{ label: string; items: NavEntry[] }> = [
  {
    label: "Project",
    items: [
      { label: "Overview", to: "/project/overview" },
      { label: "Documents", to: "/project/documents" },
      { label: "Source Lists", to: "/project/source-lists" },
      { label: "Merge Repositories", to: "/project/merge" },
    ],
  },
  {
    label: "Processing",
    items: [
      { label: "Citation Extraction", to: "/processing/citation-extraction" },
      { label: "Source Capture", to: "/processing/source-capture" },
      { label: "Job History", to: "/processing/job-history" },
    ],
  },
  {
    label: "Data",
    items: [
      { label: "Manifest", to: "/data/manifest" },
      { label: "Repository Browser", to: "/data/repository-browser" },
      { label: "Citations", to: "/data/citations" },
      { label: "Bibliography", to: "/data/bibliography" },
      { label: "Contexts", to: "/data/sentences" },
      { label: "Matches", to: "/data/matches" },
    ],
  },
  {
    label: "Research",
    items: [
      { label: "Research Purpose", to: "/research/purpose" },
      { label: "Project Profile", to: "/research/project-profile" },
    ],
  },
  {
    label: "Settings",
    items: [
      { label: "LLM Backend", to: "/settings/llm-backend" },
      { label: "Ingestion Profiles", to: "/settings/ingestion-profiles" },
      { label: "Repository Settings", to: "/settings/repository" },
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
    processingStatus,
    sourceStatus,
  } = useAppState();

  const [sidebarOpen, setSidebarOpen] = useState(false);

  const repoName = useMemo(() => {
    if (!repositoryStatus?.path) return "No Repository";
    const chunks = repositoryStatus.path.split("/").filter(Boolean);
    return chunks[chunks.length - 1] || repositoryStatus.path;
  }, [repositoryStatus?.path]);

  const jobLabel = useMemo(() => {
    if (sourceRunning) {
      const processed = sourceStatus?.processed_urls || 0;
      const total = sourceStatus?.total_urls || 0;
      return sourceStopping
        ? `Source Capture stopping ${processed}/${total}`
        : `Source Capture ${processed}/${total}`;
    }
    if (processingRunning) {
      const preprocessState = String(
        processingStatus?.repository_preprocess_state || "",
      ).toLowerCase();
      if (preprocessState === "pending" || preprocessState === "running") {
        return (
          processingStatus?.repository_preprocess_message ||
          "Citation Extraction preprocessing"
        );
      }
      const finalizeState = String(
        processingStatus?.repository_finalize_state || "",
      ).toLowerCase();
      if (finalizeState === "pending" || finalizeState === "running") {
        return (
          processingStatus?.repository_finalize_message ||
          "Citation Extraction finalizing"
        );
      }
      const pct = Math.round(processingStatus?.progress_pct || 0);
      return `Citation Extraction ${pct}%`;
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

  const handleOpenProject = useCallback(async () => {
    const seedPath = repositoryStatus?.path || lastRepositoryPath;
    const selectedPath = await pickRepositoryDirectory("open", seedPath);
    if (!selectedPath) return;
    const opened = await openRepository(selectedPath);
    if (opened) {
      navigate("/project/overview");
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
      navigate("/project/documents");
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
    <div className="flex min-h-screen flex-col bg-surface">
      <header className="sticky top-0 z-40 flex h-14 items-center justify-between border-b border-outline-variant/30 bg-surface px-4 md:px-6">
        <div className="flex items-center gap-4">
          <button
            className="rounded-md border border-outline-variant/40 px-2 py-1 text-body-md md:hidden"
            onClick={() => setSidebarOpen((prev) => !prev)}
            type="button"
          >
            Menu
          </button>
          <button className="text-xl font-bold tracking-tight" onClick={() => navigate("/project/overview")} type="button">
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
          <input
            className="hidden rounded-md border border-outline-variant bg-surface-container-low px-3 py-1.5 text-body-md text-on-surface placeholder:text-on-surface-variant focus:border-primary focus:outline-none lg:block"
            placeholder="Search data..."
            type="text"
          />
          <Button variant="secondary" onClick={() => navigate("/project/overview")}>Home</Button>
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

          <Button className="mb-4 w-full" variant="primary" onClick={() => navigate("/project/documents")}>New Research</Button>

          <nav className="space-y-4 pb-6">
            {NAV_GROUPS.map((group) => (
              <section key={group.label}>
                <div className="px-2 pb-1 text-label-sm uppercase tracking-[0.09em] text-on-surface-variant/60">
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
          </nav>
        </aside>

        <main className="thin-scrollbar min-h-0 flex-1 overflow-y-auto bg-surface-container-lowest p-4 pb-24 md:p-6 md:pb-24">
          <div className="mb-4 font-mono text-label-sm uppercase tracking-[0.08em] text-on-surface-variant/70">
            {location.pathname.replace(/^\//, "").replace(/\//g, " > ") || "project > overview"}
          </div>
          <Outlet />
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

        <div className="text-right font-mono text-label-sm text-on-surface-variant">
          {dashboard?.recent_jobs?.[0]?.message || "No active jobs"}
        </div>
      </footer>
    </div>
  );
}
