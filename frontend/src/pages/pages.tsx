import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useNavigate } from "react-router-dom";

import { Button, EmptyState, InputField, MetricCard, SectionHeader, SelectField, StatusBadge, SurfaceCard, TextAreaField } from "../components/primitives";
import { STAGE_NAMES, useAppState } from "../state/AppState";
import type {
  DocumentNormalizationResult,
  IngestionProfile,
  RepositoryManifestRow,
} from "../api/types";

export { RepositoryBrowserPage } from "./RepositoryBrowserPage";

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatTimestamp(value: string): string {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}

function mergeQueuedFiles(existing: File[], incoming: FileList | File[]): File[] {
  const seen = new Set(existing.map((file) => `${file.name}::${file.size}::${file.lastModified}`));
  const next = [...existing];
  Array.from(incoming || []).forEach((file) => {
    const signature = `${file.name}::${file.size}::${file.lastModified}`;
    if (seen.has(signature)) return;
    seen.add(signature);
    next.push(file);
  });
  return next;
}

function confidenceBadge(value: number) {
  const pct = Math.max(0, Math.min(100, Math.round((value || 0) * 100)));
  if (pct >= 80) {
    return <span className="status-pill bg-success/10 text-success">{pct}%</span>;
  }
  if (pct >= 50) {
    return <span className="status-pill bg-warning/10 text-warning">{pct}%</span>;
  }
  return <span className="status-pill bg-error/10 text-error">{pct}%</span>;
}

function splitMultilineInput(value: string): string[] {
  return value
    .split(/\r?\n/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function joinMultilineInput(values: string[]): string {
  return (values || []).join("\n");
}

function createEmptyIngestionProfile(): IngestionProfile {
  return {
    profile_id: "",
    label: "",
    description: "",
    built_in: false,
    file_type_hints: [],
    reference_heading_patterns: [],
    citation_marker_patterns: [],
    bibliography_split_patterns: [],
    llm_guidance: "",
    confidence_threshold: 0.6,
    notes: [],
  };
}

function normalizationTone(
  status: string,
): "neutral" | "success" | "warning" | "error" | "active" {
  if (status === "normalized") return "success";
  if (status === "partial") return "warning";
  if (status === "failed") return "error";
  if (status === "pending") return "active";
  return "neutral";
}

function NormalizationResultsPanel({
  results,
}: {
  results: DocumentNormalizationResult[];
}) {
  return (
    <SurfaceCard>
      <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
        Standardized Markdown
      </div>
      {results.length === 0 ? (
        <div className="text-body-md text-on-surface-variant">
          No normalization metadata yet. Start extraction to generate standardized markdown files.
        </div>
      ) : (
        <div className="space-y-3">
          {results.map((result) => (
            <div
              key={`${result.filename}-${result.selected_profile_id}-${result.status}`}
              className="rounded-md bg-surface-container-low px-3 py-3"
            >
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="text-body-md font-semibold">{result.filename}</div>
                <StatusBadge
                  text={result.status}
                  tone={normalizationTone(result.status)}
                />
              </div>
              <div className="mt-2 flex flex-wrap items-center gap-2 text-label-sm text-on-surface-variant">
                <span>Profile: {result.selected_profile_label || result.selected_profile_id || "Auto-detect"}</span>
                {confidenceBadge(result.confidence_score)}
                {result.used_llm_fallback && (
                  <StatusBadge text="LLM fallback" tone="active" />
                )}
              </div>
              <div className="mt-2 text-body-md text-on-surface-variant">
                Citations matched {result.matched_citation_markers}/{result.total_citation_markers} |
                unresolved {result.unresolved_citation_markers} | works cited links{" "}
                {result.works_cited_linked_entries}/{result.bibliography_entry_count}
              </div>
              {result.standardized_markdown_path && (
                <div className="mt-2 text-label-sm text-on-surface-variant">
                  Markdown: <span className="font-mono">{result.standardized_markdown_path}</span>
                </div>
              )}
              {result.metadata_path && (
                <div className="text-label-sm text-on-surface-variant">
                  Metadata: <span className="font-mono">{result.metadata_path}</span>
                </div>
              )}
              {result.warnings.length > 0 && (
                <div className="mt-2 space-y-1">
                  {result.warnings.map((warning) => (
                    <div
                      key={`${result.filename}-${warning}`}
                      className="rounded-md bg-warning/10 px-2 py-1 text-label-sm text-warning"
                    >
                      {warning}
                    </div>
                  ))}
                </div>
              )}
              {result.error_message && (
                <div className="mt-2 rounded-md bg-error/10 px-2 py-1 text-label-sm text-error">
                  {result.error_message}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </SurfaceCard>
  );
}

function TableBlock({
  title,
  rows,
  empty,
}: {
  title: string;
  rows: React.ReactNode;
  empty: boolean;
}) {
  if (empty) {
    return <EmptyState title={`No ${title.toLowerCase()} data`} detail="Run extraction or import data to populate this table." />;
  }
  return (
    <SurfaceCard className="overflow-auto p-0">
      <div className="border-b border-outline-variant/30 bg-surface-container-high px-4 py-3 text-title-sm font-semibold">{title}</div>
      <div className="max-h-[65vh] overflow-auto thin-scrollbar">
        <table className="data-table">{rows}</table>
      </div>
    </SurfaceCard>
  );
}

export function LandingPage() {
  const navigate = useNavigate();
  const {
    openRepository,
    createRepository,
    pickRepositoryDirectory,
    lastRepositoryPath,
    gateError,
    gateMessage,
  } = useAppState();
  const [mode, setMode] = useState<"open" | "create">("open");
  const [openPath, setOpenPath] = useState("");
  const [createPath, setCreatePath] = useState("");

  const browseForMode = async (targetMode: "open" | "create") => {
    const seedPath =
      targetMode === "open"
        ? openPath.trim() || lastRepositoryPath
        : createPath.trim() || lastRepositoryPath;
    const selected = await pickRepositoryDirectory(targetMode, seedPath);
    if (!selected) return;
    if (targetMode === "open") {
      setOpenPath(selected);
      return;
    }
    setCreatePath(selected);
  };

  const submitOpenRepository = async () => {
    let selectedPath = openPath.trim();
    if (!selectedPath) {
      selectedPath = await pickRepositoryDirectory("open", lastRepositoryPath);
      if (!selectedPath) return;
      setOpenPath(selectedPath);
    }
    const opened = await openRepository(selectedPath);
    if (opened) {
      navigate("/project/overview");
    }
  };

  const submitCreateRepository = async () => {
    let selectedPath = createPath.trim();
    if (!selectedPath) {
      selectedPath = await pickRepositoryDirectory("create", lastRepositoryPath);
      if (!selectedPath) return;
      setCreatePath(selectedPath);
    }
    const created = await createRepository(selectedPath);
    if (created) {
      navigate("/project/ingest");
    }
  };

  const activePath = mode === "open" ? openPath : createPath;

  return (
    <div className="relative min-h-screen overflow-hidden bg-surface">
      <div className="pointer-events-none absolute inset-0 opacity-20">
        <div className="absolute -left-40 top-20 h-[320px] w-[420px] rounded-full bg-primary blur-[120px]" />
        <div className="absolute bottom-0 right-0 h-[360px] w-[420px] rounded-full bg-tertiary blur-[120px]" />
      </div>

      <div className="relative mx-auto max-w-6xl px-4 py-10 md:py-16">
        <div className="mb-10 flex items-center justify-between">
          <div className="text-3xl font-bold tracking-tight">ResearchAssistant</div>
          <StatusBadge text="No repository loaded" tone="neutral" />
        </div>

        <div className="grid gap-6 lg:grid-cols-[280px_1fr]">
          <SurfaceCard className="bg-surface-container-low">
            <div className="mb-4 text-label-sm uppercase tracking-[0.1em] text-on-surface-variant">Start Mode</div>
            <div className="space-y-2 text-body-md">
              <button
                className={`w-full rounded-md border px-3 py-2 text-left transition ${
                  mode === "open"
                    ? "border-primary bg-surface-container text-on-surface"
                    : "border-outline-variant/40 bg-surface-container-low text-on-surface-variant"
                }`}
                onClick={() => setMode("open")}
                type="button"
              >
                Open Existing Project
              </button>
              <button
                className={`w-full rounded-md border px-3 py-2 text-left transition ${
                  mode === "create"
                    ? "border-primary bg-surface-container text-on-surface"
                    : "border-outline-variant/40 bg-surface-container-low text-on-surface-variant"
                }`}
                onClick={() => setMode("create")}
                type="button"
              >
                Create New Project
              </button>
            </div>
            <div className="mt-6 text-label-sm text-on-surface-variant">Last used path</div>
            <div className="mt-2 rounded-md bg-surface-container p-2 font-mono text-label-sm text-on-surface-variant">
              {lastRepositoryPath || "No path recorded yet."}
            </div>
            {lastRepositoryPath && (
              <Button
                className="mt-3 w-full"
                onClick={() => {
                  if (mode === "open") {
                    setOpenPath(lastRepositoryPath);
                  } else {
                    setCreatePath(lastRepositoryPath);
                  }
                }}
              >
                Use Last Path
              </Button>
            )}
          </SurfaceCard>

          <SurfaceCard className="bg-surface-container-low p-8">
            <h1 className="text-display-sm font-extrabold">Research Repository Workspace</h1>
            <p className="mt-3 max-w-2xl text-body-md text-on-surface-variant">
              Choose whether to open an existing repository or create a new one. All settings and
              repository files stay inside the selected repository folder.
            </p>

            <div className="mt-8">
              <SurfaceCard className="bg-surface-container">
                <div className="text-title-sm font-semibold">
                  {mode === "open" ? "Open Existing Project" : "Create New Project"}
                </div>
                <div className="mt-2 text-body-md text-on-surface-variant">
                  {mode === "open"
                    ? "Select a repository folder that already contains a .ra_repo directory."
                    : "Select a target folder. The repository scaffold and .ra_repo/settings.json will be created immediately."}
                </div>

                <div className="mt-4 grid gap-2 md:grid-cols-[1fr_auto]">
                  <InputField
                    label="Repository Path"
                    placeholder={
                      mode === "open" ? "/path/to/existing/repo" : "/path/to/new/repo"
                    }
                    value={activePath}
                    onChange={(event) => {
                      if (mode === "open") {
                        setOpenPath(event.target.value);
                      } else {
                        setCreatePath(event.target.value);
                      }
                    }}
                  />
                  <Button className="md:mt-6" onClick={() => void browseForMode(mode)}>
                    Browse Folder
                  </Button>
                </div>

                <Button
                  className="mt-4 w-full"
                  variant="primary"
                  onClick={() =>
                    void (mode === "open"
                      ? submitOpenRepository()
                      : submitCreateRepository())
                  }
                >
                  {mode === "open" ? "Open Repository" : "Create Repository"}
                </Button>
              </SurfaceCard>
            </div>

            {(gateError || gateMessage) && (
              <div className={`mt-4 rounded-md px-3 py-2 text-body-md ${gateError ? "bg-error/10 text-error" : "bg-surface-container text-on-surface"}`}>
                {gateError || gateMessage}
              </div>
            )}
          </SurfaceCard>
        </div>
      </div>
    </div>
  );
}

export function OverviewPage() {
  const navigate = useNavigate();
  const { dashboard, repositoryStatus } = useAppState();

  const metrics = dashboard?.metrics;
  const output = dashboard?.output_formats;
  const warnings = dashboard?.warning_aggregates;

  return (
    <div>
      <SectionHeader
        title="Overview"
        description="Repository dashboard and state summary for active research intelligence."
      />

      <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-6">
        <MetricCard label="Sources" value={metrics?.total_sources ?? 0} detail="Repository sources" />
        <MetricCard label="Citations" value={metrics?.total_citations ?? 0} detail="Citation rows" accent="tertiary" />
        <MetricCard label="Queued" value={metrics?.queued_count ?? 0} detail="Waiting for download" accent="warning" />
        <MetricCard label="Markdown" value={output?.markdown ?? 0} detail="Extracted files" />
        <MetricCard label="Summaries" value={output?.summaries ?? 0} detail="Summary files" />
        <MetricCard label="Ratings" value={output?.ratings ?? 0} detail="Rating files" />
      </div>

      <div className="mt-4 grid gap-4 lg:grid-cols-[1.2fr_1fr_0.8fr]">
        <SurfaceCard className="p-0">
          <div className="border-b border-outline-variant/30 bg-surface-bright px-4 py-3 text-title-sm font-semibold">Recent Jobs</div>
          <div className="space-y-2 p-4">
            {(dashboard?.recent_jobs || []).slice(0, 6).map((job) => (
              <div key={`${job.kind}-${job.job_id}-${job.updated_at}`} className="flex items-center justify-between rounded-md bg-surface-container-low px-3 py-2">
                <div>
                  <div className="text-body-md font-semibold">{job.kind === "citation_extraction" ? "Citation Extraction (Legacy)" : "Repository Processing"}</div>
                  <div className="text-label-sm font-mono text-on-surface-variant">{job.job_id}</div>
                </div>
                <StatusBadge text={job.state} tone={job.state === "failed" ? "error" : job.state === "running" ? "active" : "neutral"} />
              </div>
            ))}
            {(!dashboard?.recent_jobs || dashboard.recent_jobs.length === 0) && (
              <div className="text-body-md text-on-surface-variant">No jobs yet.</div>
            )}
          </div>
        </SurfaceCard>

        <SurfaceCard className="p-0">
          <div className="border-b border-outline-variant/30 bg-surface-bright px-4 py-3 text-title-sm font-semibold">Recent Imports</div>
          <div className="space-y-2 p-4">
            {(dashboard?.recent_imports || []).slice(0, 6).map((entry) => (
              <div key={entry.import_id} className="rounded-md bg-surface-container-low px-3 py-2">
                <div className="text-body-md font-semibold">{entry.provenance}</div>
                <div className="mt-1 text-label-sm text-on-surface-variant">
                  {entry.accepted_new} new / {entry.duplicates_skipped} duplicates
                </div>
              </div>
            ))}
            {(!dashboard?.recent_imports || dashboard.recent_imports.length === 0) && (
              <div className="text-body-md text-on-surface-variant">No imports yet.</div>
            )}
          </div>
        </SurfaceCard>

        <SurfaceCard>
          <div className="text-title-sm font-semibold text-error">System Health</div>
          <div className="mt-3 space-y-3 text-body-md">
            <div>Missing files: <span className="font-mono">{warnings?.missing_files ?? 0}</span></div>
            <div>Orphaned rows: <span className="font-mono">{warnings?.orphaned_citation_rows ?? 0}</span></div>
            <div>Failed fetches: <span className="font-mono">{warnings?.failed_fetches ?? 0}</span></div>
            <div>Failed ratings: <span className="font-mono">{warnings?.failed_ratings ?? 0}</span></div>
            <div>Incomplete summaries: <span className="font-mono">{warnings?.incomplete_summaries ?? 0}</span></div>
          </div>
          <div className="mt-4 text-label-sm text-on-surface-variant">Repository state: {repositoryStatus?.download_state || "idle"}</div>
        </SurfaceCard>
      </div>

      <SurfaceCard className="mt-4">
        <div className="mb-2 text-label-sm uppercase tracking-[0.1em] text-on-surface-variant">Command Center</div>
        <div className="grid gap-2 md:grid-cols-3 lg:grid-cols-6">
          <Button onClick={() => navigate("/project/ingest")}>Ingest Sources</Button>
          <Button onClick={() => navigate("/data/repository-browser")}>Open Repository Browser</Button>
          <Button onClick={() => navigate("/processing/citation-extraction")}>Citation Extraction (Legacy)</Button>
          <Button onClick={() => navigate("/data/manifest")}>Open Manifest</Button>
          <Button onClick={() => navigate("/processing/source-capture")}>Run Repository Processing</Button>
          <Button onClick={() => navigate("/data/citations")}>Open Citations</Button>
        </div>
      </SurfaceCard>
    </div>
  );
}

export function DocumentsPage() {
  const navigate = useNavigate();
  const {
    ingestSeedFiles,
    ingestRepositoryDocuments,
    repoMessage,
    repoError,
    repositoryStatus,
    sourceTaskDraft,
  } = useAppState();
  const [seedFiles, setSeedFiles] = useState<File[]>([]);
  const [documentFiles, setDocumentFiles] = useState<File[]>([]);
  const [seedSubmitting, setSeedSubmitting] = useState(false);
  const [documentSubmitting, setDocumentSubmitting] = useState(false);

  const addSeedFiles = (incoming: FileList | File[]) =>
    setSeedFiles((prev) => mergeQueuedFiles(prev, incoming));
  const addDocumentFiles = (incoming: FileList | File[]) =>
    setDocumentFiles((prev) => mergeQueuedFiles(prev, incoming));

  const removeSeedFile = (index: number) =>
    setSeedFiles((prev) => prev.filter((_, currentIndex) => currentIndex !== index));
  const removeDocumentFile = (index: number) =>
    setDocumentFiles((prev) => prev.filter((_, currentIndex) => currentIndex !== index));

  const submitSeedFiles = async () => {
    if (seedFiles.length === 0 || seedSubmitting) return;
    setSeedSubmitting(true);
    try {
      await ingestSeedFiles(seedFiles);
      setSeedFiles([]);
    } finally {
      setSeedSubmitting(false);
    }
  };

  const submitDocumentFiles = async () => {
    if (documentFiles.length === 0 || documentSubmitting) return;
    setDocumentSubmitting(true);
    try {
      await ingestRepositoryDocuments(documentFiles);
      setDocumentFiles([]);
    } finally {
      setDocumentSubmitting(false);
    }
  };

  return (
    <div>
      <SectionHeader
        title="Ingest"
        description="Bring seed links or primary documents into the repository. Uploaded files become repository sources; citation extraction now lives under Advanced/Legacy."
      />

      <div className="grid gap-4 xl:grid-cols-2">
        <SurfaceCard>
          <div className="text-title-sm font-semibold">Seed Links / Reports</div>
          <div className="mt-2 text-body-md text-on-surface-variant">
            Accepts deep research reports and link lists. The ingest step only harvests links and nearby titles, assigns stable source IDs, and queues the repository for later processing.
          </div>
          <div
            className="mt-4 rounded-lg border border-dashed border-outline-variant bg-surface-container-low p-6 text-center"
            onDrop={(event) => {
              event.preventDefault();
              addSeedFiles(event.dataTransfer.files);
            }}
            onDragOver={(event) => event.preventDefault()}
          >
            <div className="text-body-md">Drop CSV, XLSX, MD, PDF, or DOCX files here</div>
            <label className="mt-3 inline-flex cursor-pointer items-center rounded-md bg-surface-variant px-3 py-2 text-body-md">
              Select Seed Files
              <input
                className="hidden"
                multiple
                accept=".csv,.xlsx,.md,.pdf,.docx"
                type="file"
                onChange={(event) => addSeedFiles(event.target.files || [])}
              />
            </label>
          </div>
          {seedFiles.length > 0 ? (
            <div className="mt-4 space-y-2">
              {seedFiles.map((file, index) => (
                <div key={`${file.name}-${file.lastModified}`} className="flex items-center justify-between rounded-md bg-surface-container-low px-3 py-2">
                  <div>
                    <div className="text-body-md font-medium">{file.name}</div>
                    <div className="text-label-sm text-on-surface-variant">{formatBytes(file.size)}</div>
                  </div>
                  <Button variant="danger" onClick={() => removeSeedFile(index)}>Remove</Button>
                </div>
              ))}
            </div>
          ) : (
            <div className="mt-4 text-body-md text-on-surface-variant">No seed files selected.</div>
          )}
          <div className="mt-4 flex flex-wrap gap-2">
            <Button variant="primary" disabled={seedFiles.length === 0 || seedSubmitting} onClick={() => void submitSeedFiles()}>
              {seedSubmitting ? "Importing..." : "Import Seed Sources"}
            </Button>
            <Button onClick={() => setSeedFiles([])}>Clear Queue</Button>
          </div>
        </SurfaceCard>

        <SurfaceCard>
          <div className="text-title-sm font-semibold">Add Documents To Repository</div>
          <div className="mt-2 text-body-md text-on-surface-variant">
            Upload PDFs, docs, markdown, or HTML directly into the repository. These become first-class local sources and can be processed in bulk through convert, catalog, summary, and tagging phases.
          </div>
          <div
            className="mt-4 rounded-lg border border-dashed border-outline-variant bg-surface-container-low p-6 text-center"
            onDrop={(event) => {
              event.preventDefault();
              addDocumentFiles(event.dataTransfer.files);
            }}
            onDragOver={(event) => event.preventDefault()}
          >
            <div className="text-body-md">Drop PDF, DOC, DOCX, HTML, MD, RTF, or TXT files here</div>
            <label className="mt-3 inline-flex cursor-pointer items-center rounded-md bg-surface-variant px-3 py-2 text-body-md">
              Select Repository Documents
              <input
                className="hidden"
                multiple
                accept=".pdf,.doc,.docx,.html,.htm,.md,.rtf,.txt"
                type="file"
                onChange={(event) => addDocumentFiles(event.target.files || [])}
              />
            </label>
          </div>
          {documentFiles.length > 0 ? (
            <div className="mt-4 space-y-2">
              {documentFiles.map((file, index) => (
                <div key={`${file.name}-${file.lastModified}`} className="flex items-center justify-between rounded-md bg-surface-container-low px-3 py-2">
                  <div>
                    <div className="text-body-md font-medium">{file.name}</div>
                    <div className="text-label-sm text-on-surface-variant">{formatBytes(file.size)}</div>
                  </div>
                  <Button variant="danger" onClick={() => removeDocumentFile(index)}>Remove</Button>
                </div>
              ))}
            </div>
          ) : (
            <div className="mt-4 text-body-md text-on-surface-variant">No repository documents selected.</div>
          )}
          <div className="mt-4 flex flex-wrap gap-2">
            <Button variant="primary" disabled={documentFiles.length === 0 || documentSubmitting} onClick={() => void submitDocumentFiles()}>
              {documentSubmitting ? "Adding..." : "Add To Repository"}
            </Button>
            <Button onClick={() => setDocumentFiles([])}>Clear Queue</Button>
          </div>
        </SurfaceCard>
      </div>

      <SurfaceCard className="mt-4">
        <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Next Step</div>
        <div className="text-body-md text-on-surface-variant">
          After ingest, run repository processing to fetch URL sources, convert uploaded documents, catalog metadata, summarize against the research purpose, and apply relevance ratings and tags.
        </div>
        <div className="mt-3 grid gap-3 lg:grid-cols-[minmax(0,1fr)_auto]">
          <div className="rounded-md bg-surface-container-low px-3 py-3 text-body-md text-on-surface-variant">
            Repository path: <span className="font-mono">{repositoryStatus?.path || "-"}</span>
            <br />
            Next processing scope: <span className="font-mono">{sourceTaskDraft.import_id || "latest repository state"}</span>
          </div>
          <Button className="lg:mt-4" variant="primary" onClick={() => navigate("/processing/source-capture")}>
            Open Repository Processing
          </Button>
        </div>
      </SurfaceCard>

      {(repoError || repoMessage) && (
        <div className={`mt-4 rounded-md px-3 py-2 text-body-md ${repoError ? "bg-error/10 text-error" : "bg-surface-container text-on-surface"}`}>
          {repoError || repoMessage}
        </div>
      )}
    </div>
  );
}

export function SourceListsPage() {
  const {
    importSourceList,
    rebuildRepositoryOutputs,
    cleanupRepositoryLayout,
    repositoryStatus,
    repoMessage,
    repoError,
  } = useAppState();
  const [sourceFile, setSourceFile] = useState<File | null>(null);

  return (
    <div>
      <SectionHeader title="Source Lists" description="Import source URL spreadsheets directly into the repository." />

      <SurfaceCard>
        <div className="text-title-sm font-semibold">Import Source URL Spreadsheet</div>
        <div className="mt-3 flex flex-wrap items-center gap-3">
          <input accept=".csv,.xlsx" type="file" onChange={(event) => setSourceFile(event.target.files?.[0] || null)} />
          <Button variant="primary" onClick={() => importSourceList(sourceFile)}>Import Spreadsheet</Button>
        </div>
        <div className="mt-2 text-body-md text-on-surface-variant">CSV/XLSX must include a URL-like column.</div>
      </SurfaceCard>

      <SurfaceCard className="mt-4">
        <div className="text-title-sm font-semibold">Repository State</div>
        <div className="mt-3 space-y-2 text-body-md text-on-surface-variant">
          <div>
            Path: <span className="font-mono">{repositoryStatus?.path || "-"}</span>
          </div>
          <div>
            Queued sources: <span className="font-mono">{repositoryStatus?.queued_count || 0}</span>
          </div>
          <div>
            Repository files stay current at the project root: <span className="font-mono">manifest.csv</span>, <span className="font-mono">manifest.xlsx</span>, <span className="font-mono">citations.csv</span>
          </div>
          <div>
            Legacy source artifacts can be normalized into:
            <span className="ml-2 font-mono">{repositoryStatus?.path ? `${repositoryStatus.path}/sources/<source_id>/` : "sources/<source_id>/"}</span>
          </div>
        </div>
        <div className="mt-3 flex flex-wrap gap-2">
          <Button onClick={() => rebuildRepositoryOutputs()}>
            Rebuild Repository Files
          </Button>
          <Button variant="secondary" onClick={() => cleanupRepositoryLayout()}>
            Clean Up Existing Repository
          </Button>
        </div>
      </SurfaceCard>

      {(repoError || repoMessage) && (
        <div className={`mt-4 rounded-md px-3 py-2 text-body-md ${repoError ? "bg-error/10 text-error" : "bg-surface-container text-on-surface"}`}>
          {repoError || repoMessage}
        </div>
      )}
    </div>
  );
}

export function MergeRepositoriesPage() {
  const { mergeRepositories } = useAppState();
  const [paths, setPaths] = useState("");

  return (
    <div>
      <SectionHeader title="Merge Repositories" description="Merge external repository sources into the current repository." />
      <SurfaceCard>
        <TextAreaField
          label="External Repository Paths (one per line)"
          rows={5}
          value={paths}
          onChange={(event) => setPaths(event.target.value)}
          placeholder="/path/to/repo-a\n/path/to/repo-b"
        />
        <Button className="mt-3" variant="primary" onClick={() => mergeRepositories(paths.split("\n"))}>
          Merge Into Current Repository
        </Button>
      </SurfaceCard>
    </div>
  );
}

export function CitationExtractionPage() {
  const {
    files,
    addFiles,
    removeFileAtIndex,
    clearFiles,
    documentImports,
    startProcessing,
    reprocessStoredDocuments,
    clearRepositoryCitations,
    processingStatus,
    processingRunning,
    warnings,
    settingsDraft,
    setSettingsDraft,
    ingestionProfiles,
    defaultIngestionProfileId,
    selectedIngestionProfileId,
    setSelectedIngestionProfileId,
    selectedReprocessImportIds,
    setSelectedReprocessImportIds,
  } = useAppState();

  const stageRows = processingStatus?.stages || [];
  const toggleReprocessImport = (importId: string) => {
    setSelectedReprocessImportIds((current) =>
      current.includes(importId)
        ? current.filter((item) => item !== importId)
        : [...current, importId],
    );
  };

  return (
    <div>
      <SectionHeader
        title="Citation Extraction (Legacy)"
        description="Run the legacy citation-centric document pipeline. Repository-first ingest and source processing now live under Project > Ingest and Processing > Repository Processing."
        right={
          <StatusBadge
            text={processingRunning ? "Engine Running" : "Engine Ready"}
            tone={processingRunning ? "active" : "success"}
          />
        }
      />

      <div className="grid gap-4 lg:grid-cols-[320px_1fr]">
        <div className="space-y-4">
          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Documents to run</div>
            <div
              className="rounded-lg border border-dashed border-outline-variant bg-surface-container-low p-4 text-center"
              onDrop={(event) => {
                event.preventDefault();
                addFiles(event.dataTransfer.files);
              }}
              onDragOver={(event) => event.preventDefault()}
            >
              <div className="text-body-md">Drop PDF, DOCX, or MD files here for the legacy citation pipeline</div>
              <label className="mt-3 inline-flex cursor-pointer items-center rounded-md bg-surface-variant px-3 py-2 text-body-md">
                Select Legacy Files
                <input
                  className="hidden"
                  multiple
                  accept=".pdf,.docx,.md"
                  type="file"
                  onChange={(event) => addFiles(event.target.files || [])}
                />
              </label>
            </div>
            <div className="mt-3 space-y-2">
              {files.length === 0 ? (
                <div className="text-body-md text-on-surface-variant">No staged legacy documents yet.</div>
              ) : (
                files.map((file, index) => (
                  <div key={`${file.name}-${file.lastModified}`} className="rounded-md bg-surface-container-low px-3 py-2">
                    <div className="flex items-center justify-between gap-2">
                      <div>
                        <div className="text-body-md font-medium">{file.name}</div>
                        <div className="text-label-sm text-on-surface-variant">{formatBytes(file.size)}</div>
                      </div>
                      <Button variant="danger" onClick={() => removeFileAtIndex(index)}>Remove</Button>
                    </div>
                  </div>
                ))
              )}
            </div>
            <div className="mt-3 flex flex-wrap gap-2">
              <Button onClick={clearFiles}>Clear Staged Files</Button>
            </div>
          </SurfaceCard>

          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Stored Repository Imports</div>
            {documentImports.length === 0 ? (
              <div className="text-body-md text-on-surface-variant">
                No rerunnable document imports yet. Run an extraction upload first.
              </div>
            ) : (
              <div className="space-y-2">
                {documentImports.map((item) => {
                  const selected = selectedReprocessImportIds.includes(item.import_id);
                  return (
                    <label
                      key={item.import_id}
                      className={`block cursor-pointer rounded-md border px-3 py-3 ${
                        selected
                          ? "border-primary bg-primary/5"
                          : "border-outline-variant bg-surface-container-low"
                      }`}
                    >
                      <div className="flex items-start gap-3">
                        <input
                          checked={selected}
                          type="checkbox"
                          onChange={() => toggleReprocessImport(item.import_id)}
                        />
                        <div className="min-w-0 flex-1">
                          <div className="flex flex-wrap items-center gap-2">
                            <div className="text-body-md font-medium">{item.import_id}</div>
                            <StatusBadge text={item.document_count === 1 ? "1 doc" : `${item.document_count} docs`} tone="neutral" />
                          </div>
                          <div className="mt-1 text-label-sm text-on-surface-variant">
                            Imported {formatTimestamp(item.imported_at)} | profile{" "}
                            <span className="font-mono">{item.selected_profile_id || "auto_detect"}</span>
                          </div>
                          <div className="mt-2 text-label-sm text-on-surface-variant">
                            {item.documents.map((document) => document.filename).join(", ")}
                          </div>
                        </div>
                      </div>
                    </label>
                  );
                })}
              </div>
            )}
            <div className="mt-3 text-body-md text-on-surface-variant">
              Reprocessing updates citations, paragraph detection, and title backfills from stored repository documents without rerunning downloads, summaries, or ratings.
            </div>
          </SurfaceCard>

          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Extraction options</div>
            <SelectField
              label="Normalization Profile"
              value={selectedIngestionProfileId}
              onChange={(event) => setSelectedIngestionProfileId(event.target.value)}
            >
              <option value="">
                Auto-detect{defaultIngestionProfileId ? ` (${defaultIngestionProfileId})` : ""}
              </option>
              {ingestionProfiles.map((profile) => (
                <option key={profile.profile_id} value={profile.profile_id}>
                  {profile.label}
                  {profile.built_in ? " [Built-in]" : ""}
                </option>
              ))}
            </SelectField>
            <div className="mb-3 text-body-md text-on-surface-variant">
              Selected profile for the next run:{" "}
              <span className="font-mono">
                {selectedIngestionProfileId || "auto_detect"}
              </span>
            </div>
            <label className="mb-2 flex items-center gap-2 text-body-md">
              <input
                checked={settingsDraft.use_llm}
                onChange={(event) =>
                  setSettingsDraft((prev) => ({
                    ...prev,
                    use_llm: event.target.checked,
                  }))
                }
                type="checkbox"
              />
              Use LLM-assisted bibliography repair
            </label>
            <TextAreaField
              label="Research Purpose"
              rows={3}
              value={settingsDraft.research_purpose}
              onChange={(event) =>
                setSettingsDraft((prev) => ({
                  ...prev,
                  research_purpose: event.target.value,
                }))
              }
            />
          </SurfaceCard>

          <div className="space-y-2">
            <Button className="w-full" variant="primary" disabled={files.length === 0 || processingRunning} onClick={() => startProcessing()}>
              {processingRunning ? "Running Extraction..." : "Run Extraction"}
            </Button>
            <Button
              className="w-full"
              disabled={selectedReprocessImportIds.length === 0 || processingRunning}
              onClick={() => reprocessStoredDocuments()}
            >
              {processingRunning ? "Running Reprocess..." : "Reprocess Selected Imports"}
            </Button>
            <Button
              className="w-full"
              variant="danger"
              disabled={processingRunning}
              onClick={async () => {
                const confirmed = window.confirm(
                  "Clear all currently stored citation rows from this repository? You can re-run extraction afterward to regenerate them.",
                );
                if (!confirmed) return;
                await clearRepositoryCitations();
              }}
            >
              Clear Stored Citations
            </Button>
          </div>
        </div>

        <div className="space-y-4">
          {processingStatus && (
            <SurfaceCard>
              <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Run Context</div>
              <div className="text-body-md text-on-surface-variant">
                Mode: <span className="font-mono">{processingStatus.processing_mode || "process_documents"}</span>
              </div>
              {processingStatus.repository_preprocess_state &&
                processingStatus.repository_preprocess_state !== "completed" &&
                processingStatus.repository_preprocess_state !== "skipped" && (
                  <div className="mt-2 text-body-md text-on-surface-variant">
                    Repository preprocess:{" "}
                    <span className="font-mono">{processingStatus.repository_preprocess_state}</span>
                    {processingStatus.repository_preprocess_message
                      ? ` | ${processingStatus.repository_preprocess_message}`
                      : ""}
                  </div>
                )}
              {processingStatus.repository_finalize_state && (
                <div className="mt-2 text-body-md text-on-surface-variant">
                  Repository finalize:{" "}
                  <span className="font-mono">{processingStatus.repository_finalize_state}</span>
                  {processingStatus.repository_finalize_message
                    ? ` | ${processingStatus.repository_finalize_message}`
                    : ""}
                </div>
              )}
              {processingStatus.target_import_ids && processingStatus.target_import_ids.length > 0 && (
                <div className="mt-2 text-body-md text-on-surface-variant">
                  Target imports:{" "}
                  <span className="font-mono">
                    {processingStatus.target_import_ids.join(", ")}
                  </span>
                </div>
              )}
            </SurfaceCard>
          )}

          <SurfaceCard>
            <div className="mb-3 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Stage Progress</div>
            <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-4">
              {(stageRows.length === 0
                ? Object.keys(STAGE_NAMES).map((key) => ({ stage: key, status: "pending", item_count: 0, warnings: [], errors: [] }))
                : stageRows
              ).map((stage) => (
                <div
                  key={stage.stage}
                  className={`rounded-md border-b-2 px-3 py-3 ${
                    stage.status === "completed"
                      ? "bg-surface-container border-primary"
                      : stage.status === "running"
                        ? "bg-surface-container-highest border-tertiary"
                        : stage.status === "failed"
                          ? "bg-error/10 border-error"
                          : "bg-surface-container-low border-outline-variant"
                  }`}
                >
                  <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">{STAGE_NAMES[stage.stage] || stage.stage}</div>
                  <div className="mt-2 text-body-md">{stage.status}</div>
                  {stage.item_count > 0 && <div className="text-label-sm text-on-surface-variant">{stage.item_count} items</div>}
                </div>
              ))}
            </div>
          </SurfaceCard>

          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Warnings & Errors</div>
            {warnings.length === 0 ? (
              <div className="text-body-md text-on-surface-variant">No warnings reported.</div>
            ) : (
              <div className="space-y-2">
                {warnings.map((item, index) => (
                  <div
                    key={`${item.stage}-${index}`}
                    className={`rounded-md px-3 py-2 text-body-md ${item.type === "error" ? "bg-error/10 text-error" : "bg-warning/10 text-warning"}`}
                  >
                    <span className="font-semibold">{STAGE_NAMES[item.stage] || item.stage}:</span> {item.message}
                  </div>
                ))}
              </div>
            )}
          </SurfaceCard>

          {processingStatus?.document_replacements && processingStatus.document_replacements.length > 0 && (
            <SurfaceCard>
              <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Document Replacement</div>
              <div className="space-y-2">
                {processingStatus.document_replacements.map((item) => (
                  <div key={`${item.repository_path}-${item.status}`} className="rounded-md bg-surface-container-low px-3 py-2">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="text-body-md font-medium">{item.filename}</div>
                      <StatusBadge text={item.status} tone={item.status === "replaced" ? "success" : "warning"} />
                    </div>
                    <div className="mt-1 text-label-sm text-on-surface-variant">
                      Replaced {item.replaced_existing_rows} old rows | preserved {item.preserved_existing_rows} | wrote {item.new_rows} new rows
                    </div>
                  </div>
                ))}
              </div>
            </SurfaceCard>
          )}

          <NormalizationResultsPanel results={processingStatus?.document_normalization || []} />
        </div>
      </div>
    </div>
  );
}

export function SourceCapturePage() {
  const {
    repositoryStatus,
    sourceTaskDraft,
    setSourceTaskDraft,
    runSourceTasks,
    cancelSourceTasks,
    sourceStatus,
    sourceRunning,
    sourceStopping,
    sourceError,
    hasSourceUrls,
    profiles,
    uploadProfile,
    loadProfiles,
  } = useAppState();

  const [profileFile, setProfileFile] = useState<File | null>(null);

  return (
    <div>
      <SectionHeader title="Repository Processing" description="Run fetch, convert, catalog, summary, and tagging phases directly against repository rows." />

      <div className="grid gap-4 xl:grid-cols-[420px_1fr]">
        <div className="space-y-4">
          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Repository Scope</div>
            <SelectField
              label="Target Set"
              value={sourceTaskDraft.scope}
              onChange={(event) =>
                setSourceTaskDraft((prev) => ({
                  ...prev,
                  scope: event.target.value as "all" | "queued" | "import" | "latest_import",
                }))
              }
            >
              <option value="queued">Queued rows</option>
              <option value="latest_import">Latest import</option>
              <option value="import">Specific import</option>
              <option value="all">Entire repository</option>
            </SelectField>
            {sourceTaskDraft.scope === "import" && (
              <InputField
                label="Import ID"
                placeholder="Paste repository import id"
                value={sourceTaskDraft.import_id}
                onChange={(event) =>
                  setSourceTaskDraft((prev) => ({ ...prev, import_id: event.target.value }))
                }
              />
            )}
            <div className="mt-3 text-body-md text-on-surface-variant">
              Repository path: <span className="font-mono">{repositoryStatus?.path || "-"}</span>
            </div>
            <div className="mt-2 text-body-md text-on-surface-variant">
              Uploaded repository documents automatically skip fetch and continue into later phases.
            </div>
          </SurfaceCard>

          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Web Retrieval (Network)</div>
            <label className="mb-2 flex items-center gap-2 text-body-md">
              <input
                checked={sourceTaskDraft.run_download}
                type="checkbox"
                onChange={(event) =>
                  setSourceTaskDraft((prev) => ({ ...prev, run_download: event.target.checked }))
                }
              />
              Download / refresh source files
            </label>
            <div className="mt-3 text-label-sm text-on-surface-variant">Download Outputs</div>
            {([
              ["include_raw_file", "Raw files"],
              ["include_rendered_html", "Rendered HTML"],
              ["include_rendered_pdf", "Rendered PDF"],
              ["include_markdown", "Markdown extraction"],
            ] as const).map(([key, label]) => (
              <label key={key} className="mb-2 mt-2 flex items-center gap-2 text-body-md">
                <input
                  checked={Boolean(sourceTaskDraft[key])}
                  type="checkbox"
                  onChange={(event) =>
                    setSourceTaskDraft((prev) => ({ ...prev, [key]: event.target.checked }))
                  }
                />
                {label}
              </label>
            ))}
            <label className="mb-2 mt-3 flex items-center gap-2 text-body-md">
              <input
                checked={sourceTaskDraft.force_redownload}
                type="checkbox"
                onChange={(event) =>
                  setSourceTaskDraft((prev) => ({
                    ...prev,
                    force_redownload: event.target.checked,
                    ...(event.target.checked ? { run_download: true } : {}),
                  }))
                }
              />
              Force regenerate selected download outputs
            </label>
          </SurfaceCard>

          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Local Deterministic</div>
            <div className="text-body-md text-on-surface-variant">
              Uses existing local artifacts and deterministic pipeline rules (no network calls,
              no LLM calls).
            </div>
            <div className="mt-3 text-label-sm text-on-surface-variant">
              Use "Run Failed Only" to retry only rows with previous fetch failures.
            </div>
          </SurfaceCard>

          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">AI Enrichment</div>
            <label className="mb-2 flex items-center gap-2 text-body-md">
              <input
                checked={sourceTaskDraft.run_llm_cleanup}
                type="checkbox"
                onChange={(event) =>
                  setSourceTaskDraft((prev) => ({ ...prev, run_llm_cleanup: event.target.checked }))
                }
              />
              LLM markdown cleanup
            </label>
            <label className="mb-2 flex items-center gap-2 text-body-md">
              <input
                checked={sourceTaskDraft.run_catalog}
                type="checkbox"
                onChange={(event) =>
                  setSourceTaskDraft((prev) => ({ ...prev, run_catalog: event.target.checked }))
                }
              />
              Catalog metadata: title, authors, date, document type, organization
            </label>
            <label className="mb-2 flex items-center gap-2 text-body-md">
              <input
                checked={sourceTaskDraft.run_llm_summary}
                type="checkbox"
                onChange={(event) =>
                  setSourceTaskDraft((prev) => ({ ...prev, run_llm_summary: event.target.checked }))
                }
              />
              LLM summaries
            </label>
            <label className="mb-2 flex items-center gap-2 text-body-md">
              <input
                checked={sourceTaskDraft.run_llm_rating}
                type="checkbox"
                onChange={(event) =>
                  setSourceTaskDraft((prev) => ({ ...prev, run_llm_rating: event.target.checked }))
                }
              />
              Rate sources using project profile
            </label>
            {([
              ["force_llm_cleanup", "Re-run cleanup even if cleanup file exists"],
              ["force_catalog", "Re-run catalog metadata extraction"],
              ["force_summary", "Re-run summaries"],
              ["force_rating", "Force re-rate"],
            ] as const).map(([key, label]) => (
              <label key={key} className="mb-2 flex items-center gap-2 text-body-md">
                <input
                  checked={Boolean(sourceTaskDraft[key])}
                  type="checkbox"
                  onChange={(event) =>
                    setSourceTaskDraft((prev) => ({
                      ...prev,
                      [key]: event.target.checked,
                      ...(key === "force_llm_cleanup" && event.target.checked
                        ? { run_llm_cleanup: true }
                        : {}),
                      ...(key === "force_catalog" && event.target.checked
                        ? { run_catalog: true }
                        : {}),
                      ...(key === "force_summary" && event.target.checked
                        ? { run_llm_summary: true }
                        : {}),
                      ...(key === "force_rating" && event.target.checked
                        ? { run_llm_rating: true }
                        : {}),
                    }))
                  }
                />
                {label}
              </label>
            ))}
            <SelectField
              label="Project Profile"
              value={sourceTaskDraft.project_profile_name}
              onChange={(event) =>
                setSourceTaskDraft((prev) => ({ ...prev, project_profile_name: event.target.value }))
              }
            >
              <option value="">-- Use default project profile --</option>
              {profiles.map((profile) => (
                <option key={profile.filename} value={profile.filename}>
                  {profile.name}
                </option>
              ))}
            </SelectField>
            <div className="mt-3 flex flex-wrap items-center gap-2">
              <input accept=".yaml,.yml" type="file" onChange={(event) => setProfileFile(event.target.files?.[0] || null)} />
              <Button onClick={() => uploadProfile(profileFile)}>Upload Profile</Button>
              <Button onClick={() => loadProfiles()}>Refresh Profiles</Button>
            </div>
          </SurfaceCard>

          <div className="grid gap-2 sm:grid-cols-2">
            <Button
              variant="primary"
              disabled={sourceRunning || (sourceTaskDraft.run_download && !hasSourceUrls)}
              onClick={() => runSourceTasks(false)}
            >
              Run Selected Tasks
            </Button>
            <Button
              disabled={sourceRunning || !sourceTaskDraft.run_download}
              onClick={() => runSourceTasks(true)}
            >
              Run Failed Only
            </Button>
            <Button variant="danger" disabled={!sourceRunning || sourceStopping} onClick={() => cancelSourceTasks()}>
              {sourceStopping ? "Stopping After Current Item..." : "Stop Current Run"}
            </Button>
          </div>
        </div>

        <div className="space-y-4">
          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Source Progress</div>
            {!sourceStatus ? (
              <div className="text-body-md text-on-surface-variant">No repository source task is active.</div>
            ) : (
              <>
                <div className="mb-2 flex items-center justify-between">
                  <StatusBadge
                    text={sourceStatus.state}
                    tone={
                      sourceStatus.state === "failed"
                        ? "error"
                        : sourceStatus.state === "running"
                          ? "active"
                          : sourceStatus.state === "cancelling"
                            ? "warning"
                            : "neutral"
                    }
                  />
                  <div className="font-mono text-body-md">
                    {sourceStatus.processed_urls}/{sourceStatus.total_urls}
                  </div>
                </div>
                <div className="h-2 overflow-hidden rounded-full bg-surface-container-low">
                  <div
                    className="h-full bg-primary"
                    style={{
                      width: `${
                        sourceStatus.total_urls > 0
                          ? Math.round((sourceStatus.processed_urls / sourceStatus.total_urls) * 100)
                          : 0
                      }%`,
                    }}
                  />
                </div>
                <div className="mt-2 text-body-md text-on-surface-variant">{sourceStatus.message}</div>
                {sourceStatus.state === "cancelling" && (
                  <div className="mt-3 rounded-md bg-warning/10 px-3 py-2 text-body-md text-warning">
                    Stop requested. The active LLM call is allowed to finish before the run stops.
                  </div>
                )}
                {sourceStatus.state === "cancelled" && (
                  <div className="mt-3 rounded-md bg-surface-container-high px-3 py-2 text-body-md text-on-surface-variant">
                    Run stopped. No new repository rows will be started.
                  </div>
                )}
                <div className="mt-2 text-label-sm text-on-surface-variant">
                  Scope: {sourceStatus.selected_scope || sourceTaskDraft.scope} | writes to{" "}
                  <span className="font-mono">{sourceStatus.repository_path || repositoryStatus?.path || "-"}</span>
                </div>

                <div className="mt-3 text-label-sm text-on-surface-variant">
                  Success {sourceStatus.success_count} | Partial {sourceStatus.partial_count} | Failed {sourceStatus.failed_count}
                </div>
              </>
            )}

            {sourceError && <div className="mt-3 rounded-md bg-error/10 px-3 py-2 text-body-md text-error">{sourceError}</div>}
          </SurfaceCard>

          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Status Rows</div>
            <div className="max-h-[50vh] overflow-auto thin-scrollbar">
              {(sourceStatus?.items || []).length === 0 ? (
                <div className="text-body-md text-on-surface-variant">No rows yet.</div>
              ) : (
                <div className="space-y-2">
                  {(sourceStatus?.items || []).slice(0, 200).map((item) => (
                    <div key={`${item.id}-${item.original_url}`} className="rounded-md bg-surface-container-low px-3 py-2">
                      <div className="flex items-center justify-between gap-2">
                        <div className="font-mono text-label-sm text-primary">{item.id}</div>
                        <StatusBadge
                          text={item.fetch_status || item.status}
                          tone={item.status === "failed" ? "error" : item.status === "running" ? "active" : "neutral"}
                        />
                      </div>
                      <div className="mt-1 text-body-md break-all">
                        {item.original_url || (item.source_kind === "uploaded_document" ? "[uploaded document]" : "-")}
                      </div>
                      <div className="mt-1 text-label-sm text-on-surface-variant">
                kind: {item.source_kind || "url"} | catalog: {item.catalog_status || "-"} | cleanup: {item.llm_cleanup_status || "-"} | summary: {item.summary_status || "-"} | rating: {item.rating_status || "-"}
              </div>
            </div>
          ))}
                </div>
              )}
            </div>
          </SurfaceCard>

          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Repository Writes</div>
            <div className="space-y-2 text-body-md text-on-surface-variant">
              <div>
                Manifest and citations stay up to date at the repository root:
                <span className="ml-2 font-mono">manifest.csv</span>,
                <span className="ml-2 font-mono">manifest.xlsx</span>,
                <span className="ml-2 font-mono">citations.csv</span>
              </div>
              <div>
                Per-source artifacts write under:
                <span className="ml-2 font-mono">{repositoryStatus?.path ? `${repositoryStatus.path}/sources/<source_id>/` : "sources/<source_id>/"}</span>
              </div>
            </div>
          </SurfaceCard>
        </div>
      </div>
    </div>
  );
}

export function SummariesRatingsPage() {
  return <SourceCapturePage />;
}

export function JobHistoryPage() {
  const { dashboard } = useAppState();
  return (
    <div>
      <SectionHeader title="Job History" description="Recent extraction and source capture jobs." />
      <SurfaceCard className="p-0">
        <div className="max-h-[70vh] overflow-auto thin-scrollbar">
          <table className="data-table">
            <thead>
              <tr>
                <th>Type</th>
                <th>Job ID</th>
                <th>State</th>
                <th>Updated</th>
                <th>Message</th>
              </tr>
            </thead>
            <tbody>
              {(dashboard?.recent_jobs || []).map((job) => (
                <tr key={`${job.kind}-${job.job_id}-${job.updated_at}`}>
                  <td>{job.kind}</td>
                  <td className="font-mono">{job.job_id}</td>
                  <td>{job.state}</td>
                  <td className="font-mono">{job.updated_at || "-"}</td>
                  <td>{job.message || "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </SurfaceCard>
    </div>
  );
}

export function ManifestPage() {
  const { getRepositoryManifest } = useAppState();

  const [q, setQ] = useState("");
  const [fetchStatus, setFetchStatus] = useState("");
  const [detectedType, setDetectedType] = useState("");
  const [hasSummary, setHasSummary] = useState("");
  const [hasRating, setHasRating] = useState("");
  const [sortBy, setSortBy] = useState("id");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [limit, setLimit] = useState(50);
  const [offset, setOffset] = useState(0);
  const [selected, setSelected] = useState<RepositoryManifestRow | null>(null);

  const queryKey = [
    "manifest",
    q,
    fetchStatus,
    detectedType,
    hasSummary,
    hasRating,
    sortBy,
    sortDir,
    limit,
    offset,
  ];

  const manifestQuery = useQuery({
    queryKey,
    queryFn: async () => {
      const params = new URLSearchParams({
        q,
        fetch_status: fetchStatus,
        detected_type: detectedType,
        sort_by: sortBy,
        sort_dir: sortDir,
        limit: String(limit),
        offset: String(offset),
      });
      if (hasSummary === "true" || hasSummary === "false") {
        params.set("has_summary", hasSummary);
      }
      if (hasRating === "true" || hasRating === "false") {
        params.set("has_rating", hasRating);
      }
      return getRepositoryManifest(params);
    },
    staleTime: 1000,
  });

  const rows = manifestQuery.data?.rows || [];
  const total = manifestQuery.data?.total || 0;

  return (
    <div>
      <SectionHeader title="Manifest" description="Source-level repository inventory and enrichment status." />

      <SurfaceCard className="mb-4">
        <div className="grid gap-2 md:grid-cols-4 lg:grid-cols-8">
          <InputField label="Search" value={q} onChange={(event) => setQ(event.target.value)} placeholder="title, url, source doc" />
          <InputField label="Fetch Status" value={fetchStatus} onChange={(event) => setFetchStatus(event.target.value)} placeholder="success / failed / queued" />
          <InputField label="Detected Type" value={detectedType} onChange={(event) => setDetectedType(event.target.value)} placeholder="pdf / html / document" />
          <SelectField label="Has Summary" value={hasSummary} onChange={(event) => setHasSummary(event.target.value)}>
            <option value="">Any</option>
            <option value="true">Yes</option>
            <option value="false">No</option>
          </SelectField>
          <SelectField label="Has Rating" value={hasRating} onChange={(event) => setHasRating(event.target.value)}>
            <option value="">Any</option>
            <option value="true">Yes</option>
            <option value="false">No</option>
          </SelectField>
          <InputField label="Sort By" value={sortBy} onChange={(event) => setSortBy(event.target.value)} placeholder="id" />
          <SelectField label="Sort Dir" value={sortDir} onChange={(event) => setSortDir(event.target.value as "asc" | "desc")}>
            <option value="asc">asc</option>
            <option value="desc">desc</option>
          </SelectField>
          <InputField
            label="Page Size"
            type="number"
            min={1}
            max={200}
            value={String(limit)}
            onChange={(event) => setLimit(Math.max(1, Math.min(200, parseInt(event.target.value || "50", 10))))}
          />
        </div>
      </SurfaceCard>

      <div className="grid gap-4 xl:grid-cols-[1fr_320px]">
        <SurfaceCard className="overflow-auto p-0">
          <div className="max-h-[66vh] overflow-auto thin-scrollbar">
            <table className="data-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Title</th>
                  <th>URL</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Summary</th>
                  <th>Rating</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => (
                  <tr key={`${row.id}-${row.original_url}`} onClick={() => setSelected(row)} className="cursor-pointer">
                    <td className="font-mono text-primary">{row.id}</td>
                    <td>{row.title || "-"}</td>
                    <td className="max-w-[320px] truncate font-mono" title={row.original_url}>{row.original_url || "-"}</td>
                    <td>{row.detected_type || "-"}</td>
                    <td>{row.fetch_status || "-"}</td>
                    <td>{row.summary_status || "-"}</td>
                    <td>{row.rating_status || "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="flex items-center justify-between border-t border-outline-variant/30 bg-surface-container px-4 py-3">
            <div className="text-body-md text-on-surface-variant">Showing {rows.length} of {total}</div>
            <div className="flex gap-2">
              <Button disabled={offset <= 0} onClick={() => setOffset((prev) => Math.max(0, prev - limit))}>Prev</Button>
              <Button disabled={offset + limit >= total} onClick={() => setOffset((prev) => prev + limit)}>Next</Button>
            </div>
          </div>
        </SurfaceCard>

        <SurfaceCard>
          <div className="mb-2 text-title-sm font-semibold">Source Details</div>
          {!selected ? (
            <div className="text-body-md text-on-surface-variant">Select a manifest row to inspect source metadata.</div>
          ) : (
            <div className="space-y-2 text-body-md">
              <div className="font-mono text-label-sm text-primary">{selected.id}</div>
              <div className="font-semibold">{selected.title || "Untitled source"}</div>
              <div className="break-all font-mono text-label-sm text-on-surface-variant">{selected.original_url}</div>
              <div>Type: {selected.detected_type || "-"}</div>
              <div>Status: {selected.fetch_status || "-"}</div>
              <div>Summary: {selected.summary_status || "-"}</div>
              <div>Rating: {selected.rating_status || "-"}</div>
              <div>Rating Confidence: {String(selected.rating_confidence ?? "-")}</div>
              <div>Source Doc: {selected.source_document_name || "-"}</div>
              <div>Confidence/Notes: {selected.notes || "-"}</div>
              {selected.summary_text && (
                <div>
                  <div className="pt-2 text-label-sm text-on-surface-variant">Summary Text</div>
                  <div className="whitespace-pre-wrap">{selected.summary_text}</div>
                </div>
              )}
              {selected.rating_rationale && (
                <div>
                  <div className="pt-2 text-label-sm text-on-surface-variant">Rating Rationale</div>
                  <div className="whitespace-pre-wrap">{selected.rating_rationale}</div>
                </div>
              )}
              {selected.relevant_sections && (
                <div>
                  <div className="pt-2 text-label-sm text-on-surface-variant">Relevant Sections</div>
                  <div className="whitespace-pre-wrap">{selected.relevant_sections}</div>
                </div>
              )}
              <div className="pt-2 text-label-sm text-on-surface-variant">Files</div>
              <div className="flex flex-wrap gap-2">
                {selected.raw_file && <StatusBadge text="RAW" tone="neutral" />}
                {selected.rendered_file && <StatusBadge text="HTML" tone="neutral" />}
                {selected.rendered_pdf_file && <StatusBadge text="PDF" tone="neutral" />}
                {selected.markdown_file && <StatusBadge text="MD" tone="neutral" />}
              </div>
            </div>
          )}
        </SurfaceCard>
      </div>
    </div>
  );
}

function ResultTables({ pane }: { pane: "bibliography" | "citations" | "sentences" | "matches" }) {
  const { getRepositoryCitationData } = useAppState();

  const repositoryCitationQuery = useQuery({
    queryKey: ["repository-citation-data"],
    queryFn: () => getRepositoryCitationData(),
    staleTime: 3000,
  });

  const activeBibliography = repositoryCitationQuery.data?.bibliography;
  const activeCitations = repositoryCitationQuery.data?.citations;

  if (pane === "bibliography") {
    const rows = activeBibliography?.entries || [];
    return (
      <TableBlock
        title="Bibliography"
        empty={rows.length === 0}
        rows={
          <>
            <thead>
              <tr>
                <th>#</th>
                <th>Authors</th>
                <th>Title</th>
                <th>Year</th>
                <th>URL</th>
                <th>DOI</th>
                <th>Confidence</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={`${row.ref_number}-${row.title}`}>
                  <td>{row.ref_number || "-"}</td>
                  <td>{(row.authors || []).join("; ") || "-"}</td>
                  <td>{row.title || row.raw_text?.slice(0, 80) || "-"}</td>
                  <td>{row.year || "-"}</td>
                  <td className="max-w-[280px] truncate" title={row.url}>{row.url || "-"}</td>
                  <td>{row.doi || "-"}</td>
                  <td>{confidenceBadge(row.parse_confidence || 0)}</td>
                </tr>
              ))}
            </tbody>
          </>
        }
      />
    );
  }

  if (pane === "citations") {
    const rows = activeCitations?.citations || [];
    return (
      <TableBlock
        title="Citations"
        empty={rows.length === 0}
        rows={
          <>
            <thead>
              <tr>
                <th>Marker</th>
                <th>Ref Numbers</th>
                <th>Page</th>
                <th>Style</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={`${row.raw_marker}-${index}`}>
                  <td>{row.raw_marker}</td>
                  <td>{(row.ref_numbers || []).join(", ")}</td>
                  <td>{row.page_number || "-"}</td>
                  <td>{row.style}</td>
                </tr>
              ))}
            </tbody>
          </>
        }
      />
    );
  }

  if (pane === "sentences") {
    const rows = activeCitations?.sentences || [];
    return (
      <TableBlock
        title="Paragraph Context"
        empty={rows.length === 0}
        rows={
          <>
            <thead>
              <tr>
                <th>Page</th>
                <th>Paragraph Context</th>
                <th>Citations</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={`${(row.paragraph || row.text).slice(0, 20)}-${index}`}>
                  <td>{row.page_number || "-"}</td>
                  <td className="max-w-[540px] truncate" title={row.paragraph || row.text}>
                    {row.paragraph || row.text}
                  </td>
                  <td>{row.citation_ids?.length || 0}</td>
                </tr>
              ))}
            </tbody>
          </>
        }
      />
    );
  }

  const matches = activeCitations?.matches || [];
  const bibliographyEntries = activeBibliography?.entries || [];
  return (
    <TableBlock
      title="Matches"
      empty={matches.length === 0}
      rows={
        <>
          <thead>
            <tr>
              <th>Ref #</th>
              <th>Cited Entry</th>
              <th>Confidence</th>
              <th>Method</th>
            </tr>
          </thead>
          <tbody>
            {matches.map((match, index) => {
              const entry =
                match.matched_bib_entry_index !== null
                  ? bibliographyEntries[match.matched_bib_entry_index]
                  : null;
              return (
                <tr key={`${match.ref_number}-${index}`}>
                  <td>{match.ref_number}</td>
                  <td>{entry?.title || entry?.raw_text?.slice(0, 80) || "-"}</td>
                  <td>{confidenceBadge(match.match_confidence || 0)}</td>
                  <td>{match.match_method || "-"}</td>
                </tr>
              );
            })}
          </tbody>
        </>
      }
    />
  );
}

export function CitationsPage() {
  return (
    <div>
      <SectionHeader title="Citations" description="Inspect repository citation rows accumulated from processed documents." />
      <ResultTables pane="citations" />
    </div>
  );
}

export function BibliographyPage() {
  return (
    <div>
      <SectionHeader title="Bibliography" description="Inspect bibliography entries represented in repository citation data." />
      <ResultTables pane="bibliography" />
    </div>
  );
}

export function SentencesPage() {
  return (
    <div>
      <SectionHeader title="Paragraph Context" description="Inspect the paragraph block where each citation was found." />
      <ResultTables pane="sentences" />
    </div>
  );
}

export function MatchesPage() {
  return (
    <div>
      <SectionHeader title="Matches" description="Inspect citation-to-bibliography matching results." />
      <ResultTables pane="matches" />
    </div>
  );
}

export function ResearchPurposePage() {
  const { settingsDraft, setSettingsDraft, saveRepoSettings, savingSettings } = useAppState();

  return (
    <div>
      <SectionHeader title="Research Purpose" description="Set project-level framing used for LLM summaries and ratings." />
      <SurfaceCard>
        <TextAreaField
          label="Research Purpose"
          rows={8}
          value={settingsDraft.research_purpose}
          onChange={(event) =>
            setSettingsDraft((prev) => ({
              ...prev,
              research_purpose: event.target.value,
            }))
          }
        />
        <Button className="mt-3" variant="primary" disabled={savingSettings} onClick={() => saveRepoSettings()}>
          {savingSettings ? "Saving..." : "Save Research Purpose"}
        </Button>
      </SurfaceCard>
    </div>
  );
}

export function ProjectProfilePage() {
  const { profiles, sourceTaskDraft, setSourceTaskDraft, loadProfiles } = useAppState();

  return (
    <div>
      <SectionHeader
        title="Project Profile"
        description="Manage project profile YAML files used in source rating. The bundled default profile adapts its rubric to the saved research purpose."
      />
      <SurfaceCard>
        <SelectField
          label="Selected Profile"
          value={sourceTaskDraft.project_profile_name}
          onChange={(event) =>
            setSourceTaskDraft((prev) => ({
              ...prev,
              project_profile_name: event.target.value,
            }))
          }
        >
          <option value="">-- Use default project profile --</option>
          {profiles.map((profile) => (
            <option key={profile.filename} value={profile.filename}>
              {profile.name}
            </option>
          ))}
        </SelectField>
        <Button className="mt-3" onClick={() => loadProfiles()}>Refresh Profiles</Button>
      </SurfaceCard>
    </div>
  );
}

export function IngestionProfilesPage() {
  const {
    ingestionProfiles,
    ingestionProfileSuggestions,
    loadIngestionProfiles,
    saveIngestionProfile,
    deleteIngestionProfile,
    loadIngestionProfileSuggestions,
    acceptIngestionProfileSuggestion,
    rejectIngestionProfileSuggestion,
    repoMessage,
    repoError,
  } = useAppState();
  const [draft, setDraft] = useState<IngestionProfile>(() => createEmptyIngestionProfile());
  const [editingProfileId, setEditingProfileId] = useState("");

  const builtInProfiles = ingestionProfiles.filter((profile) => profile.built_in);
  const customProfiles = ingestionProfiles.filter((profile) => !profile.built_in);
  const pendingSuggestions = ingestionProfileSuggestions.filter(
    (suggestion) => suggestion.status === "pending",
  );

  const loadProfileIntoEditor = (profile: IngestionProfile, duplicate = false) => {
    const nextProfile = duplicate
      ? {
          ...profile,
          profile_id: `${profile.profile_id}_custom`,
          label: `${profile.label} Copy`,
          built_in: false,
        }
      : {
          ...profile,
          built_in: false,
        };
    setEditingProfileId(duplicate ? "" : profile.profile_id);
    setDraft(nextProfile);
  };

  const resetDraft = () => {
    setEditingProfileId("");
    setDraft(createEmptyIngestionProfile());
  };

  return (
    <div>
      <SectionHeader
        title="Ingestion Profiles"
        description="Manage profile-driven standardized markdown normalization for uploaded documents."
        right={
          <div className="flex flex-wrap gap-2">
            <Button
              onClick={() => {
                void Promise.all([
                  loadIngestionProfiles(),
                  loadIngestionProfileSuggestions(),
                ]);
              }}
            >
              Refresh
            </Button>
            <Button variant="secondary" onClick={resetDraft}>
              New Custom Profile
            </Button>
          </div>
        }
      />

      {(repoError || repoMessage) && (
        <div
          className={`mb-4 rounded-md px-3 py-2 text-body-md ${
            repoError ? "bg-error/10 text-error" : "bg-surface-container text-on-surface"
          }`}
        >
          {repoError || repoMessage}
        </div>
      )}

      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_minmax(0,1.1fr)]">
        <div className="space-y-4">
          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
              Built-in Profiles
            </div>
            <div className="space-y-3">
              {builtInProfiles.map((profile) => (
                <div key={profile.profile_id} className="rounded-md bg-surface-container-low px-3 py-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div>
                      <div className="text-body-md font-semibold">{profile.label}</div>
                      <div className="font-mono text-label-sm text-on-surface-variant">
                        {profile.profile_id}
                      </div>
                    </div>
                    <Button onClick={() => loadProfileIntoEditor(profile, true)}>Copy To Custom</Button>
                  </div>
                  {profile.description && (
                    <div className="mt-2 text-body-md text-on-surface-variant">{profile.description}</div>
                  )}
                  <div className="mt-2 text-label-sm text-on-surface-variant">
                    File hints: {(profile.file_type_hints || []).join(", ") || "None"}
                  </div>
                </div>
              ))}
            </div>
          </SurfaceCard>

          <SurfaceCard>
            <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
              Custom Profiles
            </div>
            {customProfiles.length === 0 ? (
              <div className="text-body-md text-on-surface-variant">
                No custom profiles yet.
              </div>
            ) : (
              <div className="space-y-3">
                {customProfiles.map((profile) => (
                  <div key={profile.profile_id} className="rounded-md bg-surface-container-low px-3 py-3">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div>
                        <div className="text-body-md font-semibold">{profile.label}</div>
                        <div className="font-mono text-label-sm text-on-surface-variant">
                          {profile.profile_id}
                        </div>
                      </div>
                      <div className="flex flex-wrap gap-2">
                        <Button onClick={() => loadProfileIntoEditor(profile)}>Edit</Button>
                        <Button
                          variant="danger"
                          onClick={() => {
                            void deleteIngestionProfile(profile.profile_id);
                            if (editingProfileId === profile.profile_id) {
                              resetDraft();
                            }
                          }}
                        >
                          Delete
                        </Button>
                      </div>
                    </div>
                    {profile.description && (
                      <div className="mt-2 text-body-md text-on-surface-variant">{profile.description}</div>
                    )}
                    <div className="mt-2 text-label-sm text-on-surface-variant">
                      Confidence threshold: {profile.confidence_threshold}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </SurfaceCard>
        </div>

        <SurfaceCard>
          <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
            {editingProfileId ? `Edit ${editingProfileId}` : "New Custom Profile"}
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            <InputField
              label="Profile ID"
              value={draft.profile_id}
              onChange={(event) =>
                setDraft((prev) => ({
                  ...prev,
                  profile_id: event.target.value,
                }))
              }
            />
            <InputField
              label="Label"
              value={draft.label}
              onChange={(event) =>
                setDraft((prev) => ({
                  ...prev,
                  label: event.target.value,
                }))
              }
            />
            <InputField
              label="Confidence Threshold"
              type="number"
              min={0}
              max={1}
              step={0.05}
              value={String(draft.confidence_threshold)}
              onChange={(event) =>
                setDraft((prev) => ({
                  ...prev,
                  confidence_threshold: Number.parseFloat(event.target.value || "0.6"),
                }))
              }
            />
            <TextAreaField
              className="md:col-span-2"
              label="Description"
              rows={3}
              value={draft.description}
              onChange={(event) =>
                setDraft((prev) => ({
                  ...prev,
                  description: event.target.value,
                }))
              }
            />
            <TextAreaField
              label="File Type Hints"
              rows={4}
              value={joinMultilineInput(draft.file_type_hints)}
              onChange={(event) =>
                setDraft((prev) => ({
                  ...prev,
                  file_type_hints: splitMultilineInput(event.target.value),
                }))
              }
            />
            <TextAreaField
              label="Reference Heading Patterns"
              rows={4}
              value={joinMultilineInput(draft.reference_heading_patterns)}
              onChange={(event) =>
                setDraft((prev) => ({
                  ...prev,
                  reference_heading_patterns: splitMultilineInput(event.target.value),
                }))
              }
            />
            <TextAreaField
              label="Citation Marker Patterns"
              rows={4}
              value={joinMultilineInput(draft.citation_marker_patterns)}
              onChange={(event) =>
                setDraft((prev) => ({
                  ...prev,
                  citation_marker_patterns: splitMultilineInput(event.target.value),
                }))
              }
            />
            <TextAreaField
              label="Bibliography Split Patterns"
              rows={4}
              value={joinMultilineInput(draft.bibliography_split_patterns)}
              onChange={(event) =>
                setDraft((prev) => ({
                  ...prev,
                  bibliography_split_patterns: splitMultilineInput(event.target.value),
                }))
              }
            />
            <TextAreaField
              className="md:col-span-2"
              label="LLM Guidance"
              rows={5}
              value={draft.llm_guidance}
              onChange={(event) =>
                setDraft((prev) => ({
                  ...prev,
                  llm_guidance: event.target.value,
                }))
              }
            />
            <TextAreaField
              className="md:col-span-2"
              label="Notes"
              rows={4}
              value={joinMultilineInput(draft.notes)}
              onChange={(event) =>
                setDraft((prev) => ({
                  ...prev,
                  notes: splitMultilineInput(event.target.value),
                }))
              }
            />
          </div>

          <div className="mt-4 flex flex-wrap gap-2">
            <Button
              variant="primary"
              disabled={!draft.profile_id.trim() || !draft.label.trim()}
              onClick={() =>
                void saveIngestionProfile({
                  ...draft,
                  built_in: false,
                })
              }
            >
              Save Profile
            </Button>
            <Button variant="secondary" onClick={resetDraft}>
              Reset Editor
            </Button>
          </div>
        </SurfaceCard>
      </div>

      <SurfaceCard className="mt-4">
        <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
          Pending Suggestions
        </div>
        {pendingSuggestions.length === 0 ? (
          <div className="text-body-md text-on-surface-variant">
            No pending profile suggestions.
          </div>
        ) : (
          <div className="space-y-3">
            {pendingSuggestions.map((suggestion) => (
              <div key={suggestion.suggestion_id} className="rounded-md bg-surface-container-low px-3 py-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <div className="text-body-md font-semibold">
                      {suggestion.proposed_profile.label}
                    </div>
                    <div className="font-mono text-label-sm text-on-surface-variant">
                      {suggestion.proposed_profile.profile_id}
                    </div>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <Button onClick={() => void acceptIngestionProfileSuggestion(suggestion.suggestion_id)}>
                      Accept
                    </Button>
                    <Button
                      variant="danger"
                      onClick={() => void rejectIngestionProfileSuggestion(suggestion.suggestion_id)}
                    >
                      Reject
                    </Button>
                  </div>
                </div>
                {suggestion.reason && (
                  <div className="mt-2 text-body-md text-on-surface-variant">
                    {suggestion.reason}
                  </div>
                )}
                {suggestion.example_filename && (
                  <div className="mt-2 text-label-sm text-on-surface-variant">
                    Example: <span className="font-mono">{suggestion.example_filename}</span>
                  </div>
                )}
                {suggestion.example_excerpt && (
                  <pre className="mt-2 thin-scrollbar max-h-[18vh] overflow-auto rounded-md bg-surface px-3 py-2 text-label-sm">
                    {suggestion.example_excerpt}
                  </pre>
                )}
              </div>
            ))}
          </div>
        )}
      </SurfaceCard>
    </div>
  );
}


export function LlmBackendPage() {
  const { settingsDraft, setSettingsDraft, models, loadModels, loadingModels, saveRepoSettings, savingSettings } = useAppState();

  return (
    <div>
      <SectionHeader title="LLM Backend" description="Configure per-repository LLM backend settings." />

      <SurfaceCard>
        <div className="grid gap-3 md:grid-cols-2">
          <SelectField
            label="Backend Type"
            value={settingsDraft.llm_backend.kind}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: { ...prev.llm_backend, kind: event.target.value },
              }))
            }
          >
            <option value="ollama">Ollama (Local)</option>
            <option value="openai">OpenAI-Compatible</option>
          </SelectField>

          <InputField
            label="Base URL"
            value={settingsDraft.llm_backend.base_url}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: { ...prev.llm_backend, base_url: event.target.value },
              }))
            }
          />

          <InputField
            label="API Key"
            type="password"
            value={settingsDraft.llm_backend.api_key}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: { ...prev.llm_backend, api_key: event.target.value },
              }))
            }
          />

          <div className="grid gap-2">
            <span className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Model</span>
            <div className="flex gap-2">
              <select
                className="min-w-0 flex-1 rounded-md border border-outline-variant bg-surface-container-lowest px-3 py-2 text-body-md"
                value={settingsDraft.llm_backend.model}
                onChange={(event) =>
                  setSettingsDraft((prev) => ({
                    ...prev,
                    llm_backend: { ...prev.llm_backend, model: event.target.value },
                  }))
                }
              >
                <option value="">-- Load models first --</option>
                {models.map((model) => (
                  <option key={model} value={model}>{model}</option>
                ))}
              </select>
              <Button onClick={() => loadModels()}>{loadingModels ? "Loading..." : "Load Models"}</Button>
            </div>
          </div>

          <InputField
            label="Temperature"
            type="number"
            step="0.1"
            min={0}
            max={2}
            value={String(settingsDraft.llm_backend.temperature)}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  temperature: parseFloat(event.target.value || "0"),
                },
              }))
            }
          />

          <SelectField
            label="Think Mode"
            value={settingsDraft.llm_backend.think_mode}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  think_mode: event.target.value as "default" | "think" | "no_think",
                },
              }))
            }
          >
            <option value="default">Default</option>
            <option value="think">Think</option>
            <option value="no_think">No Think</option>
          </SelectField>

          <InputField
            label="Context Window"
            type="number"
            min={2048}
            max={131072}
            step={1024}
            value={String(settingsDraft.llm_backend.num_ctx)}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  num_ctx: parseInt(event.target.value || "8192", 10),
                },
              }))
            }
          />

          <InputField
            label="Max Source Chars"
            type="number"
            min={0}
            max={120000}
            step={1000}
            value={String(settingsDraft.llm_backend.max_source_chars)}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  max_source_chars: parseInt(event.target.value || "0", 10),
                },
              }))
            }
          />

          <InputField
            label="LLM Timeout (sec)"
            type="number"
            min={30}
            max={1800}
            step={30}
            value={String(settingsDraft.llm_backend.llm_timeout)}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  llm_timeout: parseFloat(event.target.value || "300"),
                },
              }))
            }
          />
        </div>

        <label className="mt-3 flex items-center gap-2 text-body-md">
          <input
            checked={settingsDraft.use_llm}
            type="checkbox"
            onChange={(event) => setSettingsDraft((prev) => ({ ...prev, use_llm: event.target.checked }))}
          />
          Use LLM-assisted features by default
        </label>

        <Button className="mt-3" variant="primary" disabled={savingSettings} onClick={() => saveRepoSettings()}>
          {savingSettings ? "Saving..." : "Save Backend Settings"}
        </Button>
      </SurfaceCard>
    </div>
  );
}

export function RepositorySettingsPage() {
  const { settingsDraft, setSettingsDraft, saveRepoSettings, savingSettings, repositoryStatus } = useAppState();

  return (
    <div>
      <SectionHeader title="Repository Settings" description="Project-level runtime controls and repository metadata." />

      <SurfaceCard>
        <InputField
          label="Fetch Delay (seconds)"
          type="number"
          min={1}
          max={10}
          step={0.5}
          value={String(settingsDraft.fetch_delay)}
          onChange={(event) =>
            setSettingsDraft((prev) => ({
              ...prev,
              fetch_delay: parseFloat(event.target.value || "2"),
            }))
          }
        />

        <div className="mt-3 text-body-md text-on-surface-variant">
          Path: <span className="font-mono">{repositoryStatus?.path || "-"}</span>
        </div>
        <div className="text-body-md text-on-surface-variant">
          Schema Version: <span className="font-mono">{repositoryStatus?.schema_version || "-"}</span>
        </div>

        <Button className="mt-3" variant="primary" disabled={savingSettings} onClick={() => saveRepoSettings()}>
          {savingSettings ? "Saving..." : "Save Repository Settings"}
        </Button>
      </SurfaceCard>
    </div>
  );
}

export function AdvancedSettingsPage() {
  const { settingsDraft, sourceTaskDraft, repositoryStatus } = useAppState();

  return (
    <div>
      <SectionHeader title="Advanced" description="Operational details and raw state for debugging and review." />
      <SurfaceCard>
        <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Repository Status</div>
        <pre className="thin-scrollbar max-h-[24vh] overflow-auto rounded-md bg-surface-container-low p-3 text-label-sm">
          {JSON.stringify(repositoryStatus, null, 2)}
        </pre>

        <div className="mb-2 mt-4 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Settings Draft</div>
        <pre className="thin-scrollbar max-h-[24vh] overflow-auto rounded-md bg-surface-container-low p-3 text-label-sm">
          {JSON.stringify(settingsDraft, null, 2)}
        </pre>

        <div className="mb-2 mt-4 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Source Task Draft</div>
        <pre className="thin-scrollbar max-h-[24vh] overflow-auto rounded-md bg-surface-container-low p-3 text-label-sm">
          {JSON.stringify(sourceTaskDraft, null, 2)}
        </pre>
      </SurfaceCard>
    </div>
  );
}
