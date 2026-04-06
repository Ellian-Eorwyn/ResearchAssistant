import {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type Dispatch,
  type PropsWithChildren,
  type SetStateAction,
} from "react";
import { useQuery } from "@tanstack/react-query";

import { api } from "../api/client";
import type {
  BibliographyResult,
  IngestionProfile,
  IngestionProfileSuggestion,
  ExportResult,
  JobStatusResponse,
  ModelsResponse,
  ProjectProfile,
  RepoSettings,
  RepositoryCitationDataResponse,
  RepositoryDashboardResponse,
  RepositoryDocumentImportRecord,
  RepositoryManifestResponse,
  RepositorySourceTaskRequest,
  RepositoryStatusResponse,
  SourceDownloadStatus,
} from "../api/types";

import { createContext } from "react";

const DEFAULT_SETTINGS: RepoSettings = {
  llm_backend: {
    kind: "ollama",
    base_url: "http://localhost:11434",
    api_key: "",
    model: "",
    temperature: 0,
    think_mode: "default",
    num_ctx: 8192,
    max_source_chars: 0,
    llm_timeout: 300,
  },
  use_llm: false,
  research_purpose: "",
  default_project_profile_name: "",
  fetch_delay: 2,
};

const DEFAULT_SOURCE_TASKS: RepositorySourceTaskRequest = {
  rerun_failed_only: false,
  run_download: true,
  run_convert: false,
  run_catalog: true,
  run_citation_verify: false,
  run_llm_cleanup: false,
  run_llm_title: false,
  run_llm_summary: false,
  run_llm_rating: false,
  force_redownload: false,
  force_convert: false,
  force_catalog: false,
  force_citation_verify: false,
  force_llm_cleanup: false,
  force_title: false,
  force_summary: false,
  force_rating: false,
  project_profile_name: "",
  include_raw_file: true,
  include_rendered_html: true,
  include_rendered_pdf: true,
  include_markdown: true,
  scope: "queued",
  import_id: "",
};

const DEFAULT_PROJECT_PROFILE_FILENAME = "default_project_profile.yaml";

export const STAGE_NAMES: Record<string, string> = {
  ingesting: "Ingesting document",
  detecting_references: "Detecting references",
  parsing_bibliography: "Parsing bibliography",
  detecting_citations: "Detecting citations",
  extracting_sentences: "Extracting paragraph context",
  matching_citations: "Matching citations",
  exporting: "Exporting CSV",
};

interface AppStateValue {
  repoLoaded: boolean;
  repositoryStatus: RepositoryStatusResponse | null;
  dashboard: RepositoryDashboardResponse | null;
  repoSettings: RepoSettings;
  settingsDraft: RepoSettings;
  setSettingsDraft: Dispatch<SetStateAction<RepoSettings>>;
  sourceTaskDraft: RepositorySourceTaskRequest;
  setSourceTaskDraft: Dispatch<SetStateAction<RepositorySourceTaskRequest>>;
  processingJobId: string | null;
  sourceTaskJobId: string | null;
  files: File[];
  hasSourceUrls: boolean;
  processingStatus: JobStatusResponse | null;
  sourceStatus: SourceDownloadStatus | null;
  profiles: ProjectProfile[];
  documentImports: RepositoryDocumentImportRecord[];
  ingestionProfiles: IngestionProfile[];
  ingestionProfileSuggestions: IngestionProfileSuggestion[];
  defaultIngestionProfileId: string;
  selectedIngestionProfileId: string;
  setSelectedIngestionProfileId: Dispatch<SetStateAction<string>>;
  selectedReprocessImportIds: string[];
  setSelectedReprocessImportIds: Dispatch<SetStateAction<string[]>>;
  models: string[];
  lastRepositoryPath: string;
  gateMessage: string;
  gateError: string;
  repoMessage: string;
  repoError: string;
  processingError: string;
  sourceError: string;
  savingSettings: boolean;
  loadingModels: boolean;
  processingRunning: boolean;
  sourceRunning: boolean;
  sourceStopping: boolean;
  pickRepositoryDirectory: (mode: "open" | "create" | "export", initialPath?: string) => Promise<string>;
  openRepository: (path: string) => Promise<boolean>;
  createRepository: (path: string) => Promise<boolean>;
  switchRepository: () => void;
  addFiles: (incoming: FileList | File[]) => void;
  removeFileAtIndex: (index: number) => void;
  clearFiles: () => void;
  ingestSeedFiles: (files: File[]) => Promise<void>;
  ingestRepositoryDocuments: (files: File[]) => Promise<void>;
  importSourceList: (file: File | null) => Promise<void>;
  mergeRepositories: (sourcePaths: string[]) => Promise<void>;
  rebuildRepositoryOutputs: () => Promise<void>;
  clearRepositoryCitations: () => Promise<void>;
  cleanupRepositoryLayout: () => Promise<void>;
  startProcessing: () => Promise<void>;
  reprocessStoredDocuments: () => Promise<void>;
  runSourceTasks: (rerunFailedOnly: boolean) => Promise<void>;
  trackSourceTaskJob: (jobId: string | null, message?: string) => void;
  cancelSourceTasks: () => Promise<void>;
  saveRepoSettings: (nextSettings?: RepoSettings) => Promise<void>;
  loadModels: () => Promise<void>;
  loadProfiles: () => Promise<void>;
  loadDocumentImports: () => Promise<void>;
  loadIngestionProfiles: () => Promise<void>;
  saveIngestionProfile: (profile: IngestionProfile) => Promise<void>;
  deleteIngestionProfile: (profileId: string) => Promise<void>;
  loadIngestionProfileSuggestions: () => Promise<void>;
  acceptIngestionProfileSuggestion: (suggestionId: string) => Promise<void>;
  rejectIngestionProfileSuggestion: (suggestionId: string) => Promise<void>;
  uploadProfile: (file: File | null) => Promise<void>;
  refreshDashboard: () => Promise<void>;
  getRepositoryCitationData: () => Promise<RepositoryCitationDataResponse>;
  getRepositoryManifest: (params: URLSearchParams) => Promise<RepositoryManifestResponse>;
  warnings: Array<{ type: "warning" | "error"; stage: string; message: string }>;
}

const AppStateContext = createContext<AppStateValue | null>(null);

function fileSignature(file: File): string {
  return [file.name, file.size, file.lastModified].join("::");
}

function normalizeTemperature(value: number): number {
  if (Number.isNaN(value)) return 0;
  return Math.max(0, Math.min(2, value));
}

function normalizeThinkMode(value: string): "default" | "think" | "no_think" {
  if (value === "think" || value === "no_think") return value;
  return "default";
}

function normalizeRepoSettingsDraft(settings: RepoSettings): RepoSettings {
  return {
    ...settings,
    llm_backend: {
      ...settings.llm_backend,
      temperature: normalizeTemperature(settings.llm_backend.temperature),
      think_mode: normalizeThinkMode(settings.llm_backend.think_mode),
      num_ctx: Math.max(2048, Math.min(131072, settings.llm_backend.num_ctx)),
      max_source_chars: Math.max(0, Math.min(120000, settings.llm_backend.max_source_chars)),
      llm_timeout: Math.max(30, Math.min(1800, settings.llm_backend.llm_timeout)),
    },
  };
}

function normalizeSourceUrlForDedupe(value: string): string {
  const cleaned = String(value || "")
    .trim()
    .replace(/^["'`<]+/, "")
    .replace(/[>"'`]+$/, "");
  if (!cleaned) return "";

  const candidate = cleaned.includes("://") ? cleaned : `https://${cleaned}`;
  try {
    const parsed = new URL(candidate);
    const filteredParams = Array.from(parsed.searchParams.entries())
      .filter(([key]) => {
        const lower = key.toLowerCase();
        return (
          lower !== "gclid" &&
          lower !== "fbclid" &&
          lower !== "msclkid" &&
          !lower.startsWith("utm_")
        );
      })
      .sort((a, b) => {
        if (a[0] === b[0]) return a[1].localeCompare(b[1]);
        return a[0].localeCompare(b[0]);
      });

    const query = new URLSearchParams(filteredParams).toString();
    const canonicalPath = encodeURI(decodeURI(parsed.pathname || "/"));
    return (
      `${parsed.protocol.toLowerCase()}//${parsed.host.toLowerCase()}` +
      `${canonicalPath}` +
      (query ? `?${query}` : "")
    );
  } catch {
    return candidate.toLowerCase();
  }
}

function countUniqueSourceUrls(entries: Array<{ url?: string; doi?: string }>): number {
  const seen = new Set<string>();
  entries.forEach((entry) => {
    const url = (entry.url || "").trim();
    const doi = (entry.doi || "").trim();
    const candidate = url || (doi ? `https://doi.org/${doi}` : "");
    if (!candidate) return;
    const key = normalizeSourceUrlForDedupe(candidate) || candidate.toLowerCase();
    seen.add(key);
  });
  return seen.size;
}

function isNotFoundError(error: unknown): boolean {
  const message = String((error as Error | null)?.message || "").toLowerCase();
  return message.includes("404") || message.includes("not found");
}

function repositoryPreprocessInFlight(status: JobStatusResponse | null): boolean {
  const preprocessState = String(status?.repository_preprocess_state || "").toLowerCase();
  return preprocessState === "pending" || preprocessState === "running";
}

function repositoryFinalizeInFlight(status: JobStatusResponse | null): boolean {
  const finalizeState = String(status?.repository_finalize_state || "").toLowerCase();
  return finalizeState === "pending" || finalizeState === "running";
}

export function AppStateProvider({ children }: PropsWithChildren) {
  const [repoLoaded, setRepoLoaded] = useState(false);
  const [repositoryStatus, setRepositoryStatus] = useState<RepositoryStatusResponse | null>(null);
  const [dashboard, setDashboard] = useState<RepositoryDashboardResponse | null>(null);
  const [repoSettings, setRepoSettings] = useState<RepoSettings>(DEFAULT_SETTINGS);
  const [settingsDraft, setSettingsDraft] = useState<RepoSettings>(DEFAULT_SETTINGS);
  const [sourceTaskDraft, setSourceTaskDraft] =
    useState<RepositorySourceTaskRequest>(DEFAULT_SOURCE_TASKS);
  const [models, setModels] = useState<string[]>([]);
  const [profiles, setProfiles] = useState<ProjectProfile[]>([]);
  const [documentImports, setDocumentImports] = useState<RepositoryDocumentImportRecord[]>([]);
  const [ingestionProfiles, setIngestionProfiles] = useState<IngestionProfile[]>([]);
  const [ingestionProfileSuggestions, setIngestionProfileSuggestions] = useState<
    IngestionProfileSuggestion[]
  >([]);
  const [defaultIngestionProfileId, setDefaultIngestionProfileId] = useState("");
  const [selectedIngestionProfileId, setSelectedIngestionProfileId] = useState("");
  const [selectedReprocessImportIds, setSelectedReprocessImportIds] = useState<string[]>([]);
  const [lastRepositoryPath, setLastRepositoryPath] = useState("");

  const [processingJobId, setProcessingJobId] = useState<string | null>(null);
  const [sourceTaskJobId, setSourceTaskJobId] = useState<string | null>(null);
  const [files, setFiles] = useState<File[]>([]);
  const [hasSourceUrls, setHasSourceUrls] = useState(false);

  const [processingStatus, setProcessingStatus] = useState<JobStatusResponse | null>(null);
  const [sourceStatus, setSourceStatus] = useState<SourceDownloadStatus | null>(null);

  const [gateMessage, setGateMessage] = useState("");
  const [gateError, setGateError] = useState("");
  const [repoMessage, setRepoMessage] = useState("");
  const [repoError, setRepoError] = useState("");
  const [processingError, setProcessingError] = useState("");
  const [sourceError, setSourceError] = useState("");

  const [savingSettings, setSavingSettings] = useState(false);
  const [loadingModels, setLoadingModels] = useState(false);
  const [processingPolling, setProcessingPolling] = useState(false);
  const [sourcePolling, setSourcePolling] = useState(false);

  const processingRunning =
    processingStatus?.current_stage !== undefined &&
    !(
      (processingStatus.current_stage === "completed" ||
        processingStatus.current_stage === "failed") &&
      !repositoryPreprocessInFlight(processingStatus) &&
      !repositoryFinalizeInFlight(processingStatus)
    );
  const sourceRunning =
    sourceStatus?.state === "running" ||
    sourceStatus?.state === "cancelling" ||
    repositoryStatus?.download_state === "running" ||
    repositoryStatus?.download_state === "cancelling";
  const sourceStopping =
    sourceStatus?.state === "cancelling" || repositoryStatus?.download_state === "cancelling";

  const resetJobScopedState = useCallback(() => {
    setProcessingJobId(null);
    setSourceTaskJobId(null);
    setProcessingStatus(null);
    setSourceStatus(null);
    setHasSourceUrls(false);
    setProcessingPolling(false);
    setSourcePolling(false);
    setProcessingError("");
    setSourceError("");
  }, []);

  const loadRepoSettings = useCallback(async () => {
    try {
      const settings = await api.getRepoSettings();
      setRepoSettings(settings);
      setSettingsDraft(settings);
      setSourceTaskDraft((prev) => ({
        ...prev,
        run_catalog: Boolean(settings.use_llm) || prev.run_catalog,
        run_citation_verify: Boolean(settings.use_llm) || prev.run_citation_verify,
        run_llm_summary: Boolean(settings.use_llm),
        project_profile_name:
          settings.default_project_profile_name.trim() || prev.project_profile_name,
      }));
    } catch (error) {
      setRepoError(String((error as Error).message || "Failed to load repository settings"));
    }
  }, []);

  const refreshDashboard = useCallback(async () => {
    if (!repoLoaded) return;
    try {
      const payload = await api.getRepositoryDashboard();
      setDashboard(payload);
      setRepositoryStatus(payload.status);
      setHasSourceUrls((payload.status?.total_sources || 0) > 0);
    } catch (error) {
      if (!isNotFoundError(error)) {
        setRepoError(String((error as Error).message || "Failed to load dashboard"));
      }
    }
  }, [repoLoaded]);

  const loadProfiles = useCallback(async () => {
    if (!repoLoaded) {
      setProfiles([]);
      return;
    }
    try {
      const loadedProfiles = await api.getProjectProfiles();
      setProfiles(loadedProfiles);
    } catch {
      setProfiles([]);
    }
  }, [repoLoaded]);

  useEffect(() => {
    if (!repoLoaded) return;
    setSourceTaskDraft((prev) => {
      const preferred = settingsDraft.default_project_profile_name.trim();
      if (preferred && profiles.some((profile) => profile.filename === preferred)) {
        return prev.project_profile_name === preferred
          ? prev
          : { ...prev, project_profile_name: preferred };
      }
      if (prev.project_profile_name && profiles.some((profile) => profile.filename === prev.project_profile_name)) {
        return prev;
      }
      const defaultProfile = profiles.find(
        (profile) => profile.filename === DEFAULT_PROJECT_PROFILE_FILENAME,
      );
      if (!defaultProfile) {
        return prev.project_profile_name ? { ...prev, project_profile_name: "" } : prev;
      }
      return { ...prev, project_profile_name: defaultProfile.filename };
    });
  }, [profiles, repoLoaded, settingsDraft.default_project_profile_name]);

  const loadDocumentImports = useCallback(async () => {
    if (!repoLoaded) {
      setDocumentImports([]);
      setSelectedReprocessImportIds([]);
      return;
    }
    try {
      const response = await api.getRepositoryDocumentImports();
      const imports = response.imports || [];
      setDocumentImports(imports);
      setSelectedReprocessImportIds((current) =>
        current.filter((importId) => imports.some((item) => item.import_id === importId)),
      );
    } catch {
      setDocumentImports([]);
      setSelectedReprocessImportIds([]);
    }
  }, [repoLoaded]);

  const loadIngestionProfiles = useCallback(async () => {
    if (!repoLoaded) {
      setIngestionProfiles([]);
      setDefaultIngestionProfileId("");
      return;
    }
    try {
      const response = await api.getIngestionProfiles();
      setIngestionProfiles(response.profiles || []);
      setDefaultIngestionProfileId(response.default_profile_id || "");
      setSelectedIngestionProfileId((current) => {
        if (!current) return current;
        return (response.profiles || []).some((profile) => profile.profile_id === current)
          ? current
          : "";
      });
    } catch {
      setIngestionProfiles([]);
      setDefaultIngestionProfileId("");
    }
  }, [repoLoaded]);

  const loadIngestionProfileSuggestions = useCallback(async () => {
    if (!repoLoaded) {
      setIngestionProfileSuggestions([]);
      return;
    }
    try {
      const response = await api.getIngestionProfileSuggestions();
      setIngestionProfileSuggestions(response.suggestions || []);
    } catch {
      setIngestionProfileSuggestions([]);
    }
  }, [repoLoaded]);

  const saveIngestionProfile = useCallback(
    async (profile: IngestionProfile) => {
      setRepoError("");
      try {
        const trimmedId = profile.profile_id.trim();
        const payload: IngestionProfile = {
          ...profile,
          profile_id: trimmedId,
          label: profile.label.trim(),
          description: profile.description.trim(),
          llm_guidance: profile.llm_guidance.trim(),
          built_in: false,
        };
        const existingCustom = ingestionProfiles.find(
          (item) => item.profile_id === trimmedId && !item.built_in,
        );
        const response = existingCustom
          ? await api.updateIngestionProfile(trimmedId, payload)
          : await api.saveIngestionProfile(payload);
        setRepoMessage(response.message || `Saved ingestion profile ${trimmedId}.`);
        await loadIngestionProfiles();
      } catch (error) {
        setRepoError(String((error as Error).message || "Failed to save ingestion profile"));
      }
    },
    [ingestionProfiles, loadIngestionProfiles],
  );

  const deleteIngestionProfile = useCallback(
    async (profileId: string) => {
      setRepoError("");
      try {
        const response = await api.deleteIngestionProfile(profileId);
        setRepoMessage(response.message || `Deleted ingestion profile ${profileId}.`);
        if (selectedIngestionProfileId === profileId) {
          setSelectedIngestionProfileId("");
        }
        await loadIngestionProfiles();
      } catch (error) {
        setRepoError(String((error as Error).message || "Failed to delete ingestion profile"));
      }
    },
    [loadIngestionProfiles, selectedIngestionProfileId],
  );

  const acceptIngestionProfileSuggestion = useCallback(
    async (suggestionId: string) => {
      setRepoError("");
      try {
        const response = await api.acceptIngestionProfileSuggestion(suggestionId);
        setRepoMessage(response.message || `Accepted suggestion ${suggestionId}.`);
        await Promise.all([loadIngestionProfiles(), loadIngestionProfileSuggestions()]);
      } catch (error) {
        setRepoError(String((error as Error).message || "Failed to accept suggestion"));
      }
    },
    [loadIngestionProfileSuggestions, loadIngestionProfiles],
  );

  const rejectIngestionProfileSuggestion = useCallback(
    async (suggestionId: string) => {
      setRepoError("");
      try {
        const response = await api.rejectIngestionProfileSuggestion(suggestionId);
        setRepoMessage(response.message || `Rejected suggestion ${suggestionId}.`);
        await loadIngestionProfileSuggestions();
      } catch (error) {
        setRepoError(String((error as Error).message || "Failed to reject suggestion"));
      }
    },
    [loadIngestionProfileSuggestions],
  );

  const onRepoLoaded = useCallback(
    async (status: RepositoryStatusResponse) => {
      setRepoLoaded(true);
      setRepositoryStatus(status);
      setLastRepositoryPath(status.path || "");
      setGateError("");
      setGateMessage("");
      setRepoError("");
      setRepoMessage(status.message || "");
      setFiles([]);
      resetJobScopedState();
      setHasSourceUrls((status.total_sources || 0) > 0);

      await Promise.all([
        loadRepoSettings(),
        loadProfiles(),
        loadDocumentImports(),
        loadIngestionProfiles(),
        loadIngestionProfileSuggestions(),
      ]);
      await refreshDashboard();
    },
    [
      loadDocumentImports,
      loadIngestionProfileSuggestions,
      loadIngestionProfiles,
      loadProfiles,
      loadRepoSettings,
      refreshDashboard,
      resetJobScopedState,
    ],
  );

  const openRepository = useCallback(
    async (path: string): Promise<boolean> => {
      const trimmed = path.trim();
      if (!trimmed) {
        setGateError("Enter a path first.");
        return false;
      }
      setGateError("");
      setGateMessage("Opening repository...");
      try {
        const status = await api.attachRepository(trimmed);
        await onRepoLoaded(status);
        return true;
      } catch (error) {
        setGateError(String((error as Error).message || "Failed to open repository"));
        return false;
      } finally {
        setGateMessage("");
      }
    },
    [onRepoLoaded],
  );

  const createRepository = useCallback(
    async (path: string): Promise<boolean> => {
      const trimmed = path.trim();
      if (!trimmed) {
        setGateError("Enter a path for the new repository.");
        return false;
      }
      setGateError("");
      setGateMessage("Creating repository...");
      try {
        const status = await api.createRepository(trimmed);
        await onRepoLoaded(status);
        return true;
      } catch (error) {
        setGateError(String((error as Error).message || "Failed to create repository"));
        return false;
      } finally {
        setGateMessage("");
      }
    },
    [onRepoLoaded],
  );

  const pickRepositoryDirectory = useCallback(
    async (mode: "open" | "create" | "export", initialPath = ""): Promise<string> => {
      setGateError("");
      setRepoError("");
      const seed = initialPath.trim() || lastRepositoryPath.trim();
      try {
        const result = await api.pickRepositoryDirectory(mode, seed);
        return (result.path || "").trim();
      } catch (error) {
        const message = String((error as Error).message || "Folder picker unavailable");
        if (repoLoaded) {
          setRepoError(message);
        } else {
          setGateError(message);
        }
        return "";
      }
    },
    [lastRepositoryPath, repoLoaded],
  );

  const switchRepository = useCallback(() => {
    setRepoLoaded(false);
    setRepositoryStatus(null);
    setDashboard(null);
    setFiles([]);
    setProfiles([]);
    setDocumentImports([]);
    setIngestionProfiles([]);
    setIngestionProfileSuggestions([]);
    setDefaultIngestionProfileId("");
    setSelectedIngestionProfileId("");
    setSelectedReprocessImportIds([]);
    setModels([]);
    setRepoMessage("");
    setRepoError("");
    setGateError("");
    setGateMessage("");
    setRepoSettings(DEFAULT_SETTINGS);
    setSettingsDraft(DEFAULT_SETTINGS);
    setSourceTaskDraft(DEFAULT_SOURCE_TASKS);
    resetJobScopedState();
  }, [resetJobScopedState]);

  const addFiles = useCallback((incoming: FileList | File[]) => {
    const incomingArray = Array.from(incoming || []);
    if (incomingArray.length === 0) return;
    setFiles((prev) => {
      const seen = new Set(prev.map(fileSignature));
      const next = [...prev];
      incomingArray.forEach((file) => {
        const signature = fileSignature(file);
        if (seen.has(signature)) return;
        seen.add(signature);
        next.push(file);
      });
      return next;
    });
  }, []);

  const removeFileAtIndex = useCallback((index: number) => {
    setFiles((prev) => prev.filter((_, idx) => idx !== index));
  }, []);

  const clearFiles = useCallback(() => setFiles([]), []);

  const ingestSeedFiles = useCallback(
    async (incomingFiles: File[]) => {
      const files = incomingFiles.filter(Boolean);
      if (files.length === 0) {
        setRepoError("Choose at least one seed file first.");
        return;
      }
      setRepoError("");
      setRepoMessage("Importing seed links and reports...");
      try {
        const response = await api.ingestRepositorySeedFiles(files);
        await refreshDashboard();
        setHasSourceUrls((response.total_sources || 0) > 0);
        setSourceTaskDraft((prev) => ({
          ...prev,
          scope: "import",
          import_id: response.import_id,
        }));
        setRepoMessage(
          response.message ||
            `Imported ${response.accepted_new} new seed sources (${response.duplicates_skipped} duplicates skipped).`,
        );
      } catch (error) {
        setRepoError(String((error as Error).message || "Seed ingest failed"));
      }
    },
    [refreshDashboard],
  );

  const ingestRepositoryDocuments = useCallback(
    async (incomingFiles: File[]) => {
      const files = incomingFiles.filter(Boolean);
      if (files.length === 0) {
        setRepoError("Choose at least one repository document first.");
        return;
      }
      setRepoError("");
      setRepoMessage("Adding documents to the repository...");
      try {
        const response = await api.ingestRepositoryDocuments(files);
        await refreshDashboard();
        setHasSourceUrls((response.total_sources || 0) > 0);
        setSourceTaskDraft((prev) => ({
          ...prev,
          scope: "import",
          import_id: response.import_id,
        }));
        setRepoMessage(
          response.message ||
            `Added ${response.accepted_new} repository documents (${response.duplicates_skipped} duplicates skipped).`,
        );
      } catch (error) {
        setRepoError(String((error as Error).message || "Repository document ingest failed"));
      }
    },
    [refreshDashboard],
  );

  const importSourceList = useCallback(
    async (file: File | null) => {
      if (!file) {
        setRepoError("Choose a spreadsheet file first.");
        return;
      }
      setRepoError("");
      setRepoMessage("Importing source list...");
      try {
        const response = await api.importSourceList(file);
        await refreshDashboard();
        setHasSourceUrls((response.total_sources || 0) > 0);
        setSourceTaskDraft((prev) => ({
          ...prev,
          scope: "import",
          import_id: response.import_id,
        }));
        setRepoMessage(
          `Imported ${response.accepted_new} new URLs (${response.duplicates_skipped} duplicates skipped).`,
        );
      } catch (error) {
        setRepoError(String((error as Error).message || "Source list import failed"));
      }
    },
    [refreshDashboard],
  );

  const rebuildRepositoryOutputs = useCallback(async () => {
    setRepoError("");
    setRepoMessage("Rebuilding repository outputs...");
    try {
      const response = await api.rebuildRepositoryOutputs();
      setRepoMessage(response.message || "Repository rebuilt.");
      await refreshDashboard();
    } catch (error) {
      setRepoError(String((error as Error).message || "Repository rebuild failed"));
    }
  }, [refreshDashboard]);

  const clearRepositoryCitations = useCallback(async () => {
    setRepoError("");
    setRepoMessage("Clearing stored citations...");
    try {
      const response = await api.clearRepositoryCitations();
      setRepoMessage(response.message || "Stored citations cleared.");
      await Promise.all([refreshDashboard(), loadDocumentImports()]);
    } catch (error) {
      setRepoError(String((error as Error).message || "Failed to clear citations"));
    }
  }, [loadDocumentImports, refreshDashboard]);

  const cleanupRepositoryLayout = useCallback(async () => {
    setRepoError("");
    setRepoMessage("Normalizing repository layout...");
    try {
      const response = await api.cleanupRepositoryLayout();
      setRepoMessage(response.message || "Repository layout normalized.");
      await refreshDashboard();
    } catch (error) {
      setRepoError(String((error as Error).message || "Repository cleanup failed"));
    }
  }, [refreshDashboard]);

  const mergeRepositories = useCallback(
    async (sourcePaths: string[]) => {
      const trimmed = sourcePaths.map((item) => item.trim()).filter(Boolean);
      if (trimmed.length === 0) {
        setRepoError("Enter at least one external repository path.");
        return;
      }
      setRepoError("");
      setRepoMessage("Merging repositories...");
      try {
        const response = await api.mergeRepositories(trimmed);
        setRepoMessage(response.message || "Merge started.");
        await refreshDashboard();
      } catch (error) {
        setRepoError(String((error as Error).message || "Merge failed"));
      }
    },
    [refreshDashboard],
  );

  const startProcessing = useCallback(async () => {
    if (files.length === 0) {
      setProcessingError("Add at least one PDF, DOCX, or MD file.");
      return;
    }

    setProcessingError("");
    setSourceError("");
    try {
      const savedSettings = await api.saveRepoSettings(
        normalizeRepoSettingsDraft(settingsDraft),
      );
      setRepoSettings(savedSettings);
      setSettingsDraft(savedSettings);
      const response = await api.processRepositoryDocuments(files, selectedIngestionProfileId);
      setProcessingJobId(response.job_id);
      setHasSourceUrls(true);
      setSourceTaskDraft((prev) => ({
        ...prev,
        scope: "import",
        import_id: response.import_id,
      }));
      setProcessingPolling(true);
      setRepoMessage(
        response.message ||
          `Repository citation extraction started with ${
            response.selected_profile_id || "auto-detect"
          }.`,
      );
      await refreshDashboard();
    } catch (error) {
      setProcessingError(String((error as Error).message || "Failed to start extraction"));
    }
  }, [files, refreshDashboard, selectedIngestionProfileId, settingsDraft]);

  const reprocessStoredDocuments = useCallback(async () => {
    if (selectedReprocessImportIds.length === 0) {
      setProcessingError("Select at least one stored document import.");
      return;
    }

    setProcessingError("");
    setSourceError("");
    try {
      const savedSettings = await api.saveRepoSettings(
        normalizeRepoSettingsDraft(settingsDraft),
      );
      setRepoSettings(savedSettings);
      setSettingsDraft(savedSettings);
      const response = await api.reprocessRepositoryDocuments({
        target_import_ids: selectedReprocessImportIds,
        profile_override: selectedIngestionProfileId,
      });
      setProcessingJobId(response.job_id);
      setProcessingPolling(true);
      setRepoMessage(
        response.message ||
          `Repository document reprocessing started with ${
            response.selected_profile_id || "auto-detect"
          }.`,
      );
      await refreshDashboard();
    } catch (error) {
      setProcessingError(String((error as Error).message || "Failed to start reprocessing"));
    }
  }, [refreshDashboard, selectedIngestionProfileId, selectedReprocessImportIds, settingsDraft]);

  const runSourceTasks = useCallback(
    async (rerunFailedOnly: boolean) => {
      if (!repositoryStatus?.attached) {
        setSourceError("Open a repository first.");
        return;
      }
      const normalizedPayload = {
        ...sourceTaskDraft,
        project_profile_name:
          sourceTaskDraft.project_profile_name || settingsDraft.default_project_profile_name,
        run_download: Boolean(sourceTaskDraft.run_download || sourceTaskDraft.force_redownload),
        run_catalog: Boolean(
          sourceTaskDraft.run_catalog ||
            sourceTaskDraft.force_catalog ||
            sourceTaskDraft.run_llm_title ||
            sourceTaskDraft.force_title ||
            sourceTaskDraft.run_citation_verify ||
            sourceTaskDraft.force_citation_verify,
        ),
        run_citation_verify: Boolean(
          sourceTaskDraft.run_citation_verify || sourceTaskDraft.force_citation_verify,
        ),
        run_llm_cleanup: Boolean(
          sourceTaskDraft.run_llm_cleanup || sourceTaskDraft.force_llm_cleanup,
        ),
        run_llm_title: Boolean(sourceTaskDraft.run_llm_title || sourceTaskDraft.force_title),
        run_llm_summary: Boolean(
          sourceTaskDraft.run_llm_summary || sourceTaskDraft.force_summary,
        ),
        run_llm_rating: Boolean(sourceTaskDraft.run_llm_rating || sourceTaskDraft.force_rating),
      };
      if (
        !normalizedPayload.run_download &&
        !normalizedPayload.run_catalog &&
        !normalizedPayload.run_citation_verify &&
        !normalizedPayload.run_llm_cleanup &&
        !normalizedPayload.run_llm_title &&
        !normalizedPayload.run_llm_summary &&
        !normalizedPayload.run_llm_rating
      ) {
        setSourceError("Select at least one source phase to run.");
        return;
      }
      if (
        normalizedPayload.run_download &&
        !normalizedPayload.include_raw_file &&
        !normalizedPayload.include_rendered_html &&
        !normalizedPayload.include_rendered_pdf &&
        !normalizedPayload.include_markdown
      ) {
        setSourceError("Select at least one download output type.");
        return;
      }

      setSourceError("");
      setSourceTaskDraft((prev) => ({
        ...prev,
        run_download: normalizedPayload.run_download,
        run_catalog: normalizedPayload.run_catalog,
        run_citation_verify: normalizedPayload.run_citation_verify,
        run_llm_cleanup: normalizedPayload.run_llm_cleanup,
        run_llm_title: normalizedPayload.run_llm_title,
        run_llm_summary: normalizedPayload.run_llm_summary,
        run_llm_rating: normalizedPayload.run_llm_rating,
      }));
      try {
        const response = await api.startRepositorySourceTasks({
          ...normalizedPayload,
          rerun_failed_only: rerunFailedOnly,
        });
        setSourceTaskJobId(response.job_id || null);
        setSourcePolling(true);
        setRepoMessage(response.message || "Repository source tasks started.");
      } catch (error) {
        setSourceError(String((error as Error).message || "Failed to start source tasks"));
      }
    },
    [repositoryStatus?.attached, setSourceTaskDraft, settingsDraft.default_project_profile_name, sourceTaskDraft],
  );

  const cancelSourceTasks = useCallback(async () => {
    if (!sourceTaskJobId) return;
    setSourceError("");
    try {
      const response = await api.cancelSourceDownload(sourceTaskJobId);
      if (response.status === "cancelling") {
        setSourceStatus((prev) =>
          prev
            ? {
                ...prev,
                state: "cancelling",
                cancel_requested: true,
                stop_after_current_item: true,
                message: response.message || prev.message || "Stop requested.",
              }
            : prev,
        );
        setRepoMessage(response.message || "Stop requested. Finishing the current item before stopping.");
      }
      setSourcePolling(true);
    } catch (error) {
      setSourceError(String((error as Error).message || "Failed to cancel source tasks"));
    }
  }, [sourceTaskJobId]);

  const trackSourceTaskJob = useCallback((jobId: string | null, message = "") => {
    setSourceTaskJobId(jobId);
    setSourceStatus(null);
    setSourcePolling(Boolean(jobId));
    if (message) {
      setRepoMessage(message);
    }
  }, []);

  const saveRepoSettings = useCallback(async (nextSettings?: RepoSettings) => {
    if (!repoLoaded) return;
    setSavingSettings(true);
    setRepoError("");
    try {
      const payload = normalizeRepoSettingsDraft(nextSettings ?? settingsDraft);
      const saved = await api.saveRepoSettings(payload);
      setRepoSettings(saved);
      setSettingsDraft(saved);
      setSourceTaskDraft((prev) => ({
        ...prev,
        project_profile_name:
          saved.default_project_profile_name.trim() || prev.project_profile_name,
      }));
      setRepoMessage("Settings saved.");
      await refreshDashboard();
    } catch (error) {
      setRepoError(String((error as Error).message || "Failed to save settings"));
    } finally {
      setSavingSettings(false);
    }
  }, [refreshDashboard, repoLoaded, settingsDraft]);

  const loadModels = useCallback(async () => {
    setLoadingModels(true);
    setRepoError("");
    try {
      const query = new URLSearchParams({
        backend_kind: settingsDraft.llm_backend.kind,
        base_url: settingsDraft.llm_backend.base_url,
        api_key: settingsDraft.llm_backend.api_key,
      });
      const response: ModelsResponse = await api.getModels(query);
      setModels(response.models || []);
      if (response.error) {
        setRepoError(response.error);
      }
    } catch (error) {
      setRepoError(String((error as Error).message || "Failed to load models"));
    } finally {
      setLoadingModels(false);
    }
  }, [settingsDraft.llm_backend.api_key, settingsDraft.llm_backend.base_url, settingsDraft.llm_backend.kind]);

  const uploadProfile = useCallback(
    async (file: File | null) => {
      if (!file) return;
      try {
        const response = await api.uploadProjectProfile(file);
        await loadProfiles();
        setSettingsDraft((prev) => ({
          ...prev,
          default_project_profile_name: response.filename,
        }));
        setSourceTaskDraft((prev) => ({
          ...prev,
          project_profile_name: response.filename,
        }));
        setRepoMessage(`Uploaded profile ${response.filename}.`);
      } catch (error) {
        setRepoError(String((error as Error).message || "Profile upload failed"));
      }
    },
    [loadProfiles],
  );

  const getRepositoryManifest = useCallback(async (params: URLSearchParams) => {
    return api.getRepositoryManifest(params);
  }, []);

  const getRepositoryCitationData = useCallback(async () => {
    return api.getRepositoryCitationData();
  }, []);

  const repoStatusQuery = useQuery({
    queryKey: ["repo-status", repoLoaded],
    queryFn: api.getRepoStatus,
    enabled: repoLoaded,
    refetchInterval: (query) => {
      const data = query.state.data as RepositoryStatusResponse | undefined;
      return data?.download_state === "running" || data?.download_state === "cancelling"
        ? 1500
        : false;
    },
  });

  useEffect(() => {
    if (repoStatusQuery.data) {
      setRepositoryStatus(repoStatusQuery.data);
      setHasSourceUrls((repoStatusQuery.data.total_sources || 0) > 0);
    }
  }, [repoStatusQuery.data]);

  const dashboardQuery = useQuery({
    queryKey: ["repo-dashboard", repoLoaded],
    queryFn: api.getRepositoryDashboard,
    enabled: repoLoaded,
    refetchInterval: 5000,
  });

  useEffect(() => {
    if (dashboardQuery.data) {
      setDashboard(dashboardQuery.data);
      setRepositoryStatus(dashboardQuery.data.status);
      setHasSourceUrls((dashboardQuery.data.status?.total_sources || 0) > 0);
    }
  }, [dashboardQuery.data]);

  const processingQuery = useQuery({
    queryKey: ["processing-status", processingJobId],
    queryFn: () => api.getProcessingStatus(String(processingJobId)),
    enabled: Boolean(processingJobId && processingPolling),
    refetchInterval: 800,
    retry: false,
  });

  useEffect(() => {
    if (!processingQuery.data) return;
    setProcessingStatus(processingQuery.data);
    const done =
      (processingQuery.data.current_stage === "completed" ||
        processingQuery.data.current_stage === "failed") &&
      !repositoryFinalizeInFlight(processingQuery.data);
    if (!done) return;

    setProcessingPolling(false);
    Promise.all([refreshDashboard(), loadDocumentImports()]).catch(() => {
      // ignored
    });
  }, [loadDocumentImports, processingQuery.data, refreshDashboard]);

  const sourceQuery = useQuery({
    queryKey: ["source-status", sourceTaskJobId],
    queryFn: () => api.getSourceStatus(String(sourceTaskJobId)),
    enabled: Boolean(sourceTaskJobId && sourcePolling),
    refetchInterval: 1000,
    retry: false,
  });

  useEffect(() => {
    if (!sourceQuery.data) return;
    setSourceStatus(sourceQuery.data);
    if (
      sourceQuery.data.state === "completed" ||
      sourceQuery.data.state === "failed" ||
      sourceQuery.data.state === "cancelled"
    ) {
      setSourcePolling(false);
      if (sourceQuery.data.state === "cancelled" && sourceQuery.data.message) {
        setRepoMessage(sourceQuery.data.message);
      }
      refreshDashboard().catch(() => {
        // ignored
      });
    }
  }, [sourceQuery.data, refreshDashboard]);

  useEffect(() => {
    const loadLastRepoPath = async () => {
      try {
        const appSettings = await api.getAppSettings();
        setLastRepositoryPath((appSettings.last_repository_path || "").trim());
      } catch {
        // Keep landing page visible and let user choose.
      }
    };
    loadLastRepoPath().catch(() => {
      // ignored
    });
  }, []);

  const warnings = useMemo(() => {
    const items: Array<{ type: "warning" | "error"; stage: string; message: string }> = [];
    (processingStatus?.stages || []).forEach((stage) => {
      (stage.warnings || []).forEach((warning) => {
        items.push({
          type: "warning",
          stage: stage.stage,
          message: warning,
        });
      });
      (stage.errors || []).forEach((error) => {
        items.push({
          type: "error",
          stage: stage.stage,
          message: error,
        });
      });
    });
    return items;
  }, [processingStatus?.stages]);

  const value = useMemo<AppStateValue>(
    () => ({
      repoLoaded,
      repositoryStatus,
      dashboard,
      repoSettings,
      settingsDraft,
      setSettingsDraft,
      sourceTaskDraft,
      setSourceTaskDraft,
      processingJobId,
      sourceTaskJobId,
      files,
      hasSourceUrls,
      processingStatus,
      sourceStatus,
      profiles,
      documentImports,
      ingestionProfiles,
      ingestionProfileSuggestions,
      defaultIngestionProfileId,
      selectedIngestionProfileId,
      setSelectedIngestionProfileId,
      selectedReprocessImportIds,
      setSelectedReprocessImportIds,
      models,
      lastRepositoryPath,
      gateMessage,
      gateError,
      repoMessage,
      repoError,
      processingError,
      sourceError,
      savingSettings,
      loadingModels,
      processingRunning,
      sourceRunning,
      sourceStopping,
      pickRepositoryDirectory,
      openRepository,
      createRepository,
      switchRepository,
      addFiles,
      removeFileAtIndex,
      clearFiles,
      ingestSeedFiles,
      ingestRepositoryDocuments,
      importSourceList,
      mergeRepositories,
      rebuildRepositoryOutputs,
      clearRepositoryCitations,
      cleanupRepositoryLayout,
      startProcessing,
      reprocessStoredDocuments,
      runSourceTasks,
      trackSourceTaskJob,
      cancelSourceTasks,
      saveRepoSettings,
      loadModels,
      loadProfiles,
      loadDocumentImports,
      loadIngestionProfiles,
      saveIngestionProfile,
      deleteIngestionProfile,
      loadIngestionProfileSuggestions,
      acceptIngestionProfileSuggestion,
      rejectIngestionProfileSuggestion,
      uploadProfile,
      refreshDashboard,
      getRepositoryCitationData,
      getRepositoryManifest,
      warnings,
    }),
    [
      repoLoaded,
      repositoryStatus,
      dashboard,
      repoSettings,
      settingsDraft,
      sourceTaskDraft,
      processingJobId,
      sourceTaskJobId,
      files,
      hasSourceUrls,
      processingStatus,
      sourceStatus,
      profiles,
      documentImports,
      ingestionProfiles,
      ingestionProfileSuggestions,
      defaultIngestionProfileId,
      selectedIngestionProfileId,
      selectedReprocessImportIds,
      models,
      lastRepositoryPath,
      gateMessage,
      gateError,
      repoMessage,
      repoError,
      processingError,
      sourceError,
      savingSettings,
      loadingModels,
      processingRunning,
      sourceRunning,
      sourceStopping,
      pickRepositoryDirectory,
      openRepository,
      createRepository,
      switchRepository,
      addFiles,
      removeFileAtIndex,
      clearFiles,
      ingestSeedFiles,
      ingestRepositoryDocuments,
      importSourceList,
      mergeRepositories,
      rebuildRepositoryOutputs,
      clearRepositoryCitations,
      cleanupRepositoryLayout,
      startProcessing,
      reprocessStoredDocuments,
      runSourceTasks,
      trackSourceTaskJob,
      cancelSourceTasks,
      saveRepoSettings,
      loadModels,
      loadProfiles,
      loadDocumentImports,
      loadIngestionProfiles,
      saveIngestionProfile,
      deleteIngestionProfile,
      loadIngestionProfileSuggestions,
      acceptIngestionProfileSuggestion,
      rejectIngestionProfileSuggestion,
      uploadProfile,
      refreshDashboard,
      getRepositoryCitationData,
      getRepositoryManifest,
      warnings,
    ],
  );

  return <AppStateContext.Provider value={value}>{children}</AppStateContext.Provider>;
}

export function useAppState(): AppStateValue {
  const value = useContext(AppStateContext);
  if (!value) {
    throw new Error("useAppState must be used inside AppStateProvider");
  }
  return value;
}

export function useExportSummaryText(
  exportData: ExportResult | null,
  bibliography: BibliographyResult | null,
): string {
  if (!exportData) return "No export rows yet.";
  const rows = exportData.rows || [];
  const entries = bibliography?.entries || [];
  const sourceUrlCount = countUniqueSourceUrls(entries);
  return (
    `${rows.length} rows exported | ` +
    `${exportData.matched_count || 0} matched | ` +
    `${exportData.unmatched_count || 0} unmatched | ` +
    `${exportData.total_bib_entries || 0} bibliography entries | ` +
    `${sourceUrlCount} URLs ready for source tasks`
  );
}
