import type {
  AppSettings,
  IngestionProfile,
  IngestionProfileActionResponse,
  IngestionProfileListResponse,
  IngestionProfileSuggestionActionResponse,
  IngestionProfileSuggestionListResponse,
  JobStatusResponse,
  ModelsResponse,
  PickDirectoryResponse,
  ProjectProfile,
  ProjectProfileGenerateRequest,
  ProjectProfileGenerateResponse,
  ProjectProfileSaveRequest,
  ProjectProfileSaveResponse,
  RepoSettings,
  RepositoryActionResponse,
  RepositoryBundleExportRequest,
  RepositoryColumnConfig,
  RepositoryColumnPromptFixResponse,
  RepositoryColumnRunStartResponse,
  RepositoryColumnRunStatus,
  RepositoryCitationDataResponse,
  RepositoryCitationRisExportRequest,
  RepositoryDashboardResponse,
  RepositoryDuplicateCandidateResponse,
  RepositoryDocumentImportListResponse,
  RepositoryFileDownloadResult,
  RepositoryImportResponse,
  RepositoryManifestExportRequest,
  RepositoryManifestResponse,
  RepositoryManifestFilterPayload,
  RepositoryMergeResponse,
  RepositoryProcessDocumentsResponse,
  RepositoryReprocessDocumentsRequest,
  RepositoryReprocessDocumentsResponse,
  RepositorySourceBulkRisReadyResponse,
  RepositorySourceDeleteResponse,
  RepositorySourceExportRequest,
  RepositorySourceExportResponse,
  RepositorySourcePatchRequest,
  RepositorySourceTaskRequest,
  RepositorySourceTaskResponse,
  RepositoryStatusResponse,
  SearchImportResponse,
  SearchJobStatus,
  SourceCancelResponse,
  SourceDownloadStatus,
} from "./types";

async function parseApiResponse<T>(resp: Response): Promise<T> {
  let body: unknown = null;
  try {
    body = await resp.json();
  } catch {
    body = null;
  }
  if (!resp.ok) {
    const detail =
      (body as { detail?: string; message?: string } | null)?.detail ||
      (body as { detail?: string; message?: string } | null)?.message ||
      `API error: ${resp.status}`;
    throw new Error(detail);
  }
  return body as T;
}

async function apiGet<T>(path: string): Promise<T> {
  const resp = await fetch(`/api/${path}`);
  return parseApiResponse<T>(resp);
}

async function apiPost<T>(path: string, body: unknown): Promise<T> {
  const resp = await fetch(`/api/${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return parseApiResponse<T>(resp);
}

function parseContentDispositionFilename(value: string | null): string {
  const header = value || "";
  const utf8Match = header.match(/filename\*=UTF-8''([^;]+)/i);
  if (utf8Match?.[1]) {
    try {
      return decodeURIComponent(utf8Match[1]);
    } catch {
      return utf8Match[1];
    }
  }
  const simpleMatch = header.match(/filename="([^"]+)"/i) || header.match(/filename=([^;]+)/i);
  return simpleMatch?.[1]?.trim() || "export.ris";
}

async function apiPostDownload(path: string, body: unknown): Promise<RepositoryFileDownloadResult> {
  const resp = await fetch(`/api/${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    await parseApiResponse<never>(resp);
  }
  return {
    blob: await resp.blob(),
    filename: parseContentDispositionFilename(resp.headers.get("Content-Disposition")),
    requestedCount: Number(resp.headers.get("X-ResearchAssistant-Requested-Count") || "0"),
    exportedCount: Number(resp.headers.get("X-ResearchAssistant-Exported-Count") || "0"),
    skippedCount: Number(resp.headers.get("X-ResearchAssistant-Skipped-Count") || "0"),
  };
}

async function apiPut<T>(path: string, body: unknown): Promise<T> {
  const resp = await fetch(`/api/${path}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return parseApiResponse<T>(resp);
}

async function apiPatch<T>(path: string, body: unknown): Promise<T> {
  const resp = await fetch(`/api/${path}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return parseApiResponse<T>(resp);
}

async function apiDelete<T>(path: string): Promise<T> {
  const resp = await fetch(`/api/${path}`, {
    method: "DELETE",
  });
  return parseApiResponse<T>(resp);
}

async function apiPostFile<T>(path: string, file: File): Promise<T> {
  const formData = new FormData();
  formData.append("file", file);
  const resp = await fetch(`/api/${path}`, {
    method: "POST",
    body: formData,
  });
  return parseApiResponse<T>(resp);
}

async function apiPostFiles<T>(path: string, files: File[], fieldName = "files"): Promise<T> {
  const formData = new FormData();
  files.forEach((file) => formData.append(fieldName, file));
  const resp = await fetch(`/api/${path}`, {
    method: "POST",
    body: formData,
  });
  return parseApiResponse<T>(resp);
}

export const api = {
  getAppSettings: () => apiGet<AppSettings>("settings"),
  getRepoStatus: () => apiGet<RepositoryStatusResponse>("repository/status"),
  createRepository: (path: string) =>
    apiPost<RepositoryStatusResponse>("repository/create", { path }),
  attachRepository: (path: string) =>
    apiPost<RepositoryStatusResponse>("repository/attach", { path }),
  getRepoSettings: () => apiGet<RepoSettings>("repository/settings"),
  saveRepoSettings: (settings: RepoSettings) =>
    apiPut<RepoSettings>("repository/settings", settings),
  getModels: (query: URLSearchParams) => apiGet<ModelsResponse>(`models?${query.toString()}`),
  ingestRepositorySeedFiles: (files: File[]) =>
    apiPostFiles<RepositoryImportResponse>("repository/ingest/seed-files", files),
  ingestRepositoryDocuments: (files: File[]) =>
    apiPostFiles<RepositoryImportResponse>("repository/ingest/documents", files),
  importSourceList: (file: File) =>
    apiPostFile<RepositoryImportResponse>("repository/import/source-list", file),
  getIngestionProfiles: () =>
    apiGet<IngestionProfileListResponse>("repository/ingestion-profiles"),
  saveIngestionProfile: (profile: IngestionProfile) =>
    apiPost<IngestionProfileActionResponse>("repository/ingestion-profiles", profile),
  updateIngestionProfile: (profileId: string, profile: IngestionProfile) =>
    apiPut<IngestionProfileActionResponse>(`repository/ingestion-profiles/${encodeURIComponent(profileId)}`, profile),
  deleteIngestionProfile: (profileId: string) =>
    apiDelete<IngestionProfileActionResponse>(`repository/ingestion-profiles/${encodeURIComponent(profileId)}`),
  getIngestionProfileSuggestions: () =>
    apiGet<IngestionProfileSuggestionListResponse>("repository/ingestion-profile-suggestions"),
  acceptIngestionProfileSuggestion: (suggestionId: string) =>
    apiPost<IngestionProfileSuggestionActionResponse>(
      `repository/ingestion-profile-suggestions/${encodeURIComponent(suggestionId)}/accept`,
      {},
    ),
  rejectIngestionProfileSuggestion: (suggestionId: string) =>
    apiPost<IngestionProfileSuggestionActionResponse>(
      `repository/ingestion-profile-suggestions/${encodeURIComponent(suggestionId)}/reject`,
      {},
    ),
  processRepositoryDocuments: async (files: File[], profileOverride = "") => {
    const formData = new FormData();
    files.forEach((file) => formData.append("files", file));
    if (profileOverride.trim()) {
      formData.append("profile_override", profileOverride.trim());
    }
    const resp = await fetch("/api/repository/process-documents", {
      method: "POST",
      body: formData,
    });
    return parseApiResponse<RepositoryProcessDocumentsResponse>(resp);
  },
  getRepositoryDocumentImports: () =>
    apiGet<RepositoryDocumentImportListResponse>("repository/document-imports"),
  reprocessRepositoryDocuments: (payload: RepositoryReprocessDocumentsRequest) =>
    apiPost<RepositoryReprocessDocumentsResponse>("repository/reprocess-documents", {
      target_import_ids: payload.target_import_ids,
      profile_override: payload.profile_override,
    }),
  rebuildRepositoryOutputs: () =>
    apiPost<RepositoryActionResponse>("repository/rebuild", {}),
  clearRepositoryCitations: () =>
    apiPost<RepositoryActionResponse>("repository/clear-citations", {}),
  cleanupRepositoryLayout: () =>
    apiPost<RepositoryActionResponse>("repository/cleanup", {}),
  startRepositorySourceTasks: (payload: RepositorySourceTaskRequest) =>
    apiPost<RepositorySourceTaskResponse>("repository/source-tasks", payload),
  mergeRepositories: (sourcePaths: string[]) =>
    apiPost<RepositoryMergeResponse>("repository/merge", { source_paths: sourcePaths }),
  pickRepositoryDirectory: (mode: "open" | "create" | "export", initialPath = "") =>
    apiGet<PickDirectoryResponse>(
      `repository/pick-directory?mode=${encodeURIComponent(mode)}&initial_path=${encodeURIComponent(initialPath)}`,
    ),
  getProcessingStatus: (jobId: string) => apiGet<JobStatusResponse>(`status/${jobId}`),
  cancelSourceDownload: (jobId: string) => apiPost<SourceCancelResponse>(`sources/${jobId}/cancel`, {}),
  getSourceStatus: (jobId: string) => apiGet<SourceDownloadStatus>(`sources/${jobId}/status`),
  getProjectProfiles: () => apiGet<ProjectProfile[]>("project-profiles"),
  uploadProjectProfile: (file: File) => apiPostFile<{ filename: string; name: string }>("project-profiles/upload", file),
  generateProjectProfile: (payload: ProjectProfileGenerateRequest) =>
    apiPost<ProjectProfileGenerateResponse>("project-profiles/generate", payload),
  saveProjectProfile: (filename: string, payload: ProjectProfileSaveRequest) =>
    apiPut<ProjectProfileSaveResponse>(`project-profiles/${encodeURIComponent(filename)}`, payload),
  getRepositoryDashboard: () => apiGet<RepositoryDashboardResponse>("repository/dashboard"),
  getRepositoryCitationData: () =>
    apiGet<RepositoryCitationDataResponse>("repository/citation-data"),
  getRepositoryManifest: (query: URLSearchParams) =>
    apiGet<RepositoryManifestResponse>(`repository/manifest?${query.toString()}`),
  createRepositoryColumn: (label: string) =>
    apiPost<RepositoryColumnConfig>("repository/columns", { label }),
  updateRepositoryColumn: (
    columnId: string,
    payload: {
      label?: string;
      instruction_prompt?: string;
      output_constraint?: RepositoryColumnConfig["output_constraint"];
      include_row_context?: boolean;
      include_source_text?: boolean;
    },
  ) =>
    apiPatch<RepositoryColumnConfig>(
      `repository/columns/${encodeURIComponent(columnId)}`,
      payload,
    ),
  fixRepositoryColumnPrompt: (columnId: string, draftPrompt: string) =>
    apiPost<RepositoryColumnPromptFixResponse>(
      `repository/columns/${encodeURIComponent(columnId)}/fix-prompt`,
      { draft_prompt: draftPrompt },
    ),
  startRepositoryColumnRun: (
    columnId: string,
      payload: {
      filters: RepositoryManifestFilterPayload;
      scope?: "filtered" | "all" | "empty_only" | "selected";
      source_ids?: string[];
      confirm_overwrite?: boolean;
    },
  ) =>
    apiPost<RepositoryColumnRunStartResponse>(
      `repository/columns/${encodeURIComponent(columnId)}/run`,
      {
        filters: payload.filters,
        scope: payload.scope || "filtered",
        source_ids: payload.source_ids || [],
        confirm_overwrite: Boolean(payload.confirm_overwrite),
      },
    ),
  getRepositoryColumnRunStatus: (jobId: string) =>
    apiGet<RepositoryColumnRunStatus>(`repository/column-runs/${encodeURIComponent(jobId)}`),
  patchRepositorySource: (sourceId: string, payload: RepositorySourcePatchRequest) =>
    apiPatch<RepositoryManifestResponse["rows"][number]>(
      `repository/sources/${encodeURIComponent(sourceId)}`,
      payload,
    ),
  bulkMarkRepositorySourcesRisReady: (sourceIds: string[]) =>
    apiPost<RepositorySourceBulkRisReadyResponse>("repository/sources/bulk-mark-ris-ready", {
      source_ids: sourceIds,
    }),
  getRepositoryDuplicateCandidates: () =>
    apiPost<RepositoryDuplicateCandidateResponse>("repository/sources/duplicate-candidates", {}),
  deleteRepositorySources: (sourceIds: string[]) =>
    apiPost<RepositorySourceDeleteResponse>("repository/sources/bulk-delete", {
      source_ids: sourceIds,
    }),
  exportRepositorySourceFiles: (payload: RepositorySourceExportRequest) =>
    apiPost<RepositorySourceExportResponse>("repository/sources/export-files", {
      source_ids: payload.source_ids,
      file_kinds: payload.file_kinds,
      destination_path: payload.destination_path,
    }),
  exportRepositoryBundle: (payload: RepositoryBundleExportRequest) =>
    apiPostDownload("repository/export-bundle", {
      mode: payload.mode,
      scope: payload.scope,
      source_ids: payload.source_ids,
      file_kinds: payload.file_kinds,
      base_url: payload.base_url,
    }),
  exportRepositoryCitationRis: (payload: RepositoryCitationRisExportRequest) =>
    apiPostDownload("repository/citations/export-ris", payload),
  exportRepositoryManifest: (payload: RepositoryManifestExportRequest) =>
    apiPostDownload("repository/manifest/export", payload),

  // Search
  startSearch: (prompt: string, targetCount: number) =>
    apiPost<SearchJobStatus>("search/start", { prompt, target_count: targetCount }),
  getSearchStatus: (jobId: string) =>
    apiGet<SearchJobStatus>(`search/${encodeURIComponent(jobId)}/status`),
  cancelSearch: (jobId: string) =>
    apiPost<{ status: string }>(`search/${encodeURIComponent(jobId)}/cancel`, {}),
  importSearchResults: (jobId: string, minRelevance: number) =>
    apiPost<SearchImportResponse>(`search/${encodeURIComponent(jobId)}/import`, {
      min_relevance: minRelevance,
    }),
};
