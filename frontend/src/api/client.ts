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
  RepoSettings,
  RepositoryActionResponse,
  RepositoryCitationDataResponse,
  RepositoryDashboardResponse,
  RepositoryDocumentImportListResponse,
  RepositoryImportResponse,
  RepositoryManifestResponse,
  RepositoryMergeResponse,
  RepositoryProcessDocumentsResponse,
  RepositoryReprocessDocumentsRequest,
  RepositoryReprocessDocumentsResponse,
  RepositorySourceDeleteResponse,
  RepositorySourceExportRequest,
  RepositorySourceExportResponse,
  RepositorySourceTaskRequest,
  RepositorySourceTaskResponse,
  RepositoryStatusResponse,
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

async function apiPut<T>(path: string, body: unknown): Promise<T> {
  const resp = await fetch(`/api/${path}`, {
    method: "PUT",
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
  getRepositoryDashboard: () => apiGet<RepositoryDashboardResponse>("repository/dashboard"),
  getRepositoryCitationData: () =>
    apiGet<RepositoryCitationDataResponse>("repository/citation-data"),
  getRepositoryManifest: (query: URLSearchParams) =>
    apiGet<RepositoryManifestResponse>(`repository/manifest?${query.toString()}`),
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
};
