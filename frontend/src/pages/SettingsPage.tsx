import { useState } from "react";

import { Button, InputField, SectionHeader, SelectField, SurfaceCard } from "../components/primitives";
import { PROVIDER_LABELS, useAppState } from "../state/AppState";
import type { BackendProvider } from "../api/types";

export function SettingsPage() {
  const {
    appSettingsDraft,
    setAppSettingsDraft,
    saveAppSettings,
    selectBackendProfile,
    saveBackendProfile,
    duplicateBackendProfile,
    deleteBackendProfile,
    setActiveBackendProvider,
    renameActiveBackendProfile,
    mergeRepositories,
    pickRepositoryDirectory,
    repoError,
    repoMessage,
    repositoryStatus,
    savingSettings,
    models,
    loadModels,
    loadingModels,
  } = useAppState();
  const [mergePending, setMergePending] = useState(false);

  const backendProfiles = appSettingsDraft.backend_profiles;
  const activeProfile = backendProfiles.find(
    (profile) => profile.id === appSettingsDraft.active_profile_id,
  );
  const activeProvider: BackendProvider = activeProfile?.provider ?? "custom";

  const handleMergeRepository = async () => {
    setMergePending(true);
    try {
      const selectedPath = await pickRepositoryDirectory(
        "open",
        repositoryStatus?.path || "",
      );
      if (!selectedPath) return;
      await mergeRepositories([selectedPath]);
    } finally {
      setMergePending(false);
    }
  };

  return (
    <div className="space-y-4">
      <SectionHeader
        title="Settings"
        description="Configure the language model backend, fetch pacing, search, and merge operations. LLM, fetch, and search settings apply across all repositories."
      />

      {(repoMessage || repoError) && (
        <SurfaceCard className={repoError ? "border border-error/30 bg-error/10" : ""}>
          <div className={repoError ? "text-body-md text-error" : "text-body-md text-on-surface"}>
            {repoError || repoMessage}
          </div>
        </SurfaceCard>
      )}

      <SurfaceCard>
        <div className="mb-3 text-title-sm font-semibold">LLM Backend Settings</div>

        <div className="mb-4 rounded-md border border-outline-variant bg-surface-container-low p-3">
          <div className="grid gap-3 md:grid-cols-2">
            <SelectField
              label="Saved Backend"
              value={appSettingsDraft.active_profile_id}
              onChange={(event) => void selectBackendProfile(event.target.value)}
            >
              {backendProfiles.map((profile) => (
                <option key={profile.id} value={profile.id}>
                  {profile.name || "(unnamed)"}
                </option>
              ))}
            </SelectField>

            <InputField
              label="Profile Name"
              value={activeProfile?.name ?? ""}
              onChange={(event) => renameActiveBackendProfile(event.target.value)}
              placeholder="e.g. Claude (work)"
            />
          </div>
          <div className="mt-3 flex flex-wrap gap-2">
            <Button variant="primary" disabled={savingSettings} onClick={() => void saveBackendProfile()}>
              {savingSettings ? "Saving..." : "Save Backend"}
            </Button>
            <Button variant="secondary" onClick={() => void duplicateBackendProfile()}>
              Duplicate
            </Button>
            <Button
              variant="danger"
              disabled={backendProfiles.length <= 1}
              onClick={() => void deleteBackendProfile(appSettingsDraft.active_profile_id)}
            >
              Delete
            </Button>
          </div>
        </div>

        <div className="grid gap-3 md:grid-cols-2">
          <SelectField
            label="Provider"
            value={activeProvider}
            onChange={(event) => setActiveBackendProvider(event.target.value as BackendProvider)}
          >
            {PROVIDER_LABELS.map((provider) => (
              <option key={provider.value} value={provider.value}>
                {provider.label}
              </option>
            ))}
          </SelectField>

          <InputField
            label="Base URL"
            value={appSettingsDraft.llm_backend.base_url}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  base_url: event.target.value,
                },
              }))
            }
          />

          <InputField
            label="API Key"
            type="password"
            value={appSettingsDraft.llm_backend.api_key}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  api_key: event.target.value,
                },
              }))
            }
          />

          <div className="grid gap-1 text-body-md">
            <span className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Model</span>
            <div className="flex gap-2">
              <select
                className="min-w-0 flex-1 rounded-md border border-outline-variant bg-surface-container-lowest px-3 py-2 text-body-md text-on-surface focus:border-primary focus:outline-none"
                value={appSettingsDraft.llm_backend.model}
                onChange={(event) =>
                  setAppSettingsDraft((prev) => ({
                    ...prev,
                    llm_backend: {
                      ...prev.llm_backend,
                      model: event.target.value,
                    },
                  }))
                }
              >
                <option value="">-- Load models first --</option>
                {models.map((model) => (
                  <option key={model} value={model}>
                    {model}
                  </option>
                ))}
              </select>
              <Button onClick={() => void loadModels()}>
                {loadingModels ? "Loading..." : "Load Models"}
              </Button>
            </div>
          </div>

          <InputField
            label="Temperature"
            type="number"
            min={0}
            max={2}
            step={0.1}
            value={String(appSettingsDraft.llm_backend.temperature)}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  temperature: Number.parseFloat(event.target.value || "0"),
                },
              }))
            }
          />

          <SelectField
            label="Reasoning"
            value={appSettingsDraft.llm_backend.reasoning_level}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  reasoning_level: event.target.value as
                    | "default"
                    | "off"
                    | "low"
                    | "medium"
                    | "high",
                },
              }))
            }
          >
            <option value="default">Default</option>
            <option value="off">Off</option>
            <option value="low">Low</option>
            <option value="medium">Medium</option>
            <option value="high">High</option>
          </SelectField>

          <InputField
            label="Context Window"
            type="number"
            min={2048}
            max={262144}
            step={1024}
            value={String(appSettingsDraft.llm_backend.num_ctx)}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  num_ctx: Number.parseInt(event.target.value || "8192", 10),
                },
              }))
            }
          />

          <InputField
            label="Max Tokens (0 = provider default)"
            type="number"
            min={0}
            max={200000}
            step={256}
            value={String(appSettingsDraft.llm_backend.max_tokens)}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  max_tokens: Number.parseInt(event.target.value || "0", 10),
                },
              }))
            }
          />

          <InputField
            label="Max Source Chars"
            type="number"
            min={0}
            step={1000}
            value={String(appSettingsDraft.llm_backend.max_source_chars)}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  max_source_chars: Number.parseInt(event.target.value || "0", 10),
                },
              }))
            }
          />

          <InputField
            label="LLM Timeout (sec)"
            type="number"
            min={30}
            step={30}
            value={String(appSettingsDraft.llm_backend.llm_timeout)}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  llm_timeout: Number.parseFloat(event.target.value || "300"),
                },
              }))
            }
          />
        </div>

        <label className="mt-3 flex items-center gap-2 text-body-md text-on-surface">
          <input
            checked={appSettingsDraft.use_llm}
            type="checkbox"
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                use_llm: event.target.checked,
              }))
            }
          />
          Use LLM-assisted features by default
        </label>

        <div className="mt-4 flex flex-wrap gap-2">
          <Button variant="primary" disabled={savingSettings} onClick={() => void saveAppSettings()}>
            {savingSettings ? "Saving..." : "Save LLM Settings"}
          </Button>
        </div>
      </SurfaceCard>

      <SurfaceCard>
        <div className="mb-3 text-title-sm font-semibold">Fetch Delay Settings</div>
        <div className="grid gap-3 md:grid-cols-[minmax(0,260px)_minmax(0,1fr)]">
          <InputField
            label="Fetch Delay (seconds)"
            type="number"
            min={0}
            max={30}
            step={0.5}
            value={String(appSettingsDraft.fetch_delay)}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                fetch_delay: Number.parseFloat(event.target.value || "2"),
              }))
            }
          />
          <div className="rounded-md bg-surface-container-low p-3 text-body-md text-on-surface-variant">
            Applies to repository fetch and scrape work. Increase this when the target site needs a
            slower crawl or when you want to reduce parallel request pressure.
          </div>
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          <Button variant="primary" disabled={savingSettings} onClick={() => void saveAppSettings()}>
            {savingSettings ? "Saving..." : "Save Fetch Delay"}
          </Button>
        </div>
      </SurfaceCard>

      <SurfaceCard>
        <div className="mb-3 text-title-sm font-semibold">Manual Capture</div>
        <div className="grid gap-3 md:grid-cols-[minmax(0,260px)_minmax(0,1fr)]">
          <InputField
            label="Watch folder"
            value={appSettingsDraft.manual_capture_watch_dir || ""}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                manual_capture_watch_dir: event.target.value,
              }))
            }
            placeholder="~/Downloads"
          />
          <div className="rounded-md bg-surface-container-low p-3 text-body-md text-on-surface-variant">
            Where your browser saves pages you collect by hand. Resolve Fetches watches this folder
            and offers recent files for one-click attaching. Leave empty to use{" "}
            <code>~/Downloads</code>. Files here are only ever read, never moved or deleted.
          </div>
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          <Button variant="primary" disabled={savingSettings} onClick={() => void saveAppSettings()}>
            {savingSettings ? "Saving..." : "Save Watch Folder"}
          </Button>
        </div>
      </SurfaceCard>

      <SurfaceCard>
        <div className="mb-3 text-title-sm font-semibold">Search Settings</div>
        <div className="grid gap-3 md:grid-cols-[minmax(0,260px)_minmax(0,1fr)]">
          <InputField
            label="SearXNG Base URL"
            value={appSettingsDraft.searxng_base_url || ""}
            onChange={(event) =>
              setAppSettingsDraft((prev) => ({
                ...prev,
                searxng_base_url: event.target.value,
              }))
            }
            placeholder="http://llms/searxng/"
          />
          <div className="rounded-md bg-surface-container-low p-3 text-body-md text-on-surface-variant">
            URL of your local SearXNG instance for AI-powered web search. Leave empty to disable the
            search feature. JSON format must be enabled on the SearXNG instance.
          </div>
        </div>

        <div className="mt-4 flex flex-wrap gap-2">
          <Button variant="primary" disabled={savingSettings} onClick={() => void saveAppSettings()}>
            {savingSettings ? "Saving..." : "Save Search Settings"}
          </Button>
        </div>
      </SurfaceCard>

      <SurfaceCard>
        <div className="mb-3 text-title-sm font-semibold">Repository Merging</div>
        <div className="rounded-md bg-surface-container-low p-3 text-body-md text-on-surface-variant">
          Current repository:
          <div className="mt-2 font-mono text-label-sm text-on-surface">
            {repositoryStatus?.path || "No repository attached"}
          </div>
        </div>
        <div className="mt-3 text-body-md text-on-surface-variant">
          Choose one external repository folder. It will be merged immediately into the attached repository.
        </div>
        <div className="mt-4 flex flex-wrap gap-2">
          <Button variant="primary" disabled={mergePending} onClick={() => void handleMergeRepository()}>
            {mergePending ? "Picking Repository..." : "Merge Repository"}
          </Button>
        </div>
      </SurfaceCard>
    </div>
  );
}
