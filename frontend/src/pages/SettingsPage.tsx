import { useState } from "react";

import { Button, InputField, SectionHeader, SelectField, SurfaceCard } from "../components/primitives";
import { useAppState } from "../state/AppState";

export function SettingsPage() {
  const {
    mergeRepositories,
    pickRepositoryDirectory,
    repoError,
    repoMessage,
    repositoryStatus,
    saveRepoSettings,
    savingSettings,
    settingsDraft,
    setSettingsDraft,
    models,
    loadModels,
    loadingModels,
  } = useAppState();
  const [mergePending, setMergePending] = useState(false);

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
        description="Configure the attached repository's language model backend, fetch pacing, and merge operations."
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
        <div className="grid gap-3 md:grid-cols-2">
          <SelectField
            label="Backend Type"
            value={settingsDraft.llm_backend.kind}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  kind: event.target.value,
                },
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
            value={settingsDraft.llm_backend.api_key}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
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
                value={settingsDraft.llm_backend.model}
                onChange={(event) =>
                  setSettingsDraft((prev) => ({
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
            value={String(settingsDraft.llm_backend.temperature)}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  temperature: Number.parseFloat(event.target.value || "0"),
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
            step={1024}
            value={String(settingsDraft.llm_backend.num_ctx)}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                llm_backend: {
                  ...prev.llm_backend,
                  num_ctx: Number.parseInt(event.target.value || "8192", 10),
                },
              }))
            }
          />

          <InputField
            label="Max Source Chars"
            type="number"
            min={0}
            step={1000}
            value={String(settingsDraft.llm_backend.max_source_chars)}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
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
            value={String(settingsDraft.llm_backend.llm_timeout)}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
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
            checked={settingsDraft.use_llm}
            type="checkbox"
            onChange={(event) =>
              setSettingsDraft((prev) => ({
                ...prev,
                use_llm: event.target.checked,
              }))
            }
          />
          Use LLM-assisted features by default
        </label>

        <div className="mt-4 flex flex-wrap gap-2">
          <Button variant="primary" disabled={savingSettings} onClick={() => void saveRepoSettings()}>
            {savingSettings ? "Saving..." : "Save LLM Backend Settings"}
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
            value={String(settingsDraft.fetch_delay)}
            onChange={(event) =>
              setSettingsDraft((prev) => ({
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
          <Button variant="primary" disabled={savingSettings} onClick={() => void saveRepoSettings()}>
            {savingSettings ? "Saving..." : "Save Fetch Delay"}
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
