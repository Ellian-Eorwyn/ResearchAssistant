import { useMemo, useRef, useState } from "react";

import { api } from "../api/client";
import { Button, InputField, SectionHeader, SelectField, SurfaceCard, TextAreaField } from "../components/primitives";
import { useAppState } from "../state/AppState";

const DEFAULT_PROJECT_PROFILE_FILENAME = "default_project_profile.yaml";

function buildProfileFilename(value: string): string {
  const stem = value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return `${stem || "project_profile"}.yaml`;
}

function generationBlockReason(
  useLlm: boolean,
  baseUrl: string,
  model: string,
): string {
  if (!useLlm) {
    return "Enable LLM-assisted features in Settings to generate project profile drafts.";
  }
  if (!baseUrl.trim()) {
    return "Set an LLM backend base URL in Settings to generate project profile drafts.";
  }
  if (!model.trim()) {
    return "Choose an LLM model in Settings to generate project profile drafts.";
  }
  return "";
}

export function AiGuidancePage() {
  const {
    profiles,
    settingsDraft,
    setSettingsDraft,
    saveRepoSettings,
    savingSettings,
    loadProfiles,
  } = useAppState();

  const uploadRef = useRef<HTMLInputElement | null>(null);
  const [statusMessage, setStatusMessage] = useState("");
  const [statusError, setStatusError] = useState("");
  const [draftName, setDraftName] = useState("");
  const [draftFilename, setDraftFilename] = useState("");
  const [draftContent, setDraftContent] = useState("");
  const [draftDirty, setDraftDirty] = useState(false);
  const [generationPending, setGenerationPending] = useState(false);
  const [saveDraftPending, setSaveDraftPending] = useState(false);
  const generationReason = useMemo(
    () =>
      generationBlockReason(
        settingsDraft.use_llm,
        settingsDraft.llm_backend.base_url,
        settingsDraft.llm_backend.model,
      ),
    [
      settingsDraft.llm_backend.base_url,
      settingsDraft.llm_backend.model,
      settingsDraft.use_llm,
    ],
  );

  const activeProfile = settingsDraft.default_project_profile_name.trim();
  const availableProfiles = profiles.length > 0 ? profiles : [{ filename: DEFAULT_PROJECT_PROFILE_FILENAME, name: "default_project_profile" }];

  const setActiveProfile = async (filename: string) => {
    const nextSettings = {
      ...settingsDraft,
      default_project_profile_name: filename,
    };
    setSettingsDraft(nextSettings);
    setStatusError("");
    try {
      await saveRepoSettings(nextSettings);
      setStatusMessage(`Active project profile set to ${filename || DEFAULT_PROJECT_PROFILE_FILENAME}.`);
    } catch (error) {
      setStatusError(String((error as Error).message || "Failed to save active profile"));
    }
  };

  const handleUpload = async (file: File | null) => {
    if (!file) return;
    setStatusMessage("");
    setStatusError("");
    try {
      const response = await api.uploadProjectProfile(file);
      await loadProfiles();
      const nextSettings = {
        ...settingsDraft,
        default_project_profile_name: response.filename,
      };
      setSettingsDraft(nextSettings);
      await saveRepoSettings(nextSettings);
      setDraftName(response.name);
      setDraftFilename(response.filename);
      setStatusMessage(`Uploaded and activated ${response.filename}.`);
    } catch (error) {
      setStatusError(String((error as Error).message || "Failed to upload project profile"));
    }
  };

  const generateDraft = async () => {
    const profileName = draftName.trim() || "Generated Project Profile";
    const filename = draftFilename.trim() || buildProfileFilename(profileName);
    setGenerationPending(true);
    setStatusMessage("");
    setStatusError("");
    try {
      const response = await api.generateProjectProfile({
        research_purpose: settingsDraft.research_purpose,
        profile_name: profileName,
        filename,
      });
      setDraftName(response.profile_name || profileName);
      setDraftFilename(response.filename || filename);
      setDraftContent(response.content || "");
      setDraftDirty(false);
      setStatusMessage(`Generated draft ${response.filename}. Review the YAML before saving.`);
    } catch (error) {
      setStatusError(String((error as Error).message || "Failed to generate project profile"));
    } finally {
      setGenerationPending(false);
    }
  };

  const saveGeneratedDraft = async () => {
    const filename = draftFilename.trim() || buildProfileFilename(draftName || "project_profile");
    if (!draftContent.trim()) {
      setStatusError("Generate or paste YAML content before saving.");
      return;
    }
    setSaveDraftPending(true);
    setStatusMessage("");
    setStatusError("");
    try {
      const response = await api.saveProjectProfile(filename, {
        content: draftContent,
      });
      await loadProfiles();
      const nextSettings = {
        ...settingsDraft,
        default_project_profile_name: response.filename,
      };
      setSettingsDraft(nextSettings);
      await saveRepoSettings(nextSettings);
      setDraftName(response.name || draftName);
      setDraftFilename(response.filename);
      setDraftContent(response.content || draftContent);
      setDraftDirty(false);
      setStatusMessage(`Saved and activated ${response.filename}.`);
    } catch (error) {
      setStatusError(String((error as Error).message || "Failed to save generated profile"));
    } finally {
      setSaveDraftPending(false);
    }
  };

  return (
    <div className="space-y-4">
      <SectionHeader
        title="AI Guidance"
        description="Set the research prompt and manage the project profile that drives summaries, metadata extraction, and source ratings."
      />

      {(statusMessage || statusError) && (
        <SurfaceCard className={statusError ? "border border-error/30 bg-error/10" : ""}>
          <div className={statusError ? "text-body-md text-error" : "text-body-md text-on-surface"}>
            {statusError || statusMessage}
          </div>
        </SurfaceCard>
      )}

      <SurfaceCard>
        <div className="mb-3 text-title-sm font-semibold">Research Prompt</div>
        <TextAreaField
          label="Research Prompt"
          rows={10}
          value={settingsDraft.research_purpose}
          onChange={(event) =>
            setSettingsDraft((prev) => ({
              ...prev,
              research_purpose: event.target.value,
            }))
          }
        />
        <div className="mt-3 flex flex-wrap gap-2">
          <Button variant="primary" disabled={savingSettings} onClick={() => void saveRepoSettings()}>
            {savingSettings ? "Saving..." : "Save Research Prompt"}
          </Button>
        </div>
      </SurfaceCard>

      <div className="grid gap-4 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)]">
        <SurfaceCard>
          <div className="mb-3 text-title-sm font-semibold">Project Profiles</div>
          <div className="grid gap-3">
            <SelectField
              label="Active Project Profile"
              value={activeProfile}
              onChange={(event) => void setActiveProfile(event.target.value)}
            >
              <option value="">Default bundled profile</option>
              {availableProfiles.map((profile) => (
                <option key={profile.filename} value={profile.filename}>
                  {profile.name} ({profile.filename})
                </option>
              ))}
            </SelectField>

            <div className="rounded-md bg-surface-container-low p-3 text-body-md text-on-surface-variant">
              Browser ratings stay aligned to the canonical profile contract. Generated YAML should keep
              `overall_relevance`, `depth_score`, `relevant_detail_score`, rationale, sections, tags, and flags.
            </div>

            <div className="flex flex-wrap gap-2">
              <Button onClick={() => void loadProfiles()}>Refresh Profiles</Button>
              <Button onClick={() => uploadRef.current?.click()}>Upload YAML Profile</Button>
            </div>

            <input
              ref={uploadRef}
              className="hidden"
              accept=".yaml,.yml"
              type="file"
              onChange={(event) => {
                const file = event.target.files?.[0] || null;
                void handleUpload(file);
                event.currentTarget.value = "";
              }}
            />

            <div className="space-y-2">
              {availableProfiles.map((profile) => (
                <div
                  key={profile.filename}
                  className="rounded-md bg-surface-container-low px-3 py-2 text-body-md"
                >
                  <div className="font-semibold text-on-surface">{profile.name}</div>
                  <div className="font-mono text-label-sm text-on-surface-variant">{profile.filename}</div>
                </div>
              ))}
            </div>
          </div>
        </SurfaceCard>

        <SurfaceCard>
          <div className="mb-3 flex items-start justify-between gap-3">
            <div>
              <div className="text-title-sm font-semibold">Generate Project Profile</div>
              <div className="mt-1 text-body-md text-on-surface-variant">
                Build a repo-local YAML draft from the current research prompt, then edit and save it.
              </div>
            </div>
            {generationReason ? (
              <span className="rounded-md bg-warning/10 px-3 py-2 text-label-sm uppercase tracking-[0.08em] text-warning">
                Generation disabled
              </span>
            ) : null}
          </div>

          <div className="grid gap-3 md:grid-cols-2">
            <InputField
              label="Profile Name"
              placeholder="Housing Evidence Review"
              value={draftName}
              onChange={(event) => {
                const value = event.target.value;
                setDraftName(value);
                if (!draftDirty && !draftFilename.trim()) {
                  setDraftFilename(buildProfileFilename(value));
                }
              }}
            />
            <InputField
              label="Filename"
              placeholder="housing_evidence_review.yaml"
              value={draftFilename}
              onChange={(event) => setDraftFilename(event.target.value)}
            />
          </div>

          {generationReason && (
            <div className="mt-3 rounded-md bg-warning/10 px-3 py-2 text-body-md text-warning">
              {generationReason}
            </div>
          )}

          <div className="mt-3 flex flex-wrap gap-2">
            <Button
              variant="primary"
              disabled={Boolean(generationReason) || generationPending}
              onClick={() => void generateDraft()}
            >
              {generationPending ? "Generating..." : "Generate Draft"}
            </Button>
            <Button
              disabled={saveDraftPending || !draftContent.trim()}
              onClick={() => void saveGeneratedDraft()}
            >
              {saveDraftPending ? "Saving..." : "Save Draft As Active Profile"}
            </Button>
          </div>

          <TextAreaField
            className="mt-4"
            label="Profile YAML"
            rows={22}
            placeholder="Generated YAML appears here."
            value={draftContent}
            onChange={(event) => {
              setDraftContent(event.target.value);
              setDraftDirty(true);
            }}
          />
        </SurfaceCard>
      </div>
    </div>
  );
}
