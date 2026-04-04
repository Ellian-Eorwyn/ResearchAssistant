import { Navigate, Route, Routes } from "react-router-dom";

import { AppShell } from "./layout/AppShell";
import {
  AdvancedSettingsPage,
  BibliographyPage,
  CitationExtractionPage,
  CitationsPage,
  DocumentsPage,
  IngestionProfilesPage,
  JobHistoryPage,
  LandingPage,
  LlmBackendPage,
  ManifestPage,
  MatchesPage,
  MergeRepositoriesPage,
  OverviewPage,
  ProjectProfilePage,
  RepositoryBrowserPage,
  RepositorySettingsPage,
  ResearchPurposePage,
  SentencesPage,
  SourceCapturePage,
} from "./pages/pages";
import { useAppState } from "./state/AppState";

export default function App() {
  const { repoLoaded } = useAppState();

  if (!repoLoaded) {
    return <LandingPage />;
  }

  return (
    <Routes>
      <Route element={<AppShell />} path="/">
        <Route element={<Navigate replace to="/project/overview" />} index />

        <Route element={<OverviewPage />} path="project/overview" />
        <Route element={<DocumentsPage />} path="project/ingest" />
        <Route element={<Navigate replace to="/project/ingest" />} path="project/documents" />
        <Route element={<Navigate replace to="/project/ingest" />} path="project/source-lists" />
        <Route element={<MergeRepositoriesPage />} path="project/merge" />

        <Route element={<CitationExtractionPage />} path="processing/citation-extraction" />
        <Route element={<SourceCapturePage />} path="processing/source-capture" />
        <Route element={<Navigate replace to="/processing/source-capture" />} path="processing/summaries-ratings" />
        <Route element={<JobHistoryPage />} path="processing/job-history" />

        <Route element={<ManifestPage />} path="data/manifest" />
        <Route element={<RepositoryBrowserPage />} path="data/repository-browser" />
        <Route element={<CitationsPage />} path="data/citations" />
        <Route element={<BibliographyPage />} path="data/bibliography" />
        <Route element={<SentencesPage />} path="data/sentences" />
        <Route element={<MatchesPage />} path="data/matches" />

        <Route element={<ResearchPurposePage />} path="research/purpose" />
        <Route element={<ProjectProfilePage />} path="research/project-profile" />
        <Route element={<LlmBackendPage />} path="settings/llm-backend" />
        <Route element={<IngestionProfilesPage />} path="settings/ingestion-profiles" />
        <Route element={<RepositorySettingsPage />} path="settings/repository" />
        <Route element={<AdvancedSettingsPage />} path="settings/advanced" />

        <Route element={<Navigate replace to="/project/overview" />} path="*" />
      </Route>
    </Routes>
  );
}
