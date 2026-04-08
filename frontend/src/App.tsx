import { Navigate, Route, Routes } from "react-router-dom";

import { COVERED_LEGACY_REDIRECTS } from "./appRouteConfig";
import { AppShell } from "./layout/AppShell";
import {
  AdvancedSettingsPage,
  AiGuidancePage,
  BibliographyPage,
  CitationExtractionPage,
  CitationsPage,
  IngestionProfilesPage,
  JobHistoryPage,
  LandingPage,
  MatchesPage,
  OverviewPage,
  RepositoryBrowserPage,
  SearchPage,
  SentencesPage,
  SettingsPage,
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
        <Route element={<Navigate replace to="/browser" />} index />

        <Route element={<RepositoryBrowserPage />} path="browser" />
        <Route element={<SearchPage />} path="search" />
        <Route element={<AiGuidancePage />} path="ai-guidance" />
        <Route element={<SettingsPage />} path="settings" />

        <Route element={<OverviewPage />} path="project/overview" />
        <Route element={<CitationExtractionPage />} path="processing/citation-extraction" />
        <Route element={<JobHistoryPage />} path="processing/job-history" />
        <Route element={<CitationsPage />} path="data/citations" />
        <Route element={<BibliographyPage />} path="data/bibliography" />
        <Route element={<SentencesPage />} path="data/sentences" />
        <Route element={<MatchesPage />} path="data/matches" />
        <Route element={<IngestionProfilesPage />} path="settings/ingestion-profiles" />
        <Route element={<AdvancedSettingsPage />} path="settings/advanced" />

        {COVERED_LEGACY_REDIRECTS.map((entry) => (
          <Route
            key={entry.from}
            element={<Navigate replace to={entry.to} />}
            path={entry.from}
          />
        ))}

        <Route element={<Navigate replace to="/browser" />} path="*" />
      </Route>
    </Routes>
  );
}
