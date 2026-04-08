import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { api } from "../api/client";
import type { SearchJobStatus, SearchImportResponse } from "../api/types";
import { Button, SectionHeader, SurfaceCard, TextAreaField } from "../components/primitives";
import { useAppState } from "../state/AppState";

const POLL_INTERVAL_MS = 2000;
const DEFAULT_TARGET_COUNT = 200;
const DEFAULT_THRESHOLD = 0.40;

function relevanceBadge(score: number): string {
  if (score >= 0.7) return "text-green-400";
  if (score >= 0.4) return "text-yellow-400";
  return "text-on-surface-variant";
}

function stateLabel(state: SearchJobStatus["state"]): string {
  switch (state) {
    case "pending":
      return "Preparing...";
    case "generating_queries":
      return "Generating search queries...";
    case "searching":
      return "Searching the web...";
    case "scoring":
      return "Scoring relevance...";
    case "completed":
      return "Search complete";
    case "failed":
      return "Search failed";
    default:
      return state;
  }
}

export function SearchPage() {
  const { repoSettings } = useAppState();

  const [prompt, setPrompt] = useState("");
  const [targetCount, setTargetCount] = useState(DEFAULT_TARGET_COUNT);
  const [jobId, setJobId] = useState<string | null>(null);
  const [status, setStatus] = useState<SearchJobStatus | null>(null);
  const [threshold, setThreshold] = useState(DEFAULT_THRESHOLD);
  const [importResult, setImportResult] = useState<SearchImportResponse | null>(null);
  const [importing, setImporting] = useState(false);
  const [error, setError] = useState("");
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const isActive =
    status != null &&
    status.state !== "completed" &&
    status.state !== "failed";

  // ---- Polling ----
  const stopPolling = useCallback(() => {
    if (pollRef.current) {
      clearInterval(pollRef.current);
      pollRef.current = null;
    }
  }, []);

  useEffect(() => {
    if (!jobId || !isActive) {
      stopPolling();
      return;
    }
    const tick = async () => {
      try {
        const s = await api.getSearchStatus(jobId);
        setStatus(s);
        if (s.state === "completed" || s.state === "failed") {
          stopPolling();
        }
      } catch {
        // keep polling on transient errors
      }
    };
    pollRef.current = setInterval(() => void tick(), POLL_INTERVAL_MS);
    return stopPolling;
  }, [jobId, isActive, stopPolling]);

  // ---- Actions ----
  const handleStart = async () => {
    setError("");
    setImportResult(null);
    setStatus(null);
    try {
      const s = await api.startSearch(prompt.trim(), targetCount);
      setJobId(s.job_id);
      setStatus(s);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const handleCancel = async () => {
    if (!jobId) return;
    try {
      await api.cancelSearch(jobId);
    } catch {
      // best-effort
    }
  };

  const handleImport = async () => {
    if (!jobId) return;
    setImporting(true);
    setError("");
    try {
      const res = await api.importSearchResults(jobId, threshold);
      setImportResult(res);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setImporting(false);
    }
  };

  // ---- Computed ----
  const results = status?.results ?? [];
  const passingResults = useMemo(
    () => results.filter((r) => r.relevance_score >= threshold),
    [results, threshold],
  );

  const canStart = prompt.trim().length > 0 && !isActive;
  const isCompleted = status?.state === "completed";
  const isFailed = status?.state === "failed";
  const hasSearxng = Boolean(repoSettings.searxng_base_url);
  const hasLlm = repoSettings.use_llm;

  return (
    <div className="space-y-4">
      <SectionHeader
        title="Search"
        description="Enter a research prompt to find and evaluate sources from the web using AI-powered search."
      />

      {/* Prerequisites check */}
      {(!hasSearxng || !hasLlm) && (
        <SurfaceCard className="border border-warning/30 bg-warning/10">
          <div className="text-body-md text-warning">
            {!hasLlm && "LLM must be enabled in Settings. "}
            {!hasSearxng && "SearXNG base URL must be configured in Settings."}
          </div>
        </SurfaceCard>
      )}

      {error && (
        <SurfaceCard className="border border-error/30 bg-error/10">
          <div className="text-body-md text-error">{error}</div>
        </SurfaceCard>
      )}

      {/* Input Section */}
      <SurfaceCard>
        <div className="mb-3 text-title-sm font-semibold">Research Prompt</div>
        <TextAreaField
          label=""
          value={prompt}
          onChange={(e) => setPrompt(e.target.value)}
          placeholder="Describe what you're looking for, e.g. 'Recent studies on the environmental impact of lithium mining in South America'"
          rows={3}
        />

        <div className="mt-3 flex items-end gap-4">
          <div className="flex flex-col gap-1">
            <label className="text-label-sm text-on-surface-variant">
              Target results: {targetCount}
            </label>
            <input
              type="range"
              min={50}
              max={500}
              step={50}
              value={targetCount}
              onChange={(e) => setTargetCount(Number(e.target.value))}
              className="w-48 accent-primary"
            />
          </div>
          <Button
            variant="primary"
            disabled={!canStart || !hasSearxng || !hasLlm}
            onClick={() => void handleStart()}
          >
            Search
          </Button>
        </div>
      </SurfaceCard>

      {/* Progress Section */}
      {status && isActive && (
        <SurfaceCard>
          <div className="mb-2 text-title-sm font-semibold">{stateLabel(status.state)}</div>

          {status.generated_queries.length > 0 && (
            <div className="mb-3">
              <div className="mb-1 text-label-sm text-on-surface-variant">Generated queries:</div>
              <ul className="list-inside list-disc space-y-0.5 text-body-sm text-on-surface-variant">
                {status.generated_queries.map((q, i) => (
                  <li key={i}>{q}</li>
                ))}
              </ul>
            </div>
          )}

          <div className="flex flex-wrap gap-4 text-body-md text-on-surface-variant">
            {status.total_queries > 0 && (
              <span>
                Queries: {status.queries_completed}/{status.total_queries}
              </span>
            )}
            {status.results_found > 0 && <span>Results found: {status.results_found}</span>}
            {status.results_total > 0 && (
              <span>
                Scored: {status.results_scored}/{status.results_total}
              </span>
            )}
          </div>

          {/* Progress bar */}
          {status.state === "scoring" && status.results_total > 0 && (
            <div className="mt-3 h-2 w-full overflow-hidden rounded-full bg-surface-container-low">
              <div
                className="h-full rounded-full bg-primary transition-all"
                style={{
                  width: `${Math.round((status.results_scored / status.results_total) * 100)}%`,
                }}
              />
            </div>
          )}

          <div className="mt-3">
            <Button variant="secondary" onClick={() => void handleCancel()}>
              Cancel
            </Button>
          </div>
        </SurfaceCard>
      )}

      {/* Failed state */}
      {isFailed && status && (
        <SurfaceCard className="border border-error/30 bg-error/10">
          <div className="text-body-md text-error">
            Search failed: {status.error_message || "Unknown error"}
          </div>
        </SurfaceCard>
      )}

      {/* Results Section */}
      {isCompleted && results.length > 0 && (
        <>
          {/* Threshold slider + import */}
          <SurfaceCard>
            <div className="flex flex-wrap items-center gap-6">
              <div className="flex flex-col gap-1">
                <label className="text-label-sm text-on-surface-variant">
                  Relevance threshold: {threshold.toFixed(2)}
                </label>
                <input
                  type="range"
                  min={0}
                  max={1}
                  step={0.01}
                  value={threshold}
                  onChange={(e) => setThreshold(Number(e.target.value))}
                  className="w-64 accent-primary"
                />
              </div>

              <div className="text-body-md text-on-surface">
                <span className="font-semibold text-primary">{passingResults.length}</span>
                {" of "}
                <span>{results.length}</span>
                {" results at \u2265 "}
                {threshold.toFixed(2)} relevance
              </div>

              <Button
                variant="primary"
                disabled={importing || passingResults.length === 0}
                onClick={() => void handleImport()}
              >
                {importing
                  ? "Importing..."
                  : `Add ${passingResults.length} Result${passingResults.length !== 1 ? "s" : ""} to Repository`}
              </Button>
            </div>

            {importResult && (
              <div className="mt-3 rounded-md bg-surface-container-low p-3 text-body-md text-on-surface-variant">
                Imported {importResult.imported_count} new source
                {importResult.imported_count !== 1 ? "s" : ""}.
                {importResult.duplicates_skipped > 0 &&
                  ` ${importResult.duplicates_skipped} duplicate${importResult.duplicates_skipped !== 1 ? "s" : ""} skipped.`}
                {importResult.message && ` ${importResult.message}`}
              </div>
            )}
          </SurfaceCard>

          {/* Results table */}
          <SurfaceCard>
            <div className="mb-2 text-title-sm font-semibold">
              Results ({results.length})
            </div>
            <div className="thin-scrollbar max-h-[600px] overflow-auto">
              <table className="data-table w-full text-body-sm">
                <thead>
                  <tr>
                    <th className="w-16 text-right">Score</th>
                    <th>Title</th>
                    <th className="hidden md:table-cell">Snippet</th>
                    <th className="w-20">Engine</th>
                    <th className="hidden lg:table-cell w-24">Date</th>
                  </tr>
                </thead>
                <tbody>
                  {results.map((r, i) => {
                    const below = r.relevance_score < threshold;
                    return (
                      <tr
                        key={i}
                        className={below ? "opacity-35" : undefined}
                      >
                        <td className={`text-right font-mono font-semibold ${relevanceBadge(r.relevance_score)}`}>
                          {r.relevance_score.toFixed(2)}
                        </td>
                        <td>
                          <a
                            href={r.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="text-primary hover:underline"
                            title={r.url}
                          >
                            {r.title || r.url}
                          </a>
                        </td>
                        <td className="hidden max-w-xs truncate text-on-surface-variant md:table-cell">
                          {r.snippet}
                        </td>
                        <td className="text-on-surface-variant">{r.engine}</td>
                        <td className="hidden text-on-surface-variant lg:table-cell">
                          {r.published_date || "-"}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </SurfaceCard>
        </>
      )}

      {isCompleted && results.length === 0 && (
        <SurfaceCard>
          <div className="py-8 text-center text-body-md text-on-surface-variant">
            No results found. Try broadening your search prompt.
          </div>
        </SurfaceCard>
      )}
    </div>
  );
}
