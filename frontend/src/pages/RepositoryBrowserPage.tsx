import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type KeyboardEvent as ReactKeyboardEvent,
  type MouseEvent as ReactMouseEvent,
} from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";

import { api } from "../api/client";
import type {
  CitationFieldEvidence,
  RepositoryColumnOutputConstraint,
  RepositoryColumnRunStatus,
  RepositoryManifestColumn,
  RepositoryManifestRow,
  RepositorySourceFileKind,
  RepositorySourcePatchRequest,
} from "../api/types";
import {
  Button,
  EmptyState,
  InputField,
  SectionHeader,
  SelectField,
  StatusBadge,
  SurfaceCard,
  TextAreaField,
} from "../components/primitives";
import { useAppState } from "../state/AppState";
import {
  buildRepositoryManifestFilterPayload,
  buildRepositoryBrowserQuery,
  buildRepositoryBrowserStorageKey,
  clampRepositoryBrowserColumnWidth,
  labelRepositoryBrowserColumn,
  migrateRepositoryBrowserVisibleColumns,
  mergeRepositoryBrowserColumns,
  nextRepositoryBrowserSort,
  resolveRepositoryBrowserColumnWidth,
  REPOSITORY_BROWSER_COLUMN_CATEGORIES,
  REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS,
  REPOSITORY_BROWSER_FILE_COLUMNS,
  REPOSITORY_BROWSER_PAGE_SIZE,
  toggleRepositoryBrowserSelection,
  type RepositoryBrowserFilters,
  type RepositoryBrowserStoredState,
} from "./repositoryBrowserUtils";

const REPOSITORY_BROWSER_SELECTION_COLUMN_WIDTH = 52;
const REPOSITORY_BROWSER_ACTION_ROW_HEIGHT = 76;
const REPOSITORY_BROWSER_ACTION_RAIL_WIDTH = 60;

const DEFAULT_FILTERS: RepositoryBrowserFilters = {
  q: "",
  fetchStatus: "",
  detectedType: "",
  sourceKind: "",
  documentType: "",
  organizationType: "",
  organizationName: "",
  authorNames: "",
  publicationDate: "",
  tagsText: "",
  hasSummary: "",
  hasRating: "",
  ratingOverallRelevanceMin: "",
  ratingOverallRelevanceMax: "",
  ratingDepthScoreMin: "",
  ratingDepthScoreMax: "",
  ratingRelevantDetailScoreMin: "",
  ratingRelevantDetailScoreMax: "",
  citationType: "",
  citationDoi: "",
  citationReportNumber: "",
  citationStandardNumber: "",
  citationMissingFields: "",
  citationReady: "",
  citationConfidenceMin: "",
  citationConfidenceMax: "",
  sortBy: "",
  sortDir: "",
  limit: REPOSITORY_BROWSER_PAGE_SIZE,
  offset: 0,
};

type BrowserTaskScope = "all" | "selected" | "empty_only";

interface SourceDetailsDraft {
  title: string;
  author_names: string;
  publication_date: string;
  document_type: string;
  organization_name: string;
  organization_type: string;
  tags_text: string;
  notes: string;
  summary_text: string;
  overall_relevance: string;
  depth_score: string;
  relevant_detail_score: string;
  rating_rationale: string;
  relevant_sections: string;
  citation_title: string;
  citation_authors: string;
  citation_issued: string;
  citation_type: string;
  citation_url: string;
  citation_publisher: string;
  citation_container_title: string;
  citation_volume: string;
  citation_issue: string;
  citation_pages: string;
  citation_doi: string;
  citation_report_number: string;
  citation_standard_number: string;
  citation_language: string;
  citation_accessed: string;
}

const CITATION_EDIT_FIELDS: Array<{
  draftKey: keyof Pick<
    SourceDetailsDraft,
    | "citation_title"
    | "citation_authors"
    | "citation_issued"
    | "citation_type"
    | "citation_url"
    | "citation_publisher"
    | "citation_container_title"
    | "citation_volume"
    | "citation_issue"
    | "citation_pages"
    | "citation_doi"
    | "citation_report_number"
    | "citation_standard_number"
    | "citation_language"
    | "citation_accessed"
  >;
  rowKey: keyof Pick<
    RepositoryManifestRow,
    | "citation_title"
    | "citation_authors"
    | "citation_issued"
    | "citation_type"
    | "citation_url"
    | "citation_publisher"
    | "citation_container_title"
    | "citation_volume"
    | "citation_issue"
    | "citation_pages"
    | "citation_doi"
    | "citation_report_number"
    | "citation_standard_number"
    | "citation_language"
    | "citation_accessed"
  >;
  overrideField: string;
}> = [
  { draftKey: "citation_title", rowKey: "citation_title", overrideField: "title" },
  { draftKey: "citation_authors", rowKey: "citation_authors", overrideField: "authors" },
  { draftKey: "citation_issued", rowKey: "citation_issued", overrideField: "issued" },
  { draftKey: "citation_type", rowKey: "citation_type", overrideField: "item_type" },
  { draftKey: "citation_url", rowKey: "citation_url", overrideField: "url" },
  { draftKey: "citation_publisher", rowKey: "citation_publisher", overrideField: "publisher" },
  {
    draftKey: "citation_container_title",
    rowKey: "citation_container_title",
    overrideField: "container_title",
  },
  { draftKey: "citation_volume", rowKey: "citation_volume", overrideField: "volume" },
  { draftKey: "citation_issue", rowKey: "citation_issue", overrideField: "issue" },
  { draftKey: "citation_pages", rowKey: "citation_pages", overrideField: "pages" },
  { draftKey: "citation_doi", rowKey: "citation_doi", overrideField: "doi" },
  {
    draftKey: "citation_report_number",
    rowKey: "citation_report_number",
    overrideField: "report_number",
  },
  {
    draftKey: "citation_standard_number",
    rowKey: "citation_standard_number",
    overrideField: "standard_number",
  },
  { draftKey: "citation_language", rowKey: "citation_language", overrideField: "language" },
  { draftKey: "citation_accessed", rowKey: "citation_accessed", overrideField: "accessed" },
];

const CITATION_EVIDENCE_FIELDS: Array<{
  key: string;
  label: string;
  valueKey: keyof Pick<
    RepositoryManifestRow,
    | "citation_type"
    | "citation_title"
    | "citation_authors"
    | "citation_issued"
    | "citation_publisher"
    | "citation_container_title"
    | "citation_volume"
    | "citation_issue"
    | "citation_pages"
    | "citation_doi"
    | "citation_url"
    | "citation_report_number"
    | "citation_standard_number"
    | "citation_language"
    | "citation_accessed"
  >;
}> = [
  { key: "item_type", label: "Type", valueKey: "citation_type" },
  { key: "title", label: "Title", valueKey: "citation_title" },
  { key: "authors", label: "Authors", valueKey: "citation_authors" },
  { key: "issued", label: "Issued", valueKey: "citation_issued" },
  { key: "publisher", label: "Publisher", valueKey: "citation_publisher" },
  { key: "container_title", label: "Container Title", valueKey: "citation_container_title" },
  { key: "volume", label: "Volume", valueKey: "citation_volume" },
  { key: "issue", label: "Issue", valueKey: "citation_issue" },
  { key: "pages", label: "Pages", valueKey: "citation_pages" },
  { key: "doi", label: "DOI", valueKey: "citation_doi" },
  { key: "url", label: "URL", valueKey: "citation_url" },
  { key: "report_number", label: "Report Number", valueKey: "citation_report_number" },
  { key: "standard_number", label: "Standard Number", valueKey: "citation_standard_number" },
  { key: "language", label: "Language", valueKey: "citation_language" },
  { key: "accessed", label: "Accessed", valueKey: "citation_accessed" },
];

function hasRawFileWithSuffix(row: RepositoryManifestRow, suffixes: string[]): boolean {
  const rawFile = String(row.raw_file || "").trim().toLowerCase();
  return suffixes.some((suffix) => rawFile.endsWith(suffix));
}

function hasFileForKind(row: RepositoryManifestRow, kind: RepositorySourceFileKind): boolean {
  if (kind === "pdf") return hasRawFileWithSuffix(row, [".pdf"]);
  if (kind === "html") return hasRawFileWithSuffix(row, [".html", ".htm"]);
  if (kind === "rendered") {
    return Boolean(String(row.rendered_file || "").trim() || String(row.rendered_pdf_file || "").trim());
  }
  return Boolean(String(row.llm_cleanup_file || "").trim() || String(row.markdown_file || "").trim());
}

function buildFileHref(row: RepositoryManifestRow, kind: RepositorySourceFileKind): string {
  return `/api/repository/sources/${encodeURIComponent(row.id)}/files/${kind}`;
}

function formatCellValue(value: string | number | boolean | null | undefined): string {
  if (value === null || value === undefined || value === "") return "—";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  return String(value);
}

function formatScoreDraft(value: unknown): string {
  if (value === null || value === undefined || value === "") return "";
  return String(value);
}

function parseScoreDraft(value: string): number | null | undefined {
  const normalized = value.trim();
  if (!normalized) return null;
  const parsed = Number(normalized);
  if (!Number.isFinite(parsed)) return undefined;
  return Math.max(0, Math.min(1, parsed));
}

function splitSemicolonValues(value: string): string[] {
  return String(value || "")
    .split(";")
    .map((item) => item.trim())
    .filter(Boolean);
}

function parseCitationFieldEvidenceMap(
  rawValue: string | null | undefined,
): Record<string, CitationFieldEvidence> {
  if (!rawValue) return {};
  try {
    const parsed = JSON.parse(rawValue);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    const entries = Object.entries(parsed as Record<string, unknown>);
    const normalized: Record<string, CitationFieldEvidence> = {};
    entries.forEach(([key, value]) => {
      if (!value || typeof value !== "object" || Array.isArray(value)) return;
      const candidate = value as Record<string, unknown>;
      normalized[key] = {
        value: String(candidate.value || ""),
        source_type: String(candidate.source_type || ""),
        source_label: String(candidate.source_label || ""),
        evidence: String(candidate.evidence || ""),
        confidence: Number(candidate.confidence || 0),
        manual_override: Boolean(candidate.manual_override),
      };
    });
    return normalized;
  } catch {
    return {};
  }
}

function createSourceDetailsDraft(row: RepositoryManifestRow): SourceDetailsDraft {
  return {
    title: String(row.title || ""),
    author_names: String(row.author_names || ""),
    publication_date: String(row.publication_date || ""),
    document_type: String(row.document_type || ""),
    organization_name: String(row.organization_name || ""),
    organization_type: String(row.organization_type || ""),
    tags_text: String(row.tags_text || ""),
    notes: String(row.notes || ""),
    summary_text: String(row.summary_text || ""),
    overall_relevance: formatScoreDraft(row.rating_overall_relevance),
    depth_score: formatScoreDraft(row.rating_depth_score),
    relevant_detail_score: formatScoreDraft(row.rating_relevant_detail_score),
    rating_rationale: String(row.rating_rationale || ""),
    relevant_sections: String(row.relevant_sections || ""),
    citation_title: String(row.citation_title || ""),
    citation_authors: String(row.citation_authors || ""),
    citation_issued: String(row.citation_issued || ""),
    citation_type: String(row.citation_type || ""),
    citation_url: String(row.citation_url || ""),
    citation_publisher: String(row.citation_publisher || ""),
    citation_container_title: String(row.citation_container_title || ""),
    citation_volume: String(row.citation_volume || ""),
    citation_issue: String(row.citation_issue || ""),
    citation_pages: String(row.citation_pages || ""),
    citation_doi: String(row.citation_doi || ""),
    citation_report_number: String(row.citation_report_number || ""),
    citation_standard_number: String(row.citation_standard_number || ""),
    citation_language: String(row.citation_language || ""),
    citation_accessed: String(row.citation_accessed || ""),
  };
}

function buildSourcePatch(
  row: RepositoryManifestRow | null,
  draft: SourceDetailsDraft | null,
): RepositorySourcePatchRequest | null {
  if (!row || !draft) return null;
  const patch: RepositorySourcePatchRequest = {};

  const textFields: Array<keyof Pick<
    SourceDetailsDraft,
    | "title"
    | "author_names"
    | "publication_date"
    | "document_type"
    | "organization_name"
    | "organization_type"
    | "tags_text"
    | "notes"
    | "summary_text"
    | "rating_rationale"
    | "relevant_sections"
  >> = [
    "title",
    "author_names",
    "publication_date",
    "document_type",
    "organization_name",
    "organization_type",
    "tags_text",
    "notes",
    "summary_text",
    "rating_rationale",
    "relevant_sections",
  ];

  textFields.forEach((field) => {
    const nextValue = draft[field];
    const currentValue = String((row as Record<string, unknown>)[field] || "");
    if (nextValue !== currentValue) {
      (patch as Record<string, unknown>)[field] = nextValue;
    }
  });

  const numericFields: Array<{
    draftKey: keyof Pick<
      SourceDetailsDraft,
      "overall_relevance" | "depth_score" | "relevant_detail_score"
    >;
    rowKey: keyof Pick<
      RepositoryManifestRow,
      "rating_overall_relevance" | "rating_depth_score" | "rating_relevant_detail_score"
    >;
  }> = [
    { draftKey: "overall_relevance", rowKey: "rating_overall_relevance" },
    { draftKey: "depth_score", rowKey: "rating_depth_score" },
    { draftKey: "relevant_detail_score", rowKey: "rating_relevant_detail_score" },
  ];

  numericFields.forEach(({ draftKey, rowKey }) => {
    const nextValue = parseScoreDraft(draft[draftKey]);
    if (nextValue === undefined) return;
    const currentRaw = row[rowKey];
    const currentValue =
      currentRaw === null || currentRaw === undefined || currentRaw === ""
        ? null
        : Number(currentRaw);
    if ((currentValue ?? null) !== (nextValue ?? null)) {
      (patch as Record<string, unknown>)[draftKey] = nextValue;
    }
  });

  const changedCitationOverrideFields: string[] = [];
  CITATION_EDIT_FIELDS.forEach(({ draftKey, rowKey, overrideField }) => {
    const nextValue = draft[draftKey];
    const currentValue = String(row[rowKey] || "");
    if (nextValue !== currentValue) {
      (patch as Record<string, unknown>)[draftKey] = nextValue;
      changedCitationOverrideFields.push(overrideField);
    }
  });
  if (changedCitationOverrideFields.length > 0) {
    patch.citation_override_fields = changedCitationOverrideFields;
  }

  return Object.keys(patch).length > 0 ? patch : null;
}

function downloadBlob(blob: Blob, filename: string): void {
  const objectUrl = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = objectUrl;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  window.URL.revokeObjectURL(objectUrl);
}

function statusTone(status: string): "neutral" | "success" | "warning" | "error" | "active" {
  const normalized = status.trim().toLowerCase();
  if (!normalized || normalized === "idle" || normalized === "unknown") return "neutral";
  if (
    normalized === "generated" ||
    normalized === "completed" ||
    normalized === "success" ||
    normalized === "existing"
  ) {
    return "success";
  }
  if (normalized === "running" || normalized === "pending" || normalized === "queued") {
    return "active";
  }
  if (normalized === "partial" || normalized === "cancelling") {
    return "warning";
  }
  if (normalized.includes("fail") || normalized.includes("error") || normalized === "cancelled") {
    return "error";
  }
  return "neutral";
}

function columnWidthStyle(
  columnWidths: Record<string, number>,
  columnKey: string,
): { minWidth: string; width: string; maxWidth: string } {
  const width = resolveRepositoryBrowserColumnWidth(columnWidths, columnKey);
  return {
    minWidth: `${width}px`,
    width: `${width}px`,
    maxWidth: `${width}px`,
  };
}

function SourceDetailsDrawer({
  row,
  draft,
  onChange,
  saveState,
  saveError,
}: {
  row: RepositoryManifestRow;
  draft: SourceDetailsDraft;
  onChange: (field: keyof SourceDetailsDraft, value: string) => void;
  saveState: "idle" | "saving" | "saved" | "error";
  saveError: string;
}) {
  const fileLinks = REPOSITORY_BROWSER_FILE_COLUMNS.filter((column) =>
    hasFileForKind(row, column.kind),
  );
  const citationEvidenceMap = useMemo(
    () => parseCitationFieldEvidenceMap(row.citation_field_evidence_json),
    [row.citation_field_evidence_json],
  );
  const citationBlockedReasons = splitSemicolonValues(String(row.citation_blocked_reasons || ""));
  const citationManualOverrideFields = splitSemicolonValues(
    String(row.citation_manual_override_fields || ""),
  );
  const citationStatus = String(row.citation_verification_status || "");
  const citationVerifiedAt = String(row.citation_verified_at || "");
  const citationConfidence = row.citation_confidence;
  const citationReady = Boolean(row.citation_ready);

  return (
    <aside className="min-h-0 h-full">
      <SurfaceCard className="flex h-full min-h-0 flex-col p-0">
        <div className="thin-scrollbar flex-1 overflow-y-auto p-4">
          <div className="space-y-4">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="text-title-sm font-semibold">Source Details</div>
            <div className="mt-1 font-mono text-label-sm text-on-surface-variant">{row.id}</div>
          </div>
          <StatusBadge
            text={
              saveState === "saving"
                ? "Saving"
                : saveState === "saved"
                  ? "Saved"
                  : saveState === "error"
                    ? "Save Error"
                    : "Idle"
            }
            tone={
              saveState === "saving"
                ? "active"
                : saveState === "saved"
                  ? "success"
                  : saveState === "error"
                    ? "error"
                    : "neutral"
            }
          />
        </div>

        {saveError && (
          <div className="rounded-md bg-error/10 px-3 py-2 text-body-md text-error">
            {saveError}
          </div>
        )}

        <div className="grid gap-2 sm:grid-cols-2">
          <div className="rounded-md bg-surface-container-low p-3">
            <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Fetch</div>
            <div className="mt-1">
              <StatusBadge text={row.fetch_status || "unknown"} tone={statusTone(row.fetch_status || "")} />
            </div>
          </div>
          <div className="rounded-md bg-surface-container-low p-3">
            <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Catalog</div>
            <div className="mt-1">
              <StatusBadge text={row.catalog_status || "unknown"} tone={statusTone(row.catalog_status || "")} />
            </div>
          </div>
          <div className="rounded-md bg-surface-container-low p-3">
            <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Summary</div>
            <div className="mt-1">
              <StatusBadge text={row.summary_status || "unknown"} tone={statusTone(row.summary_status || "")} />
            </div>
          </div>
          <div className="rounded-md bg-surface-container-low p-3">
            <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Citation</div>
            <div className="mt-1">
              <StatusBadge
                text={citationStatus || (citationReady ? "verified" : "unknown")}
                tone={statusTone(citationStatus || (citationReady ? "verified" : ""))}
              />
            </div>
          </div>
          <div className="rounded-md bg-surface-container-low p-3">
            <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Rating</div>
            <div className="mt-1">
              <StatusBadge text={row.rating_status || "unknown"} tone={statusTone(row.rating_status || "")} />
            </div>
          </div>
        </div>

        <div className="rounded-md bg-surface-container-low p-3">
          <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Document Type</div>
          <div className="mt-1 text-body-md text-on-surface">{row.detected_type || "Unknown"}</div>
          {row.source_kind && (
            <div className="mt-1 text-label-sm text-on-surface-variant">Source kind: {row.source_kind}</div>
          )}
        </div>

        <div>
          <div className="mb-2 text-title-sm font-semibold">Display Metadata</div>
          <div className="mb-3 text-body-md text-on-surface-variant">
            These fields drive repository browsing and local organization. They are separate from
            the citation metadata used for RIS export.
          </div>
          <div className="mb-2 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">Files</div>
          {fileLinks.length === 0 ? (
            <div className="rounded-md bg-surface-container-low px-3 py-2 text-body-md text-on-surface-variant">
              No repository files available for this row yet.
            </div>
          ) : (
            <div className="flex flex-wrap gap-2">
              {fileLinks.map((column) => (
                <a
                  key={column.id}
                  className="inline-flex items-center rounded-md bg-surface-container-low px-3 py-2 text-body-md text-primary hover:underline"
                  href={buildFileHref(row, column.kind)}
                  rel="noreferrer"
                  target="_blank"
                >
                  Open {column.label}
                </a>
              ))}
            </div>
          )}
        </div>

        <div className="grid gap-3">
          <InputField
            label="Title"
            value={draft.title}
            onChange={(event) => onChange("title", event.target.value)}
          />
          <InputField
            label="Authors"
            value={draft.author_names}
            onChange={(event) => onChange("author_names", event.target.value)}
          />
          <InputField
            label="Publication Date"
            value={draft.publication_date}
            onChange={(event) => onChange("publication_date", event.target.value)}
          />
          <InputField
            label="Document Type"
            value={draft.document_type}
            onChange={(event) => onChange("document_type", event.target.value)}
          />
          <InputField
            label="Organization"
            value={draft.organization_name}
            onChange={(event) => onChange("organization_name", event.target.value)}
          />
          <InputField
            label="Organization Type"
            value={draft.organization_type}
            onChange={(event) => onChange("organization_type", event.target.value)}
          />
          <InputField
            label="Tags"
            value={draft.tags_text}
            onChange={(event) => onChange("tags_text", event.target.value)}
          />
          <TextAreaField
            label="Notes"
            rows={4}
            value={draft.notes}
            onChange={(event) => onChange("notes", event.target.value)}
          />
        </div>

        <details className="rounded-md bg-surface-container-low p-3" open>
          <summary className="cursor-pointer text-title-sm font-semibold">
            Citation Metadata (RIS)
          </summary>
          <div className="mt-3 space-y-3">
            <div className="grid gap-2 sm:grid-cols-2">
              <div className="rounded-md bg-surface px-3 py-2">
                <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
                  Verification Status
                </div>
                <div className="mt-2">
                  <StatusBadge
                    text={citationStatus || (citationReady ? "verified" : "candidate")}
                    tone={statusTone(citationStatus || (citationReady ? "verified" : ""))}
                  />
                </div>
              </div>
              <div className="rounded-md bg-surface px-3 py-2">
                <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
                  RIS Export
                </div>
                <div className="mt-2">
                  <StatusBadge
                    text={citationReady ? "Ready" : "Blocked"}
                    tone={citationReady ? "success" : "warning"}
                  />
                </div>
              </div>
            </div>

            <div className="grid gap-2 sm:grid-cols-2">
              <div className="rounded-md bg-surface px-3 py-2 text-body-md">
                <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
                  Confidence
                </div>
                <div className="mt-1 text-on-surface">
                  {citationConfidence === null || citationConfidence === undefined || citationConfidence === ""
                    ? "—"
                    : String(citationConfidence)}
                </div>
              </div>
              <div className="rounded-md bg-surface px-3 py-2 text-body-md">
                <div className="text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
                  Verified At
                </div>
                <div className="mt-1 text-on-surface">{citationVerifiedAt || "—"}</div>
              </div>
            </div>

            {citationBlockedReasons.length > 0 && (
              <div className="rounded-md bg-warning/10 px-3 py-2 text-body-md text-on-surface">
                <div className="mb-1 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
                  Blocked Reasons
                </div>
                <div className="space-y-1">
                  {citationBlockedReasons.map((reason) => (
                    <div key={reason}>{reason}</div>
                  ))}
                </div>
              </div>
            )}

            {citationManualOverrideFields.length > 0 && (
              <div className="rounded-md bg-surface px-3 py-2 text-body-md text-on-surface">
                <div className="mb-1 text-label-sm uppercase tracking-[0.08em] text-on-surface-variant">
                  Manual Overrides
                </div>
                <div>{citationManualOverrideFields.join(", ")}</div>
              </div>
            )}

            <div className="grid gap-3">
              <InputField
                label="Citation Title"
                value={draft.citation_title}
                onChange={(event) => onChange("citation_title", event.target.value)}
              />
              <InputField
                label="Citation Authors"
                value={draft.citation_authors}
                onChange={(event) => onChange("citation_authors", event.target.value)}
              />
              <div className="grid gap-3 md:grid-cols-2">
                <InputField
                  label="Citation Issued"
                  value={draft.citation_issued}
                  onChange={(event) => onChange("citation_issued", event.target.value)}
                />
                <InputField
                  label="Citation Type"
                  value={draft.citation_type}
                  onChange={(event) => onChange("citation_type", event.target.value)}
                />
              </div>
              <InputField
                label="Citation URL"
                value={draft.citation_url}
                onChange={(event) => onChange("citation_url", event.target.value)}
              />
              <div className="grid gap-3 md:grid-cols-2">
                <InputField
                  label="Publisher"
                  value={draft.citation_publisher}
                  onChange={(event) => onChange("citation_publisher", event.target.value)}
                />
                <InputField
                  label="Container Title"
                  value={draft.citation_container_title}
                  onChange={(event) => onChange("citation_container_title", event.target.value)}
                />
              </div>
              <div className="grid gap-3 md:grid-cols-3">
                <InputField
                  label="Volume"
                  value={draft.citation_volume}
                  onChange={(event) => onChange("citation_volume", event.target.value)}
                />
                <InputField
                  label="Issue"
                  value={draft.citation_issue}
                  onChange={(event) => onChange("citation_issue", event.target.value)}
                />
                <InputField
                  label="Pages"
                  value={draft.citation_pages}
                  onChange={(event) => onChange("citation_pages", event.target.value)}
                />
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <InputField
                  label="DOI"
                  value={draft.citation_doi}
                  onChange={(event) => onChange("citation_doi", event.target.value)}
                />
                <InputField
                  label="Report Number"
                  value={draft.citation_report_number}
                  onChange={(event) => onChange("citation_report_number", event.target.value)}
                />
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <InputField
                  label="Standard Number"
                  value={draft.citation_standard_number}
                  onChange={(event) => onChange("citation_standard_number", event.target.value)}
                />
                <InputField
                  label="Language"
                  value={draft.citation_language}
                  onChange={(event) => onChange("citation_language", event.target.value)}
                />
              </div>
              <InputField
                label="Accessed"
                value={draft.citation_accessed}
                onChange={(event) => onChange("citation_accessed", event.target.value)}
              />
            </div>
          </div>
        </details>

        <details className="rounded-md bg-surface-container-low p-3">
          <summary className="cursor-pointer text-title-sm font-semibold">
            Citation Evidence
          </summary>
          <div className="mt-3 space-y-3">
            {CITATION_EVIDENCE_FIELDS.filter(({ key, valueKey }) => {
              const evidence = citationEvidenceMap[key];
              return Boolean(
                String(row[valueKey] || "").trim() ||
                  evidence?.source_type ||
                  evidence?.source_label ||
                  evidence?.evidence,
              );
            }).map(({ key, label, valueKey }) => {
              const evidence = citationEvidenceMap[key];
              return (
                <div key={key} className="rounded-md bg-surface px-3 py-3">
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="text-title-sm font-semibold">{label}</div>
                    <div className="flex flex-wrap items-center gap-2">
                      {evidence?.manual_override && (
                        <StatusBadge text="Manual Override" tone="warning" />
                      )}
                      <StatusBadge
                        text={String(evidence?.source_label || evidence?.source_type || "No evidence")}
                        tone={statusTone(String(evidence?.source_type || ""))}
                      />
                    </div>
                  </div>
                  <div className="mt-2 text-body-md text-on-surface">
                    {String(row[valueKey] || "—")}
                  </div>
                  <div className="mt-2 grid gap-2 text-body-md text-on-surface-variant md:grid-cols-2">
                    <div>
                      <span className="font-semibold text-on-surface">Source:</span>{" "}
                      {evidence?.source_type || "—"}
                    </div>
                    <div>
                      <span className="font-semibold text-on-surface">Confidence:</span>{" "}
                      {evidence && Number.isFinite(evidence.confidence)
                        ? evidence.confidence.toFixed(2)
                        : "—"}
                    </div>
                  </div>
                  {evidence?.evidence && (
                    <div className="mt-2 whitespace-pre-wrap rounded-md bg-surface-container-low px-3 py-2 text-body-md text-on-surface">
                      {evidence.evidence}
                    </div>
                  )}
                </div>
              );
            })}
            {CITATION_EVIDENCE_FIELDS.every(({ key, valueKey }) => {
              const evidence = citationEvidenceMap[key];
              return !String(row[valueKey] || "").trim() && !evidence?.evidence && !evidence?.source_type;
            }) && (
              <div className="rounded-md bg-surface px-3 py-2 text-body-md text-on-surface-variant">
                No citation evidence is stored for this source yet.
              </div>
            )}
          </div>
        </details>

        <details className="rounded-md bg-surface-container-low p-3" open>
          <summary className="cursor-pointer text-title-sm font-semibold">Summary</summary>
          <TextAreaField
            className="mt-3"
            label="Summary Text"
            rows={8}
            value={draft.summary_text}
            onChange={(event) => onChange("summary_text", event.target.value)}
          />
        </details>

        <div className="grid gap-3 md:grid-cols-3">
          <InputField
            label="Overall Relevance"
            type="number"
            min={0}
            max={1}
            step={0.05}
            value={draft.overall_relevance}
            onChange={(event) => onChange("overall_relevance", event.target.value)}
          />
          <InputField
            label="Depth Score"
            type="number"
            min={0}
            max={1}
            step={0.05}
            value={draft.depth_score}
            onChange={(event) => onChange("depth_score", event.target.value)}
          />
          <InputField
            label="Detail Score"
            type="number"
            min={0}
            max={1}
            step={0.05}
            value={draft.relevant_detail_score}
            onChange={(event) => onChange("relevant_detail_score", event.target.value)}
          />
        </div>

        <details className="rounded-md bg-surface-container-low p-3">
          <summary className="cursor-pointer text-title-sm font-semibold">Rating Rationale</summary>
          <TextAreaField
            className="mt-3"
            label="Rating Rationale"
            rows={7}
            value={draft.rating_rationale}
            onChange={(event) => onChange("rating_rationale", event.target.value)}
          />
        </details>

        <TextAreaField
          label="Relevant Sections"
          rows={6}
          value={draft.relevant_sections}
          onChange={(event) => onChange("relevant_sections", event.target.value)}
        />
          </div>
        </div>
      </SurfaceCard>
    </aside>
  );
}

interface ColumnPromptDraftState {
  columnId: string;
  label: string;
  prompt: string;
  outputConstraint: RepositoryColumnOutputConstraint | null;
}

type ColumnRunScope = "all" | "empty_only" | "selected";

interface ColumnRunScopeDraftState {
  columnId: string;
  label: string;
  scope: ColumnRunScope;
}

function repositoryColumnActionButtonClass(disabled = false): string {
  return [
    "inline-flex h-7 w-7 items-center justify-center rounded-sm border border-outline-variant/40 transition",
    disabled
      ? "cursor-not-allowed opacity-40"
      : "bg-surface text-on-surface-variant hover:border-primary hover:text-on-surface",
  ].join(" ");
}

function columnRunScopeOptionClass(selected: boolean, disabled: boolean): string {
  return [
    "rounded-md border px-3 py-3 text-left transition",
    selected
      ? "border-primary bg-primary/10 text-on-surface"
      : "border-outline-variant/30 bg-surface-container-low text-on-surface-variant",
    disabled ? "cursor-not-allowed opacity-40" : "hover:border-primary/50 hover:text-on-surface",
  ].join(" ");
}

function ColumnInstructionsIcon() {
  return (
    <svg aria-hidden="true" className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24">
      <path
        d="M9 18h6M10 22h4M8.2 14.7c-1.6-1.2-2.7-3.1-2.7-5.2a6.5 6.5 0 1 1 13 0c0 2.1-1 4-2.7 5.2-.8.6-1.3 1.3-1.5 2.1h-4.6c-.2-.8-.7-1.5-1.5-2.1Z"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.7"
      />
    </svg>
  );
}

function ColumnRunIcon() {
  return (
    <svg aria-hidden="true" className="h-3.5 w-3.5" fill="currentColor" viewBox="0 0 24 24">
      <path d="M8 6.5c0-1 1.1-1.6 2-.9l8 5.5a1.1 1.1 0 0 1 0 1.8l-8 5.5A1.1 1.1 0 0 1 8 17.5v-11Z" />
    </svg>
  );
}

function ColumnRenameIcon() {
  return (
    <svg aria-hidden="true" className="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24">
      <path
        d="m15.5 5.5 3 3M6 18l3.5-.7L19 7.8a1.4 1.4 0 0 0 0-2l-.8-.8a1.4 1.4 0 0 0-2 0l-9.5 9.5L6 18Z"
        stroke="currentColor"
        strokeLinecap="round"
        strokeLinejoin="round"
        strokeWidth="1.7"
      />
    </svg>
  );
}

function formatColumnActionStatus(
  column: RepositoryManifestColumn,
  activeRun: RepositoryColumnRunStatus | null,
): string {
  if (activeRun?.column_id === column.key) {
    return `${activeRun.processed_rows}/${activeRun.total_rows} processed`;
  }
  if (!column.processable) return "Not available";
  if (!column.instruction_prompt.trim()) return "No instructions";
  if (column.last_run_status.trim()) {
    return column.last_run_status.replace(/_/g, " ");
  }
  return "Ready";
}

function ColumnPromptModal({
  draft,
  fixingPrompt,
  llmReady,
  savePending,
  error,
  onChange,
  onCancel,
  onFixPrompt,
  onSave,
}: {
  draft: ColumnPromptDraftState;
  fixingPrompt: boolean;
  llmReady: boolean;
  savePending: boolean;
  error: string;
  onChange: (nextValue: string) => void;
  onCancel: () => void;
  onFixPrompt: () => void;
  onSave: () => void;
}) {
  return (
    <div className="fixed inset-0 z-40 flex items-center justify-center bg-surface/80 p-4 backdrop-blur-sm">
      <div
        className="w-full max-w-3xl rounded-xl border border-outline-variant/40 bg-surface-container p-5 shadow-2xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-title-sm font-semibold">Column Instructions</div>
            <div className="mt-1 text-body-md text-on-surface-variant">{draft.label}</div>
          </div>
          <button
            className="rounded-sm px-2 py-1 text-label-sm text-on-surface-variant hover:text-on-surface"
            onClick={onCancel}
            type="button"
          >
            Close
          </button>
        </div>

        <TextAreaField
          className="mt-4"
          label="Prompt"
          rows={12}
          value={draft.prompt}
          onChange={(event) => onChange(event.target.value)}
        />

        <div className="mt-3 text-body-md text-on-surface-variant">
          Fix Up Prompt and Run require an enabled LLM backend with a selected model.
        </div>
        {!llmReady && (
          <div className="mt-2 rounded-md bg-warning/10 px-3 py-2 text-body-md text-warning">
            Configure and enable the repository LLM backend to fix prompts or run a column.
          </div>
        )}
        {error && (
          <div className="mt-3 rounded-md bg-error/10 px-3 py-2 text-body-md text-error">
            {error}
          </div>
        )}

        <div className="mt-5 flex flex-wrap justify-end gap-2">
          <Button onClick={onCancel}>Cancel</Button>
          <Button
            disabled={!llmReady || !draft.prompt.trim() || fixingPrompt || savePending}
            onClick={onFixPrompt}
          >
            {fixingPrompt ? "Fixing..." : "Fix Up Prompt"}
          </Button>
          <Button
            disabled={!draft.prompt.trim() || savePending || fixingPrompt}
            variant="primary"
            onClick={onSave}
          >
            {savePending ? "Saving..." : "Save"}
          </Button>
        </div>
      </div>
    </div>
  );
}

function ColumnRunScopeModal({
  draft,
  selectedCount,
  startPending,
  onCancel,
  onChangeScope,
  onConfirm,
}: {
  draft: ColumnRunScopeDraftState;
  selectedCount: number;
  startPending: boolean;
  onCancel: () => void;
  onChangeScope: (scope: ColumnRunScope) => void;
  onConfirm: () => void;
}) {
  const selectedDisabled = selectedCount === 0;
  return (
    <div className="fixed inset-0 z-40 flex items-center justify-center bg-surface/80 p-4 backdrop-blur-sm">
      <div className="w-full max-w-xl rounded-xl border border-outline-variant/40 bg-surface-container p-5 shadow-2xl">
        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="text-title-sm font-semibold">Run Column</div>
            <div className="mt-1 text-body-md text-on-surface-variant">{draft.label}</div>
          </div>
          <button
            className="rounded-sm px-2 py-1 text-label-sm text-on-surface-variant hover:text-on-surface"
            disabled={startPending}
            onClick={onCancel}
            type="button"
          >
            Close
          </button>
        </div>

        <div className="mt-4 grid gap-2">
          <button
            className={columnRunScopeOptionClass(draft.scope === "all", false)}
            disabled={startPending}
            onClick={() => onChangeScope("all")}
            type="button"
          >
            <div className="font-semibold text-on-surface">Whole Dataset</div>
            <div className="mt-1 text-body-md text-on-surface-variant">
              Run this column across every repository row.
            </div>
          </button>
          <button
            className={columnRunScopeOptionClass(draft.scope === "empty_only", false)}
            disabled={startPending}
            onClick={() => onChangeScope("empty_only")}
            type="button"
          >
            <div className="font-semibold text-on-surface">Blank Rows Only</div>
            <div className="mt-1 text-body-md text-on-surface-variant">
              Fill only rows where this column is currently empty.
            </div>
          </button>
          <button
            className={columnRunScopeOptionClass(draft.scope === "selected", selectedDisabled)}
            disabled={startPending || selectedDisabled}
            onClick={() => onChangeScope("selected")}
            type="button"
          >
            <div className="font-semibold text-on-surface">Selected Rows</div>
            <div className="mt-1 text-body-md text-on-surface-variant">
              {selectedDisabled
                ? "Select one or more rows in the table to use this scope."
                : `Run only the ${selectedCount} selected row${selectedCount === 1 ? "" : "s"}.`}
            </div>
          </button>
        </div>

        <div className="mt-5 flex justify-end gap-2">
          <Button disabled={startPending} onClick={onCancel}>
            Cancel
          </Button>
          <Button disabled={startPending} variant="primary" onClick={onConfirm}>
            {startPending ? "Starting..." : "Start Run"}
          </Button>
        </div>
      </div>
    </div>
  );
}

export function RepositoryBrowserPage() {
  const queryClient = useQueryClient();
  const {
    getRepositoryManifest,
    loadProfiles,
    processingRunning,
    refreshDashboard,
    repositoryStatus,
    saveRepoSettings,
    settingsDraft,
    setSettingsDraft,
    sourceRunning,
    sourceTaskDraft,
    setSourceTaskDraft,
    trackSourceTaskJob,
    profiles,
  } = useAppState();

  const addLinksRef = useRef<HTMLInputElement | null>(null);
  const addFilesRef = useRef<HTMLInputElement | null>(null);
  const headerCheckboxRef = useRef<HTMLInputElement | null>(null);
  const renameInputRef = useRef<HTMLInputElement | null>(null);
  const resizeRef = useRef<{
    columnKey: string;
    startWidth: number;
    startX: number;
  } | null>(null);

  const [filters, setFilters] = useState<RepositoryBrowserFilters>(DEFAULT_FILTERS);
  const [visibleColumns, setVisibleColumns] = useState<string[]>(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [lastAnchorId, setLastAnchorId] = useState<string | null>(null);
  const [showColumnChooser, setShowColumnChooser] = useState(false);
  const [showFilters, setShowFilters] = useState(false);
  const [browserTaskScope, setBrowserTaskScope] = useState<BrowserTaskScope>("empty_only");
  const [actionMessage, setActionMessage] = useState("");
  const [actionError, setActionError] = useState("");
  const [linksPending, setLinksPending] = useState(false);
  const [filesPending, setFilesPending] = useState(false);
  const [downloadAllPending, setDownloadAllPending] = useState(false);
  const [downloadAllWithCleanup, setDownloadAllWithCleanup] = useState(false);
  const [runPending, setRunPending] = useState(false);
  const [deletePending, setDeletePending] = useState(false);
  const [risExportPending, setRisExportPending] = useState(false);
  const [activeRowId, setActiveRowId] = useState<string | null>(null);
  const [detailDraft, setDetailDraft] = useState<SourceDetailsDraft | null>(null);
  const [detailSaveState, setDetailSaveState] = useState<"idle" | "saving" | "saved" | "error">("idle");
  const [detailSaveError, setDetailSaveError] = useState("");
  const [storageLoaded, setStorageLoaded] = useState(false);
  const [columnPromptDraft, setColumnPromptDraft] = useState<ColumnPromptDraftState | null>(null);
  const [columnPromptFixing, setColumnPromptFixing] = useState(false);
  const [columnPromptSaving, setColumnPromptSaving] = useState(false);
  const [columnPromptError, setColumnPromptError] = useState("");
  const [renamingColumnId, setRenamingColumnId] = useState<string | null>(null);
  const [renamingColumnLabel, setRenamingColumnLabel] = useState("");
  const [columnCreatePending, setColumnCreatePending] = useState(false);
  const [columnRenamePending, setColumnRenamePending] = useState(false);
  const [columnRunScopeDraft, setColumnRunScopeDraft] = useState<ColumnRunScopeDraftState | null>(null);
  const [columnRunStarting, setColumnRunStarting] = useState(false);
  const [columnRunJobId, setColumnRunJobId] = useState("");

  const storageKey = useMemo(
    () => buildRepositoryBrowserStorageKey(repositoryStatus?.path || ""),
    [repositoryStatus?.path],
  );

  useEffect(() => {
    setStorageLoaded(false);
    try {
      const raw = window.localStorage.getItem(storageKey);
      if (!raw) {
        setVisibleColumns(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS);
        setColumnWidths({});
        setStorageLoaded(true);
        return;
      }
      const parsed = JSON.parse(raw) as RepositoryBrowserStoredState;
      if (Array.isArray(parsed.visibleColumns) && parsed.visibleColumns.length > 0) {
        setVisibleColumns(migrateRepositoryBrowserVisibleColumns(parsed.visibleColumns));
      } else {
        setVisibleColumns(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS);
      }
      if (parsed.columnWidths && typeof parsed.columnWidths === "object") {
        const nextWidths = Object.fromEntries(
          Object.entries(parsed.columnWidths).map(([key, value]) => [
            key,
            clampRepositoryBrowserColumnWidth(Number(value)),
          ]),
        );
        setColumnWidths(nextWidths);
      } else {
        setColumnWidths({});
      }
      setStorageLoaded(true);
    } catch {
      setVisibleColumns(REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS);
      setColumnWidths({});
      setStorageLoaded(true);
    }
  }, [storageKey]);

  useEffect(() => {
    if (!storageLoaded) return;
    try {
      window.localStorage.setItem(
        storageKey,
        JSON.stringify({
          visibleColumns,
          columnWidths,
        } satisfies RepositoryBrowserStoredState),
      );
    } catch {
      // Ignore localStorage failures.
    }
  }, [columnWidths, storageKey, storageLoaded, visibleColumns]);

  const queryParams = useMemo(() => buildRepositoryBrowserQuery(filters), [filters]);
  const queryString = queryParams.toString();
  const manifestQueryKey = useMemo(
    () => ["repository-browser-manifest", repositoryStatus?.path || "", queryString],
    [queryString, repositoryStatus?.path],
  );

  const manifestQuery = useQuery({
    queryKey: manifestQueryKey,
    queryFn: () => getRepositoryManifest(queryParams),
    staleTime: 0,
    placeholderData: (previous) => previous,
    refetchInterval:
      sourceRunning ||
      processingRunning ||
      Boolean(columnRunJobId) ||
      repositoryStatus?.download_state === "running" ||
      repositoryStatus?.download_state === "cancelling"
        ? 1500
        : false,
  });
  const columnRunQuery = useQuery({
    queryKey: ["repository-browser-column-run", repositoryStatus?.path || "", columnRunJobId],
    queryFn: () => api.getRepositoryColumnRunStatus(columnRunJobId),
    enabled: Boolean(columnRunJobId),
    refetchInterval: (query) => {
      const state = (query.state.data as RepositoryColumnRunStatus | undefined)?.state;
      return state === "pending" || state === "running" ? 1000 : false;
    },
    retry: false,
  });

  const rows = manifestQuery.data?.rows || [];
  const totalRows = manifestQuery.data?.total || 0;
  const llmReady = Boolean(
    settingsDraft.use_llm &&
      settingsDraft.llm_backend.base_url.trim() &&
      settingsDraft.llm_backend.model.trim(),
  );
  const activeColumnRun =
    columnRunQuery.data &&
    (columnRunQuery.data.state === "pending" || columnRunQuery.data.state === "running")
      ? columnRunQuery.data
      : null;
  const allColumns = useMemo(
    () => mergeRepositoryBrowserColumns(manifestQuery.data?.columns || []),
    [manifestQuery.data?.columns],
  );
  const columnById = useMemo(() => {
    const entries = new Map<string, RepositoryManifestColumn>();
    allColumns.forEach((column) => entries.set(column.key, column));
    return entries;
  }, [allColumns]);
  const renderedColumns = useMemo(
    () =>
      visibleColumns
        .map((columnId) => columnById.get(columnId))
        .filter(Boolean) as RepositoryManifestColumn[],
    [columnById, visibleColumns],
  );
  const tableWidth = useMemo(
    () =>
      REPOSITORY_BROWSER_SELECTION_COLUMN_WIDTH +
      renderedColumns.reduce(
        (total, column) => total + resolveRepositoryBrowserColumnWidth(columnWidths, column.key),
        0,
      ),
    [columnWidths, renderedColumns],
  );
  const actionRowWidth = tableWidth + REPOSITORY_BROWSER_ACTION_RAIL_WIDTH;
  const columnVisibilityCategories = useMemo(() => {
    const configuredKeys = new Set(
      REPOSITORY_BROWSER_COLUMN_CATEGORIES.flatMap((category) => category.columnKeys),
    );
    const otherKeys = allColumns
      .map((column) => column.key)
      .filter((columnKey) => !configuredKeys.has(columnKey));
    if (otherKeys.length === 0) return REPOSITORY_BROWSER_COLUMN_CATEGORIES;
    return [
      ...REPOSITORY_BROWSER_COLUMN_CATEGORIES,
      {
        id: "other",
        label: "Other",
        columnKeys: otherKeys,
      },
    ];
  }, [allColumns]);

  useEffect(() => {
    if (!manifestQuery.data?.columns?.length) return;
    const available = new Set(allColumns.map((column) => column.key));
    setVisibleColumns((prev) => {
      const filtered = prev.filter((column) => available.has(column));
      if (filtered.length > 0) return filtered;
      return REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS.filter((column) => available.has(column));
    });
  }, [allColumns, manifestQuery.data?.columns]);

  useEffect(() => {
    setSelectedIds(new Set());
    setLastAnchorId(null);
    setActiveRowId(null);
    setDetailDraft(null);
    setActionMessage("");
    setActionError("");
    setColumnPromptDraft(null);
    setColumnPromptError("");
    setRenamingColumnId(null);
    setRenamingColumnLabel("");
    setColumnRunScopeDraft(null);
    setColumnRunStarting(false);
    setColumnRunJobId("");
  }, [repositoryStatus?.path]);

  useEffect(() => {
    const total = manifestQuery.data?.total || 0;
    if (total === 0 && filters.offset !== 0) {
      setFilters((prev) => ({ ...prev, offset: 0 }));
      return;
    }
    if (total > 0 && filters.offset >= total) {
      const maxOffset = Math.floor((total - 1) / REPOSITORY_BROWSER_PAGE_SIZE) * REPOSITORY_BROWSER_PAGE_SIZE;
      if (maxOffset !== filters.offset) {
        setFilters((prev) => ({ ...prev, offset: maxOffset }));
      }
    }
  }, [filters.offset, manifestQuery.data?.total]);

  const visibleIds = useMemo(() => rows.map((row) => row.id), [rows]);
  const selectedVisibleCount = useMemo(
    () => visibleIds.filter((id) => selectedIds.has(id)).length,
    [selectedIds, visibleIds],
  );
  const allVisibleSelected = rows.length > 0 && selectedVisibleCount === rows.length;
  const someVisibleSelected = selectedVisibleCount > 0 && selectedVisibleCount < rows.length;

  useEffect(() => {
    if (!headerCheckboxRef.current) return;
    headerCheckboxRef.current.indeterminate = someVisibleSelected;
  }, [someVisibleSelected]);

  const activeRow = useMemo(
    () => rows.find((row) => row.id === activeRowId) || null,
    [activeRowId, rows],
  );

  useEffect(() => {
    if (!activeRowId) {
      setDetailDraft(null);
      setDetailSaveState("idle");
      setDetailSaveError("");
    } else {
      setDetailDraft(null);
      setDetailSaveState("idle");
      setDetailSaveError("");
    }
  }, [activeRowId]);

  useEffect(() => {
    if (!activeRow || detailDraft) return;
    setDetailDraft(createSourceDetailsDraft(activeRow));
  }, [activeRow, detailDraft]);

  useEffect(() => {
    if (!activeRowId) return;
    if (rows.some((row) => row.id === activeRowId)) return;
    setActiveRowId(null);
  }, [activeRowId, rows]);

  const detailPatch = useMemo(
    () => buildSourcePatch(activeRow, detailDraft),
    [activeRow, detailDraft],
  );

  useEffect(() => {
    if (!activeRow || !detailPatch) return;
    setDetailSaveState("saving");
    setDetailSaveError("");
    const timer = window.setTimeout(async () => {
      try {
        const updatedRow = await api.patchRepositorySource(activeRow.id, detailPatch);
        queryClient.setQueryData(
          manifestQueryKey,
          (previous: typeof manifestQuery.data) =>
            previous
              ? {
                  ...previous,
                  rows: previous.rows.map((row) => (row.id === updatedRow.id ? updatedRow : row)),
                }
              : previous,
        );
        setDetailDraft(createSourceDetailsDraft(updatedRow));
        setDetailSaveState("saved");
        setDetailSaveError("");
        void refreshDashboard();
      } catch (error) {
        setDetailSaveState("error");
        setDetailSaveError(String((error as Error).message || "Failed to save source details"));
      }
    }, 700);
    return () => window.clearTimeout(timer);
  }, [activeRow, detailPatch, manifestQueryKey, queryClient, refreshDashboard]);

  useEffect(() => {
    if (!renamingColumnId) return;
    renameInputRef.current?.focus();
    renameInputRef.current?.select();
    document
      .getElementById(`repository-column-control-${renamingColumnId}`)
      ?.scrollIntoView({ behavior: "smooth", block: "nearest", inline: "center" });
  }, [renamingColumnId, renderedColumns]);

  useEffect(() => {
    if (!columnRunQuery.data) return;
    if (columnRunQuery.data.state === "pending" || columnRunQuery.data.state === "running") {
      return;
    }
    if (columnRunQuery.data.state === "failed") {
      setActionError(columnRunQuery.data.message || "Column run failed.");
      setActionMessage("");
    } else {
      setActionMessage(columnRunQuery.data.message || "Column run completed.");
      setActionError("");
    }
    setColumnRunJobId("");
    void Promise.all([manifestQuery.refetch(), refreshDashboard()]);
  }, [columnRunQuery.data, manifestQuery, refreshDashboard]);

  useEffect(() => {
    const handleMouseMove = (event: MouseEvent) => {
      const activeResize = resizeRef.current;
      if (!activeResize) return;
      const delta = event.clientX - activeResize.startX;
      setColumnWidths((prev) => ({
        ...prev,
        [activeResize.columnKey]: clampRepositoryBrowserColumnWidth(activeResize.startWidth + delta),
      }));
    };

    const handleMouseUp = () => {
      if (!resizeRef.current) return;
      resizeRef.current = null;
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };

    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, []);

  const patchFilters = (patch: Partial<RepositoryBrowserFilters>, resetOffset = true) => {
    setFilters((prev) => ({
      ...prev,
      ...patch,
      offset: resetOffset ? 0 : patch.offset ?? prev.offset,
      limit: REPOSITORY_BROWSER_PAGE_SIZE,
    }));
  };

  const toggleVisibleColumn = (columnId: string) => {
    setVisibleColumns((prev) => {
      if (prev.includes(columnId)) {
        if (prev.length === 1) return prev;
        return prev.filter((item) => item !== columnId);
      }
      return [...prev, columnId];
    });
  };

  const resetHiddenFilters = () => {
    setFilters((prev) => ({
      ...DEFAULT_FILTERS,
      q: prev.q,
      sortBy: prev.sortBy,
      sortDir: prev.sortDir,
      limit: REPOSITORY_BROWSER_PAGE_SIZE,
      offset: 0,
    }));
  };

  const toggleCategoryColumns = (columnKeys: string[], checked: boolean) => {
    const keys = columnKeys.filter((columnKey) =>
      allColumns.some((column) => column.key === columnKey),
    );
    if (keys.length === 0) return;
    setVisibleColumns((prev) => {
      const next = new Set(prev);
      keys.forEach((key) => {
        if (checked) {
          next.add(key);
          return;
        }
        if (next.size > 1 || !next.has(key)) {
          next.delete(key);
        }
      });
      if (next.size === 0) return prev;
      return allColumns.map((column) => column.key).filter((columnKey) => next.has(columnKey));
    });
  };

  const toggleSelectAllVisible = (checked: boolean) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      visibleIds.forEach((id) => {
        if (checked) {
          next.add(id);
        } else {
          next.delete(id);
        }
      });
      return next;
    });
    setLastAnchorId(visibleIds[0] || null);
  };

  const handleRowCheckbox = (event: ChangeEvent<HTMLInputElement>, rowId: string) => {
    const nativeEvent = event.nativeEvent as MouseEvent | Event;
    const result = toggleRepositoryBrowserSelection({
      orderedIds: visibleIds,
      currentSelectedIds: selectedIds,
      targetId: rowId,
      checked: event.target.checked,
      lastAnchorId,
      shiftKey: "shiftKey" in nativeEvent ? Boolean(nativeEvent.shiftKey) : false,
    });
    setSelectedIds(result.selectedIds);
    setLastAnchorId(result.anchorId);
  };

  const handleSort = (column: RepositoryManifestColumn) => {
    if (!column.sortable) return;
    patchFilters(nextRepositoryBrowserSort(filters.sortBy, filters.sortDir, column.key), true);
  };

  const openColumnPromptModal = (column: RepositoryManifestColumn) => {
    setColumnPromptDraft({
      columnId: column.key,
      label: labelRepositoryBrowserColumn(column.key, column.label),
      prompt: column.instruction_prompt || "",
      outputConstraint: column.output_constraint,
    });
    setColumnPromptError("");
  };

  const handleSaveColumnPrompt = async () => {
    if (!columnPromptDraft) return;
    setColumnPromptSaving(true);
    setColumnPromptError("");
    try {
      await api.updateRepositoryColumn(columnPromptDraft.columnId, {
        instruction_prompt: columnPromptDraft.prompt,
        output_constraint: columnPromptDraft.outputConstraint,
      });
      setColumnPromptDraft(null);
      setActionMessage(`Saved instructions for ${columnPromptDraft.label}.`);
      setActionError("");
      void manifestQuery.refetch();
    } catch (error) {
      setColumnPromptError(
        String((error as Error).message || "Failed to save column instructions"),
      );
    } finally {
      setColumnPromptSaving(false);
    }
  };

  const handleFixColumnPrompt = async () => {
    if (!columnPromptDraft) return;
    setColumnPromptFixing(true);
    setColumnPromptError("");
    try {
      const response = await api.fixRepositoryColumnPrompt(
        columnPromptDraft.columnId,
        columnPromptDraft.prompt,
      );
      setColumnPromptDraft((prev) =>
        prev
          ? {
              ...prev,
              prompt: response.prompt,
              outputConstraint: response.output_constraint,
            }
          : prev,
      );
    } catch (error) {
      setColumnPromptError(
        String((error as Error).message || "Failed to improve the prompt"),
      );
    } finally {
      setColumnPromptFixing(false);
    }
  };

  const handleCreateColumn = async () => {
    if (columnCreatePending) return;
    setColumnCreatePending(true);
    setActionMessage("");
    setActionError("");
    try {
      const created = await api.createRepositoryColumn("New Column");
      setVisibleColumns((prev) => (prev.includes(created.id) ? prev : [...prev, created.id]));
      setRenamingColumnId(created.id);
      setRenamingColumnLabel(created.label);
      await manifestQuery.refetch();
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to create custom column"));
    } finally {
      setColumnCreatePending(false);
    }
  };

  const beginColumnRename = (column: RepositoryManifestColumn) => {
    if (!column.renamable) return;
    setRenamingColumnId(column.key);
    setRenamingColumnLabel(column.label);
    setActionError("");
  };

  const commitColumnRename = async () => {
    if (!renamingColumnId) return;
    setColumnRenamePending(true);
    setActionError("");
    try {
      await api.updateRepositoryColumn(renamingColumnId, {
        label: renamingColumnLabel,
      });
      setRenamingColumnId(null);
      setRenamingColumnLabel("");
      void manifestQuery.refetch();
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to rename column"));
    } finally {
      setColumnRenamePending(false);
    }
  };

  const cancelColumnRename = () => {
    setRenamingColumnId(null);
    setRenamingColumnLabel("");
  };

  const handleRenameKeyDown = (event: ReactKeyboardEvent<HTMLInputElement>) => {
    if (event.key === "Enter") {
      event.preventDefault();
      void commitColumnRename();
      return;
    }
    if (event.key === "Escape") {
      event.preventDefault();
      cancelColumnRename();
    }
  };

  const openColumnRunScopeModal = (column: RepositoryManifestColumn) => {
    if (!column.processable || activeColumnRun) return;
    setColumnRunScopeDraft({
      columnId: column.key,
      label: labelRepositoryBrowserColumn(column.key, column.label),
      scope: selectedIds.size > 0 ? "selected" : "empty_only",
    });
    setActionError("");
  };

  const handleRunColumn = async (
    draft: ColumnRunScopeDraftState,
    confirmOverwrite = false,
  ) => {
    if (activeColumnRun) return;
    const column = columnById.get(draft.columnId);
    if (!column || !column.processable) return;
    setActionMessage("");
    setActionError("");
    setColumnRunStarting(true);
    try {
      const response = await api.startRepositoryColumnRun(column.key, {
        filters: buildRepositoryManifestFilterPayload(filters),
        scope: draft.scope === "all" ? "all" : draft.scope,
        source_ids: draft.scope === "selected" ? Array.from(selectedIds) : [],
        confirm_overwrite: confirmOverwrite,
      });
      if (response.status === "confirmation_required") {
        const confirmed = window.confirm(
          response.message || `Overwrite ${response.populated_rows} populated cell(s)?`,
        );
        if (confirmed) {
          await handleRunColumn(draft, true);
        }
        return;
      }
      setColumnRunJobId(response.job_id);
      setColumnRunScopeDraft(null);
      setActionMessage(response.message || `Started ${column.label} column run.`);
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to start column run"));
    } finally {
      setColumnRunStarting(false);
    }
  };

  const beginColumnResize = (
    event: ReactMouseEvent<HTMLButtonElement>,
    columnKey: string,
  ) => {
    event.preventDefault();
    event.stopPropagation();
    const headerCell = event.currentTarget.parentElement;
    const fallbackWidth = headerCell?.getBoundingClientRect().width || 180;
    resizeRef.current = {
      columnKey,
      startX: event.clientX,
      startWidth: columnWidths[columnKey] ?? fallbackWidth,
    };
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  };

  const saveActiveProfile = async (filename: string) => {
    const nextSettings = {
      ...settingsDraft,
      default_project_profile_name: filename,
    };
    setSettingsDraft(nextSettings);
    setActionMessage("");
    setActionError("");
    try {
      await saveRepoSettings(nextSettings);
      setActionMessage(`Active project profile set to ${filename || "default_project_profile.yaml"}.`);
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to save active project profile"));
    }
  };

  const handleAddLinks = async (files: File[]) => {
    if (files.length === 0) return;
    setLinksPending(true);
    setActionMessage("");
    setActionError("");
    try {
      const response = await api.ingestRepositorySeedFiles(files);
      await refreshDashboard();
      setSourceTaskDraft((prev) => ({
        ...prev,
        scope: "import",
        import_id: response.import_id,
        source_ids: [],
        run_download: true,
        run_convert: true,
        run_catalog: false,
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
        include_raw_file: true,
        include_rendered_html: true,
        include_rendered_pdf: true,
        include_markdown: true,
      }));
      const startResponse = await api.startRepositorySourceTasks({
        ...sourceTaskDraft,
        scope: "import",
        import_id: response.import_id,
        source_ids: [],
        rerun_failed_only: false,
        run_download: true,
        run_convert: true,
        run_catalog: false,
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
        include_raw_file: true,
        include_rendered_html: true,
        include_rendered_pdf: true,
        include_markdown: true,
        project_profile_name: settingsDraft.default_project_profile_name,
      });
      setActionMessage(
        startResponse.message ||
          `Imported ${response.accepted_new} new link source(s) and started deterministic fetch/convert for that batch.`,
      );
      trackSourceTaskJob(startResponse.job_id || null, startResponse.message || "");
      void manifestQuery.refetch();
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to import links"));
    } finally {
      setLinksPending(false);
    }
  };

  const handleAddFiles = async (files: File[]) => {
    if (files.length === 0) return;
    setFilesPending(true);
    setActionMessage("");
    setActionError("");
    try {
      const response = await api.ingestRepositoryDocuments(files);
      await refreshDashboard();
      setActionMessage(
        response.message ||
          `Imported ${response.accepted_new} file(s) into the repository. No AI enrichment was started.`,
      );
      void manifestQuery.refetch();
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to import files"));
    } finally {
      setFilesPending(false);
    }
  };

  const handleDownloadAllSources = async () => {
    if (downloadAllPending) return;
    if (downloadAllWithCleanup && !llmReady) {
      setActionError("Configure and enable the repository LLM backend before using auto cleanup.");
      setActionMessage("");
      return;
    }

    setDownloadAllPending(true);
    setActionMessage("");
    setActionError("");
    try {
      const response = await api.startRepositorySourceTasks({
        ...sourceTaskDraft,
        scope: "all",
        import_id: "",
        source_ids: [],
        rerun_failed_only: false,
        run_download: true,
        run_convert: true,
        run_catalog: false,
        run_citation_verify: false,
        run_llm_cleanup: downloadAllWithCleanup,
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
        include_raw_file: true,
        include_rendered_html: true,
        include_rendered_pdf: true,
        include_markdown: true,
        project_profile_name: settingsDraft.default_project_profile_name,
      });
      setActionMessage(
        response.message ||
          `Started download and conversion for ${response.total_urls} repository source(s).`,
      );
      trackSourceTaskJob(response.job_id || null, response.message || "");
      void refreshDashboard();
      void manifestQuery.refetch();
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to start repository download"));
    } finally {
      setDownloadAllPending(false);
    }
  };

  const handleRunTasks = async () => {
    const selectedTaskIds = Array.from(selectedIds);
    const scope = browserTaskScope === "selected" ? "all" : browserTaskScope;
    const sourceIds = browserTaskScope === "selected" ? selectedTaskIds : [];
    if (
      !sourceTaskDraft.run_llm_cleanup &&
      !sourceTaskDraft.run_catalog &&
      !sourceTaskDraft.run_citation_verify &&
      !sourceTaskDraft.run_llm_title &&
      !sourceTaskDraft.run_llm_summary &&
      !sourceTaskDraft.run_llm_rating
    ) {
      setActionError("Select at least one AI enrichment task.");
      return;
    }
    if (browserTaskScope === "selected" && sourceIds.length === 0) {
      setActionError("Select one or more rows before running enrichment on manually selected rows.");
      return;
    }

    setRunPending(true);
    setActionMessage("");
    setActionError("");
    try {
      const response = await api.startRepositorySourceTasks({
        ...sourceTaskDraft,
        scope,
        import_id: "",
        source_ids: sourceIds,
        rerun_failed_only: false,
        run_download: false,
        run_convert: false,
        run_catalog: Boolean(sourceTaskDraft.run_catalog),
        run_citation_verify: Boolean(sourceTaskDraft.run_citation_verify),
        run_llm_cleanup: Boolean(sourceTaskDraft.run_llm_cleanup),
        run_llm_title: Boolean(sourceTaskDraft.run_llm_title),
        run_llm_summary: Boolean(sourceTaskDraft.run_llm_summary),
        run_llm_rating: Boolean(sourceTaskDraft.run_llm_rating),
        force_redownload: false,
        force_convert: false,
        force_catalog: false,
        force_citation_verify: false,
        force_llm_cleanup: false,
        force_title: false,
        force_summary: false,
        force_rating: false,
        include_raw_file: true,
        include_rendered_html: true,
        include_rendered_pdf: true,
        include_markdown: true,
        project_profile_name: settingsDraft.default_project_profile_name,
      });
      setActionMessage(response.message || "Repository enrichment started.");
      trackSourceTaskJob(response.job_id || null, response.message || "");
      void refreshDashboard();
      void manifestQuery.refetch();
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to start repository enrichment"));
    } finally {
      setRunPending(false);
    }
  };

  const handleDeleteSelected = async () => {
    const ids = Array.from(selectedIds);
    if (ids.length === 0 || deletePending) return;
    const confirmed = window.confirm(
      `Delete ${ids.length} selected source(s) and their linked repository files?`,
    );
    if (!confirmed) return;

    setDeletePending(true);
    setActionMessage("");
    setActionError("");
    try {
      const response = await api.deleteRepositorySources(ids);
      setSelectedIds(new Set());
      setLastAnchorId(null);
      setActiveRowId((current) => (current && ids.includes(current) ? null : current));
      await refreshDashboard();
      await manifestQuery.refetch();
      setActionMessage(response.message || "Selected sources deleted.");
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to delete selected sources"));
    } finally {
      setDeletePending(false);
    }
  };

  const handleExportRis = async (sourceIds: string[], label: string) => {
    if (risExportPending) return;
    if (sourceIds.length === 0) {
      setActionError(`No ${label.toLowerCase()} available for RIS export.`);
      return;
    }

    setRisExportPending(true);
    setActionMessage("");
    setActionError("");
    try {
      const result = await api.exportRepositoryCitationRis({
        scope: "selected",
        source_ids: sourceIds,
        filters: {
          q: "",
          fetch_status: "",
          detected_type: "",
          source_kind: "",
          document_type: "",
          organization_type: "",
          organization_name: "",
          author_names: "",
          publication_date: "",
          tags_text: "",
          has_summary: null,
          has_rating: null,
          rating_overall_min: null,
          rating_overall_max: null,
          rating_overall_relevance_min: null,
          rating_overall_relevance_max: null,
          rating_depth_score_min: null,
          rating_depth_score_max: null,
          rating_relevant_detail_score_min: null,
          rating_relevant_detail_score_max: null,
          citation_type: "",
          citation_doi: "",
          citation_report_number: "",
          citation_standard_number: "",
          citation_missing_fields: "",
          citation_ready: null,
          citation_confidence_min: null,
          citation_confidence_max: null,
        },
      });
      downloadBlob(result.blob, result.filename);
      setActionMessage(
        `Downloaded ${result.exportedCount} RIS citation record(s) for ${label.toLowerCase()}` +
          (result.skippedCount ? ` (${result.skippedCount} incomplete source(s) skipped).` : "."),
      );
    } catch (error) {
      setActionError(String((error as Error).message || "Failed to export RIS citations"));
    } finally {
      setRisExportPending(false);
    }
  };

  const availableProfiles =
    profiles.length > 0
      ? profiles
      : [{ filename: "default_project_profile.yaml", name: "default_project_profile" }];

  return (
    <div className="flex h-full min-h-0 flex-col gap-4 overflow-hidden">
      <SectionHeader
        title="Browser"
        description="Ingest sources, run targeted repository enrichment, and browse repository documents in a live spreadsheet view."
      />

      {(actionMessage || actionError) && (
        <SurfaceCard className={actionError ? "border border-error/30 bg-error/10" : ""}>
          <div className={actionError ? "text-body-md text-error" : "text-body-md text-on-surface"}>
            {actionError || actionMessage}
          </div>
        </SurfaceCard>
      )}

      <SurfaceCard className="thin-scrollbar shrink-0 overflow-y-auto max-h-[34vh]">
        <div className="grid gap-4 xl:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)_minmax(0,0.85fr)]">
          <div className="space-y-3">
            <div className="text-title-sm font-semibold">Ingestion And Context</div>
            <div className="flex flex-wrap gap-2">
              <Button
                variant="primary"
                disabled={linksPending}
                onClick={() => addLinksRef.current?.click()}
              >
                {linksPending ? "Importing Links..." : "Add Links"}
              </Button>
              <Button disabled={filesPending} onClick={() => addFilesRef.current?.click()}>
                {filesPending ? "Importing Files..." : "Add Files"}
              </Button>
              <Button
                disabled={
                  downloadAllPending ||
                  sourceRunning ||
                  !(Number(repositoryStatus?.total_sources || 0) > 0)
                }
                onClick={() => void handleDownloadAllSources()}
              >
                {downloadAllPending ? "Starting Download..." : "Download All Sources"}
              </Button>
            </div>
            <label className="flex items-center gap-2 text-body-md text-on-surface-variant">
              <input
                checked={downloadAllWithCleanup}
                disabled={downloadAllPending}
                type="checkbox"
                onChange={(event) => setDownloadAllWithCleanup(event.target.checked)}
              />
              Auto clean markdown with LLM after download
            </label>
            {downloadAllWithCleanup && !llmReady && (
              <div className="rounded-md bg-warning/10 px-3 py-2 text-body-md text-warning">
                LLM cleanup requires an enabled repository LLM backend and selected model.
              </div>
            )}
            <SelectField
              label="Active Project Profile"
              value={settingsDraft.default_project_profile_name}
              onChange={(event) => void saveActiveProfile(event.target.value)}
            >
              <option value="">Default bundled profile</option>
              {availableProfiles.map((profile) => (
                <option key={profile.filename} value={profile.filename}>
                  {profile.name}
                </option>
              ))}
            </SelectField>
            <div className="text-body-md text-on-surface-variant">
              `Add Links` imports seed/link files, then starts deterministic fetch and conversion only.
              `Add Files` imports local documents into the repository without triggering AI enrichment.
              `Download All Sources` runs the existing repository download/conversion pipeline across the full repository.
            </div>
            <input
              ref={addLinksRef}
              className="hidden"
              accept=".csv,.xlsx,.md,.pdf,.docx"
              multiple
              type="file"
              onChange={(event) => {
                const files = Array.from(event.target.files || []);
                void handleAddLinks(files);
                event.currentTarget.value = "";
              }}
            />
            <input
              ref={addFilesRef}
              className="hidden"
              accept=".pdf,.doc,.docx,.html,.htm,.md,.rtf,.txt"
              multiple
              type="file"
              onChange={(event) => {
                const files = Array.from(event.target.files || []);
                void handleAddFiles(files);
                event.currentTarget.value = "";
              }}
            />
          </div>

          <div className="space-y-3">
            <div className="text-title-sm font-semibold">AI Enrichment Panel</div>
            <div className="grid gap-2 md:grid-cols-2">
              <label className="flex items-center gap-2 text-body-md">
                <input
                  checked={sourceTaskDraft.run_llm_cleanup}
                  type="checkbox"
                  onChange={(event) =>
                    setSourceTaskDraft((prev) => ({
                      ...prev,
                      run_llm_cleanup: event.target.checked,
                    }))
                  }
                />
                LLM markdown cleanup
              </label>
              <label className="flex items-center gap-2 text-body-md">
                <input
                  checked={sourceTaskDraft.run_catalog}
                  type="checkbox"
                  onChange={(event) =>
                    setSourceTaskDraft((prev) => ({
                      ...prev,
                      run_catalog: event.target.checked,
                    }))
                  }
                />
                Catalog metadata (display fields)
              </label>
              <label className="flex items-center gap-2 text-body-md">
                <input
                  checked={sourceTaskDraft.run_llm_title}
                  type="checkbox"
                  onChange={(event) =>
                    setSourceTaskDraft((prev) => ({
                      ...prev,
                      run_llm_title: event.target.checked,
                    }))
                  }
                />
                Title resolution
              </label>
              <label className="flex items-center gap-2 text-body-md">
                <input
                  checked={sourceTaskDraft.run_citation_verify}
                  type="checkbox"
                  onChange={(event) =>
                    setSourceTaskDraft((prev) => ({
                      ...prev,
                      run_citation_verify: event.target.checked,
                    }))
                  }
                />
                Citation verification
              </label>
              <label className="flex items-center gap-2 text-body-md">
                <input
                  checked={sourceTaskDraft.run_llm_summary}
                  type="checkbox"
                  onChange={(event) =>
                    setSourceTaskDraft((prev) => ({
                      ...prev,
                      run_llm_summary: event.target.checked,
                    }))
                  }
                />
                LLM summaries
              </label>
              <label className="flex items-center gap-2 text-body-md">
                <input
                  checked={sourceTaskDraft.run_llm_rating}
                  type="checkbox"
                  onChange={(event) =>
                    setSourceTaskDraft((prev) => ({
                      ...prev,
                      run_llm_rating: event.target.checked,
                    }))
                  }
                />
                Rating sources
              </label>
            </div>

            <SelectField
              label="Scope"
              value={browserTaskScope}
              onChange={(event) => setBrowserTaskScope(event.target.value as BrowserTaskScope)}
            >
              <option value="all">Entire repository</option>
              <option value="selected">Manually selected rows</option>
              <option value="empty_only">Empty spaces only</option>
            </SelectField>

            <div className="rounded-md bg-surface-container-low p-3 text-body-md text-on-surface-variant">
              Catalog metadata updates browsing fields. Citation verification separately builds the
              authoritative RIS-ready citation record, including verification status, evidence, and
              export blocking when required fields are not verified.
            </div>

            <div className="flex flex-wrap gap-2">
              <Button variant="primary" disabled={runPending} onClick={() => void handleRunTasks()}>
                {runPending ? "Starting..." : "Run Selected Tasks"}
              </Button>
              <Button onClick={() => void loadProfiles()}>Refresh Profiles</Button>
            </div>
          </div>

          <div className="space-y-3">
            <div className="text-title-sm font-semibold">View Customization</div>
            <div className="flex flex-wrap gap-2">
              <Button onClick={() => setShowColumnChooser((prev) => !prev)}>
                {showColumnChooser ? "Hide Columns" : "Column Visibility"}
              </Button>
              <Button onClick={() => setShowFilters((prev) => !prev)}>
                {showFilters ? "Hide Filters" : "Filters"}
              </Button>
              <Button
                variant="ghost"
                onClick={() => {
                  setVisibleColumns(
                    REPOSITORY_BROWSER_DEFAULT_VISIBLE_COLUMNS.filter((columnId) =>
                      allColumns.some((column) => column.key === columnId),
                    ),
                  );
                  setColumnWidths({});
                }}
              >
                Reset Columns
              </Button>
            </div>
            <div className="text-body-md text-on-surface-variant">
              Column visibility changes are stored per repository path. Column widths can be dragged from the table header.
            </div>
          </div>
        </div>

        {showColumnChooser && (
          <div className="mt-4 rounded-md border border-outline-variant/30 bg-surface-container-low p-4">
            <div className="space-y-4">
              {columnVisibilityCategories.map((category) => {
                const categoryColumns = category.columnKeys
                  .map((columnKey) => columnById.get(columnKey))
                  .filter(Boolean) as RepositoryManifestColumn[];
                if (categoryColumns.length === 0) return null;
                const checkedCount = categoryColumns.filter((column) =>
                  visibleColumns.includes(column.key),
                ).length;
                const allChecked = checkedCount === categoryColumns.length;
                const someChecked = checkedCount > 0 && checkedCount < categoryColumns.length;
                return (
                  <section key={category.id}>
                    <div className="mb-2 flex items-center justify-between gap-3">
                      <label className="flex items-center gap-2 text-title-sm font-semibold">
                        <input
                          checked={allChecked}
                          onChange={(event) =>
                            toggleCategoryColumns(category.columnKeys, event.target.checked)
                          }
                          ref={(element) => {
                            if (!element) return;
                            element.indeterminate = someChecked;
                          }}
                          type="checkbox"
                        />
                        {category.label}
                      </label>
                      <div className="text-label-sm text-on-surface-variant">
                        {checkedCount}/{categoryColumns.length} visible
                      </div>
                    </div>
                    <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
                      {categoryColumns.map((column) => (
                        <label key={column.key} className="flex items-center gap-2 text-body-md">
                          <input
                            checked={visibleColumns.includes(column.key)}
                            onChange={() => toggleVisibleColumn(column.key)}
                            type="checkbox"
                          />
                          {labelRepositoryBrowserColumn(column.key, column.label)}
                        </label>
                      ))}
                    </div>
                  </section>
                );
              })}
            </div>
          </div>
        )}

        {showFilters && (
          <div className="mt-4 rounded-md border border-outline-variant/30 bg-surface-container-low p-4">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
              <div className="text-title-sm font-semibold">Advanced Filters</div>
              <Button variant="ghost" onClick={resetHiddenFilters}>
                Reset Filters
              </Button>
            </div>
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              <InputField
                label="Fetch Status"
                value={filters.fetchStatus}
                onChange={(event) => patchFilters({ fetchStatus: event.target.value })}
              />
              <InputField
                label="Detected Type"
                value={filters.detectedType}
                onChange={(event) => patchFilters({ detectedType: event.target.value })}
              />
              <InputField
                label="Source Kind"
                value={filters.sourceKind}
                onChange={(event) => patchFilters({ sourceKind: event.target.value })}
              />
              <InputField
                label="Document Type"
                value={filters.documentType}
                onChange={(event) => patchFilters({ documentType: event.target.value })}
              />
              <InputField
                label="Organization Type"
                value={filters.organizationType}
                onChange={(event) => patchFilters({ organizationType: event.target.value })}
              />
              <InputField
                label="Organization"
                value={filters.organizationName}
                onChange={(event) => patchFilters({ organizationName: event.target.value })}
              />
              <InputField
                label="Authors"
                value={filters.authorNames}
                onChange={(event) => patchFilters({ authorNames: event.target.value })}
              />
              <InputField
                label="Publication Date"
                value={filters.publicationDate}
                onChange={(event) => patchFilters({ publicationDate: event.target.value })}
              />
              <InputField
                label="Tags"
                value={filters.tagsText}
                onChange={(event) => patchFilters({ tagsText: event.target.value })}
              />
              <SelectField
                label="Has Summary"
                value={filters.hasSummary}
                onChange={(event) => patchFilters({ hasSummary: event.target.value })}
              >
                <option value="">Any</option>
                <option value="true">Yes</option>
                <option value="false">No</option>
              </SelectField>
              <SelectField
                label="Has Rating"
                value={filters.hasRating}
                onChange={(event) => patchFilters({ hasRating: event.target.value })}
              >
                <option value="">Any</option>
                <option value="true">Yes</option>
                <option value="false">No</option>
              </SelectField>
              <SelectField
                label="RIS Ready"
                value={filters.citationReady}
                onChange={(event) => patchFilters({ citationReady: event.target.value })}
              >
                <option value="">Any</option>
                <option value="true">Yes</option>
                <option value="false">No</option>
              </SelectField>
            </div>

            <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
              <InputField
                label="Relevance Min"
                value={filters.ratingOverallRelevanceMin}
                onChange={(event) => patchFilters({ ratingOverallRelevanceMin: event.target.value })}
              />
              <InputField
                label="Relevance Max"
                value={filters.ratingOverallRelevanceMax}
                onChange={(event) => patchFilters({ ratingOverallRelevanceMax: event.target.value })}
              />
              <InputField
                label="Depth Min"
                value={filters.ratingDepthScoreMin}
                onChange={(event) => patchFilters({ ratingDepthScoreMin: event.target.value })}
              />
              <InputField
                label="Depth Max"
                value={filters.ratingDepthScoreMax}
                onChange={(event) => patchFilters({ ratingDepthScoreMax: event.target.value })}
              />
              <InputField
                label="Detail Min"
                value={filters.ratingRelevantDetailScoreMin}
                onChange={(event) => patchFilters({ ratingRelevantDetailScoreMin: event.target.value })}
              />
              <InputField
                label="Detail Max"
                value={filters.ratingRelevantDetailScoreMax}
                onChange={(event) => patchFilters({ ratingRelevantDetailScoreMax: event.target.value })}
              />
              <InputField
                label="Citation Confidence Min"
                value={filters.citationConfidenceMin}
                onChange={(event) => patchFilters({ citationConfidenceMin: event.target.value })}
              />
              <InputField
                label="Citation Confidence Max"
                value={filters.citationConfidenceMax}
                onChange={(event) => patchFilters({ citationConfidenceMax: event.target.value })}
              />
              <InputField
                label="Citation Type"
                value={filters.citationType}
                onChange={(event) => patchFilters({ citationType: event.target.value })}
              />
              <InputField
                label="Citation DOI"
                value={filters.citationDoi}
                onChange={(event) => patchFilters({ citationDoi: event.target.value })}
              />
              <InputField
                label="Report Number"
                value={filters.citationReportNumber}
                onChange={(event) => patchFilters({ citationReportNumber: event.target.value })}
              />
              <InputField
                label="Standard Number"
                value={filters.citationStandardNumber}
                onChange={(event) => patchFilters({ citationStandardNumber: event.target.value })}
              />
            </div>

            <div className="mt-3">
              <InputField
                label="Missing Citation Fields"
                value={filters.citationMissingFields}
                onChange={(event) => patchFilters({ citationMissingFields: event.target.value })}
              />
            </div>
          </div>
        )}
      </SurfaceCard>

      <div className="grid min-h-0 flex-1 gap-4 xl:grid-cols-[minmax(0,1fr)_24rem]">
        <div className="flex min-h-0 flex-col gap-4">
          <SurfaceCard>
            <div className="grid gap-3 md:grid-cols-[minmax(0,1fr)_auto_auto]">
              <InputField
                label="Global Search"
                placeholder="Search all manifest fields"
                value={filters.q}
                onChange={(event) => patchFilters({ q: event.target.value })}
              />
              <div className="md:mt-6 text-body-md text-on-surface-variant">
                250 rows per page
              </div>
              <div className="md:mt-5 flex flex-wrap gap-2">
                <Button
                  disabled={selectedIds.size === 0 || deletePending}
                  variant="danger"
                  onClick={() => void handleDeleteSelected()}
                >
                  {deletePending ? "Deleting..." : `Delete Selected${selectedIds.size ? ` (${selectedIds.size})` : ""}`}
                </Button>
              </div>
            </div>
          </SurfaceCard>

          <SurfaceCard className="flex min-h-0 flex-1 flex-col overflow-hidden p-0">
            {manifestQuery.isLoading ? (
              <div className="p-6 text-body-md text-on-surface-variant">Loading repository sources...</div>
            ) : manifestQuery.isError ? (
              <div className="p-6 text-body-md text-error">
                {String((manifestQuery.error as Error)?.message || "Failed to load repository sources")}
              </div>
            ) : rows.length === 0 ? (
              <div className="p-4">
                <EmptyState
                  title="No matching sources"
                  detail="Adjust the search or hidden filters, or import additional sources into the repository."
                />
              </div>
            ) : (
              <div className="flex min-h-0 flex-1 flex-col">
                <div className="thin-scrollbar min-h-0 flex-1 overflow-auto">
                  <div
                    className="repository-browser-action-row sticky top-0 z-10"
                    style={{
                      width: `${actionRowWidth}px`,
                      minWidth: `${actionRowWidth}px`,
                      height: `${REPOSITORY_BROWSER_ACTION_ROW_HEIGHT}px`,
                    }}
                  >
                    <div
                      aria-hidden="true"
                      className="repository-browser-action-cell border-r border-outline-variant/20"
                      style={{
                        width: `${REPOSITORY_BROWSER_SELECTION_COLUMN_WIDTH}px`,
                        minWidth: `${REPOSITORY_BROWSER_SELECTION_COLUMN_WIDTH}px`,
                      }}
                    />
                    {renderedColumns.map((column) => {
                      const style = columnWidthStyle(columnWidths, column.key);
                      const label = labelRepositoryBrowserColumn(column.key, column.label);
                      const runningThisColumn = activeColumnRun?.column_id === column.key;
                      const instructionsDisabled = !column.processable;
                      const runDisabled =
                        !column.processable ||
                        !llmReady ||
                        !column.instruction_prompt.trim() ||
                        Boolean(activeColumnRun);
                      return (
                        <div
                          key={`action-${column.key}`}
                          className="repository-browser-action-cell"
                          id={`repository-column-control-${column.key}`}
                          style={style}
                        >
                          <div className="flex items-start justify-between gap-2">
                            <div className="min-w-0">
                              <div className="truncate text-[11px] font-semibold uppercase tracking-[0.08em] text-on-surface">
                                {label}
                              </div>
                              <div className="mt-1 truncate text-[11px] text-on-surface-variant">
                                {formatColumnActionStatus(column, activeColumnRun)}
                              </div>
                            </div>
                            {runningThisColumn && (
                              <StatusBadge text="Running" tone="active" />
                            )}
                          </div>

                          <div className="mt-2 flex flex-wrap gap-1">
                            <button
                              aria-label={`Edit instructions for ${label}`}
                              className={repositoryColumnActionButtonClass(instructionsDisabled)}
                              disabled={instructionsDisabled}
                              onClick={() => openColumnPromptModal(column)}
                              title="Instructions"
                              type="button"
                            >
                              <ColumnInstructionsIcon />
                            </button>
                            <button
                              aria-label={`Run ${label}`}
                              className={repositoryColumnActionButtonClass(runDisabled)}
                              disabled={runDisabled}
                              onClick={() => openColumnRunScopeModal(column)}
                              title={runningThisColumn ? "Running" : "Run"}
                              type="button"
                            >
                              <ColumnRunIcon />
                            </button>
                            {column.renamable && (
                              <button
                                aria-label={`Rename ${label}`}
                                className={repositoryColumnActionButtonClass(columnRenamePending)}
                                disabled={columnRenamePending}
                                onClick={() => beginColumnRename(column)}
                                title="Rename"
                                type="button"
                              >
                                <ColumnRenameIcon />
                              </button>
                            )}
                          </div>
                        </div>
                      );
                    })}
                    <div
                      className="repository-browser-action-rail"
                      style={{
                        width: `${REPOSITORY_BROWSER_ACTION_RAIL_WIDTH}px`,
                        minWidth: `${REPOSITORY_BROWSER_ACTION_RAIL_WIDTH}px`,
                      }}
                    >
                      <button
                        aria-label="Add custom column"
                        className="repository-browser-add-column-button"
                        disabled={columnCreatePending}
                        onClick={() => void handleCreateColumn()}
                        type="button"
                      >
                        {columnCreatePending ? "…" : "+"}
                      </button>
                    </div>
                  </div>
                  <table
                    className="data-table"
                    style={{ width: `${tableWidth}px`, minWidth: `${tableWidth}px` }}
                  >
                    <colgroup>
                      <col style={{ width: `${REPOSITORY_BROWSER_SELECTION_COLUMN_WIDTH}px` }} />
                      {renderedColumns.map((column) => {
                        const style = columnWidthStyle(columnWidths, column.key);
                        return (
                          <col
                            key={column.key}
                            style={style ? { width: style.width } : undefined}
                          />
                        );
                      })}
                    </colgroup>
                    <thead>
                      <tr>
                        <th
                          className="w-12"
                          style={{ top: `${REPOSITORY_BROWSER_ACTION_ROW_HEIGHT}px`, zIndex: 2 }}
                        >
                          <input
                            ref={headerCheckboxRef}
                            checked={allVisibleSelected}
                            onChange={(event) => toggleSelectAllVisible(event.target.checked)}
                            type="checkbox"
                          />
                        </th>
                        {renderedColumns.map((column) => {
                          const active = filters.sortBy === column.key;
                          const directionLabel = active ? (filters.sortDir === "asc" ? "↑" : "↓") : "";
                          return (
                            <th
                              key={column.key}
                              style={{
                                ...columnWidthStyle(columnWidths, column.key),
                                top: `${REPOSITORY_BROWSER_ACTION_ROW_HEIGHT}px`,
                                zIndex: 2,
                              }}
                            >
                              <div className="group flex items-center justify-between gap-2">
                                {renamingColumnId === column.key ? (
                                  <div className="flex min-w-0 flex-1 items-center gap-2">
                                    <input
                                      ref={renameInputRef}
                                      className="min-w-0 flex-1 rounded-sm border border-primary/60 bg-surface px-2 py-1 text-body-md text-on-surface focus:border-primary focus:outline-none"
                                      disabled={columnRenamePending}
                                      value={renamingColumnLabel}
                                      onChange={(event) => setRenamingColumnLabel(event.target.value)}
                                      onKeyDown={handleRenameKeyDown}
                                    />
                                    <button
                                      className="text-label-sm text-primary hover:underline"
                                      disabled={columnRenamePending}
                                      onClick={() => void commitColumnRename()}
                                      type="button"
                                    >
                                      Save
                                    </button>
                                    <button
                                      className="text-label-sm text-on-surface-variant hover:text-on-surface"
                                      disabled={columnRenamePending}
                                      onClick={cancelColumnRename}
                                      type="button"
                                    >
                                      Cancel
                                    </button>
                                  </div>
                                ) : column.sortable ? (
                                  <button
                                    className="flex min-w-0 items-center gap-2 text-left text-label-sm uppercase tracking-[0.08em] text-on-surface-variant hover:text-on-surface"
                                    onClick={() => handleSort(column)}
                                    type="button"
                                  >
                                    <span className="truncate">
                                      {labelRepositoryBrowserColumn(column.key, column.label)}
                                    </span>
                                    <span>{directionLabel}</span>
                                  </button>
                                ) : (
                                  <span className="truncate">
                                    {labelRepositoryBrowserColumn(column.key, column.label)}
                                  </span>
                                )}
                                <button
                                  aria-label={`Resize ${labelRepositoryBrowserColumn(column.key, column.label)} column`}
                                  className="h-7 w-3 shrink-0 cursor-col-resize rounded-sm bg-outline-variant/40 opacity-60 hover:bg-primary hover:opacity-100 group-hover:opacity-100"
                                  onMouseDown={(event) => beginColumnResize(event, column.key)}
                                  type="button"
                                />
                              </div>
                            </th>
                          );
                        })}
                      </tr>
                    </thead>
                    <tbody>
                      {rows.map((row) => {
                        const isSelected = selectedIds.has(row.id);
                        const isActive = activeRowId === row.id;
                        return (
                          <tr
                            key={`${row.id}-${row.original_url}`}
                            className={isActive ? "bg-surface-container-highest" : isSelected ? "bg-surface-container-high" : ""}
                            onClick={() => setActiveRowId(row.id)}
                          >
                            <td onClick={(event) => event.stopPropagation()}>
                              <input
                                checked={isSelected}
                                onChange={(event) => handleRowCheckbox(event, row.id)}
                                type="checkbox"
                              />
                            </td>
                            {renderedColumns.map((column) => {
                              const style = columnWidthStyle(columnWidths, column.key);
                              if (column.key.startsWith("file_")) {
                                const kind = column.key.replace("file_", "") as RepositorySourceFileKind;
                                return (
                                  <td key={`${row.id}-${column.key}`} style={style}>
                                    {hasFileForKind(row, kind) ? (
                                      <a
                                        className="text-primary hover:underline"
                                        href={buildFileHref(row, kind)}
                                        onClick={(event) => event.stopPropagation()}
                                        rel="noreferrer"
                                        target="_blank"
                                      >
                                        Open
                                      </a>
                                    ) : (
                                      <span className="text-on-surface-variant/60">—</span>
                                    )}
                                  </td>
                                );
                              }

                              const value = row[column.key];
                              const formatted = formatCellValue(value);
                              const isLongText =
                                column.key === "summary_text" ||
                                column.key === "rating_rationale" ||
                                column.key === "relevant_sections";
                              return (
                                <td
                                  key={`${row.id}-${column.key}`}
                                  className={isLongText ? "data-cell-wrap repository-browser-long-cell" : ""}
                                  style={style}
                                  title={formatted === "—" ? "" : formatted}
                                >
                                  {formatted}
                                </td>
                              );
                            })}
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>

                <div className="flex items-center justify-between border-t border-outline-variant/30 bg-surface-container px-4 py-3">
                  <div className="text-body-md text-on-surface-variant">
                    Showing {rows.length} rows on this page out of {totalRows} total
                  </div>
                  <div className="flex gap-2">
                    <Button
                      disabled={filters.offset <= 0}
                      onClick={() =>
                        patchFilters(
                          { offset: Math.max(0, filters.offset - REPOSITORY_BROWSER_PAGE_SIZE) },
                          false,
                        )
                      }
                    >
                      Prev
                    </Button>
                    <Button
                      disabled={filters.offset + REPOSITORY_BROWSER_PAGE_SIZE >= totalRows}
                      onClick={() =>
                        patchFilters(
                          { offset: filters.offset + REPOSITORY_BROWSER_PAGE_SIZE },
                          false,
                        )
                      }
                    >
                      Next
                    </Button>
                  </div>
                </div>
              </div>
            )}
          </SurfaceCard>

          <SurfaceCard className="shrink-0">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div className="text-title-sm font-semibold">Export RIS</div>
                <div className="mt-1 text-body-md text-on-surface-variant">
                  Export citations from the selected rows or from the currently displayed page.
                </div>
              </div>
              <div className="flex flex-wrap gap-2">
                <Button
                  variant="primary"
                  disabled={selectedIds.size === 0 || risExportPending}
                  onClick={() => void handleExportRis(Array.from(selectedIds), "Selected Rows")}
                >
                  {risExportPending ? "Exporting..." : "Export RIS For Selected Rows"}
                </Button>
                <Button
                  disabled={rows.length === 0 || risExportPending}
                  onClick={() => void handleExportRis(rows.map((row) => row.id), "Displayed Rows")}
                >
                  Export RIS For Displayed Rows
                </Button>
              </div>
            </div>
          </SurfaceCard>
        </div>

        {activeRow && detailDraft ? (
          <SourceDetailsDrawer
            draft={detailDraft}
            row={activeRow}
            saveError={detailSaveError}
            saveState={detailSaveState}
            onChange={(field, value) => {
              setDetailDraft((prev) => (prev ? { ...prev, [field]: value } : prev));
              setDetailSaveState("idle");
              setDetailSaveError("");
            }}
          />
        ) : (
          <SurfaceCard className="h-full">
            <EmptyState
              title="Select A Source"
              detail="Click a row to inspect status, open files, and edit metadata, summary, and rating fields."
            />
          </SurfaceCard>
        )}
      </div>
      {columnPromptDraft && (
        <ColumnPromptModal
          draft={columnPromptDraft}
          error={columnPromptError}
          fixingPrompt={columnPromptFixing}
          llmReady={llmReady}
          savePending={columnPromptSaving}
          onCancel={() => {
            if (columnPromptSaving || columnPromptFixing) return;
            setColumnPromptDraft(null);
            setColumnPromptError("");
          }}
          onChange={(nextValue) => {
            setColumnPromptDraft((prev) => (prev ? { ...prev, prompt: nextValue } : prev));
            setColumnPromptError("");
          }}
          onFixPrompt={() => void handleFixColumnPrompt()}
          onSave={() => void handleSaveColumnPrompt()}
        />
      )}
      {columnRunScopeDraft && (
        <ColumnRunScopeModal
          draft={columnRunScopeDraft}
          selectedCount={selectedIds.size}
          startPending={columnRunStarting}
          onCancel={() => {
            if (columnRunStarting) return;
            setColumnRunScopeDraft(null);
          }}
          onChangeScope={(scope) =>
            setColumnRunScopeDraft((prev) => (prev ? { ...prev, scope } : prev))
          }
          onConfirm={() => void handleRunColumn(columnRunScopeDraft)}
        />
      )}
    </div>
  );
}
