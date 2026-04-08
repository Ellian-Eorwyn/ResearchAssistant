"""HTML generation for repository export bundle viewers."""

from __future__ import annotations

import html
import json
from typing import Any


def _json_for_html(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")


def _js_string(value: str) -> str:
    return json.dumps(str(value or ""))


def _html_text(value: str) -> str:
    return html.escape(str(value or ""), quote=False)


def _build_initial_subtitle(payload: dict[str, Any], *, export_label: str) -> str:
    rows = payload.get("rows")
    row_count = len(rows) if isinstance(rows, list) else 0
    parts = [f"{row_count} source{'' if row_count == 1 else 's'}", export_label]
    if str(payload.get("exportScope") or "").strip().lower() == "selected":
        parts.append("selected export")
    else:
        parts.append("full repository export")
    file_kinds = payload.get("bundleFileKinds")
    if isinstance(file_kinds, list) and file_kinds:
        kinds = ", ".join(str(kind or "").upper() for kind in file_kinds if str(kind or "").strip())
        if kinds:
            parts.append(f"files: {kinds}")
    exported_at = str(payload.get("exportedAt") or "").strip()
    if exported_at:
        parts.append(f"generated {exported_at}")
    return " · ".join(parts)


_STYLE_BLOCK = """
*, *::before, *::after { box-sizing: border-box; }
:root {
  --bg: #f3f5f7;
  --card: #ffffff;
  --card-alt: #f8fafc;
  --border: #dde3ea;
  --border-strong: #cad4df;
  --text: #15202b;
  --text-muted: #586575;
  --text-soft: #7b8794;
  --accent: #1357dd;
  --accent-soft: #ecf3ff;
  --accent-strong: #0b44b8;
  --success: #107651;
  --warning: #b36300;
  --danger: #b42318;
  --shadow: 0 10px 30px rgba(15, 23, 42, 0.08);
  --radius: 16px;
  --radius-sm: 10px;
  --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
  --mono: "SFMono-Regular", "SF Mono", Menlo, Consolas, monospace;
}
html { font-size: 15px; }
body {
  margin: 0;
  min-height: 100vh;
  background:
    radial-gradient(circle at top left, rgba(19,87,221,0.08), transparent 26%),
    linear-gradient(180deg, #f7f9fc 0%, var(--bg) 35%, #eef2f6 100%);
  color: var(--text);
  font-family: var(--font);
  line-height: 1.5;
}
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
button, input { font: inherit; }
.shell {
  max-width: 1720px;
  margin: 0 auto;
  padding: 28px;
}
.header {
  background: rgba(255,255,255,0.88);
  border: 1px solid rgba(221,227,234,0.9);
  border-radius: 22px;
  box-shadow: var(--shadow);
  backdrop-filter: blur(12px);
  padding: 24px 24px 18px;
}
.header-top {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 20px;
}
.eyebrow {
  margin: 0 0 6px;
  font-size: 0.74rem;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: var(--accent);
  font-weight: 700;
}
.title {
  margin: 0;
  font-size: 1.7rem;
  line-height: 1.1;
  letter-spacing: -0.03em;
}
.subtitle {
  margin: 10px 0 0;
  color: var(--text-muted);
  max-width: 72ch;
}
.header-controls {
  display: flex;
  flex-direction: column;
  align-items: stretch;
  gap: 14px;
  width: min(760px, 100%);
}
.search-row {
  display: flex;
  gap: 12px;
  align-items: center;
}
.search-wrap {
  position: relative;
  flex: 1 1 auto;
}
.search-wrap svg {
  position: absolute;
  left: 14px;
  top: 50%;
  transform: translateY(-50%);
  color: var(--text-soft);
  pointer-events: none;
}
.search-input {
  width: 100%;
  padding: 12px 14px 12px 42px;
  border-radius: 12px;
  border: 1px solid var(--border);
  background: var(--card);
  outline: none;
  transition: border-color 0.15s, box-shadow 0.15s;
}
.search-input:focus {
  border-color: var(--accent);
  box-shadow: 0 0 0 4px rgba(19,87,221,0.12);
}
.button-row {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}
.button {
  appearance: none;
  border: 1px solid var(--border-strong);
  background: var(--card);
  color: var(--text);
  padding: 10px 14px;
  border-radius: 12px;
  font-weight: 600;
  cursor: pointer;
  transition: transform 0.12s ease, box-shadow 0.12s ease, border-color 0.12s ease, background 0.12s ease;
}
.button:hover:not(:disabled) {
  transform: translateY(-1px);
  box-shadow: 0 8px 20px rgba(15, 23, 42, 0.08);
  border-color: var(--accent);
}
.button:disabled {
  cursor: not-allowed;
  opacity: 0.55;
}
.button.primary {
  background: linear-gradient(135deg, var(--accent) 0%, var(--accent-strong) 100%);
  border-color: var(--accent);
  color: #fff;
}
.button.primary:hover:not(:disabled) {
  border-color: var(--accent-strong);
}
.status-row {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 16px;
}
.pill {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  min-height: 34px;
  padding: 7px 12px;
  border-radius: 999px;
  border: 1px solid var(--border);
  background: var(--card-alt);
  color: var(--text-muted);
  font-size: 0.86rem;
}
.pill strong { color: var(--text); }
.pill.hidden { display: none; }
.pill.warning {
  background: #fff7eb;
  border-color: #f3d29b;
  color: var(--warning);
}
.pill.success {
  background: #edfdf4;
  border-color: #a6e6be;
  color: var(--success);
}
.pill.error {
  background: #fff1f2;
  border-color: #f6c2c8;
  color: var(--danger);
}
.layout {
  display: grid;
  grid-template-columns: minmax(0, 1.45fr) minmax(360px, 0.95fr);
  gap: 22px;
  margin-top: 22px;
}
.panel {
  min-width: 0;
  background: rgba(255,255,255,0.92);
  border: 1px solid rgba(221,227,234,0.95);
  border-radius: 22px;
  box-shadow: var(--shadow);
  overflow: hidden;
}
.panel-head {
  padding: 18px 20px;
  border-bottom: 1px solid var(--border);
  background: linear-gradient(180deg, rgba(255,255,255,0.92), rgba(248,250,252,0.92));
}
.panel-title {
  margin: 0;
  font-size: 1rem;
  font-weight: 700;
}
.panel-subtitle {
  margin: 6px 0 0;
  color: var(--text-muted);
  font-size: 0.88rem;
}
.table-wrap {
  overflow: auto;
  max-height: calc(100vh - 240px);
}
table {
  width: 100%;
  border-collapse: collapse;
  table-layout: fixed;
}
thead th {
  position: sticky;
  top: 0;
  z-index: 2;
  background: rgba(248,250,252,0.97);
  border-bottom: 1px solid var(--border);
  padding: 0;
  text-align: left;
}
th.checkbox-col, td.checkbox-col {
  width: 48px;
  text-align: center;
}
th.col-title { width: 33%; }
th.col-authors { width: 18%; }
th.col-date { width: 13%; }
th.col-org { width: 16%; }
th.col-rating { width: 10%; }
th.col-type { width: 10%; }
.sort-button {
  width: 100%;
  border: 0;
  background: transparent;
  padding: 12px 14px;
  color: var(--text-soft);
  text-align: left;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  font-size: 0.74rem;
  font-weight: 700;
  cursor: pointer;
}
.sort-button:hover { color: var(--accent); }
.sort-arrow {
  display: inline-block;
  min-width: 10px;
  margin-left: 6px;
}
tbody tr {
  border-bottom: 1px solid #edf1f5;
  cursor: pointer;
  transition: background 0.12s ease;
}
tbody tr:hover { background: #f5f9ff; }
tbody tr.active {
  background: linear-gradient(90deg, rgba(19,87,221,0.12), rgba(19,87,221,0.03));
}
tbody td {
  padding: 14px;
  vertical-align: top;
}
.row-title {
  font-weight: 700;
  line-height: 1.35;
  margin-bottom: 4px;
}
.row-meta {
  font-size: 0.78rem;
  color: var(--text-soft);
}
.cell-muted {
  color: var(--text-muted);
  font-size: 0.86rem;
}
.rating-badge {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  min-width: 54px;
  padding: 5px 10px;
  border-radius: 999px;
  background: #eef2f7;
  color: var(--text);
  font-weight: 700;
  font-size: 0.82rem;
}
.rating-badge.high {
  background: #eafaf1;
  color: var(--success);
}
.rating-badge.medium {
  background: #fff7eb;
  color: var(--warning);
}
.rating-badge.low {
  background: #fff1f2;
  color: var(--danger);
}
.empty-state {
  padding: 54px 20px;
  text-align: center;
  color: var(--text-muted);
}
.detail {
  display: flex;
  flex-direction: column;
  min-height: calc(100vh - 240px);
}
.detail-scroll {
  padding: 20px 22px 26px;
  overflow: auto;
  max-height: calc(100vh - 240px);
}
.detail-placeholder {
  padding: 36px 12px;
  color: var(--text-muted);
}
.detail-title {
  margin: 0;
  font-size: 1.4rem;
  line-height: 1.2;
  letter-spacing: -0.02em;
}
.detail-subtitle {
  margin: 10px 0 0;
  color: var(--text-muted);
}
.detail-badges {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin-top: 14px;
}
.detail-chip {
  padding: 6px 10px;
  border-radius: 999px;
  background: var(--accent-soft);
  color: var(--accent);
  font-size: 0.8rem;
  font-weight: 700;
}
.detail-section {
  margin-top: 22px;
  padding-top: 22px;
  border-top: 1px solid var(--border);
}
.detail-section h3 {
  margin: 0 0 12px;
  font-size: 0.95rem;
  letter-spacing: -0.01em;
}
.detail-section p {
  margin: 0 0 12px;
  color: var(--text-muted);
}
.meta-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 14px 16px;
}
.meta-item dt {
  margin: 0 0 4px;
  color: var(--text-soft);
  font-size: 0.77rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  font-weight: 700;
}
.meta-item dd {
  margin: 0;
  color: var(--text);
  word-break: break-word;
}
.file-links {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
}
.file-link {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-radius: 12px;
  border: 1px solid var(--border);
  background: var(--card-alt);
  color: var(--text);
  font-weight: 600;
}
.file-link:hover {
  border-color: var(--accent);
  color: var(--accent);
  text-decoration: none;
}
.rich-text { color: var(--text-muted); }
.rich-text p:last-child { margin-bottom: 0; }
.markdown-toggle {
  border: 1px solid var(--border);
  border-radius: 14px;
  background: var(--card-alt);
  overflow: hidden;
}
.markdown-toggle summary {
  list-style: none;
  cursor: pointer;
  padding: 14px 16px;
  font-weight: 700;
}
.markdown-toggle summary::-webkit-details-marker { display: none; }
.markdown-toggle[open] summary {
  border-bottom: 1px solid var(--border);
}
.markdown-body {
  padding: 16px;
  font-size: 0.92rem;
  line-height: 1.7;
}
.markdown-body h1, .markdown-body h2, .markdown-body h3, .markdown-body h4, .markdown-body h5, .markdown-body h6 {
  color: var(--text);
  line-height: 1.3;
  margin: 1.4em 0 0.6em;
}
.markdown-body h1 { font-size: 1.45rem; }
.markdown-body h2 { font-size: 1.2rem; }
.markdown-body h3 { font-size: 1.05rem; }
.markdown-body p, .markdown-body ul, .markdown-body ol, .markdown-body blockquote, .markdown-body table, .markdown-body pre {
  margin: 0 0 1em;
}
.markdown-body ul, .markdown-body ol { padding-left: 1.4em; }
.markdown-body blockquote {
  margin-left: 0;
  padding: 0.7em 1em;
  border-left: 3px solid var(--accent);
  background: #f5f9ff;
  border-radius: 0 10px 10px 0;
}
.markdown-body code {
  font-family: var(--mono);
  font-size: 0.88em;
  background: #eef2f7;
  padding: 0.15em 0.35em;
  border-radius: 6px;
}
.markdown-body pre {
  overflow: auto;
  background: #0f172a;
  color: #e2e8f0;
  padding: 14px 16px;
  border-radius: 14px;
}
.markdown-body pre code {
  background: transparent;
  padding: 0;
  color: inherit;
}
.markdown-body table {
  width: 100%;
  border-collapse: collapse;
}
.markdown-body th, .markdown-body td {
  border: 1px solid var(--border);
  padding: 8px 10px;
  text-align: left;
}
.markdown-body th {
  background: #f8fafc;
}
@media (max-width: 1100px) {
  .layout {
    grid-template-columns: 1fr;
  }
  .table-wrap, .detail-scroll {
    max-height: none;
  }
}
@media (max-width: 760px) {
  .shell { padding: 16px; }
  .header { padding: 18px; border-radius: 18px; }
  .header-top { flex-direction: column; }
  .meta-grid { grid-template-columns: 1fr; }
  .table-wrap { overflow-x: auto; }
  table { min-width: 880px; }
}
"""


_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Repository Browser</title>
<style>
__STYLE_BLOCK__
</style>
</head>
<body>
<div class="shell">
  <header class="header">
    <div class="header-top">
      <div>
        <p class="eyebrow">__EYEBROW__</p>
        <h1 class="title" id="repoTitle">__REPOSITORY_NAME__</h1>
        <p class="subtitle" id="repoSubtitle">__INITIAL_SUBTITLE__</p>
      </div>
      <div class="header-controls">
        <div class="search-row">
          <div class="search-wrap">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><circle cx="11" cy="11" r="8"></circle><path d="m21 21-4.35-4.35"></path></svg>
            <input id="searchInput" class="search-input" type="search" autocomplete="off" placeholder="Search titles, authors, organizations, summaries, and notes">
          </div>
        </div>
        <div class="button-row">
__BUTTONS__
        </div>
      </div>
    </div>
    <div class="status-row">
      <div id="resultsSummary" class="pill"></div>
      <div id="folderStatus" class="pill__FOLDER_STATUS_EXTRA_CLASS__"></div>
      <div id="supportNotice" class="pill warning hidden__SUPPORT_NOTICE_EXTRA_CLASS__"></div>
      <div id="exportStatus" class="pill hidden"></div>
    </div>
  </header>

  <main class="layout">
    <section class="panel">
      <div class="panel-head">
        <h2 class="panel-title">Sources</h2>
        <p class="panel-subtitle">Sort by the header row, search across the repository, and use checkboxes to export a subset.</p>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th class="checkbox-col"><input id="selectAllVisible" type="checkbox" aria-label="Select all visible sources"></th>
              <th class="col-title"><button class="sort-button" type="button" data-sort="title">Title <span class="sort-arrow" data-arrow="title"></span></button></th>
              <th class="col-authors"><button class="sort-button" type="button" data-sort="authors">Authors <span class="sort-arrow" data-arrow="authors"></span></button></th>
              <th class="col-date"><button class="sort-button" type="button" data-sort="publicationDate">Date <span class="sort-arrow" data-arrow="publicationDate"></span></button></th>
              <th class="col-org"><button class="sort-button" type="button" data-sort="organization">Organization <span class="sort-arrow" data-arrow="organization"></span></button></th>
              <th class="col-rating"><button class="sort-button" type="button" data-sort="overallRating">Relevance <span class="sort-arrow" data-arrow="overallRating"></span></button></th>
              <th class="col-type"><button class="sort-button" type="button" data-sort="type">Type <span class="sort-arrow" data-arrow="type"></span></button></th>
            </tr>
          </thead>
          <tbody id="tableBody"></tbody>
        </table>
      </div>
    </section>

    <section class="panel detail">
      <div class="panel-head">
        <h2 class="panel-title">Details</h2>
        <p class="panel-subtitle">Read-only metadata, summaries, ratings, linked files, and full markdown.</p>
      </div>
      <div id="detailPane" class="detail-scroll"></div>
    </section>
  </main>
</div>

<script>
const BUNDLE = __BUNDLE_DATA__;
__MODE_CONSTANTS__
const TEXT_ENCODER = new TextEncoder();

let allRows = [];
let rowsById = new Map();
let visibleRows = [];
let selectedIds = new Set();
let lastAnchorId = "";
let activeRowId = "";
let searchQuery = "";
let sortColumn = "overallRating";
let sortDirection = "desc";
let exportInFlight = false;
__MODE_STATE__

document.addEventListener("DOMContentLoaded", init);

function init() {
  allRows = (Array.isArray(BUNDLE.rows) ? BUNDLE.rows : []).map(normalizeRow);
  rowsById = new Map(allRows.map((row) => [row.id, row]));
  activeRowId = allRows[0] ? allRows[0].id : "";

  document.getElementById("repoTitle").textContent = BUNDLE.repositoryName || "Repository Browser";
  document.title = (BUNDLE.repositoryName || "Repository Browser") + __TITLE_SUFFIX_JSON__;
  document.getElementById("repoSubtitle").textContent = buildSubtitle();
  bindEvents();
  renderSupportState();
  applyFilters();
}

function buildSubtitle() {
  const parts = [];
  parts.push((Array.isArray(BUNDLE.rows) ? BUNDLE.rows.length : 0) + " source" + ((BUNDLE.rows || []).length === 1 ? "" : "s"));
  parts.push(__EXPORT_LABEL_JSON__);
  if (BUNDLE.exportScope === "selected") {
    parts.push("selected export");
  } else {
    parts.push("full repository export");
  }
  if (Array.isArray(BUNDLE.bundleFileKinds) && BUNDLE.bundleFileKinds.length) {
    parts.push("files: " + BUNDLE.bundleFileKinds.map((kind) => String(kind || "").toUpperCase()).join(", "));
  }
  if (BUNDLE.exportedAt) {
    parts.push("generated " + formatDateTime(BUNDLE.exportedAt));
  }
  return parts.join(" · ");
}

function normalizeRow(row) {
  const normalized = { ...row };
  normalized.id = String(row.id || "");
  normalized.sourceTitle = String(row.sourceTitle || "");
  normalized.sourceAuthors = String(row.sourceAuthors || "").trim();
  normalized.title = String(row.title || row.sourceTitle || ("Source " + normalized.id)).trim() || ("Source " + normalized.id);
  normalized.authors = String(row.authors || row.sourceAuthors || "").trim();
  normalized.publicationDate = String(row.publicationDate || row.publicationYear || "").trim();
  normalized.publicationYear = String(row.publicationYear || "").trim();
  normalized.organization = String(row.organization || "").trim();
  normalized.organizationType = String(row.organizationType || "").trim();
  normalized.documentType = String(row.documentType || "").trim();
  normalized.citationType = String(row.citationType || "").trim();
  normalized.typeLabel = normalized.documentType || normalized.citationType || "Unspecified";
  normalized.reportNumber = String(row.reportNumber || "").trim();
  normalized.overallRating = String(row.overallRating || "").trim();
  normalized.summary = String(row.summary || "").trim();
  normalized.ratingRationale = String(row.ratingRationale || "").trim();
  normalized.relevantSections = String(row.relevantSections || "").trim();
  normalized.exportUrl = String(row.exportUrl || row.citationUrl || row.sourceUrl || "").trim();
  normalized.citationUrl = String(row.citationUrl || "").trim();
  normalized.sourceUrl = String(row.sourceUrl || "").trim();
  normalized.markdown = String(row.markdown || "").trim();
  normalized.markdownCharCount = Number.isFinite(Number(row.markdownCharCount)) ? Number(row.markdownCharCount) : 0;
  normalized.files = normalizeFiles(row.files);
  normalized.fileList = Object.values(normalized.files);
  normalized.customFields = Array.isArray(row.customFields) ? row.customFields.map((field) => ({
    key: String(field.key || ""),
    label: String(field.label || field.key || ""),
    value: String(field.value || ""),
  })) : [];
  normalized.csvRecord = row.csvRecord && typeof row.csvRecord === "object" ? row.csvRecord : {};
  normalized.ris = String(row.ris || "");
  normalized._ratingSort = parseNumber(normalized.overallRating);
  normalized._search = [
    normalized.title,
    normalized.sourceTitle,
    normalized.authors,
    normalized.publicationDate,
    normalized.organization,
    normalized.documentType,
    normalized.citationType,
    normalized.reportNumber,
    normalized.summary,
    normalized.ratingRationale,
    normalized.relevantSections,
    normalized.markdown.slice(0, 12000),
    normalized.customFields.map((field) => field.label + " " + field.value).join(" "),
  ].join(" ").toLowerCase();
  normalized._markdownHtml = "";
  return normalized;
}

function normalizeFiles(files) {
  if (!files || typeof files !== "object") {
    return {};
  }
  const normalized = {};
  for (const key of Object.keys(files)) {
    const entry = normalizeFileEntry(key, files[key]);
    if (entry) {
      normalized[key] = entry;
    }
  }
  return normalized;
}

__NORMALIZE_FILE_ENTRY__

function basenameFromPath(value) {
  return String(value || "").split(/[\\\\/]/).filter(Boolean).pop() || "";
}

function inferFileLabel(kind, ...values) {
  for (const value of values) {
    const name = basenameFromPath(value);
    const match = name.match(/\\.([a-z0-9]+)$/i);
    if (match) {
      return match[1].toUpperCase();
    }
  }
  return String(kind || "").trim().toUpperCase() || "FILE";
}

function bindEvents() {
  document.getElementById("searchInput").addEventListener("input", (event) => {
    searchQuery = String(event.target.value || "").trim().toLowerCase();
    applyFilters();
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "/" && event.target === document.body) {
      event.preventDefault();
      document.getElementById("searchInput").focus();
    }
  });

  document.querySelectorAll(".sort-button").forEach((button) => {
    button.addEventListener("click", () => {
      const nextColumn = String(button.dataset.sort || "");
      if (!nextColumn) {
        return;
      }
      if (sortColumn === nextColumn) {
        sortDirection = sortDirection === "asc" ? "desc" : "asc";
      } else {
        sortColumn = nextColumn;
        sortDirection = nextColumn === "overallRating" ? "desc" : "asc";
      }
      applyFilters();
    });
  });

  document.getElementById("tableBody").addEventListener("click", (event) => {
    const checkbox = event.target.closest("input[data-select-id]");
    if (checkbox) {
      event.stopPropagation();
      toggleSelection(String(checkbox.dataset.selectId || ""), checkbox.checked, Boolean(event.shiftKey));
      return;
    }
    const row = event.target.closest("tr[data-row-id]");
    if (!row) {
      return;
    }
    activeRowId = String(row.dataset.rowId || "");
    renderTable();
    renderDetail();
  });

  document.getElementById("selectAllVisible").addEventListener("change", (event) => {
    const checked = Boolean(event.target.checked);
    visibleRows.forEach((row) => {
      if (checked) {
        selectedIds.add(row.id);
      } else {
        selectedIds.delete(row.id);
      }
    });
    lastAnchorId = visibleRows[0] ? visibleRows[0].id : "";
    renderTable();
    updateStatusSummary();
    updateButtonStates();
  });

__BIND_MODE_EVENTS__
  document.getElementById("exportSelectedBundleBtn").addEventListener("click", () => exportBundle("selected"));
  document.getElementById("exportAllBundleBtn").addEventListener("click", () => exportBundle("all"));
  document.getElementById("exportSelectedRisBtn").addEventListener("click", exportSelectedRis);
}

function renderSupportState() {
  const supportNotice = document.getElementById("supportNotice");
__RENDER_SUPPORT_STATE__
  updateFolderStatus();
  updateButtonStates();
}

function applyFilters() {
  visibleRows = allRows.filter((row) => !searchQuery || row._search.includes(searchQuery));
  visibleRows.sort(compareRows);

  if (!visibleRows.some((row) => row.id === activeRowId)) {
    activeRowId = visibleRows[0] ? visibleRows[0].id : "";
  }

  renderTable();
  renderDetail();
  updateStatusSummary();
  updateButtonStates();
  updateSortIndicators();
}

function compareRows(left, right) {
  const leftValue = sortValue(left, sortColumn);
  const rightValue = sortValue(right, sortColumn);
  const comparison = compareValues(leftValue, rightValue);
  if (comparison !== 0) {
    return sortDirection === "asc" ? comparison : -comparison;
  }
  return left.title.localeCompare(right.title, undefined, { sensitivity: "base" });
}

function sortValue(row, column) {
  if (column === "authors") {
    return row.authors.toLowerCase();
  }
  if (column === "publicationDate") {
    return row.publicationDate.toLowerCase();
  }
  if (column === "organization") {
    return row.organization.toLowerCase();
  }
  if (column === "overallRating") {
    return row._ratingSort;
  }
  if (column === "type") {
    return row.typeLabel.toLowerCase();
  }
  return row.title.toLowerCase();
}

function compareValues(left, right) {
  const leftBlank = isBlankValue(left);
  const rightBlank = isBlankValue(right);
  if (leftBlank && rightBlank) {
    return 0;
  }
  if (leftBlank) {
    return 1;
  }
  if (rightBlank) {
    return -1;
  }
  if (typeof left === "number" && typeof right === "number") {
    return left - right;
  }
  return String(left).localeCompare(String(right), undefined, { sensitivity: "base", numeric: true });
}

function isBlankValue(value) {
  if (value === "" || value === null || value === undefined) {
    return true;
  }
  if (typeof value === "number") {
    return Number.isNaN(value) || value === Number.NEGATIVE_INFINITY;
  }
  return false;
}

function renderTable() {
  const tableBody = document.getElementById("tableBody");
  if (!visibleRows.length) {
    tableBody.innerHTML = '<tr><td colspan="7"><div class="empty-state">No sources match the current search.</div></td></tr>';
    updateSelectAllState();
    return;
  }

  tableBody.innerHTML = visibleRows.map((row) => {
    const checked = selectedIds.has(row.id) ? " checked" : "";
    const activeClass = row.id === activeRowId ? " active" : "";
    const titleMeta = [
      row.id ? "ID " + row.id : "",
      row.reportNumber ? row.reportNumber : "",
      row.sourceTitle && row.sourceTitle !== row.title ? "Source title: " + row.sourceTitle : "",
    ].filter(Boolean).join(" · ");
    return `
      <tr data-row-id="${escAttr(row.id)}" class="${activeClass.trim()}">
        <td class="checkbox-col"><input type="checkbox" data-select-id="${escAttr(row.id)}"${checked} aria-label="Select ${escAttr(row.title)}"></td>
        <td>
          <div class="row-title">${esc(row.title)}</div>
          <div class="row-meta">${esc(titleMeta || "No additional citation notes")}</div>
        </td>
        <td class="cell-muted">${esc(row.authors || "Unknown")}</td>
        <td class="cell-muted">${esc(row.publicationDate || "Undated")}</td>
        <td class="cell-muted">${esc(row.organization || "Unspecified")}</td>
        <td>${renderRatingBadge(row)}</td>
        <td class="cell-muted">${esc(row.typeLabel)}</td>
      </tr>
    `;
  }).join("");
  updateSelectAllState();
}

function renderRatingBadge(row) {
  const value = row.overallRating || "—";
  const parsed = row._ratingSort;
  let tone = "";
  if (Number.isFinite(parsed)) {
    if (parsed >= 0.75) {
      tone = " high";
    } else if (parsed >= 0.4) {
      tone = " medium";
    } else {
      tone = " low";
    }
  }
  return '<span class="rating-badge' + tone + '">' + esc(value) + "</span>";
}

function renderDetail() {
  const detailPane = document.getElementById("detailPane");
  const row = rowsById.get(activeRowId);
  if (!row) {
    detailPane.innerHTML = '<div class="detail-placeholder">Select a source to review metadata, summaries, ratings, linked files, and full markdown.</div>';
    return;
  }

  const badges = [
    row.overallRating ? "Relevance " + row.overallRating : "",
    row.documentType ? row.documentType : "",
    row.citationType && row.citationType !== row.documentType ? row.citationType : "",
    row.organizationType ? row.organizationType : "",
  ].filter(Boolean);
  const fileLinks = row.fileList.map((file) => {
    const href = fileHref(file);
    const label = file.label;
    if (!href) {
      return '<span class="file-link">' + esc(label) + "</span>";
    }
    return '<a class="file-link" href="' + escAttr(href) + '" target="_blank" rel="noopener">' + esc(label) + "</a>";
  }).join("");
  const metadataItems = [
    ["Authors", row.authors || "Unknown"],
    ["Publication date", row.publicationDate || "Undated"],
    ["Organization", row.organization || "Unspecified"],
    ["Organization type", row.organizationType || "Unspecified"],
    ["Document type", row.documentType || "Unspecified"],
    ["Citation type", row.citationType || "Unspecified"],
    ["Report number", row.reportNumber || "None"],
    ["Markdown chars", row.markdownCharCount ? formatCount(row.markdownCharCount) : "0"],
    ["Source ID", row.id || ""],
    ["URL", row.exportUrl ? '<a href="' + escAttr(row.exportUrl) + '" target="_blank" rel="noopener">' + esc(row.exportUrl) + "</a>" : "Not available"],
  ];

  if (row.markdown && !row._markdownHtml) {
    row._markdownHtml = renderMarkdown(row.markdown);
  }

  detailPane.innerHTML = `
    <h2 class="detail-title">${esc(row.title)}</h2>
    <p class="detail-subtitle">${esc(row.authors || "Unknown author")}</p>
    <div class="detail-badges">${badges.map((badge) => '<span class="detail-chip">' + esc(badge) + "</span>").join("")}</div>

    <section class="detail-section">
      <h3>Metadata</h3>
      <dl class="meta-grid">${metadataItems.map(([label, value]) => renderMetaItem(label, value)).join("")}</dl>
    </section>

    <section class="detail-section">
      <h3>Exported Files</h3>
      ${fileLinks ? '<div class="file-links">' + fileLinks + "</div>" : "<p>No source files were included in this export for this row.</p>"}
    </section>

    ${renderRichTextSection("Summary", row.summary, "No summary is available for this source.")}

    ${row.markdown ? `
      <section class="detail-section">
        <h3>Full Markdown</h3>
        <details class="markdown-toggle">
          <summary>Show extracted markdown</summary>
          <div class="markdown-body">${row._markdownHtml}</div>
        </details>
      </section>
    ` : `
      <section class="detail-section">
        <h3>Full Markdown</h3>
        <p>No markdown text is available for this source.</p>
      </section>
    `}

    ${renderRichTextSection("Relevance Rationale", row.ratingRationale, "No rationale is available for this source.")}
    ${renderRichTextSection("Relevant Sections", row.relevantSections, "No relevant sections were recorded for this source.")}
  `;
}

function renderMetaItem(label, value) {
  return '<div class="meta-item"><dt>' + esc(label) + "</dt><dd>" + value + "</dd></div>";
}

function renderRichTextSection(title, text, emptyText) {
  if (!text) {
    return '<section class="detail-section"><h3>' + esc(title) + "</h3><p>" + esc(emptyText) + "</p></section>";
  }
  return '<section class="detail-section"><h3>' + esc(title) + '</h3><div class="rich-text">' + renderMarkdown(text) + "</div></section>";
}

function updateSelectAllState() {
  const checkbox = document.getElementById("selectAllVisible");
  const visibleSelectedCount = visibleRows.filter((row) => selectedIds.has(row.id)).length;
  checkbox.checked = visibleRows.length > 0 && visibleSelectedCount === visibleRows.length;
  checkbox.indeterminate = visibleSelectedCount > 0 && visibleSelectedCount < visibleRows.length;
}

function updateSortIndicators() {
  document.querySelectorAll("[data-arrow]").forEach((element) => {
    if (element.dataset.arrow === sortColumn) {
      element.textContent = sortDirection === "asc" ? "▲" : "▼";
    } else {
      element.textContent = "";
    }
  });
}

function updateStatusSummary() {
  const summary = document.getElementById("resultsSummary");
  const visibleCount = visibleRows.length;
  const totalCount = allRows.length;
  const selectedCount = selectedIds.size;
  summary.innerHTML = "<strong>" + visibleCount + "</strong> of <strong>" + totalCount + "</strong> shown · <strong>" + selectedCount + "</strong> selected";
}

function updateFolderStatus() {
__UPDATE_FOLDER_STATUS__
}

function updateButtonStates() {
__UPDATE_BUTTON_STATES__
}

function toggleSelection(sourceId, checked, shiftKey) {
  const normalizedSourceId = String(sourceId || "");
  if (!normalizedSourceId) {
    return;
  }
  if (shiftKey && lastAnchorId) {
    const orderedIds = visibleRows.map((row) => row.id);
    const startIndex = orderedIds.indexOf(lastAnchorId);
    const endIndex = orderedIds.indexOf(normalizedSourceId);
    if (startIndex >= 0 && endIndex >= 0) {
      const [from, to] = startIndex <= endIndex ? [startIndex, endIndex] : [endIndex, startIndex];
      orderedIds.slice(from, to + 1).forEach((id) => {
        if (checked) {
          selectedIds.add(id);
        } else {
          selectedIds.delete(id);
        }
      });
    } else if (checked) {
      selectedIds.add(normalizedSourceId);
    } else {
      selectedIds.delete(normalizedSourceId);
    }
  } else if (checked) {
    selectedIds.add(normalizedSourceId);
  } else {
    selectedIds.delete(normalizedSourceId);
  }
  lastAnchorId = normalizedSourceId;
  updateStatusSummary();
  updateButtonStates();
  updateSelectAllState();
}

__MODE_FUNCTIONS__

function exportSelectedRis() {
  const rows = getSelectedRows();
  if (!rows.length) {
    setExportStatus("Select at least one source before exporting RIS.", "warning");
    return;
  }
  const risText = buildRisText(rows);
  if (!risText.trim()) {
    setExportStatus("The selected rows do not contain RIS-ready citation data.", "warning");
    return;
  }
  downloadBlob(new Blob([risText], { type: "application/x-research-info-systems" }), "selected-citations.ris");
  setExportStatus("Downloaded selected-citations.ris.", "success");
}

async function runExportTask(task) {
  if (exportInFlight) {
    return;
  }
  exportInFlight = true;
  updateButtonStates();
  try {
    await task();
  } catch (error) {
    if (!isAbortError(error)) {
      setExportStatus(__RUN_EXPORT_ERROR_MESSAGE_JSON__, "error");
    }
  } finally {
    exportInFlight = false;
    updateButtonStates();
  }
}

function getSelectedRows() {
  return allRows.filter((row) => selectedIds.has(row.id));
}

function buildCsvHeaders() {
  if (Array.isArray(BUNDLE.csvHeaders) && BUNDLE.csvHeaders.length) {
    return BUNDLE.csvHeaders.map((header) => String(header || ""));
  }
  const first = allRows.find((row) => row.csvRecord && typeof row.csvRecord === "object");
  return first ? Object.keys(first.csvRecord) : [];
}

function buildCsvText(rows) {
  const headers = buildCsvHeaders();
  const lines = [headers.map(escapeCsvCell).join(",")];
  rows.forEach((row) => {
    lines.push(headers.map((header) => escapeCsvCell(row.csvRecord && row.csvRecord[header] != null ? String(row.csvRecord[header]) : "")).join(","));
  });
  return "\\ufeff" + lines.join("\\r\\n") + "\\r\\n";
}

function buildRisText(rows) {
  const entries = rows
    .map((row) => String(row.ris || "").replace(/\\s+$/g, ""))
    .filter(Boolean);
  return entries.length ? entries.join("\\r\\n") + "\\r\\n" : "";
}

function escapeCsvCell(value) {
  const text = String(value || "");
  if (!/[",\\r\\n]/.test(text)) {
    return text;
  }
  return '"' + text.replace(/"/g, '""') + '"';
}

function setExportStatus(message, tone) {
  const pill = document.getElementById("exportStatus");
  pill.textContent = message;
  pill.className = "pill";
  if (tone) {
    pill.classList.add(tone);
  }
  if (!message) {
    pill.classList.add("hidden");
  }
}

function isAbortError(error) {
  return error && (error.name === "AbortError" || error.message === "The user aborted a request.");
}

function parseNumber(value) {
  if (value === "" || value === null || value === undefined) {
    return Number.NEGATIVE_INFINITY;
  }
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : Number.NEGATIVE_INFINITY;
}

function formatCount(value) {
  const number = Number(value || 0);
  if (!Number.isFinite(number)) {
    return "0";
  }
  return new Intl.NumberFormat(undefined).format(number);
}

function formatDateTime(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value || "");
  }
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function downloadBlob(blob, filename) {
  const href = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = href;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(href), 5000);
}

class SimpleZipBuilder {
  constructor() {
    this.files = [];
    this.crcTable = createCrcTable();
  }

  addText(name, text) {
    this.addFile(name, TEXT_ENCODER.encode(String(text || "")));
  }

  addFile(name, bytes) {
    const data = bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes);
    const filename = String(name || "").replace(/^\\/+/g, "");
    if (!filename) {
      return;
    }
    this.files.push({
      filename,
      nameBytes: TEXT_ENCODER.encode(filename),
      data,
      crc: crc32(data, this.crcTable),
    });
  }

  toBlob() {
    const parts = [];
    const centralDirectory = [];
    let offset = 0;
    const utf8Flag = 0x0800;

    this.files.forEach((file) => {
      const localHeader = new Uint8Array(30 + file.nameBytes.length);
      const localView = new DataView(localHeader.buffer);
      localView.setUint32(0, 0x04034b50, true);
      localView.setUint16(4, 20, true);
      localView.setUint16(6, utf8Flag, true);
      localView.setUint16(8, 0, true);
      localView.setUint16(10, 0, true);
      localView.setUint16(12, 0, true);
      localView.setUint32(14, file.crc >>> 0, true);
      localView.setUint32(18, file.data.length, true);
      localView.setUint32(22, file.data.length, true);
      localView.setUint16(26, file.nameBytes.length, true);
      localView.setUint16(28, 0, true);
      localHeader.set(file.nameBytes, 30);
      parts.push(localHeader, file.data);

      const centralHeader = new Uint8Array(46 + file.nameBytes.length);
      const centralView = new DataView(centralHeader.buffer);
      centralView.setUint32(0, 0x02014b50, true);
      centralView.setUint16(4, 20, true);
      centralView.setUint16(6, 20, true);
      centralView.setUint16(8, utf8Flag, true);
      centralView.setUint16(10, 0, true);
      centralView.setUint16(12, 0, true);
      centralView.setUint16(14, 0, true);
      centralView.setUint32(16, file.crc >>> 0, true);
      centralView.setUint32(20, file.data.length, true);
      centralView.setUint32(24, file.data.length, true);
      centralView.setUint16(28, file.nameBytes.length, true);
      centralView.setUint16(30, 0, true);
      centralView.setUint16(32, 0, true);
      centralView.setUint16(34, 0, true);
      centralView.setUint16(36, 0, true);
      centralView.setUint32(38, 0, true);
      centralView.setUint32(42, offset, true);
      centralHeader.set(file.nameBytes, 46);
      centralDirectory.push(centralHeader);

      offset += localHeader.length + file.data.length;
    });

    const centralSize = centralDirectory.reduce((sum, part) => sum + part.length, 0);
    const endRecord = new Uint8Array(22);
    const endView = new DataView(endRecord.buffer);
    endView.setUint32(0, 0x06054b50, true);
    endView.setUint16(4, 0, true);
    endView.setUint16(6, 0, true);
    endView.setUint16(8, this.files.length, true);
    endView.setUint16(10, this.files.length, true);
    endView.setUint32(12, centralSize, true);
    endView.setUint32(16, offset, true);
    endView.setUint16(20, 0, true);

    return new Blob(parts.concat(centralDirectory, [endRecord]), { type: "application/zip" });
  }
}

function createCrcTable() {
  const table = new Uint32Array(256);
  for (let index = 0; index < 256; index += 1) {
    let value = index;
    for (let bit = 0; bit < 8; bit += 1) {
      value = (value & 1) ? (0xedb88320 ^ (value >>> 1)) : (value >>> 1);
    }
    table[index] = value >>> 0;
  }
  return table;
}

function crc32(bytes, table) {
  let crc = 0xffffffff;
  for (let index = 0; index < bytes.length; index += 1) {
    crc = table[(crc ^ bytes[index]) & 0xff] ^ (crc >>> 8);
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function renderMarkdown(text) {
  if (!text) {
    return "";
  }

  const lines = String(text).replace(/\\r\\n/g, "\\n").split("\\n");
  let html = "";
  let index = 0;
  let paragraph = [];
  let listType = "";

  function flushParagraph() {
    if (!paragraph.length) {
      return;
    }
    const content = paragraph.join("\\n").trim();
    if (content) {
      html += "<p>" + inlineMarkdown(content) + "</p>";
    }
    paragraph = [];
  }

  function closeList() {
    if (!listType) {
      return;
    }
    html += listType === "ul" ? "</ul>" : "</ol>";
    listType = "";
  }

  while (index < lines.length) {
    const line = lines[index];

    if (line.trimStart().startsWith("```")) {
      flushParagraph();
      closeList();
      index += 1;
      let code = "";
      while (index < lines.length && !lines[index].trimStart().startsWith("```")) {
        code += lines[index] + "\\n";
        index += 1;
      }
      index += 1;
      html += "<pre><code>" + escCode(code.replace(/\\n$/, "")) + "</code></pre>";
      continue;
    }

    const headingMatch = line.match(/^(#{1,6})\\s+(.+)/);
    if (headingMatch) {
      flushParagraph();
      closeList();
      const level = headingMatch[1].length;
      html += "<h" + level + ">" + inlineMarkdown(headingMatch[2].replace(/\\s*#+\\s*$/, "")) + "</h" + level + ">";
      index += 1;
      continue;
    }

    if (/^(\\*{3,}|-{3,}|_{3,})\\s*$/.test(line.trim())) {
      flushParagraph();
      closeList();
      html += "<hr>";
      index += 1;
      continue;
    }

    if (line.trimStart().startsWith(">")) {
      flushParagraph();
      closeList();
      let quote = "";
      while (index < lines.length && lines[index].trimStart().startsWith(">")) {
        quote += lines[index].replace(/^\\s*>\\s?/, "") + "\\n";
        index += 1;
      }
      html += "<blockquote><p>" + inlineMarkdown(quote.trim()) + "</p></blockquote>";
      continue;
    }

    if (line.includes("|") && index + 1 < lines.length && /^\\s*\\|?[\\s:-]+\\|/.test(lines[index + 1])) {
      flushParagraph();
      closeList();
      const headerCells = parseTableRow(line);
      index += 2;
      html += "<table><thead><tr>" + headerCells.map((cell) => "<th>" + inlineMarkdown(cell) + "</th>").join("") + "</tr></thead><tbody>";
      while (index < lines.length && lines[index].includes("|")) {
        const cells = parseTableRow(lines[index]);
        html += "<tr>" + cells.map((cell) => "<td>" + inlineMarkdown(cell) + "</td>").join("") + "</tr>";
        index += 1;
      }
      html += "</tbody></table>";
      continue;
    }

    const unorderedMatch = line.match(/^\\s*[*+-]\\s+(.*)/);
    if (unorderedMatch) {
      flushParagraph();
      if (listType !== "ul") {
        closeList();
        listType = "ul";
        html += "<ul>";
      }
      html += "<li>" + inlineMarkdown(unorderedMatch[1]) + "</li>";
      index += 1;
      continue;
    }

    const orderedMatch = line.match(/^\\s*\\d+[.)]\\s+(.*)/);
    if (orderedMatch) {
      flushParagraph();
      if (listType !== "ol") {
        closeList();
        listType = "ol";
        html += "<ol>";
      }
      html += "<li>" + inlineMarkdown(orderedMatch[1]) + "</li>";
      index += 1;
      continue;
    }

    if (!line.trim()) {
      flushParagraph();
      closeList();
      index += 1;
      continue;
    }

    paragraph.push(line);
    index += 1;
  }

  flushParagraph();
  closeList();
  return html;
}

function parseTableRow(line) {
  return line.replace(/^\\s*\\|/, "").replace(/\\|\\s*$/, "").split("|").map((cell) => cell.trim());
}

function inlineMarkdown(text) {
  let output = esc(text);
  output = output.replace(/`([^`]+)`/g, "<code>$1</code>");
  output = output.replace(/!\\[([^\\]]*)\\]\\(([^)]+)\\)/g, '<img src="$2" alt="$1">');
  output = output.replace(/\\[([^\\]]+)\\]\\(([^)]+)\\)/g, '<a href="$2" target="_blank" rel="noopener">$1</a>');
  output = output.replace(/\\*\\*([^*]+)\\*\\*/g, "<strong>$1</strong>");
  output = output.replace(/__([^_]+)__/g, "<strong>$1</strong>");
  output = output.replace(/\\*([^*]+)\\*/g, "<em>$1</em>");
  output = output.replace(/_([^_]+)_/g, "<em>$1</em>");
  output = output.replace(/  \\n/g, "<br>");
  return output;
}

function esc(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function escAttr(value) {
  return esc(value).replace(/'/g, "&#39;");
}

function escCode(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}
</script>
</body>
</html>
"""


_OFFLINE_BUTTONS = """
          <button id="chooseFolderBtn" class="button" type="button">Choose Package Folder</button>
          <button id="exportSelectedBundleBtn" class="button primary" type="button">Export Selected Bundle</button>
          <button id="exportAllBundleBtn" class="button" type="button">Export All Bundle</button>
          <button id="exportSelectedRisBtn" class="button" type="button">Export Selected RIS</button>
""".strip("\n")


_CLOUD_BUTTONS = """
          <button id="exportSelectedBundleBtn" class="button primary" type="button">Export Selected Bundle</button>
          <button id="exportAllBundleBtn" class="button" type="button">Export All Bundle</button>
          <button id="exportSelectedRisBtn" class="button" type="button">Export Selected RIS</button>
""".strip("\n")


_OFFLINE_NORMALIZE_FILE_ENTRY = """
function normalizeFileEntry(kind, value) {
  const normalizedKind = String(kind || "").trim().toLowerCase();
  if (!normalizedKind) {
    return null;
  }
  if (typeof value === "string") {
    const relativePath = String(value || "").trim();
    if (!relativePath) {
      return null;
    }
    return {
      kind: normalizedKind,
      label: inferFileLabel(normalizedKind, relativePath),
      displayName: basenameFromPath(relativePath) || (normalizedKind.toUpperCase() + " file"),
      relativePath,
      storageName: "",
    };
  }
  if (!value || typeof value !== "object") {
    return null;
  }
  const relativePath = String(value.relativePath || "").trim();
  if (!relativePath) {
    return null;
  }
  return {
    kind: normalizedKind,
    label: inferFileLabel(normalizedKind, relativePath, value.displayName),
    displayName:
      String(value.displayName || "").trim() ||
      basenameFromPath(relativePath) ||
      (normalizedKind.toUpperCase() + " file"),
    relativePath,
    storageName: "",
  };
}
""".strip("\n")


_CLOUD_NORMALIZE_FILE_ENTRY = """
function normalizeFileEntry(kind, value) {
  const normalizedKind = String(kind || "").trim().toLowerCase();
  if (!normalizedKind || !value || typeof value !== "object") {
    return null;
  }
  const storageName = String(value.storageName || "").trim();
  if (!storageName) {
    return null;
  }
  return {
    kind: normalizedKind,
    label: inferFileLabel(normalizedKind, storageName, value.displayName),
    displayName:
      String(value.displayName || "").trim() ||
      basenameFromPath(storageName) ||
      (normalizedKind.toUpperCase() + " file"),
    storageName,
  };
}
""".strip("\n")


_OFFLINE_MODE_FUNCTIONS = """
async function handleChooseFolder() {
  if (!DIRECTORY_PICKER_SUPPORTED) {
    setExportStatus("Local folder access is not supported in this browser.", "warning");
    return;
  }
  try {
    packageRootHandle = await window.showDirectoryPicker({ mode: "read" });
    updateFolderStatus();
    updateButtonStates();
    setExportStatus("Package folder selected.", "success");
  } catch (error) {
    if (isAbortError(error)) {
      return;
    }
    setExportStatus("Unable to read the selected package folder.", "error");
  }
}

async function ensurePackageRootHandle() {
  if (packageRootHandle) {
    return packageRootHandle;
  }
  packageRootHandle = await window.showDirectoryPicker({ mode: "read" });
  updateFolderStatus();
  updateButtonStates();
  return packageRootHandle;
}

async function exportBundle(scope) {
  const rows = scope === "selected" ? getSelectedRows() : allRows.slice();
  if (!rows.length) {
    setExportStatus("Select at least one source before exporting a subset.", "warning");
    return;
  }
  if (!DIRECTORY_PICKER_SUPPORTED) {
    setExportStatus("This browser cannot rebuild ZIP packages from a local export folder.", "warning");
    return;
  }

  await runExportTask(async () => {
    const rootHandle = await ensurePackageRootHandle();
    const filePaths = collectRelativeFilePaths(rows);
    const zip = new SimpleZipBuilder();
    let missingFiles = 0;

    for (const relativePath of filePaths) {
      try {
        const bytes = await readRelativeFile(rootHandle, relativePath);
        zip.addFile(relativePath, bytes);
      } catch (_error) {
        missingFiles += 1;
      }
    }

    zip.addText("research-export.csv", buildCsvText(rows));
    zip.addText("citations.ris", buildRisText(rows));
    const blob = zip.toBlob();
    const filename = scope === "selected" ? "selected-repository-export.zip" : "repository-export.zip";
    downloadBlob(blob, filename);

    const exportedFiles = filePaths.length - missingFiles;
    if (missingFiles > 0) {
      setExportStatus("Downloaded " + filename + " with " + exportedFiles + " file" + (exportedFiles === 1 ? "" : "s") + " and " + missingFiles + " missing from the chosen folder.", "warning");
    } else {
      setExportStatus("Downloaded " + filename + ".", "success");
    }
  });
}

function collectRelativeFilePaths(rows) {
  const seen = new Set();
  const relativePaths = [];
  rows.forEach((row) => {
    row.fileList.forEach((file) => {
      const relativePath = String(file.relativePath || "");
      if (!relativePath || seen.has(relativePath)) {
        return;
      }
      seen.add(relativePath);
      relativePaths.push(relativePath);
    });
  });
  relativePaths.sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base", numeric: true }));
  return relativePaths;
}

async function readRelativeFile(rootHandle, relativePath) {
  const parts = String(relativePath || "").split("/").filter(Boolean);
  if (!parts.length) {
    throw new Error("Invalid relative path");
  }
  let directoryHandle = rootHandle;
  for (let index = 0; index < parts.length - 1; index += 1) {
    directoryHandle = await directoryHandle.getDirectoryHandle(parts[index]);
  }
  const fileHandle = await directoryHandle.getFileHandle(parts[parts.length - 1]);
  const file = await fileHandle.getFile();
  return new Uint8Array(await file.arrayBuffer());
}

function fileHref(file) {
  return String(file.relativePath || "");
}
""".strip("\n")


_CLOUD_MODE_FUNCTIONS = """
async function exportBundle(scope) {
  const rows = scope === "selected" ? getSelectedRows() : allRows.slice();
  if (!rows.length) {
    setExportStatus("Select at least one source before exporting a subset.", "warning");
    return;
  }

  await runExportTask(async () => {
    const files = collectRemoteFiles(rows);
    const zip = new SimpleZipBuilder();
    let missingFiles = 0;
    const accessHint = buildRemoteExportAccessHint();

    for (const file of files) {
      try {
        const bytes = await fetchRemoteFile(file);
        zip.addFile("files/" + file.storageName, bytes);
      } catch (_error) {
        missingFiles += 1;
      }
    }

    const exportedFiles = files.length - missingFiles;
    if (files.length > 0 && exportedFiles === 0) {
      setExportStatus(
        "Bundle export could not read any files from BASE_URL. Direct file links can still open without cross-origin byte access. " + accessHint,
        "error",
      );
      return;
    }

    zip.addText("manifest.json", buildManifestText(rows, scope));
    zip.addText("research-export.csv", buildCsvText(rows));
    zip.addText("citations.ris", buildRisText(rows));

    const blob = zip.toBlob();
    const filename = scope === "selected" ? "selected-repository-cloud-export.zip" : "repository-cloud-export.zip";
    downloadBlob(blob, filename);

    if (missingFiles > 0) {
      setExportStatus(
        "Downloaded " + filename + " with " + exportedFiles + " file" + (exportedFiles === 1 ? "" : "s") + " and " + missingFiles + " unavailable from BASE_URL. " + accessHint,
        "warning",
      );
    } else {
      setExportStatus("Downloaded " + filename + ".", "success");
    }
  });
}

function collectRemoteFiles(rows) {
  const seen = new Set();
  const files = [];
  rows.forEach((row) => {
    row.fileList.forEach((file) => {
      const storageName = String(file.storageName || "");
      if (!storageName || seen.has(storageName)) {
        return;
      }
      seen.add(storageName);
      files.push(file);
    });
  });
  files.sort((left, right) =>
    String(left.storageName || "").localeCompare(String(right.storageName || ""), undefined, { sensitivity: "base", numeric: true }),
  );
  return files;
}

async function fetchRemoteFile(file) {
  const response = await fetch(buildStorageUrl(file.storageName), { mode: "cors", credentials: "omit" });
  if (!response.ok) {
    throw new Error("Unable to fetch remote file");
  }
  return new Uint8Array(await response.arrayBuffer());
}

function fileHref(file) {
  return file.storageName ? buildStorageUrl(file.storageName) : "";
}

function buildStorageUrl(storageName) {
  const baseUrl = normalizeBaseUrl(BASE_URL);
  return baseUrl + encodeURIComponent(String(storageName || ""));
}

function normalizeBaseUrl(value) {
  const trimmed = String(value || "").trim();
  if (!trimmed) {
    return "";
  }
  return trimmed.replace(/\\/+$/, "") + "/";
}

function buildRemoteExportAccessHint() {
  const baseUrl = normalizeBaseUrl(BASE_URL);
  if (!baseUrl) {
    return "Set BASE_URL to the uploaded file location and try again.";
  }
  if (window.location.protocol === "file:" && /^https?:\\/\\//i.test(baseUrl)) {
    return 'Use "./files/" for local preview or host index.html on your static site before exporting a bundle.';
  }
  const viewerOrigin = currentViewerOriginLabel();
  const storageOrigin = resolvedOriginLabel(baseUrl);
  if (viewerOrigin && storageOrigin && viewerOrigin !== storageOrigin) {
    return "Allow GET from " + viewerOrigin + " in the storage host CORS policy.";
  }
  return "Confirm the uploaded storageName files are reachable from BASE_URL.";
}

function currentViewerOriginLabel() {
  const origin = String(window.location.origin || "");
  if (!origin || origin === "null" || window.location.protocol === "file:") {
    return "";
  }
  return origin;
}

function resolvedOriginLabel(value) {
  try {
    const url = new URL(String(value || ""), window.location.href);
    return String(url.origin || "");
  } catch (_error) {
    return "";
  }
}

function buildManifestText(rows, scope) {
  return JSON.stringify(buildManifestPayload(rows, scope), null, 2);
}

function buildManifestPayload(rows, scope) {
  return {
    repositoryName: String(BUNDLE.repositoryName || "Repository Browser"),
    exportMode: "cloud",
    exportScope: scope,
    bundleFileKinds: Array.isArray(BUNDLE.bundleFileKinds) ? BUNDLE.bundleFileKinds.map((kind) => String(kind || "")) : [],
    exportedAt: new Date().toISOString(),
    csvHeaders: buildCsvHeaders(),
    rows: rows.map((row) => ({
      id: row.id,
      sourceTitle: row.sourceTitle,
      title: row.title,
      sourceAuthors: row.sourceAuthors,
      authors: row.authors,
      publicationDate: row.publicationDate,
      publicationYear: row.publicationYear,
      organization: row.organization,
      organizationType: row.organizationType,
      overallRating: row.overallRating,
      summary: row.summary,
      ratingRationale: row.ratingRationale,
      relevantSections: row.relevantSections,
      citationType: row.citationType,
      reportNumber: row.reportNumber,
      citationUrl: row.citationUrl,
      sourceUrl: row.sourceUrl,
      exportUrl: row.exportUrl,
      documentType: row.documentType,
      markdownCharCount: row.markdownCharCount,
      customFields: row.customFields.map((field) => ({
        key: field.key,
        label: field.label,
        value: field.value,
      })),
      files: Object.fromEntries(
        row.fileList.map((file) => [
          file.kind,
          {
            kind: file.kind,
            displayName: file.displayName,
            storageName: file.storageName,
          },
        ]),
      ),
      csvRecord: row.csvRecord,
      ris: row.ris,
      markdown: row.markdown,
    })),
  };
}
""".strip("\n")


def _render_viewer_html(
    *,
    payload: dict[str, Any],
    eyebrow: str,
    buttons: str,
    mode_constants: str,
    mode_state: str,
    title_suffix: str,
    export_label: str,
    normalize_file_entry: str,
    bind_mode_events: str,
    render_support_state: str,
    update_folder_status: str,
    update_button_states: str,
    mode_functions: str,
    run_export_error_message: str,
    folder_status_extra_class: str = "",
    support_notice_extra_class: str = "",
) -> str:
    repository_name = str(payload.get("repositoryName") or "Repository Browser")
    return (
        _HTML_TEMPLATE.replace("__STYLE_BLOCK__", _STYLE_BLOCK.strip("\n"))
        .replace("__EYEBROW__", eyebrow)
        .replace("__REPOSITORY_NAME__", _html_text(repository_name))
        .replace(
            "__INITIAL_SUBTITLE__",
            _html_text(_build_initial_subtitle(payload, export_label=export_label)),
        )
        .replace("__BUTTONS__", buttons)
        .replace("__FOLDER_STATUS_EXTRA_CLASS__", folder_status_extra_class)
        .replace("__SUPPORT_NOTICE_EXTRA_CLASS__", support_notice_extra_class)
        .replace("__BUNDLE_DATA__", _json_for_html(payload))
        .replace("__MODE_CONSTANTS__", mode_constants)
        .replace("__MODE_STATE__", mode_state)
        .replace("__TITLE_SUFFIX_JSON__", _js_string(title_suffix))
        .replace("__EXPORT_LABEL_JSON__", _js_string(export_label))
        .replace("__NORMALIZE_FILE_ENTRY__", normalize_file_entry)
        .replace("__BIND_MODE_EVENTS__", bind_mode_events)
        .replace("__RENDER_SUPPORT_STATE__", render_support_state)
        .replace("__UPDATE_FOLDER_STATUS__", update_folder_status)
        .replace("__UPDATE_BUTTON_STATES__", update_button_states)
        .replace("__MODE_FUNCTIONS__", mode_functions)
        .replace("__RUN_EXPORT_ERROR_MESSAGE_JSON__", _js_string(run_export_error_message))
    )


def _build_offline_viewer_html(payload: dict[str, Any]) -> str:
    return _render_viewer_html(
        payload=payload,
        eyebrow="Offline Repository Browser",
        buttons=_OFFLINE_BUTTONS,
        mode_constants='const DIRECTORY_PICKER_SUPPORTED = typeof window.showDirectoryPicker === "function";',
        mode_state="let packageRootHandle = null;",
        title_suffix=" - Offline Browser",
        export_label="offline export",
        normalize_file_entry=_OFFLINE_NORMALIZE_FILE_ENTRY,
        bind_mode_events='  document.getElementById("chooseFolderBtn").addEventListener("click", handleChooseFolder);\n',
        render_support_state="""
  if (DIRECTORY_PICKER_SUPPORTED) {
    supportNotice.classList.add("hidden");
  } else {
    supportNotice.textContent = "Bundle re-export requires a Chromium-based browser with local folder access. Browsing and RIS export still work.";
    supportNotice.classList.remove("hidden");
  }
""".rstrip("\n"),
        update_folder_status="""
  const folderStatus = document.getElementById("folderStatus");
  if (!DIRECTORY_PICKER_SUPPORTED) {
    folderStatus.textContent = "Local folder access unavailable";
    return;
  }
  folderStatus.textContent = packageRootHandle ? "Package folder linked for re-export" : "Package folder will be requested on first ZIP export";
""".rstrip("\n"),
        update_button_states="""
  const selectedCount = selectedIds.size;
  const zipDisabled = exportInFlight || !DIRECTORY_PICKER_SUPPORTED;
  document.getElementById("chooseFolderBtn").disabled = exportInFlight || !DIRECTORY_PICKER_SUPPORTED;
  document.getElementById("exportSelectedBundleBtn").disabled = zipDisabled || selectedCount === 0;
  document.getElementById("exportAllBundleBtn").disabled = zipDisabled || allRows.length === 0;
  document.getElementById("exportSelectedRisBtn").disabled = exportInFlight || selectedCount === 0;
""".rstrip("\n"),
        mode_functions=_OFFLINE_MODE_FUNCTIONS,
        run_export_error_message="Export failed. Choose the extracted package root and try again.",
    )


def _build_cloud_viewer_html(payload: dict[str, Any], *, base_url: str) -> str:
    return _render_viewer_html(
        payload=payload,
        eyebrow="Cloud Repository Browser",
        buttons=_CLOUD_BUTTONS,
        mode_constants=(
            '// Change this one line after upload. Use "./files/" for local preview.\n'
            f"const BASE_URL = {_js_string(base_url)};"
        ),
        mode_state="",
        title_suffix=" - Cloud Browser",
        export_label="cloud export",
        normalize_file_entry=_CLOUD_NORMALIZE_FILE_ENTRY,
        bind_mode_events="",
        render_support_state="""
  supportNotice.textContent = "";
  supportNotice.classList.add("hidden");
""".rstrip("\n"),
        update_folder_status="""
  const folderStatus = document.getElementById("folderStatus");
  folderStatus.textContent = "";
  folderStatus.classList.add("hidden");
""".rstrip("\n"),
        update_button_states="""
  const selectedCount = selectedIds.size;
  document.getElementById("exportSelectedBundleBtn").disabled = exportInFlight || selectedCount === 0;
  document.getElementById("exportAllBundleBtn").disabled = exportInFlight || allRows.length === 0;
  document.getElementById("exportSelectedRisBtn").disabled = exportInFlight || selectedCount === 0;
""".rstrip("\n"),
        mode_functions=_CLOUD_MODE_FUNCTIONS,
        run_export_error_message="Export failed. Verify BASE_URL and confirm the storage host allows cross-origin file fetches.",
        folder_status_extra_class=" hidden",
        support_notice_extra_class=" hidden",
    )


def build_repository_bundle_viewer_html(payload: dict[str, Any], *, base_url: str = "") -> str:
    mode = str(payload.get("exportMode") or "offline").strip().lower() or "offline"
    if mode == "cloud":
        return _build_cloud_viewer_html(payload, base_url=base_url)
    return _build_offline_viewer_html(payload)
