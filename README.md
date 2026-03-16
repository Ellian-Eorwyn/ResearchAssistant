# ResearchAssistant

ResearchAssistant is a local-first web application for citation extraction and source capture.

It helps you go from research documents to structured citation data, then optionally download and package cited source URLs (PDF/HTML/document pages) into a reproducible local dataset.

## What The Application Can Do

### 1) Extract citation evidence from uploaded documents

Supported upload types:
- `.pdf`
- `.docx`
- `.md`

Pipeline behavior:
- Ingests one or more documents in a single job.
- Detects references/bibliography sections using heading and pattern heuristics.
- Parses bibliography entries into structured fields (authors, title, year, DOI, URL, etc.).
- Detects in-text numeric citations (for example `[1]`, `[2, 3]`, `[4-7]`).
- Extracts citing sentences with nearby context.
- Matches citations to bibliography entries by reference number.
- Exports a consolidated `citations.csv`.

### 2) Import source URL spreadsheets and run source capture

You can upload a source list as CSV/XLSX and merge it into a job (with or without full extraction from documents).

Source capture workflow can:
- Normalize and deduplicate URLs.
- Download original source content.
- Attempt markdown extraction for PDFs, HTML, and supported document formats.
- Save per-source metadata and status.
- Generate manifest files (`manifest.csv`, `manifest.xlsx`).
- Produce a ZIP bundle (`output_run.zip`) for easy sharing.
- Re-run failed URLs only.
- Cancel an active source download run.

### 3) Generate optional LLM-assisted outputs

With an LLM backend configured, the app can:
- Generate per-source summaries from extracted markdown.
- Use optional vision OCR fallback for low-quality PDF pages during conversion.

LLM usage is optional; the core extraction pipeline works without it.

### 4) Maintain an attached repository for incremental research

Attached Repository mode is for persistent, incremental source collection.

It can:
- Attach an absolute local folder path and scan existing outputs.
- Import new URLs from spreadsheets.
- Import references/URLs from research documents (`.pdf`, `.docx`, `.md`).
- Skip duplicates using normalized URL dedupe keys.
- Continue source IDs from the highest existing numeric ID.
- Download only queued URLs.
- Rebuild repository-wide `manifest.csv`, `manifest.xlsx`, and `citations.csv`.
- Preserve existing files (no renaming/moving of prior content).

## How Users Can Use It

### 1) Setup (dedicated virtualenv)

```bash
./scripts/bootstrap_venv.sh
```

This command:
- creates `.venv` if missing,
- upgrades pip/setuptools/wheel,
- installs `requirements.txt`,
- installs Playwright Chromium.

Re-running is safe.

### 2) Run the app

```bash
./scripts/run_dev.sh
```

The app launches on `http://127.0.0.1:8000` and opens your browser automatically.

### 3) Standard workflow (document extraction)

1. Upload one or more research files in **Upload Documents**.
2. Optional: upload a CSV/XLSX source list in **Upload Source URL Spreadsheet**.
3. Optional: configure LLM backend in **LLM Backend Settings** and save.
4. Click **Extract Citations**.
5. Watch stage-by-stage progress.
6. Review results in Bibliography/Citations/Sentences/Matches tabs.
7. Download `citations.csv` or `wikiclaude_export.db` from the Export panel.
8. Optional: click **Download Sources** to build source manifests and output bundle.

### 4) Attached Repository workflow (incremental mode)

1. Enter an absolute folder path in **Attached Repository** and click **Attach + Scan**.
2. Import URLs from a spreadsheet or references from a document.
3. Click **Download Queued Sources**.
4. Use **Manifest CSV**, **Manifest XLSX**, and **Citations CSV** buttons to export repository-level files.
5. Use **Rebuild Spreadsheets** after manual edits or merges.

## Source Spreadsheet Format

Accepted file types:
- `.csv`
- `.xlsx`

Required column:
- Any URL-equivalent header, including `URL`, `SourceURL`, `OriginalURL`, `FinalURL`, `Link`, `URI`, `Website`.

Optional columns (auto-detected by common names):
- reference number (`RefNumber`, `CitationNumber`, etc.)
- source document name
- title
- authors
- year
- DOI
- raw text/entry text

Rows without a usable URL are skipped.

## Output Files And Data Layout

Per job data is stored under `data/`:
- `data/uploads/<job_id>/` uploaded input files
- `data/artifacts/<job_id>/` stage artifacts and status
- `data/exports/<job_id>/citations.csv` export table
- `data/exports/<job_id>/wikiclaude_export.db` SQLite export compatible with Wiki-Claude schema
- `data/exports/<job_id>/output_run/` source capture outputs

Typical `output_run/` contents:
- `manifest.csv`
- `manifest.xlsx`
- `originals/`
- `rendered/`
- `markdown/`
- `summaries/` (if LLM summaries enabled)
- `metadata/`
- `logs/`
- `output_run.zip` (at `data/exports/<job_id>/output_run.zip`)

Attached repository mode writes repository-level outputs at the attached folder root:
- `manifest.csv`
- `manifest.xlsx`
- `citations.csv`
- `sources/<source_id>/...` captured files per source
- `.ra_repo/` internal state, lock, and backup snapshots

## Troubleshooting

Re-run bootstrap:

```bash
./scripts/bootstrap_venv.sh
```

Targeted fixes:

```bash
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m playwright install chromium
```

If source extraction quality is limited for some URLs, the app may show runtime guidance in the UI (for optional local tools like Playwright browser binaries, `textutil`, or `tesseract`).

## Run Tests

```bash
.venv/bin/python -m unittest discover -s tests
```
