# ResearchAssistant Overview (Technical Teammate)

## Summary

ResearchAssistant is a local-first FastAPI + static frontend application for turning research documents into structured citation data, then optionally collecting and packaging cited sources into reproducible local artifacts.

Supported document ingestion includes `.pdf`, `.docx`, and `.md`. The core extraction flow parses references/bibliography sections, detects in-text numeric citations, extracts citing sentence/paragraph context, matches citations to bibliography entries, and exports results as `citations.csv` plus optional `wikiclaude_export.db` (SQLite).

## Key Interfaces And User Interaction

Users run the app locally via `./scripts/run_dev.sh`, which launches the FastAPI app and opens the browser. Runtime entry point in `run.py` is currently `http://127.0.0.1:7995`.

### Main UI workflow

1. Upload one or more documents and click **Extract Citations**.
2. Optionally upload a CSV/XLSX source URL spreadsheet to seed or merge source capture inputs.
3. Optionally configure LLM backend settings (Ollama or OpenAI-compatible) for cleanup/summarization/rating tasks.
4. Monitor stage progress and warnings/errors during processing.
5. Review outputs in **Bibliography**, **Citations**, **Sentences**, and **Matches** tabs.
6. Download exports (`citations.csv`, SQLite), then run source-capture tasks (download, LLM cleanup, LLM summaries, optional rating) with rerun/cancel controls.

### Attached Repository workflow (incremental mode)

1. Attach and scan a local repository path.
2. Import URLs from spreadsheet or from a research document.
3. Download queued sources only.
4. Rebuild repository-level manifests/citations exports.
5. Optionally merge two repositories (`new` output directory or merge into primary).

## What The App Provides

- Structured citation intelligence from unstructured documents.
- Consolidated citation datasets for downstream analysis (`citations.csv`, SQLite export).
- Source acquisition and packaging artifacts: `manifest.csv`, `manifest.xlsx`, raw/rendered/markdown outputs, optional summaries/ratings, and ZIP bundle output.
- Incremental repository operations with dedupe, source ID continuity, and preservation of existing files.

## Validation Scenarios

Use these checks to validate this overview against the codebase:

1. README workflow and output descriptions in `README.md`.
2. Frontend controls and user flows in `frontend/index.html` and `frontend/app.js`.
3. API capabilities in routers (`upload`, `pipeline`, `results`, `sources`, `settings`, `repository`).
4. Runtime startup host/port behavior in `run.py`.

## Assumptions

- Audience is technical teammates (onboarding/implementation context).
- Detail level is intentionally detailed rather than marketing-style concise copy.
- This is an overview document, not a full API reference or architecture deep dive.
