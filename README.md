# ResearchAssistant

ResearchAssistant is a local-first research workspace for building and maintaining a structured source repository.

It combines:
- source ingestion and download
- markdown conversion and cleanup
- repository-wide metadata and citation management
- a spreadsheet-style browser for reviewing and editing sources
- LLM-powered column processing for classification, extraction, normalization, and enrichment

Everything is designed around a persistent on-disk repository that you control.

## What It Can Do

- Create or attach a local research repository.
- Import source lists from `.csv` and `.xlsx`.
- Import local files such as `.pdf`, `.doc`, `.docx`, `.html`, `.md`, `.rtf`, and `.txt`.
- Download source content and convert it into repository files and markdown.
- Optionally clean markdown with an LLM.
- Generate and update display metadata such as title, authors, publication year, document type, organization, tags, notes, summaries, and ratings.
- Verify citation metadata for RIS export.
- Browse the repository in a spreadsheet view with filtering, sorting, selection, inline metadata editing, and source-detail inspection.
- Add custom columns that persist with the repository.
- Save per-column instructions and run LLM processing across the whole dataset, blank rows, or selected rows.
- Export repository data as spreadsheet files (`.csv` or `.xlsx`) or export citations as RIS.

## Core Concepts

### Repository-first workflow

ResearchAssistant is centered on a repository folder on disk. That folder becomes the working dataset for a project and stores:

- repository manifests
- per-source files
- per-source metadata artifacts
- citation data
- per-column configuration
- repository-scoped settings

### Spreadsheet browser

The Browser view is the main workspace. It gives you:

- an ingestion and context panel for adding links/files and choosing the active project profile
- an AI enrichment panel for running repository-wide processing tasks
- a spreadsheet for filtering, sorting, selecting, exporting, and reviewing rows
- a source details panel for editing metadata and checking file/status details
- per-column controls for instructions, running LLM jobs, renaming custom columns, and adding new columns

### Optional LLM features

The app works without an LLM for repository management, downloading, conversion, and manual review.

When an LLM backend is configured, ResearchAssistant can also:

- clean extracted markdown
- improve column prompts with `Fix Up Prompt`
- run built-in metadata/citation column generation
- run custom analytical or extraction columns across repository rows

Custom column processing can be configured to use:

- the primary source text
- relevant row metadata from the same row

By default, primary source text is included and row metadata is not.

## Install From GitHub Source

### Prerequisites

- Python 3
- Node.js and `npm`
- macOS, Linux, or another environment that can run Python, Node, and Playwright Chromium

### 1. Clone the repository

```bash
git clone https://github.com/Ellian-Eorwyn/ResearchAssistantLLM
cd ResearchAssistantLLM
```

### 2. Set up Python dependencies and Playwright

```bash
./scripts/bootstrap_venv.sh
```

This script:

- creates `.venv` if needed
- installs `requirements.txt`
- installs Playwright Chromium

### 3. Install frontend dependencies

```bash
cd frontend
npm install
cd ..
```

## Run The App

### One-command local run

```bash
./scripts/run_dev.sh
```

This will:

- ensure the virtual environment exists
- install frontend dependencies if needed
- build the frontend
- start the FastAPI server
- open the app in your browser at [http://127.0.0.1:7995](http://127.0.0.1:7995)

### Hot-reload development mode

Backend:

```bash
.venv/bin/python run.py
```

Frontend:

```bash
cd frontend
npm run dev
```

Then open the Vite URL shown in the terminal. The frontend proxies API calls to the local backend.

## How To Use It

### 1. Open or create a repository

Start by creating a new repository folder or attaching an existing one in the app.

Each repository stores its own:

- source records
- files
- settings
- project profile selection
- column configuration

### 2. Add sources

Use the **Ingestion and Context** area to:

- add link spreadsheets
- add local files
- download all repository sources
- optionally run automatic LLM markdown cleanup after download
- select the active project profile

### 3. Run repository enrichment

Use the **AI Enrichment Panel** to run repository-wide tasks such as:

- markdown cleanup
- catalog metadata generation
- citation verification
- title resolution
- summaries
- ratings

You can scope runs to the full repository, selected rows, or empty spaces only.

### 4. Work in the spreadsheet browser

The Browser view supports:

- type-aware sorting
- free-text and structured filtering
- row selection
- column resizing
- a resizable split between the sheet and source details
- always-visible horizontal scrolling
- inline viewing of the current repository page

The source details panel shows:

- source status
- available files
- display metadata
- citation metadata
- summaries
- ratings

### 5. Add and run custom columns

Every processable column has an interaction area above the table with:

- instructions
- run
- rename for custom columns

You can:

1. Click `+` to add a custom column.
2. Rename the column in the header.
3. Open `Instructions`.
4. Save a prompt for that column.
5. Optionally use `Fix Up Prompt`.
6. Choose whether the run should use source text, row metadata, or both.
7. Run the column on the whole dataset, blank rows, or selected rows.

This supports workflows like:

- yes/no classification
- document tagging
- title normalization
- date cleanup
- short extraction columns
- research-specific custom flags

### 6. Export results

The Browser export area supports:

- **Spreadsheet export**
  - `.csv`
  - `.xlsx`
  - whole database, selected rows, or currently displayed rows
  - all columns or only currently visible columns
- **RIS export**
  - citation records only
  - whole database, selected rows, or currently displayed rows

## Repository Layout

An attached repository typically contains:

```text
your-repo/
  manifest.csv
  manifest.xlsx
  citations.csv
  sources/
    000001/
    000002/
    ...
  project_profiles/
  .ra_repo/
    repository.json
    repository_state.json
    settings.json
    jobs/
    backups/
```

`sources/<source_id>/` contains the per-source artifacts generated by ingestion, download, conversion, cleanup, cataloging, citation verification, summaries, and ratings.

## LLM Setup

LLM support is configured per repository.

Current backend configuration supports:

- Ollama-style local backends
- OpenAI-style backends

Typical uses for the LLM in this app:

- cleaning markdown
- improving prompts
- generating column values
- enriching metadata and citation fields

If no LLM is configured, the repository browser and manual workflows still work.

## Testing

Backend:

```bash
PYTHONPATH=. pytest -q
```

Frontend:

```bash
cd frontend
npm test
npm run build
```

## Tech Stack

- FastAPI backend
- React + Vite frontend
- Pydantic models
- Playwright Chromium for web capture support
- OpenPyXL for spreadsheet export

## Notes

- The app is local-first. Repository data is stored on disk, not in a hosted service.
- Repository settings are scoped to the repository itself.
- Exported spreadsheet files can be simplified to only the currently visible columns in the Browser.
