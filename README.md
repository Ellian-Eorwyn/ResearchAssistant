# ResearchAssistantLLM

Local-first citation extraction and source-capture workflow.

## Setup (Dedicated Virtualenv)

Use the project-local `.venv` to keep dependencies isolated from your global Python/Conda environment.

```bash
./scripts/bootstrap_venv.sh
```

This command:
- creates `.venv` if missing,
- upgrades pip/setuptools/wheel inside `.venv`,
- installs `requirements.txt`,
- installs Playwright Chromium.

Re-running the script is safe and idempotent.

## Run

```bash
./scripts/run_dev.sh
```

The run wrapper always launches with `.venv/bin/python`. If `.venv` is missing, it bootstraps automatically first.

## Attached Repository Mode

The UI supports attaching a persistent local repository folder for incremental
source expansion:

- attach and scan an absolute local folder path,
- import URLs from CSV/XLSX spreadsheets,
- import URLs from research documents (PDF/DOCX/MD references + inline citation links),
- skip duplicate URLs using normalized dedupe keys,
- continue source IDs from the highest existing numeric ID,
- rebuild repository-wide `manifest.csv` / `manifest.xlsx` and `citations.csv`.

## Troubleshooting

Run these from the project root:

```bash
./scripts/bootstrap_venv.sh
```

Or targeted fixes:

```bash
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python -m playwright install chromium
```

Once the app runs inside `.venv`, global package conflicts (for example `transformers` vs `huggingface-hub`) are isolated from this project.
