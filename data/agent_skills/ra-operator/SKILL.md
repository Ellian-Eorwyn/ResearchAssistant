---
name: ra-operator
description: Operate a ResearchAssistant repository after it is set up - run the whole analysis in one command, and make after-the-fact data fixes. Use when the user asks to run everything / re-run the columns, change a single cell value, mark a source resolved or send it back to Resolve Fetches, edit a column's prompt, turn the model on, change the research prompt or project profile, or re-run image extraction or fetch signals. For first-time setup from a spreadsheet, use the ra-workflow skill instead.
license: MIT
allowed-tools: Bash(.agents/bin/ra *)
---

# Operate the repository

Every task here is **one command**. The command does the work, prints a summary,
then `--- json ---`, then JSON whose `next` field tells you what to do next and
whether you may do it yet. You never read code or edit files.

**The one rule:** start with `.agents/bin/ra doctor`. It checks the repository is
attached, the model is on, and the backend answers, and every problem it reports
comes with a `remedy` you can run. Then follow `next`. Each `next` entry has a
`gate`:

- `gate: "go"` — run it now.
- `gate: "ask_user"` — **stop, tell the user the `why`, wait for their answer,**
  then run it. Printed as `ASK THE USER FIRST`. Anything that spends model calls
  or writes to the repository is gated.

Every command is safe to run twice. If you are ever unsure of the state, run
`.agents/bin/ra where` — it reads the live repository and tells you what to do.

**Never** edit `.ra_repo/repository_state.json`, `manifest.csv`, `manifest.xlsx`,
or anything under `sources/` by hand — there is a command for each thing, and
hand-edits are erased when files regenerate.

## How the repository is shaped

- A **source** is one document (a web page, PDF, or video). It has a **fetch
  status** (`success`, `partial`, `blocked`, `failed`, `queued`) and captured
  files on disk. `partial`/`blocked`/`failed` sources appear on the **Resolve
  Fetches** screen for you to re-fetch or replace.
- A **column** is one question asked of every source. Each column has a **prompt**,
  a **scope** (`none` = source text only, `fetch_metadata` = text + safe fetch
  facts like the URL and date signals, `all` = text + all row data), and optional
  **allowed values**. Running a column fills one **cell** per source — one model
  call each.
- A **project profile** scores every source for relevance; the **research prompt**
  is injected into every column and the profile. A **backend** (a local or remote
  model) does the model calls, gated by an app-wide **use-llm** switch.
- The pipeline order matters: fetch → convert (text) → cleanup → **fetch signals
  (`date_signals`)** and **images** → columns. Some columns read the signals or
  image results, so those must exist first. `ra full-run` does this order for you.

## Recipes

**Run (or re-run) the whole analysis** — after editing prompts, the profile, or
the research prompt, or on a fresh repository once sources are fetched:

```bash
.agents/bin/ra full-run
```

It refreshes fetch signals, analyses images, then runs every column over every
source (overwriting old values). Add `--ids 000012,000034` to re-run only some
sources. Gated — confirm the spend with the user first.

**Run only the columns** (e.g. only fill empty cells, or overwrite all):

```bash
.agents/bin/ra run-columns                     # fill empty cells only
.agents/bin/ra run-columns --confirm-overwrite # recode every source
```

**Change one cell value** — e.g. a citation the model declined, or a hand-checked
correction. First without `--apply` to preview, then run the exact `--apply`
command it prints:

```bash
.agents/bin/ra set-cell "Year Published" 000052 2025
.agents/bin/ra set-cell "Year Published" 000052 2025 --overwrite --apply
```

Name the column by label or id (`custom_...`). A value outside the column's
allowed answers is flagged in the preview but still stored.

**Mark a source resolved, or send it back to Resolve Fetches:**

```bash
.agents/bin/ra set-fetch-status 000029,000060 success          # resolved
.agents/bin/ra set-fetch-status 000048 partial --reason "PDF added; re-run"
```

**Add the real file a link pointed to, then re-code that source** (e.g. a page
that was actually a PDF): attach it, then full-run just that source. full-run
handles the stale-text cleanup for you.

```bash
.agents/bin/ra attach /path/to/file.pdf --source-id 000048 --role raw_file --overwrite --apply
.agents/bin/ra full-run --ids 000048
```

**Edit a column's prompt / scope / allowed values** (then re-code it):

```bash
.agents/bin/ra edit-column "Sector" --prompt-file new_prompt.txt --scope none --allowed "residential,commercial,not sure" --fallback "not sure"
.agents/bin/ra edit-column "VPP Definitions" --create --prompt-file vpp_prompt.txt --scope none
.agents/bin/ra run-columns --confirm-overwrite
```

**Re-run one step on its own:**

```bash
.agents/bin/ra refresh-metadata --scope all   # regenerate fetch signals from saved files, no re-download
.agents/bin/ra images --scope all             # extract + classify + describe page images
```

**Turn the model on / see the backend:**

```bash
.agents/bin/ra config --show
.agents/bin/ra config --use-llm on
```

**Change the research prompt or the active project profile:**

```bash
.agents/bin/ra set-purpose --file research_prompt.txt   # or --show
.agents/bin/ra profile --list
.agents/bin/ra profile --set yolovpp.yaml
```

## If a command fails

Exit codes: `1` blockers or failures, `2` used wrongly, `3` the server needs
restarting, `4` the app is not running. Report the `problems` and their `remedy`
rather than improvising.
