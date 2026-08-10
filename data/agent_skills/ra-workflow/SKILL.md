---
name: ra-workflow
description: Run a ResearchAssistant repository end to end - read a planning spreadsheet of URLs and prompts, create the sources and columns, fetch the documents, deal with whatever failed, and run the analysis. Use whenever the user points at a spreadsheet of sources, asks to fetch or re-fetch documents, asks what state the repository is in, or asks to run a column's prompt over the sources.
license: MIT
allowed-tools: Bash(.agents/bin/ra *)
---

# Run the repository workflow

Every step is one command. The command does the work and tells you what to do
next. You never need to write code, build a request, parse a spreadsheet, poll a
job, or decide which error is worth retrying.

**Every command prints a summary, then `--- json ---`, then JSON. Read the
`next` field — it tells you what to do next, and whether you may do it yet.**

Each entry in `next` carries a `gate`:

- `gate: "go"` — run it now.
- `gate: "ask_user"` — **stop.** Tell the user the command's `why`, wait for
  their answer, and only then run it. The printed form says
  `ASK THE USER FIRST` above the command.

Never run an `ask_user` entry because it is the obvious next step. It is gated
precisely because it downloads for a long time, spends model calls, or writes.

All commands are safe to run twice. If you lose track, run `.agents/bin/ra where`
— it always tells you the truth about the repository and what to do next.

## Never do these

1. Never edit `.ra_repo/repository_state.json`.
2. Never edit `manifest.csv`, `manifest.xlsx`, or `citations.csv` — they are
   regenerated and your edits are erased.
3. Never add, rename or delete anything under `sources/` yourself.
4. Never start a fetch or a column run without asking the user first.
5. Never retype a prompt from the spreadsheet — the commands carry it exactly.

## Step 0 — Check the repository is ready

```bash
.agents/bin/ra doctor
```

Read `ok_to_fetch` and `ok_to_run_columns` separately. Fetching needs the app
and the repository; running columns also needs a working model. If
`ok_to_run_columns` is false, you can still fetch — say so rather than stopping.

Report any `problems` and their `remedy` to the user verbatim.

## Step 1 — Read the spreadsheet

```bash
.agents/bin/ra plan-sheet <path-to-spreadsheet>
```

This finds the header row, the prompts row and the data rows, repairs damaged
characters, and lists the sources and columns it found.

Tell the user the summary and **every anomaly**. Common ones:

- `column_without_prompt` — that column will not be created. Normal.
- `mojibake_repaired` — damaged characters were fixed. Mention that the original
  file still contains them.
- `document_row` — that row's URL cell names a document rather than an address,
  so nothing will be fetched for it. It keeps its id and is left out of
  `create-sources`; the user supplies the file and you attach it (see below).
- `duplicate_url` — several rows share a URL. Read the next section before
  running `create-sources`, because it will refuse the whole request otherwise.
- `duplicate_source_id` — the sheet needs a human. Stop and ask.

If the header or prompts row is wrong, re-run with `--header-row N` or
`--prompts-row N` (0-based).

### When rows share a URL

`create-sources` refuses a request containing the same URL twice, and one such
pair blocks every other row with it. Ask the user which they want:

- **One source per unique URL** — re-run with `--merge-duplicate-urls`. Rows
  sharing a URL collapse into the one with the lowest id, which records the ids
  it now stands for in a `Merged ID#s` column.
- **One source per row** — they fix the sheet. Repeats of a URL are usually
  deliberate (the same page found through different channels), so do not assume
  merging is what they want; the merged source keeps only one row's own data
  unless that data is in a provided column, where it is combined.

### Columns of provided data

A column with a heading and data but **no prompt** holds the user's own work —
a collection date, the channel a link came from. `plan-sheet` lists these
separately and they are imported rather than run. Step 3a fills them.

### Documents to attach

Rows reported as `document_row` have an id but nothing to fetch. Ask the user
for the files, have them save each one named for its id (`ID#109.pdf`), and use
the `repo-attach-files` skill. Attaching creates the source **with that id**, so
do this at any point — before or after `create-sources` — and the numbering
still matches the sheet.

## Step 2 — Create the sources

```bash
.agents/bin/ra create-sources          # shows what would happen
.agents/bin/ra create-sources --apply  # does it
```

Always run it without `--apply` first and show the user the summary. Only add
`--apply` after they agree.

## Step 3 — Create the columns

```bash
.agents/bin/ra create-columns
.agents/bin/ra create-columns --apply
```

Same pattern. The prompts are carried across exactly as written; do not edit
them, because they usually specify an exact list of allowed answers.

Where a prompt lists its answers literally, the dry run shows them as
`answers restricted to [...]` and the column will only ever store one of them —
anything else becomes the prompt's own fallback. **Show the user that list** and
check it matches what they intended; it is read from their prompt, so a prompt
that formats its list unusually may yield nothing. A
`column_without_allowed_values` warning names the columns left unconstrained,
which is normal for free-text columns like a citation or a year.

For columns that already exist from an earlier run:

```bash
.agents/bin/ra set-constraints
.agents/bin/ra set-constraints --apply
```

## Step 3a — Import the columns of provided data

Only if `plan-sheet` reported any. These cost no model calls, so they are worth
doing before the fetch.

```bash
.agents/bin/ra set-values
.agents/bin/ra set-values --apply
```

Same pattern: dry run, show the user, then `--apply`. It writes one column at a
time and reports each separately. Cells that already hold a different value are
left alone; add `--overwrite` only if the user says to replace them.

## Step 4 — Ask, then fetch

Tell the user how many sources will be downloaded and that it takes a while.
**Wait for them to answer.** Then:

```bash
.agents/bin/ra fetch --wait
```

If it comes back with `terminal: false`, it is still running. Say so and use the
command in `next` to check again.

To have the local model tidy each page's text as it arrives, add the `cleanup`
phase. It is one model call per source on top of the download, so say what that
costs before offering it:

```bash
.agents/bin/ra fetch --phases fetch,convert,cleanup --wait
```

## Step 5 — Deal with what failed

```bash
.agents/bin/ra triage
```

Failures are grouped, each with an explanation and an exact remedy command.
Show the user the groups, then act on them:

- **retryable** — offer to run `.agents/bin/ra retry --wait`.
- **needs_manual_document** — the pipeline already tried a headless browser and
  was refused. Ask the user to download those pages by hand, put them anywhere in
  the repository, and then use the `repo-attach-files` skill. Do not try to work
  around the block.
- **broken_url** — the address is wrong. Show it and ask.

## Step 6 — Ask, then run the columns

Tell the user how many columns and how many sources, and that each cell is one
model call. **Wait for them to answer.**

Offer these three, and say you recommend the first:

1. **One column first** — run the column `next` names, alone, and show them the
   values it produced. `next` already prefers a column whose prompt lists exact
   allowed values, because a misread prompt shows up after one column instead of
   all of them.
2. All columns, one after another, reporting after each.
3. Only the columns they name.

```bash
.agents/bin/ra columns                      # list ids and labels
.agents/bin/ra run-column <id> --wait
```

**Report after every column before starting the next.** Give them:

- `succeeded_rows` and `failed_rows` out of `total_rows`
- any `row_errors` — and if `row_errors_truncated` is true, say how many are not
  shown
- three or four of the actual values produced, so they can see whether the
  answers match what they intended

If the command reports `confirmation_required`, the run would overwrite existing
values. Ask the user, then use the command in `next`.

If the run reports rows that `stored the column's fallback`, say so and show a
few — the cells hold a valid-looking value the model did not actually choose,
either because it declined or because it answered outside the allowed list.

If `ra where` reports values `computed from text that has since been rebuilt`,
those cells are wrong and `--scope empty_only` will not revisit them. The exact
re-run command is in `next`.

To change a prompt and redo a column, edit it in the app, then re-run that column
with `--scope all --confirm-overwrite`.

## If you get lost

```bash
.agents/bin/ra where
```

## If a command fails

The exit code says what happened: `1` blockers or failures, `2` you used the
command wrongly, `3` the server needs restarting, `4` the app is not running.
Report the `problems` and their `remedy` rather than trying something else.
