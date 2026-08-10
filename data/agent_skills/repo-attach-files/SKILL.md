---
name: repo-attach-files
description: Attach documents the user downloaded by hand to ResearchAssistant sources. Use when a fetch failed and the user has saved the page or PDF themselves, when files need registering against existing sources, or when a folder of documents should become new sources. Stores each file in the repository's own layout, rebuilds the source's text from it, and clears the failed fetch.
license: MIT
allowed-tools: Bash(.agents/bin/ra *)
---

# Attach hand-downloaded documents

When a site refuses the fetcher, the fix is for the user to save the page
themselves. This registers those files properly: into the source's own folder
with the right name, with the row updated, and — importantly — **the source's
text rebuilt from the new file**, so later analysis reads the real document
instead of the error page the failed fetch left behind.

**Every command prints a summary, then `--- json ---`, then JSON. Read the
`next` field — it tells you what to do next, and whether you may do it yet.**
An entry with `gate: "go"` you run now; one with `gate: "ask_user"` prints
`ASK THE USER FIRST`, so tell the user its `why` and wait for their answer.

## Never do these

1. Never copy files into `sources/` yourself — the app will not see them.
2. Never edit `.ra_repo/repository_state.json`, `manifest.csv`, or `*_metadata.json`.

## Where to put the files

Anywhere inside the repository. A folder of your own is fine. Files outside the
repository are refused.

## Attach them

```bash
.agents/bin/ra attach path/to/document.pdf            # shows what would happen
.agents/bin/ra attach path/to/document.pdf --apply    # does it
```

Run it without `--apply` first and show the user the summary. With no paths at
all it picks up everything in `.ra_repo/inbox/`.

Files are **moved**, not copied, so a successful run empties the source folder.
The source's text is rebuilt automatically afterwards.

## If it refuses

Report the `problems` and their remedies. The ones you will actually meet:

| problem | what to do |
|---|---|
| `ambiguous_slot` | The file type could be several things. Ask the user which, then add `--role`, for example `--role raw_file`. |
| `slot_occupied` | That source already has this kind of file and the content differs. Show both, ask, then add `--overwrite`. |
| `path_outside_repository` | Ask the user to move the file inside the repository. |
| `unknown_source_id` | The id given does not exist. Run `.agents/bin/ra where` and show them what does. |
| `filename_id_not_found` | The `000123_` prefix names a source that is not there. That prefix is written by the app itself, so the file came from this repository and the source really should exist — unlike an `ID#123` name, which creates the source. |
| `id_claimed_twice` | Two files name the same id. Rename one, or give each an explicit `--source-id`. |

## Let the filename do the work

The quickest loop, and the one to teach the user: **name each file for the id it
belongs to** — `ID#109.pdf`, `ID-109.html`, `id 109 report.pdf` — drop them all
in one folder inside the repository, and attach the folder in a single command.

```bash
.agents/bin/ra attach user-files/import --apply
```

Each file goes to the source that id names. If no such source exists yet, one is
**created with that id**, which is how a planning sheet's `document_row` entries
get their documents without any renumbering afterwards.

The `id` is required in the name. `2024-report.pdf` is treated as an ordinary
filename, not as a request for source 2024.

## Naming a target explicitly

The file usually finds its own source: identical content is skipped, an
`ID#42` or `000042_` filename names a source, and anything unmatched becomes a
new source. To be explicit:

```bash
.agents/bin/ra attach report.pdf --source-id 000042 --role raw_file --apply
```

Use `--role raw_file` for the document itself, and `--role rendered_pdf_file`
for a print-to-PDF of a page you also saved as HTML.

## After attaching

```bash
.agents/bin/ra where
```

The source should now read `success`. If it does not, say so rather than
attaching again.

A source that was `blocked` is resolved the same way: attaching a fetch-phase
artifact clears the block *and* releases the LLM phases that were held back
while it stood, so a following `ra run` picks them up. One exception, and the
plan warns about it as `attached_file_is_a_block_page`: if the HTML you attached
is itself the bot wall, the file is stored but the source stays `blocked`. That
means the saved page was the challenge screen rather than the article — get the
real page and attach that.

**If any column already ran against that source, its values are now wrong.**
They were computed from whatever text the failed fetch left behind — usually an
error or paywall page. `ra where` counts these as
`value(s) computed from text that has since been rebuilt` and puts the exact
re-run command in `next`, ahead of anything else. Those cells are filled, so
nothing else will ever flag them: tell the user and run what `next` says.
