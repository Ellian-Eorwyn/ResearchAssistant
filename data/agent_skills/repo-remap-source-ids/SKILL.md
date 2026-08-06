---
name: repo-remap-source-ids
description: Change the six-digit id of ResearchAssistant sources, identified by their URL. Use when the user wants to renumber sources, swap two ids, or make the repository match a spreadsheet's own numbering. Rewrites the state, citations, discovery links and import records, and renames the on-disk folders and files to match, all as one transaction that either fully succeeds or fully unwinds.
license: MIT
allowed-tools: Bash(.agents/bin/ra *)
---

# Renumber sources

A source id names its folder, prefixes every one of its files, and is
referenced by citations, discovery links and import records. Changing one by
hand breaks the repository in a way that only shows up later. This does all of
it at once, and undoes everything if any check fails.

Swaps and cycles work: send every change in one request and the sources are
moved through a staging area, so two sources can exchange ids safely.

## Never do these

1. Never rename anything under `sources/` yourself.
2. Never edit `.ra_repo/repository_state.json` or `manifest.csv`.

## Renumber

```bash
.agents/bin/ra remap https://example.com/a=5 https://example.com/b=3
.agents/bin/ra remap https://example.com/a=5 https://example.com/b=3 --apply
```

Run it without `--apply` first and show the user what would change. Send every
change in one command — that is what makes a swap work.

For a source with no URL (an uploaded document), name it by id instead:

```bash
.agents/bin/ra remap id:000012=40 --apply
```

## Read the answer

- Any `problems` → **stop** and report them. Nothing changed.
- The summary lists what would change, grouped by source.
- A `remap_cycle` note just means a swap was recognised and is safe.
- After applying, a `rolled_back` result means nothing changed and the reason is
  in the output; do not retry the same command.

## Common blockers

| code | what to do |
|---|---|
| `url_not_found` | No source has that URL. Run `.agents/bin/ra where`. |
| `url_ambiguous` | Several sources match. Use `source_id` instead. |
| `new_id_collides` | Another source holds that id and is not moving. Include it in the same request to swap them. |
| `new_id_duplicate` | Two sources were given the same id. |
| `stray_manifest_would_override` | A stray manifest elsewhere in the repository would undo this on the next launch. Show the path and ask the user to remove it. |

## Afterwards

```bash
.agents/bin/ra where
```
