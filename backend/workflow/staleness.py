"""Notice when a source's text changed under column values already computed.

The case this exists for: a fetch is blocked, the pipeline stores the error page
as the source's text, the columns are run against it, and only later does the
user hand-download the real document. Attaching it rebuilds the text -- but the
cells still hold answers derived from a Cloudflare notice, and nothing said so.
`--scope empty_only` will not revisit them either, because they are not empty.

So the values look coded, are wrong, and are invisible. This records which
columns went stale, `ra where` reports them, and the remedy it hands back
re-runs exactly those rows.

Detection is by content hash taken either side of a convert, rather than by
timestamps: a convert that rewrites identical text is not a change, and a run
that skipped a source must not mark it.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any


def _text_fingerprint(base: Path, row: Any) -> str:
    """Hash of the text a column run would actually read for this source."""
    parts = []
    for attr in ("llm_cleanup_file", "markdown_file"):
        rel = str(getattr(row, attr, "") or "")
        if not rel:
            continue
        try:
            parts.append(hashlib.sha256((base / rel).read_bytes()).hexdigest())
        except OSError:
            parts.append(f"missing:{rel}")
    return "|".join(parts)


def computed_column_ids(column_configs: Any) -> set[str]:
    """Columns whose values a model derived from the source text.

    A column with no instruction prompt holds data the user supplied -- a
    collection date, the channel a link came from -- imported by
    `set_column_values` rather than computed from anything. Rebuilding a
    source's text cannot invalidate it, and the remedy for staleness is to
    re-run the column, which for one of these would replace the user's own data
    with whatever the model says. So they are never stale.
    """
    return {
        str(config.id)
        for config in column_configs or []
        if str(getattr(config, "instruction_prompt", "") or "").strip()
    }


def snapshot(service: Any) -> dict[str, tuple[str, tuple[str, ...]]]:
    """Per source: its text fingerprint and the computed columns already filled."""
    from backend.storage.attached_repository import _load_column_configs, _load_source_rows

    if not getattr(service, "is_attached", False):
        return {}
    base = Path(service.path)
    with service._writer_lock():
        state = service._load_state_locked()
        rows = _load_source_rows(state.get("sources", []))
        computed = computed_column_ids(_load_column_configs(state.get("column_configs", [])))
    return {
        str(row.id): (
            _text_fingerprint(base, row),
            tuple(
                key
                for key, value in (row.custom_fields or {}).items()
                if str(value or "").strip() and key in computed
            ),
        )
        for row in rows
    }


def mark_stale(service: Any, before: dict[str, tuple[str, tuple[str, ...]]]) -> int:
    """Compare against `before` and record which columns went stale.

    Returns the number of (source, column) pairs newly marked.
    """
    from backend.storage.attached_repository import (
        _load_citation_rows,
        _load_column_configs,
        _load_source_rows,
    )

    if not before or not getattr(service, "is_attached", False):
        return 0

    base = Path(service.path)
    marked = 0
    with service._writer_lock():
        state = service._load_state_locked()
        rows = _load_source_rows(state.get("sources", []))
        configs = _load_column_configs(state.get("column_configs", []))
        # Re-checked here as well as in `snapshot`: a column can lose its prompt
        # between the two, and marking on the stale side would be the harmful
        # direction -- the remedy overwrites imported data.
        computed = computed_column_ids(configs)
        changed = False

        for row in rows:
            existing = {c for c in (row.stale_column_ids or "").split(",") if c}
            # Purge marks that should never have been written -- against a
            # provided column, or against a column since deleted. A repository
            # carrying one from an earlier version heals on the next convert
            # rather than nagging forever about data no run can fix.
            valid = existing & computed
            if valid != existing:
                row.stale_column_ids = ",".join(sorted(valid))
                existing = valid
                changed = True

            previous = before.get(str(row.id))
            if not previous:
                continue  # created during this run, so nothing predates it
            old_fingerprint, had_values = previous
            had_values = tuple(key for key in had_values if key in computed)
            if not had_values:
                continue
            if _text_fingerprint(base, row) == old_fingerprint:
                continue  # the text is byte-identical; no value went stale

            merged = existing | set(had_values)
            if merged != existing:
                marked += len(merged - existing)
                row.stale_column_ids = ",".join(sorted(merged))
                changed = True

        if changed:
            service._save_state_locked(
                sources=rows,
                citations=_load_citation_rows(state.get("citations", [])),
                imports=state.get("imports", []),
                column_configs=configs,
            )
    return marked


def stale_pairs(rows: list[Any]) -> dict[str, list[str]]:
    """`{column_id: [source_id, ...]}` for every value known to predate its text."""
    out: dict[str, list[str]] = {}
    for row in rows:
        for column_id in (row.stale_column_ids or "").split(","):
            if column_id:
                out.setdefault(column_id, []).append(str(row.id))
    return out
