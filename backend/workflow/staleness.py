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


def snapshot(service: Any) -> dict[str, tuple[str, tuple[str, ...]]]:
    """Per source: its text fingerprint and the columns that already hold a value."""
    from backend.storage.attached_repository import _load_source_rows

    if not getattr(service, "is_attached", False):
        return {}
    base = Path(service.path)
    with service._writer_lock():
        rows = _load_source_rows(service._load_state_locked().get("sources", []))
    return {
        str(row.id): (
            _text_fingerprint(base, row),
            tuple(
                key
                for key, value in (row.custom_fields or {}).items()
                if str(value or "").strip()
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
        changed = False

        for row in rows:
            previous = before.get(str(row.id))
            if not previous:
                continue  # created during this run, so nothing predates it
            old_fingerprint, had_values = previous
            if not had_values:
                continue
            if _text_fingerprint(base, row) == old_fingerprint:
                continue  # the text is byte-identical; no value went stale

            existing = {c for c in (row.stale_column_ids or "").split(",") if c}
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
                column_configs=_load_column_configs(state.get("column_configs", [])),
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
