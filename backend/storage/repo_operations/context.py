"""Shared read/write context for repository operations.

Every operation reads the whole repository state into an `OperationContext`,
mutates it in memory, and writes it back through the service's existing locked
helpers. Nothing here takes the writer lock -- callers already hold it.

Import rule: this package must not import `attached_repository` at module scope
(that module imports the engine). Helpers from it are pulled in inside
functions instead.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from backend.models.export import ExportRow
from backend.models.repository import RepositoryColumnConfig
from backend.models.sources import SourceManifestRow


@dataclass
class OperationContext:
    """The full mutable state of an attached repository, plus its service."""

    service: Any
    repo_root: Path
    rows: list[SourceManifestRow]
    citations: list[ExportRow]
    imports: list[dict[str, Any]]
    column_configs: list[RepositoryColumnConfig]
    meta: dict[str, Any]
    journal: Any = None  # MoveJournal during apply(); None during plan()
    dropped_row_count: int = 0
    dropped_citation_count: int = 0
    notes: list[str] = field(default_factory=list)

    def row_by_id(self, source_id: str) -> SourceManifestRow | None:
        target = str(source_id or "").strip()
        if not target:
            return None
        for row in self.rows:
            if row.id == target:
                return row
        return None

    def live_ids(self) -> set[str]:
        return {row.id for row in self.rows if row.id}


def load_context_locked(service: Any, *, journal: Any = None) -> OperationContext:
    """Read the repository into memory. The writer lock must already be held."""
    from backend.storage.attached_repository import (
        _load_citation_rows,
        _load_column_configs,
        _load_source_rows,
    )

    state = service._load_state_locked()
    raw_sources = state.get("sources", []) or []
    raw_citations = state.get("citations", []) or []

    rows = _load_source_rows(raw_sources)
    citations = _load_citation_rows(raw_citations)

    return OperationContext(
        service=service,
        repo_root=service.path,
        rows=rows,
        citations=citations,
        imports=list(state.get("imports", []) or []),
        column_configs=_load_column_configs(state.get("column_configs", []) or []),
        meta=service._load_meta_locked(),
        journal=journal,
        # `_safe_manifest_row` swallows validation failures, so a row that no
        # longer parses vanishes silently. Track the gap so verify can catch a
        # mutation that corrupted a row rather than reporting a clean save.
        dropped_row_count=max(0, len(raw_sources) - len(rows)),
        dropped_citation_count=max(0, len(raw_citations) - len(citations)),
    )


def save_context_locked(ctx: OperationContext) -> None:
    """Persist the context and rebuild derived artifacts. Lock must be held."""
    from backend.storage.attached_repository import SCHEMA_VERSION, _utc_now_iso

    ctx.service._save_state_locked(
        sources=ctx.rows,
        citations=ctx.citations,
        imports=ctx.imports,
        column_configs=ctx.column_configs,
    )
    ctx.service._save_meta_locked(
        {
            **ctx.meta,
            "schema_version": SCHEMA_VERSION,
            "updated_at": _utc_now_iso(),
        }
    )
    # `_save_state_locked` reconciles discovery links in place, so rebuilding
    # from the same list writes the reconciled values to the CSV/XLSX too.
    ctx.service._rebuild_outputs_locked(ctx.rows, ctx.citations)


def state_fingerprint_locked(service: Any) -> str:
    """Cheap content hash of the authoritative state, for stale-plan detection."""
    digest = hashlib.sha256()
    for path in (service._state_path(), service._meta_path()):
        try:
            digest.update(path.read_bytes())
        except OSError:
            digest.update(b"<missing>")
        digest.update(b"\x00")
    return digest.hexdigest()


def params_fingerprint(operation: str, params: dict[str, Any]) -> str:
    """Stable hash of an apply request, used as the idempotency fingerprint."""
    payload = json.dumps(
        {"operation": operation, "params": params},
        sort_keys=True,
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
