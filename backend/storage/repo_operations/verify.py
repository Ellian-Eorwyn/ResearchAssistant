"""Post-condition checks for the repository operations engine.

`verify_repository_locked` is run against a freshly reloaded context after an
operation writes, so it checks what actually landed on disk rather than what
the applier believed it wrote.

The engine compares the result against a baseline taken before the operation
and rolls back only on *new* issues. A repository that was already slightly
broken (a hand-deleted artifact, say) therefore stays usable instead of making
every future operation impossible.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from backend.models.operations import VerifyIssue

from .context import OperationContext


def issue_signature(issue: VerifyIssue) -> str:
    return f"{issue.code}|{issue.subject}"


def _is_in_source_dir(path: Path, repo_root: Path, source_id: str) -> bool:
    from backend.storage.attached_repository import SOURCES_DIR_NAME

    try:
        return path.resolve().parent == (repo_root / SOURCES_DIR_NAME / source_id).resolve()
    except OSError:
        return False


def verify_repository_locked(service: Any, ctx: OperationContext) -> list[VerifyIssue]:
    """Check every invariant a source id participates in. Lock must be held."""
    from backend.storage.attached_repository import (
        FILE_FIELDS,
        SOURCES_DIR_NAME,
        _next_source_id_from_rows,
        _parse_numeric_id,
        _source_row_identity_key,
    )
    from backend.pipeline.source_downloader import parse_source_id_list

    issues: list[VerifyIssue] = []

    def add(code: str, message: str, subject: str = "") -> None:
        issues.append(VerifyIssue(code=code, message=message, subject=subject))

    # --- state integrity -------------------------------------------------
    if ctx.dropped_row_count:
        add(
            "state_unparseable",
            f"{ctx.dropped_row_count} source row(s) in repository_state.json no longer "
            "parse as SourceManifestRow and were silently dropped on load.",
            "sources",
        )
    if ctx.dropped_citation_count:
        add(
            "state_unparseable",
            f"{ctx.dropped_citation_count} citation row(s) no longer parse and were dropped.",
            "citations",
        )

    live_ids = ctx.live_ids()

    seen_ids: set[str] = set()
    for row in ctx.rows:
        subject = f"source:{row.id or '<blank>'}"

        if not str(row.id or "").strip():
            add("id_missing", "Source row has an empty id.", subject)
            continue
        if row.id in seen_ids:
            add("duplicate_source_id", f"Source id {row.id} appears more than once.", subject)
        seen_ids.add(row.id)

        if row.repository_source_id != row.id:
            add(
                "id_mismatch",
                f"repository_source_id {row.repository_source_id!r} does not match id {row.id!r}.",
                subject,
            )

        numeric = _parse_numeric_id(row.id)
        if numeric is None:
            add("id_not_numeric", f"Source id {row.id!r} is not numeric.", subject)
        elif row.id != f"{numeric:06d}":
            add(
                "id_not_canonical",
                f"Source id {row.id!r} is not in canonical six-digit form ({numeric:06d}).",
                subject,
            )

        # --- artifacts ---------------------------------------------------
        for field_name in FILE_FIELDS:
            rel_value = str(getattr(row, field_name, "") or "").strip()
            if not rel_value:
                continue
            artifact_subject = f"source:{row.id}.{field_name}"

            direct = ctx.repo_root / Path(rel_value)
            resolved = service._resolve_repository_artifact_path(row, field_name, rel_value)
            if resolved is None or not resolved.is_file():
                add(
                    "missing_artifact",
                    f"{field_name} points at {rel_value!r} but no file resolves there.",
                    artifact_subject,
                )
                continue
            if not Path(rel_value).is_absolute() and not direct.is_file():
                # Resolved only via a legacy/canonical fallback: the stored path
                # is stale, which is exactly the half-finished-rename symptom.
                add(
                    "artifact_path_stale",
                    f"{field_name} is stored as {rel_value!r} but the file was only found "
                    f"at {resolved}.",
                    artifact_subject,
                )
            if not service._is_path_within_repo(resolved):
                add(
                    "artifact_outside_repo",
                    f"{field_name} resolves to {resolved}, outside the repository.",
                    artifact_subject,
                )
            elif _is_in_source_dir(resolved, ctx.repo_root, row.id) and not resolved.name.startswith(
                f"{row.id}_"
            ):
                # Only files that live in the source's own directory carry the
                # naming invariant. Legacy flat layouts (markdown/, summaries/)
                # are a pre-existing condition, not something to fail on.
                add(
                    "artifact_id_prefix_mismatch",
                    f"{field_name} file {resolved.name!r} is not prefixed with {row.id}_.",
                    artifact_subject,
                )

        # --- the serialized copy of the row ------------------------------
        metadata_abs = ctx.repo_root / SOURCES_DIR_NAME / row.id / f"{row.id}_metadata.json"
        if metadata_abs.is_file():
            try:
                stored_id = str(json.loads(metadata_abs.read_text(encoding="utf-8")).get("id") or "")
            except Exception:
                stored_id = "<unreadable>"
            if stored_id != row.id:
                add(
                    "stale_metadata_file",
                    f"{metadata_abs.name} records id {stored_id!r}, expected {row.id!r}.",
                    subject,
                )

        # --- discovery links ---------------------------------------------
        parent = str(row.discovered_from or "").strip()
        if parent and parent not in live_ids:
            add(
                "orphan_discovery_link",
                f"discovered_from points at {parent!r}, which is not a live source.",
                subject,
            )
        for child in parse_source_id_list(str(row.discovered_source_ids or "")):
            if child not in live_ids:
                add(
                    "orphan_discovery_link",
                    f"discovered_source_ids references {child!r}, which is not a live source.",
                    subject,
                )

    # --- citations -------------------------------------------------------
    for citation in ctx.citations:
        ref = str(citation.repository_source_id or "").strip()
        if ref and ref not in live_ids:
            add(
                "orphan_citation",
                f"Citation references source {ref!r}, which no longer exists.",
                f"citation:{ref}",
            )

    # --- imports ---------------------------------------------------------
    for record in ctx.imports:
        if not isinstance(record, dict):
            continue
        import_id = str(record.get("import_id") or record.get("id") or "?")
        for ref in record.get("source_ids", []) or []:
            if str(ref).strip() and str(ref).strip() not in live_ids:
                add(
                    "orphan_import_ref",
                    f"Import {import_id} lists source {ref!r}, which no longer exists.",
                    f"import:{import_id}",
                )
        for document in record.get("documents", []) or []:
            if not isinstance(document, dict):
                continue
            ref = str(document.get("source_id") or "").strip()
            if ref and ref not in live_ids:
                add(
                    "orphan_import_ref",
                    f"Import {import_id} document references source {ref!r}, which no longer exists.",
                    f"import:{import_id}",
                )

    # --- id allocation ----------------------------------------------------
    expected_next = _next_source_id_from_rows(ctx.rows)
    try:
        recorded_next = int(ctx.meta.get("next_source_id") or 1)
    except (TypeError, ValueError):
        recorded_next = 1
    if recorded_next < expected_next:
        add(
            "next_source_id_too_low",
            f"next_source_id is {recorded_next} but the highest source id needs at least "
            f"{expected_next}; the next import would collide.",
            "meta.next_source_id",
        )

    # --- url uniqueness ---------------------------------------------------
    identity_owner: dict[str, str] = {}
    for row in ctx.rows:
        key = _source_row_identity_key(row)
        if not key:
            continue
        if key in identity_owner:
            add(
                "dedupe_key_collision",
                f"Sources {identity_owner[key]} and {row.id} share identity key {key!r}; "
                "one of them will be dropped on the next attach.",
                f"source:{row.id}",
            )
            continue
        identity_owner[key] = row.id

    # --- leftover directories ---------------------------------------------
    sources_dir = ctx.repo_root / SOURCES_DIR_NAME
    if sources_dir.is_dir():
        for child in sorted(sources_dir.iterdir()):
            if child.is_dir() and child.name not in live_ids:
                add(
                    "stray_source_dir",
                    f"Directory sources/{child.name}/ does not correspond to any live source.",
                    f"sources/{child.name}",
                )

    # --- the one that matters most ----------------------------------------
    issues.extend(_verify_attach_would_not_renumber(service, ctx))

    return issues


def _verify_attach_would_not_renumber(service: Any, ctx: OperationContext) -> list[VerifyIssue]:
    """Simulate what the next `attach()` scan-merge would do to these rows.

    `attach()` re-reads stray manifests, collapses rows by URL identity, and
    reassigns ids that collide or aren't numeric -- without moving files. A
    mutation that looks clean today can therefore be silently undone on the
    next launch. Running the real merge over a copy is the only faithful check.
    """
    issues: list[VerifyIssue] = []
    try:
        copies = [row.model_copy(deep=True) for row in ctx.rows]
        merged = service._merge_source_rows(copies)
    except Exception as exc:  # pragma: no cover - defensive
        return [
            VerifyIssue(
                code="attach_simulation_failed",
                message=f"Could not simulate the next attach: {exc}",
                subject="attach",
            )
        ]

    before = {row.id for row in ctx.rows}
    after = {row.id for row in merged.rows}

    for lost in sorted(before - after):
        issues.append(
            VerifyIssue(
                code="next_attach_would_renumber",
                message=(
                    f"The next attach would drop or renumber source {lost}. "
                    "The change would not survive a restart."
                ),
                subject=f"source:{lost}",
            )
        )
    for gained in sorted(after - before):
        issues.append(
            VerifyIssue(
                code="next_attach_would_renumber",
                message=f"The next attach would reassign a source to id {gained}.",
                subject=f"source:{gained}",
            )
        )
    if merged.duplicate_urls_removed:
        issues.append(
            VerifyIssue(
                code="next_attach_would_merge_rows",
                message=(
                    f"The next attach would collapse {merged.duplicate_urls_removed} row(s) "
                    "as duplicates."
                ),
                subject="attach",
            )
        )
    return issues
