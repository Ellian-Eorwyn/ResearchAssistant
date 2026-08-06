"""Renumber repository sources by their main fetch URL.

A source id is not just a database key: it names the `sources/<id>/` directory,
prefixes every artifact filename, and is referenced by citations, discovery
links, and import records. Changing one by hand breaks the repository in ways
that only surface on the next attach, when the scan-merge silently renumbers
rows to resolve the damage.

This operation rewrites all of it at once. Swaps and cycles work because every
moved directory is evacuated to staging before any is landed, so there is never
a moment where two sources want the same directory.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from backend.models.operations import PlanChange, PlanIssue

from .base import OperationDefinition
from .context import OperationContext

MAX_SOURCE_ID = 999999


class RemapPair(BaseModel):
    url: str = ""
    new_id: str = ""
    # Escape hatch for uploaded documents, which have no fetch URL.
    source_id: str = ""


class RemapSourceIdsParams(BaseModel):
    pairs: list[RemapPair] = Field(default_factory=list, min_length=1)


# ---------------------------------------------------------------------------
# planning
# ---------------------------------------------------------------------------


def _resolve_id_map(
    ctx: OperationContext,
    params: RemapSourceIdsParams,
) -> tuple[dict[str, str], list[PlanIssue], list[PlanIssue]]:
    """Work out the old->new mapping and everything wrong with it. Reads only.

    Planning and applying both go through here, so the change set a user
    approves is derived by exactly the same code that executes it.
    """
    from backend.storage.attached_repository import SOURCES_DIR_NAME, repository_dedupe_key

    blockers: list[PlanIssue] = []
    warnings: list[PlanIssue] = []

    def block(code: str, message: str, subject: str = "") -> None:
        blockers.append(PlanIssue(code=code, message=message, subject=subject))

    def warn(code: str, message: str, subject: str = "") -> None:
        warnings.append(PlanIssue(code=code, message=message, subject=subject))

    # --- resolve each pair to exactly one row ----------------------------
    id_map: dict[str, str] = {}
    requested_new: dict[str, str] = {}  # new_id -> old_id

    for index, pair in enumerate(params.pairs):
        subject = pair.source_id or pair.url or f"pairs[{index}]"
        row = _resolve_row(ctx, pair, repository_dedupe_key, block, warn, subject)
        if row is None:
            continue

        new_id = _normalize_new_id(pair.new_id)
        if new_id is None:
            block(
                "new_id_invalid",
                f"{pair.new_id!r} is not a valid source id. Use a number between 1 and "
                f"{MAX_SOURCE_ID}.",
                subject,
            )
            continue

        if row.id in id_map:
            block("source_listed_twice", f"Source {row.id} appears more than once.", subject)
            continue
        if new_id in requested_new:
            block(
                "new_id_duplicate",
                f"Two sources were both assigned id {new_id}.",
                subject,
            )
            continue

        if new_id == row.id:
            warn("remap_noop", f"Source {row.id} is already id {new_id}; skipping.", subject)
            continue

        id_map[row.id] = new_id
        requested_new[new_id] = row.id

    if blockers or not id_map:
        return id_map, blockers, warnings

    # --- a new id may only land on a slot nobody keeps --------------------
    moving = set(id_map)
    for new_id, old_id in sorted(requested_new.items()):
        holder = ctx.row_by_id(new_id)
        if holder is not None and holder.id not in moving:
            block(
                "new_id_collides",
                f"Source {old_id} cannot take id {new_id}: source {holder.id} already holds it "
                "and is not being moved. Include it in the same request to swap them.",
                f"source:{old_id}",
            )
        # Only an *orphaned* directory is a problem here. One owned by a live
        # source is already reported as `new_id_collides`; saying it twice just
        # muddies the report.
        target_dir = ctx.repo_root / SOURCES_DIR_NAME / new_id
        if holder is None and new_id not in moving and target_dir.exists():
            block(
                "target_dir_occupied",
                f"Directory sources/{new_id}/ already exists but belongs to no source. "
                "Remove or rename it first.",
                f"sources/{new_id}",
            )

    if blockers:
        return id_map, blockers, warnings

    for cycle in _find_cycles(id_map):
        warn(
            "remap_cycle",
            "These sources exchange ids: "
            + " -> ".join(cycle + [cycle[0]])
            + ". They are moved through a staging area, so the swap is safe.",
            f"source:{cycle[0]}",
        )

    # --- anything that would undo the change on the next attach ------------
    _check_stray_manifests(ctx, moving, block, warn)

    return id_map, blockers, warnings


def plan(
    ctx: OperationContext,
    params: RemapSourceIdsParams,
) -> tuple[list[PlanChange], list[PlanIssue], list[PlanIssue], str]:
    from backend.pipeline.source_downloader import format_source_id_list, parse_source_id_list
    from backend.storage.attached_repository import (
        FILE_FIELDS,
        SOURCES_DIR_NAME,
        _repository_source_file_path,
    )

    changes: list[PlanChange] = []
    id_map, blockers, warnings = _resolve_id_map(ctx, params)

    def warn(code: str, message: str, subject: str = "") -> None:
        warnings.append(PlanIssue(code=code, message=message, subject=subject))

    if blockers:
        return changes, blockers, warnings, "The request could not be resolved."
    if not id_map:
        return changes, blockers, warnings, "Every source already has the requested id."

    requested_new = {new: old for old, new in id_map.items()}

    # --- build the change set ---------------------------------------------
    for old_id, new_id in sorted(id_map.items()):
        row = ctx.row_by_id(old_id)
        if row is None:  # pragma: no cover - resolved above
            continue
        subject = f"source:{old_id}"

        changes.append(
            PlanChange(kind="row_field", subject=subject, field="id", before=old_id, after=new_id)
        )
        changes.append(
            PlanChange(
                kind="row_field",
                subject=subject,
                field="repository_source_id",
                before=row.repository_source_id,
                after=new_id,
            )
        )

        source_dir = ctx.repo_root / SOURCES_DIR_NAME / old_id
        if source_dir.is_dir():
            for item in sorted(source_dir.rglob("*")):
                if not item.is_file():
                    continue
                rel = item.relative_to(source_dir)
                changes.append(
                    PlanChange(
                        kind="file_move",
                        subject=subject,
                        before=f"{SOURCES_DIR_NAME}/{old_id}/{rel.as_posix()}",
                        after=f"{SOURCES_DIR_NAME}/{new_id}/"
                        f"{_renamed_relative(rel, old_id, new_id).as_posix()}",
                    )
                )

        for field_name in FILE_FIELDS:
            rel_value = str(getattr(row, field_name, "") or "").strip()
            if not rel_value:
                continue
            if not _is_under_source_dir(rel_value, old_id):
                warn(
                    "artifact_outside_source_dir",
                    f"{field_name} points at {rel_value!r}, outside sources/{old_id}/. "
                    "It is left where it is and keeps its current name.",
                    f"{subject}.{field_name}",
                )
                continue
            new_rel = _repository_source_file_path(
                source_id=new_id,
                field=field_name,
                source_name=Path(rel_value).name,
                source_row_id=old_id,
            ).as_posix()
            changes.append(
                PlanChange(
                    kind="row_field",
                    subject=subject,
                    field=field_name,
                    before=rel_value,
                    after=new_rel,
                )
            )

    # references held by other rows
    for row in ctx.rows:
        subject = f"source:{row.id}"
        parent = str(row.discovered_from or "").strip()
        if parent and parent in id_map:
            changes.append(
                PlanChange(
                    kind="row_field",
                    subject=subject,
                    field="discovered_from",
                    before=parent,
                    after=id_map[parent],
                )
            )
        children = parse_source_id_list(str(row.discovered_source_ids or ""))
        if any(child in id_map for child in children):
            changes.append(
                PlanChange(
                    kind="row_field",
                    subject=subject,
                    field="discovered_source_ids",
                    before=format_source_id_list(children),
                    after=format_source_id_list([id_map.get(child, child) for child in children]),
                )
            )

    for citation in ctx.citations:
        ref = str(citation.repository_source_id or "").strip()
        if ref in id_map:
            changes.append(
                PlanChange(
                    kind="state_field",
                    subject=f"citation:{ref}",
                    field="repository_source_id",
                    before=ref,
                    after=id_map[ref],
                )
            )

    for record in ctx.imports:
        if not isinstance(record, dict):
            continue
        import_id = str(record.get("import_id") or record.get("id") or "?")
        refs = [str(item) for item in record.get("source_ids", []) or []]
        if any(ref in id_map for ref in refs):
            changes.append(
                PlanChange(
                    kind="state_field",
                    subject=f"import:{import_id}",
                    field="source_ids",
                    before=", ".join(refs),
                    after=", ".join(id_map.get(ref, ref) for ref in refs),
                )
            )
        for document in record.get("documents", []) or []:
            if isinstance(document, dict) and str(document.get("source_id") or "") in id_map:
                old_ref = str(document["source_id"])
                changes.append(
                    PlanChange(
                        kind="state_field",
                        subject=f"import:{import_id}",
                        field="documents[].source_id",
                        before=old_ref,
                        after=id_map[old_ref],
                    )
                )

    new_next = max(int(item) for item in requested_new) + 1
    try:
        current_next = int(ctx.meta.get("next_source_id") or 1)
    except (TypeError, ValueError):
        current_next = 1
    if new_next > current_next:
        changes.append(
            PlanChange(
                kind="meta_field",
                subject="meta.next_source_id",
                field="next_source_id",
                before=str(current_next),
                after=str(new_next),
            )
        )

    pairs_text = ", ".join(f"{old} -> {new}" for old, new in sorted(id_map.items()))
    summary = f"Renumber {len(id_map)} source(s): {pairs_text}."
    return changes, blockers, warnings, summary


def _resolve_row(
    ctx: OperationContext,
    pair: RemapPair,
    repository_dedupe_key: Any,
    block: Any,
    warn: Any,
    subject: str,
) -> Any:
    """Find the one row a pair refers to, or record why we can't."""
    if str(pair.source_id or "").strip():
        row = ctx.row_by_id(pair.source_id)
        if row is None:
            block("unknown_source_id", f"No source has id {pair.source_id!r}.", subject)
        return row

    url = str(pair.url or "").strip()
    if not url:
        block("url_required", "Each pair needs either a url or a source_id.", subject)
        return None

    key = repository_dedupe_key(url)
    if not key:
        block("url_invalid", f"{url!r} is not a usable URL.", subject)
        return None

    matches = [row for row in ctx.rows if repository_dedupe_key(row.original_url or "") == key]
    if not matches:
        # Only fall back to redirect/canonical URLs once the primary key misses,
        # and say so -- the user asked about the main fetch URL.
        matches = [
            row
            for row in ctx.rows
            if repository_dedupe_key(row.final_url or "") == key
            or repository_dedupe_key(row.canonical_url or "") == key
        ]
        if len(matches) == 1:
            warn(
                "url_matched_via_final_url",
                f"{url!r} is not source {matches[0].id}'s main fetch URL; it matched its "
                "final or canonical URL instead.",
                subject,
            )

    if not matches:
        block("url_not_found", f"No source has {url!r} as its fetch URL.", subject)
        return None
    if len(matches) > 1:
        ids = ", ".join(row.id for row in matches)
        block(
            "url_ambiguous",
            f"{url!r} matches {len(matches)} sources ({ids}). Use source_id to pick one.",
            subject,
        )
        return None

    row = matches[0]
    if str(row.source_kind or "").strip().lower() == "uploaded_document":
        warn(
            "url_on_uploaded_document",
            f"Source {row.id} is an uploaded document; matching it by URL is unusual.",
            subject,
        )
    return row


def _check_stray_manifests(ctx: OperationContext, moving: set[str], block: Any, warn: Any) -> None:
    """Find manifest files that the next attach would merge back over us.

    `attach()` rglobs the repository for manifests outside `.ra_repo` and merges
    them into state keyed by URL. A stray copy holding a *higher*-priority row
    for a source we are renumbering would resurrect the old id.
    """
    from backend.storage.attached_repository import (
        CITATIONS_CSV_NAME,
        MANIFEST_CSV_NAME,
        _row_priority,
        _source_row_identity_key,
    )

    state_by_key = {_source_row_identity_key(row): row for row in ctx.rows if row.id in moving}
    if not state_by_key:
        return

    # The canonical root artifacts are rebuilt from state on every write, so
    # they are scan inputs but never a threat. Everything else is suspect.
    canonical = {
        (ctx.repo_root / MANIFEST_CSV_NAME).resolve(),
        (ctx.repo_root / CITATIONS_CSV_NAME).resolve(),
    }

    readers = (
        (MANIFEST_CSV_NAME, ctx.service._read_manifest_csv),
        ("06_sources_manifest.json", ctx.service._read_sources_artifact_json),
    )

    for filename, reader in readers:
        for path in sorted(ctx.service._iter_paths_named(filename)):
            try:
                if path.resolve() in canonical:
                    continue
            except OSError:
                continue

            rel = _relative(ctx.repo_root, path)
            try:
                stray_rows = reader(path, rel)
            except Exception:
                warn("stray_manifest_unreadable", f"Could not read {rel}; ignoring it.", rel)
                continue

            overrides: list[str] = []
            for stray in stray_rows:
                state_row = state_by_key.get(_source_row_identity_key(stray))
                if state_row is None:
                    continue
                if _row_priority(stray) > _row_priority(state_row):
                    overrides.append(state_row.id)

            if overrides:
                block(
                    "stray_manifest_would_override",
                    f"{rel} holds a higher-priority copy of source(s) "
                    f"{', '.join(sorted(set(overrides)))}. The next attach would merge it back "
                    "and undo this renumbering. Move or delete that file first.",
                    rel,
                )
            else:
                warn(
                    "stray_manifest_present",
                    f"{rel} is scanned on every attach but does not override anything here.",
                    rel,
                )


# ---------------------------------------------------------------------------
# applying
# ---------------------------------------------------------------------------


def apply(ctx: OperationContext, params: RemapSourceIdsParams, plan_obj: Any) -> int:
    from backend.pipeline.source_downloader import format_source_id_list, parse_source_id_list
    from backend.storage.attached_repository import (
        FILE_FIELDS,
        SOURCES_DIR_NAME,
        _next_source_id_from_rows,
        _repository_source_file_path,
    )

    id_map, blockers, _ = _resolve_id_map(ctx, params)
    if blockers:  # pragma: no cover - the engine re-plans and stops first
        raise RuntimeError("remap_source_ids was applied with unresolved blockers")
    if not id_map:
        return 0

    journal = ctx.journal
    sources_root = ctx.repo_root / SOURCES_DIR_NAME
    staging = journal.staging_dir

    # Phase 1 -- evacuate every moved directory before landing any of them.
    # This is what makes swaps and cycles work: no directory name is ever
    # wanted by two sources at the same time.
    evacuated: dict[str, Path] = {}
    for old_id in sorted(id_map):
        source_dir = sources_root / old_id
        if not source_dir.is_dir():
            continue
        target = staging / old_id
        journal.move(source_dir, target)
        evacuated[old_id] = target

    # Phase 2 -- land each into its new home, renaming as we go.
    for old_id, new_id in sorted(id_map.items()):
        staged = evacuated.get(old_id)
        if staged is None:
            continue
        destination = sources_root / new_id
        destination.mkdir(parents=True, exist_ok=True)
        journal.record_created(destination)

        for item in sorted(staged.rglob("*")):
            if not item.is_file():
                continue
            rel = item.relative_to(staged)
            journal.move(item, destination / _renamed_relative(rel, old_id, new_id))

    # Phase 3 -- rewrite every reference.
    applied = 0

    # Bind every row object *before* mutating any id. A swap reuses ids, so
    # looking rows up by id mid-loop would find the row just renamed.
    rows_by_old_id = {old_id: ctx.row_by_id(old_id) for old_id in id_map}

    for old_id, new_id in id_map.items():
        row = rows_by_old_id.get(old_id)
        if row is None:  # pragma: no cover
            continue
        for field_name in FILE_FIELDS:
            rel_value = str(getattr(row, field_name, "") or "").strip()
            if not rel_value or not _is_under_source_dir(rel_value, old_id):
                continue
            setattr(
                row,
                field_name,
                _repository_source_file_path(
                    source_id=new_id,
                    field=field_name,
                    source_name=Path(rel_value).name,
                    source_row_id=old_id,
                ).as_posix(),
            )
            applied += 1
        row.id = new_id
        row.repository_source_id = new_id
        applied += 1

    # Discovery links must be rewritten on *both* sides before saving.
    # `_save_state_locked` reconciles them with a union that never removes, so
    # an old id left on either side becomes permanent.
    for row in ctx.rows:
        parent = str(row.discovered_from or "").strip()
        if parent in id_map:
            row.discovered_from = id_map[parent]
            applied += 1
        children = parse_source_id_list(str(row.discovered_source_ids or ""))
        if any(child in id_map for child in children):
            row.discovered_source_ids = format_source_id_list(
                [id_map.get(child, child) for child in children]
            )
            applied += 1

    for citation in ctx.citations:
        ref = str(citation.repository_source_id or "").strip()
        if ref in id_map:
            citation.repository_source_id = id_map[ref]
            applied += 1

    for record in ctx.imports:
        if not isinstance(record, dict):
            continue
        refs = record.get("source_ids")
        if isinstance(refs, list):
            rewritten = [id_map.get(str(item), str(item)) for item in refs]
            if rewritten != [str(item) for item in refs]:
                record["source_ids"] = rewritten
                applied += 1
        for document in record.get("documents", []) or []:
            if not isinstance(document, dict):
                continue
            ref = str(document.get("source_id") or "").strip()
            if ref in id_map:
                document["source_id"] = id_map[ref]
                applied += 1

    ctx.rows = ctx.service._sort_rows(ctx.rows)
    ctx.citations = ctx.service._sort_citations(ctx.citations)

    try:
        current_next = int(ctx.meta.get("next_source_id") or 1)
    except (TypeError, ValueError):
        current_next = 1
    ctx.meta["next_source_id"] = max(current_next, _next_source_id_from_rows(ctx.rows))

    # Rewrite each moved row's serialized copy last, so it reflects the final
    # field values. This also sets `metadata_file` for us. `protect` first:
    # the file was carried over by the directory move, so rollback would
    # otherwise put it back at the old path holding the new id.
    for row in rows_by_old_id.values():
        if row is None:
            continue
        journal.protect(ctx.repo_root / SOURCES_DIR_NAME / row.id / f"{row.id}_metadata.json")
        ctx.service._write_repository_source_metadata(row)

    return applied


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _normalize_new_id(value: str) -> str | None:
    text = str(value or "").strip()
    if not text.isdigit():
        return None
    number = int(text)
    if number < 1 or number > MAX_SOURCE_ID:
        return None
    return f"{number:06d}"


def _renamed_relative(rel: Path, old_id: str, new_id: str) -> Path:
    """Apply the `<id>_` filename convention to a path inside a source dir."""
    name = rel.name
    if name.startswith(old_id):
        renamed = new_id + name[len(old_id) :]
    elif name.startswith(f"{new_id}_"):
        renamed = name
    else:
        renamed = f"{new_id}_{name}"
    return rel.parent / renamed


def _is_under_source_dir(rel_value: str, source_id: str) -> bool:
    from backend.storage.attached_repository import SOURCES_DIR_NAME

    candidate = Path(rel_value)
    if candidate.is_absolute():
        return False
    parts = candidate.parts
    return len(parts) >= 2 and parts[0] == SOURCES_DIR_NAME and parts[1] == source_id


def _find_cycles(id_map: dict[str, str]) -> list[list[str]]:
    """Cycles in the old->new permutation, e.g. a swap or a three-way rotate."""
    cycles: list[list[str]] = []
    seen: set[str] = set()
    for start in sorted(id_map):
        if start in seen:
            continue
        chain: list[str] = []
        node = start
        while node in id_map and node not in seen:
            seen.add(node)
            chain.append(node)
            node = id_map[node]
        if node == start and len(chain) > 1:
            cycles.append(chain)
    return cycles


def _relative(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)


DEFINITION = OperationDefinition(
    name="remap_source_ids",
    title="Renumber sources",
    description=(
        "Change the six-digit id of one or more sources, identified by their main fetch "
        "URL. Rewrites the state, citations, discovery links, and import records, and "
        "renames the on-disk source directories and artifact files to match. Swaps and "
        "cycles are supported."
    ),
    params_model=RemapSourceIdsParams,
    planner=plan,
    applier=apply,
    identity_remap=lambda ctx, params: _resolve_id_map(ctx, params)[0],
)
