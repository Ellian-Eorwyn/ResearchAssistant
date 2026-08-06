"""Create repository sources from a list of URLs, with chosen ids.

The app's own seed-file import allocates ids sequentially, which is right when
the ids do not matter. When they come from a spreadsheet that already numbers
its rows, they matter a great deal -- and renumbering afterwards means moving
every directory a second time.

This operation takes the id and the URL together, so a spreadsheet's numbering
survives into the repository in one reviewable step. New rows are created
`queued`, which is exactly what the fetch phase looks for.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, Field

from backend.models.operations import PlanChange, PlanIssue
from backend.models.sources import SourceManifestRow

from .base import OperationDefinition
from .context import OperationContext

MAX_SOURCE_ID = 999999


class SourceSpec(BaseModel):
    url: str = ""
    # Leave blank to let the repository allocate the next free id.
    id: str = ""
    title: str = ""
    notes: str = ""


class CreateSourcesParams(BaseModel):
    sources: list[SourceSpec] = Field(default_factory=list, min_length=1)
    # A URL the repository already holds is usually a re-run, not a mistake.
    skip_existing: bool = True


class _Planned:
    def __init__(self, spec: SourceSpec, url: str, source_id: str) -> None:
        self.spec = spec
        self.url = url
        self.source_id = source_id


def _resolve(
    ctx: OperationContext,
    params: CreateSourcesParams,
) -> tuple[list[_Planned], list[PlanIssue], list[PlanIssue]]:
    """Decide what will be created. Reads only; shared by plan and apply."""
    from backend.pipeline.source_downloader import normalize_url
    from backend.storage.attached_repository import (
        _next_source_id_from_rows,
        repository_dedupe_key,
    )

    blockers: list[PlanIssue] = []
    warnings: list[PlanIssue] = []

    def block(code: str, message: str, subject: str) -> None:
        blockers.append(PlanIssue(code=code, message=message, subject=subject))

    def warn(code: str, message: str, subject: str) -> None:
        warnings.append(PlanIssue(code=code, message=message, subject=subject))

    existing_by_key = {
        repository_dedupe_key(row.original_url or row.final_url or ""): row
        for row in ctx.rows
        if (row.original_url or row.final_url)
    }
    taken_ids = ctx.live_ids()

    # Reserve every explicitly requested id before auto-allocating any, so an
    # auto-assigned row never steals a number a later entry asked for.
    requested_ids: set[str] = set()
    for spec in params.sources:
        normalized = _normalize_id(spec.id)
        if normalized:
            requested_ids.add(normalized)

    try:
        next_free = int(ctx.meta.get("next_source_id") or 1)
    except (TypeError, ValueError):
        next_free = 1
    next_free = max(next_free, _next_source_id_from_rows(ctx.rows))

    planned: list[_Planned] = []
    seen_keys: dict[str, str] = {}
    assigned_ids: set[str] = set()

    for index, spec in enumerate(params.sources):
        subject = spec.id or spec.url or f"sources[{index}]"

        raw_url = str(spec.url or "").strip()
        if not raw_url:
            block("url_required", "Each entry needs a url.", subject)
            continue

        normalized_url, error = normalize_url(raw_url)
        if error or not normalized_url:
            block("url_invalid", f"{raw_url!r} is not a usable URL ({error}).", subject)
            continue

        # `normalize_url` is lenient by design -- it will happily treat
        # "not a url at all" as a hostname. That is fine for a hand-typed
        # address, but a spreadsheet column full of prose would become a batch
        # of sources that only fail much later, during fetch. Catch it here,
        # where the user can still fix the cell.
        host_problem = _implausible_host(normalized_url)
        if host_problem:
            block(
                "url_invalid",
                f"{raw_url!r} does not look like a web address ({host_problem}). "
                "Check the URL column for this row.",
                subject,
            )
            continue

        key = repository_dedupe_key(normalized_url)

        duplicate_in_request = seen_keys.get(key)
        if duplicate_in_request:
            block(
                "duplicate_url_in_request",
                f"{raw_url!r} appears twice in this request (also as {duplicate_in_request}).",
                subject,
            )
            continue

        existing = existing_by_key.get(key)
        if existing is not None:
            if params.skip_existing:
                warn(
                    "url_already_present",
                    f"{raw_url!r} is already source {existing.id}; skipping.",
                    subject,
                )
                continue
            block(
                "url_already_present",
                f"{raw_url!r} is already source {existing.id}.",
                subject,
            )
            continue

        source_id = _normalize_id(spec.id)
        if str(spec.id or "").strip() and source_id is None:
            block(
                "id_invalid",
                f"{spec.id!r} is not a valid source id. Use a number between 1 and "
                f"{MAX_SOURCE_ID}, or leave it blank to auto-assign.",
                subject,
            )
            continue

        if source_id is None:
            while (
                f"{next_free:06d}" in taken_ids
                or f"{next_free:06d}" in requested_ids
                or f"{next_free:06d}" in assigned_ids
            ):
                next_free += 1
            source_id = f"{next_free:06d}"
            next_free += 1
        else:
            if source_id in taken_ids:
                block(
                    "id_taken",
                    f"Source id {source_id} already exists in this repository.",
                    subject,
                )
                continue
            if source_id in assigned_ids:
                block(
                    "id_duplicate_in_request",
                    f"Source id {source_id} was requested twice.",
                    subject,
                )
                continue

        assigned_ids.add(source_id)
        seen_keys[key] = source_id
        planned.append(_Planned(spec, normalized_url, source_id))

    return planned, blockers, warnings


def plan(
    ctx: OperationContext,
    params: CreateSourcesParams,
) -> tuple[list[PlanChange], list[PlanIssue], list[PlanIssue], str]:
    planned, blockers, warnings = _resolve(ctx, params)
    changes: list[PlanChange] = []

    for item in planned:
        subject = f"source:{item.source_id}"
        changes.append(
            PlanChange(
                kind="row_create",
                subject=subject,
                field="original_url",
                after=item.url,
                detail="queued for fetch",
            )
        )
        if item.spec.title.strip():
            changes.append(
                PlanChange(
                    kind="row_field", subject=subject, field="title", after=item.spec.title.strip()
                )
            )

    if planned:
        highest = max(int(item.source_id) for item in planned)
        try:
            current_next = int(ctx.meta.get("next_source_id") or 1)
        except (TypeError, ValueError):
            current_next = 1
        if highest + 1 > current_next:
            changes.append(
                PlanChange(
                    kind="meta_field",
                    subject="meta.next_source_id",
                    field="next_source_id",
                    before=str(current_next),
                    after=str(highest + 1),
                )
            )

    skipped = len([w for w in warnings if w.code == "url_already_present"])
    parts = []
    if planned:
        ids = [item.source_id for item in planned]
        parts.append(f"create {len(planned)} source(s) ({_id_range(ids)}), queued for fetch")
    if skipped:
        parts.append(f"skip {skipped} already present")
    summary = ("Will " + ", ".join(parts) + ".") if parts else "Nothing to create."

    return changes, blockers, warnings, summary


def apply(ctx: OperationContext, params: CreateSourcesParams, plan_obj: Any) -> int:
    from backend.storage.attached_repository import SOURCES_DIR_NAME, _utc_now_iso

    planned, blockers, _ = _resolve(ctx, params)
    if blockers:  # pragma: no cover - the engine re-plans and stops first
        raise RuntimeError("create_sources was applied with unresolved blockers")
    if not planned:
        return 0

    now = _utc_now_iso()
    import_id = uuid.uuid4().hex[:12]
    created_ids: list[str] = []

    for item in planned:
        row = SourceManifestRow(
            id=item.source_id,
            repository_source_id=item.source_id,
            source_kind="url",
            import_type="agent_source_list",
            imported_at=now,
            provenance_ref=f"{import_id}:{item.url}",
            original_url=item.url,
            # `queued` is what the fetch phase selects on. A blank status would
            # also work, but naming it makes the row's state explicit.
            fetch_status="queued",
            title=item.spec.title.strip(),
            title_status="existing" if item.spec.title.strip() else "not_requested",
            notes=item.spec.notes.strip(),
        )

        source_dir = ctx.repo_root / SOURCES_DIR_NAME / item.source_id
        if not source_dir.exists():
            ctx.journal.record_created(source_dir)
        ctx.journal.protect(source_dir / f"{item.source_id}_metadata.json")
        ctx.service._write_repository_source_metadata(row)

        ctx.rows.append(row)
        created_ids.append(item.source_id)

    ctx.rows = ctx.service._sort_rows(ctx.rows)
    ctx.imports.append(
        {
            "import_id": import_id,
            "import_type": "agent_source_list",
            "imported_at": now,
            "provenance_ref": "agent_source_list",
            "source_ids": created_ids,
        }
    )

    try:
        current_next = int(ctx.meta.get("next_source_id") or 1)
    except (TypeError, ValueError):
        current_next = 1
    ctx.meta["next_source_id"] = max(current_next, max(int(i) for i in created_ids) + 1)

    return len(created_ids)


def _implausible_host(url: str) -> str:
    """Describe why a URL's host cannot be real, or "" if it looks fine."""
    from urllib.parse import urlsplit

    try:
        host = urlsplit(url).hostname or ""
    except ValueError:
        return "unparseable host"
    if not host:
        return "no hostname"
    if any(character.isspace() for character in host):
        return "the hostname contains spaces"
    if host == "localhost" or _is_ip_literal(host):
        return ""
    if "." not in host:
        return "the hostname has no dot"
    if host.startswith(".") or host.endswith(".") or ".." in host:
        return "the hostname is malformed"
    return ""


def _is_ip_literal(host: str) -> bool:
    import ipaddress

    try:
        ipaddress.ip_address(host.strip("[]"))
        return True
    except ValueError:
        return False


def _normalize_id(value: str) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if not text.isdigit():
        return None
    number = int(text)
    if number < 1 or number > MAX_SOURCE_ID:
        return None
    return f"{number:06d}"


def _id_range(ids: list[str]) -> str:
    numbers = sorted(int(i) for i in ids)
    if not numbers:
        return ""
    if numbers[-1] - numbers[0] + 1 == len(numbers):
        return f"{numbers[0]:06d}-{numbers[-1]:06d}"
    return f"{numbers[0]:06d}...{numbers[-1]:06d}"


DEFINITION = OperationDefinition(
    name="create_sources",
    title="Create sources from a URL list",
    description=(
        "Add sources to the repository from a list of URLs, each optionally carrying the "
        "id it should take. Use this to bring a spreadsheet's own numbering into the "
        "repository in one step, rather than importing and renumbering afterwards. New "
        "sources are created queued, ready for the fetch phase."
    ),
    params_model=CreateSourcesParams,
    planner=plan,
    applier=apply,
)
