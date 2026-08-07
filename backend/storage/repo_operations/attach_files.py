"""Attach hand-collected files to repository sources.

When a scrape fails, the fix is usually to download the document by hand. This
operation registers those files the way the pipeline would have: into
`sources/<id>/` with the canonical `<id>_` filename, with the row's artifact
field, phase metadata, and fetch status updated so the app treats the source as
fetched rather than queueing it for another failed attempt.

Files are staged in `.ra_repo/inbox/`, which sits inside the internal directory
and is therefore never picked up by the scan-merge on attach.

Planning and applying share `_classify`, so the change set a user approves is
derived by exactly the same code that later executes it.
"""

from __future__ import annotations

import mimetypes
import re
import uuid
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from backend.models.operations import PlanChange, PlanIssue
from backend.models.sources import SourceManifestRow, SourcePhaseMetadata

from .base import OperationDefinition
from .context import OperationContext, file_sha256

INBOX_DIR_NAME = "inbox"

_ID_PREFIX_RE = re.compile(r"^(\d{6})_")

# Which artifact slot a file fills, keyed by the filename convention the
# pipeline itself writes. Matching these keeps the resolver's legacy fallbacks
# coherent with what we write. Order matters: longest match first.
_STEM_SLOTS: tuple[tuple[str, str], ...] = (
    ("_source.ocr.pdf", "ocr_pdf_file"),
    ("_video_info.json", "catalog_file"),
    ("_llm_clean.md", "llm_cleanup_file"),
    ("_rendered.html", "rendered_file"),
    ("_rendered.pdf", "rendered_pdf_file"),
    ("_catalog.json", "catalog_file"),
    ("_summary.md", "summary_file"),
    ("_rating.json", "rating_file"),
    ("_clean.md", "markdown_file"),
)

# Extensions with exactly one sensible slot.
_EXT_SLOTS: dict[str, str] = {
    ".html": "rendered_file",
    ".htm": "rendered_file",
    ".mp4": "video_file",
    ".mov": "video_file",
    ".webm": "video_file",
    ".mkv": "video_file",
    ".mp3": "audio_file",
    ".m4a": "audio_file",
    ".wav": "audio_file",
    ".opus": "audio_file",
    ".jpg": "thumbnail_file",
    ".jpeg": "thumbnail_file",
    ".png": "thumbnail_file",
    ".webp": "thumbnail_file",
}

# Extensions that could reasonably be several things. These need an explicit
# `role` rather than a guess -- putting a summary into `markdown_file` would
# quietly corrupt the source text every later phase reads.
_AMBIGUOUS_EXT: dict[str, tuple[str, ...]] = {
    ".pdf": ("raw_file", "rendered_pdf_file", "ocr_pdf_file"),
    ".md": ("markdown_file", "llm_cleanup_file", "summary_file"),
    ".json": ("catalog_file", "rating_file"),
    ".txt": ("raw_file", "markdown_file"),
}

# Which pipeline phase owns each slot, so attaching marks the right one done.
_SLOT_PHASE: dict[str, str] = {
    "raw_file": "fetch",
    "rendered_file": "fetch",
    "rendered_pdf_file": "fetch",
    "video_file": "fetch",
    "audio_file": "fetch",
    "thumbnail_file": "fetch",
    "ocr_pdf_file": "convert",
    "markdown_file": "convert",
    "llm_cleanup_file": "cleanup",
    "catalog_file": "catalog",
    "summary_file": "summary",
    "rating_file": "rating",
}

WRITABLE_SLOTS = frozenset(_SLOT_PHASE)

ACTION_ATTACH = "attach"
ACTION_CREATE = "create"
ACTION_SKIP = "skip"

# Codes this module raises through a variable rather than a literal, so the
# reference-table generator cannot find them by reading the source. Declared
# here so they still reach the docs; `_path_refusal` returns them as tuples.
EXTRA_BLOCKER_CODES = (
    "path_outside_repository",
    "path_already_managed",
    "path_is_internal_state",
)


class AttachFileHint(BaseModel):
    path: str = ""
    source_id: str = ""
    url: str = ""
    role: str = ""
    overwrite: bool = False
    title: str = ""


class AttachFilesParams(BaseModel):
    scan_inbox: bool = True
    paths: list[str] = Field(default_factory=list)
    hints: list[AttachFileHint] = Field(default_factory=list)
    allow_new_sources: bool = True


class _Candidate:
    """One resolved file and the decision made about it."""

    def __init__(self, path: Path, hint: AttachFileHint, subject: str) -> None:
        self.path = path
        self.hint = hint
        self.subject = subject
        self.sha256 = ""
        self.action = ACTION_SKIP
        self.target_id = ""
        self.slot = ""
        self.existing_rel = ""
        self.replaced_path: Path | None = None
        self.new_rel = ""
        self.title = ""
        # The file is already stored; only the row's bookkeeping needs fixing.
        self.metadata_only = False


# ---------------------------------------------------------------------------
# classification -- the single source of truth for both phases
# ---------------------------------------------------------------------------


def _classify(
    ctx: OperationContext,
    params: AttachFilesParams,
) -> tuple[list[_Candidate], list[PlanIssue], list[PlanIssue]]:
    """Decide what happens to every requested file. Reads only."""
    from backend.storage.attached_repository import (
        SUPPORTED_MANUAL_SOURCE_EXTENSIONS,
        _repository_source_file_path,
        repository_dedupe_key,
    )

    blockers: list[PlanIssue] = []
    warnings: list[PlanIssue] = []

    def block(code: str, message: str, subject: str) -> None:
        blockers.append(PlanIssue(code=code, message=message, subject=subject))

    def warn(code: str, message: str, subject: str) -> None:
        warnings.append(PlanIssue(code=code, message=message, subject=subject))

    candidates = _collect_candidates(ctx, params, block)

    upload_sha: dict[str, SourceManifestRow] = {
        str(row.sha256 or "").strip().lower(): row
        for row in ctx.rows
        if str(row.source_kind or "").lower() == "uploaded_document"
        and str(row.sha256 or "").strip()
    }
    next_id = _next_id(ctx)
    claimed: set[tuple[str, str]] = set()

    for candidate in candidates:
        subject = candidate.subject
        candidate.sha256 = file_sha256(candidate.path)

        # 1. Content we already hold.
        duplicate = upload_sha.get(candidate.sha256)
        if duplicate is not None:
            candidate.action = ACTION_SKIP
            warn(
                "duplicate_sha256",
                f"Identical content is already stored as source {duplicate.id}; skipping.",
                subject,
            )
            continue

        # 2-4. Which source does this belong to?
        row, resolution_failed = _resolve_target(ctx, candidate, repository_dedupe_key, block, subject)
        if resolution_failed:
            continue

        # 5. No match -- become a new source.
        if row is None:
            if not params.allow_new_sources:
                block(
                    "no_target_for_file",
                    "No source matched and creating new sources is disabled. Pass a source_id "
                    "or url for this file.",
                    subject,
                )
                continue
            ext = candidate.path.suffix.lower()
            if ext not in SUPPORTED_MANUAL_SOURCE_EXTENSIONS:
                block(
                    "unsupported_new_source_type",
                    f"{ext or 'This file type'} cannot become a new source. Supported: "
                    + ", ".join(sorted(SUPPORTED_MANUAL_SOURCE_EXTENSIONS))
                    + ". Attach it to an existing source with a source_id instead.",
                    subject,
                )
                continue
            candidate.action = ACTION_CREATE
            candidate.slot = "raw_file"
            candidate.target_id = f"{next_id:06d}"
            candidate.new_rel = _repository_source_file_path(
                source_id=candidate.target_id,
                field="raw_file",
                source_name=safe_source_name(candidate.path.name),
            ).as_posix()
            candidate.title = str(candidate.hint.title or "").strip()
            next_id += 1
            upload_sha[candidate.sha256] = SourceManifestRow(id=candidate.target_id)
            continue

        # 6. Which artifact slot does it fill?
        slot, slot_issue = _select_slot(candidate)
        if slot_issue is not None:
            blockers.append(slot_issue.model_copy(update={"subject": subject}))
            continue

        if (row.id, slot) in claimed:
            block(
                "slot_claimed_twice",
                f"Two files in this request both target {slot} on source {row.id}.",
                subject,
            )
            continue
        claimed.add((row.id, slot))

        candidate.existing_rel = str(getattr(row, slot, "") or "").strip()
        if candidate.existing_rel:
            existing_abs = ctx.service._resolve_repository_artifact_path(
                row, slot, candidate.existing_rel
            )
            if existing_abs is not None and existing_abs.is_file():
                if file_sha256(existing_abs) == candidate.sha256:
                    # Same bytes. Normally nothing to do -- but if the row's
                    # own bookkeeping is still wrong (a fetch left marked
                    # failed, say), re-running should converge it rather than
                    # skip. Attaching is then metadata-only: no file moves.
                    if _resolves_fetch(slot, row.fetch_status):
                        candidate.action = ACTION_ATTACH
                        candidate.target_id = row.id
                        candidate.slot = slot
                        candidate.new_rel = candidate.existing_rel
                        candidate.metadata_only = True
                        continue
                    candidate.action = ACTION_SKIP
                    warn(
                        "already_attached",
                        f"Source {row.id} already holds this exact file as {slot}.",
                        subject,
                    )
                    continue
                if not candidate.hint.overwrite:
                    block(
                        "slot_occupied",
                        f"Source {row.id} already has a {slot} at {candidate.existing_rel} "
                        f"(sha256 {file_sha256(existing_abs)[:12]}). The new file is sha256 "
                        f"{candidate.sha256[:12]}. Re-plan with overwrite set for this file "
                        "to replace it.",
                        subject,
                    )
                    continue
                candidate.replaced_path = existing_abs

        candidate.action = ACTION_ATTACH
        candidate.target_id = row.id
        candidate.slot = slot
        candidate.new_rel = _repository_source_file_path(
            source_id=row.id,
            field=slot,
            source_name=safe_source_name(candidate.path.name),
        ).as_posix()

    return candidates, blockers, warnings


# ---------------------------------------------------------------------------
# planning
# ---------------------------------------------------------------------------


def plan(
    ctx: OperationContext,
    params: AttachFilesParams,
) -> tuple[list[PlanChange], list[PlanIssue], list[PlanIssue], str]:
    candidates, blockers, warnings = _classify(ctx, params)
    changes: list[PlanChange] = []

    if not candidates and not blockers:
        return changes, blockers, warnings, "No files staged in .ra_repo/inbox/."

    for candidate in candidates:
        if candidate.action == ACTION_SKIP:
            continue
        subject = f"source:{candidate.target_id}"

        if candidate.action == ACTION_CREATE:
            changes.append(
                PlanChange(
                    kind="row_create",
                    subject=subject,
                    field="raw_file",
                    after=candidate.new_rel,
                    detail=f"New uploaded-document source from {candidate.subject}",
                )
            )
            changes.append(
                PlanChange(
                    kind="file_move",
                    subject=subject,
                    field="raw_file",
                    before=candidate.subject,
                    after=candidate.new_rel,
                )
            )
            continue

        row = ctx.row_by_id(candidate.target_id)
        changes.append(
            PlanChange(
                kind="file_move",
                subject=subject,
                field=candidate.slot,
                before=candidate.subject,
                after=candidate.new_rel,
                detail=(
                    f"Replaces {candidate.existing_rel} (kept in the operation backup)"
                    if candidate.replaced_path is not None
                    else ""
                ),
            )
        )
        changes.append(
            PlanChange(
                kind="row_field",
                subject=subject,
                field=candidate.slot,
                before=candidate.existing_rel,
                after=candidate.new_rel,
            )
        )
        phase = _SLOT_PHASE.get(candidate.slot, "")
        if phase and row is not None:
            changes.append(
                PlanChange(
                    kind="row_field",
                    subject=subject,
                    field=f"phase_metadata.{phase}.status",
                    before=_phase_status(row, phase),
                    after="completed",
                )
            )
        if row is not None and _resolves_fetch(candidate.slot, row.fetch_status):
            changes.append(
                PlanChange(
                    kind="row_field",
                    subject=subject,
                    field="fetch_status",
                    before=str(row.fetch_status or ""),
                    after="success",
                    detail="Resolves the fetch, so it is not retried or left flagged as failed.",
                )
            )

    attach_count = sum(1 for item in candidates if item.action == ACTION_ATTACH)
    create_count = sum(1 for item in candidates if item.action == ACTION_CREATE)
    skip_count = sum(1 for item in candidates if item.action == ACTION_SKIP)

    parts: list[str] = []
    if attach_count:
        parts.append(f"attach {attach_count} file(s) to existing sources")
    if create_count:
        parts.append(f"create {create_count} new source(s)")
    if skip_count:
        parts.append(f"skip {skip_count} file(s)")
    summary = ("Will " + ", ".join(parts) + ".") if parts else "Nothing to do."

    return changes, blockers, warnings, summary


# ---------------------------------------------------------------------------
# applying
# ---------------------------------------------------------------------------


def apply(ctx: OperationContext, params: AttachFilesParams, plan_obj: Any) -> int:
    from backend.storage.attached_repository import (
        _extract_markdown_seed_title,
        _local_document_detected_type,
        _utc_now_iso,
    )

    candidates, blockers, _ = _classify(ctx, params)
    if blockers:  # pragma: no cover - the engine re-plans and stops first
        raise RuntimeError("attach_files was applied with unresolved blockers")

    journal = ctx.journal
    now = _utc_now_iso()
    import_id = uuid.uuid4().hex[:12]
    applied = 0
    created_ids: list[str] = []
    document_records: list[dict[str, str]] = []

    for candidate in candidates:
        if candidate.action == ACTION_SKIP:
            continue

        destination = ctx.repo_root / candidate.new_rel
        # Record a source directory we bring into existence, so rollback takes
        # the whole thing away again.
        if not destination.parent.exists():
            journal.record_created(destination.parent)
        destination.parent.mkdir(parents=True, exist_ok=True)

        if candidate.action == ACTION_CREATE:
            journal.move(candidate.path, destination)
            ext = candidate.path.suffix.lower()
            title = candidate.title
            if not title and ext == ".md":
                title = _extract_markdown_seed_title(
                    destination.read_text(encoding="utf-8", errors="replace")
                )
            source_name = candidate.path.name

            # Mirror `import_manual_documents` field for field, so a
            # hand-attached document is indistinguishable from an uploaded one.
            new_row = SourceManifestRow(
                id=candidate.target_id,
                repository_source_id=candidate.target_id,
                source_kind="uploaded_document",
                import_type="document_source",
                imported_at=now,
                provenance_ref=f"{import_id}:{source_name}",
                source_document_name=source_name,
                original_url="",
                final_url="",
                fetch_status="not_applicable",
                content_type=mimetypes.guess_type(source_name)[0] or "",
                detected_type=_local_document_detected_type(ext),
                fetch_method="local_upload",
                title=title,
                title_status="extracted" if title else "not_requested",
                raw_file=candidate.new_rel,
                notes="local_document; manual_attach",
                fetched_at=now,
                sha256=candidate.sha256,
            )
            new_row.phase_metadata["fetch"] = SourcePhaseMetadata(
                phase="fetch",
                status="completed",
                completed_at=now,
                content_digest=candidate.sha256,
            )
            journal.protect(destination.parent / f"{candidate.target_id}_metadata.json")
            ctx.service._write_repository_source_metadata(new_row)
            ctx.rows.append(new_row)
            created_ids.append(candidate.target_id)
            document_records.append(
                {
                    "filename": source_name,
                    "repository_path": candidate.new_rel,
                    "sha256": candidate.sha256,
                    "source_id": candidate.target_id,
                }
            )
            applied += 1
            continue

        row = ctx.row_by_id(candidate.target_id)
        if row is None:  # pragma: no cover - classified against these rows
            continue

        if not candidate.metadata_only:
            if candidate.replaced_path is not None and candidate.replaced_path.is_file():
                # Keep a recoverable copy, then clear the way for the move.
                journal.stash(candidate.replaced_path)
                candidate.replaced_path.unlink()
            journal.move(candidate.path, destination)
        setattr(row, candidate.slot, candidate.new_rel)
        applied += 1

        # `sha256` is the identity key for uploaded documents, so it may only
        # ever describe the primary artifact.
        if candidate.slot == "raw_file":
            row.sha256 = candidate.sha256
            row.content_type = mimetypes.guess_type(candidate.path.name)[0] or row.content_type
            # `detected_type` decides how the convert phase reads the file. Left
            # at whatever the original fetch guessed, a PDF attached over a
            # failed HTML fetch is parsed as HTML, and the source's text becomes
            # several million characters of the PDF's own bytes.
            detected = _local_document_detected_type(candidate.path.suffix.lower())
            if detected:
                row.detected_type = detected
            if candidate.replaced_path is not None:
                # The old title describes the artifact just replaced -- for a
                # blocked fetch that is "Attention Required! | Cloudflare".
                # Clear it so it is derived from this file instead.
                row.title = ""
                row.title_status = ""

        phase = _SLOT_PHASE.get(candidate.slot, "")
        if phase:
            row.phase_metadata[phase] = SourcePhaseMetadata(
                phase=phase,
                status="completed",
                completed_at=now,
                content_digest=candidate.sha256,
            )

        if _resolves_fetch(candidate.slot, row.fetch_status):
            # Either the fetch had not run yet, or it failed and this file is
            # the very thing it could not get. A status that already succeeded
            # is left alone: the user is topping up an otherwise good source.
            row.fetch_status = "success"
            row.error_message = ""
            row.fetch_method = "manual_attach"
            row.fetched_at = row.fetched_at or now
        row.notes = _add_note(row.notes, "manual_attach")

        journal.protect(destination.parent / f"{row.id}_metadata.json")
        ctx.service._write_repository_source_metadata(row)

    if created_ids:
        ctx.rows = ctx.service._sort_rows(ctx.rows)
        ctx.imports.append(
            {
                "import_id": import_id,
                "import_type": "manual_attach",
                "imported_at": now,
                "provenance_ref": "manual_attach",
                "source_ids": created_ids,
                "documents": document_records,
            }
        )

    try:
        current_next = int(ctx.meta.get("next_source_id") or 1)
    except (TypeError, ValueError):
        current_next = 1
    ctx.meta["next_source_id"] = max(current_next, _next_id(ctx))

    return applied


# ---------------------------------------------------------------------------
# resolution helpers
# ---------------------------------------------------------------------------


def _collect_candidates(
    ctx: OperationContext,
    params: AttachFilesParams,
    block: Any,
) -> list[_Candidate]:
    """Resolve every requested path, refusing anything outside the safe roots."""
    inbox = ctx.repo_root / ".ra_repo" / INBOX_DIR_NAME

    hints_by_key: dict[str, AttachFileHint] = {}
    requested: list[str] = []
    for raw in params.paths:
        requested.extend(_expand(raw, ctx.repo_root, inbox))
    for hint in params.hints:
        if str(hint.path or "").strip():
            hints_by_key[_normalize_key(hint.path)] = hint
            requested.extend(_expand(hint.path, ctx.repo_root, inbox))

    if params.scan_inbox and inbox.is_dir():
        for item in sorted(inbox.rglob("*")):
            if item.is_file() and not item.name.startswith("."):
                requested.append(str(item))

    seen: set[Path] = set()
    candidates: list[_Candidate] = []

    for raw in requested:
        text = str(raw or "").strip()
        if not text:
            continue
        candidate_path = Path(text)
        if not candidate_path.is_absolute():
            candidate_path = inbox / candidate_path
        # Report paths the way the user sees them in the repository, not as
        # whatever absolute string the inbox scan produced.
        subject = _relative(ctx.repo_root, candidate_path)

        if candidate_path.is_symlink():
            block("symlink_not_allowed", f"{subject} is a symlink; refusing to follow it.", subject)
            continue

        resolved = candidate_path.resolve()

        # Containment is checked before existence, so a traversal attempt is
        # reported as what it is rather than as a missing file.
        refusal = _path_refusal(resolved, ctx.repo_root)
        if refusal:
            block(refusal[0], f"{subject} {refusal[1]}", subject)
            continue

        if not candidate_path.is_file():
            block("file_not_found", f"{subject} is not a file.", subject)
            continue

        if resolved in seen:
            continue
        seen.add(resolved)
        hint = (
            hints_by_key.get(_normalize_key(text))
            or hints_by_key.get(_normalize_key(resolved.name))
            or AttachFileHint(path=text)
        )
        candidates.append(_Candidate(resolved, hint, _relative(ctx.repo_root, resolved)))

    return candidates


def _resolve_target(
    ctx: OperationContext,
    candidate: _Candidate,
    repository_dedupe_key: Any,
    block: Any,
    subject: str,
) -> tuple[SourceManifestRow | None, bool]:
    """Pick the source a file belongs to, in the documented order.

    Returns `(row, failed)`. `failed` distinguishes "no hint matched, so make a
    new source" from "a hint was given but could not be resolved".
    """
    hint = candidate.hint

    if str(hint.source_id or "").strip():
        row = ctx.row_by_id(hint.source_id)
        if row is None:
            block("unknown_source_id", f"No source has id {hint.source_id!r}.", subject)
            return None, True
        return row, False

    if str(hint.url or "").strip():
        key = repository_dedupe_key(hint.url)
        matches = [row for row in ctx.rows if repository_dedupe_key(row.original_url or "") == key]
        if not matches:
            block("url_not_found", f"No source has {hint.url!r} as its fetch URL.", subject)
            return None, True
        if len(matches) > 1:
            block(
                "url_ambiguous",
                f"{hint.url!r} matches sources {', '.join(row.id for row in matches)}. "
                "Use source_id instead.",
                subject,
            )
            return None, True
        return matches[0], False

    match = _ID_PREFIX_RE.match(candidate.path.name)
    if match:
        row = ctx.row_by_id(match.group(1))
        if row is None:
            block(
                "filename_id_not_found",
                f"The filename claims source {match.group(1)}, but no such source exists.",
                subject,
            )
            return None, True
        return row, False

    return None, False


def _select_slot(candidate: _Candidate) -> tuple[str, PlanIssue | None]:
    """Decide which artifact field a file fills."""
    role = str(candidate.hint.role or "").strip()
    if role:
        if role == "metadata_file":
            return "", PlanIssue(
                code="slot_not_writable",
                message="metadata_file is generated from the row and cannot be attached.",
            )
        if role not in WRITABLE_SLOTS:
            return "", PlanIssue(
                code="unknown_role",
                message=f"{role!r} is not an artifact slot. Choose one of: "
                + ", ".join(sorted(WRITABLE_SLOTS))
                + ".",
            )
        return role, None

    name = candidate.path.name.lower()
    for suffix, slot in _STEM_SLOTS:
        if name.endswith(suffix):
            return slot, None

    ext = candidate.path.suffix.lower()
    if ext in _EXT_SLOTS:
        return _EXT_SLOTS[ext], None
    if ext in _AMBIGUOUS_EXT:
        return "", PlanIssue(
            code="ambiguous_slot",
            message=f"A {ext} file could be any of: "
            + ", ".join(_AMBIGUOUS_EXT[ext])
            + ". Set role for this file to say which.",
        )
    return "raw_file", None


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------


def _next_id(ctx: OperationContext) -> int:
    from backend.storage.attached_repository import _next_source_id_from_rows

    try:
        recorded = int(ctx.meta.get("next_source_id") or 1)
    except (TypeError, ValueError):
        recorded = 1
    return max(recorded, _next_source_id_from_rows(ctx.rows))


def _resolves_fetch(slot: str, fetch_status: str) -> bool:
    """Whether attaching this artifact should mark the source as fetched.

    Two different situations, deliberately kept apart:

    * A source that has not been fetched yet (blank or `queued`) is satisfied by
      any artifact -- the user has the content, so re-downloading is waste.
    * A source whose fetch *failed* is only resolved by a fetch-phase artifact.
      A summary or a rating says nothing about whether the page was ever
      retrieved, so the failure should stay visible.
    """
    status = str(fetch_status or "").strip()
    if status in {"", "queued"}:
        return True
    return status == "failed" and _SLOT_PHASE.get(slot) == "fetch"


def _phase_status(row: SourceManifestRow, phase: str) -> str:
    entry = (row.phase_metadata or {}).get(phase)
    return str(getattr(entry, "status", "") or "") if entry is not None else ""


def _add_note(value: str, note: str) -> str:
    parts = [part.strip() for part in str(value or "").split(";") if part.strip()]
    if note not in parts:
        parts.append(note)
    return "; ".join(parts)


_UNSAFE_NAME_CHARS = re.compile(r'[<>:"/\\|?*\x00-\x1f]')
_MAX_STEM = 60


def safe_source_name(name: str) -> str:
    """Make a user-supplied filename safe to store in the repository.

    Browser "Save page as" produces names like
    `Explainer: What is a VPP? | Reuters.html`. Colons and pipes are illegal in
    Windows filenames and awkward in shells and exports, so a repository that
    stored them verbatim would stop being portable. The stem is also capped,
    because the id prefix and slot suffix still have to fit.
    """
    original = Path(str(name or "")).name
    suffix = "".join(Path(original).suffixes[-2:]) if "." in original else ""
    stem = original[: len(original) - len(suffix)] if suffix else original

    stem = _UNSAFE_NAME_CHARS.sub("-", stem)
    stem = re.sub(r"\s+", " ", stem).strip(" .-")
    stem = re.sub(r"-{2,}", "-", stem)
    if len(stem) > _MAX_STEM:
        stem = stem[:_MAX_STEM].rstrip(" .-")
    suffix = _UNSAFE_NAME_CHARS.sub("-", suffix)

    return (stem + suffix) if stem else (suffix.lstrip(".") or "document")


def _expand(raw: str, repo_root: Path, inbox: Path) -> list[str]:
    """Turn one requested path into files, expanding a directory in place.

    Pointing at a folder of hand-saved documents is the natural way to ask for
    this, so it should not require listing every file.
    """
    text = str(raw or "").strip()
    if not text:
        return []
    candidate = Path(text)
    if not candidate.is_absolute():
        candidate = inbox / candidate
    if candidate.is_dir():
        return [
            str(item)
            for item in sorted(candidate.rglob("*"))
            if item.is_file() and not item.name.startswith(".")
        ]
    return [text]


def _path_refusal(resolved: Path, repo_root: Path) -> tuple[str, str] | None:
    """Why a path may not be attached, or None if it is acceptable.

    The invariant that matters is "inside this repository" -- a folder the user
    made in their own repo is a perfectly reasonable place to stage documents.
    What must stay off limits is the managed artifact tree and the internal
    state directory, since attaching from those would either duplicate what the
    repository already owns or feed its own bookkeeping back into itself.
    """
    if not _is_within(resolved, repo_root.resolve()):
        return ("path_outside_repository", "is outside the repository.")

    relative = resolved.relative_to(repo_root.resolve())
    parts = relative.parts

    if parts and parts[0] == "sources":
        return ("path_already_managed", "is already a managed repository artifact.")

    if parts and parts[0] == ".ra_repo":
        # The inbox is the one part of the internal directory meant for this.
        if len(parts) < 2 or parts[1] != INBOX_DIR_NAME:
            return (
                "path_is_internal_state",
                "is internal repository state, not a document.",
            )

    return None


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _normalize_key(value: str) -> str:
    return str(value or "").strip().replace("\\", "/").lstrip("./").lower()


def _relative(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)


DEFINITION = OperationDefinition(
    name="attach_files",
    title="Attach files to sources",
    description=(
        "Register files staged in .ra_repo/inbox/ against repository sources. Each file is "
        "either matched to an existing source (by explicit id, by fetch URL, or by a "
        "000123_ filename prefix) and stored in the right artifact slot, or turned into a "
        "new uploaded-document source. Attaching marks the source fetched so it is not "
        "queued for another download attempt."
    ),
    params_model=AttachFilesParams,
    planner=plan,
    applier=apply,
)
