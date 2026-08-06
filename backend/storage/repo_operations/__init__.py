"""Transactional repository operations.

An operation is a named mutation split into two phases:

  plan(params)  -- strictly read-only. Returns the concrete change set plus
                   `blockers` (hard stops) and `warnings`, so a human or an
                   agent can review the exact consequences first.
  apply(params) -- takes a state snapshot, journals every file move, writes,
                   re-verifies from disk, and rolls back on any regression.

The safety property is structural, not behavioural: an operation cannot leave
the repository in a state that is worse than it found it, because the engine
compares post-conditions against a baseline taken before the write and undoes
everything if any new violation appears.

The writer lock is held by the caller for the whole of `apply`. Nothing in
this package may call a lock-taking *public* service method -- `_writer_lock`
opens a fresh file descriptor per entry, so a second acquisition from the same
process deadlocks on `flock`.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import ValidationError

from backend.models.operations import (
    OperationDescriptor,
    OperationPlan,
    OperationResult,
    PlanIssue,
    VerifyIssue,
)

from .base import OperationDefinition
from .context import (
    OperationContext,
    load_context_locked,
    params_fingerprint,
    save_context_locked,
    state_fingerprint_locked,
)
from .journal import MoveJournal, operations_dir
from .verify import issue_signature, verify_repository_locked

__all__ = [
    "OPERATIONS",
    "OperationContext",
    "apply_operation_locked",
    "list_operations",
    "load_operation_result",
    "params_fingerprint",
    "plan_operation_locked",
    "recover_incomplete_operations_locked",
    "state_fingerprint_locked",
]


def _build_registry() -> dict[str, OperationDefinition]:
    # Imported here so the operation modules can import from this package.
    from .attach_files import DEFINITION as ATTACH_FILES
    from .create_columns import DEFINITION as CREATE_COLUMNS
    from .create_sources import DEFINITION as CREATE_SOURCES
    from .remap_source_ids import DEFINITION as REMAP_SOURCE_IDS
    from .set_column_constraints import DEFINITION as SET_COLUMN_CONSTRAINTS

    return {
        item.name: item
        for item in (
            CREATE_SOURCES,
            CREATE_COLUMNS,
            SET_COLUMN_CONSTRAINTS,
            REMAP_SOURCE_IDS,
            ATTACH_FILES,
        )
    }


_REGISTRY: dict[str, OperationDefinition] | None = None


def _registry() -> dict[str, OperationDefinition]:
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = _build_registry()
    return _REGISTRY


class OPERATIONS:
    """Registry accessor. `OPERATIONS.names()` is the canonical name list."""

    @staticmethod
    def names() -> list[str]:
        return sorted(_registry())

    @staticmethod
    def get(name: str) -> OperationDefinition | None:
        return _registry().get(str(name or "").strip())

    @staticmethod
    def __contains__(name: str) -> bool:  # pragma: no cover - convenience
        return str(name or "").strip() in _registry()


def list_operations() -> list[OperationDescriptor]:
    return [
        OperationDescriptor(
            name=item.name,
            title=item.title,
            description=item.description,
            input_schema=item.params_model.model_json_schema(),
            mutating=True,
        )
        for _, item in sorted(_registry().items())
    ]


class UnknownOperationError(ValueError):
    pass


def _resolve(operation: str) -> OperationDefinition:
    definition = OPERATIONS.get(operation)
    if definition is None:
        raise UnknownOperationError(f"Unknown operation: {operation}")
    return definition


def _parse_params(definition: OperationDefinition, params: dict[str, Any]) -> Any | list[PlanIssue]:
    try:
        return definition.params_model.model_validate(params or {})
    except ValidationError as exc:
        issues: list[PlanIssue] = []
        for error in exc.errors():
            location = ".".join(str(part) for part in error.get("loc", ()))
            issues.append(
                PlanIssue(
                    code="invalid_params",
                    message=error.get("msg", "Invalid value"),
                    subject=location,
                )
            )
        return issues


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


def plan_operation_locked(
    service: Any,
    operation: str,
    params: dict[str, Any],
) -> OperationPlan:
    """Build a change set without touching anything. Lock must be held."""
    from backend.storage.attached_repository import _utc_now_iso

    definition = _resolve(operation)
    plan = OperationPlan(
        operation=definition.name,
        plan_id=uuid.uuid4().hex[:12],
        created_at=_utc_now_iso(),
        state_fingerprint=state_fingerprint_locked(service),
        params=dict(params or {}),
    )

    parsed = _parse_params(definition, params)
    if isinstance(parsed, list):
        plan.blockers = parsed
        plan.summary = "The request could not be understood."
        return plan

    ctx = load_context_locked(service)
    changes, blockers, warnings, summary = definition.planner(ctx, parsed)
    plan.changes = changes
    plan.blockers = blockers
    plan.warnings = warnings
    plan.summary = summary
    return plan


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


def apply_operation_locked(
    service: Any,
    operation: str,
    params: dict[str, Any],
    *,
    run_id: str = "",
    expected_fingerprint: str = "",
) -> OperationResult:
    """Plan, snapshot, mutate, verify, and roll back on regression.

    The writer lock must already be held by the caller and must not be
    re-entered anywhere below.
    """
    definition = _resolve(operation)
    run_id = str(run_id or "").strip() or uuid.uuid4().hex[:12]

    result = OperationResult(operation=definition.name, run_id=run_id)

    # 1. Refuse to race a background worker. Its `_save_state_locked` would
    #    land after our snapshot, and a rollback would silently discard it.
    busy = _busy_reason(service)
    if busy:
        result.status = "blocked"
        result.plan = OperationPlan(
            operation=definition.name,
            params=dict(params or {}),
            blockers=[PlanIssue(code="repository_busy", message=busy, subject="repository")],
        )
        result.message = busy
        return result

    # 2. Reject a plan built against a state that has since moved.
    current_fingerprint = state_fingerprint_locked(service)
    if expected_fingerprint and expected_fingerprint != current_fingerprint:
        result.status = "blocked"
        result.plan = OperationPlan(
            operation=definition.name,
            params=dict(params or {}),
            state_fingerprint=current_fingerprint,
            blockers=[
                PlanIssue(
                    code="state_changed",
                    message=(
                        "The repository changed after this plan was created. "
                        "Re-plan and review the new change set before applying."
                    ),
                    subject="repository",
                )
            ],
        )
        result.message = "The repository changed since the plan was created."
        return result

    # 3. Re-plan from scratch; never trust a client-supplied change set.
    plan = plan_operation_locked(service, definition.name, params)
    result.plan = plan
    if plan.blockers:
        result.status = "blocked"
        result.message = plan.summary or "The operation was blocked."
        _store_result(service, run_id, result)
        return result
    if not plan.changes:
        result.status = "noop"
        result.verify_passed = True
        result.message = plan.summary or "Nothing to do."
        _store_result(service, run_id, result)
        return result

    parsed = _parse_params(definition, params)
    if isinstance(parsed, list):  # pragma: no cover - caught in plan already
        result.status = "blocked"
        result.plan.blockers = parsed
        return result

    # Baseline the invariants. Pre-existing damage must not make every future
    # operation impossible, so only *new* violations trigger a rollback.
    baseline_ctx = load_context_locked(service)
    baseline = {
        issue_signature(issue) for issue in verify_repository_locked(service, baseline_ctx)
    }
    baseline |= _remapped_baseline(definition, baseline_ctx, parsed, baseline)

    backup_dir = service._create_backup_snapshot_locked(f"pre_{definition.name}")
    result.backup_dir = str(backup_dir or "")

    journal = MoveJournal(service.path, run_id)
    journal.begin(operation=definition.name, state_backup_dir=backup_dir)

    try:
        ctx = load_context_locked(service, journal=journal)
        applied = definition.applier(ctx, parsed, plan)
        save_context_locked(ctx)

        # Verify what actually landed on disk, not what the applier believed.
        fresh = load_context_locked(service)
        issues = verify_repository_locked(service, fresh)
        regressions = [issue for issue in issues if issue_signature(issue) not in baseline]
    except BaseException as exc:  # noqa: BLE001 - rollback then re-raise nothing
        result.status = "rolled_back"
        result.rollback_ok = _rollback(service, journal)
        result.rollback_performed = True
        result.message = f"{type(exc).__name__}: {exc}"
        _store_result(service, run_id, result)
        return result

    if regressions:
        result.status = "rolled_back"
        result.verify_issues = regressions
        result.rollback_ok = _rollback(service, journal)
        result.rollback_performed = True
        result.message = (
            f"Verification found {len(regressions)} new problem(s); "
            "the repository was restored and nothing was changed."
        )
        _store_result(service, run_id, result)
        return result

    journal.commit()
    result.status = "applied"
    result.applied_changes = applied
    result.verify_passed = True
    result.message = plan.summary or f"Applied {applied} change(s)."
    _store_result(service, run_id, result)
    return result


def _remapped_baseline(
    definition: OperationDefinition,
    ctx: OperationContext,
    params: Any,
    baseline: set[str],
) -> set[str]:
    """Translate baseline signatures through an operation's id remapping.

    Without this, renumbering source 000001 to 000007 makes every pre-existing
    issue on 000001 look like a brand-new issue on 000007, and the operation
    rolls itself back for damage it did not cause.
    """
    if definition.identity_remap is None:
        return set()
    try:
        id_map = definition.identity_remap(ctx, params)
    except Exception:  # pragma: no cover - defensive
        return set()
    if not id_map:
        return set()

    translated: set[str] = set()
    for signature in baseline:
        code, _, subject = signature.partition("|")
        for old_id, new_id in id_map.items():
            if old_id and old_id in subject:
                translated.add(f"{code}|{subject.replace(old_id, new_id)}")
    return translated


def _busy_reason(service: Any) -> str:
    thread = getattr(service, "_download_thread", None)
    if thread is not None and getattr(thread, "is_alive", lambda: False)():
        return (
            "A repository job is currently running. Wait for it to finish before "
            "applying an operation."
        )
    return ""


def _rollback(service: Any, journal: MoveJournal) -> bool:
    """Undo a partial apply: files first, then state, then derived artifacts."""
    files_ok = journal.rollback()
    state_ok = journal.restore_state_files()

    outputs_ok = True
    try:
        ctx = load_context_locked(service)
        service._rebuild_outputs_locked(ctx.rows, ctx.citations)
    except Exception:  # pragma: no cover - defensive
        outputs_ok = False

    return bool(files_ok and state_ok and outputs_ok)


# ---------------------------------------------------------------------------
# result storage and crash recovery
# ---------------------------------------------------------------------------


def _store_result(service: Any, run_id: str, result: OperationResult) -> None:
    try:
        run_dir = operations_dir(service.path) / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "result.json").write_text(
            result.model_dump_json(indent=2),
            encoding="utf-8",
        )
    except OSError:  # pragma: no cover - best effort
        pass


def load_operation_result(repo_root: Any, run_id: str) -> OperationResult | None:
    from pathlib import Path

    path = operations_dir(Path(repo_root)) / str(run_id or "") / "result.json"
    if not path.is_file():
        return None
    try:
        return OperationResult.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def recover_incomplete_operations_locked(service: Any) -> list[str]:
    """Roll back any operation left mid-apply by a crash. Lock must be held.

    Runs from `_ensure_scaffold_locked`, which is the one place guaranteed to
    execute under the writer lock on every attach and create.
    """
    recovered: list[str] = []
    for journal in MoveJournal.find_incomplete(service.path):
        journal.rollback()
        journal.restore_state_files()
        recovered.append(journal.run_id)

    if recovered:
        try:
            ctx = load_context_locked(service)
            service._rebuild_outputs_locked(ctx.rows, ctx.citations)
        except Exception:  # pragma: no cover - defensive
            pass
    return recovered


def verify_repository(service: Any) -> list[VerifyIssue]:
    """Public health check over the current state. Lock must be held."""
    return verify_repository_locked(service, load_context_locked(service))
