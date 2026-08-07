"""Start a job and wait for it, without the caller writing a polling loop.

Terminal detection is the subtle part. Two traps this avoids:

* `counts.processed == counts.total` never reliably holds. Rows whose phases end
  in a mix of outcomes match none of the branches in `_build_agent_run_counts`,
  and `processed` is separately floored against a fallback, so the counters do
  not reconcile. Use the job's own `state`.
* A column run reports `state="completed"` even when rows failed — it is only
  `"failed"` when *every* row failed. So the state and the outcome are reported
  as two different things.
"""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from typing import Any

from .models import RunOutcome, na, nas

MAX_WAIT_SECONDS = 120.0
POLL_SECONDS = 3.0
STALL_SECONDS = 600.0
TERMINAL_STATES = frozenset({"completed", "failed", "cancelled"})


# ---------------------------------------------------------------------------
# the breadcrumb
# ---------------------------------------------------------------------------


def _last_run_path(repo_root: Path) -> Path:
    return Path(repo_root) / ".ra_repo" / "workflow" / "last_run.json"


def write_last_run(repo_root: Path, *, kind: str, run_id: str, detail: str = "") -> None:
    """Record which job was just started, so `ra where` can name it.

    Advisory only. Losing this file costs a helpful message, nothing more.
    """
    try:
        path = _last_run_path(repo_root)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps({"kind": kind, "id": run_id, "detail": detail}, ensure_ascii=False),
            encoding="utf-8",
        )
    except OSError:
        pass


def read_last_run(repo_root: Path) -> dict[str, str] | None:
    try:
        return json.loads(_last_run_path(repo_root).read_text(encoding="utf-8"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# source-phase runs
# ---------------------------------------------------------------------------


def start_source_phases(
    service: Any,
    *,
    phases: list[str],
    scope: str = "queued",
    source_ids: list[str] | None = None,
    force: bool = False,
    limit: int | None = None,
    import_id: str = "",
    live_jobs: Any = None,
    live_jobs_lock: Any = None,
) -> str:
    """Start a run and return its id."""
    from backend.models.agent import AgentRunSourcePhasesRequest
    from backend.routers.agent import _to_repository_task_request

    payload = AgentRunSourcePhasesRequest(
        scope=scope,
        import_id=import_id,
        source_ids=list(source_ids or []),
        phases=list(phases),
        force=force,
        limit=limit,
        idempotency_key=uuid.uuid4().hex,
    )
    response = service.start_source_tasks(
        payload=_to_repository_task_request(payload),
        live_jobs=live_jobs,
        live_jobs_lock=live_jobs_lock,
    )
    write_last_run(service.path, kind="source_phases", run_id=response.job_id)
    return response.job_id


def source_run_outcome(
    service: Any,
    run_id: str,
    *,
    live_jobs: Any = None,
    live_jobs_lock: Any = None,
) -> RunOutcome:
    record = service.get_agent_run(run_id, live_jobs=live_jobs, live_jobs_lock=live_jobs_lock)
    outcome = RunOutcome(
        kind="source_phases",
        run_id=run_id,
        state=record.state or ("running" if not record.completed_at else "completed"),
        counts=record.counts.model_dump(mode="json"),
        phase_states={k: str(v.status or "") for k, v in (record.phase_states or {}).items()},
    )
    outcome.terminal = bool(record.terminal) or outcome.state in TERMINAL_STATES
    counts = outcome.counts
    outcome.summary = (
        f"{counts.get('processed', 0)}/{counts.get('total', 0)} processed — "
        f"{counts.get('success', 0)} ok, {counts.get('failed', 0)} failed, "
        f"{counts.get('partial', 0)} partial"
    )
    if outcome.terminal:
        outcome.next = nas("ra triage") if counts.get("failed") else nas("ra where")
    else:
        outcome.next = nas(f"ra watch {run_id}")
    return outcome


# ---------------------------------------------------------------------------
# column runs
# ---------------------------------------------------------------------------


def start_column_run(
    service: Any,
    column_id: str,
    *,
    scope: str = "empty_only",
    source_ids: list[str] | None = None,
    confirm_overwrite: bool = False,
) -> RunOutcome:
    """Start a column run, or report that confirmation is needed."""
    from backend.models.repository import RepositoryColumnRunRequest

    response = service.start_column_run(
        column_id,
        payload=RepositoryColumnRunRequest(
            scope=scope,
            source_ids=list(source_ids or []),
            confirm_overwrite=confirm_overwrite,
        ),
    )
    if str(response.status or "") == "confirmation_required":
        return RunOutcome(
            kind="column",
            column_id=column_id,
            confirmation_required=True,
            state="confirmation_required",
            terminal=True,
            total_rows=response.total_rows,
            summary=(
                f"{response.populated_rows} of {response.total_rows} row(s) already have a "
                "value. Running this scope would overwrite them."
            ),
            next=[na(f"ra run-column {column_id} --scope {scope} --confirm-overwrite --wait")],
        )

    write_last_run(service.path, kind="column", run_id=response.job_id)
    return RunOutcome(
        kind="column",
        run_id=response.job_id,
        column_id=column_id,
        state="running",
        total_rows=response.total_rows,
        summary=f"Started on {response.total_rows} row(s).",
        next=nas(f"ra watch {response.job_id}"),
    )


def column_run_outcome(service: Any, job_id: str) -> RunOutcome:
    status = service.get_column_run_status(job_id)
    outcome = RunOutcome(
        kind="column",
        run_id=job_id,
        column_id=status.column_id,
        column_label=status.column_label,
        state=str(status.state or ""),
        total_rows=status.total_rows,
        processed_rows=status.processed_rows,
        succeeded_rows=status.succeeded_rows,
        failed_rows=status.failed_rows,
        row_errors=[
            {"source_id": e.source_id, "message": e.message} for e in (status.row_errors or [])
        ],
        coerced_rows=int(getattr(status, "coerced_rows", 0) or 0),
        coercions=[
            {"source_id": c.source_id, "returned": c.returned, "stored": c.stored}
            for c in (getattr(status, "coercions", None) or [])
        ],
    )
    outcome.terminal = outcome.state in TERMINAL_STATES
    # `row_errors` stops at 10 while `failed_rows` keeps counting, so say so
    # rather than letting the caller imply it saw every failure.
    outcome.row_errors_truncated = status.failed_rows > len(status.row_errors or [])

    if outcome.terminal:
        if status.failed_rows == 0:
            outcome.outcome = "all_succeeded"
        elif status.succeeded_rows == 0:
            outcome.outcome = "all_failed"
        else:
            outcome.outcome = "partial"
        outcome.summary = (
            f"{status.succeeded_rows} succeeded, {status.failed_rows} failed "
            f"of {status.total_rows}."
        )
        if outcome.row_errors_truncated:
            outcome.summary += (
                f" Showing {len(outcome.row_errors)} of {status.failed_rows} error(s)."
            )
        if outcome.coerced_rows:
            # Said out loud because the cells look fine: the fallback is a
            # valid value, so a column can fill with plausible answers the
            # model never actually chose.
            outcome.summary += (
                f" {outcome.coerced_rows} of {status.total_rows} row(s) stored the column's "
                "fallback because the model declined or answered outside the allowed "
                "values -- check these before trusting the column."
            )
        outcome.next = nas("ra where")
    else:
        outcome.summary = f"{status.processed_rows}/{status.total_rows} rows processed."
        outcome.next = nas(f"ra watch {job_id}")
    return outcome


# ---------------------------------------------------------------------------
# waiting
# ---------------------------------------------------------------------------


# A run is registered a moment before its first status is written, so asking
# for the status immediately can raise even though the run started fine.
STATUS_GRACE_SECONDS = 10.0


def _outcome_once(fetch_outcome, *, grace_seconds: float = STATUS_GRACE_SECONDS) -> RunOutcome:
    """Fetch the outcome, tolerating a status that has not appeared yet.

    Without this, `--wait` reports "Column run status not found" for a run that
    is running perfectly well, and the obvious retry then fails with "a
    repository operation is already running" -- two misleading errors for a
    healthy job.
    """
    deadline = time.monotonic() + max(0.0, grace_seconds)
    while True:
        try:
            return fetch_outcome()
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.25)


def wait_for(
    fetch_outcome,
    *,
    wait_seconds: float = MAX_WAIT_SECONDS,
    poll_seconds: float = POLL_SECONDS,
) -> RunOutcome:
    """Poll until terminal, the budget runs out, or progress stalls.

    The budget matters: a 500-URL fetch outlives any HTTP client, so returning
    `terminal: false` with a `ra watch` instruction is the honest answer rather
    than holding a connection open until something times out.
    """
    budget = max(0.0, min(float(wait_seconds), MAX_WAIT_SECONDS))
    deadline = time.monotonic() + budget
    last_signature = None
    last_change = time.monotonic()

    outcome = _outcome_once(fetch_outcome)
    while not outcome.terminal:
        signature = (outcome.state, outcome.processed_rows, tuple(sorted(outcome.counts.items())))
        if signature != last_signature:
            last_signature, last_change = signature, time.monotonic()
        elif time.monotonic() - last_change > STALL_SECONDS:
            outcome.terminal_reason = "stalled"
            outcome.summary += " No progress for 10 minutes; it may be stuck."
            return outcome

        if time.monotonic() >= deadline:
            outcome.terminal_reason = "wait_budget_exhausted"
            return outcome

        time.sleep(min(poll_seconds, max(0.1, deadline - time.monotonic())))
        outcome = _outcome_once(fetch_outcome)

    return outcome
