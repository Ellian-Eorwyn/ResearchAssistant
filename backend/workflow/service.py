"""The one entry point every surface goes through.

The REST router, the MCP dispatcher and the bundled `ra` CLI all call this and
nothing else. That is what keeps a shell-driven agent and an MCP-driven agent
behaving identically instead of drifting into two implementations of the same
workflow.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from . import WORKFLOW_CONTRACT_VERSION, staleness
from .models import (
    AttachOutcome,
    Orientation,
    Preflight,
    RunOutcome,
    SheetPlan,
    TriageReport,
    na,
    nas,
)
from .orientation import orientation as _orientation
from .preflight import run_preflight as _run_preflight
from .runs import (
    column_run_outcome,
    source_run_outcome,
    start_column_run,
    start_source_phases,
    wait_for,
)
from .sheet import (
    parse_planning_sheet,
    sheet_plan_to_create_columns_params,
    sheet_plan_to_create_sources_params,
)
from .triage import triage_failures as _triage_failures


class WorkflowService:
    def __init__(self, repository_service: Any, *, app_state: Any = None) -> None:
        self.repository = repository_service
        self.app_state = app_state

    # -- meta -------------------------------------------------------------

    def version(self) -> dict[str, Any]:
        attached = bool(getattr(self.repository, "is_attached", False))
        return {
            "contract_version": WORKFLOW_CONTRACT_VERSION,
            "attached": attached,
            # `path` raises when nothing is attached, so it is only read here.
            "repository_path": str(self.repository.path) if attached else "",
        }

    # -- read-only --------------------------------------------------------

    def preflight(self, *, refresh_capabilities: bool = False) -> Preflight:
        settings = self.repository.load_effective_settings()
        return _run_preflight(
            self.repository, settings, refresh_capabilities=refresh_capabilities
        )

    def orientation(self, *, include_column_stats: bool = True) -> Orientation:
        return _orientation(self.repository, include_column_stats=include_column_stats)

    def triage(self, *, phase: str = "fetch") -> TriageReport:
        return _triage_failures(self.repository, phase=phase)

    def parse_sheet(
        self,
        path: str,
        *,
        header_row: int | None = None,
        prompts_row: int | None = None,
        no_prompts_row: bool = False,
        repair_encoding: str = "auto",
    ) -> SheetPlan:
        return parse_planning_sheet(
            Path(path),
            header_row=header_row,
            prompts_row=prompts_row,
            no_prompts_row=no_prompts_row,
            repair_encoding=repair_encoding,
        )

    def sheet_to_params(self, plan: SheetPlan, kind: str) -> dict[str, Any]:
        if kind == "create_sources":
            return sheet_plan_to_create_sources_params(plan)
        if kind == "create_columns":
            return sheet_plan_to_create_columns_params(plan)
        raise ValueError(f"Unknown params kind: {kind}")

    # -- operations -------------------------------------------------------

    def run_operation(
        self,
        operation: str,
        params: dict[str, Any],
        *,
        apply: bool = False,
        idempotency_key: str = "",
    ) -> dict[str, Any]:
        """Plan, and optionally apply, in one round trip.

        Taking and consuming the fingerprint server-side closes the window where
        the repository could change between a caller's plan and its apply.
        `apply=True` still re-plans internally and refuses on blockers, so an
        over-eager call cannot skip review of a problem.
        """
        plan = self.repository.plan_repo_operation(operation, params)
        payload: dict[str, Any] = {
            "operation": operation,
            "applied": False,
            "plan": plan.model_dump(mode="json"),
        }

        def out(actions):
            return [action.model_dump(mode="json") for action in actions]

        if plan.blockers:
            payload["summary"] = plan.summary or "Blocked."
            payload["next"] = out(nas("Fix the blockers listed above, then run this again."))
            return payload
        if not apply:
            payload["summary"] = plan.summary
            # Contains `--apply`, so this gates itself.
            payload["next"] = out(
                [
                    na(
                        f"Re-run with --apply to make these {len(plan.changes)} change(s).",
                        why="Show the user this plan and get their agreement first.",
                    )
                ]
            )
            return payload
        if not plan.changes:
            payload["summary"] = plan.summary or "Nothing to do."
            payload["next"] = out(nas("ra where"))
            return payload

        result = self.repository.apply_repo_operation(
            operation,
            params,
            expected_fingerprint=plan.state_fingerprint,
        )
        payload["applied"] = result.status == "applied"
        payload["result"] = result.model_dump(mode="json")
        payload["summary"] = result.message or result.status
        payload["next"] = out(
            nas("ra where") if payload["applied"] else nas("ra doctor", "ra where")
        )

        if idempotency_key and result.run_id:
            try:
                fingerprint = self.repository.repo_operation_fingerprint(operation, params)
                self.repository.remember_agent_idempotency(
                    idempotency_key, fingerprint, result.run_id
                )
            except Exception:
                pass
        return payload

    # -- jobs -------------------------------------------------------------

    def _live(self) -> tuple[Any, Any]:
        state = self.app_state
        return (
            getattr(state, "source_download_jobs", None),
            getattr(state, "source_download_lock", None),
        )

    def run_source_phases(
        self,
        *,
        phases: list[str],
        scope: str = "queued",
        source_ids: list[str] | None = None,
        force: bool = False,
        limit: int | None = None,
        import_id: str = "",
        wait_seconds: float = 0.0,
    ) -> RunOutcome:
        jobs, lock = self._live()
        # Taken before the run so a convert that rewrites a source's text under
        # values already computed from the old text can be spotted afterwards.
        before = staleness.snapshot(self.repository) if "convert" in phases else {}
        run_id = start_source_phases(
            self.repository,
            phases=phases,
            scope=scope,
            source_ids=source_ids,
            force=force,
            limit=limit,
            import_id=import_id,
            live_jobs=jobs,
            live_jobs_lock=lock,
        )
        fetch = lambda: source_run_outcome(  # noqa: E731 - a tiny closure over the ids
            self.repository, run_id, live_jobs=jobs, live_jobs_lock=lock
        )
        outcome = wait_for(fetch, wait_seconds=wait_seconds) if wait_seconds else fetch()
        # Only once the run has actually finished: a mid-flight comparison would
        # mark sources the run has not reached yet.
        if before and outcome.terminal:
            staleness.mark_stale(self.repository, before)
        return outcome

    def run_column(
        self,
        column_id: str,
        *,
        scope: str = "empty_only",
        source_ids: list[str] | None = None,
        confirm_overwrite: bool = False,
        wait_seconds: float = 0.0,
    ) -> RunOutcome:
        started = start_column_run(
            self.repository,
            column_id,
            scope=scope,
            source_ids=source_ids,
            confirm_overwrite=confirm_overwrite,
        )
        if started.confirmation_required or not started.run_id or not wait_seconds:
            return started
        return wait_for(
            lambda: column_run_outcome(self.repository, started.run_id),
            wait_seconds=wait_seconds,
        )

    def watch(self, run_id: str, *, wait_seconds: float = 0.0) -> RunOutcome:
        """Report on a run without needing to be told which kind it is."""
        jobs, lock = self._live()
        try:
            fetch = lambda: column_run_outcome(self.repository, run_id)  # noqa: E731
            outcome = fetch()
        except Exception:
            fetch = lambda: source_run_outcome(  # noqa: E731
                self.repository, run_id, live_jobs=jobs, live_jobs_lock=lock
            )
            outcome = fetch()
        if wait_seconds and not outcome.terminal:
            return wait_for(fetch, wait_seconds=wait_seconds)
        return outcome

    # -- attach -----------------------------------------------------------

    def attach_files(
        self,
        *,
        paths: list[str] | None = None,
        hints: list[dict[str, Any]] | None = None,
        scan_inbox: bool = True,
        allow_new_sources: bool = True,
        apply: bool = False,
        reconvert: bool = True,
        wait_seconds: float = 0.0,
    ) -> AttachOutcome:
        """Attach documents and, on apply, rebuild their text.

        The re-convert is the point. Without it the attached page sits alongside
        markdown extracted from whatever error page the failed fetch returned,
        and every later phase reads that stale text. Knowing to run a forced
        convert afterwards was the single most obscure step in this workflow.
        """
        params = {
            "scan_inbox": scan_inbox,
            "paths": list(paths or []),
            "hints": list(hints or []),
            "allow_new_sources": allow_new_sources,
        }
        payload = self.run_operation("attach_files", params, apply=apply)
        outcome = AttachOutcome(
            applied=bool(payload.get("applied")),
            status=str((payload.get("result") or {}).get("status") or "planned"),
            plan=payload.get("plan") or {},
            summary=str(payload.get("summary") or ""),
            next=list(payload.get("next") or []),
        )

        touched = sorted(
            {
                change.get("subject", "").split(":", 1)[1]
                for change in outcome.plan.get("changes", [])
                if change.get("subject", "").startswith("source:")
            }
        )
        outcome.attached_source_ids = touched

        if outcome.applied and reconvert and touched:
            outcome.reconvert = self.run_source_phases(
                phases=["convert"],
                source_ids=touched,
                force=True,
                wait_seconds=wait_seconds or 60.0,
            )
            outcome.summary += f" Rebuilt text for {len(touched)} source(s)."
            outcome.next = outcome.reconvert.next
        return outcome
