"""Answer "where am I?" in one call.

There is no persisted workflow state by design, so this has to substitute for
it. An agent that remembers nothing about the previous step runs this, reads
`next_actions[0]`, and is correct.

Everything here is derived from the repository as it currently stands, which is
what makes it safe: there is no stored progress to go stale, and re-running any
step is harmless because the snapshot simply reflects the new reality.
"""

from __future__ import annotations

import collections
from typing import Any

from .models import ActiveJob, ColumnSummary, NextAction, Orientation, na

MAX_FAILURE_EXAMPLES = 5

# Fetch statuses meaning the document was never obtained. Both need the user to
# act; `blocked` simply names *why* the fetch came back without the page.
UNFETCHED_STATUSES = frozenset({"failed", "blocked"})


def orientation(service: Any, *, include_column_stats: bool = True) -> Orientation:
    from backend.storage.attached_repository import (
        _load_column_configs,
        _load_source_rows,
        build_manifest_record,
    )

    report = Orientation()
    if not getattr(service, "is_attached", False):
        report.summary = "No repository is attached."
        report.next_actions = [na("Open a repository in the app first.")]
        return report

    report.attached = True
    report.repository_path = str(service.path)

    with service._writer_lock():
        state = service._load_state_locked()
        rows = _load_source_rows(state.get("sources", []))
        configs = _load_column_configs(state.get("column_configs", []))
        records = (
            [build_manifest_record(row, base_dir=service.path, column_configs=configs) for row in rows]
            if include_column_stats
            else []
        )

    report.total_sources = len(rows)
    report.sources_by_fetch_status = dict(
        collections.Counter(str(row.fetch_status or "(blank)") for row in rows)
    )
    report.sources_with_markdown = sum(1 for row in rows if row.markdown_file)

    failures = collections.Counter()
    examples: dict[str, list[str]] = {}
    for row in rows:
        # `blocked` is a failure the user has to act on, not a lesser state: the
        # row holds a bot wall instead of the document. Leaving it out here made
        # a repository whose only problem was blocked fetches look clean, and
        # kept `ra triage` out of the suggested next actions.
        if str(row.fetch_status or "") not in UNFETCHED_STATUSES:
            continue
        code = _error_code(row)
        failures[code] += 1
        examples.setdefault(code, [])
        if len(examples[code]) < MAX_FAILURE_EXAMPLES:
            examples[code].append(row.id)
    report.failures_by_code = dict(failures)
    report.failure_examples = examples

    from .staleness import stale_pairs

    stale = stale_pairs(rows)
    for config in configs:
        constraint = getattr(config, "output_constraint", None)
        # A column with no prompt is not run, so it cannot hold a value computed
        # from text and cannot go stale. Reporting one anyway offers a remedy
        # that would overwrite the user's imported data with model output.
        has_prompt = bool((config.instruction_prompt or "").strip())
        summary = ColumnSummary(
            id=config.id,
            label=config.label,
            kind=config.kind,
            has_prompt=has_prompt,
            requires_llm=has_prompt,
            # Maintained by the storage layer on every run. Without it a column
            # that ran and produced nothing is indistinguishable from one never
            # attempted, so `_next_actions` would recommend it forever.
            last_run_status=str(getattr(config, "last_run_status", "") or ""),
            allowed_values=list(getattr(constraint, "allowed_values", None) or []),
            stale_source_ids=sorted(stale.get(config.id, [])) if has_prompt else [],
        )
        if records:
            filled = sum(1 for record in records if str(record.get(config.id, "") or "").strip())
            summary.filled_rows = filled
            summary.empty_rows = len(records) - filled
        report.columns.append(summary)

    try:
        status = service.get_status()
        report.health = {
            "missing_files": int(getattr(status.health, "missing_files", 0) or 0),
            "orphaned_citation_rows": int(
                getattr(status.health, "orphaned_citation_rows", 0) or 0
            ),
        }
        download_state = str(status.download_state or "idle")
    except Exception:  # pragma: no cover - defensive
        download_state = "unknown"

    report.active_job = _active_job(service, download_state)
    report.summary = _summarize(report)
    report.next_actions = _next_actions(report)
    return report


def _error_code(row: Any) -> str:
    entry = (getattr(row, "phase_metadata", None) or {}).get("fetch")
    code = str(getattr(entry, "error_code", "") or "") if entry is not None else ""
    if code:
        return code
    from backend.pipeline.source_downloader import _phase_error_code

    return _phase_error_code(str(row.error_message or "")) or "unknown"


def _active_job(service: Any, download_state: str) -> ActiveJob:
    running = download_state in {"running", "cancelling"}
    job = ActiveJob(running=running, state=download_state)
    if not running:
        return job

    # A breadcrumb, written when the workflow layer starts a run, so the agent
    # can be told which id to watch. Advisory: if it is missing or stale we
    # still report that something is running.
    from .runs import read_last_run

    last = read_last_run(service.path)
    if last:
        job.kind = str(last.get("kind") or "")
        job.job_id = str(last.get("id") or "")
        job.detail = f"Watch it with: ra watch {job.job_id}"
    else:
        job.detail = "A job is running but its id was not recorded. Check the app."
    return job


def _summarize(report: Orientation) -> str:
    if not report.total_sources:
        return "Repository is empty: no sources yet."

    by_status = report.sources_by_fetch_status
    parts = [f"{report.total_sources} source(s)"]
    for key in ("success", "partial", "failed", "blocked", "queued"):
        if by_status.get(key):
            parts.append(f"{by_status[key]} {key}")
    prompted = sum(1 for c in report.columns if c.has_prompt)
    if report.columns:
        unrun = sum(1 for c in report.columns if c.has_prompt and c.filled_rows == 0)
        parts.append(f"{prompted} prompted column(s)" + (f", {unrun} never run" if unrun else ""))
    if report.active_job.running:
        parts.append(f"a job is {report.active_job.state}")
    stale = sum(len(c.stale_source_ids) for c in report.columns)
    if stale:
        parts.append(f"{stale} value(s) computed from text that has since been rebuilt")
    return ", ".join(parts) + "."


def _never_run(column: ColumnSummary) -> bool:
    """Has this column genuinely not been attempted?

    `filled_rows == 0` alone is not enough: a column that ran and produced
    nothing looks identical to one never tried, and recommending it again on
    every orientation is an infinite loop. `last_run_status` is what tells them
    apart.
    """
    return column.filled_rows == 0 and not column.last_run_status


def _first_to_run(prompted: list[ColumnSummary]) -> ColumnSummary | None:
    """Pick the column worth running first.

    A column whose prompt lists exact allowed answers is the one that shows a
    misread prompt after one run instead of after fourteen, so it is preferred
    over sheet order.
    """
    never_run = [c for c in prompted if _never_run(c)]
    if not never_run:
        return None
    return next((c for c in never_run if c.allowed_values), never_run[0])


def _next_actions(report: Orientation) -> list[NextAction]:
    """The ordered list of literal commands that stands in for stored progress.

    Each carries its own gate, because the model reads this rather than the
    skill's prose.
    """
    if report.active_job.running:
        return [na(f"ra watch {report.active_job.job_id}" if report.active_job.job_id else "ra where")]

    actions: list[NextAction] = []
    if not report.total_sources:
        # The dry run first, deliberately: `--apply` is offered only once the
        # user has seen what it would do.
        return [na("ra plan-sheet <spreadsheet>"), na("ra create-sources")]

    queued = report.sources_by_fetch_status.get("queued", 0) + report.sources_by_fetch_status.get(
        "(blank)", 0
    )
    if queued:
        actions.append(
            na("ra fetch --wait", why=f"This downloads {queued} source(s) and takes a while.")
        )
    if report.failures_by_code:
        actions.append(na("ra triage"))

    prompted = [c for c in report.columns if c.has_prompt]

    # Ahead of any new column: these cells are filled, so `--scope empty_only`
    # will never revisit them and nothing else would ever mention them again.
    for column in prompted:
        if column.stale_source_ids and not queued:
            ids = ",".join(column.stale_source_ids)
            actions.append(
                na(
                    f"ra run-column {column.id} --scope selected --ids {ids} "
                    "--confirm-overwrite --wait",
                    why=(
                        f"{len(column.stale_source_ids)} value(s) in {column.label!r} were "
                        "computed from text that has since been rebuilt, so they are wrong "
                        "until re-run."
                    ),
                )
            )

    if not prompted:
        actions.append(na("ra create-columns"))
    elif not queued:
        # The bulk path a weak model should follow: full-run does the steps in
        # the order columns depend on -- refresh fetch signals (date_signals),
        # analyse images, then run every column -- so date/visual columns are
        # never coded before their inputs exist. It is gated, so the user still
        # confirms the spend.
        unfilled = [c for c in prompted if (getattr(c, "empty_rows", 0) or 0) > 0]
        if unfilled:
            actions.append(
                na(
                    "ra full-run",
                    why=(
                        f"{len(unfilled)} column(s) have empty cells. full-run refreshes fetch "
                        "signals and images, then runs every column over "
                        f"{report.total_sources} source(s) -- many model calls."
                    ),
                )
            )
        first = _first_to_run(prompted)
        if first is not None:
            actions.append(
                na(
                    f"ra run-column {first.id} --wait",
                    why=(
                        f"Cautious alternative: run just {first.label!r} first "
                        f"({report.total_sources} model calls) to check its prompt before a full run."
                    ),
                )
            )

    if not actions:
        actions.append(na("ra where"))
    return actions
