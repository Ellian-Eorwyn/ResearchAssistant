"""Group failed sources into what to do about them.

Reads `phase_metadata[phase].error_code`, which the pipeline already populated —
there is no need to split `error_message` on the colon, and doing so by hand is
one of the things that went wrong when this was written as prose guidance.

Each group comes back with a complete remedy command, ids already filled in, so
the answer is copied rather than composed.
"""

from __future__ import annotations

from typing import Any

from .codes import classify, remedy_for
from .models import FailureExample, FailureGroup, TriageReport, na, nas

MAX_EXAMPLES = 4
MAX_IDS_PER_GROUP = 200


def triage_failures(service: Any, *, phase: str = "fetch", limit: int = 500) -> TriageReport:
    from backend.storage.attached_repository import _load_source_rows

    report = TriageReport(phase=phase)
    if not getattr(service, "is_attached", False):
        report.summary = "No repository attached."
        return report

    with service._writer_lock():
        rows = _load_source_rows(service._load_state_locked().get("sources", []))

    failed = [row for row in rows if _phase_failed(row, phase)][:limit]
    report.total_failed = len(failed)
    if not failed:
        report.summary = f"No {phase} failures."
        report.next = nas("ra where")
        return report

    buckets: dict[tuple[str, str], list[Any]] = {}
    for row in failed:
        code, detail = _phase_error(row, phase)
        _, _, pattern = classify(code, detail)
        buckets.setdefault((code, pattern), []).append(row)

    for (code, pattern), group_rows in sorted(buckets.items(), key=lambda kv: -len(kv[1])):
        detail = _phase_error(group_rows[0], phase)[1]
        classification, explanation, _ = classify(code, detail)
        ids = [row.id for row in group_rows][:MAX_IDS_PER_GROUP]
        report.groups.append(
            FailureGroup(
                error_code=code or "unknown",
                detail_pattern=pattern,
                classification=classification,
                count=len(group_rows),
                source_ids=ids,
                examples=[
                    FailureExample(
                        id=row.id,
                        url=row.original_url or row.final_url,
                        error=_phase_error(row, phase)[1] or row.error_message,
                    )
                    for row in group_rows[:MAX_EXAMPLES]
                ],
                explanation=explanation,
                remedy_command=remedy_for(classification, ids),
            )
        )

    retryable = sum(g.count for g in report.groups if g.classification.startswith("retryable"))
    manual = sum(g.count for g in report.groups if g.classification == "needs_manual_document")
    broken = sum(g.count for g in report.groups if g.classification == "broken_url")

    parts = [f"{report.total_failed} failed"]
    if retryable:
        parts.append(f"{retryable} worth retrying")
    if manual:
        parts.append(f"{manual} needing a manual download")
    if broken:
        parts.append(f"{broken} with a bad URL")
    report.summary = ", ".join(parts) + "."
    report.next = [na(g.remedy_command) for g in report.groups if g.remedy_command][:3] or nas(
        "ra where"
    )
    return report


def _phase_error(row: Any, phase: str) -> tuple[str, str]:
    """The code and detail for a phase, preferring the structured record."""
    entry = (getattr(row, "phase_metadata", None) or {}).get(phase)
    if entry is not None:
        code = str(getattr(entry, "error_code", "") or "")
        detail = str(getattr(entry, "error", "") or "")
        if code:
            return code, detail

    # Rows written before phase metadata existed only have the joined string.
    from backend.pipeline.source_downloader import _phase_error_code

    message = str(getattr(row, "error_message", "") or "")
    return _phase_error_code(message), message


def _phase_failed(row: Any, phase: str) -> bool:
    """Whether this row still needs attention for the given phase.

    For fetch, the row's `fetch_status` overrules the phase record. Phase
    metadata is written once per run and is not revised when a later run, or a
    hand-attached document, fixes the source -- so trusting it alone reports
    sources that are already fine and sends the user chasing them.
    """
    if phase == "fetch":
        status = str(getattr(row, "fetch_status", "") or "")
        if status in {"success", "partial", "not_applicable"}:
            return False
        if status == "failed":
            return True

    entry = (getattr(row, "phase_metadata", None) or {}).get(phase)
    return entry is not None and str(getattr(entry, "status", "") or "") == "failed"
