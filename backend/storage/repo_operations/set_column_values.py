"""Fill a column with values the user supplied, rather than ones a model wrote.

A planning spreadsheet usually carries two kinds of column. Most are questions
for the model, and `create_columns` turns those into columns with prompts. But
some hold work the user already did -- the date a link was collected, the search
tool that surfaced it -- and those have a heading and data but no prompt. Until
this existed there was nowhere to put them: `create_columns` makes the column,
and nothing at all writes a cell.

Values live in `SourceManifestRow.custom_fields`, keyed by column id, which is
where a column run writes them too. So an imported column is an ordinary column
in every later respect: it filters, sorts, and exports like any other.

Kept apart from `create_columns` rather than folded into it as a `values` field,
because that operation skips a label that already exists -- so a re-run, which
is exactly when a user is fixing or extending their data, would skip the column
and silently never write the values.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from backend.models.operations import PlanChange, PlanIssue

from .base import OperationDefinition
from .context import OperationContext

# Enough of the change list to review the shape of an import without printing a
# line per source for a repository of several hundred.
MAX_DETAILED_CHANGES = 8


class SetColumnValuesParams(BaseModel):
    # Either names the column. The label is what a spreadsheet knows about.
    column_id: str = ""
    column_label: str = ""
    # source id -> value. Ids are the repository's own six-digit form.
    values: dict[str, str] = Field(default_factory=dict)
    # Off by default: a cell that already holds something was either imported
    # earlier or written by a run, and neither should be replaced by surprise.
    overwrite: bool = False


class _Planned:
    def __init__(self, row: Any, before: str, after: str) -> None:
        self.row = row
        self.before = before
        self.after = after


def _normalize_label(value: str) -> str:
    return " ".join(str(value or "").split()).strip()


def _resolve(
    ctx: OperationContext,
    params: SetColumnValuesParams,
) -> tuple[Any, list[_Planned], list[PlanIssue], list[PlanIssue]]:
    blockers: list[PlanIssue] = []
    warnings: list[PlanIssue] = []

    def block(code: str, message: str, subject: str) -> None:
        blockers.append(PlanIssue(code=code, message=message, subject=subject))

    def warn(code: str, message: str, subject: str) -> None:
        warnings.append(PlanIssue(code=code, message=message, subject=subject))

    config = None
    if params.column_id:
        config = next((c for c in ctx.column_configs if c.id == params.column_id), None)
        if config is None:
            block(
                "unknown_column_id",
                f"No column called {params.column_id!r}.",
                params.column_id,
            )
    elif params.column_label:
        wanted = _normalize_label(params.column_label).casefold()
        matches = [
            c
            for c in ctx.column_configs
            if c.kind == "custom" and _normalize_label(c.label).casefold() == wanted
        ]
        if not matches:
            block(
                "unknown_column_label",
                f"No custom column is called {params.column_label!r}. "
                "Create it first, or name the column by id.",
                params.column_label,
            )
        elif len(matches) > 1:
            block(
                "column_label_ambiguous",
                f"{len(matches)} columns are called {params.column_label!r} "
                f"({', '.join(c.id for c in matches)}). Name one by id.",
                params.column_label,
            )
        else:
            config = matches[0]
    else:
        block("column_required", "Name the column, by column_id or column_label.", "")

    if not params.values:
        block("values_required", "No values were given.", "")

    if config is None or blockers:
        return config, [], blockers, warnings

    planned: list[_Planned] = []
    missing: list[str] = []
    occupied: list[str] = []

    for raw_id, raw_value in params.values.items():
        source_id = str(raw_id or "").strip()
        # A spreadsheet says 109 where the repository says 000109.
        if source_id.isdigit():
            source_id = f"{int(source_id):06d}"
        row = ctx.row_by_id(source_id)
        if row is None:
            missing.append(str(raw_id))
            continue

        after = str(raw_value or "").strip()
        before = str((row.custom_fields or {}).get(config.id, "") or "")
        if before == after:
            continue
        if before and not params.overwrite:
            occupied.append(source_id)
            continue
        planned.append(_Planned(row, before, after))

    if missing:
        warn(
            "unknown_source_id",
            f"{len(missing)} value(s) name a source that is not in this repository "
            f"and will be skipped: {', '.join(missing[:8])}"
            + ("..." if len(missing) > 8 else ""),
            "",
        )
    if occupied:
        warn(
            "value_already_present",
            f"{len(occupied)} cell(s) already hold a different value and will be left "
            f"alone: {', '.join(occupied[:8])}"
            + ("..." if len(occupied) > 8 else "")
            + ". Re-run with overwrite to replace them.",
            "",
        )

    # A value that is not one of the column's own listed answers reads oddly next
    # to model-written cells; surface it in the dry-run so the caller sees it
    # before --apply (set-cell is written verbatim, unchecked against constraints).
    allowed = list(
        getattr(getattr(config, "output_constraint", None), "allowed_values", None) or []
    )
    if allowed:
        allowed_set = {str(v).strip().casefold() for v in allowed}
        off = [p for p in planned if p.after and p.after.casefold() not in allowed_set]
        if off:
            warn(
                "value_not_in_allowed",
                f"{len(off)} value(s) are not among {config.label!r}'s allowed answers "
                f"({', '.join(repr(v) for v in allowed[:8])}"
                + ("..." if len(allowed) > 8 else "")
                + "): "
                + ", ".join(repr(p.after[:40]) for p in off[:6])
                + ("..." if len(off) > 6 else "")
                + ". Stored as-is.",
                "",
            )

    return config, planned, blockers, warnings


def plan(
    ctx: OperationContext,
    params: SetColumnValuesParams,
) -> tuple[list[PlanChange], list[PlanIssue], list[PlanIssue], str]:
    config, planned, blockers, warnings = _resolve(ctx, params)
    changes: list[PlanChange] = []

    for item in planned[:MAX_DETAILED_CHANGES]:
        changes.append(
            PlanChange(
                kind="row_field",
                subject=f"source:{item.row.id}",
                field=f"custom_fields.{config.id}",
                before=item.before,
                after=item.after,
                detail=f"{config.label} -> {item.after[:80]}",
            )
        )
    if len(planned) > MAX_DETAILED_CHANGES:
        rest = len(planned) - MAX_DETAILED_CHANGES
        changes.append(
            PlanChange(
                kind="row_field",
                subject=f"column:{config.id}",
                field=f"custom_fields.{config.id}",
                after=f"{rest} more",
                detail=f"and {rest} further source(s) not listed here",
            )
        )

    if not planned:
        summary = "Every cell already holds the value given." if config else "Nothing to set."
    else:
        distinct = len({item.after for item in planned})
        summary = (
            f"Will set {config.label!r} on {len(planned)} source(s), "
            f"{distinct} distinct value(s)."
        )
    return changes, blockers, warnings, summary


def apply(ctx: OperationContext, params: SetColumnValuesParams, plan_obj: Any) -> int:
    config, planned, blockers, _ = _resolve(ctx, params)
    if blockers:  # pragma: no cover - the engine re-plans and stops first
        raise RuntimeError("set_column_values was applied with unresolved blockers")
    if not planned:
        return 0

    for item in planned:
        if item.row.custom_fields is None:  # pragma: no cover - defensive
            item.row.custom_fields = {}
        item.row.custom_fields[config.id] = item.after
        ctx.service._write_repository_source_metadata(item.row)

    return len(planned)


DEFINITION = OperationDefinition(
    name="set_column_values",
    title="Import values into a column",
    description=(
        "Write values the user supplied into a column, one per source, without running the "
        "model. Use this for a planning spreadsheet's own columns -- the ones with a heading "
        "and data but no prompt, such as a collection date or the channel a link came from. "
        "Cells that already hold a different value are left alone unless overwrite is set."
    ),
    params_model=SetColumnValuesParams,
    planner=plan,
    applier=apply,
)
