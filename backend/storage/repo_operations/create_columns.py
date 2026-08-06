"""Create several custom columns in one transaction.

Creating them one at a time works, but a planning spreadsheet brings fourteen at
once and `create_column` takes the writer lock and regenerates `manifest.csv`
and `manifest.xlsx` on every call — so fourteen columns over several hundred
sources means fourteen full manifest rebuilds. Doing it as one operation also
means the user reviews the whole coding scheme before any of it appears.

The fit with the engine is unusually clean: `OperationContext` already carries
`column_configs` and `save_context_locked` already persists them, so there is no
context change, and with no file moves the journal is unused.
"""

from __future__ import annotations

import uuid
from typing import Any

from pydantic import BaseModel, Field

from backend.models.operations import PlanChange, PlanIssue
from backend.models.repository import RepositoryColumnConfig, RepositoryColumnOutputConstraint

from .base import OperationDefinition
from .context import OperationContext


class ColumnSpec(BaseModel):
    label: str = ""
    instruction_prompt: str = ""
    output_constraint: dict[str, Any] | None = None
    include_row_context: bool = False
    include_source_text: bool = True


class CreateColumnsParams(BaseModel):
    columns: list[ColumnSpec] = Field(default_factory=list, min_length=1)
    # A column that already exists is a re-run, not a mistake.
    skip_existing: bool = True


class _Planned:
    def __init__(self, spec: ColumnSpec, label: str, column_id: str) -> None:
        self.spec = spec
        self.label = label
        self.column_id = column_id
        self.constraint = resolved_constraint(spec)


def resolved_constraint(spec: ColumnSpec) -> dict[str, Any] | None:
    """An explicit constraint wins; otherwise read one out of the prompt.

    Without this every column is created with `allowed_values: []` and an empty
    fallback, so a model's stray answer lands in the data verbatim. Extraction
    only fires on a prompt that literally lists its answers, and the planner
    prints what it found so the user reviews it before `--apply`.
    """
    if spec.output_constraint:
        return dict(spec.output_constraint)
    from backend.workflow.constraints import derive_constraint

    return derive_constraint(spec.instruction_prompt or "")


def _normalize_label(value: str) -> str:
    return " ".join(str(value or "").split()).strip()


def _resolve(
    ctx: OperationContext,
    params: CreateColumnsParams,
) -> tuple[list[_Planned], list[PlanIssue], list[PlanIssue]]:
    blockers: list[PlanIssue] = []
    warnings: list[PlanIssue] = []

    existing = {
        _normalize_label(config.label).casefold(): config
        for config in ctx.column_configs
        if config.kind == "custom"
    }
    taken_ids = {config.id for config in ctx.column_configs}

    planned: list[_Planned] = []
    seen: dict[str, int] = {}

    for index, spec in enumerate(params.columns):
        label = _normalize_label(spec.label)
        subject = label or f"columns[{index}]"

        if not label:
            blockers.append(
                PlanIssue(code="label_required", message="Every column needs a label.", subject=subject)
            )
            continue
        if not str(spec.instruction_prompt or "").strip():
            blockers.append(
                PlanIssue(
                    code="prompt_required",
                    message=f"Column {label!r} has no instruction prompt.",
                    subject=subject,
                )
            )
            continue

        key = label.casefold()
        if key in seen:
            blockers.append(
                PlanIssue(
                    code="label_duplicate_in_request",
                    message=f"Column {label!r} appears twice in this request.",
                    subject=subject,
                )
            )
            continue
        seen[key] = index

        if key in existing:
            if params.skip_existing:
                warnings.append(
                    PlanIssue(
                        code="column_label_exists",
                        message=(
                            f"A custom column called {label!r} already exists "
                            f"({existing[key].id}); skipping. Use update_column_prompt to "
                            "change its prompt."
                        ),
                        subject=subject,
                    )
                )
                continue
            blockers.append(
                PlanIssue(
                    code="column_label_exists",
                    message=f"A custom column called {label!r} already exists.",
                    subject=subject,
                )
            )
            continue

        column_id = ""
        while not column_id or column_id in taken_ids:
            column_id = f"custom_{uuid.uuid4().hex[:8]}"
        taken_ids.add(column_id)
        planned.append(_Planned(spec, label, column_id))

    return planned, blockers, warnings


def plan(
    ctx: OperationContext,
    params: CreateColumnsParams,
) -> tuple[list[PlanChange], list[PlanIssue], list[PlanIssue], str]:
    planned, blockers, warnings = _resolve(ctx, params)
    changes: list[PlanChange] = []

    unconstrained: list[str] = []
    for item in planned:
        subject = f"column:{item.column_id}"
        prompt = item.spec.instruction_prompt.strip()
        values = list((item.constraint or {}).get("allowed_values") or [])
        detail = f"{len(prompt)} character prompt"
        if values:
            # Shown in full: this is the review that makes reading values out of
            # a prompt safe, so the user has to be able to see what was read.
            detail += f"; answers restricted to {values!r}"
            fallback = (item.constraint or {}).get("fallback_value") or ""
            detail += f", falling back to {fallback!r}" if fallback else ", no fallback"
        else:
            unconstrained.append(item.label)
        changes.append(
            PlanChange(
                kind="row_create",
                subject=subject,
                field="label",
                after=item.label,
                detail=detail,
            )
        )

    if unconstrained:
        warnings.append(
            PlanIssue(
                code="column_without_allowed_values",
                message=(
                    f"{len(unconstrained)} column(s) do not list their answers literally, so "
                    "anything the model returns is stored as-is: "
                    + ", ".join(repr(label) for label in unconstrained[:8])
                    + ("..." if len(unconstrained) > 8 else "")
                ),
                subject="",
            )
        )

    skipped = len([w for w in warnings if w.code == "column_label_exists"])
    constrained = len(planned) - len(unconstrained)
    parts = []
    if planned:
        parts.append(f"create {len(planned)} column(s)")
    if constrained:
        parts.append(f"constrain {constrained} to their listed answers")
    if skipped:
        parts.append(f"skip {skipped} that already exist")
    summary = ("Will " + ", ".join(parts) + ".") if parts else "Nothing to create."
    return changes, blockers, warnings, summary


def apply(ctx: OperationContext, params: CreateColumnsParams, plan_obj: Any) -> int:
    planned, blockers, _ = _resolve(ctx, params)
    if blockers:  # pragma: no cover - the engine re-plans and stops first
        raise RuntimeError("create_columns was applied with unresolved blockers")
    if not planned:
        return 0

    for item in planned:
        constraint = None
        if item.constraint:
            try:
                constraint = RepositoryColumnOutputConstraint.model_validate(item.constraint)
            except Exception:
                constraint = None
        ctx.column_configs.append(
            RepositoryColumnConfig(
                id=item.column_id,
                label=item.label,
                kind="custom",
                instruction_prompt=item.spec.instruction_prompt.strip(),
                output_constraint=constraint,
                include_row_context=bool(item.spec.include_row_context),
                include_source_text=bool(item.spec.include_source_text),
            )
        )

    return len(planned)


DEFINITION = OperationDefinition(
    name="create_columns",
    title="Create columns from prompts",
    description=(
        "Add several custom columns at once, each with the instruction prompt that will be "
        "run against every source. Use this to turn a planning spreadsheet's coding scheme "
        "into repository columns in one reviewable step. A column whose label already "
        "exists is skipped."
    ),
    params_model=CreateColumnsParams,
    planner=plan,
    applier=apply,
)
