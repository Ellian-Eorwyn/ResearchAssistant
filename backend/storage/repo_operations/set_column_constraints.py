"""Give columns that already exist the allowed answers their prompts list.

`create_columns` skips a label that is already there, which is right — a re-run
is not a mistake — but it means columns created before constraints were derived
keep `allowed_values: []` for good. Those are exactly the columns about to be
run, so they need a way to catch up that does not involve deleting and
recreating them (which would throw away any values already collected).

Re-derived from each column's own stored prompt, so this is idempotent: running
it twice changes nothing the second time.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from backend.models.operations import PlanChange, PlanIssue
from backend.models.repository import RepositoryColumnOutputConstraint

from .base import OperationDefinition
from .context import OperationContext


class SetColumnConstraintsParams(BaseModel):
    # Empty means every custom column with a prompt.
    column_ids: list[str] = Field(default_factory=list)
    # Off by default: a constraint someone set by hand in the app is a decision,
    # not a gap to be filled in.
    overwrite_existing: bool = False


class _Planned:
    def __init__(
        self,
        config: Any,
        constraint: dict[str, Any] | None,
        *,
        add_row_context: bool = False,
    ) -> None:
        self.config = config
        self.constraint = constraint
        self.add_row_context = add_row_context


def _resolve(
    ctx: OperationContext,
    params: SetColumnConstraintsParams,
) -> tuple[list[_Planned], list[PlanIssue], list[PlanIssue]]:
    from backend.workflow.constraints import derive_constraint, needs_row_context

    blockers: list[PlanIssue] = []
    warnings: list[PlanIssue] = []

    by_id = {config.id: config for config in ctx.column_configs}
    for column_id in params.column_ids:
        if column_id not in by_id:
            blockers.append(
                PlanIssue(
                    code="unknown_column_id",
                    message=f"No column called {column_id!r}.",
                    subject=column_id,
                )
            )

    wanted = params.column_ids or [
        config.id
        for config in ctx.column_configs
        if config.kind == "custom" and str(config.instruction_prompt or "").strip()
    ]

    planned: list[_Planned] = []
    for column_id in wanted:
        config = by_id.get(column_id)
        if config is None:
            continue

        prompt = str(config.instruction_prompt or "")
        # A prompt that reads the row's metadata but is not sent any is the more
        # damaging of the two gaps, so it is checked even when the constraint is
        # already set.
        add_row_context = needs_row_context(prompt) and not bool(
            getattr(config, "include_row_context", False)
        )

        existing = getattr(config, "output_constraint", None)
        has_values = bool(getattr(existing, "allowed_values", None))
        if has_values and not params.overwrite_existing:
            if not add_row_context:
                warnings.append(
                    PlanIssue(
                        code="constraint_already_set",
                        message=(
                            f"{config.label!r} already restricts its answers; leaving it alone. "
                            "Use overwrite_existing to replace it."
                        ),
                        subject=column_id,
                    )
                )
                continue
            planned.append(_Planned(config, None, add_row_context=True))
            continue

        constraint = derive_constraint(prompt)
        if not constraint and not add_row_context:
            warnings.append(
                PlanIssue(
                    code="column_without_allowed_values",
                    message=(
                        f"{config.label!r} does not list its answers literally, so nothing "
                        "can be derived. Its output stays unconstrained."
                    ),
                    subject=column_id,
                )
            )
            continue
        planned.append(_Planned(config, constraint, add_row_context=add_row_context))

    return planned, blockers, warnings


def plan(
    ctx: OperationContext,
    params: SetColumnConstraintsParams,
) -> tuple[list[PlanChange], list[PlanIssue], list[PlanIssue], str]:
    planned, blockers, warnings = _resolve(ctx, params)

    changes: list[PlanChange] = []
    for item in planned:
        if item.add_row_context:
            changes.append(
                PlanChange(
                    kind="row_update",
                    subject=f"column:{item.config.id}",
                    field="include_row_context",
                    before="False",
                    after="True",
                    detail=(
                        f"{item.config.label} -> its prompt reads the row's metadata, "
                        "which was not being sent"
                    ),
                )
            )
        if not item.constraint:
            continue
        changes.append(
            PlanChange(
                kind="row_update",
                subject=f"column:{item.config.id}",
                field="output_constraint.allowed_values",
                before=repr(
                    list(
                        getattr(
                            getattr(item.config, "output_constraint", None),
                            "allowed_values",
                            None,
                        )
                        or []
                    )
                ),
                after=repr(item.constraint["allowed_values"]),
                # The values in full: this line is the review, so it has to carry
                # what was read out of the prompt, not just how many.
                detail=(
                    f"{item.config.label} -> "
                    + " | ".join(item.constraint["allowed_values"])
                    + (
                        f"   (anything else becomes {item.constraint['fallback_value']!r})"
                        if item.constraint["fallback_value"]
                        else "   (anything else becomes blank)"
                    )
                ),
            )
        )

    constrained = len([i for i in planned if i.constraint])
    contexted = len([i for i in planned if i.add_row_context])
    parts = []
    if constrained:
        parts.append(f"constrain {constrained} column(s) to the answers their prompts list")
    if contexted:
        parts.append(f"give {contexted} the row metadata their prompts read")
    summary = (
        ("Will " + ", ".join(parts) + ".") if parts else "Every column already matches its prompt."
    )
    return changes, blockers, warnings, summary


def apply(ctx: OperationContext, params: SetColumnConstraintsParams, plan_obj: Any) -> int:
    planned, blockers, _ = _resolve(ctx, params)
    if blockers:  # pragma: no cover - the engine re-plans and stops first
        raise RuntimeError("set_column_constraints was applied with unresolved blockers")

    applied = 0
    for item in planned:
        touched = False
        if item.add_row_context:
            item.config.include_row_context = True
            touched = True
        if item.constraint:
            try:
                item.config.output_constraint = RepositoryColumnOutputConstraint.model_validate(
                    item.constraint
                )
                touched = True
            except Exception:
                pass
        applied += 1 if touched else 0
    return applied


DEFINITION = OperationDefinition(
    name="set_column_constraints",
    title="Make each column's settings match what its prompt asks for",
    description=(
        "Read each column's own instruction prompt and bring the column's settings into line "
        "with it: store the answers it lists literally as the output constraint, so a stray "
        "model answer becomes the prompt's own fallback instead of landing in the data, and "
        "turn on the row metadata for a prompt that tells the model to read it. Columns whose "
        "prompts ask for neither are left alone."
    ),
    params_model=SetColumnConstraintsParams,
    planner=plan,
    applier=apply,
)
