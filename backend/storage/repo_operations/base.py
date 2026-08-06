"""The shape of an operation.

Kept separate from `__init__` so operation modules and the registry can both
import it without a circular import.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel

from backend.models.operations import OperationPlan, PlanChange, PlanIssue

from .context import OperationContext

# changes, blockers, warnings, summary
PlanResult = tuple[list[PlanChange], list[PlanIssue], list[PlanIssue], str]


@dataclass(frozen=True)
class OperationDefinition:
    name: str
    title: str
    description: str
    params_model: type[BaseModel]
    # Read-only. Must not touch the filesystem or the state.
    planner: Callable[[OperationContext, Any], PlanResult]
    # Mutates `ctx` and moves files through `ctx.journal`. Returns a change count.
    applier: Callable[[OperationContext, Any, OperationPlan], int]
    # Optional. Returns the old-id -> new-id mapping the operation will perform.
    #
    # Verification issues are baselined before the operation so pre-existing
    # damage does not make every future operation impossible. That baseline is
    # keyed by subject, and subjects embed source ids -- so an operation that
    # renames ids must declare the mapping, or a pre-existing issue reappears
    # under its new id and is misread as a regression.
    identity_remap: Callable[[OperationContext, Any], dict[str, str]] | None = None
