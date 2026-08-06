"""Models for the repository operations engine.

An operation is a named, reviewable mutation of the attached repository. Every
operation is split into a read-only `plan` phase and a transactional `apply`
phase so an agent (or a human) can inspect the exact change set, and its
consequences, before anything is written.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PlanChange(BaseModel):
    """One concrete edit an operation intends to make."""

    kind: str = ""  # state_field | row_create | row_field | file_move | file_write | meta_field
    subject: str = ""  # "source:000003" | "meta.next_source_id" | a repo-relative path
    field: str = ""
    before: str = ""
    after: str = ""
    detail: str = ""


class PlanIssue(BaseModel):
    """A blocker or warning raised while planning."""

    code: str = ""  # stable snake_case identifier, safe to branch on
    message: str = ""
    subject: str = ""


class VerifyIssue(PlanIssue):
    """A post-condition violation found after applying an operation."""


class OperationPlan(BaseModel):
    operation: str = ""
    plan_id: str = ""
    created_at: str = ""
    state_fingerprint: str = ""
    params: dict[str, Any] = Field(default_factory=dict)
    changes: list[PlanChange] = Field(default_factory=list)
    blockers: list[PlanIssue] = Field(default_factory=list)
    warnings: list[PlanIssue] = Field(default_factory=list)
    summary: str = ""

    @property
    def applicable(self) -> bool:
        return not self.blockers and bool(self.changes)


class OperationResult(BaseModel):
    operation: str = ""
    run_id: str = ""
    # applied  -> the change set is on disk and verified
    # noop     -> nothing to do
    # blocked  -> refused before touching anything
    # rolled_back -> attempted, failed verification or raised, and was undone
    status: str = "blocked"
    plan: OperationPlan = Field(default_factory=OperationPlan)
    applied_changes: int = 0
    verify_passed: bool = False
    verify_issues: list[VerifyIssue] = Field(default_factory=list)
    rollback_performed: bool = False
    rollback_ok: bool = True
    backup_dir: str = ""
    message: str = ""


class OperationDescriptor(BaseModel):
    name: str = ""
    title: str = ""
    description: str = ""
    input_schema: dict[str, Any] = Field(default_factory=dict)
    mutating: bool = True
