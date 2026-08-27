"""Payload models for the workflow layer.

Every response carries a `summary` a model can relay verbatim and a `next` list
of literal commands. Those two fields are what let an agent with no memory of
the previous step still do the right thing.

`next` entries carry their own gate. The skills tell a model to ask the user
before a fetch or a column run, but the skills are prose and `next` is data — a
small model follows the data, so an ungated `next` handed it the exact command
the prose said to stop at. The gate now travels with the command, and
`gate_for` is the single place that decides, so no call site can forget.
"""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# next actions
# ---------------------------------------------------------------------------


class NextAction(BaseModel):
    command: str = ""
    gate: Literal["go", "ask_user"] = "go"
    why: str = ""


# Matched in order, so the more specific reason wins. Anything that spends the
# user's time, spends model calls, or writes to the repository needs their word
# first.
_GATED: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"--confirm-overwrite\b"),
        "This overwrites values that are already in the column.",
    ),
    (
        re.compile(r"\bra run-column\b"),
        "Every source costs one model call. Say how many and wait for an answer.",
    ),
    (re.compile(r"\bra fetch\b"), "This downloads the sources and takes a while."),
    (re.compile(r"\bra retry\b"), "This re-downloads the sources that failed."),
    (re.compile(r"\bra convert\b"), "This rebuilds the stored text for those sources."),
    (
        re.compile(r"\bra images\b"),
        "This classifies and describes page images with the vision model, which spends model calls.",
    ),
    (
        re.compile(r"\bra run-columns\b"),
        "This runs many columns; every source costs one model call per column. Say how many and wait.",
    ),
    (
        re.compile(r"\bra full-run\b"),
        "This runs the whole pipeline (signals, images, every column) over the sources — many model calls.",
    ),
    (
        re.compile(r"\bra set-fetch-status\b"),
        "This changes a source's fetch status in the repository.",
    ),
    (
        re.compile(r"\bra edit-column\b"),
        "This changes a column's prompt or settings, which affects every future run of it.",
    ),
    (
        re.compile(r"\bra config --use-llm\b"),
        "This changes whether the model runs for the whole app.",
    ),
    (
        re.compile(r"\bra set-purpose --file\b"),
        "This rewrites the repository's research prompt.",
    ),
    (
        re.compile(r"\bra profile --(set|upload)\b"),
        "This changes which project profile scores the sources.",
    ),
    (re.compile(r"--apply\b"), "This writes to the repository."),
)


def gate_for(command: str) -> tuple[str, str]:
    """Return `(gate, why)` for a literal command. The single source of truth."""
    for pattern, why in _GATED:
        if pattern.search(command):
            return "ask_user", why
    return "go", ""


def na(command: str, *, why: str = "") -> NextAction:
    """Build a `NextAction`, gating it from the command text."""
    gate, default_why = gate_for(command)
    return NextAction(command=command, gate=gate, why=why or default_why)


def nas(*commands: str) -> list[NextAction]:
    return [na(command) for command in commands]

# ---------------------------------------------------------------------------
# encoding
# ---------------------------------------------------------------------------


class EncodingRepair(BaseModel):
    applied: bool = False
    codec: str = ""
    cells_examined: int = 0
    cells_changed: int = 0
    suspicious_before: int = 0
    suspicious_after: int = 0
    refused_reason: str = ""


# ---------------------------------------------------------------------------
# spreadsheet parsing
# ---------------------------------------------------------------------------


class RowCandidate(BaseModel):
    row: int
    score: float
    reason: str = ""


class RowLayout(BaseModel):
    header_row: int = -1
    prompts_row: int = -1
    first_data_row: int = -1
    id_column: int = -1
    url_column: int = -1
    header_candidates: list[RowCandidate] = Field(default_factory=list)
    detection: dict[str, str] = Field(default_factory=dict)


class SheetSource(BaseModel):
    row: int
    id: str = ""
    url: str = ""
    # Ids of other sheet rows holding the same URL, which this source now stands
    # for. Empty unless the caller asked for duplicates to be merged.
    merged_ids: list[str] = Field(default_factory=list)
    merged_rows: list[int] = Field(default_factory=list)


class SheetDocument(BaseModel):
    """A row that names a document to be supplied by hand rather than a URL.

    Kept apart from `SheetSource` because there is nothing to fetch: the row is
    real and keeps its id, but handing it to `create_sources` could only ever
    produce `url_invalid`.
    """

    row: int
    id: str = ""
    label: str = ""


class SheetProvidedColumn(BaseModel):
    """A column the user filled in themselves, rather than one the model writes.

    It has a heading and data but no prompt, so it is not a column to run --
    it is a column to import, keyed by the source id each value belongs to.
    """

    index: int
    label: str = ""
    values: dict[str, str] = Field(default_factory=dict)


class SheetColumn(BaseModel):
    index: int
    label: str = ""
    prompt: str = ""


class SheetAnomaly(BaseModel):
    code: str
    message: str
    subject: str = ""
    row: int | None = None


class SheetPlan(BaseModel):
    path: str = ""
    source_format: str = ""
    layout: RowLayout = Field(default_factory=RowLayout)
    encoding_repair: EncodingRepair = Field(default_factory=EncodingRepair)
    sources: list[SheetSource] = Field(default_factory=list)
    documents: list[SheetDocument] = Field(default_factory=list)
    columns: list[SheetColumn] = Field(default_factory=list)
    provided_columns: list[SheetProvidedColumn] = Field(default_factory=list)
    skipped_columns: list[SheetColumn] = Field(default_factory=list)
    anomalies: list[SheetAnomaly] = Field(default_factory=list)
    summary: str = ""

    @property
    def blocking_anomalies(self) -> list[SheetAnomaly]:
        return [item for item in self.anomalies if item.code in BLOCKING_SHEET_ANOMALIES]


# Anomalies that mean "do not proceed without a human looking". Everything else
# is reported and the workflow continues.
BLOCKING_SHEET_ANOMALIES = frozenset(
    {
        "no_header_row_found",
        "no_url_column_found",
        "no_sources_found",
        "mojibake_unrepairable",
    }
)


# ---------------------------------------------------------------------------
# preflight
# ---------------------------------------------------------------------------


class Check(BaseModel):
    id: str
    ok: bool = False
    severity: Literal["blocker", "blocker_for_columns", "warning", "info"] = "blocker"
    detail: str = ""
    remedy: str = ""


class Preflight(BaseModel):
    ok: bool = False
    ok_to_fetch: bool = False
    ok_to_run_columns: bool = False
    contract_version: int = 0
    checks: list[Check] = Field(default_factory=list)
    summary: str = ""
    next: list[NextAction] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# orientation
# ---------------------------------------------------------------------------


class ColumnSummary(BaseModel):
    id: str = ""
    label: str = ""
    kind: str = ""
    requires_llm: bool = False
    has_prompt: bool = False
    filled_rows: int = 0
    empty_rows: int = 0
    last_run_status: str = ""
    # A column whose prompt lists exact answers is the one worth running first:
    # a misread prompt shows up in one column instead of all of them.
    allowed_values: list[str] = Field(default_factory=list)
    # Sources whose stored value here was computed from text that has since been
    # rebuilt. The cell is filled, so nothing else would flag it.
    stale_source_ids: list[str] = Field(default_factory=list)


class ActiveJob(BaseModel):
    running: bool = False
    kind: str = ""
    job_id: str = ""
    state: str = ""
    detail: str = ""


class Orientation(BaseModel):
    repository_path: str = ""
    attached: bool = False
    total_sources: int = 0
    sources_by_fetch_status: dict[str, int] = Field(default_factory=dict)
    sources_with_markdown: int = 0
    failures_by_code: dict[str, int] = Field(default_factory=dict)
    failure_examples: dict[str, list[str]] = Field(default_factory=dict)
    columns: list[ColumnSummary] = Field(default_factory=list)
    active_job: ActiveJob = Field(default_factory=ActiveJob)
    health: dict[str, int] = Field(default_factory=dict)
    summary: str = ""
    next_actions: list[NextAction] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# triage
# ---------------------------------------------------------------------------

Classification = Literal[
    "retryable",
    "retryable_convert",
    "needs_manual_document",
    "broken_url",
    "environment",
    "ignore",
    "unknown",
]


class FailureExample(BaseModel):
    id: str = ""
    url: str = ""
    error: str = ""


class FailureGroup(BaseModel):
    error_code: str = ""
    detail_pattern: str = ""
    classification: Classification = "unknown"
    count: int = 0
    source_ids: list[str] = Field(default_factory=list)
    examples: list[FailureExample] = Field(default_factory=list)
    explanation: str = ""
    remedy_command: str = ""


class TriageReport(BaseModel):
    phase: str = "fetch"
    total_failed: int = 0
    groups: list[FailureGroup] = Field(default_factory=list)
    summary: str = ""
    next: list[NextAction] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# runs
# ---------------------------------------------------------------------------


class RunOutcome(BaseModel):
    kind: Literal["source_phases", "column"] = "source_phases"
    run_id: str = ""
    state: str = ""
    terminal: bool = False
    terminal_reason: str = ""
    # source-phase runs
    counts: dict[str, int] = Field(default_factory=dict)
    phase_states: dict[str, str] = Field(default_factory=dict)
    # column runs
    column_id: str = ""
    column_label: str = ""
    outcome: str = ""
    total_rows: int = 0
    processed_rows: int = 0
    succeeded_rows: int = 0
    failed_rows: int = 0
    row_errors: list[dict[str, str]] = Field(default_factory=list)
    row_errors_truncated: bool = False
    # Rows whose answer the column's allowed values rejected, so the stored
    # value is the fallback rather than anything the model chose.
    coerced_rows: int = 0
    coercions: list[dict[str, str]] = Field(default_factory=list)
    confirmation_required: bool = False
    summary: str = ""
    next: list[NextAction] = Field(default_factory=list)


class AttachOutcome(BaseModel):
    applied: bool = False
    status: str = ""
    attached_source_ids: list[str] = Field(default_factory=list)
    reconvert: RunOutcome | None = None
    plan: dict[str, Any] = Field(default_factory=dict)
    summary: str = ""
    next: list[NextAction] = Field(default_factory=list)
