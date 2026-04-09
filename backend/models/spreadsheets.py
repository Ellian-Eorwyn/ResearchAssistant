"""Models for spreadsheet workspace sessions and tabular data editing."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

from backend.models.repository import RepositoryColumnOutputConstraint


SpreadsheetDataType = Literal[
    "string",
    "integer",
    "number",
    "boolean",
    "null",
    "mixed",
]

SpreadsheetSourceFormat = Literal[
    "csv",
    "xlsx",
    "json",
    "jsonl",
    "ndjson",
    "parquet",
    "sqlite",
]


class SpreadsheetFilterRequest(BaseModel):
    q: str = ""


class SpreadsheetTargetDescriptor(BaseModel):
    id: str
    label: str
    kind: Literal["sheet", "table", "json_path", "file"] = "file"
    selector: dict[str, Any] = Field(default_factory=dict)
    row_count: int = 0
    column_count: int = 0


class SpreadsheetColumnConfig(BaseModel):
    id: str
    source_key: str
    label: str
    kind: Literal["source", "custom"] = "source"
    data_type: SpreadsheetDataType = "string"
    ordinal: int = 0
    instruction_prompt: str = ""
    output_constraint: RepositoryColumnOutputConstraint | None = None
    input_column_ids: list[str] = Field(default_factory=list)
    last_run_at: str = ""
    last_run_status: str = ""


class SpreadsheetSessionSummary(BaseModel):
    session_id: str
    filename: str
    original_filename: str = ""
    source_format: SpreadsheetSourceFormat
    created_at: str = ""
    updated_at: str = ""
    active_target_id: str = ""
    target_count: int = 0


class SpreadsheetSessionResponse(BaseModel):
    session: SpreadsheetSessionSummary
    targets: list[SpreadsheetTargetDescriptor] = Field(default_factory=list)
    active_target: SpreadsheetTargetDescriptor | None = None
    columns: list[SpreadsheetColumnConfig] = Field(default_factory=list)


class SpreadsheetWorkspaceStatusResponse(BaseModel):
    available: bool = False
    current_session_id: str = ""
    sessions: list[SpreadsheetSessionSummary] = Field(default_factory=list)
    current_session: SpreadsheetSessionResponse | None = None


class SpreadsheetSessionTargetSelectRequest(BaseModel):
    target_id: str


class SpreadsheetSessionUploadResponse(BaseModel):
    session: SpreadsheetSessionResponse
    message: str = ""


class SpreadsheetManifestResponse(BaseModel):
    rows: list[dict[str, Any]] = Field(default_factory=list)
    total: int = 0
    limit: int = 50
    offset: int = 0
    sort_by: str = ""
    sort_dir: Literal["asc", "desc", ""] = ""
    columns: list[SpreadsheetColumnConfig] = Field(default_factory=list)
    filters: SpreadsheetFilterRequest = Field(default_factory=SpreadsheetFilterRequest)


class SpreadsheetRowPatchRequest(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


class SpreadsheetColumnCreateRequest(BaseModel):
    label: str = ""


class SpreadsheetColumnUpdateRequest(BaseModel):
    label: str | None = None
    instruction_prompt: str | None = None
    output_constraint: RepositoryColumnOutputConstraint | None = None
    input_column_ids: list[str] | None = None


class SpreadsheetColumnPromptFixRequest(BaseModel):
    draft_prompt: str = ""


class SpreadsheetColumnPromptFixResponse(BaseModel):
    status: str = "completed"
    column_id: str
    prompt: str = ""
    output_constraint: RepositoryColumnOutputConstraint | None = None
    notes: list[str] = Field(default_factory=list)


class SpreadsheetColumnRunRowError(BaseModel):
    row_id: str
    message: str = ""


class SpreadsheetColumnRunRequest(BaseModel):
    filters: SpreadsheetFilterRequest = Field(default_factory=SpreadsheetFilterRequest)
    scope: Literal["filtered", "all", "empty_only", "selected"] = "filtered"
    row_ids: list[str] = Field(default_factory=list)
    confirm_overwrite: bool = False


class SpreadsheetColumnRunStartResponse(BaseModel):
    job_id: str = ""
    status: Literal["started", "confirmation_required"] = "started"
    column_id: str
    total_rows: int = 0
    populated_rows: int = 0
    message: str = ""


class SpreadsheetColumnRunStatus(BaseModel):
    job_id: str
    session_id: str
    target_id: str
    column_id: str
    column_label: str = ""
    state: Literal["pending", "running", "completed", "failed", "cancelled"] = "pending"
    total_rows: int = 0
    processed_rows: int = 0
    succeeded_rows: int = 0
    failed_rows: int = 0
    current_row_id: str = ""
    message: str = ""
    started_at: str = ""
    completed_at: str = ""
    row_errors: list[SpreadsheetColumnRunRowError] = Field(default_factory=list)


class SpreadsheetExportRequest(BaseModel):
    session_id: str = ""

