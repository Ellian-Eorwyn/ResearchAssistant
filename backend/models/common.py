"""Common models shared across the pipeline."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


class PipelineStage(str, Enum):
    PENDING = "pending"
    INGESTING = "ingesting"
    DETECTING_REFERENCES = "detecting_references"
    PARSING_BIBLIOGRAPHY = "parsing_bibliography"
    DETECTING_CITATIONS = "detecting_citations"
    EXTRACTING_SENTENCES = "extracting_sentences"
    MATCHING_CITATIONS = "matching_citations"
    EXPORTING = "exporting"
    COMPLETED = "completed"
    FAILED = "failed"


class StageStatus(BaseModel):
    stage: PipelineStage
    status: str = "pending"  # pending | running | completed | failed
    started_at: datetime | None = None
    completed_at: datetime | None = None
    item_count: int = 0
    warnings: list[str] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


class JobStatusResponse(BaseModel):
    job_id: str
    current_stage: PipelineStage
    stages: list[StageStatus]
    progress_pct: float = 0.0
    created_at: datetime
    completed_at: datetime | None = None


class FileInfo(BaseModel):
    filename: str
    file_type: str  # pdf, docx, md
    size_bytes: int


class UploadResponse(BaseModel):
    job_id: str
    files: list[FileInfo]


class ProcessingConfig(BaseModel):
    use_llm: bool = False
    research_purpose: str = ""


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
