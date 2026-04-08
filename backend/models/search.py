"""Models for AI-powered web search via SearXNG."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    prompt: str
    target_count: int = Field(default=200, ge=50, le=500)


class SearchResultItem(BaseModel):
    url: str = ""
    title: str = ""
    snippet: str = ""
    engine: str = ""
    engines: list[str] = Field(default_factory=list)
    searxng_score: float = 0.0
    category: str = ""
    published_date: str = ""
    relevance_score: float = 0.0
    relevance_scored: bool = False


class SearchJobStatus(BaseModel):
    job_id: str
    state: Literal[
        "pending",
        "generating_queries",
        "searching",
        "scoring",
        "completed",
        "failed",
    ] = "pending"
    prompt: str = ""
    generated_queries: list[str] = Field(default_factory=list)
    queries_completed: int = 0
    total_queries: int = 0
    results_found: int = 0
    results_scored: int = 0
    results_total: int = 0
    results: list[SearchResultItem] = Field(default_factory=list)
    error_message: str = ""


class SearchImportRequest(BaseModel):
    min_relevance: float = Field(default=0.5, ge=0.0, le=1.0)


class SearchImportResponse(BaseModel):
    imported_count: int = 0
    duplicates_skipped: int = 0
    total_sources: int = 0
    message: str = ""
