"""Models for AI-powered web search via SearXNG."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    prompt: str
    target_count: int = Field(default=200, ge=50, le=500)
    categories: list[str] = Field(default_factory=list)
    language: str = ""
    time_range: Literal["", "day", "month", "year"] = ""


class SearchLanguageOption(BaseModel):
    value: str = ""
    label: str = ""


class SearchOptionsDefaults(BaseModel):
    categories: list[str] = Field(default_factory=lambda: ["general"])
    language: str = "auto"
    time_range: Literal["", "day", "month", "year"] = ""


class SearchOptionsResponse(BaseModel):
    categories: list[str] = Field(default_factory=list)
    languages: list[SearchLanguageOption] = Field(default_factory=list)
    time_ranges: list[Literal["day", "month", "year"]] = Field(default_factory=list)
    supports_oa_doi_helper: bool = False
    defaults: SearchOptionsDefaults = Field(default_factory=SearchOptionsDefaults)


class SearchResultItem(BaseModel):
    url: str = ""
    title: str = ""
    snippet: str = ""
    engine: str = ""
    engines: list[str] = Field(default_factory=list)
    authors: list[str] = Field(default_factory=list)
    doi: str = ""
    html_url: str = ""
    pdf_url: str = ""
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
    categories: list[str] = Field(default_factory=list)
    language: str = ""
    time_range: Literal["", "day", "month", "year"] = ""
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
