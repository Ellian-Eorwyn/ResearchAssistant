"""Models for application settings and LLM backend configuration."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class LLMBackendConfig(BaseModel):
    kind: str = "ollama"  # ollama | openai
    base_url: str = "http://localhost:11434"
    api_key: str = ""
    model: str = ""
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    think_mode: Literal["default", "think", "no_think"] = "default"
    num_ctx: int = Field(default=8192, ge=2048, le=131072)
    max_source_chars: int = Field(default=0, ge=0, le=120000)
    llm_timeout: float = Field(default=300.0, ge=30.0, le=1800.0)


class RepoSettings(BaseModel):
    """Per-repository settings stored in {repo}/.ra_repo/settings.json."""

    llm_backend: LLMBackendConfig = Field(default_factory=LLMBackendConfig)
    use_llm: bool = False
    research_purpose: str = ""
    fetch_delay: float = Field(default=2.0, ge=1.0, le=10.0)


class AppSettings(BaseModel):
    """Runtime-only application settings.

    Repository-specific settings live in the repository itself.
    `last_repository_path` is surfaced for UI convenience only and is not
    intended for persistent storage.
    """

    last_repository_path: str = ""


class ModelsResponse(BaseModel):
    models: list[str]
    error: str = ""
