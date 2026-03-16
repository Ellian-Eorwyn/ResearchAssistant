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


class AppSettings(BaseModel):
    llm_backend: LLMBackendConfig = Field(default_factory=LLMBackendConfig)
    use_llm: bool = False
    research_purpose: str = ""
    repository_path: str = ""
    fetch_delay: float = Field(default=2.0, ge=1.0, le=10.0)


class ModelsResponse(BaseModel):
    models: list[str]
    error: str = ""
