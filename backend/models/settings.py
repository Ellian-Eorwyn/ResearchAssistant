"""Models for application settings and LLM backend configuration."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


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


class AppSettings(BaseModel):
    """App-level settings persisted in data/settings.json.

    These settings apply across all repositories and include infrastructure
    configuration like the LLM backend and search engine.
    """

    last_repository_path: str = ""
    llm_backend: LLMBackendConfig = Field(default_factory=LLMBackendConfig)
    use_llm: bool = False
    searxng_base_url: str = ""
    fetch_delay: float = Field(default=2.0, ge=1.0, le=10.0)


class RepoSettings(BaseModel):
    """Per-repository settings stored in {repo}/.ra_repo/settings.json.

    Only contains settings that are specific to a single repository.
    Extra fields from older config files are silently ignored for
    backward compatibility.
    """

    model_config = ConfigDict(extra="ignore")

    research_purpose: str = ""
    default_project_profile_name: str = ""


class EffectiveSettings(BaseModel):
    """Merged view of app-level and repo-level settings.

    Backend consumers use this to get the full configuration picture.
    Field names match the old RepoSettings layout so existing callers
    can switch with minimal changes.
    """

    llm_backend: LLMBackendConfig = Field(default_factory=LLMBackendConfig)
    use_llm: bool = False
    research_purpose: str = ""
    default_project_profile_name: str = ""
    fetch_delay: float = Field(default=2.0, ge=1.0, le=10.0)
    searxng_base_url: str = ""

    @classmethod
    def from_app_and_repo(
        cls,
        app: AppSettings,
        repo: RepoSettings,
    ) -> EffectiveSettings:
        return cls(
            llm_backend=app.llm_backend,
            use_llm=app.use_llm,
            fetch_delay=app.fetch_delay,
            searxng_base_url=app.searxng_base_url,
            research_purpose=repo.research_purpose,
            default_project_profile_name=repo.default_project_profile_name,
        )


class ModelsResponse(BaseModel):
    models: list[str]
    error: str = ""
