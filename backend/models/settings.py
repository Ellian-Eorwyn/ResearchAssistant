"""Models for application settings and LLM backend configuration."""

from __future__ import annotations

from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class LLMBackendConfig(BaseModel):
    kind: str = "ollama"  # ollama | openai | anthropic
    base_url: str = "http://localhost:11434"
    api_key: str = ""
    model: str = ""
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    # Legacy Ollama-oriented control, kept for backward compatibility. The
    # cross-provider control below (`reasoning_level`) supersedes it; when
    # reasoning_level is "default" the client falls back to this value.
    think_mode: Literal["default", "think", "no_think"] = "default"
    # Unified reasoning control mapped per provider in backend/llm/client.py:
    # Ollama -> think on/off, OpenAI -> reasoning_effort, Anthropic -> effort.
    reasoning_level: Literal["default", "off", "low", "medium", "high"] = "default"
    num_ctx: int = Field(default=8192, ge=2048, le=262144)
    # 0 means "omit / let the provider decide". Anthropic requires max_tokens,
    # so the client substitutes a floor there.
    max_tokens: int = Field(default=0, ge=0, le=200000)
    max_source_chars: int = Field(default=0, ge=0, le=120000)
    llm_timeout: float = Field(default=300.0, ge=30.0, le=1800.0)


# Friendly provider presets surfaced in the UI. Each maps to a concrete backend
# `kind` plus a sensible default base_url. Together.ai / Llama.cpp / Custom all
# ride the OpenAI-compatible path; Claude uses the native `anthropic` path.
PROVIDER_PRESETS: dict[str, dict[str, str]] = {
    "openai": {"kind": "openai", "base_url": "https://api.openai.com/v1"},
    "anthropic": {"kind": "anthropic", "base_url": "https://api.anthropic.com"},
    "together": {"kind": "openai", "base_url": "https://api.together.xyz/v1"},
    "ollama": {"kind": "ollama", "base_url": "http://localhost:11434"},
    "llamacpp": {"kind": "openai", "base_url": "http://localhost:8080/v1"},
    "custom": {"kind": "openai", "base_url": ""},
}

BackendProvider = Literal[
    "openai", "anthropic", "together", "ollama", "llamacpp", "custom"
]


def _provider_for_kind(kind: str) -> BackendProvider:
    """Reverse-map a backend `kind` to a provider preset name.

    Used when synthesizing a profile from a legacy single-backend config, where
    only the `kind` is known. Ollama maps to itself; everything else defaults to
    the generic "custom" OpenAI-compatible provider.
    """
    normalized = (kind or "").strip().lower()
    if normalized == "ollama":
        return "ollama"
    if normalized == "anthropic":
        return "anthropic"
    return "custom"


class BackendProfile(BaseModel):
    """A named, saved LLM backend the user can switch between."""

    id: str = Field(default_factory=lambda: uuid4().hex)
    name: str = ""
    provider: BackendProvider = "custom"
    config: LLMBackendConfig = Field(default_factory=LLMBackendConfig)


class AppSettings(BaseModel):
    """App-level settings persisted in data/settings.json.

    These settings apply across all repositories and include infrastructure
    configuration like the LLM backend and search engine.
    """

    last_repository_path: str = ""
    # Saved library of named backends the user can switch between. `llm_backend`
    # below is the *active* (derived) config -- it always mirrors the profile
    # named by `active_profile_id`, kept in sync by resolve_effective_backend().
    # Every downstream UnifiedLLMClient(effective.llm_backend) call site is
    # therefore untouched by the profiles feature.
    backend_profiles: list[BackendProfile] = Field(default_factory=list)
    active_profile_id: str = ""
    llm_backend: LLMBackendConfig = Field(default_factory=LLMBackendConfig)
    # Optional dedicated backend for image (vision) work. When unset, image
    # classification/description reuse `llm_backend` -- the default backend is
    # already multimodal. Set this only to point image work at a different
    # port/model without disturbing the text path.
    vision_backend: LLMBackendConfig | None = None
    use_llm: bool = False
    searxng_base_url: str = ""
    fetch_delay: float = Field(default=2.0, ge=1.0, le=10.0)
    # Where a browser drops files the user collected by hand. Left blank rather
    # than defaulting to a literal path so the fallback stays in one place and
    # someone who never opens Settings still gets a working watch folder.
    manual_capture_watch_dir: str = ""

    @field_validator("searxng_base_url")
    @classmethod
    def normalize_searxng_url(cls, v: str) -> str:
        v = v.strip().rstrip("/")
        if v.endswith("/search"):
            v = v[: -len("/search")]
        return v


def resolve_effective_backend(app: AppSettings) -> AppSettings:
    """Reconcile the saved-profile library with the active `llm_backend`.

    Mutates and returns `app` so that:
    - a config with no profiles yet gets a "Default" profile synthesized from
      its current `llm_backend` (the migration path for existing single-backend
      users);
    - `active_profile_id` always points at a real profile;
    - `llm_backend` mirrors the active profile's config, so every downstream
      consumer keeps reading the single `llm_backend` field.

    Called from the store loader and the PUT /settings handler. Implemented as a
    plain helper (not a validator) because the PUT path merges via
    model_copy(update=...), which does not re-run validators.
    """
    if not app.backend_profiles:
        provider = _provider_for_kind(app.llm_backend.kind)
        app.backend_profiles = [
            BackendProfile(
                name="Default",
                provider=provider,
                config=app.llm_backend.model_copy(),
            )
        ]
        app.active_profile_id = app.backend_profiles[0].id

    ids = {p.id for p in app.backend_profiles}
    if app.active_profile_id not in ids:
        app.active_profile_id = app.backend_profiles[0].id

    active = next(p for p in app.backend_profiles if p.id == app.active_profile_id)
    app.llm_backend = active.config.model_copy()
    return app


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
    # Effective vision backend: always concrete (falls back to llm_backend).
    vision_backend: LLMBackendConfig = Field(default_factory=LLMBackendConfig)
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
            vision_backend=app.vision_backend or app.llm_backend,
            use_llm=app.use_llm,
            fetch_delay=app.fetch_delay,
            searxng_base_url=app.searxng_base_url,
            research_purpose=repo.research_purpose,
            default_project_profile_name=repo.default_project_profile_name,
        )


class ModelsResponse(BaseModel):
    models: list[str]
    error: str = ""
