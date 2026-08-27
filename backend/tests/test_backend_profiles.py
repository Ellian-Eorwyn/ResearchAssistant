"""Tests for saved backend profiles, cross-provider reasoning, and the native
Anthropic client path."""

from __future__ import annotations

import tempfile
import threading
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.llm.client import (
    UnifiedLLMClient,
    _anthropic_effort,
    _anthropic_text,
    _anthropic_usage,
    _effective_reasoning_level,
    _ollama_think_from_level,
    _openai_reasoning_effort,
)
from backend.models.settings import (
    PROVIDER_PRESETS,
    AppSettings,
    BackendProfile,
    LLMBackendConfig,
    resolve_effective_backend,
)
from backend.routers import settings as settings_router
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


# ---- resolve_effective_backend ----


def test_resolve_synthesizes_default_profile_from_llm_backend() -> None:
    app = AppSettings(
        llm_backend=LLMBackendConfig(kind="openai", base_url="http://llms:8004", model="chat")
    )
    resolve_effective_backend(app)

    assert len(app.backend_profiles) == 1
    profile = app.backend_profiles[0]
    assert profile.name == "Default"
    assert profile.provider == "custom"
    assert profile.config.model == "chat"
    assert app.active_profile_id == profile.id
    # llm_backend still mirrors the (only) profile.
    assert app.llm_backend.model == "chat"


def test_resolve_infers_provider_for_ollama_and_anthropic_kinds() -> None:
    ollama = resolve_effective_backend(
        AppSettings(llm_backend=LLMBackendConfig(kind="ollama"))
    )
    assert ollama.backend_profiles[0].provider == "ollama"

    anthropic = resolve_effective_backend(
        AppSettings(llm_backend=LLMBackendConfig(kind="anthropic"))
    )
    assert anthropic.backend_profiles[0].provider == "anthropic"


def test_resolve_projects_active_profile_into_llm_backend() -> None:
    a = BackendProfile(name="A", provider="ollama", config=LLMBackendConfig(kind="ollama", model="a"))
    b = BackendProfile(
        name="B", provider="anthropic", config=LLMBackendConfig(kind="anthropic", model="claude-opus-5")
    )
    app = AppSettings(backend_profiles=[a, b], active_profile_id=b.id)
    resolve_effective_backend(app)

    assert app.llm_backend.kind == "anthropic"
    assert app.llm_backend.model == "claude-opus-5"


def test_resolve_falls_back_when_active_id_is_dangling() -> None:
    a = BackendProfile(name="A", config=LLMBackendConfig(model="a"))
    b = BackendProfile(name="B", config=LLMBackendConfig(model="b"))
    app = AppSettings(backend_profiles=[a, b], active_profile_id="nope")
    resolve_effective_backend(app)

    assert app.active_profile_id == a.id
    assert app.llm_backend.model == "a"


# ---- cross-provider reasoning maps ----


def test_effective_reasoning_prefers_level_then_falls_back_to_think_mode() -> None:
    assert _effective_reasoning_level(LLMBackendConfig(reasoning_level="high")) == "high"
    assert _effective_reasoning_level(LLMBackendConfig(think_mode="think")) == "high"
    assert _effective_reasoning_level(LLMBackendConfig(think_mode="no_think")) == "off"
    assert _effective_reasoning_level(LLMBackendConfig()) == "default"


def test_provider_specific_reasoning_maps() -> None:
    assert _ollama_think_from_level("low") is True
    assert _ollama_think_from_level("off") is False
    assert _ollama_think_from_level("default") is None

    assert _openai_reasoning_effort("high") == "high"
    assert _openai_reasoning_effort("off") == "minimal"
    assert _openai_reasoning_effort("default") is None

    assert _anthropic_effort("medium") == "medium"
    assert _anthropic_effort("off") is None
    assert _anthropic_effort("default") is None


# ---- OpenAI body honors configured params ----


def test_openai_body_uses_temperature_max_tokens_and_reasoning() -> None:
    client = UnifiedLLMClient(
        LLMBackendConfig(kind="openai", model="gpt", temperature=0.7, max_tokens=1000, reasoning_level="low")
    )
    body = client._build_openai_body("S", "U", "json")

    assert body["temperature"] == 0.7
    assert body["max_tokens"] == 1000
    assert body["reasoning_effort"] == "low"
    assert body["response_format"] == {"type": "json_object"}


def test_openai_body_omits_max_tokens_and_effort_by_default() -> None:
    client = UnifiedLLMClient(LLMBackendConfig(kind="openai", model="gpt"))
    body = client._build_openai_body("S", "U", None)

    assert "max_tokens" not in body
    assert "reasoning_effort" not in body


# ---- native Anthropic body / parsing ----


def test_anthropic_body_shape_with_effort() -> None:
    client = UnifiedLLMClient(
        LLMBackendConfig(kind="anthropic", model="claude-opus-5", reasoning_level="high", max_tokens=0)
    )
    body = client._build_anthropic_body("SYS", "USER", "json")

    assert body["model"] == "claude-opus-5"
    assert body["max_tokens"] == 4096  # floor substituted for 0
    assert body["system"].startswith("SYS")
    assert "json" in body["system"].lower()
    assert body["messages"] == [{"role": "user", "content": "USER"}]
    assert body["output_config"] == {"effort": "high"}
    # Sampling params are never sent to Anthropic (current models 400 on them).
    assert "temperature" not in body


def test_anthropic_body_disables_thinking_when_reasoning_off() -> None:
    client = UnifiedLLMClient(LLMBackendConfig(kind="anthropic", model="m", reasoning_level="off"))
    body = client._build_anthropic_body("S", "U", None)

    assert body["thinking"] == {"type": "disabled"}
    assert "output_config" not in body


def test_anthropic_usage_and_text_parsing() -> None:
    usage = _anthropic_usage({"usage": {"input_tokens": 10, "output_tokens": 5}})
    assert usage == {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}

    text = _anthropic_text(
        {
            "content": [
                {"type": "thinking", "text": "ignored"},
                {"type": "text", "text": "hello "},
                {"type": "text", "text": "world"},
            ]
        }
    )
    assert text == "hello world"


def test_provider_presets_cover_all_targets() -> None:
    assert PROVIDER_PRESETS["anthropic"]["kind"] == "anthropic"
    assert PROVIDER_PRESETS["openai"]["kind"] == "openai"
    assert PROVIDER_PRESETS["together"]["kind"] == "openai"
    assert PROVIDER_PRESETS["llamacpp"]["kind"] == "openai"
    assert PROVIDER_PRESETS["ollama"]["kind"] == "ollama"


# ---- settings router round-trip ----


def _make_settings_client() -> TestClient:
    tmp = tempfile.TemporaryDirectory(prefix="backend-profiles-tests-")
    service = AttachedRepositoryService(store=FileStore(base_dir=Path(tmp.name) / "app"))
    app = FastAPI()
    app.state.file_store = service.store
    app.state.repository_service = service
    app.state.source_download_jobs = {}
    app.state.source_download_lock = threading.Lock()
    app.include_router(settings_router.router, prefix="/api")
    client = TestClient(app)
    client._tmp = tmp  # keep the tempdir alive for the client's lifetime
    return client


def test_get_settings_synthesizes_default_profile() -> None:
    client = _make_settings_client()
    data = client.get("/api/settings").json()

    assert len(data["backend_profiles"]) == 1
    assert data["active_profile_id"] == data["backend_profiles"][0]["id"]


def test_put_settings_persists_profiles_and_switches_active() -> None:
    client = _make_settings_client()

    ollama = {
        "id": "prof-ollama",
        "name": "Local",
        "provider": "ollama",
        "config": {"kind": "ollama", "base_url": "http://localhost:11434", "model": "llama"},
    }
    claude = {
        "id": "prof-claude",
        "name": "Claude",
        "provider": "anthropic",
        "config": {
            "kind": "anthropic",
            "base_url": "https://api.anthropic.com",
            "api_key": "sk-secret",
            "model": "claude-opus-5",
            "reasoning_level": "high",
        },
    }
    saved = client.put(
        "/api/settings",
        json={"backend_profiles": [ollama, claude], "active_profile_id": "prof-claude"},
    ).json()

    # Active profile is projected into llm_backend for every downstream consumer.
    assert saved["active_profile_id"] == "prof-claude"
    assert saved["llm_backend"]["kind"] == "anthropic"
    assert saved["llm_backend"]["model"] == "claude-opus-5"
    assert saved["llm_backend"]["reasoning_level"] == "high"

    # Round-trips through disk.
    again = client.get("/api/settings").json()
    assert len(again["backend_profiles"]) == 2
    assert again["llm_backend"]["model"] == "claude-opus-5"

    # Switch active -> llm_backend follows.
    switched = client.put("/api/settings", json={"active_profile_id": "prof-ollama"}).json()
    assert switched["llm_backend"]["kind"] == "ollama"
    assert switched["llm_backend"]["model"] == "llama"
