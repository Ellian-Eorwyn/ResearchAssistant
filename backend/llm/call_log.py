"""Session and file-backed logging for LLM calls."""

from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from backend.models.settings import LLMBackendConfig


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def estimate_context_tokens(*parts: object) -> int:
    """Cheap cross-provider estimate for prompt/context tokens.

    Provider usage metadata is preferred when available. This fallback keeps
    local backends and failed calls visible without bringing in tokenizer deps.
    """
    text = "\n".join(str(part or "") for part in parts)
    if not text:
        return 0
    return max(1, round(len(text) / 4))


class LLMCallLogEntry(BaseModel):
    id: str
    started_at: str
    completed_at: str = ""
    status: str = "running"
    backend_kind: str
    base_url: str
    model: str
    response_format: str | None = None
    call_type: str = "chat"
    system_prompt: str = ""
    user_prompt: str = ""
    prompt_preview: str = ""
    prompt_chars: int = 0
    estimated_context_tokens: int = 0
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    context_tokens_used: int = 0
    response_chars: int = 0
    duration_ms: int = 0
    error: str = ""


class LLMCallLogSummary(BaseModel):
    session_started_at: str
    total_calls: int
    completed_calls: int
    failed_calls: int
    running_calls: int
    largest_context_tokens: int
    largest_call: LLMCallLogEntry | None = None
    recent_calls: list[LLMCallLogEntry] = Field(default_factory=list)
    log_file: str


class LLMCallLogger:
    """Thread-safe process-session LLM call logger."""

    def __init__(self, data_dir: Path):
        self.session_started_at = _utc_now()
        self._lock = threading.Lock()
        self._entries: list[LLMCallLogEntry] = []
        self._by_id: dict[str, LLMCallLogEntry] = {}
        self._log_dir = data_dir / "llm_call_logs"
        self._log_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self._log_file = self._log_dir / f"session-{stamp}.jsonl"

    @property
    def log_file(self) -> Path:
        return self._log_file

    def start_chat(
        self,
        config: LLMBackendConfig,
        system_prompt: str,
        user_prompt: str,
        response_format: str | None,
        *,
        call_type: str = "chat",
    ) -> str:
        prompt_chars = len(system_prompt or "") + len(user_prompt or "")
        estimated_tokens = estimate_context_tokens(system_prompt, user_prompt)
        entry = LLMCallLogEntry(
            id=str(uuid.uuid4()),
            started_at=_utc_now(),
            backend_kind=config.kind,
            base_url=config.base_url,
            model=config.model,
            response_format=response_format,
            call_type=call_type,
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            prompt_preview=_preview(user_prompt or system_prompt),
            prompt_chars=prompt_chars,
            estimated_context_tokens=estimated_tokens,
            context_tokens_used=estimated_tokens,
        )
        with self._lock:
            self._entries.append(entry)
            self._by_id[entry.id] = entry
        return entry.id

    def finish(
        self,
        call_id: str,
        *,
        response_text: str = "",
        usage: dict[str, Any] | None = None,
        error: BaseException | str | None = None,
    ) -> None:
        with self._lock:
            entry = self._by_id.get(call_id)
            if entry is None:
                return
            start = _parse_dt(entry.started_at)
            completed = datetime.now(timezone.utc)
            prompt_tokens = _usage_int(usage, "prompt_tokens", "input_tokens")
            completion_tokens = _usage_int(usage, "completion_tokens", "output_tokens")
            total_tokens = _usage_int(usage, "total_tokens")
            entry.completed_at = completed.isoformat()
            entry.duration_ms = max(0, round((completed - start).total_seconds() * 1000))
            entry.response_chars = len(response_text or "")
            entry.prompt_tokens = prompt_tokens
            entry.completion_tokens = completion_tokens
            entry.total_tokens = total_tokens
            entry.context_tokens_used = prompt_tokens or entry.estimated_context_tokens
            if error:
                entry.status = "failed"
                entry.error = str(error)
            else:
                entry.status = "completed"
            self._append_locked(entry)

    def summary(self, limit: int = 20) -> LLMCallLogSummary:
        with self._lock:
            recent = list(reversed(self._entries[-limit:]))
            completed = [entry for entry in self._entries if entry.status == "completed"]
            failed = [entry for entry in self._entries if entry.status == "failed"]
            running = [entry for entry in self._entries if entry.status == "running"]
            finished = [entry for entry in self._entries if entry.status != "running"]
            largest = max(finished, key=lambda entry: entry.context_tokens_used, default=None)
            return LLMCallLogSummary(
                session_started_at=self.session_started_at,
                total_calls=len(self._entries),
                completed_calls=len(completed),
                failed_calls=len(failed),
                running_calls=len(running),
                largest_context_tokens=largest.context_tokens_used if largest else 0,
                largest_call=largest,
                recent_calls=recent,
                log_file=str(self._log_file),
            )

    def _append_locked(self, entry: LLMCallLogEntry) -> None:
        with self._log_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry.model_dump(mode="json"), ensure_ascii=False) + "\n")


_LOGGER: LLMCallLogger | None = None
_LOGGER_LOCK = threading.Lock()


def get_llm_call_logger(data_dir: Path | None = None) -> LLMCallLogger:
    global _LOGGER
    with _LOGGER_LOCK:
        if _LOGGER is None:
            root = data_dir or Path(__file__).parent.parent.parent / "data"
            _LOGGER = LLMCallLogger(root)
        return _LOGGER


def _preview(text: str, limit: int = 240) -> str:
    collapsed = " ".join(str(text or "").split())
    if len(collapsed) <= limit:
        return collapsed
    return f"{collapsed[:limit - 1]}..."


def _parse_dt(value: str) -> datetime:
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return datetime.now(timezone.utc)


def _usage_int(usage: dict[str, Any] | None, *keys: str) -> int | None:
    if not isinstance(usage, dict):
        return None
    for key in keys:
        value = usage.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return round(value)
    return None
