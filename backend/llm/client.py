"""Unified LLM client for Ollama and OpenAI-compatible backends."""

from __future__ import annotations

import base64
import json
import logging

import httpx

from backend.llm.call_log import get_llm_call_logger
from backend.models.settings import LLMBackendConfig

logger = logging.getLogger(__name__)

OCR_SYSTEM = "You are an OCR engine. Return only extracted text from the image."


class UnifiedLLMClient:
    """Wraps both Ollama and OpenAI-compatible API backends."""

    def __init__(self, config: LLMBackendConfig):
        self.config = config
        self._client = httpx.AsyncClient(timeout=120.0)
        self._sync_client: httpx.Client | None = None

    async def close(self) -> None:
        await self._client.aclose()

    def sync_close(self) -> None:
        if self._sync_client is not None:
            self._sync_client.close()
            self._sync_client = None

    def _get_sync_client(self) -> httpx.Client:
        if self._sync_client is None:
            self._sync_client = httpx.Client(timeout=self.config.llm_timeout)
        return self._sync_client

    async def list_models(self) -> list[str]:
        """List available models from the configured backend."""
        base = self.config.base_url.rstrip("/")
        try:
            if self.config.kind == "ollama":
                resp = await self._client.get(f"{base}/api/tags")
                resp.raise_for_status()
                data = resp.json()
                return [m["name"] for m in data.get("models", [])]
            elif self.config.kind == "anthropic":
                resp = await self._client.get(
                    f"{base}/v1/models", headers=self._anthropic_headers()
                )
                resp.raise_for_status()
                data = resp.json()
                return [m["id"] for m in data.get("data", [])]
            else:
                headers = self._openai_headers()
                for path in ["/v1/models", "/models"]:
                    try:
                        resp = await self._client.get(
                            f"{base}{path}", headers=headers
                        )
                        if resp.status_code == 404:
                            continue
                        resp.raise_for_status()
                        data = resp.json()
                        return [m["id"] for m in data.get("data", [])]
                    except httpx.HTTPStatusError as e:
                        if e.response.status_code == 404:
                            continue
                        raise
                return []
        except Exception as e:
            logger.error(f"Failed to list models: {e}")
            raise

    async def chat_completion(
        self,
        system_prompt: str,
        user_prompt: str,
        response_format: str | None = "json",
    ) -> str:
        """Send a chat completion request and return the text response."""
        call_id = get_llm_call_logger().start_chat(
            self.config,
            system_prompt,
            user_prompt,
            response_format,
        )
        try:
            if self.config.kind == "ollama":
                content, usage = await self._ollama_chat(
                    system_prompt, user_prompt, response_format
                )
            elif self.config.kind == "anthropic":
                content, usage = await self._anthropic_chat(
                    system_prompt, user_prompt, response_format
                )
            else:
                content, usage = await self._openai_chat(
                    system_prompt, user_prompt, response_format
                )
            get_llm_call_logger().finish(call_id, response_text=content, usage=usage)
            return content
        except Exception as exc:
            get_llm_call_logger().finish(call_id, error=exc)
            raise

    def sync_chat_completion(
        self,
        system_prompt: str,
        user_prompt: str,
        response_format: str | None = "json",
    ) -> str:
        """Synchronous chat completion using a shared httpx.Client."""
        call_id = get_llm_call_logger().start_chat(
            self.config,
            system_prompt,
            user_prompt,
            response_format,
        )
        try:
            if self.config.kind == "ollama":
                content, usage = self._ollama_chat_sync(
                    system_prompt, user_prompt, response_format
                )
            elif self.config.kind == "anthropic":
                content, usage = self._anthropic_chat_sync(
                    system_prompt, user_prompt, response_format
                )
            else:
                content, usage = self._openai_chat_sync(
                    system_prompt, user_prompt, response_format
                )
            get_llm_call_logger().finish(call_id, response_text=content, usage=usage)
            return content
        except Exception as exc:
            get_llm_call_logger().finish(call_id, error=exc)
            raise

    async def vision_ocr(
        self,
        prompt: str,
        image_bytes: bytes,
        mime_type: str = "image/png",
    ) -> str:
        """Run OCR-style extraction from an image using a multimodal model."""
        call_id = get_llm_call_logger().start_chat(
            self.config,
            OCR_SYSTEM,
            f"{prompt}\n\n[image: {mime_type}, {len(image_bytes)} bytes]",
            None,
            call_type="vision_ocr",
        )
        try:
            if self.config.kind == "ollama":
                content, usage = await self._ollama_vision(
                    OCR_SYSTEM, prompt, image_bytes, None
                )
            elif self.config.kind == "anthropic":
                content, usage = await self._anthropic_vision(
                    OCR_SYSTEM, prompt, image_bytes, mime_type, None
                )
            else:
                content, usage = await self._openai_vision(
                    OCR_SYSTEM, prompt, image_bytes, mime_type, None
                )
            get_llm_call_logger().finish(call_id, response_text=content, usage=usage)
            return content
        except Exception as exc:
            get_llm_call_logger().finish(call_id, error=exc)
            raise

    async def vision_chat(
        self,
        system_prompt: str,
        user_prompt: str,
        image_bytes: bytes,
        mime_type: str = "image/png",
        response_format: str | None = "json",
    ) -> str:
        """Send a multimodal (image + text) chat request and return the text.

        Generalizes :meth:`vision_ocr`: the system prompt and JSON response format
        are caller-controlled, so the same multimodal path serves image
        classification and description, not just OCR.
        """
        call_id = get_llm_call_logger().start_chat(
            self.config,
            system_prompt,
            f"{user_prompt}\n\n[image: {mime_type}, {len(image_bytes)} bytes]",
            response_format,
            call_type="vision_chat",
        )
        try:
            if self.config.kind == "ollama":
                content, usage = await self._ollama_vision(
                    system_prompt, user_prompt, image_bytes, response_format
                )
            elif self.config.kind == "anthropic":
                content, usage = await self._anthropic_vision(
                    system_prompt, user_prompt, image_bytes, mime_type, response_format
                )
            else:
                content, usage = await self._openai_vision(
                    system_prompt, user_prompt, image_bytes, mime_type, response_format
                )
            get_llm_call_logger().finish(call_id, response_text=content, usage=usage)
            return content
        except Exception as exc:
            get_llm_call_logger().finish(call_id, error=exc)
            raise

    # ---- Async chat methods ----

    async def _ollama_chat(
        self, system: str, user: str, fmt: str | None
    ) -> tuple[str, dict]:
        base = self.config.base_url.rstrip("/")
        body = self._build_ollama_body(system, user, fmt)
        resp = await self._client.post(f"{base}/api/chat", json=body)
        resp.raise_for_status()
        data = resp.json()
        return data["message"]["content"], _ollama_usage(data)

    async def _openai_chat(
        self, system: str, user: str, fmt: str | None
    ) -> tuple[str, dict]:
        base = self.config.base_url.rstrip("/")
        headers = self._openai_headers()
        body = self._build_openai_body(system, user, fmt)

        for path in ["/v1/chat/completions", "/chat/completions"]:
            try:
                resp = await self._client.post(
                    f"{base}{path}", json=body, headers=headers
                )
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"], data.get("usage", {})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                raise

        raise ValueError("Could not reach chat completions endpoint")

    # ---- Sync chat methods ----

    def _ollama_chat_sync(
        self, system: str, user: str, fmt: str | None
    ) -> tuple[str, dict]:
        client = self._get_sync_client()
        base = self.config.base_url.rstrip("/")
        body = self._build_ollama_body(system, user, fmt)
        resp = client.post(
            f"{base}/api/chat", json=body, timeout=self.config.llm_timeout
        )
        resp.raise_for_status()
        data = resp.json()
        return data["message"]["content"], _ollama_usage(data)

    def _openai_chat_sync(
        self, system: str, user: str, fmt: str | None
    ) -> tuple[str, dict]:
        client = self._get_sync_client()
        base = self.config.base_url.rstrip("/")
        headers = self._openai_headers()
        body = self._build_openai_body(system, user, fmt)

        for path in ["/v1/chat/completions", "/chat/completions"]:
            try:
                resp = client.post(
                    f"{base}{path}",
                    json=body,
                    headers=headers,
                    timeout=self.config.llm_timeout,
                )
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"], data.get("usage", {})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                raise

        raise ValueError("Could not reach chat completions endpoint")

    # ---- Shared body builders ----

    def _build_ollama_body(
        self, system: str, user: str, fmt: str | None
    ) -> dict:
        body: dict = {
            "model": self.config.model,
            "stream": False,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "options": {
                "temperature": self.config.temperature,
                "num_ctx": self.config.num_ctx,
            },
        }
        if fmt == "json":
            body["format"] = "json"
        think_value = _ollama_think_from_level(
            _effective_reasoning_level(self.config)
        )
        if think_value is not None:
            body["think"] = think_value
        return body

    def _build_openai_body(
        self, system: str, user: str, fmt: str | None
    ) -> dict:
        # Honor the configured temperature/max_tokens (this path used to hardcode
        # temperature=0 and drop everything else). reasoning_effort is only sent
        # for a non-default reasoning_level so standard chat models -- which
        # don't accept it -- are unaffected; reasoning models (o-series, gpt-5)
        # additionally reject `temperature`/`max_tokens`, an accepted edge for a
        # user-configured OpenAI-compatible endpoint.
        body: dict = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": self.config.temperature,
        }
        if fmt == "json":
            body["response_format"] = {"type": "json_object"}
        if self.config.max_tokens > 0:
            body["max_tokens"] = self.config.max_tokens
        effort = _openai_reasoning_effort(_effective_reasoning_level(self.config))
        if effort is not None:
            body["reasoning_effort"] = effort
        return body

    # ---- Vision methods (async only) ----

    async def _ollama_vision(
        self, system: str, user: str, image_bytes: bytes, fmt: str | None
    ) -> tuple[str, dict]:
        base = self.config.base_url.rstrip("/")
        image_b64 = base64.b64encode(image_bytes).decode("ascii")
        body: dict = {
            "model": self.config.model,
            "stream": False,
            "messages": [
                {"role": "system", "content": system},
                {
                    "role": "user",
                    "content": user,
                    "images": [image_b64],
                },
            ],
            "options": {"temperature": self.config.temperature},
        }
        if fmt == "json":
            body["format"] = "json"
        think_value = _ollama_think_from_level(
            _effective_reasoning_level(self.config)
        )
        if think_value is not None:
            body["think"] = think_value
        resp = await self._client.post(f"{base}/api/chat", json=body)
        resp.raise_for_status()
        data = resp.json()
        return str(data.get("message", {}).get("content", "")).strip(), _ollama_usage(data)

    async def _openai_vision(
        self,
        system: str,
        user: str,
        image_bytes: bytes,
        mime_type: str,
        fmt: str | None,
    ) -> tuple[str, dict]:
        base = self.config.base_url.rstrip("/")
        headers = self._openai_headers()
        image_b64 = base64.b64encode(image_bytes).decode("ascii")
        content = [
            {"type": "text", "text": user},
            {
                "type": "image_url",
                "image_url": {"url": f"data:{mime_type};base64,{image_b64}"},
            },
        ]
        body: dict = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": content},
            ],
            "temperature": self.config.temperature,
        }
        if fmt == "json":
            body["response_format"] = {"type": "json_object"}
        if self.config.max_tokens > 0:
            body["max_tokens"] = self.config.max_tokens
        effort = _openai_reasoning_effort(_effective_reasoning_level(self.config))
        if effort is not None:
            body["reasoning_effort"] = effort

        for path in ["/v1/chat/completions", "/chat/completions"]:
            try:
                resp = await self._client.post(
                    f"{base}{path}",
                    json=body,
                    headers=headers,
                )
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                data = resp.json()
                message = data["choices"][0]["message"]["content"]
                return _normalize_openai_message_content(message), data.get("usage", {})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                raise

        raise ValueError("Could not reach multimodal chat completions endpoint")

    def _openai_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        return headers

    # ---- Anthropic (native Messages API) ----

    def _anthropic_headers(self) -> dict[str, str]:
        return {
            "content-type": "application/json",
            "x-api-key": self.config.api_key,
            "anthropic-version": "2023-06-01",
        }

    def _build_anthropic_body(
        self, system: str, user: str, fmt: str | None
    ) -> dict:
        # Anthropic's Messages API differs from OpenAI's: `system` is top-level,
        # `max_tokens` is required, and there is no JSON response_format (we
        # emulate it with a system instruction). Sampling params (temperature)
        # are intentionally omitted -- current Claude models reject them with a
        # 400 -- so reasoning is expressed via `output_config.effort` / a
        # disabled `thinking` block instead.
        system_text = system
        if fmt == "json":
            system_text = (
                f"{system}\n\nRespond with a single valid JSON object and "
                "nothing else."
            ).strip()
        body: dict = {
            "model": self.config.model,
            "max_tokens": self.config.max_tokens or 4096,
            "system": system_text,
            "messages": [{"role": "user", "content": user}],
        }
        level = _effective_reasoning_level(self.config)
        if level == "off":
            body["thinking"] = {"type": "disabled"}
        else:
            effort = _anthropic_effort(level)
            if effort is not None:
                body["output_config"] = {"effort": effort}
        return body

    async def _anthropic_chat(
        self, system: str, user: str, fmt: str | None
    ) -> tuple[str, dict]:
        base = self.config.base_url.rstrip("/")
        body = self._build_anthropic_body(system, user, fmt)
        resp = await self._client.post(
            f"{base}/v1/messages", json=body, headers=self._anthropic_headers()
        )
        resp.raise_for_status()
        data = resp.json()
        return _anthropic_text(data), _anthropic_usage(data)

    def _anthropic_chat_sync(
        self, system: str, user: str, fmt: str | None
    ) -> tuple[str, dict]:
        client = self._get_sync_client()
        base = self.config.base_url.rstrip("/")
        body = self._build_anthropic_body(system, user, fmt)
        resp = client.post(
            f"{base}/v1/messages",
            json=body,
            headers=self._anthropic_headers(),
            timeout=self.config.llm_timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        return _anthropic_text(data), _anthropic_usage(data)

    async def _anthropic_vision(
        self,
        system: str,
        user: str,
        image_bytes: bytes,
        mime_type: str,
        fmt: str | None,
    ) -> tuple[str, dict]:
        base = self.config.base_url.rstrip("/")
        image_b64 = base64.b64encode(image_bytes).decode("ascii")
        body = self._build_anthropic_body(system, user, fmt)
        body["messages"] = [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user},
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": mime_type,
                            "data": image_b64,
                        },
                    },
                ],
            }
        ]
        resp = await self._client.post(
            f"{base}/v1/messages", json=body, headers=self._anthropic_headers()
        )
        resp.raise_for_status()
        data = resp.json()
        return _anthropic_text(data), _anthropic_usage(data)


def _normalize_openai_message_content(content: object) -> str:
    if isinstance(content, str):
        return content.strip()
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str) and text.strip():
                    parts.append(text.strip())
        return "\n".join(parts).strip()
    return str(content).strip()


def _ollama_usage(data: dict) -> dict[str, int]:
    prompt_tokens = data.get("prompt_eval_count")
    completion_tokens = data.get("eval_count")
    usage: dict[str, int] = {}
    if isinstance(prompt_tokens, int):
        usage["prompt_tokens"] = prompt_tokens
    if isinstance(completion_tokens, int):
        usage["completion_tokens"] = completion_tokens
    if isinstance(prompt_tokens, int) and isinstance(completion_tokens, int):
        usage["total_tokens"] = prompt_tokens + completion_tokens
    return usage


def _effective_reasoning_level(config: LLMBackendConfig) -> str:
    """Resolve the cross-provider reasoning level for a config.

    Prefers the unified `reasoning_level`; when it is "default" it falls back to
    the legacy Ollama `think_mode` so existing configs keep behaving.
    """
    level = (config.reasoning_level or "default").strip().lower()
    if level != "default":
        return level
    return {"think": "high", "no_think": "off"}.get(config.think_mode, "default")


def _ollama_think_from_level(level: str) -> bool | None:
    if level in ("low", "medium", "high"):
        return True
    if level == "off":
        return False
    return None  # "default" -> omit the field


def _openai_reasoning_effort(level: str) -> str | None:
    if level in ("low", "medium", "high"):
        return level
    if level == "off":
        return "minimal"
    return None  # "default" -> omit the field


def _anthropic_effort(level: str) -> str | None:
    if level in ("low", "medium", "high"):
        return level
    return None  # "off"/"default" handled by the caller


def _anthropic_text(data: dict) -> str:
    blocks = data.get("content", []) or []
    parts = [
        b.get("text", "")
        for b in blocks
        if isinstance(b, dict) and b.get("type") == "text"
    ]
    return "".join(parts).strip()


def _anthropic_usage(data: dict) -> dict[str, int]:
    usage_raw = data.get("usage", {}) or {}
    prompt_tokens = usage_raw.get("input_tokens")
    completion_tokens = usage_raw.get("output_tokens")
    usage: dict[str, int] = {}
    if isinstance(prompt_tokens, int):
        usage["prompt_tokens"] = prompt_tokens
    if isinstance(completion_tokens, int):
        usage["completion_tokens"] = completion_tokens
    if isinstance(prompt_tokens, int) and isinstance(completion_tokens, int):
        usage["total_tokens"] = prompt_tokens + completion_tokens
    return usage
