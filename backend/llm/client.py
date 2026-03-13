"""Unified LLM client for Ollama and OpenAI-compatible backends."""

from __future__ import annotations

import base64
import json
import logging

import httpx

from backend.models.settings import LLMBackendConfig

logger = logging.getLogger(__name__)


class UnifiedLLMClient:
    """Wraps both Ollama and OpenAI-compatible API backends."""

    def __init__(self, config: LLMBackendConfig):
        self.config = config
        self._client = httpx.AsyncClient(timeout=120.0)

    async def close(self) -> None:
        await self._client.aclose()

    async def list_models(self) -> list[str]:
        """List available models from the configured backend."""
        base = self.config.base_url.rstrip("/")
        try:
            if self.config.kind == "ollama":
                resp = await self._client.get(f"{base}/api/tags")
                resp.raise_for_status()
                data = resp.json()
                return [m["name"] for m in data.get("models", [])]
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
        if self.config.kind == "ollama":
            return await self._ollama_chat(system_prompt, user_prompt, response_format)
        else:
            return await self._openai_chat(system_prompt, user_prompt, response_format)

    async def vision_ocr(
        self,
        prompt: str,
        image_bytes: bytes,
        mime_type: str = "image/png",
    ) -> str:
        """Run OCR-style extraction from an image using a multimodal model."""
        if self.config.kind == "ollama":
            return await self._ollama_vision_ocr(prompt, image_bytes)
        return await self._openai_vision_ocr(prompt, image_bytes, mime_type)

    async def _ollama_chat(
        self, system: str, user: str, fmt: str | None
    ) -> str:
        base = self.config.base_url.rstrip("/")
        body: dict = {
            "model": self.config.model,
            "stream": False,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "options": {
                "temperature": self.config.temperature,
                "num_ctx": 8192,
            },
        }
        if fmt == "json":
            body["format"] = "json"
        think_value = _ollama_think_value(self.config.think_mode)
        if think_value is not None:
            body["think"] = think_value

        resp = await self._client.post(f"{base}/api/chat", json=body)
        resp.raise_for_status()
        return resp.json()["message"]["content"]

    async def _openai_chat(
        self, system: str, user: str, fmt: str | None
    ) -> str:
        base = self.config.base_url.rstrip("/")
        headers = self._openai_headers()
        body: dict = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": 0,
        }
        if fmt == "json":
            body["response_format"] = {"type": "json_object"}

        for path in ["/v1/chat/completions", "/chat/completions"]:
            try:
                resp = await self._client.post(
                    f"{base}{path}", json=body, headers=headers
                )
                if resp.status_code == 404:
                    continue
                resp.raise_for_status()
                return resp.json()["choices"][0]["message"]["content"]
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                raise

        raise ValueError("Could not reach chat completions endpoint")

    async def _ollama_vision_ocr(self, prompt: str, image_bytes: bytes) -> str:
        base = self.config.base_url.rstrip("/")
        image_b64 = base64.b64encode(image_bytes).decode("ascii")
        body: dict = {
            "model": self.config.model,
            "stream": False,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are an OCR engine. Return only extracted text from the image."
                    ),
                },
                {
                    "role": "user",
                    "content": prompt,
                    "images": [image_b64],
                },
            ],
            "options": {"temperature": self.config.temperature},
        }
        think_value = _ollama_think_value(self.config.think_mode)
        if think_value is not None:
            body["think"] = think_value
        resp = await self._client.post(f"{base}/api/chat", json=body)
        resp.raise_for_status()
        return str(resp.json().get("message", {}).get("content", "")).strip()

    async def _openai_vision_ocr(
        self,
        prompt: str,
        image_bytes: bytes,
        mime_type: str,
    ) -> str:
        base = self.config.base_url.rstrip("/")
        headers = self._openai_headers()
        image_b64 = base64.b64encode(image_bytes).decode("ascii")
        content = [
            {"type": "text", "text": prompt},
            {
                "type": "image_url",
                "image_url": {"url": f"data:{mime_type};base64,{image_b64}"},
            },
        ]
        body: dict = {
            "model": self.config.model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are an OCR engine. Return only extracted text from the image."
                    ),
                },
                {"role": "user", "content": content},
            ],
            "temperature": 0,
        }

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
                message = resp.json()["choices"][0]["message"]["content"]
                return _normalize_openai_message_content(message)
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


def _ollama_think_value(think_mode: str) -> bool | None:
    mode = (think_mode or "default").strip().lower()
    if mode == "think":
        return True
    if mode == "no_think":
        return False
    return None
