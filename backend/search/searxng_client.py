"""HTTP client for querying a SearXNG instance."""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

CONFIG_CACHE_TTL_SECONDS = 30.0
TIME_RANGE_OPTIONS = ("day", "month", "year")

_CONFIG_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CONFIG_CACHE_LOCK = threading.Lock()


def _normalize_str_list(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        normalized: list[str] = []
        for item in value:
            text = str(item or "").strip()
            if text:
                normalized.append(text)
        return normalized
    return []


def _clean_search_result(result: dict[str, Any]) -> dict[str, Any]:
    published_date = str(
        result.get("published_date")
        or result.get("publishedDate")
        or result.get("pubdate")
        or ""
    ).strip()
    authors = _normalize_str_list(result.get("authors"))
    if not authors:
        authors = _normalize_str_list(result.get("author"))
    engines = _normalize_str_list(result.get("engines"))
    if not engines and result.get("engine"):
        engines = [str(result.get("engine") or "").strip()]
    return {
        **result,
        "title": str(result.get("title") or "").strip(),
        "url": str(result.get("url") or "").strip(),
        "content": str(result.get("content") or "").strip(),
        "engine": str(result.get("engine") or "").strip(),
        "engines": engines,
        "category": str(result.get("category") or "").strip(),
        "published_date": published_date,
        "authors": authors,
        "doi": str(result.get("doi") or "").strip(),
        "html_url": str(result.get("html_url") or "").strip(),
        "pdf_url": str(result.get("pdf_url") or "").strip(),
        "score": float(result.get("score", 0.0) or 0.0),
    }


def _normalize_locale_label(value: str, code: str) -> str:
    text = str(value or "").strip()
    return text or code


def _normalize_config_payload(payload: dict[str, Any]) -> dict[str, Any]:
    categories = sorted(
        {
            str(category).strip()
            for category in _normalize_str_list(payload.get("categories"))
            if str(category).strip()
        },
        key=lambda value: value.lower(),
    )

    raw_locales = payload.get("locales")
    locale_items: list[dict[str, str]] = []
    if isinstance(raw_locales, dict):
        for code, label in raw_locales.items():
            normalized_code = str(code or "").strip()
            if not normalized_code:
                continue
            locale_items.append(
                {
                    "value": normalized_code,
                    "label": _normalize_locale_label(str(label or ""), normalized_code),
                }
            )
    locale_items.sort(key=lambda item: item["label"].lower())
    locale_values = {item["value"] for item in locale_items}
    if "auto" not in locale_values:
        locale_items.insert(0, {"value": "auto", "label": "Auto-detect"})
    if "all" not in locale_values:
        insert_index = 1 if locale_items and locale_items[0]["value"] == "auto" else 0
        locale_items.insert(insert_index, {"value": "all", "label": "Default language"})

    raw_plugins = payload.get("plugins")
    plugin_names = {
        str(plugin.get("name") or "").strip()
        for plugin in raw_plugins
        if isinstance(plugin, dict)
    } if isinstance(raw_plugins, list) else set()

    engines = payload.get("engines")
    has_time_range_support = False
    if isinstance(engines, list):
        has_time_range_support = any(
            bool(engine.get("enabled")) and bool(engine.get("time_range_support"))
            for engine in engines
            if isinstance(engine, dict)
        )

    default_language = "auto" if any(item["value"] == "auto" for item in locale_items) else ""
    return {
        "categories": categories,
        "languages": locale_items,
        "time_ranges": list(TIME_RANGE_OPTIONS) if has_time_range_support else [],
        "supports_oa_doi_helper": bool(
            payload.get("default_doi_resolver")
            or payload.get("doi_resolvers")
            or "oa_doi_rewrite" in plugin_names
        ),
        "defaults": {
            "categories": ["general"] if "general" in categories else categories[:1],
            "language": default_language,
            "time_range": "",
        },
        "plugins": sorted(plugin_names),
    }


class SearXNGClient:
    """Synchronous client for the SearXNG JSON search API."""

    def __init__(self, base_url: str, *, timeout: float = 30.0):
        url = base_url.strip().rstrip("/")
        if url.endswith("/search"):
            url = url[: -len("/search")]
        self.base_url = url
        self._client = httpx.Client(timeout=timeout)

    def close(self) -> None:
        self._client.close()

    def build_search_params(
        self,
        query: str,
        *,
        page: int = 1,
        categories: list[str] | None = None,
        language: str = "",
        time_range: str = "",
        enabled_plugins: list[str] | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "q": query,
            "format": "json",
            "pageno": page,
        }
        normalized_categories = [value for value in _normalize_str_list(categories) if value]
        if normalized_categories:
            params["categories"] = ",".join(normalized_categories)
        normalized_language = str(language or "").strip()
        if normalized_language:
            params["language"] = normalized_language
        normalized_time_range = str(time_range or "").strip().lower()
        if normalized_time_range in TIME_RANGE_OPTIONS:
            params["time_range"] = normalized_time_range
        normalized_plugins = [value for value in _normalize_str_list(enabled_plugins) if value]
        if normalized_plugins:
            params["enabled_plugins"] = ",".join(normalized_plugins)
        return params

    def search(
        self,
        query: str,
        page: int = 1,
        *,
        categories: list[str] | None = None,
        language: str = "",
        time_range: str = "",
        enabled_plugins: list[str] | None = None,
    ) -> dict:
        """Execute a single search query and return the raw JSON response."""
        resp = self._client.get(
            f"{self.base_url}/search",
            params=self.build_search_params(
                query,
                page=page,
                categories=categories,
                language=language,
                time_range=time_range,
                enabled_plugins=enabled_plugins,
            ),
        )
        resp.raise_for_status()
        return resp.json()

    def get_config(self, *, force_refresh: bool = False) -> dict[str, Any]:
        cache_key = self.base_url
        now = time.monotonic()
        if not force_refresh:
            with _CONFIG_CACHE_LOCK:
                cached = _CONFIG_CACHE.get(cache_key)
            if cached and (now - cached[0]) < CONFIG_CACHE_TTL_SECONDS:
                return cached[1]

        response = self._client.get(f"{self.base_url}/config")
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("SearXNG /config did not return an object")
        normalized = _normalize_config_payload(payload)
        with _CONFIG_CACHE_LOCK:
            _CONFIG_CACHE[cache_key] = (now, normalized)
        return normalized

    def search_paginated(
        self,
        query: str,
        target_results: int = 100,
        *,
        max_pages: int = 5,
        page_delay: float = 0.5,
        categories: list[str] | None = None,
        language: str = "",
        time_range: str = "",
        enabled_plugins: list[str] | None = None,
    ) -> list[dict]:
        """Fetch multiple pages of results for a single query.

        Returns a flat list of result dicts from the SearXNG response.
        """
        results: list[dict] = []
        pages_needed = min(max_pages, max(1, (target_results + 19) // 20))

        for page in range(1, pages_needed + 1):
            try:
                data = self.search(
                    query,
                    page=page,
                    categories=categories,
                    language=language,
                    time_range=time_range,
                    enabled_plugins=enabled_plugins,
                )
                page_results = [
                    _clean_search_result(item)
                    for item in data.get("results", [])
                    if isinstance(item, dict)
                ]
                if not page_results:
                    break
                results.extend(page_results)
                if len(results) >= target_results:
                    break
            except httpx.HTTPError as exc:
                if page == 1:
                    raise
                logger.warning(
                    "SearXNG page %d for query %r failed: %s", page, query, exc
                )
                break

            if page < pages_needed:
                time.sleep(page_delay)

        return results[:target_results]
