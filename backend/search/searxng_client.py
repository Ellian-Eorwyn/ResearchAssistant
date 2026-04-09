"""HTTP client for querying a SearXNG instance."""

from __future__ import annotations

import logging
import time

import httpx

logger = logging.getLogger(__name__)


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

    def search(self, query: str, page: int = 1) -> dict:
        """Execute a single search query and return the raw JSON response."""
        resp = self._client.get(
            f"{self.base_url}/search",
            params={"q": query, "format": "json", "pageno": page},
        )
        resp.raise_for_status()
        return resp.json()

    def search_paginated(
        self,
        query: str,
        target_results: int = 100,
        *,
        max_pages: int = 5,
        page_delay: float = 0.5,
    ) -> list[dict]:
        """Fetch multiple pages of results for a single query.

        Returns a flat list of result dicts from the SearXNG response.
        """
        results: list[dict] = []
        pages_needed = min(max_pages, max(1, (target_results + 19) // 20))

        for page in range(1, pages_needed + 1):
            try:
                data = self.search(query, page=page)
                page_results = data.get("results", [])
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
