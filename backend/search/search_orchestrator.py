"""Background orchestrator for AI-powered web search."""

from __future__ import annotations

import json
import logging
import math
from urllib.parse import urlparse

from backend.llm.client import UnifiedLLMClient
from backend.llm.prompts import (
    SEARCH_QUERY_GENERATION_SYSTEM,
    SEARCH_QUERY_GENERATION_USER,
    SEARCH_RELEVANCE_SYSTEM,
    SEARCH_RELEVANCE_USER,
)
from backend.models.search import SearchJobStatus, SearchResultItem
from backend.models.settings import LLMBackendConfig
from backend.search.searxng_client import SearXNGClient

logger = logging.getLogger(__name__)

RELEVANCE_BATCH_SIZE = 10


def _normalize_url_key(url: str) -> str:
    """Produce a deduplication key from a URL."""
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        path = parsed.path.rstrip("/")
        return f"{parsed.scheme}://{host}{path}"
    except Exception:
        return url.strip().lower()


class SearchOrchestrator:
    """Runs in a background thread to generate queries, search, and score."""

    def __init__(
        self,
        job_id: str,
        prompt: str,
        research_purpose: str,
        searxng_base_url: str,
        llm_config: LLMBackendConfig,
        target_count: int = 200,
        categories: list[str] | None = None,
        language: str = "",
        time_range: str = "",
    ) -> None:
        self.job_id = job_id
        self.prompt = prompt
        self.research_purpose = research_purpose
        self.searxng_base_url = searxng_base_url
        self.llm_config = llm_config
        self.target_count = target_count
        self.categories = [str(value).strip() for value in (categories or []) if str(value).strip()]
        self.language = str(language or "").strip()
        self.time_range = str(time_range or "").strip()

        self.status = SearchJobStatus(
            job_id=job_id,
            prompt=prompt,
            categories=list(self.categories),
            language=self.language,
            time_range=self.time_range,
        )
        self._cancel_requested = False

    def cancel(self) -> None:
        self._cancel_requested = True

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------

    def run(self) -> SearchJobStatus:
        """Execute the full search pipeline. Called from a daemon thread."""
        llm = UnifiedLLMClient(self.llm_config)
        searxng = SearXNGClient(self.searxng_base_url)
        try:
            self._generate_queries(llm)
            if self._cancel_requested:
                self.status.state = "completed"
                return self.status

            self._execute_searches(searxng)
            if self._cancel_requested:
                self.status.state = "completed"
                return self.status

            self._score_results(llm)
            self.status.state = "completed"
        except Exception as exc:
            logger.exception("Search job %s failed", self.job_id)
            self.status.state = "failed"
            self.status.error_message = str(exc)
        finally:
            llm.sync_close()
            searxng.close()

        return self.status

    # ------------------------------------------------------------------
    # Step 1: generate diverse search queries via LLM
    # ------------------------------------------------------------------

    def _generate_queries(self, llm: UnifiedLLMClient) -> None:
        self.status.state = "generating_queries"
        query_count = max(5, min(12, self.target_count // 40))

        user_prompt = SEARCH_QUERY_GENERATION_USER.format(
            research_purpose=self.research_purpose or "(not specified)",
            search_prompt=self.prompt,
            query_count=query_count,
        )
        filter_context: list[str] = []
        if self.categories:
            filter_context.append(
                f"Preferred search categories: {', '.join(self.categories)}."
            )
        if self.language:
            filter_context.append(f"Preferred search language: {self.language}.")
        if self.time_range:
            filter_context.append(f"Preferred time range: {self.time_range}.")
        if filter_context:
            user_prompt = f"{user_prompt}\n\nSearch filters:\n" + "\n".join(filter_context)

        raw = llm.sync_chat_completion(
            SEARCH_QUERY_GENERATION_SYSTEM, user_prompt, response_format="json"
        )

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("LLM returned non-JSON for query generation: %s", raw[:200])
            payload = {}

        queries = payload.get("queries", [])
        if not queries:
            queries = [self.prompt]

        self.status.generated_queries = [str(q) for q in queries]
        self.status.total_queries = len(self.status.generated_queries)
        logger.info("Generated %d search queries for job %s", len(queries), self.job_id)

    # ------------------------------------------------------------------
    # Step 2: execute SearXNG searches and deduplicate
    # ------------------------------------------------------------------

    def _execute_searches(self, searxng: SearXNGClient) -> None:
        self.status.state = "searching"
        queries = self.status.generated_queries
        if not queries:
            return

        target_per_query = max(20, math.ceil(self.target_count / len(queries)))
        pages_per_query = min(5, max(1, math.ceil(target_per_query / 20)))

        seen: dict[str, SearchResultItem] = {}
        query_errors = 0

        for qi, query in enumerate(queries):
            if self._cancel_requested:
                break

            try:
                raw_results = searxng.search_paginated(
                    query,
                    target_results=target_per_query,
                    max_pages=pages_per_query,
                    categories=self.categories,
                    language=self.language,
                    time_range=self.time_range,
                )
            except Exception as exc:
                logger.warning("Search query %r failed: %s", query, exc)
                query_errors += 1
                self.status.queries_completed = qi + 1
                continue

            for r in raw_results:
                url = (r.get("url") or "").strip()
                if not url:
                    continue
                key = _normalize_url_key(url)
                existing = seen.get(key)
                score = float(r.get("score", 0.0) or 0.0)

                if existing is None or score > existing.searxng_score:
                    seen[key] = SearchResultItem(
                        url=url,
                        title=(r.get("title") or "").strip(),
                        snippet=(r.get("content") or "").strip(),
                        engine=str(r.get("engine") or ""),
                        engines=r.get("engines") or [],
                        authors=r.get("authors") or [],
                        doi=str(r.get("doi") or ""),
                        html_url=str(r.get("html_url") or ""),
                        pdf_url=str(r.get("pdf_url") or ""),
                        searxng_score=score,
                        category=str(r.get("category") or ""),
                        published_date=str(r.get("published_date") or ""),
                    )

            self.status.queries_completed = qi + 1
            self.status.results_found = len(seen)

        self.status.results = list(seen.values())
        self.status.results_total = len(self.status.results)

        if not self.status.results and query_errors > 0:
            self.status.error_message = (
                f"All {query_errors} search queries failed. "
                "Check that the SearXNG base URL is correct and the instance is reachable."
            )

        logger.info(
            "Search job %s found %d unique results from %d queries",
            self.job_id,
            self.status.results_total,
            self.status.queries_completed,
        )

    # ------------------------------------------------------------------
    # Step 3: lightweight LLM relevance scoring in batches
    # ------------------------------------------------------------------

    def _score_results(self, llm: UnifiedLLMClient) -> None:
        self.status.state = "scoring"
        results = self.status.results
        if not results:
            return

        scored_count = 0
        for batch_start in range(0, len(results), RELEVANCE_BATCH_SIZE):
            if self._cancel_requested:
                break

            batch = results[batch_start : batch_start + RELEVANCE_BATCH_SIZE]
            self._score_batch(llm, batch, batch_start)
            scored_count += len(batch)
            self.status.results_scored = scored_count

        # Sort by relevance descending
        self.status.results.sort(key=lambda r: r.relevance_score, reverse=True)

    def _score_batch(
        self, llm: UnifiedLLMClient, batch: list[SearchResultItem], start_index: int
    ) -> None:
        """Score a batch of results with a single LLM call."""
        lines: list[str] = []
        for i, item in enumerate(batch):
            lines.append(
                f"Result {i}:\n  Title: {item.title}\n  Snippet: {item.snippet}"
            )
        results_block = "\n\n".join(lines)

        user_prompt = SEARCH_RELEVANCE_USER.format(
            research_purpose=self.research_purpose or "(not specified)",
            search_prompt=self.prompt,
            results_block=results_block,
        )

        try:
            raw = llm.sync_chat_completion(
                SEARCH_RELEVANCE_SYSTEM, user_prompt, response_format="json"
            )
            payload = json.loads(raw)
        except Exception:
            logger.warning(
                "LLM relevance scoring failed for batch starting at %d", start_index
            )
            return

        scores = payload.get("scores", [])
        score_map: dict[int, float] = {}
        for entry in scores:
            if isinstance(entry, dict) and "index" in entry:
                score_map[int(entry["index"])] = float(entry.get("relevance_score", 0.0))

        for i, item in enumerate(batch):
            if i in score_map:
                item.relevance_score = score_map[i]
                item.relevance_scored = True
