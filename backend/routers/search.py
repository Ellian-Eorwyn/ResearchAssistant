"""Router for AI-powered web search via SearXNG."""

from __future__ import annotations

import logging
import threading
import uuid

from fastapi import APIRouter, HTTPException, Request

from backend.models.search import (
    SearchImportRequest,
    SearchImportResponse,
    SearchJobStatus,
    SearchRequest,
)
from backend.search.search_orchestrator import SearchOrchestrator

logger = logging.getLogger(__name__)

router = APIRouter()


def _search_worker(orchestrator: SearchOrchestrator) -> None:
    """Target for the background thread running a search job."""
    try:
        orchestrator.run()
    except Exception:
        orchestrator.status.state = "failed"
        orchestrator.status.error_message = "Unexpected error in search worker"
        logger.exception("Search worker crashed for job %s", orchestrator.job_id)


@router.post("/search/start", response_model=SearchJobStatus)
async def start_search(request: Request, payload: SearchRequest) -> SearchJobStatus:
    """Start a new AI-powered search job."""
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")

    settings = service.load_repo_settings()
    if not settings.use_llm:
        raise HTTPException(status_code=400, detail="LLM must be enabled in settings")
    if not settings.searxng_base_url:
        raise HTTPException(
            status_code=400,
            detail="SearXNG base URL must be configured in settings",
        )

    job_id = uuid.uuid4().hex[:12]
    orchestrator = SearchOrchestrator(
        job_id=job_id,
        prompt=payload.prompt,
        research_purpose=settings.research_purpose,
        searxng_base_url=settings.searxng_base_url,
        llm_config=settings.llm_backend,
        target_count=payload.target_count,
    )

    lock: threading.Lock = request.app.state.search_jobs_lock
    jobs: dict = request.app.state.search_jobs
    with lock:
        jobs[job_id] = orchestrator

    thread = threading.Thread(target=_search_worker, args=(orchestrator,), daemon=True)
    thread.start()

    return orchestrator.status


@router.get("/search/{job_id}/status", response_model=SearchJobStatus)
async def get_search_status(job_id: str, request: Request) -> SearchJobStatus:
    """Poll search job status and partial results."""
    lock: threading.Lock = request.app.state.search_jobs_lock
    jobs: dict = request.app.state.search_jobs
    with lock:
        orchestrator = jobs.get(job_id)
    if not orchestrator:
        raise HTTPException(status_code=404, detail="Search job not found")
    return orchestrator.status


@router.post("/search/{job_id}/cancel")
async def cancel_search(job_id: str, request: Request) -> dict:
    """Request cancellation of a running search job."""
    lock: threading.Lock = request.app.state.search_jobs_lock
    jobs: dict = request.app.state.search_jobs
    with lock:
        orchestrator = jobs.get(job_id)
    if not orchestrator:
        raise HTTPException(status_code=404, detail="Search job not found")
    orchestrator.cancel()
    return {"status": "cancel_requested"}


@router.post("/search/{job_id}/import", response_model=SearchImportResponse)
async def import_search_results(
    job_id: str, request: Request, payload: SearchImportRequest
) -> SearchImportResponse:
    """Import search results above relevance threshold into the repository."""
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")

    lock: threading.Lock = request.app.state.search_jobs_lock
    jobs: dict = request.app.state.search_jobs
    with lock:
        orchestrator = jobs.get(job_id)
    if not orchestrator:
        raise HTTPException(status_code=404, detail="Search job not found")
    if orchestrator.status.state != "completed":
        raise HTTPException(status_code=400, detail="Search has not completed yet")

    passing = [
        r for r in orchestrator.status.results
        if r.relevance_score >= payload.min_relevance
    ]

    if not passing:
        return SearchImportResponse(message="No results above the relevance threshold")

    csv_lines = ["url"]
    for r in passing:
        csv_lines.append(r.url)
    csv_content = "\n".join(csv_lines).encode("utf-8")

    try:
        result = service.import_source_list(
            filename="search_import.csv", content=csv_content
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return SearchImportResponse(
        imported_count=result.accepted_new,
        duplicates_skipped=result.duplicates_skipped,
        total_sources=result.total_sources,
        message=result.message,
    )
