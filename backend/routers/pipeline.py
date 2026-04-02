"""Pipeline router: start processing and check status."""

from __future__ import annotations

import threading

from fastapi import APIRouter, Body, HTTPException, Request

from backend.models.common import JobStatusResponse, PipelineStage, ProcessingConfig, StageStatus
from backend.pipeline.orchestrator import PipelineOrchestrator

router = APIRouter()


def _job_store(request: Request, job_id: str):
    repo_service = getattr(request.app.state, "repository_service", None)
    if repo_service is not None:
        return repo_service.job_store_for(job_id)
    return request.app.state.file_store


@router.post("/process/{job_id}")
async def start_processing(
    job_id: str,
    request: Request,
    config: ProcessingConfig = Body(default_factory=ProcessingConfig),
) -> dict:
    store = _job_store(request, job_id)
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    status = store.get_job_status(job_id)
    if status and status.get("current_stage") not in (
        PipelineStage.PENDING.value,
        PipelineStage.FAILED.value,
        PipelineStage.COMPLETED.value,
    ):
        raise HTTPException(status_code=409, detail="Job is already running")

    # Reset status if re-running
    if status and status.get("current_stage") in (
        PipelineStage.FAILED.value,
        PipelineStage.COMPLETED.value,
    ):
        store.create_job.__func__  # just a check
        # Re-initialize status
        from datetime import datetime, timezone

        store.save_job_status(
            job_id,
            {
                "job_id": job_id,
                "current_stage": PipelineStage.PENDING.value,
                "stages": [
                    StageStatus(stage=s).model_dump(mode="json")
                    for s in [
                        PipelineStage.INGESTING,
                        PipelineStage.DETECTING_REFERENCES,
                        PipelineStage.PARSING_BIBLIOGRAPHY,
                        PipelineStage.DETECTING_CITATIONS,
                        PipelineStage.EXTRACTING_SENTENCES,
                        PipelineStage.MATCHING_CITATIONS,
                        PipelineStage.EXPORTING,
                    ]
                ],
                "progress_pct": 0.0,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": None,
            },
        )

    orchestrator = PipelineOrchestrator(job_id, store, config)

    # Run in a background thread to not block the event loop
    thread = threading.Thread(target=orchestrator.run, daemon=True)
    thread.start()

    return {"job_id": job_id, "status": "started"}


@router.get("/status/{job_id}")
async def get_status(job_id: str, request: Request) -> dict:
    store = _job_store(request, job_id)
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    status = store.get_job_status(job_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Job status not found")

    return status
