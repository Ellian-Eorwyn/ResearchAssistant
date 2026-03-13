"""Source download router: trigger and monitor local source capture workflow."""

from __future__ import annotations

import threading

from fastapi import APIRouter, Body, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

from backend.models.bibliography import BibliographyArtifact
from backend.models.common import PipelineStage
from backend.models.settings import AppSettings
from backend.models.sources import SourceListUploadResponse
from backend.pipeline.source_downloader import SourceDownloadOrchestrator
from backend.pipeline.source_list_parser import parse_source_list_upload

router = APIRouter()


class SourceDownloadRequest(BaseModel):
    rerun_failed_only: bool = False


@router.post("/sources/upload-list", response_model=SourceListUploadResponse)
async def upload_source_list(
    request: Request,
    file: UploadFile = File(...),
    job_id: str | None = Form(default=None),
) -> SourceListUploadResponse:
    store = request.app.state.file_store
    filename = file.filename or "sources.csv"
    content = await file.read()

    try:
        parsed = parse_source_list_upload(filename=filename, content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    merged_with_existing_job = False
    target_job_id = (job_id or "").strip()
    if target_job_id:
        if not store.job_exists(target_job_id):
            raise HTTPException(status_code=404, detail="Job not found")
        merged_with_existing_job = True
    else:
        target_job_id = store.create_job()

    existing_raw = store.load_artifact(target_job_id, "03_bibliography")
    if existing_raw:
        try:
            existing = BibliographyArtifact.model_validate(existing_raw)
        except Exception:
            existing = BibliographyArtifact(
                sections=[],
                entries=[],
                total_raw_entries=0,
                parse_failures=0,
            )
    else:
        existing = BibliographyArtifact(
            sections=[],
            entries=[],
            total_raw_entries=0,
            parse_failures=0,
        )

    merged_entries = [*existing.entries, *parsed.entries]
    merged_bibliography = BibliographyArtifact(
        sections=existing.sections,
        entries=merged_entries,
        total_raw_entries=len(merged_entries),
        parse_failures=existing.parse_failures,
    )

    store.save_upload(target_job_id, filename, content)
    store.save_artifact(
        target_job_id,
        "03_bibliography",
        merged_bibliography.model_dump(mode="json"),
    )

    return SourceListUploadResponse(
        job_id=target_job_id,
        filename=filename,
        total_rows=parsed.total_rows,
        accepted_rows=parsed.accepted_rows,
        missing_url_rows=parsed.missing_url_rows,
        estimated_duplicate_urls=parsed.estimated_duplicate_urls,
        merged_with_existing_job=merged_with_existing_job,
        total_urls_in_job=len(merged_entries),
    )


@router.post("/sources/{job_id}/download")
async def start_source_download(
    job_id: str,
    request: Request,
    payload: SourceDownloadRequest = Body(default_factory=SourceDownloadRequest),
) -> dict:
    store = request.app.state.file_store
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    bibliography = store.load_artifact(job_id, "03_bibliography")
    if bibliography is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "Bibliography artifact not found. Run extraction first "
                "or upload a source URL spreadsheet."
            ),
        )

    pipeline_status = store.get_job_status(job_id) or {}
    current_stage = pipeline_status.get("current_stage")
    if current_stage not in (
        PipelineStage.COMPLETED.value,
        PipelineStage.FAILED.value,
        PipelineStage.PENDING.value,
    ):
        raise HTTPException(
            status_code=409,
            detail="Citation extraction is still running. Wait for completion first.",
        )

    source_status = store.get_source_status(job_id) or {}
    if source_status.get("state") == "running":
        raise HTTPException(status_code=409, detail="Source download is already running")

    raw_settings = store.load_settings()
    settings = AppSettings(**raw_settings) if raw_settings else AppSettings()
    jobs = request.app.state.source_download_jobs
    jobs_lock = request.app.state.source_download_lock

    orchestrator = SourceDownloadOrchestrator(
        job_id=job_id,
        store=store,
        rerun_failed_only=payload.rerun_failed_only,
        use_llm=settings.use_llm,
        llm_backend=settings.llm_backend,
        research_purpose=settings.research_purpose,
    )

    with jobs_lock:
        if job_id in jobs:
            raise HTTPException(status_code=409, detail="Source download is already running")
        jobs[job_id] = orchestrator

    def _run_and_cleanup() -> None:
        try:
            orchestrator.run()
        finally:
            with jobs_lock:
                current = jobs.get(job_id)
                if current is orchestrator:
                    jobs.pop(job_id, None)

    thread = threading.Thread(target=_run_and_cleanup, daemon=True)
    thread.start()

    return {
        "job_id": job_id,
        "status": "started",
        "rerun_failed_only": payload.rerun_failed_only,
    }


@router.post("/sources/{job_id}/cancel")
async def cancel_source_download(job_id: str, request: Request) -> dict:
    store = request.app.state.file_store
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    status = store.get_source_status(job_id) or {}
    if status.get("state") != "running":
        return {"job_id": job_id, "status": "not_running"}

    jobs = request.app.state.source_download_jobs
    jobs_lock = request.app.state.source_download_lock
    with jobs_lock:
        orchestrator = jobs.get(job_id)

    if orchestrator is None:
        return {"job_id": job_id, "status": "running_no_handle"}

    orchestrator.request_cancel()
    return {"job_id": job_id, "status": "cancelling"}


@router.get("/sources/{job_id}/status")
async def get_source_download_status(job_id: str, request: Request) -> dict:
    store = request.app.state.file_store
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    status = store.get_source_status(job_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Source download status not found")
    return status


@router.get("/sources/{job_id}/manifest/csv")
async def download_source_manifest_csv(job_id: str, request: Request):
    store = request.app.state.file_store
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    manifest_path = store.get_sources_manifest_csv_path(job_id)
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail="Source manifest CSV not found")
    return FileResponse(
        path=str(manifest_path),
        media_type="text/csv",
        filename="manifest.csv",
    )


@router.get("/sources/{job_id}/manifest/xlsx")
async def download_source_manifest_xlsx(job_id: str, request: Request):
    store = request.app.state.file_store
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    manifest_path = store.get_sources_manifest_xlsx_path(job_id)
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail="Source manifest XLSX not found")
    return FileResponse(
        path=str(manifest_path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="manifest.xlsx",
    )


@router.get("/sources/{job_id}/bundle")
async def download_source_bundle(job_id: str, request: Request):
    store = request.app.state.file_store
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    manifest_path = store.get_sources_manifest_csv_path(job_id)
    if not manifest_path.exists():
        raise HTTPException(status_code=404, detail="Source outputs not found")

    bundle_path = store.get_sources_bundle_path(job_id)
    if not bundle_path.exists():
        bundle_path = store.build_sources_bundle(job_id)

    return FileResponse(
        path=str(bundle_path),
        media_type="application/zip",
        filename="output_run.zip",
    )
