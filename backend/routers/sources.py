"""Source download router: trigger and monitor local source capture workflow."""

from __future__ import annotations

import logging
import threading

from fastapi import APIRouter, Body, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from backend.models.bibliography import BibliographyArtifact
from backend.models.common import PipelineStage
from backend.models.settings import EffectiveSettings
from backend.models.sources import (
    SourceDownloadRequest,
    SourceDownloadStatus,
    SourceItemStatus,
    SourceListUploadResponse,
    SourceManifestRow,
    SourceOutputOptions,
)
from backend.pipeline.source_downloader import (
    SourceDownloadOrchestrator,
    summarize_output_rows,
)
from backend.pipeline.source_list_parser import parse_source_list_upload

logger = logging.getLogger(__name__)

router = APIRouter()


def _job_store(request: Request, job_id: str):
    repo_service = getattr(request.app.state, "repository_service", None)
    if repo_service is not None:
        return repo_service.job_store_for(job_id)
    return request.app.state.file_store


def _selected_phase_names(
    *,
    run_download: bool,
    run_convert: bool,
    run_catalog: bool,
    run_llm_rating: bool,
    run_llm_summary: bool,
) -> list[str]:
    phases: list[str] = []
    if run_download:
        phases.append("fetch")
    if run_convert:
        phases.append("convert")
    if run_catalog:
        phases.append("catalog")
    if run_llm_rating:
        phases.append("tag")
    if run_llm_summary:
        phases.append("summarize")
    return phases


@router.post("/sources/upload-list", response_model=SourceListUploadResponse)
async def upload_source_list(
    request: Request,
    file: UploadFile = File(...),
    job_id: str | None = Form(default=None),
) -> SourceListUploadResponse:
    store = request.app.state.file_store
    repo_service = getattr(request.app.state, "repository_service", None)
    filename = file.filename or "sources.csv"
    content = await file.read()

    try:
        parsed = parse_source_list_upload(filename=filename, content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    merged_with_existing_job = False
    target_job_id = (job_id or "").strip()
    if target_job_id:
        if repo_service is not None:
            store = repo_service.job_store_for(target_job_id)
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
    store = _job_store(request, job_id)
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

    repo_service = request.app.state.repository_service

    run_download = bool(payload.run_download or payload.force_redownload)
    run_convert = bool(payload.run_convert or payload.force_convert or (run_download and payload.include_markdown))
    run_citation_verify = bool(payload.run_citation_verify or payload.force_citation_verify)
    run_catalog = bool(
        payload.run_catalog
        or payload.force_catalog
        or payload.run_llm_title
        or payload.force_title
        or run_citation_verify
    )
    run_llm_cleanup = bool(payload.run_llm_cleanup or payload.force_llm_cleanup)
    run_llm_title = bool(payload.run_llm_title or payload.force_title)
    run_llm_summary = bool(payload.run_llm_summary or payload.force_summary)
    run_llm_rating = bool(payload.run_llm_rating or payload.force_rating)

    if not (
        run_download
        or run_convert
        or run_catalog
        or run_citation_verify
        or run_llm_cleanup
        or run_llm_title
        or run_llm_summary
        or run_llm_rating
    ):
        raise HTTPException(status_code=400, detail="Select at least one phase to run")
    if run_download and not any(
        [
            payload.include_raw_file,
            payload.include_rendered_html,
            payload.include_rendered_pdf,
            payload.include_markdown,
        ]
    ):
        raise HTTPException(
            status_code=400,
            detail="Select at least one download output type",
        )
    existing_manifest = store.load_artifact(job_id, "06_sources_manifest")
    if not run_download and existing_manifest is None:
        seeded_rows = 0
        if repo_service.is_attached:
            try:
                seed_result = repo_service.seed_job_output_run(job_id)
                seeded_rows = int(seed_result.get("seeded_rows") or 0)
            except ValueError:
                seeded_rows = 0
        if seeded_rows > 0:
            logger.info(
                "Seeded %d repository rows into job %s for postprocess-only run",
                seeded_rows,
                job_id,
            )
            existing_manifest = store.load_artifact(job_id, "06_sources_manifest")
        else:
            raise HTTPException(
                status_code=409,
                detail="No existing output run found. Run download phase first.",
            )

    if not run_download and existing_manifest is None:
        raise HTTPException(
            status_code=409,
            detail="No existing output run found. Run download phase first.",
        )

    # Load effective settings (app-level + repo-level merged)
    if repo_service.is_attached:
        settings = repo_service.load_effective_settings()
    else:
        settings = EffectiveSettings()

    # Load project profile YAML if rating is requested
    project_profile_name = ""
    project_profile_yaml = ""
    if run_llm_rating:
        if repo_service.is_attached:
            try:
                project_profile_name, project_profile_yaml = repo_service._load_project_profile_yaml(
                    payload.project_profile_name,
                    research_purpose=settings.research_purpose,
                    default_when_blank=True,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
        else:
            try:
                project_profile_name, project_profile_yaml = store.resolve_project_profile(
                    payload.project_profile_name,
                    research_purpose=settings.research_purpose,
                    default_when_blank=True,
                )
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

    jobs = request.app.state.source_download_jobs
    jobs_lock = request.app.state.source_download_lock

    orchestrator = SourceDownloadOrchestrator(
        job_id=job_id,
        store=store,
        rerun_failed_only=payload.rerun_failed_only,
        use_llm=settings.use_llm,
        llm_backend=settings.llm_backend,
        research_purpose=settings.research_purpose,
        searxng_base_url=settings.searxng_base_url,
        fetch_delay=settings.fetch_delay,
        run_download=run_download,
        run_convert=run_convert,
        run_catalog=run_catalog,
        run_citation_verify=run_citation_verify,
        run_llm_cleanup=run_llm_cleanup,
        run_llm_title=run_llm_title,
        run_llm_summary=run_llm_summary,
        run_llm_rating=run_llm_rating,
        force_redownload=payload.force_redownload,
        force_convert=payload.force_convert,
        force_catalog=payload.force_catalog or payload.force_title,
        force_citation_verify=payload.force_citation_verify,
        force_llm_cleanup=payload.force_llm_cleanup,
        force_title=payload.force_title,
        force_summary=payload.force_summary,
        force_rating=payload.force_rating,
        project_profile_name=project_profile_name,
        project_profile_yaml=project_profile_yaml,
        output_options=SourceOutputOptions(
            include_raw_file=payload.include_raw_file,
            include_rendered_html=payload.include_rendered_html,
            include_rendered_pdf=payload.include_rendered_pdf,
            include_markdown=payload.include_markdown,
        ),
        selected_phases=_selected_phase_names(
            run_download=run_download,
            run_convert=run_convert,
            run_catalog=run_catalog,
            run_llm_rating=run_llm_rating,
            run_llm_summary=run_llm_summary,
        ),
    )

    with jobs_lock:
        if job_id in jobs:
            raise HTTPException(status_code=409, detail="Source download is already running")
        jobs[job_id] = orchestrator

    def _run_and_cleanup() -> None:
        try:
            orchestrator.run()
            # Auto-merge into attached repository if available
            try:
                repo_service = request.app.state.repository_service
                if repo_service.is_attached:
                    result = repo_service.merge_job_results(job_id)
                    if result.get("merged"):
                        logger.info(
                            "Auto-merged job %s into repository: %d new, %d updated, %d skipped",
                            job_id,
                            result.get("new_sources", 0),
                            result.get("updated_sources", 0),
                            result.get("skipped", 0),
                        )
            except Exception:
                logger.warning(
                    "Auto-merge into repository failed for job %s", job_id, exc_info=True
                )
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
        "run_download": run_download,
        "run_convert": run_convert,
        "run_catalog": run_catalog,
        "run_citation_verify": run_citation_verify,
        "run_llm_cleanup": run_llm_cleanup,
        "run_llm_title": run_llm_title,
        "run_llm_summary": run_llm_summary,
        "run_llm_rating": run_llm_rating,
    }


@router.post("/sources/{job_id}/cancel")
async def cancel_source_download(job_id: str, request: Request) -> dict:
    store = _job_store(request, job_id)
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    jobs = request.app.state.source_download_jobs
    jobs_lock = request.app.state.source_download_lock
    with jobs_lock:
        orchestrator = jobs.get(job_id)
    if orchestrator is not None:
        orchestrator.request_cancel()
        message = (
            getattr(getattr(orchestrator, "status", None), "message", "")
            or "Stop requested. Finishing the current item before stopping."
        )
        repo_service = getattr(request.app.state, "repository_service", None)
        if (
            repo_service is not None
            and getattr(orchestrator, "writes_to_repository", False)
            and hasattr(repo_service, "mark_source_tasks_cancelling")
        ):
            repo_service.mark_source_tasks_cancelling(message)
        return {"job_id": job_id, "status": "cancelling", "message": message}

    status = store.get_source_status(job_id) or {}
    if status.get("state") == "cancelling":
        return {
            "job_id": job_id,
            "status": "cancelling",
            "message": status.get("message") or "Stop requested.",
        }
    if status.get("state") != "running":
        return {"job_id": job_id, "status": "not_running"}

    return {"job_id": job_id, "status": "running_no_handle"}


@router.get("/sources/{job_id}/status")
async def get_source_download_status(job_id: str, request: Request) -> dict:
    store = _job_store(request, job_id)
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    status = store.get_source_status(job_id)
    if status is None:
        jobs = request.app.state.source_download_jobs
        jobs_lock = request.app.state.source_download_lock
        with jobs_lock:
            orchestrator = jobs.get(job_id)
        if orchestrator is not None:
            if orchestrator.status is not None:
                return orchestrator.status.model_dump(mode="json")

            bibliography = store.load_artifact(job_id, "03_bibliography") or {}
            if getattr(orchestrator, "target_rows", None):
                pending_total = len(getattr(orchestrator, "target_rows", []))
            else:
                pending_total = len(
                    [entry for entry in bibliography.get("entries", []) if isinstance(entry, dict)]
                )
            pending = SourceDownloadStatus(
                job_id=job_id,
                state="cancelling" if getattr(orchestrator, "cancel_requested", False) else "running",
                total_urls=pending_total,
                processed_urls=0,
                cancel_requested=bool(getattr(orchestrator, "cancel_requested", False)),
                cancel_requested_at=None,
                stop_after_current_item=False,
                message=(
                    "Stop requested | stopping before the next item"
                    if getattr(orchestrator, "cancel_requested", False)
                    else "Preparing source task run..."
                ),
                run_download=orchestrator.run_download,
                run_convert=bool(getattr(orchestrator, "run_convert", False)),
                run_catalog=bool(getattr(orchestrator, "run_catalog", False)),
                run_llm_cleanup=orchestrator.run_llm_cleanup,
                run_llm_title=bool(getattr(orchestrator, "run_llm_title", False)),
                run_llm_summary=orchestrator.run_llm_summary,
                run_llm_rating=orchestrator.run_llm_rating,
                force_redownload=orchestrator.force_redownload,
                force_convert=bool(getattr(orchestrator, "force_convert", False)),
                force_catalog=bool(getattr(orchestrator, "force_catalog", False)),
                force_llm_cleanup=orchestrator.force_llm_cleanup,
                force_title=bool(getattr(orchestrator, "force_title", False)),
                force_summary=orchestrator.force_summary,
                force_rating=orchestrator.force_rating,
                output_dir=str(getattr(orchestrator, "status_output_dir", "output_run")),
                manifest_csv=str(getattr(orchestrator, "status_manifest_csv", "output_run/manifest.csv")),
                manifest_xlsx=str(getattr(orchestrator, "status_manifest_xlsx", "output_run/manifest.xlsx")),
                bundle_file=str(getattr(orchestrator, "status_bundle_file", "output_run.zip")),
                writes_to_repository=bool(getattr(orchestrator, "writes_to_repository", False)),
                repository_path=str(getattr(orchestrator, "repository_path", "")),
                selected_scope=str(getattr(orchestrator, "selected_scope", "")),
                selected_import_id=str(getattr(orchestrator, "selected_import_id", "")),
                selected_phases=list(
                    getattr(orchestrator, "selected_phases", [])
                    or _selected_phase_names(
                        run_download=bool(getattr(orchestrator, "run_download", False)),
                        run_convert=bool(getattr(orchestrator, "run_convert", False)),
                        run_catalog=bool(getattr(orchestrator, "run_catalog", False)),
                        run_llm_rating=bool(getattr(orchestrator, "run_llm_rating", False)),
                        run_llm_summary=bool(getattr(orchestrator, "run_llm_summary", False)),
                    )
                ),
                items=[],
            )
            return pending.model_dump(mode="json")

    if status is None:
        artifact = store.load_artifact(job_id, "06_sources_manifest")
        if artifact is None:
            raise HTTPException(status_code=404, detail="Source download status not found")

        rows: list[SourceManifestRow] = []
        for raw_row in artifact.get("rows", []):
            try:
                rows.append(SourceManifestRow.model_validate(raw_row))
            except Exception:
                continue

        output_summary = summarize_output_rows(rows)
        success_count = int(artifact.get("success_count") or 0)
        failed_count = int(artifact.get("failed_count") or 0)
        partial_count = int(artifact.get("partial_count") or 0)
        if (success_count + failed_count + partial_count) == 0 and rows:
            success_count = sum(1 for row in rows if row.fetch_status == "success")
            partial_count = sum(1 for row in rows if row.fetch_status == "partial")
            failed_count = len(rows) - success_count - partial_count
        item_rows = rows[:200]
        items = [
            SourceItemStatus(
                id=row.id,
                original_url=row.original_url,
                citation_number=row.citation_number,
                source_kind=row.source_kind,
                status="completed" if row.fetch_status != "failed" else "failed",
                fetch_status=row.fetch_status,
                catalog_status=row.catalog_status,
                title_status=row.title_status,
                llm_cleanup_status=row.llm_cleanup_status,
                summary_status=row.summary_status,
                rating_status=row.rating_status,
                error_message=row.error_message,
            )
            for row in item_rows
        ]

        synthesized = SourceDownloadStatus(
            job_id=job_id,
            state="completed",
            total_urls=len(rows),
            processed_urls=len(rows),
            success_count=success_count,
            failed_count=failed_count,
            partial_count=partial_count,
            message="Loaded existing output run",
            output_dir="output_run",
            manifest_csv="output_run/manifest.csv",
            manifest_xlsx="output_run/manifest.xlsx",
            bundle_file="output_run.zip",
            output_summary=output_summary,
            items=items,
        )
        return synthesized.model_dump(mode="json")
    return status


@router.get("/sources/{job_id}/manifest/csv")
async def download_source_manifest_csv(job_id: str, request: Request):
    store = _job_store(request, job_id)
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
    store = _job_store(request, job_id)
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
    store = _job_store(request, job_id)
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
