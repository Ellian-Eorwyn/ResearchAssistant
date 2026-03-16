"""Repository router: attach local repository folders and expand source datasets."""

from __future__ import annotations

from fastapi import APIRouter, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse

from backend.models.repository import (
    AttachRepositoryRequest,
    RepositoryActionResponse,
    RepositoryExportJobRequest,
    RepositoryExportJobResponse,
    RepositoryImportResponse,
    RepositoryMergeRequest,
    RepositoryMergeResponse,
    RepositoryStatusResponse,
)
from backend.models.settings import AppSettings

router = APIRouter()


@router.post("/repository/attach", response_model=RepositoryStatusResponse)
async def attach_repository(
    request: Request,
    payload: AttachRepositoryRequest,
) -> RepositoryStatusResponse:
    service = request.app.state.repository_service
    try:
        status = service.attach(payload.path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    store = request.app.state.file_store
    raw_settings = store.load_settings()
    settings = AppSettings(**raw_settings) if raw_settings else AppSettings()
    settings.repository_path = status.path
    store.save_settings(settings.model_dump(mode="json"))
    return status


@router.get("/repository/status", response_model=RepositoryStatusResponse)
async def get_repository_status(request: Request) -> RepositoryStatusResponse:
    service = request.app.state.repository_service
    return service.get_status()


@router.post("/repository/import/source-list", response_model=RepositoryImportResponse)
async def import_repository_source_list(
    request: Request,
    file: UploadFile = File(...),
) -> RepositoryImportResponse:
    service = request.app.state.repository_service
    filename = file.filename or "sources.csv"
    content = await file.read()
    try:
        return service.import_source_list(filename=filename, content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/repository/import/document", response_model=RepositoryImportResponse)
async def import_repository_document(
    request: Request,
    file: UploadFile = File(...),
) -> RepositoryImportResponse:
    service = request.app.state.repository_service
    filename = file.filename or "document.md"
    content = await file.read()
    try:
        return service.import_document(filename=filename, content=content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/repository/download", response_model=RepositoryActionResponse)
async def start_repository_download(request: Request) -> RepositoryActionResponse:
    service = request.app.state.repository_service
    store = request.app.state.file_store
    raw_settings = store.load_settings()
    settings = AppSettings(**raw_settings) if raw_settings else AppSettings()
    try:
        return service.start_download(settings=settings)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/repository/rebuild", response_model=RepositoryActionResponse)
async def rebuild_repository_outputs(request: Request) -> RepositoryActionResponse:
    service = request.app.state.repository_service
    try:
        return service.rebuild()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/repository/export-job", response_model=RepositoryExportJobResponse)
async def create_repository_export_job(
    request: Request,
    payload: RepositoryExportJobRequest,
) -> RepositoryExportJobResponse:
    service = request.app.state.repository_service
    try:
        return service.create_export_job(scope=payload.scope, import_id=payload.import_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/repository/manifest/csv")
async def download_repository_manifest_csv(request: Request):
    service = request.app.state.repository_service
    try:
        path = service.manifest_csv_path()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not path.exists():
        raise HTTPException(status_code=404, detail="Repository manifest CSV not found")
    return FileResponse(
        path=str(path),
        media_type="text/csv",
        filename="manifest.csv",
    )


@router.get("/repository/manifest/xlsx")
async def download_repository_manifest_xlsx(request: Request):
    service = request.app.state.repository_service
    try:
        path = service.manifest_xlsx_path()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not path.exists():
        raise HTTPException(status_code=404, detail="Repository manifest XLSX not found")
    return FileResponse(
        path=str(path),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="manifest.xlsx",
    )


@router.get("/repository/citations/csv")
async def download_repository_citations_csv(request: Request):
    service = request.app.state.repository_service
    try:
        path = service.citations_csv_path()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not path.exists():
        raise HTTPException(status_code=404, detail="Repository citations CSV not found")
    return FileResponse(
        path=str(path),
        media_type="text/csv",
        filename="citations.csv",
    )


@router.post("/repository/merge", response_model=RepositoryMergeResponse)
async def merge_repositories(
    request: Request,
    payload: RepositoryMergeRequest,
) -> RepositoryMergeResponse:
    service = request.app.state.repository_service
    if not payload.primary_path.strip():
        raise HTTPException(status_code=400, detail="Primary path is required")
    if not payload.secondary_path.strip():
        raise HTTPException(status_code=400, detail="Secondary path is required")
    if payload.output_mode not in ("new", "into_primary"):
        raise HTTPException(status_code=400, detail="output_mode must be 'new' or 'into_primary'")
    if payload.output_mode == "new" and not payload.output_path.strip():
        raise HTTPException(status_code=400, detail="output_path is required when output_mode is 'new'")
    try:
        return service.start_merge(
            primary_path=payload.primary_path,
            secondary_path=payload.secondary_path,
            output_mode=payload.output_mode,
            output_path=payload.output_path,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/repository/export/sqlite")
async def export_repository_sqlite(request: Request):
    """Generate and download a SQLite database from the attached repository."""
    service = request.app.state.repository_service
    try:
        db_path = service.export_sqlite()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return FileResponse(
        path=str(db_path),
        media_type="application/x-sqlite3",
        filename="wikiclaude_export.db",
    )


@router.get("/repository/export/sqlite-taxonomy")
async def export_repository_sqlite_taxonomy(
    request: Request,
    taxonomy_preset: str | None = Query(default=None),
    taxonomy_config_path: str | None = Query(default=None),
):
    """Generate and download a SQLite database with taxonomy classification."""
    service = request.app.state.repository_service
    try:
        db_path = service.export_sqlite(
            taxonomy_preset=taxonomy_preset or "",
            taxonomy_config_path=taxonomy_config_path or "",
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return FileResponse(
        path=str(db_path),
        media_type="application/x-sqlite3",
        filename="wikiclaude_export_taxonomy.db",
    )
