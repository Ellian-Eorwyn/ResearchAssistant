"""Repository router: attach local repository folders and expand source datasets."""

from __future__ import annotations

from fastapi import APIRouter, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse

from backend.models.repository import (
    AttachRepositoryRequest,
    RepositoryActionResponse,
    RepositoryImportResponse,
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
