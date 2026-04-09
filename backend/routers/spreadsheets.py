"""Router for the spreadsheet workspace."""

from __future__ import annotations

from fastapi import APIRouter, File, HTTPException, Query, Request, Response, UploadFile

from backend.models.spreadsheets import (
    SpreadsheetColumnConfig,
    SpreadsheetColumnCreateRequest,
    SpreadsheetColumnPromptFixRequest,
    SpreadsheetColumnPromptFixResponse,
    SpreadsheetColumnRunRequest,
    SpreadsheetColumnRunStartResponse,
    SpreadsheetColumnRunStatus,
    SpreadsheetColumnUpdateRequest,
    SpreadsheetExportRequest,
    SpreadsheetManifestResponse,
    SpreadsheetRowPatchRequest,
    SpreadsheetSessionResponse,
    SpreadsheetSessionTargetSelectRequest,
    SpreadsheetSessionUploadResponse,
    SpreadsheetWorkspaceStatusResponse,
)

router = APIRouter()


@router.get("/spreadsheets/status", response_model=SpreadsheetWorkspaceStatusResponse)
async def get_spreadsheet_workspace_status(request: Request) -> SpreadsheetWorkspaceStatusResponse:
    service = request.app.state.spreadsheet_service
    try:
        return service.get_status()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/spreadsheets/upload", response_model=SpreadsheetSessionUploadResponse)
async def upload_spreadsheet_session(
    request: Request,
    file: UploadFile = File(...),
) -> SpreadsheetSessionUploadResponse:
    service = request.app.state.spreadsheet_service
    try:
        content = await file.read()
        return service.upload_session(file.filename or "spreadsheet", content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/spreadsheets/sessions/{session_id}", response_model=SpreadsheetSessionResponse)
async def get_spreadsheet_session(session_id: str, request: Request) -> SpreadsheetSessionResponse:
    service = request.app.state.spreadsheet_service
    try:
        return service.get_session(session_id)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/spreadsheets/sessions/{session_id}/activate-target", response_model=SpreadsheetSessionResponse)
async def activate_spreadsheet_target(
    session_id: str,
    request: Request,
    payload: SpreadsheetSessionTargetSelectRequest,
) -> SpreadsheetSessionResponse:
    service = request.app.state.spreadsheet_service
    try:
        return service.select_target(session_id, payload)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() or "unknown" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.get("/spreadsheets/sessions/{session_id}/manifest", response_model=SpreadsheetManifestResponse)
async def get_spreadsheet_manifest(
    session_id: str,
    request: Request,
    q: str = "",
    sort_by: str = "",
    sort_dir: str = "",
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> SpreadsheetManifestResponse:
    service = request.app.state.spreadsheet_service
    try:
        return service.list_manifest(
            session_id,
            q=q,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() or "unknown" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.patch("/spreadsheets/sessions/{session_id}/rows/{row_id}")
async def patch_spreadsheet_row(
    session_id: str,
    row_id: str,
    request: Request,
    payload: SpreadsheetRowPatchRequest,
) -> dict:
    service = request.app.state.spreadsheet_service
    try:
        return service.patch_row(session_id, row_id, payload)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() or "unknown" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/spreadsheets/sessions/{session_id}/columns", response_model=SpreadsheetColumnConfig)
async def create_spreadsheet_column(
    session_id: str,
    request: Request,
    payload: SpreadsheetColumnCreateRequest,
) -> SpreadsheetColumnConfig:
    service = request.app.state.spreadsheet_service
    try:
        return service.create_column(session_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/spreadsheets/sessions/{session_id}/columns/{column_id}", response_model=SpreadsheetColumnConfig)
async def update_spreadsheet_column(
    session_id: str,
    column_id: str,
    request: Request,
    payload: SpreadsheetColumnUpdateRequest,
) -> SpreadsheetColumnConfig:
    service = request.app.state.spreadsheet_service
    try:
        return service.update_column(session_id, column_id, payload)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() or "unknown" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post(
    "/spreadsheets/sessions/{session_id}/columns/{column_id}/fix-prompt",
    response_model=SpreadsheetColumnPromptFixResponse,
)
async def fix_spreadsheet_column_prompt(
    session_id: str,
    column_id: str,
    request: Request,
    payload: SpreadsheetColumnPromptFixRequest,
) -> SpreadsheetColumnPromptFixResponse:
    service = request.app.state.spreadsheet_service
    try:
        return service.fix_column_prompt(session_id, column_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/spreadsheets/sessions/{session_id}/columns/{column_id}/run",
    response_model=SpreadsheetColumnRunStartResponse,
)
async def start_spreadsheet_column_run(
    session_id: str,
    column_id: str,
    request: Request,
    payload: SpreadsheetColumnRunRequest,
) -> SpreadsheetColumnRunStartResponse:
    service = request.app.state.spreadsheet_service
    try:
        return service.start_column_run(session_id, column_id, payload)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() or "unknown" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.get(
    "/spreadsheets/sessions/{session_id}/column-runs/{job_id}",
    response_model=SpreadsheetColumnRunStatus,
)
async def get_spreadsheet_column_run_status(
    session_id: str,
    job_id: str,
    request: Request,
) -> SpreadsheetColumnRunStatus:
    service = request.app.state.spreadsheet_service
    try:
        return service.get_column_run_status(session_id, job_id)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/spreadsheets/export")
async def export_spreadsheet_session(
    request: Request,
    payload: SpreadsheetExportRequest,
) -> Response:
    service = request.app.state.spreadsheet_service
    try:
        content, headers, media_type = service.export_session(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return Response(content=content, media_type=media_type, headers=headers)
