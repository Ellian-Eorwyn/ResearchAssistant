"""Repository router: attach local repository folders and expand source datasets."""

from __future__ import annotations

import os
import platform
import subprocess

from fastapi import APIRouter, Body, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, Response

from backend.models.ingestion_profiles import (
    IngestionProfile,
    IngestionProfileActionResponse,
    IngestionProfileListResponse,
    IngestionProfileSuggestionActionResponse,
    IngestionProfileSuggestionListResponse,
)
from backend.models.repository import (
    AttachRepositoryRequest,
    CreateRepositoryRequest,
    RepositoryCitationRisExportRequest,
    RepositoryActionResponse,
    RepositoryBundleExportRequest,
    RepositoryColumnConfig,
    RepositoryColumnCreateRequest,
    RepositoryColumnPromptFixRequest,
    RepositoryColumnPromptFixResponse,
    RepositoryColumnRunRequest,
    RepositoryColumnRunStartResponse,
    RepositoryColumnRunStatus,
    RepositoryColumnUpdateRequest,
    RepositoryDocumentImportListResponse,
    RepositoryDuplicateCandidateResponse,
    RepositoryManifestExportRequest,
    RepositorySourceBulkRisReadyRequest,
    RepositorySourceBulkRisReadyResponse,
    RepositorySourceDeleteRequest,
    RepositorySourceDeleteResponse,
    RepositorySourceExportRequest,
    RepositorySourceExportResponse,
    RepositorySourcePatchRequest,
    RepositoryExportJobRequest,
    RepositoryExportJobResponse,
    RepositoryImportResponse,
    RepositoryMergeRequest,
    RepositoryMergeResponse,
    RepositoryProcessDocumentsResponse,
    RepositoryReprocessDocumentsRequest,
    RepositoryReprocessDocumentsResponse,
    RepositorySourceTaskRequest,
    RepositorySourceTaskResponse,
    RepositoryStatusResponse,
)
from backend.models.settings import RepoSettings

router = APIRouter()


def _pick_directory_dialog(mode: str, initial_path: str = "") -> str:
    """Open a native folder picker and return the selected absolute path."""
    if mode == "open":
        title = "Select an existing ResearchAssistant repository"
    elif mode == "create":
        title = "Select a folder for the new ResearchAssistant repository"
    else:
        title = "Select an export destination folder"
    normalized_initial = (initial_path or "").strip()
    initialdir = (
        normalized_initial
        if normalized_initial and os.path.isdir(normalized_initial)
        else os.path.expanduser("~")
    )

    # Use the OS-native dialog on macOS (no Tk root window side effects).
    if platform.system().lower() == "darwin":
        return _pick_directory_dialog_macos(title=title, initialdir=initialdir)

    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as exc:  # pragma: no cover - environment-specific
        raise RuntimeError("Native folder picker is unavailable in this runtime.") from exc

    root = tk.Tk()
    # Keep helper root hidden; the native chooser is shown as a child dialog.
    root.withdraw()
    try:
        root.attributes("-topmost", True)
    except Exception:
        pass
    root.update()
    try:
        selected = filedialog.askdirectory(
            parent=root,
            title=title,
            initialdir=initialdir,
            mustexist=(mode in {"open", "export"}),
        )
    finally:
        try:
            root.attributes("-topmost", False)
        except Exception:
            pass
        root.destroy()
    return str(selected or "").strip()


def _pick_directory_dialog_macos(title: str, initialdir: str) -> str:
    """Use AppleScript choose-folder dialog so the system file browser is used."""
    escaped_title = (
        str(title or "")
        .replace("\\", "\\\\")
        .replace('"', '\\"')
    )
    escaped_initialdir = (
        str(initialdir or os.path.expanduser("~"))
        .replace("\\", "\\\\")
        .replace('"', '\\"')
    )
    script_lines = [
        'tell application "Finder" to activate',
        f'set defaultFolder to POSIX file "{escaped_initialdir}"',
        f'set chosenFolder to choose folder with prompt "{escaped_title}" default location defaultFolder',
        "return POSIX path of chosenFolder",
    ]
    try:
        result = subprocess.run(
            ["osascript", *sum([["-e", line] for line in script_lines], [])],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception as exc:  # pragma: no cover - environment-specific
        raise RuntimeError("Native folder picker is unavailable in this runtime.") from exc

    if result.returncode != 0:
        combined = f"{result.stderr}\n{result.stdout}".strip().lower()
        if "user canceled" in combined or "cancel" in combined:
            return ""
        raise RuntimeError("Native folder picker failed.")

    return str(result.stdout or "").strip()


# ---- Browse directories (web-based picker) ----

@router.get("/repository/browse-directory")
async def browse_directory(
    path: str = Query(""),
    show_hidden: bool = Query(False),
) -> dict:
    """List subdirectories at *path* for the web-based folder browser."""
    from pathlib import Path as _Path

    target = (path or "").strip()
    if not target:
        target = os.path.expanduser("~")
    resolved = _Path(target).resolve()

    if not resolved.is_dir():
        return {
            "current_path": str(resolved),
            "parent_path": str(resolved.parent),
            "entries": [],
            "error": f"Not a directory: {resolved}",
        }

    entries: list[dict] = []
    try:
        for item in sorted(resolved.iterdir(), key=lambda p: p.name.lower()):
            if not item.is_dir():
                continue
            if not show_hidden and item.name.startswith("."):
                continue
            entries.append({
                "name": item.name,
                "path": str(item),
                "is_ra_repo": (item / ".ra_repo").is_dir(),
            })
    except PermissionError:
        return {
            "current_path": str(resolved),
            "parent_path": str(resolved.parent),
            "entries": [],
            "error": f"Permission denied: {resolved}",
        }

    # Sort: .ra_repo directories first, then alphabetical
    entries.sort(key=lambda e: (not e["is_ra_repo"], e["name"].lower()))

    return {
        "current_path": str(resolved),
        "parent_path": str(resolved.parent) if resolved.parent != resolved else "",
        "entries": entries,
        "error": "",
    }


# ---- Create / Open ----

@router.post("/repository/create", response_model=RepositoryStatusResponse)
async def create_repository(
    request: Request,
    payload: CreateRepositoryRequest,
) -> RepositoryStatusResponse:
    service = request.app.state.repository_service
    try:
        status = service.create(payload.path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return status


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
    return status


# ---- Per-repo settings ----

@router.get("/repository/settings")
async def get_repo_settings(request: Request) -> dict:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    return service.load_repo_settings().model_dump(mode="json")


@router.put("/repository/settings")
async def save_repo_settings(
    request: Request,
    payload: dict = Body(...),
) -> dict:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    current = service.load_repo_settings()
    merged = current.model_dump(mode="json")
    merged.update(payload or {})
    settings = RepoSettings(**merged)
    service.save_repo_settings(settings)
    return settings.model_dump(mode="json")


@router.get("/repository/ingestion-profiles", response_model=IngestionProfileListResponse)
async def list_ingestion_profiles(request: Request) -> IngestionProfileListResponse:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    return service.list_ingestion_profiles()


@router.post("/repository/ingestion-profiles", response_model=IngestionProfileActionResponse)
async def create_ingestion_profile(
    request: Request,
    payload: IngestionProfile,
) -> IngestionProfileActionResponse:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    try:
        return service.save_ingestion_profile(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.put("/repository/ingestion-profiles/{profile_id}", response_model=IngestionProfileActionResponse)
async def update_ingestion_profile(
    profile_id: str,
    request: Request,
    payload: IngestionProfile,
) -> IngestionProfileActionResponse:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    try:
        updated = payload.model_copy(update={"profile_id": profile_id, "built_in": False})
        return service.save_ingestion_profile(updated)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.delete("/repository/ingestion-profiles/{profile_id}", response_model=IngestionProfileActionResponse)
async def delete_ingestion_profile(
    profile_id: str,
    request: Request,
) -> IngestionProfileActionResponse:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    try:
        return service.delete_ingestion_profile(profile_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get(
    "/repository/ingestion-profile-suggestions",
    response_model=IngestionProfileSuggestionListResponse,
)
async def list_ingestion_profile_suggestions(
    request: Request,
) -> IngestionProfileSuggestionListResponse:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    return service.list_ingestion_profile_suggestions()


@router.get(
    "/repository/document-imports",
    response_model=RepositoryDocumentImportListResponse,
)
async def list_repository_document_imports(
    request: Request,
) -> RepositoryDocumentImportListResponse:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    return service.list_document_imports()


@router.post(
    "/repository/ingestion-profile-suggestions/{suggestion_id}/accept",
    response_model=IngestionProfileSuggestionActionResponse,
)
async def accept_ingestion_profile_suggestion(
    suggestion_id: str,
    request: Request,
) -> IngestionProfileSuggestionActionResponse:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    try:
        return service.accept_ingestion_profile_suggestion(suggestion_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post(
    "/repository/ingestion-profile-suggestions/{suggestion_id}/reject",
    response_model=IngestionProfileSuggestionActionResponse,
)
async def reject_ingestion_profile_suggestion(
    suggestion_id: str,
    request: Request,
) -> IngestionProfileSuggestionActionResponse:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    try:
        return service.reject_ingestion_profile_suggestion(suggestion_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


# ---- Status ----

@router.get("/repository/status", response_model=RepositoryStatusResponse)
async def get_repository_status(request: Request) -> RepositoryStatusResponse:
    service = request.app.state.repository_service
    return service.get_status()


@router.get("/repository/pick-directory")
async def pick_repository_directory(
    mode: str = Query(default="open"),
    initial_path: str = Query(default=""),
) -> dict:
    normalized_mode = (mode or "").strip().lower()
    if normalized_mode not in {"open", "create", "export"}:
        raise HTTPException(
            status_code=400,
            detail="Invalid mode. Use `open`, `create`, or `export`.",
        )
    try:
        selected = _pick_directory_dialog(normalized_mode, initial_path=initial_path)
    except RuntimeError as exc:  # pragma: no cover - environment-specific
        raise HTTPException(status_code=501, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - environment-specific
        raise HTTPException(status_code=500, detail=f"Folder picker failed: {exc}") from exc
    return {"path": selected}


@router.get("/repository/dashboard")
async def get_repository_dashboard(request: Request) -> dict:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    try:
        return service.get_dashboard()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/repository/manifest")
async def get_repository_manifest(
    request: Request,
    q: str = "",
    fetch_status: str = "",
    detected_type: str = "",
    source_kind: str = "",
    document_type: str = "",
    organization_type: str = "",
    organization_name: str = "",
    author_names: str = "",
    publication_date: str = "",
    tags_text: str = "",
    has_summary: bool | None = Query(default=None),
    has_rating: bool | None = Query(default=None),
    rating_overall_min: float | None = Query(default=None),
    rating_overall_max: float | None = Query(default=None),
    rating_overall_relevance_min: float | None = Query(default=None),
    rating_overall_relevance_max: float | None = Query(default=None),
    rating_depth_score_min: float | None = Query(default=None),
    rating_depth_score_max: float | None = Query(default=None),
    rating_relevant_detail_score_min: float | None = Query(default=None),
    rating_relevant_detail_score_max: float | None = Query(default=None),
    citation_type: str = "",
    citation_doi: str = "",
    citation_report_number: str = "",
    citation_standard_number: str = "",
    citation_missing_fields: str = "",
    citation_ready: bool | None = Query(default=None),
    citation_confidence_min: float | None = Query(default=None),
    citation_confidence_max: float | None = Query(default=None),
    sort_by: str = "",
    sort_dir: str = "",
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    try:
        return service.list_manifest(
            q=q,
            fetch_status=fetch_status,
            detected_type=detected_type,
            source_kind=source_kind,
            document_type=document_type,
            organization_type=organization_type,
            organization_name=organization_name,
            author_names=author_names,
            publication_date=publication_date,
            tags_text=tags_text,
            has_summary=has_summary,
            has_rating=has_rating,
            rating_overall_min=rating_overall_min,
            rating_overall_max=rating_overall_max,
            rating_overall_relevance_min=rating_overall_relevance_min,
            rating_overall_relevance_max=rating_overall_relevance_max,
            rating_depth_score_min=rating_depth_score_min,
            rating_depth_score_max=rating_depth_score_max,
            rating_relevant_detail_score_min=rating_relevant_detail_score_min,
            rating_relevant_detail_score_max=rating_relevant_detail_score_max,
            citation_type=citation_type,
            citation_doi=citation_doi,
            citation_report_number=citation_report_number,
            citation_standard_number=citation_standard_number,
            citation_missing_fields=citation_missing_fields,
            citation_ready=citation_ready,
            citation_confidence_min=citation_confidence_min,
            citation_confidence_max=citation_confidence_max,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=limit,
            offset=offset,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/repository/citation-data")
async def get_repository_citation_data(request: Request) -> dict:
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="No repository attached")
    try:
        return service.get_citation_data()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ---- Import ----

@router.post("/repository/ingest/seed-files", response_model=RepositoryImportResponse)
async def ingest_repository_seed_files(
    request: Request,
    files: list[UploadFile] = File(...),
) -> RepositoryImportResponse:
    service = request.app.state.repository_service
    prepared_files = [
        (upload.filename or "seed_upload", await upload.read())
        for upload in files
    ]
    try:
        return service.import_seed_files(prepared_files)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/repository/ingest/documents", response_model=RepositoryImportResponse)
async def ingest_repository_documents(
    request: Request,
    files: list[UploadFile] = File(...),
) -> RepositoryImportResponse:
    service = request.app.state.repository_service
    prepared_files = [
        (upload.filename or "document", await upload.read())
        for upload in files
    ]
    try:
        return service.import_manual_documents(prepared_files)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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


@router.post("/repository/process-documents", response_model=RepositoryProcessDocumentsResponse)
async def process_repository_documents(
    request: Request,
    files: list[UploadFile] = File(...),
    profile_override: str = Form(default=""),
) -> RepositoryProcessDocumentsResponse:
    service = request.app.state.repository_service
    settings = service.load_effective_settings()

    prepared_files: list[tuple[str, bytes]] = []
    for file in files:
        prepared_files.append((file.filename or "document.md", await file.read()))

    try:
        return service.process_documents(
            prepared_files,
            settings=settings,
            profile_override=profile_override,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/repository/reprocess-documents",
    response_model=RepositoryReprocessDocumentsResponse,
)
async def reprocess_repository_documents(
    request: Request,
    payload: RepositoryReprocessDocumentsRequest,
) -> RepositoryReprocessDocumentsResponse:
    service = request.app.state.repository_service
    settings = service.load_effective_settings()
    try:
        return service.reprocess_documents(
            target_import_ids=payload.target_import_ids,
            settings=settings,
            profile_override=payload.profile_override,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ---- Download / Rebuild ----

@router.post("/repository/download", response_model=RepositoryActionResponse)
async def start_repository_download(request: Request) -> RepositoryActionResponse:
    service = request.app.state.repository_service
    settings = service.load_effective_settings()
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


@router.post("/repository/clear-citations", response_model=RepositoryActionResponse)
async def clear_repository_citations(request: Request) -> RepositoryActionResponse:
    service = request.app.state.repository_service
    try:
        return service.clear_citations()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/repository/cleanup", response_model=RepositoryActionResponse)
async def cleanup_repository_layout(request: Request) -> RepositoryActionResponse:
    service = request.app.state.repository_service
    try:
        return service.cleanup_repository_layout()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/repository/source-tasks", response_model=RepositorySourceTaskResponse)
async def run_repository_source_tasks(
    request: Request,
    payload: RepositorySourceTaskRequest,
) -> RepositorySourceTaskResponse:
    service = request.app.state.repository_service
    settings = service.load_effective_settings()
    jobs = request.app.state.source_download_jobs
    jobs_lock = request.app.state.source_download_lock
    try:
        return service.start_source_tasks(
            payload=payload,
            settings=settings,
            live_jobs=jobs,
            live_jobs_lock=jobs_lock,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/repository/sources/{source_id}/files/{kind}")
async def open_repository_source_file(
    source_id: str,
    kind: str,
    request: Request,
):
    service = request.app.state.repository_service
    try:
        path, media_type, headers = service.resolve_source_file(source_id=source_id, kind=kind)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() or "no file" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
    return FileResponse(
        path=str(path),
        media_type=media_type,
        headers=headers,
    )


@router.patch("/repository/sources/{source_id}")
async def patch_repository_source(
    source_id: str,
    request: Request,
    payload: RepositorySourcePatchRequest,
) -> dict:
    service = request.app.state.repository_service
    try:
        return service.update_source(
            source_id,
            patch=payload.model_dump(exclude_unset=True),
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "unknown source_id" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/repository/columns", response_model=RepositoryColumnConfig)
async def create_repository_column(
    request: Request,
    payload: RepositoryColumnCreateRequest,
) -> RepositoryColumnConfig:
    service = request.app.state.repository_service
    try:
        return service.create_column(payload.label)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.patch("/repository/columns/{column_id}", response_model=RepositoryColumnConfig)
async def update_repository_column(
    column_id: str,
    request: Request,
    payload: RepositoryColumnUpdateRequest,
) -> RepositoryColumnConfig:
    service = request.app.state.repository_service
    try:
        return service.update_column(
            column_id,
            patch=payload.model_dump(exclude_unset=True),
        )
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "unknown" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post(
    "/repository/columns/{column_id}/fix-prompt",
    response_model=RepositoryColumnPromptFixResponse,
)
async def fix_repository_column_prompt(
    column_id: str,
    request: Request,
    payload: RepositoryColumnPromptFixRequest,
) -> RepositoryColumnPromptFixResponse:
    service = request.app.state.repository_service
    try:
        return service.fix_column_prompt(column_id, draft_prompt=payload.draft_prompt)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/repository/columns/{column_id}/run",
    response_model=RepositoryColumnRunStartResponse,
)
async def start_repository_column_run(
    column_id: str,
    request: Request,
    payload: RepositoryColumnRunRequest,
) -> RepositoryColumnRunStartResponse:
    service = request.app.state.repository_service
    try:
        return service.start_column_run(column_id, payload=payload)
    except ValueError as exc:
        detail = str(exc)
        if "already running" in detail.lower():
            raise HTTPException(status_code=409, detail=detail) from exc
        raise HTTPException(status_code=400, detail=detail) from exc


@router.get(
    "/repository/column-runs/{job_id}",
    response_model=RepositoryColumnRunStatus,
)
async def get_repository_column_run_status(
    job_id: str,
    request: Request,
) -> RepositoryColumnRunStatus:
    service = request.app.state.repository_service
    try:
        return service.get_column_run_status(job_id)
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "not found" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post(
    "/repository/sources/duplicate-candidates",
    response_model=RepositoryDuplicateCandidateResponse,
)
async def scan_repository_source_duplicates(
    request: Request,
) -> RepositoryDuplicateCandidateResponse:
    service = request.app.state.repository_service
    try:
        return service.find_duplicate_source_candidates()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/repository/sources/bulk-mark-ris-ready",
    response_model=RepositorySourceBulkRisReadyResponse,
)
async def bulk_mark_repository_sources_ris_ready(
    request: Request,
    payload: RepositorySourceBulkRisReadyRequest,
) -> RepositorySourceBulkRisReadyResponse:
    service = request.app.state.repository_service
    try:
        return service.bulk_mark_sources_ris_ready(payload.source_ids)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/repository/sources/bulk-delete",
    response_model=RepositorySourceDeleteResponse,
)
async def bulk_delete_repository_sources(
    request: Request,
    payload: RepositorySourceDeleteRequest,
) -> RepositorySourceDeleteResponse:
    service = request.app.state.repository_service
    try:
        return service.delete_sources(payload.source_ids)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post(
    "/repository/sources/export-files",
    response_model=RepositorySourceExportResponse,
)
async def export_repository_source_files(
    request: Request,
    payload: RepositorySourceExportRequest,
) -> RepositorySourceExportResponse:
    service = request.app.state.repository_service
    try:
        return service.export_source_files(
            source_ids=payload.source_ids,
            file_kinds=payload.file_kinds,
            destination_path=payload.destination_path,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/repository/citations/export-ris")
async def export_repository_citations_ris(
    request: Request,
    payload: RepositoryCitationRisExportRequest,
) -> Response:
    service = request.app.state.repository_service
    try:
        content, headers = service.export_citations_ris(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return Response(
        content=content,
        media_type="application/x-research-info-systems; charset=utf-8",
        headers=headers,
    )


@router.post("/repository/export-bundle")
async def export_repository_bundle(
    request: Request,
    payload: RepositoryBundleExportRequest,
) -> Response:
    service = request.app.state.repository_service
    try:
        content, headers = service.export_repository_bundle(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return Response(
        content=content,
        media_type="application/zip",
        headers=headers,
    )


@router.post("/repository/manifest/export")
async def export_repository_manifest(
    request: Request,
    payload: RepositoryManifestExportRequest,
) -> Response:
    service = request.app.state.repository_service
    try:
        content, headers, media_type = service.export_manifest(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return Response(
        content=content,
        media_type=media_type,
        headers=headers,
    )


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


# ---- Exports ----

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


# ---- Merge ----

@router.post("/repository/merge", response_model=RepositoryMergeResponse)
async def merge_repositories(
    request: Request,
    payload: RepositoryMergeRequest,
) -> RepositoryMergeResponse:
    service = request.app.state.repository_service
    if not payload.source_paths:
        raise HTTPException(status_code=400, detail="At least one source path is required")
    try:
        return service.start_merge(source_paths=payload.source_paths)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ---- SQLite ----

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
