"""Upload router: accepts file uploads and creates jobs."""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, File, Request, UploadFile

from backend.models.common import FileInfo, UploadResponse

router = APIRouter()

ALLOWED_EXTENSIONS = {".pdf", ".docx", ".md"}


def _next_unique_filename(filename: str, used_filenames: set[str]) -> str:
    """Return a filesystem-safe, non-colliding filename for this job."""
    base_name = Path(filename).name or "upload"
    stem = Path(base_name).stem or "upload"
    suffix = Path(base_name).suffix

    candidate = base_name
    counter = 2
    while candidate.lower() in used_filenames:
        candidate = f"{stem}_{counter}{suffix}"
        counter += 1

    used_filenames.add(candidate.lower())
    return candidate


@router.post("/upload", response_model=UploadResponse)
async def upload_files(
    request: Request,
    files: list[UploadFile] = File(...),
) -> UploadResponse:
    store = request.app.state.file_store
    job_id = store.create_job()
    file_infos: list[FileInfo] = []
    used_filenames: set[str] = set()

    for f in files:
        # Validate extension
        original_filename = (f.filename or "unknown").strip() or "unknown"
        ext = Path(original_filename).suffix.lower()

        if ext not in ALLOWED_EXTENSIONS:
            continue

        filename = _next_unique_filename(original_filename, used_filenames)
        content = await f.read()
        store.save_upload(job_id, filename, content)

        file_type = ext.lstrip(".")
        file_infos.append(
            FileInfo(
                filename=filename,
                file_type=file_type,
                size_bytes=len(content),
            )
        )

    return UploadResponse(job_id=job_id, files=file_infos)
