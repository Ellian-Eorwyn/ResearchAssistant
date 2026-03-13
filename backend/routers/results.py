"""Results router: retrieve pipeline artifacts and CSV exports."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse

router = APIRouter()

STAGE_FILES = {
    "ingestion": "01_ingestion",
    "references": "02_references",
    "bibliography": "03_bibliography",
    "citations": "04_citations",
    "export": "05_export",
    "sources": "06_sources_manifest",
}


@router.get("/results/{job_id}")
async def get_results(
    job_id: str,
    request: Request,
    stage: str | None = Query(None),
) -> dict:
    store = request.app.state.file_store
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    if stage:
        artifact_name = STAGE_FILES.get(stage)
        if not artifact_name:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown stage: {stage}. Valid: {list(STAGE_FILES.keys())}",
            )
        data = store.load_artifact(job_id, artifact_name)
        if data is None:
            raise HTTPException(
                status_code=404, detail=f"Artifact for stage '{stage}' not found"
            )
        return data

    # Return summary of all available artifacts
    available: dict[str, dict] = {}
    for stage_name, file_name in STAGE_FILES.items():
        data = store.load_artifact(job_id, file_name)
        if data is not None:
            # Return lightweight summary, not full data
            summary: dict = {"available": True}
            if stage_name == "ingestion":
                docs = data.get("documents", [])
                summary["document_count"] = len(docs)
                summary["filenames"] = [d.get("filename", "") for d in docs]
            elif stage_name == "bibliography":
                entries = data.get("entries", [])
                summary["entry_count"] = len(entries)
                summary["parse_failures"] = data.get("parse_failures", 0)
            elif stage_name == "citations":
                summary["citation_count"] = len(data.get("citations", []))
                summary["sentence_count"] = len(data.get("sentences", []))
                summary["match_count"] = len(data.get("matches", []))
            elif stage_name == "export":
                summary["row_count"] = len(data.get("rows", []))
                summary["matched"] = data.get("matched_count", 0)
                summary["unmatched"] = data.get("unmatched_count", 0)
            elif stage_name == "sources":
                rows = data.get("rows", [])
                summary["row_count"] = len(rows)
                summary["success_count"] = data.get("success_count", 0)
                summary["failed_count"] = data.get("failed_count", 0)
                summary["partial_count"] = data.get("partial_count", 0)
            available[stage_name] = summary
        else:
            available[stage_name] = {"available": False}

    return {"job_id": job_id, "artifacts": available}


@router.get("/export/{job_id}/csv")
async def export_csv(job_id: str, request: Request):
    store = request.app.state.file_store
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    csv_path = store.get_export_path(job_id)
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="CSV export not found")

    return FileResponse(
        path=str(csv_path),
        media_type="text/csv",
        filename="citations.csv",
    )
