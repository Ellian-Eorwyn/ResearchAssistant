"""Results router: retrieve pipeline artifacts and CSV exports."""

from __future__ import annotations

import os
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import FileResponse

from backend.models.export import ExportArtifact
from backend.models.sources import SourceManifestRow
from backend.pipeline.stage_export_sqlite import build_wikiclaude_sqlite_db
from backend.taxonomies.presets import BUILTIN_PRESETS, get_taxonomy_config

router = APIRouter()

STAGE_FILES = {
    "ingestion": "01_ingestion",
    "references": "02_references",
    "bibliography": "03_bibliography",
    "citations": "04_citations",
    "export": "05_export",
    "sources": "06_sources_manifest",
}


def _resolve_taxonomy_config_path(value: str | None) -> Path:
    if value and value.strip():
        explicit = Path(value.strip()).expanduser()
        if explicit.exists() and explicit.is_file():
            return explicit
        raise HTTPException(
            status_code=400,
            detail=f"Taxonomy config not found: {explicit}",
        )

    env_value = (os.getenv("WIKICLAUDE_TAXONOMY_PATH") or "").strip()
    candidates = []
    if env_value:
        candidates.append(Path(env_value).expanduser())
    candidates.extend(
        [
            Path.home() / "Obsidian" / "Dev" / "Wiki-Claude" / "config" / "domains.yaml",
            Path(__file__).resolve().parents[2].parent / "Wiki-Claude" / "config" / "domains.yaml",
        ]
    )
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            return candidate

    raise HTTPException(
        status_code=400,
        detail=(
            "Taxonomy config not found. Provide `taxonomy_config_path` query param or set "
            "`WIKICLAUDE_TAXONOMY_PATH`."
        ),
    )


def _resolve_output_file(output_dir: Path, relative_path: str) -> Path | None:
    rel = (relative_path or "").strip()
    if not rel:
        return None
    candidate = (output_dir / rel).resolve()
    base = output_dir.resolve()
    if candidate != base and base not in candidate.parents:
        return None
    return candidate


def _load_markdown_by_source_id(store, job_id: str, source_rows: list[SourceManifestRow]) -> dict[str, str]:
    output_dir = store.get_sources_output_dir(job_id)
    markdown_by_source_id: dict[str, str] = {}
    for src in source_rows:
        src_id = (src.repository_source_id or src.id or "").strip()
        if not src_id or src_id in markdown_by_source_id:
            continue
        for field_name in ("llm_cleanup_file", "markdown_file"):
            rel_path = getattr(src, field_name, "") or ""
            full_path = _resolve_output_file(output_dir, rel_path)
            if not full_path or not full_path.is_file():
                continue
            try:
                markdown_by_source_id[src_id] = full_path.read_text(
                    encoding="utf-8",
                    errors="replace",
                )
                break
            except OSError:
                continue
    return markdown_by_source_id


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


@router.get("/export/{job_id}/sqlite")
async def export_sqlite(job_id: str, request: Request):
    store = request.app.state.file_store
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    export_data = store.load_artifact(job_id, "05_export")
    if export_data is None:
        raise HTTPException(status_code=404, detail="Export artifact not found")

    try:
        export_artifact = ExportArtifact.model_validate(export_data)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Invalid export artifact: {exc}") from exc

    source_data = store.load_artifact(job_id, "06_sources_manifest") or {}
    source_rows: list[SourceManifestRow] = []
    for raw in source_data.get("rows", []):
        try:
            source_rows.append(SourceManifestRow.model_validate(raw))
        except Exception:
            continue
    markdown_by_source_id = _load_markdown_by_source_id(store, job_id, source_rows)

    sqlite_path = store.get_export_dir(job_id) / "wikiclaude_export.db"
    build_wikiclaude_sqlite_db(
        db_path=sqlite_path,
        export_rows=export_artifact.rows,
        source_rows=source_rows,
        markdown_by_source_id=markdown_by_source_id or None,
    )

    return FileResponse(
        path=str(sqlite_path),
        media_type="application/x-sqlite3",
        filename="wikiclaude_export.db",
    )


@router.get("/taxonomy-presets")
async def list_taxonomy_presets() -> list[dict]:
    """Return the list of available taxonomy presets for the frontend dropdown."""
    return [
        {"key": "none", "name": "None (default classification)"},
        *[
            {"key": k, "name": v["name"], "description": v.get("description", "")}
            for k, v in BUILTIN_PRESETS.items()
        ],
        {"key": "custom", "name": "Custom file path..."},
    ]


@router.get("/export/{job_id}/sqlite-taxonomy")
async def export_sqlite_with_taxonomy(
    job_id: str,
    request: Request,
    taxonomy_preset: str | None = Query(default=None),
    taxonomy_config_path: str | None = Query(default=None),
):
    store = request.app.state.file_store
    if not store.job_exists(job_id):
        raise HTTPException(status_code=404, detail="Job not found")

    export_data = store.load_artifact(job_id, "05_export")
    if export_data is None:
        raise HTTPException(status_code=404, detail="Export artifact not found")

    try:
        export_artifact = ExportArtifact.model_validate(export_data)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Invalid export artifact: {exc}") from exc

    source_data = store.load_artifact(job_id, "06_sources_manifest") or {}
    source_rows: list[SourceManifestRow] = []
    for raw in source_data.get("rows", []):
        try:
            source_rows.append(SourceManifestRow.model_validate(raw))
        except Exception:
            continue
    markdown_by_source_id = _load_markdown_by_source_id(store, job_id, source_rows)

    try:
        config = get_taxonomy_config(taxonomy_preset, taxonomy_config_path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    sqlite_path = store.get_export_dir(job_id) / "wikiclaude_export_taxonomy.db"
    try:
        build_wikiclaude_sqlite_db(
            db_path=sqlite_path,
            export_rows=export_artifact.rows,
            source_rows=source_rows,
            taxonomy_config=config,
            markdown_by_source_id=markdown_by_source_id or None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return FileResponse(
        path=str(sqlite_path),
        media_type="application/x-sqlite3",
        filename="wikiclaude_export_taxonomy.db",
    )
