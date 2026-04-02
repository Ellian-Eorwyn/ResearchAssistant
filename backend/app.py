"""FastAPI application factory."""

from __future__ import annotations

import json
import logging
import threading
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.models.settings import LLMBackendConfig, RepoSettings
from backend.routers import pipeline, repository, results, settings, sources, upload
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore

logger = logging.getLogger(__name__)


def _migrate_old_settings(store: FileStore, raw: dict) -> None:
    """Migrate old-format settings (with llm_backend) to new split format.
    """
    repo_path = (raw.get("repository_path") or "").strip()

    # Build RepoSettings from old fields and write into the repo if it exists
    if repo_path:
        resolved = Path(repo_path).expanduser().resolve()
        if resolved.is_dir():
            internal = resolved / ".ra_repo"
            internal.mkdir(parents=True, exist_ok=True)

            # Write repo settings
            llm_raw = raw.get("llm_backend", {})
            try:
                llm_config = LLMBackendConfig(**llm_raw)
            except Exception:
                llm_config = LLMBackendConfig()

            repo_settings = RepoSettings(
                llm_backend=llm_config,
                use_llm=raw.get("use_llm", False),
                research_purpose=raw.get("research_purpose", ""),
                fetch_delay=raw.get("fetch_delay", 2.0),
            )
            settings_path = internal / "settings.json"
            if not settings_path.exists():
                settings_path.write_text(
                    json.dumps(repo_settings.model_dump(mode="json"), ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                logger.info("Migrated per-repo settings to %s", settings_path)

            # Copy project profiles from data/project_profiles/ to repo/project_profiles/
            repo_profiles = resolved / "project_profiles"
            repo_profiles.mkdir(parents=True, exist_ok=True)
            before = {path.name for path in repo_profiles.glob("*") if path.is_file()}
            store.sync_project_profiles_to(repo_profiles)
            after = {path.name for path in repo_profiles.glob("*") if path.is_file()}
            for copied_name in sorted(after - before):
                logger.info("Copied profile %s to repo", copied_name)

    store.delete_settings()
    logger.info("Removed legacy app settings after repo-scoped migration")


def create_app() -> FastAPI:
    app = FastAPI(
        title="ResearchAssistant",
        version="0.1.0",
        description="Local-first citation extraction pipeline",
    )

    # Initialize file store
    data_dir = Path(__file__).parent.parent / "data"
    store = FileStore(base_dir=data_dir)
    app.state.file_store = store
    repository_service = AttachedRepositoryService(store=store)
    app.state.repository_service = repository_service
    app.state.source_download_jobs = {}
    app.state.source_download_lock = threading.Lock()

    raw_settings = store.load_settings()
    if raw_settings and "llm_backend" in raw_settings:
        _migrate_old_settings(store, raw_settings)
    elif raw_settings:
        store.delete_settings()
        logger.info("Removed persisted app settings; repository paths are no longer stored")

    # Register API routers
    app.include_router(upload.router, prefix="/api", tags=["upload"])
    app.include_router(pipeline.router, prefix="/api", tags=["pipeline"])
    app.include_router(results.router, prefix="/api", tags=["results"])
    app.include_router(sources.router, prefix="/api", tags=["sources"])
    app.include_router(settings.router, prefix="/api", tags=["settings"])
    app.include_router(repository.router, prefix="/api", tags=["repository"])

    # Health check
    @app.get("/api/health")
    async def health():
        return {"status": "ok"}

    # Serve frontend (Vite dist when present; source index fallback otherwise)
    frontend_source_dir = Path(__file__).parent.parent / "frontend"
    frontend_dist_dir = frontend_source_dir / "dist"
    frontend_root = (
        frontend_dist_dir
        if (frontend_dist_dir / "index.html").is_file()
        else frontend_source_dir
    )
    frontend_index = frontend_root / "index.html"

    assets_dir = frontend_root / "assets"
    if assets_dir.is_dir():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    # Keep legacy static path available for backwards compatibility.
    if frontend_source_dir.is_dir():
        app.mount("/static", StaticFiles(directory=str(frontend_source_dir)), name="static")

    @app.get("/", include_in_schema=False)
    async def serve_index():
        if not frontend_index.is_file():
            raise HTTPException(
                status_code=503,
                detail="Frontend build not found. Run `npm run build` in `frontend/`.",
            )
        return FileResponse(str(frontend_index))

    @app.get("/{full_path:path}", include_in_schema=False)
    async def spa_fallback(full_path: str):
        if full_path.startswith("api/"):
            raise HTTPException(status_code=404, detail="Not found")
        if not frontend_index.is_file():
            raise HTTPException(
                status_code=503,
                detail="Frontend build not found. Run `npm run build` in `frontend/`.",
            )

        candidate = (frontend_root / full_path).resolve()
        root_resolved = frontend_root.resolve()
        if (
            candidate.is_file()
            and (candidate == root_resolved or root_resolved in candidate.parents)
        ):
            return FileResponse(str(candidate))
        return FileResponse(str(frontend_index))

    return app
