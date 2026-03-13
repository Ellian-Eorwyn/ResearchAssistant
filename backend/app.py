"""FastAPI application factory."""

from __future__ import annotations

import threading
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.models.settings import AppSettings
from backend.routers import pipeline, repository, results, settings, sources, upload
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


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
    configured = AppSettings(**raw_settings) if raw_settings else AppSettings()
    if configured.repository_path.strip():
        try:
            repository_service.attach(configured.repository_path)
        except Exception:
            # Keep app boot resilient if the configured path is unavailable.
            pass

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

    # Serve frontend
    frontend_dir = Path(__file__).parent.parent / "frontend"
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

    @app.get("/")
    async def serve_index():
        return FileResponse(str(frontend_dir / "index.html"))

    return app
