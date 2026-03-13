"""Settings router: manage app settings and LLM model listing."""

from __future__ import annotations

from fastapi import APIRouter, Body, HTTPException, Query, Request

from backend.llm.client import UnifiedLLMClient
from backend.models.settings import AppSettings, LLMBackendConfig, ModelsResponse

router = APIRouter()


@router.get("/settings", response_model=AppSettings)
async def get_settings(request: Request) -> AppSettings:
    store = request.app.state.file_store
    raw = store.load_settings()
    return AppSettings(**raw) if raw else AppSettings()


@router.put("/settings", response_model=AppSettings)
async def save_settings(
    request: Request,
    payload: dict = Body(...),
) -> AppSettings:
    store = request.app.state.file_store
    current_raw = store.load_settings()
    current = AppSettings(**current_raw) if current_raw else AppSettings()
    merged = current.model_dump(mode="json")
    merged.update(payload or {})
    settings = AppSettings(**merged)
    store.save_settings(settings.model_dump())
    return settings


@router.get("/models", response_model=ModelsResponse)
async def list_models(
    backend_kind: str = Query(...),
    base_url: str = Query(...),
    api_key: str = Query(""),
) -> ModelsResponse:
    config = LLMBackendConfig(
        kind=backend_kind,
        base_url=base_url,
        api_key=api_key,
    )
    client = UnifiedLLMClient(config)
    try:
        models = await client.list_models()
        return ModelsResponse(models=models)
    except Exception as e:
        return ModelsResponse(models=[], error=str(e))
    finally:
        await client.close()
