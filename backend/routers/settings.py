"""Settings router: manage app-level settings and LLM model listing."""

from __future__ import annotations

from fastapi import APIRouter, Body, Query, Request

from backend.llm.client import UnifiedLLMClient
from backend.models.settings import AppSettings, LLMBackendConfig, ModelsResponse

router = APIRouter()


@router.get("/settings", response_model=AppSettings)
async def get_settings(request: Request) -> AppSettings:
    service = request.app.state.repository_service
    if service.is_attached:
        return AppSettings(last_repository_path=str(service.path))
    return AppSettings()


@router.put("/settings", response_model=AppSettings)
async def save_settings(
    request: Request,
    payload: dict = Body(...),
) -> AppSettings:
    service = request.app.state.repository_service
    if service.is_attached:
        return AppSettings(last_repository_path=str(service.path))
    return AppSettings(**(payload or {}))


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
