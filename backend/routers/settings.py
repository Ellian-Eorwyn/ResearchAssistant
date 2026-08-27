"""Settings router: manage app-level settings and LLM model listing."""

from __future__ import annotations

from fastapi import APIRouter, Body, Query, Request

from backend.llm.client import UnifiedLLMClient
from backend.models.settings import (
    AppSettings,
    LLMBackendConfig,
    ModelsResponse,
    resolve_effective_backend,
)

router = APIRouter()


@router.get("/settings", response_model=AppSettings)
async def get_settings(request: Request) -> AppSettings:
    store = request.app.state.file_store
    service = request.app.state.repository_service
    settings = store.load_app_settings()
    if service.is_attached:
        settings.last_repository_path = str(service.path)
    return settings


@router.put("/settings", response_model=AppSettings)
async def save_settings(
    request: Request,
    payload: dict = Body(...),
) -> AppSettings:
    store = request.app.state.file_store
    service = request.app.state.repository_service
    current = store.load_app_settings()
    # Overlay the incoming keys on the current settings and re-validate, so
    # nested payload fields (backend_profiles, llm_backend) are coerced from raw
    # dicts into models. A shallow model_copy(update=...) would leave them as
    # dicts and break resolve_effective_backend below.
    merged = AppSettings.model_validate({**current.model_dump(mode="json"), **payload})
    # Keep `llm_backend` in sync with the selected profile (and synthesize the
    # Default profile if the client sent none).
    merged = resolve_effective_backend(merged)
    if service.is_attached:
        merged.last_repository_path = str(service.path)
    store.save_app_settings(merged)
    return merged


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
