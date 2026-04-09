"""Agent-facing REST and MCP surface."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from typing import Any
from urllib.parse import urlencode

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse

from backend.models.agent import AgentRunSourcePhasesRequest
from backend.models.repository import RepositorySourceTaskRequest
from backend.storage.attached_repository import _utc_now_iso

router = APIRouter()


def _request_id(request: Request) -> str:
    return str(request.headers.get("x-request-id") or "").strip() or uuid.uuid4().hex[:12]


def _response_envelope(
    *,
    request_id: str,
    status: str,
    data: Any,
    links: dict[str, str] | None = None,
    error: dict[str, Any] | None = None,
    http_status: int = 200,
    headers: dict[str, str] | None = None,
) -> JSONResponse:
    payload: dict[str, Any] = {
        "status": status,
        "request_id": request_id,
        "data": data,
    }
    if error:
        payload["error"] = error
    if links:
        payload["links"] = links
    return JSONResponse(payload, status_code=http_status, headers=headers or {})


def _error_response(
    *,
    request_id: str,
    code: str,
    message: str,
    http_status: int,
    retryable: bool = False,
    data: Any = None,
    links: dict[str, str] | None = None,
    auth_required: bool = False,
) -> JSONResponse:
    headers = {"WWW-Authenticate": "Bearer"} if auth_required else None
    return _response_envelope(
        request_id=request_id,
        status="error",
        data=data,
        error={
            "code": code,
            "message": message,
            "retryable": retryable,
        },
        links=links,
        http_status=http_status,
        headers=headers,
    )


def _base_api_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


def _run_url(request: Request, run_id: str) -> str:
    return f"{_base_api_url(request)}/api/agent/v1/runs/{run_id}"


def _source_url(request: Request, source_id: str) -> str:
    return f"{_base_api_url(request)}/api/agent/v1/sources/{source_id}"


def _resource_url(request: Request, resource_id: str) -> str:
    return f"{_base_api_url(request)}/api/agent/v1/resources/{resource_id}"


def _sanitize_error_code(message: str, *, fallback: str = "agent_error") -> str:
    text = str(message or "").strip()
    if not text:
        return fallback
    lower = text.lower()
    explicit = {
        "no repository attached": "repository_not_attached",
        "attach a repository before": "repository_not_attached",
        "unknown run_id": "unknown_run",
        "unknown source_id": "unknown_source",
        "unknown resource_id": "unknown_resource",
        "invalid cursor": "invalid_cursor",
        "idempotency key already exists": "idempotency_conflict",
    }
    for needle, code in explicit.items():
        if needle in lower:
            return code
    code = re.sub(r"[^a-z0-9_]+", "_", text.split(":", 1)[0].strip().lower()).strip("_")
    return code or fallback


def _configured_agent_tokens(service) -> tuple[str, str]:
    stored = service.load_agent_tokens()
    read_token = (
        os.getenv("RESEARCHASSISTANT_AGENT_READ_TOKEN")
        or os.getenv("RA_AGENT_READ_TOKEN")
        or stored["read_token"]
    )
    write_token = (
        os.getenv("RESEARCHASSISTANT_AGENT_WRITE_TOKEN")
        or os.getenv("RA_AGENT_WRITE_TOKEN")
        or stored["write_token"]
    )
    return str(read_token).strip(), str(write_token).strip()


def _allowlisted_client_hosts() -> set[str]:
    configured = (
        os.getenv("RESEARCHASSISTANT_AGENT_ALLOWLIST")
        or os.getenv("RA_AGENT_ALLOWLIST")
        or ""
    )
    defaults = {"127.0.0.1", "::1", "localhost", "testclient"}
    if not configured.strip():
        return defaults
    configured_hosts = {
        item.strip()
        for item in configured.split(",")
        if item.strip()
    }
    return defaults | configured_hosts


def _authorize(request: Request, *, access: str, request_id: str) -> JSONResponse | None:
    service = request.app.state.repository_service
    if not service.is_attached:
        return _error_response(
            request_id=request_id,
            code="repository_not_attached",
            message="Attach a repository before using the agent API.",
            http_status=400,
        )

    client_host = str(getattr(request.client, "host", "") or "").strip().lower()
    if client_host and client_host not in _allowlisted_client_hosts():
        return _error_response(
            request_id=request_id,
            code="agent_client_not_allowed",
            message=f"Client host `{client_host}` is not in the agent allowlist.",
            http_status=403,
        )

    auth_header = str(request.headers.get("authorization") or "").strip()
    if not auth_header.lower().startswith("bearer "):
        return _error_response(
            request_id=request_id,
            code="agent_auth_required",
            message="Bearer token required.",
            http_status=401,
            auth_required=True,
        )
    token = auth_header.split(" ", 1)[1].strip()
    read_token, write_token = _configured_agent_tokens(service)

    if access == "write":
        allowed = {write_token}
    else:
        allowed = {read_token, write_token}
    if token not in allowed:
        return _error_response(
            request_id=request_id,
            code="agent_auth_invalid",
            message="Bearer token is invalid for this operation.",
            http_status=403,
        )
    return None


def _normalize_requested_phases(values: list[str]) -> list[str]:
    allowed = {"fetch", "convert", "tag", "summarize"}
    phases: list[str] = []
    seen: set[str] = set()
    for item in values:
        phase = str(item or "").strip().lower()
        if phase not in allowed or phase in seen:
            continue
        seen.add(phase)
        phases.append(phase)
    return phases


def _agent_request_fingerprint(payload: AgentRunSourcePhasesRequest) -> str:
    normalized = payload.model_dump(mode="json")
    normalized.pop("idempotency_key", None)
    normalized["phases"] = sorted(_normalize_requested_phases(payload.phases))
    normalized["source_ids"] = sorted(
        str(item).strip() for item in payload.source_ids if str(item).strip()
    )
    encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _to_repository_task_request(payload: AgentRunSourcePhasesRequest) -> RepositorySourceTaskRequest:
    phases = _normalize_requested_phases(payload.phases)
    if not phases:
        raise ValueError("At least one phase is required.")
    run_download = "fetch" in phases
    run_convert = "convert" in phases
    run_tag = "tag" in phases
    run_summarize = "summarize" in phases
    force = bool(payload.force)
    return RepositorySourceTaskRequest(
        scope=payload.scope or "queued",
        import_id=payload.import_id,
        source_ids=[str(item).strip() for item in payload.source_ids if str(item).strip()],
        limit=payload.limit,
        selected_phases=phases,
        rerun_failed_only=False,
        run_download=run_download,
        run_convert=run_convert,
        run_llm_cleanup=False,
        run_llm_title=False,
        run_llm_summary=run_summarize,
        run_llm_rating=run_tag,
        force_redownload=force and run_download,
        force_convert=force and run_convert,
        force_llm_cleanup=False,
        force_title=False,
        force_summary=force and run_summarize,
        force_rating=force and run_tag,
        project_profile_name=payload.project_profile_name,
        include_raw_file=True,
        include_rendered_html=True,
        include_rendered_pdf=True,
        include_markdown=run_convert,
    )


def _append_mutation_audit(
    request: Request,
    *,
    action: str,
    run_id: str,
    payload: dict[str, Any],
    request_id: str,
) -> None:
    service = request.app.state.repository_service
    service.append_agent_audit_record(
        {
            "timestamp": _utc_now_iso(),
            "request_id": request_id,
            "action": action,
            "run_id": run_id,
            "client_host": str(getattr(request.client, "host", "") or ""),
            "payload": payload,
        }
    )


def _build_next_link(request: Request, next_cursor: str) -> str:
    if not next_cursor:
        return ""
    return str(request.url.include_query_params(cursor=next_cursor))


def _rest_list_resources(service, *, q: str = "", kind: str = "") -> list[dict[str, Any]]:
    resources = service.list_agent_resources()
    q_norm = str(q or "").strip().lower()
    kind_norm = str(kind or "").strip().lower()
    items: list[dict[str, Any]] = []
    for resource in resources:
        if kind_norm and resource.kind.lower() != kind_norm:
            continue
        if q_norm:
            haystack = " ".join(
                [
                    resource.kind,
                    resource.path,
                    resource.title,
                    resource.short_description,
                    " ".join(resource.tags),
                ]
            ).lower()
            if q_norm not in haystack:
                continue
        items.append(resource.model_dump(mode="json"))
    return items


@router.post("/agent/v1/runs/source-phases")
async def run_agent_source_phases(
    request: Request,
    payload: AgentRunSourcePhasesRequest = Body(...),
):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="write", request_id=request_id)
    if auth_error is not None:
        return auth_error

    service = request.app.state.repository_service
    settings = service.load_effective_settings()
    try:
        fingerprint = _agent_request_fingerprint(payload)
        existing_run_id = service.resolve_agent_idempotency(payload.idempotency_key, fingerprint)
        if existing_run_id:
            run_record = service.get_agent_run(
                existing_run_id,
                live_jobs=request.app.state.source_download_jobs,
                live_jobs_lock=request.app.state.source_download_lock,
            )
            return _response_envelope(
                request_id=request_id,
                status="accepted",
                data=run_record.model_dump(mode="json"),
                links={
                    "self": str(request.url),
                    "run": _run_url(request, run_record.run_id),
                },
                http_status=202,
            )

        task_payload = _to_repository_task_request(payload)
        response = service.start_source_tasks(
            payload=task_payload,
            settings=settings,
            live_jobs=request.app.state.source_download_jobs,
            live_jobs_lock=request.app.state.source_download_lock,
        )
        service.remember_agent_idempotency(payload.idempotency_key, fingerprint, response.job_id)
        run_record = service.get_agent_run(
            response.job_id,
            live_jobs=request.app.state.source_download_jobs,
            live_jobs_lock=request.app.state.source_download_lock,
        )
        _append_mutation_audit(
            request,
            action="run_source_phases",
            run_id=response.job_id,
            request_id=request_id,
            payload=payload.model_dump(mode="json"),
        )
        return _response_envelope(
            request_id=request_id,
            status="accepted",
            data=run_record.model_dump(mode="json"),
            links={
                "self": str(request.url),
                "run": _run_url(request, response.job_id),
            },
            http_status=202,
        )
    except ValueError as exc:
        return _error_response(
            request_id=request_id,
            code=_sanitize_error_code(str(exc)),
            message=str(exc),
            http_status=409 if "idempotency" in str(exc).lower() else 400,
        )
    except RuntimeError as exc:
        return _error_response(
            request_id=request_id,
            code=_sanitize_error_code(str(exc), fallback="run_conflict"),
            message=str(exc),
            http_status=409,
        )


@router.get("/agent/v1/runs/{run_id}")
async def get_agent_run_status(run_id: str, request: Request):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error

    service = request.app.state.repository_service
    try:
        run_record = service.get_agent_run(
            run_id,
            live_jobs=request.app.state.source_download_jobs,
            live_jobs_lock=request.app.state.source_download_lock,
        )
        return _response_envelope(
            request_id=request_id,
            status="ok",
            data=run_record.model_dump(mode="json"),
            links={"self": str(request.url)},
        )
    except ValueError as exc:
        return _error_response(
            request_id=request_id,
            code=_sanitize_error_code(str(exc), fallback="unknown_run"),
            message=str(exc),
            http_status=404 if "unknown run" in str(exc).lower() else 400,
        )


@router.post("/agent/v1/runs/{run_id}/cancel")
async def cancel_agent_run(run_id: str, request: Request):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="write", request_id=request_id)
    if auth_error is not None:
        return auth_error

    service = request.app.state.repository_service
    try:
        run_record = service.cancel_agent_run(
            run_id,
            live_jobs=request.app.state.source_download_jobs,
            live_jobs_lock=request.app.state.source_download_lock,
        )
        _append_mutation_audit(
            request,
            action="cancel_run",
            run_id=run_id,
            request_id=request_id,
            payload={"run_id": run_id},
        )
        return _response_envelope(
            request_id=request_id,
            status="accepted",
            data=run_record.model_dump(mode="json"),
            links={
                "self": str(request.url),
                "run": _run_url(request, run_id),
            },
            http_status=202,
        )
    except ValueError as exc:
        return _error_response(
            request_id=request_id,
            code=_sanitize_error_code(str(exc), fallback="unknown_run"),
            message=str(exc),
            http_status=404 if "unknown run" in str(exc).lower() else 400,
        )
    except RuntimeError as exc:
        return _error_response(
            request_id=request_id,
            code=_sanitize_error_code(str(exc), fallback="run_not_cancellable"),
            message=str(exc),
            http_status=409,
        )


@router.get("/agent/v1/sources")
async def list_agent_sources(
    request: Request,
    q: str = Query(default=""),
    status: str = Query(default=""),
    fetch_status: str = Query(default=""),
    convert_status: str = Query(default=""),
    tag_status: str = Query(default=""),
    summarize_status: str = Query(default=""),
    import_id: str = Query(default=""),
    has_summary: bool | None = Query(default=None),
    has_rating: bool | None = Query(default=None),
    min_relevance: float | None = Query(default=None),
    sort_by: str = Query(default="rating_overall"),
    sort_dir: str = Query(default="desc"),
    limit: int = Query(default=50, ge=1, le=500),
    cursor: str = Query(default=""),
):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error

    service = request.app.state.repository_service
    try:
        result = service.list_agent_sources(
            q=q,
            status=status,
            fetch_status=fetch_status,
            convert_status=convert_status,
            tag_status=tag_status,
            summarize_status=summarize_status,
            import_id=import_id,
            has_summary=has_summary,
            has_rating=has_rating,
            min_relevance=min_relevance,
            sort_by=sort_by,
            sort_dir=sort_dir,
            limit=limit,
            cursor=cursor,
        )
        links = {"self": str(request.url)}
        next_link = _build_next_link(request, result.get("next_cursor", ""))
        if next_link:
            links["next"] = next_link
        return _response_envelope(
            request_id=request_id,
            status="ok",
            data=result,
            links=links,
        )
    except ValueError as exc:
        return _error_response(
            request_id=request_id,
            code=_sanitize_error_code(str(exc), fallback="invalid_query"),
            message=str(exc),
            http_status=400,
        )


@router.get("/agent/v1/sources/{source_id}")
async def get_agent_source(source_id: str, request: Request):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error

    service = request.app.state.repository_service
    try:
        source = service.get_agent_source(source_id)
        return _response_envelope(
            request_id=request_id,
            status="ok",
            data=source.model_dump(mode="json"),
            links={
                "self": str(request.url),
                "resource": source.artifact_uris.metadata,
            },
        )
    except ValueError as exc:
        return _error_response(
            request_id=request_id,
            code=_sanitize_error_code(str(exc), fallback="unknown_source"),
            message=str(exc),
            http_status=404 if "unknown source" in str(exc).lower() else 400,
        )


@router.get("/agent/v1/sources/{source_id}/content")
async def get_agent_source_content(
    source_id: str,
    request: Request,
    kind: str = Query(...),
    cursor: str = Query(default=""),
    chunk_size: int = Query(default=8000, ge=1, le=50000),
    include_offsets: bool = Query(default=False),
):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error

    service = request.app.state.repository_service
    try:
        chunk = service.get_agent_source_content(
            source_id,
            kind=kind,
            cursor=cursor,
            chunk_size=chunk_size,
        )
        data = chunk.model_dump(mode="json")
        if not include_offsets:
            data.pop("offset_start", None)
            data.pop("offset_end", None)
        links = {"self": str(request.url)}
        next_link = _build_next_link(request, chunk.next_cursor)
        if next_link:
            links["next"] = next_link
        links["resource"] = chunk.artifact_uri
        return _response_envelope(
            request_id=request_id,
            status="ok",
            data=data,
            links=links,
        )
    except ValueError as exc:
        message = str(exc)
        return _error_response(
            request_id=request_id,
            code=_sanitize_error_code(message, fallback="content_unavailable"),
            message=message,
            http_status=404 if "unknown source" in message.lower() or "no `" in message.lower() else 400,
        )


@router.get("/agent/v1/resources")
async def list_agent_resources(
    request: Request,
    q: str = Query(default=""),
    kind: str = Query(default=""),
):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error

    service = request.app.state.repository_service
    try:
        items = _rest_list_resources(service, q=q, kind=kind)
        return _response_envelope(
            request_id=request_id,
            status="ok",
            data={"items": items, "total": len(items)},
            links={"self": str(request.url)},
        )
    except ValueError as exc:
        return _error_response(
            request_id=request_id,
            code=_sanitize_error_code(str(exc), fallback="invalid_resource_query"),
            message=str(exc),
            http_status=400,
        )


@router.get("/agent/v1/resources/{resource_id}")
async def get_agent_resource(resource_id: str, request: Request):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error

    service = request.app.state.repository_service
    try:
        resource = service.get_agent_resource(resource_id)
        return _response_envelope(
            request_id=request_id,
            status="ok",
            data=resource.model_dump(mode="json"),
            links={
                "self": str(request.url),
                "resource": _resource_url(request, resource_id),
            },
        )
    except ValueError as exc:
        return _error_response(
            request_id=request_id,
            code=_sanitize_error_code(str(exc), fallback="unknown_resource"),
            message=str(exc),
            http_status=404 if "unknown resource" in str(exc).lower() else 400,
        )


def _mcp_tool_definitions() -> list[dict[str, Any]]:
    return [
        {
            "name": "run_source_phases",
            "description": "Start fetch, convert, tag, and summarize phases for repository sources.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "scope": {"type": "string"},
                    "import_id": {"type": "string"},
                    "source_ids": {"type": "array", "items": {"type": "string"}},
                    "phases": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["fetch", "convert", "tag", "summarize"],
                        },
                    },
                    "project_profile_name": {"type": "string"},
                    "force": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                    "idempotency_key": {"type": "string"},
                },
                "required": ["phases"],
            },
        },
        {
            "name": "get_run_status",
            "description": "Read normalized status for an agent source-phase run.",
            "inputSchema": {
                "type": "object",
                "properties": {"run_id": {"type": "string"}},
                "required": ["run_id"],
            },
        },
        {
            "name": "cancel_run",
            "description": "Request cooperative cancellation for a running source-phase run.",
            "inputSchema": {
                "type": "object",
                "properties": {"run_id": {"type": "string"}},
                "required": ["run_id"],
            },
        },
        {
            "name": "search_sources",
            "description": "Query repository sources and rank likely-relevant items.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "q": {"type": "string"},
                    "status": {"type": "string"},
                    "fetch_status": {"type": "string"},
                    "convert_status": {"type": "string"},
                    "tag_status": {"type": "string"},
                    "summarize_status": {"type": "string"},
                    "import_id": {"type": "string"},
                    "has_summary": {"type": "boolean"},
                    "has_rating": {"type": "boolean"},
                    "min_relevance": {"type": "number"},
                    "sort_by": {"type": "string"},
                    "sort_dir": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                    "cursor": {"type": "string"},
                },
            },
        },
        {
            "name": "get_source",
            "description": "Return metadata, freshness, and artifact URIs for one repository source.",
            "inputSchema": {
                "type": "object",
                "properties": {"source_id": {"type": "string"}},
                "required": ["source_id"],
            },
        },
    ]


def _mcp_prompts() -> list[dict[str, Any]]:
    return [
        {
            "name": "find-top-relevant-sources",
            "description": "Find the highest-relevance sources for a question using repository ratings and summaries.",
            "arguments": [
                {"name": "question", "required": True},
                {"name": "limit", "required": False},
                {"name": "min_relevance", "required": False},
            ],
        },
        {
            "name": "review-source-pack",
            "description": "Review a specific source pack and note the strongest findings, limits, and gaps.",
            "arguments": [
                {"name": "question", "required": True},
                {"name": "source_ids", "required": True},
            ],
        },
        {
            "name": "synthesize-insights-for-question",
            "description": "Synthesize source-backed insights for an orchestrator model without re-downloading content.",
            "arguments": [
                {"name": "question", "required": True},
                {"name": "source_ids", "required": True},
            ],
        },
    ]


def _mcp_resources(service) -> list[dict[str, Any]]:
    resources: list[dict[str, Any]] = []
    for resource in service.list_agent_resources():
        resources.append(
            {
                "uri": f"repo://{resource.kind}/{resource.resource_id}",
                "name": resource.title,
                "description": resource.short_description,
                "mimeType": resource.mime_type,
            }
        )
    source_items = service.list_agent_sources(limit=500, sort_by="source_id", sort_dir="asc")
    for source in source_items.get("items", []):
        source_id = str(source.get("source_id") or "").strip()
        if not source_id:
            continue
        if source.get("artifact_uris", {}).get("markdown"):
            resources.append(
                {
                    "uri": f"repo://sources/{source_id}/markdown",
                    "name": f"Source {source_id} markdown",
                    "mimeType": "text/markdown",
                }
            )
        if source.get("artifact_uris", {}).get("summary"):
            resources.append(
                {
                    "uri": f"repo://sources/{source_id}/summary",
                    "name": f"Source {source_id} summary",
                    "mimeType": "text/markdown",
                }
            )
        if source.get("artifact_uris", {}).get("rating"):
            resources.append(
                {
                    "uri": f"repo://sources/{source_id}/rating",
                    "name": f"Source {source_id} rating",
                    "mimeType": "application/json",
                }
            )
    return resources


def _mcp_prompt_messages(name: str, arguments: dict[str, Any]) -> list[dict[str, Any]]:
    question = str(arguments.get("question") or "").strip()
    source_ids = arguments.get("source_ids") or []
    if isinstance(source_ids, str):
        source_ids = [item.strip() for item in source_ids.split(",") if item.strip()]
    limit = int(arguments.get("limit") or 5)
    min_relevance = arguments.get("min_relevance")

    if name == "find-top-relevant-sources":
        text = (
            f"Question: {question}\n"
            f"Call `search_sources` sorted by `rating_overall` descending with limit {limit}."
        )
        if min_relevance not in {"", None}:
            text += f" Use `min_relevance={min_relevance}`."
        text += " Read source summaries first, then markdown for the strongest candidates."
    elif name == "review-source-pack":
        text = (
            f"Question: {question}\n"
            f"Review these source ids: {', '.join(source_ids)}.\n"
            "Read each summary and rating first. Open markdown only when summary detail is insufficient."
        )
    else:
        text = (
            f"Question: {question}\n"
            f"Synthesize source-backed insights from: {', '.join(source_ids)}.\n"
            "Use repository summaries and ratings as the first pass, then inspect markdown for exact details."
        )

    return [{"role": "user", "content": {"type": "text", "text": text}}]


def _mcp_read_resource(service, uri: str) -> list[dict[str, Any]]:
    normalized = str(uri or "").strip()
    if normalized.startswith("repo://sources/"):
        parts = normalized[len("repo://sources/") :].split("/")
        if len(parts) != 2:
            raise ValueError("Invalid source resource URI.")
        source_id, kind = parts
        chunk = service.get_agent_source_content(
            source_id,
            kind=kind,
            cursor="",
            chunk_size=500000,
        )
        return [
            {
                "uri": normalized,
                "mimeType": chunk.mime_type,
                "text": chunk.content,
            }
        ]

    for prefix in ("repo://memory/", "repo://skill/", "repo://rubric/"):
        if normalized.startswith(prefix):
            resource_id = normalized[len(prefix) :]
            content = service.get_agent_resource(resource_id)
            return [
                {
                    "uri": normalized,
                    "mimeType": content.resource.mime_type,
                    "text": content.content,
                }
            ]
    raise ValueError("Unsupported resource URI.")


def _mcp_success(request_id: Any, result: Any) -> JSONResponse:
    return JSONResponse({"jsonrpc": "2.0", "id": request_id, "result": result})


def _mcp_error(request_id: Any, code: int, message: str, data: Any = None) -> JSONResponse:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return JSONResponse({"jsonrpc": "2.0", "id": request_id, "error": error})


def _mcp_from_rest_response(request_id: Any, response: JSONResponse) -> JSONResponse:
    payload = json.loads(response.body.decode("utf-8"))
    if response.status_code >= 400 or payload.get("status") == "error":
        return _mcp_error(
            request_id,
            -32002 if response.status_code < 500 else -32003,
            payload.get("error", {}).get("message", "Agent operation failed"),
            payload.get("error"),
        )
    data = payload.get("data")
    return _mcp_success(
        request_id,
        {
            "content": [{"type": "text", "text": json.dumps(data, ensure_ascii=False)}],
            "structuredContent": data,
        },
    )


@router.post("/agent/v1/mcp")
async def agent_mcp_endpoint(request: Request, payload: dict[str, Any] = Body(...)):
    request_id = payload.get("id")
    method = str(payload.get("method") or "").strip()
    params = payload.get("params") or {}
    if not isinstance(params, dict):
        return _mcp_error(request_id, -32602, "params must be an object")

    required_access = "read"
    if method == "tools/call":
        tool_name = str(params.get("name") or "").strip()
        if tool_name in {"run_source_phases", "cancel_run"}:
            required_access = "write"
    auth_error = _authorize(
        request,
        access=required_access,
        request_id=_request_id(request),
    )
    if auth_error is not None:
        error_payload = json.loads(auth_error.body.decode("utf-8"))
        return _mcp_error(
            request_id,
            -32001,
            error_payload.get("error", {}).get("message", "Authentication failed"),
            error_payload.get("error"),
        )

    service = request.app.state.repository_service
    try:
        if method == "initialize":
            return _mcp_success(
                request_id,
                {
                    "protocolVersion": "2025-03-26",
                    "capabilities": {
                        "tools": {},
                        "resources": {},
                        "prompts": {},
                    },
                    "serverInfo": {
                        "name": "ResearchAssistant Agent Surface",
                        "version": "0.1.0",
                    },
                },
            )
        if method == "notifications/initialized":
            return _mcp_success(request_id, {})
        if method == "ping":
            return _mcp_success(request_id, {})
        if method == "tools/list":
            return _mcp_success(request_id, {"tools": _mcp_tool_definitions()})
        if method == "resources/list":
            return _mcp_success(request_id, {"resources": _mcp_resources(service)})
        if method == "prompts/list":
            return _mcp_success(request_id, {"prompts": _mcp_prompts()})
        if method == "prompts/get":
            name = str(params.get("name") or "").strip()
            return _mcp_success(
                request_id,
                {
                    "description": name,
                    "messages": _mcp_prompt_messages(name, params.get("arguments") or {}),
                },
            )
        if method == "resources/read":
            uri = str(params.get("uri") or "").strip()
            return _mcp_success(request_id, {"contents": _mcp_read_resource(service, uri)})
        if method == "tools/call":
            tool_name = str(params.get("name") or "").strip()
            arguments = params.get("arguments") or {}
            if not isinstance(arguments, dict):
                return _mcp_error(request_id, -32602, "tool arguments must be an object")

            if tool_name == "run_source_phases":
                run_payload = AgentRunSourcePhasesRequest.model_validate(arguments)
                response = await run_agent_source_phases(request, run_payload)
                return _mcp_from_rest_response(request_id, response)
            if tool_name == "get_run_status":
                run_id = str(arguments.get("run_id") or "").strip()
                response = await get_agent_run_status(run_id, request)
                return _mcp_from_rest_response(request_id, response)
            if tool_name == "cancel_run":
                run_id = str(arguments.get("run_id") or "").strip()
                response = await cancel_agent_run(run_id, request)
                return _mcp_from_rest_response(request_id, response)
            if tool_name == "search_sources":
                result = service.list_agent_sources(**arguments)
                return _mcp_success(
                    request_id,
                    {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(result, ensure_ascii=False),
                            }
                        ],
                        "structuredContent": result,
                    },
                )
            if tool_name == "get_source":
                source = service.get_agent_source(str(arguments.get("source_id") or ""))
                payload_data = source.model_dump(mode="json")
                return _mcp_success(
                    request_id,
                    {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(payload_data, ensure_ascii=False),
                            }
                        ],
                        "structuredContent": payload_data,
                    },
                )
            return _mcp_error(request_id, -32601, f"Unknown tool: {tool_name}")

        return _mcp_error(request_id, -32601, f"Unknown method: {method}")
    except ValueError as exc:
        return _mcp_error(request_id, -32002, str(exc))
    except RuntimeError as exc:
        return _mcp_error(request_id, -32003, str(exc))
