"""Workflow endpoints: the surface the bundled `ra` CLI talks to.

Auth matches the agent API exactly, since this is the same trust boundary.
Every response carries `summary` and `next`, so a caller with no memory of the
previous step still knows what to do.
"""

from __future__ import annotations

import functools
from typing import Any

from fastapi import APIRouter, Body, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from backend.routers.agent import _authorize, _error_response, _request_id, _response_envelope
from backend.workflow import WORKFLOW_CONTRACT_VERSION
from backend.workflow.service import WorkflowService
from backend.workflow.sheet import SheetReadError

router = APIRouter()

PREFIX = "/workflow/v1"


async def _offload(func, *args, **kwargs):
    """Run a blocking service call off the event loop.

    Every `wait_seconds` path ends in `workflow.runs.wait_for`, which polls with
    `time.sleep` for up to two minutes. Called inline from an `async def` route,
    that sleeps uvicorn's one event-loop thread: the server keeps listening and
    answers nothing, so the app in the browser goes blank for the whole length
    of a fetch. `ra fetch --wait` -- which the workflow skill tells agents to
    run -- re-issues that poll back to back, so the blackout lasts as long as
    the download does.

    Use this for **any** handler that can block, not only the ones taking
    `wait_seconds`. A route that reaches disk or the network under a lock is the
    same hazard with a shorter fuse.
    """
    return await run_in_threadpool(functools.partial(func, *args, **kwargs))


def _service(request: Request) -> WorkflowService:
    return WorkflowService(request.app.state.repository_service, app_state=request.app.state)


def _fail(request_id: str, exc: Exception, *, code: str = "workflow_error", status: int = 400):
    return _error_response(
        request_id=request_id, code=code, message=str(exc), http_status=status
    )


def repository_path(service: Any) -> str:
    """The attached path, or "". `service.path` raises when nothing is attached."""
    try:
        return str(service.path) if getattr(service, "is_attached", False) else ""
    except Exception:
        return ""


@router.get(f"{PREFIX}/version")
async def workflow_version(request: Request):
    """Cheap, unauthenticated-ish handshake: also the CLI's reachability probe."""
    request_id = _request_id(request)
    service = request.app.state.repository_service
    return _response_envelope(
        request_id=request_id,
        status="ok",
        data={
            "contract_version": WORKFLOW_CONTRACT_VERSION,
            "attached": bool(getattr(service, "is_attached", False)),
            "repository_path": repository_path(service),
        },
    )


@router.get(f"{PREFIX}/preflight")
async def workflow_preflight(
    request: Request,
    refresh_capabilities: bool = Query(default=False),
):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error
    try:
        report = await _offload(
            _service(request).preflight, refresh_capabilities=refresh_capabilities
        )
    except Exception as exc:
        return _fail(request_id, exc)
    return _response_envelope(
        request_id=request_id,
        status="ok" if report.ok else "error",
        data=report.model_dump(mode="json"),
        # A failed preflight is an answer, not a transport error.
        http_status=200,
    )


@router.get(f"{PREFIX}/orientation")
async def workflow_orientation(
    request: Request,
    include_column_stats: bool = Query(default=True),
):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error
    try:
        report = await _offload(
            _service(request).orientation, include_column_stats=include_column_stats
        )
    except Exception as exc:
        return _fail(request_id, exc)
    return _response_envelope(
        request_id=request_id, status="ok", data=report.model_dump(mode="json")
    )


@router.get(f"{PREFIX}/triage")
async def workflow_triage(request: Request, phase: str = Query(default="fetch")):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error
    try:
        report = await _offload(_service(request).triage, phase=phase)
    except Exception as exc:
        return _fail(request_id, exc)
    return _response_envelope(
        request_id=request_id, status="ok", data=report.model_dump(mode="json")
    )


@router.post(f"{PREFIX}/sheet/parse")
async def workflow_parse_sheet(
    request: Request,
    payload: dict[str, Any] = Body(default_factory=dict),
):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error

    path = str(payload.get("path") or "").strip()
    if not path:
        return _error_response(
            request_id=request_id,
            code="path_required",
            message="Give the path to the spreadsheet.",
            http_status=400,
        )
    try:
        plan = await _offload(
            _service(request).parse_sheet,
            path,
            header_row=payload.get("header_row"),
            prompts_row=payload.get("prompts_row"),
            no_prompts_row=bool(payload.get("no_prompts_row")),
            repair_encoding=str(payload.get("repair_encoding") or "auto"),
            merge_duplicate_urls=bool(payload.get("merge_duplicate_urls")),
        )
    except SheetReadError as exc:
        return _fail(request_id, exc, code="sheet_unreadable")
    except Exception as exc:
        return _fail(request_id, exc)

    data = plan.model_dump(mode="json")
    data["create_sources_params"] = _service(request).sheet_to_params(plan, "create_sources")
    data["create_columns_params"] = _service(request).sheet_to_params(plan, "create_columns")
    data["set_values_params"] = _service(request).sheet_to_params(plan, "set_values")
    return _response_envelope(request_id=request_id, status="ok", data=data)


@router.post(f"{PREFIX}/operations/{{operation}}")
async def workflow_run_operation(
    operation: str,
    request: Request,
    payload: dict[str, Any] = Body(default_factory=dict),
):
    request_id = _request_id(request)
    apply = bool(payload.get("apply"))
    auth_error = _authorize(
        request, access="write" if apply else "read", request_id=request_id
    )
    if auth_error is not None:
        return auth_error

    try:
        result = await _offload(
            _service(request).run_operation,
            operation,
            payload.get("params") or {},
            apply=apply,
            idempotency_key=str(payload.get("idempotency_key") or ""),
        )
    except ValueError as exc:
        message = str(exc)
        if message.lower().startswith("unknown operation"):
            return _fail(request_id, exc, code="unknown_operation", status=404)
        return _fail(request_id, exc)
    except Exception as exc:
        return _fail(request_id, exc)

    blocked = bool((result.get("plan") or {}).get("blockers"))
    return _response_envelope(
        request_id=request_id,
        status="error" if blocked else "ok",
        data=result,
        http_status=409 if blocked else 200,
    )


@router.post(f"{PREFIX}/runs/source-phases")
async def workflow_run_source_phases(
    request: Request,
    payload: dict[str, Any] = Body(default_factory=dict),
):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="write", request_id=request_id)
    if auth_error is not None:
        return auth_error
    try:
        outcome = await _offload(
            _service(request).run_source_phases,
            phases=list(payload.get("phases") or ["fetch"]),
            scope=str(payload.get("scope") or "queued"),
            source_ids=list(payload.get("source_ids") or []),
            force=bool(payload.get("force")),
            limit=payload.get("limit"),
            import_id=str(payload.get("import_id") or ""),
            wait_seconds=float(payload.get("wait_seconds") or 0),
        )
    except ValueError as exc:
        status = 409 if "already running" in str(exc).lower() else 400
        return _fail(request_id, exc, code="run_rejected", status=status)
    except Exception as exc:
        return _fail(request_id, exc)
    return _response_envelope(
        request_id=request_id, status="ok", data=outcome.model_dump(mode="json")
    )


@router.post(f"{PREFIX}/runs/columns")
async def workflow_run_column(
    request: Request,
    payload: dict[str, Any] = Body(default_factory=dict),
):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="write", request_id=request_id)
    if auth_error is not None:
        return auth_error

    column_id = str(payload.get("column_id") or "").strip()
    if not column_id:
        return _error_response(
            request_id=request_id,
            code="column_id_required",
            message="Give the column id to run.",
            http_status=400,
        )
    try:
        outcome = await _offload(
            _service(request).run_column,
            column_id,
            scope=str(payload.get("scope") or "empty_only"),
            source_ids=list(payload.get("source_ids") or []),
            confirm_overwrite=bool(payload.get("confirm_overwrite")),
            wait_seconds=float(payload.get("wait_seconds") or 0),
        )
    except ValueError as exc:
        status = 409 if "already running" in str(exc).lower() else 400
        return _fail(request_id, exc, code="run_rejected", status=status)
    except Exception as exc:
        return _fail(request_id, exc)
    return _response_envelope(
        request_id=request_id, status="ok", data=outcome.model_dump(mode="json")
    )


@router.get(f"{PREFIX}/runs/{{run_id}}")
async def workflow_watch(
    request: Request,
    run_id: str,
    wait_seconds: float = Query(default=0),
):
    request_id = _request_id(request)
    auth_error = _authorize(request, access="read", request_id=request_id)
    if auth_error is not None:
        return auth_error
    try:
        outcome = await _offload(_service(request).watch, run_id, wait_seconds=wait_seconds)
    except Exception as exc:
        return _fail(request_id, exc, code="unknown_run", status=404)
    return _response_envelope(
        request_id=request_id, status="ok", data=outcome.model_dump(mode="json")
    )


@router.post(f"{PREFIX}/attach")
async def workflow_attach(
    request: Request,
    payload: dict[str, Any] = Body(default_factory=dict),
):
    request_id = _request_id(request)
    apply = bool(payload.get("apply"))
    auth_error = _authorize(
        request, access="write" if apply else "read", request_id=request_id
    )
    if auth_error is not None:
        return auth_error
    try:
        outcome = await _offload(
            _service(request).attach_files,
            paths=list(payload.get("paths") or []),
            hints=list(payload.get("hints") or []),
            scan_inbox=bool(payload.get("scan_inbox", True)),
            allow_new_sources=bool(payload.get("allow_new_sources", True)),
            apply=apply,
            reconvert=bool(payload.get("reconvert", True)),
            wait_seconds=float(payload.get("wait_seconds") or 0),
        )
    except Exception as exc:
        return _fail(request_id, exc)

    blocked = bool((outcome.plan or {}).get("blockers"))
    return _response_envelope(
        request_id=request_id,
        status="error" if blocked else "ok",
        data=outcome.model_dump(mode="json"),
        http_status=409 if blocked else 200,
    )
