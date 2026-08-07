"""Capture router: resolve blocked fetches by hand.

Serves the Resolve Fetches page — the list of sources that could not be fetched,
a live remote-controlled browser for working past whatever is blocking them, and
the capture that writes the real page back into the same source id.
"""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, Response, UploadFile

from backend.models.capture import (
    CaptureInputRequest,
    CaptureNavigateRequest,
    CaptureRequest,
    CaptureSessionInfo,
    CaptureSessionListResponse,
    CaptureSessionRequest,
    CaptureAvailabilityResponse,
    ResolveSourceListResponse,
    ResolveSourceRow,
)
from backend.models.repository import RepositoryCaptureResponse
from backend.pipeline.fetch_verification import verify_fetch
from backend.pipeline.interactive_browser import (
    InteractiveBrowserError,
    InteractiveBrowserSession,
)
from backend.pipeline.source_capture import CAPTURE_FETCH_METHOD, UPLOAD_FETCH_METHOD, CapturedArtifacts
from backend.pipeline.source_downloader import (
    build_searchable_pdf,
    decode_bytes_to_text,
    detect_runtime_capabilities,
    effective_markdown_rel_path,
    extract_canonical_url,
    extract_markdown_with_fallback,
    extract_pdf_pages,
    extract_title,
    png_images_to_pdf_bytes,
)

logger = logging.getLogger(__name__)
router = APIRouter()

# Statuses the Resolve page offers to work on.
RESOLVABLE_STATUSES = {"blocked", "failed", "partial"}
PREVIEW_CHARS = 4000
MAX_VECTOR_PDF_PAGE_POINTS = 14000


def _service(request: Request):
    service = request.app.state.repository_service
    if not service.is_attached:
        raise HTTPException(status_code=400, detail="Attach a repository first.")
    return service


def _manager(request: Request):
    manager = request.app.state.interactive_browser
    service = request.app.state.repository_service
    manager.set_profile_root(service.path / ".ra_repo" if service.is_attached else None)
    return manager


def _session_info(session: InteractiveBrowserSession) -> CaptureSessionInfo:
    return CaptureSessionInfo(**session.info())


@router.get("/capture/availability", response_model=CaptureAvailabilityResponse)
async def capture_availability(request: Request) -> CaptureAvailabilityResponse:
    manager = request.app.state.interactive_browser
    availability = await manager.availability()
    return CaptureAvailabilityResponse(
        available=availability.available,
        headless=availability.headless,
        channel=availability.channel,
        display=availability.display,
        error=availability.error,
        guidance=availability.guidance,
    )


@router.get("/capture/blocked-sources", response_model=ResolveSourceListResponse)
async def list_blocked_sources(
    request: Request,
    include: str = Query(default="blocked,failed"),
) -> ResolveSourceListResponse:
    service = _service(request)

    # First visit to a repository fetched under an older verifier: score it now
    # so the list is not silently missing the false successes.
    reverified = False
    if not service.fetch_verification_is_current():
        try:
            service.reverify_fetches(scope="all")
            reverified = True
        except Exception:
            logger.exception("capture: automatic re-verification failed")

    wanted = {
        value.strip().lower()
        for value in (include or "").split(",")
        if value.strip()
    } or {"blocked", "failed"}

    rows: list[ResolveSourceRow] = []
    counts = {"blocked": 0, "failed": 0, "partial": 0}
    for row in service.list_source_rows():
        status = str(row.fetch_status or "").strip().lower()
        if status in counts:
            counts[status] += 1
        if status not in wanted:
            continue

        preview = ""
        rel_path = effective_markdown_rel_path(row, service.path)
        if rel_path:
            try:
                with (service.path / rel_path).open("r", encoding="utf-8", errors="replace") as fh:
                    preview = fh.read(PREVIEW_CHARS)
            except OSError:
                preview = ""

        rows.append(
            ResolveSourceRow(
                id=row.id,
                title=row.title,
                original_url=row.original_url,
                final_url=row.final_url,
                fetch_status=status,
                fetch_verification=row.fetch_verification,
                http_status=row.http_status,
                detected_type=row.detected_type,
                error_message=row.error_message,
                markdown_char_count=row.markdown_char_count,
                fetched_at=row.fetched_at,
                current_content_preview=preview,
            )
        )

    return ResolveSourceListResponse(
        rows=rows,
        total=len(rows),
        blocked_count=counts["blocked"],
        failed_count=counts["failed"],
        partial_count=counts["partial"],
        reverified=reverified,
    )


@router.post("/capture/sessions", response_model=CaptureSessionInfo)
async def create_capture_session(
    request: Request,
    payload: CaptureSessionRequest,
) -> CaptureSessionInfo:
    _service(request)
    manager = _manager(request)
    try:
        session = await manager.create(
            source_id=payload.source_id,
            url=payload.url,
            viewport_width=payload.viewport_width,
            viewport_height=payload.viewport_height,
        )
    except InteractiveBrowserError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return _session_info(session)


@router.get("/capture/sessions", response_model=CaptureSessionListResponse)
async def list_capture_sessions(request: Request) -> CaptureSessionListResponse:
    manager = request.app.state.interactive_browser
    return CaptureSessionListResponse(
        sessions=[CaptureSessionInfo(**info) for info in manager.list_sessions()]
    )


@router.get("/capture/sessions/{session_id}", response_model=CaptureSessionInfo)
async def get_capture_session(session_id: str, request: Request) -> CaptureSessionInfo:
    manager = request.app.state.interactive_browser
    try:
        return _session_info(manager.get(session_id))
    except InteractiveBrowserError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.post("/capture/sessions/{session_id}/navigate", response_model=CaptureSessionInfo)
async def navigate_capture_session(
    session_id: str,
    request: Request,
    payload: CaptureNavigateRequest,
) -> CaptureSessionInfo:
    manager = request.app.state.interactive_browser
    try:
        session = manager.get(session_id)
        if payload.action == "back":
            await session.go_back()
        elif payload.action == "forward":
            await session.go_forward()
        elif payload.action == "reload":
            await session.reload()
        else:
            await session.navigate(payload.url)
    except InteractiveBrowserError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _session_info(session)


@router.get("/capture/sessions/{session_id}/frame")
async def get_capture_frame(
    session_id: str,
    request: Request,
    after_seq: int = Query(default=0, ge=0),
    timeout_ms: int = Query(default=15000, ge=100, le=30000),
) -> Response:
    """Long-poll for the next frame.

    Held open until the page paints something newer than `after_seq`, so the
    client gets push-like latency without a WebSocket — and asking for the next
    frame only after drawing the last one gives natural backpressure.
    """
    manager = request.app.state.interactive_browser
    try:
        session = manager.get(session_id)
    except InteractiveBrowserError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    frame = await session.wait_for_frame(after_seq, timeout_ms)
    if frame is None:
        return Response(status_code=204)

    return Response(
        content=frame.data,
        media_type="image/jpeg",
        headers={
            "X-RA-Frame-Seq": str(frame.seq),
            "X-RA-Frame-Width": str(frame.width),
            "X-RA-Frame-Height": str(frame.height),
            "X-RA-Page-Url": session._safe_url(),
            "Cache-Control": "no-store",
        },
    )


@router.post("/capture/sessions/{session_id}/input")
async def send_capture_input(
    session_id: str,
    request: Request,
    payload: CaptureInputRequest,
) -> dict:
    manager = request.app.state.interactive_browser
    try:
        session = manager.get(session_id)
        await session.dispatch_input(
            [event.model_dump(exclude_none=True) for event in payload.events],
            payload.canvas_width,
            payload.canvas_height,
        )
    except InteractiveBrowserError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"seq": session.frame.seq, "current_url": session._safe_url()}


@router.delete("/capture/sessions/{session_id}")
async def close_capture_session(session_id: str, request: Request) -> dict:
    manager = request.app.state.interactive_browser
    await manager.close(session_id)
    return {"status": "closed"}


async def _capture_page_artifacts(
    session: InteractiveBrowserSession,
    payload: CaptureRequest,
) -> CapturedArtifacts:
    page = session.page
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        # A page that never goes idle (polling widgets, video) is still capturable.
        pass

    html = await page.content()
    title = await page.title()
    artifacts = CapturedArtifacts(
        raw_html=html if payload.include_raw_html else "",
        rendered_html=html if payload.include_rendered_html else "",
        final_url=page.url,
        title=title,
        canonical_url=extract_canonical_url(html),
        content_type="text/html",
        http_status=200,
        detected_type="html",
        fetch_method=CAPTURE_FETCH_METHOD,
    )

    if payload.include_rendered_pdf:
        artifacts.rendered_pdf = await _render_pdf(page, session)

    if payload.include_markdown:
        capabilities = detect_runtime_capabilities(use_llm=False, llm_backend=None)
        markdown, used_fallback, _ = extract_markdown_with_fallback(html, capabilities)
        artifacts.markdown = markdown
        artifacts.extraction_method = (
            "rendered_html_manual_fallback" if used_fallback else "rendered_html_manual"
        )

    return artifacts


async def _render_pdf(page, session: InteractiveBrowserSession) -> bytes:
    """Vector PDF where Chromium allows it, full-page screenshot otherwise."""
    try:
        await page.emulate_media(media="screen")
        scroll_height = await page.evaluate(
            """() => Math.max(
                document.documentElement?.scrollHeight || 0,
                document.body?.scrollHeight || 0,
                window.innerHeight || 0
            )"""
        )
        height = int(scroll_height or session.viewport_height)
        if height > MAX_VECTOR_PDF_PAGE_POINTS:
            height = session.viewport_height
        return await page.pdf(
            width=f"{session.viewport_width}px",
            height=f"{max(height, 1)}px",
            print_background=True,
            margin={"top": "0", "bottom": "0", "left": "0", "right": "0"},
            scale=1,
            prefer_css_page_size=False,
        )
    except Exception:
        # page.pdf() is unavailable in headful Chromium on some builds.
        try:
            shot = await page.screenshot(type="png", full_page=True)
            return png_images_to_pdf_bytes([shot])
        except Exception:
            logger.exception("capture: could not produce a rendered PDF")
            return b""


@router.post("/capture/sessions/{session_id}/capture", response_model=RepositoryCaptureResponse)
async def capture_session_into_source(
    session_id: str,
    request: Request,
    payload: CaptureRequest,
) -> RepositoryCaptureResponse:
    service = _service(request)
    manager = request.app.state.interactive_browser
    try:
        session = manager.get(session_id)
    except InteractiveBrowserError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    source_id = payload.source_id or session.source_id
    try:
        artifacts = await _capture_page_artifacts(session, payload)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Could not read the page: {exc}") from exc

    try:
        result = service.capture_source_artifacts(source_id=source_id, artifacts=artifacts)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "unknown source_id" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc

    if result.status == "captured":
        # Keep the cookies that got us through, so automated fetches of this
        # domain have a chance next time.
        await manager.save_profile_state(session)

    return result


@router.post(
    "/capture/sources/{source_id}/manual-upload",
    response_model=RepositoryCaptureResponse,
)
async def manual_upload_into_source(
    source_id: str,
    request: Request,
    file: UploadFile = File(...),
    final_url: str = Form(default=""),
) -> RepositoryCaptureResponse:
    """Last resort: the user saved the page themselves and drops the file here.

    Some sites cannot be beaten by any automated browser, so this path must
    always work even when the interactive session is unavailable.
    """
    service = _service(request)
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="The uploaded file is empty.")

    suffix = Path(file.filename or "").suffix.lower()
    artifacts = CapturedArtifacts(
        final_url=final_url,
        fetch_method=UPLOAD_FETCH_METHOD,
        http_status=200,
    )

    if suffix in {".html", ".htm", ".xhtml"}:
        html = decode_bytes_to_text(content)
        capabilities = detect_runtime_capabilities(use_llm=False, llm_backend=None)
        markdown, used_fallback, _ = extract_markdown_with_fallback(html, capabilities)
        artifacts.raw_html = html
        artifacts.markdown = markdown
        artifacts.title = extract_title(html)
        artifacts.canonical_url = extract_canonical_url(html)
        artifacts.content_type = "text/html"
        artifacts.detected_type = "html"
        artifacts.extraction_method = (
            "raw_html_manual_fallback" if used_fallback else "raw_html_manual"
        )
    elif suffix == ".pdf":
        artifacts.raw_pdf = content
        artifacts.content_type = "application/pdf"
        artifacts.detected_type = "pdf"
        pages = extract_pdf_pages(content)
        artifacts.markdown = "\n\n".join(
            str(page.get("text") or "").strip() for page in pages if page.get("text")
        ).strip()
        artifacts.extraction_method = "pdf_text_manual"
        ocr_pdf, ocr_status, _ = build_searchable_pdf(content)
        if ocr_pdf:
            artifacts.ocr_pdf = ocr_pdf
            artifacts.ocr_status = ocr_status
    elif suffix in {".md", ".markdown", ".txt"}:
        artifacts.markdown = decode_bytes_to_text(content)
        artifacts.content_type = "text/plain"
        artifacts.detected_type = "document"
        artifacts.extraction_method = "manual_markdown"
    elif suffix in {".mhtml", ".mht"}:
        raise HTTPException(
            status_code=400,
            detail=(
                "MHTML archives are not supported. Save the page as HTML "
                "(or print it to PDF) and upload that instead."
            ),
        )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type `{suffix or file.filename}`. Upload HTML, PDF, or Markdown.",
        )

    # An uploaded file has no HTTP evidence, so the verifier only sees the text;
    # judging it the same way keeps a saved block page from counting as a fix.
    verification = verify_fetch(
        http_status=None,
        final_url=final_url,
        title=artifacts.title,
        raw_html=artifacts.raw_html,
        extracted_text=artifacts.markdown,
        content_type=artifacts.content_type,
        detected_type=artifacts.detected_type,
    )
    artifacts.notes.append(f"verify_{verification.reason}")

    try:
        return service.capture_source_artifacts(source_id=source_id, artifacts=artifacts)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "unknown source_id" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc
