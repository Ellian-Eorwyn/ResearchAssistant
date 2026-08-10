"""Capture router: resolve blocked fetches by hand.

Serves the Resolve Fetches page — the list of sources that could not be fetched,
a live remote-controlled browser for working past whatever is blocking them, and
the capture that writes the real page back into the same source id.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.concurrency import run_in_threadpool

from backend.models.capture import (
    CaptureInputRequest,
    CaptureNavigateRequest,
    CaptureRequest,
    CaptureSessionInfo,
    CaptureSessionListResponse,
    CaptureSessionRequest,
    CaptureAvailabilityResponse,
    ManualAttachPathRequest,
    ResolveSourceListResponse,
    ResolveSourceRow,
    WatchFolderFile,
    WatchFolderListResponse,
)
from backend.models.repository import RepositoryCaptureResponse
from backend.pipeline.interactive_browser import (
    InteractiveBrowserError,
    InteractiveBrowserSession,
)
from backend.pipeline.source_capture import (
    CAPTURE_FETCH_METHOD,
    MAX_UPLOAD_BYTES,
    REJECTED_ARCHIVE_EXTENSIONS,
    SUPPORTED_UPLOAD_EXTENSIONS,
    CapturedArtifacts,
    UnsupportedManualUploadError,
    artifacts_from_uploaded_bytes,
)
from backend.pipeline.source_downloader import (
    detect_runtime_capabilities,
    effective_markdown_rel_path,
    extract_canonical_url,
    extract_markdown_with_fallback,
    png_images_to_pdf_bytes,
)

logger = logging.getLogger(__name__)
router = APIRouter()

# Statuses the Resolve page offers to work on.
RESOLVABLE_STATUSES = {"blocked", "failed", "partial"}
PREVIEW_CHARS = 4000
MAX_VECTOR_PDF_PAGE_POINTS = 14000

# Watch folder: where a browser drops the pages the user saved by hand.
DEFAULT_WATCH_DIR = "~/Downloads"
# A stop so a pathological folder cannot turn one request into a long scan.
MAX_WATCH_SCAN_ENTRIES = 5000


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


def _capture_or_http_error(
    service,
    source_id: str,
    artifacts: CapturedArtifacts,
) -> RepositoryCaptureResponse:
    """Write artifacts into a source, translating the service's refusals.

    Every manual route ends here, so a running job or an unknown id reads the
    same whether the file came from the live browser, an upload or the watch
    folder.
    """
    try:
        return service.capture_source_artifacts(source_id=source_id, artifacts=artifacts)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        detail = str(exc)
        status_code = 404 if "unknown source_id" in detail.lower() else 400
        raise HTTPException(status_code=status_code, detail=detail) from exc


def _watch_root(request: Request) -> Path:
    """The folder the user's browser saves into, as configured or defaulted."""
    configured = ""
    try:
        configured = str(
            request.app.state.file_store.load_app_settings().manual_capture_watch_dir or ""
        ).strip()
    except Exception:
        logger.exception("capture: could not read the watch folder setting")
    return Path(configured or DEFAULT_WATCH_DIR).expanduser()


def _is_within(candidate: Path, root: Path) -> bool:
    return candidate == root or root in candidate.parents


def _watch_path_refusal(candidate: Path, root: Path) -> tuple[str, str] | None:
    """Why this path may not be read, or None if it may.

    The client hands us a server-side filesystem path, so this is the security
    boundary, not a convenience check. The app binds loopback, which means the
    realistic attacker is a web page reaching localhost — and without
    containment this endpoint is an arbitrary-file-read that would copy
    something like `~/.ssh/id_rsa` into the repository, where the LLM phases
    would then read it back out.

    The ordering matches `repo_operations.attach_files._collect_candidates`:
    refuse a symlink before resolving, check containment before existence so a
    traversal reports as a traversal, and resolve *both* sides — on macOS
    `/tmp` is `/private/tmp` and `$HOME` sits under `/System/Volumes/Data`, so
    comparing unresolved strings produces false negatives.
    """
    if candidate.is_symlink():
        return ("symlink_not_allowed", "That path is a symlink. Attach the file itself.")

    resolved = candidate.resolve()
    # resolve() also collapses a symlinked *parent*, so a link partway up the
    # path is caught here even though the check above only sees the leaf.
    if not _is_within(resolved, root.resolve()):
        return (
            "path_outside_watch_folder",
            "That file is outside the watch folder. Change the folder in Settings, "
            "or upload the file directly.",
        )
    if not resolved.is_file():
        return ("file_not_found", "That file is no longer there.")

    suffix = resolved.suffix.lower()
    if suffix in REJECTED_ARCHIVE_EXTENSIONS:
        return (
            "unsupported_file_type",
            "MHTML archives are not supported. Save the page as HTML "
            "(or print it to PDF) and attach that instead.",
        )
    if suffix not in SUPPORTED_UPLOAD_EXTENSIONS:
        return (
            "unsupported_file_type",
            f"Unsupported file type `{suffix or resolved.name}`. Attach HTML, PDF, or Markdown.",
        )

    size = resolved.stat().st_size
    if size == 0:
        return ("empty_file", "That file is empty.")
    if size > MAX_UPLOAD_BYTES:
        return (
            "file_too_large",
            f"That file is {size // (1024 * 1024)} MB, over the "
            f"{MAX_UPLOAD_BYTES // (1024 * 1024)} MB limit.",
        )
    return None


def _scan_watch_folder(
    root: Path,
    *,
    since_ms: int,
    max_age_minutes: int,
    limit: int,
) -> WatchFolderListResponse:
    """List recent attachable files, newest first. Blocking; run in a threadpool."""
    response = WatchFolderListResponse(root=str(root))
    try:
        if not root.is_dir():
            response.error = (
                f"`{root}` is not a folder. Set a watch folder in Settings."
            )
            return response
        response.configured = True

        cutoff = time.time() - max_age_minutes * 60
        found: list[WatchFolderFile] = []
        # Depth 1 only: saving a page "complete" writes a sibling `<name>_files/`
        # folder, and recursing into it would bury the page under its own assets.
        for index, entry in enumerate(root.iterdir()):
            if index >= MAX_WATCH_SCAN_ENTRIES:
                break
            if entry.name.startswith("."):
                continue
            suffix = entry.suffix.lower()
            if suffix not in SUPPORTED_UPLOAD_EXTENSIONS:
                continue
            try:
                # Everything the attach guard would refuse is filtered here too,
                # so the list never offers a file that cannot then be attached.
                if entry.is_symlink() or not entry.is_file():
                    continue
                stat = entry.stat()
            except OSError:
                continue
            if stat.st_mtime < cutoff:
                continue
            # A zero-byte file is a download still in flight.
            if stat.st_size == 0 or stat.st_size > MAX_UPLOAD_BYTES:
                continue
            modified_ms = int(stat.st_mtime * 1000)
            found.append(
                WatchFolderFile(
                    path=str(entry),
                    name=entry.name,
                    size_bytes=stat.st_size,
                    modified_at=datetime.fromtimestamp(
                        stat.st_mtime, tz=timezone.utc
                    ).isoformat(),
                    modified_ms=modified_ms,
                    extension=suffix,
                    is_new=bool(since_ms) and modified_ms >= since_ms,
                )
            )

        found.sort(key=lambda item: item.modified_ms, reverse=True)
        response.files = found[:limit]
        return response
    except PermissionError:
        # macOS gates ~/Downloads behind Files-and-Folders access, so this is a
        # routine first-run state rather than an error worth a 500.
        response.error = (
            f"No permission to read `{root}`. On macOS, grant the app "
            "Files and Folders access under System Settings → Privacy & Security."
        )
        return response
    except OSError as exc:
        response.error = f"Could not read `{root}`: {exc}"
        return response


@router.get("/capture/watch-folder", response_model=WatchFolderListResponse)
async def list_watch_folder(
    request: Request,
    since_ms: int = Query(default=0, ge=0),
    max_age_minutes: int = Query(default=1440, ge=1, le=20160),
    limit: int = Query(default=40, ge=1, le=200),
) -> WatchFolderListResponse:
    """Recent files in the user's download folder, newest first.

    Deliberately does not require an attached repository, so the panel can
    render its guidance before one is picked. The scan is pushed to a thread:
    a blocking `iterdir()` over a large folder on the event loop would also
    stall the frame long-poll and visibly hitch the live browser stream.
    """
    root = _watch_root(request)
    return await run_in_threadpool(
        _scan_watch_folder,
        root,
        since_ms=since_ms,
        max_age_minutes=max_age_minutes,
        limit=limit,
    )


@router.post(
    "/capture/sources/{source_id}/attach-path",
    response_model=RepositoryCaptureResponse,
)
async def attach_watched_file_into_source(
    source_id: str,
    request: Request,
    payload: ManualAttachPathRequest,
) -> RepositoryCaptureResponse:
    """Attach a file the user already downloaded, by path, without re-uploading it.

    The file is only ever read — never moved, renamed or deleted. It is the
    user's own download folder, and they may well want the file afterwards.
    """
    service = _service(request)
    root = _watch_root(request)

    raw = str(payload.path or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="A file path is required.")

    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        candidate = root / candidate

    refusal = await run_in_threadpool(_watch_path_refusal, candidate, root)
    if refusal is not None:
        code, message = refusal
        raise HTTPException(
            status_code=404 if code == "file_not_found" else 400,
            detail=message,
        )

    resolved = candidate.resolve()
    content = await run_in_threadpool(resolved.read_bytes)
    try:
        artifacts = artifacts_from_uploaded_bytes(
            content=content,
            filename=resolved.name,
            final_url=payload.final_url,
        )
    except UnsupportedManualUploadError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return _capture_or_http_error(service, source_id, artifacts)


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

    result = _capture_or_http_error(service, source_id, artifacts)

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

    try:
        artifacts = artifacts_from_uploaded_bytes(
            content=content,
            filename=file.filename or "",
            final_url=final_url,
        )
    except UnsupportedManualUploadError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return _capture_or_http_error(service, source_id, artifacts)
