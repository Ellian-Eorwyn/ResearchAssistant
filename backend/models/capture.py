"""Models for the Resolve Fetches page and its interactive browser session."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class ResolveSourceRow(BaseModel):
    id: str
    title: str = ""
    original_url: str = ""
    final_url: str = ""
    fetch_status: str = ""
    fetch_verification: str = ""
    http_status: int | None = None
    detected_type: str = ""
    error_message: str = ""
    markdown_char_count: int = 0
    fetched_at: str = ""
    # The head of whatever we currently hold, so the user can see the block page
    # they are about to replace.
    current_content_preview: str = ""


class ResolveSourceListResponse(BaseModel):
    rows: list[ResolveSourceRow] = Field(default_factory=list)
    total: int = 0
    blocked_count: int = 0
    failed_count: int = 0
    partial_count: int = 0
    # True when this request triggered the one-shot retroactive re-verification.
    reverified: bool = False


class WatchFolderFile(BaseModel):
    """One file in the watch folder that could be attached to a source."""

    path: str
    name: str = ""
    size_bytes: int = 0
    modified_at: str = ""
    modified_ms: int = 0
    extension: str = ""
    # Landed after the user last said "open this source in my browser", so it is
    # almost certainly the thing they just saved.
    is_new: bool = False


class WatchFolderListResponse(BaseModel):
    root: str = ""
    # False when the folder is missing entirely, so the UI can point at Settings
    # instead of showing an empty list that looks like a bug.
    configured: bool = False
    files: list[WatchFolderFile] = Field(default_factory=list)
    error: str = ""


class ManualAttachPathRequest(BaseModel):
    path: str
    final_url: str = ""


class CaptureAvailabilityResponse(BaseModel):
    available: bool = False
    headless: bool = True
    channel: str = ""
    display: str = ""
    error: str = ""
    guidance: str = ""


class CaptureSessionRequest(BaseModel):
    source_id: str
    url: str = ""
    viewport_width: int = Field(default=1280, ge=320, le=2560)
    viewport_height: int = Field(default=800, ge=240, le=2160)


class CaptureSessionInfo(BaseModel):
    session_id: str
    source_id: str = ""
    current_url: str = ""
    title: str = ""
    viewport_width: int = 1280
    viewport_height: int = 800
    frame_mode: str = "screencast"
    frame_seq: int = 0
    headless: bool = True
    channel: str = ""
    idle_seconds: float = 0.0


class CaptureSessionListResponse(BaseModel):
    sessions: list[CaptureSessionInfo] = Field(default_factory=list)


class CaptureNavigateRequest(BaseModel):
    url: str = ""
    action: Literal["goto", "back", "forward", "reload"] = "goto"


class CaptureInputEvent(BaseModel):
    """Mirrors the CDP Input domain so the client can forward near-raw DOM events."""

    type: Literal[
        "mouseMoved",
        "mousePressed",
        "mouseReleased",
        "mouseWheel",
        "keyDown",
        "keyUp",
        "rawKeyDown",
        "char",
        "insertText",
    ]
    x: float | None = None
    y: float | None = None
    button: str | None = None
    buttons: int | None = None
    clickCount: int | None = None
    modifiers: int | None = None
    deltaX: float | None = None
    deltaY: float | None = None
    key: str | None = None
    code: str | None = None
    text: str | None = None
    unmodifiedText: str | None = None
    windowsVirtualKeyCode: int | None = None


class CaptureInputRequest(BaseModel):
    canvas_width: int = Field(default=0, ge=0)
    canvas_height: int = Field(default=0, ge=0)
    events: list[CaptureInputEvent] = Field(default_factory=list)


class CaptureRequest(BaseModel):
    source_id: str = ""
    include_raw_html: bool = True
    include_rendered_html: bool = True
    include_rendered_pdf: bool = True
    include_markdown: bool = True
