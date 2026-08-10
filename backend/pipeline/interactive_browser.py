"""A real browser the user can drive from inside the app.

Some sources cannot be fetched by a script at all: Cloudflare wants a checkbox
clicked, a publisher wants a login, a site wants cookies accepted. This module
runs a real Chromium on the server and streams its pixels to the browser tab,
forwarding the user's clicks and keystrokes back. Once they have reached the
real page, the capture handler pulls the HTML, PDF and markdown out of the *live*
page and hands them to the repository.

An `<iframe>` cannot do this job. The sites that block automated fetches are
precisely the ones that send `X-Frame-Options`/`frame-ancestors`, so the browser
would refuse to render them. A pixel stream has no such problem.

Threading note: the downloader drives Playwright through its **sync** API on a
worker thread. This module uses the **async** API on the FastAPI event loop
instead — CDP callbacks land on the loop naturally and no marshalling layer is
needed. The two Playwright instances are independent and coexist fine, but the
sync API must never be called from here.
"""

from __future__ import annotations

import asyncio
import base64
import logging
import os
import shutil
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

DEFAULT_VIEWPORT_WIDTH = 1280
DEFAULT_VIEWPORT_HEIGHT = 800
NAVIGATION_TIMEOUT_MS = 45000
# One session at a time: a headful Chromium is a heavyweight, visible thing and
# the UI only ever shows one.
MAX_SESSIONS = 1
IDLE_TIMEOUT_SECONDS = 600
REAPER_INTERVAL_SECONDS = 60
# If the compositor never pushes a frame (occluded window, headless quirk), fall
# back to explicit screenshots rather than showing the user a blank canvas.
SCREENCAST_GRACE_SECONDS = 3.0
SCREENSHOT_POLL_INTERVAL = 0.25
FRAME_WAIT_TIMEOUT_MS = 15000

BLOCKED_URL_SCHEMES = {"file", "chrome", "devtools", "view-source", "chrome-extension"}

PROFILE_DIR_NAME = "browser_profile"
STORAGE_STATE_FILE = "storage_state.json"
PROFILE_META_FILE = "profile.json"


class InteractiveBrowserError(RuntimeError):
    """Something went wrong that the user can act on."""


@dataclass
class BrowserAvailability:
    available: bool = False
    headless: bool = True
    channel: str = ""
    display: str = ""
    error: str = ""
    guidance: str = ""


def normalize_target_url(raw_url: str) -> str:
    """Reject schemes that would let a session read the server's disk."""
    candidate = str(raw_url or "").strip()
    if not candidate:
        raise InteractiveBrowserError("A URL is required.")
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    scheme = (urlsplit(candidate).scheme or "").lower()
    if scheme in BLOCKED_URL_SCHEMES:
        raise InteractiveBrowserError(f"The `{scheme}:` scheme is not allowed here.")
    if scheme not in {"http", "https"}:
        raise InteractiveBrowserError("Only http:// and https:// URLs can be opened.")
    return candidate


def _display_available() -> str:
    """Name the window server we can put a real browser window on.

    `DISPLAY`/`WAYLAND_DISPLAY` only answer this on X11 and Wayland. macOS and
    Windows set neither, so probing them there reported "no display" for every
    session and forced headless — the one configuration bot walls reject on
    sight. A logged-in desktop session on those platforms always has a
    compositor, so say so.
    """
    if sys.platform == "darwin":
        return "aqua"
    if sys.platform == "win32":
        return "windows"
    return os.environ.get("DISPLAY", "") or os.environ.get("WAYLAND_DISPLAY", "")


def _chrome_channel() -> str:
    """Prefer real Chrome over bundled Chromium.

    Bot walls fingerprint the browser build, and a stock Chrome with a persistent
    profile gets through checks that headless Chromium does not.

    Playwright locates the binary itself once it is handed a channel name, so
    this only has to answer "is it installed". A `PATH` probe cannot: on macOS
    Chrome lives in an app bundle and on Windows under Program Files, so neither
    ever appeared on `PATH` and both silently fell back to bundled Chromium.
    """
    if sys.platform == "darwin":
        if Path("/Applications/Google Chrome.app").exists():
            return "chrome"
        if Path("/Applications/Microsoft Edge.app").exists():
            return "msedge"
        return ""

    if sys.platform == "win32":
        roots = [
            os.environ.get("PROGRAMFILES", ""),
            os.environ.get("PROGRAMFILES(X86)", ""),
            os.environ.get("LOCALAPPDATA", ""),
        ]
        for root in roots:
            if root and (Path(root) / "Google/Chrome/Application/chrome.exe").exists():
                return "chrome"
        for root in roots:
            if root and (Path(root) / "Microsoft/Edge/Application/msedge.exe").exists():
                return "msedge"
        return ""

    for candidate in ("google-chrome", "google-chrome-stable", "chromium"):
        if shutil.which(candidate):
            return "chrome" if candidate.startswith("google-chrome") else "chromium"
    if Path("/opt/google/chrome/chrome").exists():
        return "chrome"
    return ""


@dataclass
class _Frame:
    data: bytes = b""
    seq: int = 0
    width: int = 0
    height: int = 0


@dataclass
class InteractiveBrowserSession:
    """One live browser page, plus the plumbing to see and drive it."""

    session_id: str
    source_id: str
    viewport_width: int = DEFAULT_VIEWPORT_WIDTH
    viewport_height: int = DEFAULT_VIEWPORT_HEIGHT

    context: Any = None
    page: Any = None
    cdp: Any = None

    frame: _Frame = field(default_factory=_Frame)
    frame_event: asyncio.Event = field(default_factory=asyncio.Event)
    frame_mode: str = "screencast"
    headless: bool = True
    channel: str = ""
    started_at: float = field(default_factory=time.monotonic)
    last_used_at: float = field(default_factory=time.monotonic)
    closing: bool = False

    _screenshot_task: asyncio.Task | None = None

    def touch(self) -> None:
        self.last_used_at = time.monotonic()

    @property
    def idle_seconds(self) -> float:
        return time.monotonic() - self.last_used_at

    def info(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "source_id": self.source_id,
            "current_url": self._safe_url(),
            "title": getattr(self.page, "_ra_title", "") or "",
            "viewport_width": self.viewport_width,
            "viewport_height": self.viewport_height,
            "frame_mode": self.frame_mode,
            "frame_seq": self.frame.seq,
            "headless": self.headless,
            "channel": self.channel,
            "idle_seconds": round(self.idle_seconds, 1),
        }

    def _safe_url(self) -> str:
        try:
            return self.page.url if self.page else ""
        except Exception:
            return ""

    # ---------------------------------------------------------------- frames

    def _publish(self, data: bytes, width: int, height: int) -> None:
        self.frame = _Frame(data=data, seq=self.frame.seq + 1, width=width, height=height)
        self.frame_event.set()
        self.frame_event = asyncio.Event()

    async def start_screencast(self) -> None:
        self.cdp = await self.context.new_cdp_session(self.page)
        self.cdp.on("Page.screencastFrame", self._on_screencast_frame)
        await self.cdp.send(
            "Page.startScreencast",
            {
                "format": "jpeg",
                "quality": 60,
                "maxWidth": self.viewport_width,
                "maxHeight": self.viewport_height,
                "everyNthFrame": 1,
            },
        )

    def _on_screencast_frame(self, params: dict[str, Any]) -> None:
        try:
            data = base64.b64decode(params.get("data") or "")
        except Exception:
            data = b""
        metadata = params.get("metadata") or {}
        if data:
            self._publish(
                data,
                int(metadata.get("deviceWidth") or self.viewport_width),
                int(metadata.get("deviceHeight") or self.viewport_height),
            )
        session_id = params.get("sessionId")
        if session_id is not None and self.cdp is not None:
            # Acknowledge immediately; Chrome will not send another frame until
            # we do, which is exactly the backpressure we want.
            asyncio.ensure_future(self._ack(session_id))

    async def _ack(self, session_id: Any) -> None:
        try:
            await self.cdp.send("Page.screencastFrameAck", {"sessionId": session_id})
        except Exception:
            pass

    async def ensure_frames_flowing(self) -> None:
        """Switch to screenshot polling if the screencast never produced a frame."""
        if self.frame.seq or self.frame_mode == "screenshot_poll":
            return
        await asyncio.sleep(SCREENCAST_GRACE_SECONDS)
        if self.frame.seq or self.closing:
            return
        logger.info("interactive browser: no screencast frames, falling back to screenshots")
        self.frame_mode = "screenshot_poll"
        self._screenshot_task = asyncio.ensure_future(self._screenshot_loop())

    async def _screenshot_loop(self) -> None:
        while not self.closing and self.page is not None:
            try:
                data = await self.page.screenshot(type="jpeg", quality=60)
                self._publish(data, self.viewport_width, self.viewport_height)
            except Exception:
                if self.closing:
                    return
            await asyncio.sleep(SCREENSHOT_POLL_INTERVAL)

    async def wait_for_frame(self, after_seq: int, timeout_ms: int) -> _Frame | None:
        """Hold the request open until there is a frame newer than `after_seq`."""
        if self.frame.seq > after_seq and self.frame.data:
            return self.frame
        try:
            await asyncio.wait_for(self.frame_event.wait(), timeout=timeout_ms / 1000)
        except asyncio.TimeoutError:
            return None
        if self.frame.seq > after_seq and self.frame.data:
            return self.frame
        return None

    # ----------------------------------------------------------------- input

    async def dispatch_input(
        self,
        events: list[dict[str, Any]],
        canvas_width: int,
        canvas_height: int,
    ) -> None:
        """Replay browser-side events into the real page via CDP.

        Coordinates are scaled here rather than client-side so the server never
        depends on the canvas reporting its scale honestly.
        """
        if self.cdp is None:
            raise InteractiveBrowserError("This session cannot accept input.")
        scale_x = self.viewport_width / canvas_width if canvas_width else 1.0
        scale_y = self.viewport_height / canvas_height if canvas_height else 1.0

        for event in events:
            event_type = str(event.get("type") or "")
            if event_type in {"mouseMoved", "mousePressed", "mouseReleased", "mouseWheel"}:
                payload = {
                    "type": event_type,
                    "x": float(event.get("x") or 0) * scale_x,
                    "y": float(event.get("y") or 0) * scale_y,
                    "button": event.get("button") or "none",
                    "buttons": int(event.get("buttons") or 0),
                    "clickCount": int(event.get("clickCount") or 0),
                    "modifiers": int(event.get("modifiers") or 0),
                }
                if event_type == "mouseWheel":
                    payload["deltaX"] = float(event.get("deltaX") or 0)
                    payload["deltaY"] = float(event.get("deltaY") or 0)
                await self.cdp.send("Input.dispatchMouseEvent", payload)
            elif event_type in {"keyDown", "keyUp", "rawKeyDown", "char"}:
                payload = {
                    "type": event_type,
                    "key": event.get("key") or "",
                    "code": event.get("code") or "",
                    "modifiers": int(event.get("modifiers") or 0),
                }
                if event.get("text"):
                    payload["text"] = event["text"]
                if event.get("unmodifiedText"):
                    payload["unmodifiedText"] = event["unmodifiedText"]
                if event.get("windowsVirtualKeyCode"):
                    payload["windowsVirtualKeyCode"] = int(event["windowsVirtualKeyCode"])
                    payload["nativeVirtualKeyCode"] = int(event["windowsVirtualKeyCode"])
                await self.cdp.send("Input.dispatchKeyEvent", payload)
            elif event_type == "insertText":
                await self.cdp.send("Input.insertText", {"text": event.get("text") or ""})
        self.touch()

    # ------------------------------------------------------------ navigation

    async def navigate(self, url: str) -> None:
        target = normalize_target_url(url)
        try:
            await self.page.goto(target, wait_until="domcontentloaded", timeout=NAVIGATION_TIMEOUT_MS)
        except Exception as exc:
            raise InteractiveBrowserError(f"Could not open {target}: {exc}") from exc
        self.touch()

    async def go_back(self) -> None:
        try:
            await self.page.go_back(timeout=NAVIGATION_TIMEOUT_MS)
        except Exception:
            pass
        self.touch()

    async def go_forward(self) -> None:
        try:
            await self.page.go_forward(timeout=NAVIGATION_TIMEOUT_MS)
        except Exception:
            pass
        self.touch()

    async def reload(self) -> None:
        try:
            await self.page.reload(timeout=NAVIGATION_TIMEOUT_MS)
        except Exception:
            pass
        self.touch()

    async def close(self) -> None:
        self.closing = True
        if self._screenshot_task is not None:
            self._screenshot_task.cancel()
        for closer in (self.cdp, self.page, self.context):
            try:
                if closer is not None and hasattr(closer, "close"):
                    await closer.close()
            except Exception:
                pass
        self.cdp = self.page = self.context = None


class InteractiveBrowserManager:
    """Owns the Playwright instance and the single live session."""

    def __init__(self, profile_root: Path | None = None) -> None:
        self._playwright: Any = None
        self._sessions: dict[str, InteractiveBrowserSession] = {}
        self._lock = asyncio.Lock()
        self._reaper: asyncio.Task | None = None
        self._profile_root = profile_root
        self._availability: BrowserAvailability | None = None
        self._availability_checked_at = 0.0

    def set_profile_root(self, root: Path | None) -> None:
        self._profile_root = root

    # ------------------------------------------------------------- lifecycle

    async def start(self) -> None:
        """Called from the FastAPI lifespan hook, never lazily from a request.

        Playwright's async API binds to the loop that started it, so it has to be
        created on the serving loop.
        """
        if self._reaper is None:
            self._reaper = asyncio.ensure_future(self._reap_idle_sessions())

    async def shutdown(self) -> None:
        if self._reaper is not None:
            self._reaper.cancel()
            self._reaper = None
        for session in list(self._sessions.values()):
            await session.close()
        self._sessions.clear()
        if self._playwright is not None:
            try:
                await self._playwright.stop()
            except Exception:
                pass
            self._playwright = None

    async def _reap_idle_sessions(self) -> None:
        """A forgotten headful Chromium is invisible and accumulates."""
        while True:
            try:
                await asyncio.sleep(REAPER_INTERVAL_SECONDS)
                for session_id, session in list(self._sessions.items()):
                    if session.idle_seconds > IDLE_TIMEOUT_SECONDS:
                        logger.info("interactive browser: reaping idle session %s", session_id)
                        await session.close()
                        self._sessions.pop(session_id, None)
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("interactive browser: reaper failed")

    async def _ensure_playwright(self) -> Any:
        if self._playwright is not None:
            return self._playwright
        try:
            from playwright.async_api import async_playwright
        except ImportError as exc:
            raise InteractiveBrowserError(
                "Playwright is not installed. Run `./scripts/bootstrap_venv.sh`."
            ) from exc
        self._playwright = await async_playwright().start()
        return self._playwright

    # ---------------------------------------------------------- availability

    async def availability(self, *, refresh: bool = False) -> BrowserAvailability:
        if (
            self._availability is not None
            and not refresh
            and (time.monotonic() - self._availability_checked_at) < 60
        ):
            return self._availability

        display = _display_available()
        channel = _chrome_channel()
        result = BrowserAvailability(headless=not bool(display), channel=channel, display=display)
        try:
            await self._ensure_playwright()
            result.available = True
        except InteractiveBrowserError as exc:
            result.error = str(exc)
            result.guidance = "./scripts/bootstrap_venv.sh"
        except Exception as exc:
            result.error = f"{type(exc).__name__}: {exc}"
            result.guidance = ".venv/bin/python -m playwright install chromium"

        self._availability = result
        self._availability_checked_at = time.monotonic()
        return result

    # -------------------------------------------------------------- sessions

    def get(self, session_id: str) -> InteractiveBrowserSession:
        session = self._sessions.get(str(session_id or "").strip())
        if session is None:
            raise InteractiveBrowserError("That browser session is no longer open.")
        session.touch()
        return session

    def list_sessions(self) -> list[dict[str, Any]]:
        return [session.info() for session in self._sessions.values()]

    def _profile_dir(self) -> Path | None:
        if self._profile_root is None:
            return None
        return self._profile_root / PROFILE_DIR_NAME / "user_data"

    async def create(
        self,
        *,
        source_id: str,
        url: str = "",
        viewport_width: int = DEFAULT_VIEWPORT_WIDTH,
        viewport_height: int = DEFAULT_VIEWPORT_HEIGHT,
    ) -> InteractiveBrowserSession:
        async with self._lock:
            # Replace rather than refuse: the user clicking a second source
            # means they are done with the first.
            while len(self._sessions) >= MAX_SESSIONS:
                oldest_id = next(iter(self._sessions))
                await self._sessions[oldest_id].close()
                self._sessions.pop(oldest_id, None)

            playwright = await self._ensure_playwright()
            display = _display_available()
            channel = _chrome_channel()
            headless = not bool(display)

            context, headless = await self._launch_context(
                playwright,
                channel=channel,
                headless=headless,
                viewport_width=viewport_width,
                viewport_height=viewport_height,
            )

            session = InteractiveBrowserSession(
                session_id=uuid.uuid4().hex,
                source_id=str(source_id or "").strip(),
                viewport_width=viewport_width,
                viewport_height=viewport_height,
                context=context,
                headless=headless,
                channel=channel,
            )
            pages = context.pages
            session.page = pages[0] if pages else await context.new_page()
            await session.page.set_viewport_size(
                {"width": viewport_width, "height": viewport_height}
            )

            try:
                await session.start_screencast()
            except Exception:
                logger.exception("interactive browser: screencast unavailable")
                session.frame_mode = "screenshot_poll"

            if url:
                try:
                    await session.navigate(url)
                except InteractiveBrowserError:
                    # A blocked landing page is exactly what the user is here to
                    # fix; keep the session open so they can work on it.
                    logger.info("interactive browser: initial navigation failed for %s", url)

            if session.frame_mode == "screencast":
                asyncio.ensure_future(session.ensure_frames_flowing())
            else:
                session._screenshot_task = asyncio.ensure_future(session._screenshot_loop())

            self._sessions[session.session_id] = session
            return session

    async def _launch_context(
        self,
        playwright: Any,
        *,
        channel: str,
        headless: bool,
        viewport_width: int,
        viewport_height: int,
    ) -> tuple[Any, bool]:
        """Launch a persistent context, retrying headless if there is no display.

        The automation flags are dropped deliberately: `--enable-automation` sets
        `navigator.webdriver`, which is the first thing a bot wall checks. This
        does not defeat serious fingerprinting — CDP itself is detectable — but it
        is the difference between being offered a checkbox and being refused.
        """
        profile_dir = self._profile_dir()
        args = [
            "--disable-blink-features=AutomationControlled",
            "--no-first-run",
            "--no-default-browser-check",
        ]
        options: dict[str, Any] = {
            "headless": headless,
            "args": args,
            "ignore_default_args": ["--enable-automation"],
            "viewport": {"width": viewport_width, "height": viewport_height},
            "locale": "en-US",
        }
        if channel:
            options["channel"] = channel

        if profile_dir is not None:
            profile_dir.mkdir(parents=True, exist_ok=True)
            launcher = lambda opts: playwright.chromium.launch_persistent_context(
                str(profile_dir), **opts
            )
        else:
            async def launcher(opts):
                browser = await playwright.chromium.launch(
                    headless=opts["headless"],
                    args=opts["args"],
                    ignore_default_args=opts["ignore_default_args"],
                    **({"channel": opts["channel"]} if "channel" in opts else {}),
                )
                return await browser.new_context(
                    viewport=opts["viewport"], locale=opts["locale"]
                )

        try:
            return await launcher(options), headless
        except Exception as first_error:
            if headless:
                raise InteractiveBrowserError(
                    f"Could not start a browser: {first_error}. "
                    "Run `.venv/bin/python -m playwright install chromium`."
                ) from first_error
            # A display was advertised but the launch failed anyway; headless is
            # more detectable but still lets the user see and click the page.
            logger.info("interactive browser: headful launch failed, retrying headless")
            options["headless"] = True
            try:
                return await launcher(options), True
            except Exception as second_error:
                raise InteractiveBrowserError(
                    f"Could not start a browser: {second_error}. "
                    "Run `.venv/bin/python -m playwright install chromium`."
                ) from second_error

    async def close(self, session_id: str) -> None:
        session = self._sessions.pop(str(session_id or "").strip(), None)
        if session is not None:
            await session.close()

    async def save_profile_state(self, session: InteractiveBrowserSession) -> None:
        """Persist cookies and the real user agent after a successful capture.

        Later automated fetches reuse both. The user agent matters as much as the
        cookies: `cf_clearance` is bound to IP *and* user agent, so replaying the
        cookie under the downloader's own agent string voids it.
        """
        if self._profile_root is None or session.context is None:
            return
        profile_root = self._profile_root / PROFILE_DIR_NAME
        profile_root.mkdir(parents=True, exist_ok=True)
        try:
            await session.context.storage_state(path=str(profile_root / STORAGE_STATE_FILE))
        except Exception:
            logger.exception("interactive browser: could not save storage state")
        try:
            user_agent = await session.page.evaluate("navigator.userAgent")
        except Exception:
            user_agent = ""
        if user_agent:
            import json

            (profile_root / PROFILE_META_FILE).write_text(
                json.dumps({"user_agent": user_agent}, indent=2), encoding="utf-8"
            )
