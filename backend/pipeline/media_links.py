"""Discovery of embedded video links inside downloaded source pages.

A downloaded article often embeds a video that is the real primary source. This
module finds those links so the downloader can promote them to source rows of
their own, and provides the predicate the downloader uses to make sure it never
runs discovery against a video page itself.
"""

from __future__ import annotations

import html
import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urljoin, urlsplit

# Hosts whose pages are video destinations rather than articles. A URL on one of
# these never has link discovery run against it -- see `is_media_platform_url`.
MEDIA_PLATFORM_HOSTS = {
    "youtube.com",
    "www.youtube.com",
    "m.youtube.com",
    "music.youtube.com",
    "youtu.be",
    "www.youtu.be",
    "youtube-nocookie.com",
    "www.youtube-nocookie.com",
}

# YouTube ids are exactly 11 id-safe characters. Anchoring the match and
# requiring a non-id character after it lets us tolerate malformed markup that
# glues query junk onto the path, without truncating a longer (invalid) segment
# into something that looks like a valid id.
YOUTUBE_VIDEO_ID_PATTERN = re.compile(r"^([A-Za-z0-9_-]{11})(?![A-Za-z0-9_-])")


def _leading_video_id(segment: str) -> str:
    match = YOUTUBE_VIDEO_ID_PATTERN.match((segment or "").strip())
    return match.group(1) if match else ""

# Paths that carry the video id as the last path segment.
_PATH_ID_PREFIXES = ("/embed/", "/shorts/", "/live/", "/v/")

# Attribute values and bare URLs that may contain a YouTube link.
_CANDIDATE_PATTERN = re.compile(
    r"""(?:href|src|content|data-src|data-video-url)\s*=\s*["']([^"']+)["']""",
    re.IGNORECASE,
)
_BARE_URL_PATTERN = re.compile(
    r"""https?://(?:[\w.-]*\.)?(?:youtube(?:-nocookie)?\.com|youtu\.be)/[^\s"'<>)\]]+""",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class DiscoveredMedia:
    """One video found on a page, normalized to its canonical watch URL."""

    video_id: str
    url: str
    platform: str = "youtube"

    @property
    def dedupe_key(self) -> str:
        return youtube_dedupe_key(self.video_id)


def _host(url: str) -> str:
    try:
        return urlsplit(url).netloc.lower().split("@")[-1].split(":")[0]
    except Exception:
        return ""


def is_media_platform_url(url: str) -> bool:
    """True when the URL points at a video platform rather than an article.

    This is the recursion guard: the downloader routes these URLs to the video
    handler instead of fetching them as HTML, so link discovery -- which only
    runs on HTML responses -- can never see a video page and re-extract from it.
    """
    host = _host(url or "")
    if not host:
        return False
    return host in MEDIA_PLATFORM_HOSTS


def youtube_video_id(url: str) -> str:
    """Extract the 11-character video id from any YouTube URL shape."""
    candidate = html.unescape((url or "").strip())
    if not candidate:
        return ""
    if candidate.startswith("//"):
        candidate = f"https:{candidate}"

    try:
        parts = urlsplit(candidate)
    except Exception:
        return ""

    host = parts.netloc.lower().split("@")[-1].split(":")[0]
    if host not in MEDIA_PLATFORM_HOSTS:
        return ""

    path = parts.path or ""

    if host in {"youtu.be", "www.youtu.be"}:
        return _leading_video_id(path.strip("/").split("/")[0])

    if path.rstrip("/").endswith("/watch") or path.rstrip("/") == "/watch":
        values = parse_qs(parts.query).get("v") or []
        return _leading_video_id(values[0]) if values else ""

    for prefix in _PATH_ID_PREFIXES:
        if path.startswith(prefix):
            return _leading_video_id(path[len(prefix) :].strip("/").split("/")[0])

    return ""


def youtube_dedupe_key(video_id: str) -> str:
    """Stable key so youtu.be, /watch?v=, /embed/ and /shorts/ collapse to one row."""
    normalized = (video_id or "").strip()
    return f"youtube:{normalized}" if normalized else ""


def youtube_watch_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}" if video_id else ""


def extract_youtube_urls(
    html_text: str,
    base_url: str = "",
    limit: int | None = None,
) -> list[DiscoveredMedia]:
    """Find every distinct YouTube video referenced by a page.

    Scans link/embed/meta attributes and bare URLs in the markup. Results are
    deduplicated by video id and returned in document order, so the same video
    embedded twice -- or linked once and embedded once -- yields one entry.
    """
    if not html_text:
        return []

    found: list[DiscoveredMedia] = []
    seen: set[str] = set()

    def consider(raw_value: str) -> None:
        if limit is not None and len(found) >= limit:
            return
        candidate = html.unescape((raw_value or "").strip())
        if not candidate:
            return
        if candidate.startswith("//"):
            candidate = f"https:{candidate}"
        elif base_url and not candidate.lower().startswith(("http://", "https://")):
            try:
                candidate = urljoin(base_url, candidate)
            except Exception:
                return

        video_id = youtube_video_id(candidate)
        if not video_id or video_id in seen:
            return
        seen.add(video_id)
        found.append(DiscoveredMedia(video_id=video_id, url=youtube_watch_url(video_id)))

    for match in _CANDIDATE_PATTERN.finditer(html_text):
        consider(match.group(1))
    for match in _BARE_URL_PATTERN.finditer(html_text):
        consider(match.group(0))

    return found


def merge_discovered_media(
    *groups: list[DiscoveredMedia],
    limit: int | None = None,
) -> list[DiscoveredMedia]:
    """Union several extraction passes (raw HTML and rendered DOM) by video id."""
    merged: list[DiscoveredMedia] = []
    seen: set[str] = set()
    for group in groups:
        for item in group or []:
            if item.video_id in seen:
                continue
            seen.add(item.video_id)
            merged.append(item)
            if limit is not None and len(merged) >= limit:
                return merged
    return merged
