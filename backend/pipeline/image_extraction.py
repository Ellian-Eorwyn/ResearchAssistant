"""Deterministic extraction of inline image references from fetched HTML.

Mirrors the pure, network-free style of ``media_links.py``: it finds ``<img>``
and related image references in a page's HTML and normalizes them to absolute
URLs, but it never downloads anything. Byte download, decoding, hashing and the
vision calls live in the source downloader, which owns the HTTP client and the
LLM capabilities. Keeping discovery pure keeps it unit-testable without a
network.
"""

from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass
from urllib.parse import unquote_to_bytes, urljoin, urlsplit

from lxml import html as lxml_html

# Attribute names that carry an image URL on <img>, including common lazy-load
# variants. Checked in order; the first non-empty one wins.
_IMG_URL_ATTRS = ("src", "data-src", "data-lazy-src", "data-original", "data-lazy")

# Extensions we accept when sniffing bare URLs in the regex fallback.
_IMG_EXT_RE = re.compile(
    r"\.(png|jpe?g|gif|webp|bmp|svg|avif|tiff?)(?:[?#]|$)", re.IGNORECASE
)

# Filename fragments that mark obvious site chrome; used by the cheap pre-filter.
_CHROME_HINT_RE = re.compile(
    r"(?:sprite|logo|favicon|pixel|spacer|1x1|placeholder)", re.IGNORECASE
)

_MIME_BY_EXT = {
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "gif": "image/gif",
    "webp": "image/webp",
    "bmp": "image/bmp",
    "svg": "image/svg+xml",
    "avif": "image/avif",
    "tif": "image/tiff",
    "tiff": "image/tiff",
}
_EXT_BY_MIME = {
    "image/png": ".png",
    "image/jpeg": ".jpg",
    "image/gif": ".gif",
    "image/webp": ".webp",
    "image/bmp": ".bmp",
    "image/svg+xml": ".svg",
    "image/avif": ".avif",
    "image/tiff": ".tiff",
}


@dataclass(frozen=True)
class DiscoveredImage:
    """One image reference found on a page, normalized to an absolute URL."""

    original_url: str  # as written in the HTML (may be relative or a data: URI)
    resolved_url: str  # absolute URL; empty for data: URIs
    alt: str = ""
    title: str = ""
    width_attr: int | None = None
    height_attr: int | None = None
    is_data_uri: bool = False
    origin: str = "img"  # img | picture | meta


def _to_int(value: object) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def pick_srcset_url(srcset: str) -> str:
    """Return the highest-resolution candidate URL from a ``srcset`` attribute.

    Handles both ``w`` (width) and ``x`` (pixel-density) descriptors; a bare
    candidate with no descriptor scores 0 and only wins if it is the only one.
    """
    best_url = ""
    best_score = -1.0
    for part in (srcset or "").split(","):
        piece = part.strip()
        if not piece:
            continue
        bits = piece.split()
        url = bits[0]
        score = 0.0
        if len(bits) > 1:
            match = re.match(r"([0-9.]+)(w|x)", bits[1].strip().lower())
            if match:
                score = float(match.group(1))
        if score >= best_score:
            best_score = score
            best_url = url
    return best_url


def resolve_url(base_url: str, url: str) -> str:
    """Resolve a possibly-relative image URL against the page's base URL.

    Returns "" for data: URIs (they carry their bytes inline, not a location)
    and handles protocol-relative ``//host/path`` references.
    """
    url = (url or "").strip()
    if not url or url.startswith("data:"):
        return ""
    if url.startswith("//"):
        scheme = urlsplit(base_url).scheme or "https"
        return f"{scheme}:{url}"
    if base_url:
        return urljoin(base_url, url)
    return url


def parse_data_uri(src: str) -> tuple[bytes, str] | None:
    """Decode a ``data:image/...`` URI into ``(bytes, mime)``; None if not one."""
    if not src or not src.startswith("data:"):
        return None
    try:
        header, data = src.split(",", 1)
    except ValueError:
        return None
    meta = header[len("data:") :]
    mime = meta.split(";")[0] or "application/octet-stream"
    try:
        if "base64" in meta:
            raw = base64.b64decode(data, validate=False)
        else:
            raw = unquote_to_bytes(data)
    except Exception:
        return None
    return raw, mime


def sniff_mime(image_bytes: bytes) -> str:
    """Best-effort magic-number sniff. Falls back to octet-stream."""
    head = image_bytes[:16]
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if head[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if head[:4] == b"RIFF" and image_bytes[8:12] == b"WEBP":
        return "image/webp"
    if head[:2] == b"BM":
        return "image/bmp"
    stripped = image_bytes[:64].lstrip().lower()
    if stripped.startswith(b"<?xml") or stripped.startswith(b"<svg"):
        return "image/svg+xml"
    return "application/octet-stream"


def mime_from_url(url: str) -> str:
    """Guess an image MIME from a URL's file extension; "" if unknown."""
    path = urlsplit(url or "").path.lower()
    match = re.search(r"\.([a-z0-9]+)$", path)
    if match:
        return _MIME_BY_EXT.get(match.group(1), "")
    return ""


def extension_for_mime(mime: str) -> str:
    return _EXT_BY_MIME.get((mime or "").split(";")[0].strip().lower(), ".img")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def passes_dimension_prefilter(
    width: object, height: object, min_edge: int
) -> bool:
    """Deterministic tiny-icon / spacer skip from *declared* dimensions.

    Unknown dimensions pass — only a real decode can measure them, so the
    caller re-checks true pixel size after download.
    """
    w = _to_int(width)
    h = _to_int(height)
    if w is not None and w < min_edge:
        return False
    if h is not None and h < min_edge:
        return False
    return True


def looks_like_chrome_url(url: str) -> bool:
    """Cheap filename-based heuristic for obvious UI chrome (logo/sprite/pixel)."""
    return bool(_CHROME_HINT_RE.search(urlsplit(url or "").path))


def extract_image_refs(
    html_text: str, base_url: str = "", limit: int | None = None
) -> list[DiscoveredImage]:
    """Find image references in a page's HTML, normalized to absolute URLs.

    Collects ``<img>`` (including lazy-load attrs and ``srcset``),
    ``<picture><source srcset>``, and ``og:image`` / ``twitter:image`` meta
    tags. Deduplicates by resolved URL (or by the raw ``data:`` payload) in
    document order. Falls back to a regex sweep when the tree parse yields
    nothing (malformed markup).
    """
    results: list[DiscoveredImage] = []
    seen: set[str] = set()

    def _add(image: DiscoveredImage) -> bool:
        key = image.resolved_url or image.original_url
        if not key or key in seen:
            return False
        seen.add(key)
        results.append(image)
        return True

    tree = None
    if html_text and html_text.strip():
        try:
            tree = lxml_html.fromstring(html_text)
        except Exception:
            tree = None

    if tree is not None:
        for el in tree.iter():
            tag = el.tag.lower() if isinstance(el.tag, str) else ""
            if tag == "img":
                url = ""
                for attr in _IMG_URL_ATTRS:
                    val = el.get(attr)
                    if val and val.strip():
                        url = val.strip()
                        break
                if not url:
                    srcset = el.get("srcset") or el.get("data-srcset")
                    if srcset:
                        url = pick_srcset_url(srcset)
                if not url:
                    continue
                _add(
                    DiscoveredImage(
                        original_url=url,
                        resolved_url=resolve_url(base_url, url),
                        alt=(el.get("alt") or "").strip(),
                        title=(el.get("title") or "").strip(),
                        width_attr=_to_int(el.get("width")),
                        height_attr=_to_int(el.get("height")),
                        is_data_uri=url.startswith("data:"),
                        origin="img",
                    )
                )
            elif tag == "source":
                srcset = el.get("srcset") or el.get("data-srcset")
                if not srcset:
                    continue
                type_attr = (el.get("type") or "").lower()
                if type_attr and not type_attr.startswith("image/"):
                    continue
                url = pick_srcset_url(srcset)
                if url:
                    _add(
                        DiscoveredImage(
                            original_url=url,
                            resolved_url=resolve_url(base_url, url),
                            is_data_uri=url.startswith("data:"),
                            origin="picture",
                        )
                    )
            elif tag == "meta":
                prop = (el.get("property") or el.get("name") or "").lower()
                if prop in (
                    "og:image",
                    "og:image:url",
                    "og:image:secure_url",
                    "twitter:image",
                    "twitter:image:src",
                ):
                    url = (el.get("content") or "").strip()
                    if url:
                        _add(
                            DiscoveredImage(
                                original_url=url,
                                resolved_url=resolve_url(base_url, url),
                                is_data_uri=url.startswith("data:"),
                                origin="meta",
                            )
                        )
            if limit is not None and len(results) >= limit:
                break

    if not results and html_text:
        for match in re.finditer(
            r"""(?:src|data-src)\s*=\s*["']([^"']+)["']""", html_text, re.IGNORECASE
        ):
            url = match.group(1).strip()
            if not url:
                continue
            if not url.startswith("data:") and not _IMG_EXT_RE.search(url):
                continue
            _add(
                DiscoveredImage(
                    original_url=url,
                    resolved_url=resolve_url(base_url, url),
                    is_data_uri=url.startswith("data:"),
                    origin="img",
                )
            )
            if limit is not None and len(results) >= limit:
                break

    if limit is not None:
        return results[:limit]
    return results
