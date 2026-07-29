"""Fetch transcript, metadata and media files for a discovered video source.

Wraps yt-dlp behind a small result object so the download orchestrator does not
need to know anything about extractor internals. yt-dlp is imported lazily and
its absence degrades to a runtime note, matching how trafilatura and Playwright
are treated elsewhere in the pipeline.
"""

from __future__ import annotations

import json
import re
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

try:  # pragma: no cover - optional runtime dependency
    import yt_dlp
except Exception:  # pragma: no cover - optional runtime dependency
    yt_dlp = None

TRANSCRIPT_LANGUAGES = ["en", "en-US", "en-GB"]
DEFAULT_MEDIA_TIMEOUT_SECONDS = 600
# How many recently emitted caption lines to compare against when collapsing the
# rolling repeats in auto-generated subtitles.
CAPTION_DEDUPE_LOOKBACK = 4


@dataclass
class VideoAssets:
    """Everything retrieved for one video, as raw bytes plus parsed metadata."""

    video_id: str = ""
    title: str = ""
    channel: str = ""
    upload_date: str = ""  # YYYY-MM-DD
    duration_seconds: int = 0
    description: str = ""
    webpage_url: str = ""
    transcript_text: str = ""
    subtitle_bytes: bytes = b""
    subtitle_ext: str = ""
    video_bytes: bytes = b""
    video_ext: str = ""
    audio_bytes: bytes = b""
    audio_ext: str = ""
    thumbnail_bytes: bytes = b""
    thumbnail_ext: str = ""
    info_json: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


def yt_dlp_available() -> bool:
    return yt_dlp is not None


def ffmpeg_available() -> bool:
    return shutil.which("ffmpeg") is not None


def _format_upload_date(raw: str) -> str:
    value = str(raw or "").strip()
    if len(value) == 8 and value.isdigit():
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    return value


def vtt_to_transcript(subtitle_text: str) -> str:
    """Flatten WebVTT/SRT captions into readable prose.

    YouTube's auto-captions scroll: each cue repeats the tail of the previous one
    so the text appears to roll upward. Deduplicating only against the immediately
    preceding line misses that, because the repeats are interleaved. Comparing
    against a short lookback window collapses the rolling repeats while still
    allowing a genuinely repeated phrase later in the talk.
    """
    if not subtitle_text:
        return ""

    lines: list[str] = []
    for raw_line in subtitle_text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.upper().startswith("WEBVTT"):
            continue
        if line.startswith(("NOTE", "Kind:", "Language:")):
            continue
        if "-->" in line:
            continue
        if line.isdigit():
            continue
        # Strip caption markup such as <c>, <00:00:01.000> and speaker tags.
        line = re.sub(r"<[^>]+>", "", line).strip()
        if not line:
            continue
        if line in lines[-CAPTION_DEDUPE_LOOKBACK:]:
            continue
        lines.append(line)

    text = " ".join(lines)
    return re.sub(r"\s{2,}", " ", text).strip()


def _read_first(directory: Path, patterns: list[str]) -> tuple[bytes, str]:
    for pattern in patterns:
        for candidate in sorted(directory.glob(pattern)):
            if candidate.is_file() and candidate.stat().st_size > 0:
                return candidate.read_bytes(), candidate.suffix.lower()
    return b"", ""


def _base_options(workdir: Path) -> dict:
    return {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "outtmpl": str(workdir / "media.%(ext)s"),
        "socket_timeout": 30,
        "retries": 2,
        "ignoreerrors": False,
    }


def fetch_video_assets(
    url: str,
    *,
    want_transcript: bool = True,
    want_video: bool = False,
    want_audio: bool = True,
    want_thumbnail: bool = True,
) -> VideoAssets:
    """Download the requested artifacts for `url`.

    Each artifact is fetched in its own yt-dlp pass so that one failing download
    (for example an age-gated video stream) still leaves the transcript and
    metadata intact. Failures are recorded on `assets.errors` rather than raised.
    """
    assets = VideoAssets(webpage_url=url)
    if yt_dlp is None:
        assets.errors.append("yt_dlp_not_installed")
        return assets

    with tempfile.TemporaryDirectory() as tmpdir:
        workdir = Path(tmpdir)

        # Pass 1: metadata plus subtitles.
        info: dict = {}
        options = _base_options(workdir)
        options["skip_download"] = True
        if want_transcript:
            options.update(
                {
                    "writesubtitles": True,
                    "writeautomaticsub": True,
                    "subtitleslangs": TRANSCRIPT_LANGUAGES,
                    "subtitlesformat": "vtt",
                }
            )
        try:
            with yt_dlp.YoutubeDL(options) as ydl:
                info = ydl.extract_info(url, download=want_transcript) or {}
        except Exception as exc:
            assets.errors.append(f"metadata_failed: {type(exc).__name__}")
            return assets

        assets.info_json = _pruned_info(info)
        assets.video_id = str(info.get("id") or "")
        assets.title = str(info.get("title") or "")
        assets.channel = str(info.get("uploader") or info.get("channel") or "")
        assets.upload_date = _format_upload_date(info.get("upload_date") or "")
        assets.description = str(info.get("description") or "")
        assets.webpage_url = str(info.get("webpage_url") or url)
        try:
            assets.duration_seconds = int(float(info.get("duration") or 0))
        except (TypeError, ValueError):
            assets.duration_seconds = 0

        if want_transcript:
            subtitle_bytes, subtitle_ext = _read_first(workdir, ["*.vtt", "*.srt"])
            if subtitle_bytes:
                assets.subtitle_bytes = subtitle_bytes
                assets.subtitle_ext = subtitle_ext
                assets.transcript_text = vtt_to_transcript(
                    subtitle_bytes.decode("utf-8", errors="replace")
                )
            else:
                assets.errors.append("transcript_unavailable")

        if want_audio:
            _download_stream(
                url,
                workdir / "audio",
                fmt="bestaudio[ext=m4a]/bestaudio/best",
                assets=assets,
                kind="audio",
            )

        if want_video:
            # Merging separate video and audio streams needs ffmpeg; without it,
            # fall back to the best single progressive stream.
            fmt = (
                "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best"
                if ffmpeg_available()
                else "best[ext=mp4]/best"
            )
            _download_stream(url, workdir / "video", fmt=fmt, assets=assets, kind="video")

        if want_thumbnail:
            _download_thumbnail(url, workdir / "thumb", assets)

    return assets


def _pruned_info(info: dict) -> dict:
    """Keep the metadata worth persisting; drop yt-dlp's format/url firehose."""
    keys = (
        "id",
        "title",
        "uploader",
        "uploader_id",
        "channel",
        "channel_id",
        "channel_url",
        "upload_date",
        "duration",
        "description",
        "webpage_url",
        "view_count",
        "like_count",
        "tags",
        "categories",
        "language",
        "license",
    )
    return {key: info.get(key) for key in keys if key in info}


def _download_stream(
    url: str,
    target_dir: Path,
    *,
    fmt: str,
    assets: VideoAssets,
    kind: str,
) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    options = _base_options(target_dir)
    options["format"] = fmt
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            ydl.download([url])
    except Exception as exc:
        assets.errors.append(f"{kind}_download_failed: {type(exc).__name__}")
        return

    data, ext = _read_first(target_dir, ["media.*", "*"])
    if not data:
        assets.errors.append(f"{kind}_download_failed: no_output")
        return
    if kind == "audio":
        assets.audio_bytes, assets.audio_ext = data, ext
    else:
        assets.video_bytes, assets.video_ext = data, ext


def _download_thumbnail(url: str, target_dir: Path, assets: VideoAssets) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    options = _base_options(target_dir)
    options.update({"skip_download": True, "writethumbnail": True})
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            ydl.extract_info(url, download=True)
    except Exception as exc:
        assets.errors.append(f"thumbnail_download_failed: {type(exc).__name__}")
        return

    data, ext = _read_first(target_dir, ["*.jpg", "*.jpeg", "*.png", "*.webp"])
    if data:
        assets.thumbnail_bytes, assets.thumbnail_ext = data, ext


def build_transcript_markdown(assets: VideoAssets) -> str:
    """Render video metadata and transcript as the source's markdown body.

    Writing this to the row's `markdown_file` lets the existing catalog, summary,
    title and rating phases operate on video sources with no special-casing.
    """
    parts: list[str] = []
    if assets.title:
        parts.append(f"# {assets.title}")
        parts.append("")

    details: list[str] = []
    if assets.channel:
        details.append(f"- **Channel:** {assets.channel}")
    if assets.upload_date:
        details.append(f"- **Published:** {assets.upload_date}")
    if assets.duration_seconds:
        minutes, seconds = divmod(assets.duration_seconds, 60)
        details.append(f"- **Duration:** {minutes}m {seconds}s")
    if assets.webpage_url:
        details.append(f"- **URL:** {assets.webpage_url}")
    if details:
        parts.extend(details)
        parts.append("")

    if assets.description.strip():
        parts.append("## Description")
        parts.append("")
        parts.append(assets.description.strip())
        parts.append("")

    if assets.transcript_text:
        parts.append("## Transcript")
        parts.append("")
        parts.append(assets.transcript_text)
        parts.append("")

    return "\n".join(parts).strip()


def build_info_json(assets: VideoAssets) -> str:
    return json.dumps(assets.info_json, indent=2, ensure_ascii=False, sort_keys=True)
