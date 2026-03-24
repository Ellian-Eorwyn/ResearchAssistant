"""Download and capture citation source URLs into local output bundles."""

from __future__ import annotations

import asyncio
import csv
import html
import hashlib
import io
import json
import logging
import re
import shutil
import subprocess
import tempfile
import threading
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypeVar
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit

import httpx
import fitz  # PyMuPDF
from openpyxl import Workbook
from openpyxl.utils import get_column_letter

from backend.llm.client import UnifiedLLMClient
from backend.llm.prompts import (
    SOURCE_MARKDOWN_CLEANUP_SYSTEM,
    SOURCE_MARKDOWN_CLEANUP_USER,
    SOURCE_RATING_SYSTEM,
    SOURCE_RATING_USER,
    SOURCE_SUMMARY_SYSTEM,
    SOURCE_SUMMARY_USER,
)
from backend.models.settings import LLMBackendConfig
from backend.models.sources import (
    SOURCE_MANIFEST_COLUMNS,
    SourceDownloadStatus,
    SourceOutputOptions,
    SourceOutputSummary,
    SourceItemStatus,
    SourceManifestArtifact,
    SourceManifestRow,
)
from backend.storage.file_store import FileStore

try:
    import trafilatura
except ImportError:  # pragma: no cover - optional at runtime
    trafilatura = None


logger = logging.getLogger(__name__)
T = TypeVar("T")
_MUPDF_MESSAGE_LOCK = threading.Lock()

HTTP_TIMEOUT_SECONDS = 25.0
PLAYWRIGHT_TIMEOUT_MS = 20000
PLAYWRIGHT_VIEWPORT_WIDTH = 1366
PLAYWRIGHT_VIEWPORT_HEIGHT = 1200
MAX_VISUAL_CAPTURE_SEGMENTS = 40
MIN_MARKDOWN_SCORE = 180
MIN_FALLBACK_MARKDOWN_SCORE = 20
MAX_CLEANUP_SOURCE_CHARS = 24000
MAX_SUMMARY_SOURCE_CHARS = 20000

NOTE_RUNTIME_MISSING_TRAFILATURA = "runtime_missing_trafilatura"
NOTE_RUNTIME_MISSING_PLAYWRIGHT = "runtime_missing_playwright"
NOTE_RUNTIME_MISSING_TEXTUTIL = "runtime_missing_textutil"
NOTE_RUNTIME_MISSING_TESSERACT = "runtime_missing_tesseract"
NOTE_RUNTIME_MISSING_LLM_VISION = "runtime_missing_llm_vision"
NOTE_BLOCKED_REQUEST = "blocked_request"
NOTE_EXTRACTION_FAILURE = "extraction_failure"
NOTE_OCR_LOCAL_USED = "ocr_local_used"
NOTE_OCR_LLM_FALLBACK_USED = "ocr_llm_fallback_used"
NOTE_DOC_CONVERSION_FAILED = "doc_conversion_failed"
NOTE_LLM_CLEANUP_FAILED = "llm_cleanup_failed"
NOTE_LLM_CLEANUP_SKIPPED_LLM_NOT_CONFIGURED = "llm_cleanup_skipped_llm_not_configured"
NOTE_SUMMARY_GENERATION_FAILED = "summary_generation_failed"
NOTE_SUMMARY_SKIPPED_LLM_NOT_CONFIGURED = "summary_skipped_llm_not_configured"
NOTE_RATING_GENERATION_FAILED = "rating_generation_failed"
NOTE_RATING_SKIPPED_LLM_NOT_CONFIGURED = "rating_skipped_llm_not_configured"
NOTE_VISUAL_CAPTURE_FAILED = "visual_capture_failed"
NOTE_VISUAL_CAPTURE_SEGMENTED = "visual_capture_segmented"

INSTALL_BOOTSTRAP_COMMAND = "./scripts/bootstrap_venv.sh"
INSTALL_REQUIREMENTS_COMMAND = ".venv/bin/python -m pip install -r requirements.txt"
INSTALL_PLAYWRIGHT_BROWSER_COMMAND = ".venv/bin/python -m playwright install chromium"

PDF_NATIVE_PAGE_MIN_CHARS = 120
PDF_NATIVE_DOC_MIN_AVG_CHARS = 180
PDF_TEXT_ALPHA_MIN_RATIO = 0.55
PDF_OCR_MIN_CHARS = 80

DOCUMENT_CONTENT_TYPE_EXT = {
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.template": ".dotx",
    "application/rtf": ".rtf",
    "text/markdown": ".md",
    "text/x-markdown": ".md",
    "application/markdown": ".md",
    "text/plain": ".txt",
}

DOCUMENT_EXTENSIONS = {
    ".doc",
    ".docx",
    ".dot",
    ".dotx",
    ".md",
    ".txt",
    ".rtf",
}

BLOCKED_PAGE_PATTERNS = [
    re.compile(r"\bjust a moment\b", re.IGNORECASE),
    re.compile(r"checking (?:if|your browser)", re.IGNORECASE),
    re.compile(r"cf[-_ ]?(?:ray|challenge|chl|turnstile)", re.IGNORECASE),
    re.compile(r"\bcaptcha\b", re.IGNORECASE),
    re.compile(r"\baccess denied\b", re.IGNORECASE),
    re.compile(r"\brequest blocked\b", re.IGNORECASE),
    re.compile(r"\bsecurity check\b", re.IGNORECASE),
]

TRACKING_PARAM_EXACT = {"gclid", "fbclid", "msclkid"}
TRACKING_PARAM_PREFIXES = ("utm_",)


@dataclass
class SourceTarget:
    id: str
    source_document_name: str
    citation_number: str
    original_url: str


@dataclass
class RuntimeCapabilities:
    trafilatura_available: bool
    playwright_python_available: bool
    playwright_browser_available: bool
    textutil_available: bool
    tesseract_available: bool
    llm_vision_enabled: bool
    runtime_notes: list[str]
    runtime_guidance: list[dict[str, str]]


@contextmanager
def suppress_mupdf_messages() -> Iterator[None]:
    """Temporarily silence noisy MuPDF parser messages for malformed PDFs."""
    with _MUPDF_MESSAGE_LOCK:
        enabled = False
        prev_errors = True
        prev_warnings = True
        try:
            prev_errors = bool(fitz.TOOLS.mupdf_display_errors())
            prev_warnings = bool(fitz.TOOLS.mupdf_display_warnings())
            fitz.TOOLS.mupdf_display_errors(False)
            fitz.TOOLS.mupdf_display_warnings(False)
            enabled = True
        except Exception:
            enabled = False

        try:
            yield
        finally:
            if enabled:
                fitz.TOOLS.mupdf_display_errors(prev_errors)
                fitz.TOOLS.mupdf_display_warnings(prev_warnings)


def run_async_in_sync(
    async_fn: Callable[..., Awaitable[T]],
    *args: Any,
    **kwargs: Any,
) -> T:
    """
    Execute an async function from synchronous code in both loop/no-loop contexts.

    If a loop is already running on this thread, run the coroutine in a helper
    thread to avoid `asyncio.run()` reentrancy errors.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(async_fn(*args, **kwargs))

    result: dict[str, Any] = {}
    done = threading.Event()

    def _runner() -> None:
        try:
            result["value"] = asyncio.run(async_fn(*args, **kwargs))
        except BaseException as exc:  # noqa: BLE001
            result["error"] = exc
        finally:
            done.set()

    worker = threading.Thread(target=_runner, daemon=True)
    worker.start()
    done.wait()

    error = result.get("error")
    if isinstance(error, BaseException):
        raise error
    return result["value"]


class PlaywrightRenderer:
    """Lazily initialized Playwright HTML renderer."""

    def __init__(
        self,
        timeout_ms: int = PLAYWRIGHT_TIMEOUT_MS,
        startup_error: str = "",
    ):
        self.timeout_ms = timeout_ms
        self._playwright = None
        self._browser = None
        self._startup_error: str = startup_error

    def render(self, url: str) -> tuple[str, str]:
        if self._startup_error:
            return "", self._startup_error
        try:
            self._ensure_started()
        except Exception as exc:  # pragma: no cover - runtime dependency
            self._startup_error = _normalize_playwright_error(exc)
            return "", self._startup_error

        page = self._browser.new_page()
        try:
            page.goto(url, wait_until="networkidle", timeout=self.timeout_ms)
            return page.content(), ""
        except Exception as exc:  # pragma: no cover - runtime/browser failures
            return "", _normalize_playwright_error(exc)
        finally:
            page.close()

    def capture_visual_pdf(self, url: str) -> tuple[bytes, str, list[str]]:
        if self._startup_error:
            return b"", self._startup_error, []
        try:
            self._ensure_started()
        except Exception as exc:  # pragma: no cover - runtime dependency
            self._startup_error = _normalize_playwright_error(exc)
            return b"", self._startup_error, []

        page = self._browser.new_page(
            viewport={
                "width": PLAYWRIGHT_VIEWPORT_WIDTH,
                "height": PLAYWRIGHT_VIEWPORT_HEIGHT,
            }
        )
        notes: list[str] = []
        try:
            page.goto(url, wait_until="networkidle", timeout=self.timeout_ms)
            page.wait_for_timeout(200)
            return self._page_to_visual_pdf(page, notes), "", notes
        except Exception as exc:  # pragma: no cover - runtime/browser failures
            return b"", _normalize_playwright_error(exc), notes
        finally:
            page.close()

    def _page_to_visual_pdf(self, page, notes: list[str]) -> bytes:
        full_page_error: Exception | None = None
        try:
            full_page_png = page.screenshot(type="png", full_page=True)
            return png_images_to_pdf_bytes([full_page_png])
        except Exception as exc:
            full_page_error = exc

        segmented_pngs = self._capture_segmented_screenshots(page)
        if segmented_pngs:
            notes.append(NOTE_VISUAL_CAPTURE_SEGMENTED)
            return png_images_to_pdf_bytes(segmented_pngs)
        if full_page_error:
            raise full_page_error
        raise RuntimeError("visual_capture_failed: no screenshot data")

    def _capture_segmented_screenshots(self, page) -> list[bytes]:
        viewport = page.viewport_size or {}
        viewport_height = int(viewport.get("height") or PLAYWRIGHT_VIEWPORT_HEIGHT)
        if viewport_height <= 0:
            viewport_height = PLAYWRIGHT_VIEWPORT_HEIGHT

        scroll_height = page.evaluate(
            """() => Math.max(
                document.documentElement?.scrollHeight || 0,
                document.body?.scrollHeight || 0,
                document.documentElement?.offsetHeight || 0,
                document.body?.offsetHeight || 0,
                window.innerHeight || 0
            )"""
        )
        total_height = int(scroll_height or viewport_height)
        if total_height <= 0:
            total_height = viewport_height

        max_scroll = max(total_height - viewport_height, 0)
        captures: list[bytes] = []
        last_scroll = -1
        y = 0

        for _ in range(MAX_VISUAL_CAPTURE_SEGMENTS):
            scroll_y = min(y, max_scroll)
            if scroll_y == last_scroll and captures:
                break
            page.evaluate("(targetY) => window.scrollTo(0, targetY)", scroll_y)
            page.wait_for_timeout(80)
            captures.append(page.screenshot(type="png", full_page=False))
            last_scroll = scroll_y
            if scroll_y >= max_scroll:
                break
            y += viewport_height

        page.evaluate("() => window.scrollTo(0, 0)")
        return captures

    def _ensure_started(self) -> None:
        if self._browser is not None:
            return
        from playwright.sync_api import sync_playwright

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=True)

    def close(self) -> None:
        try:
            if self._browser is not None:
                self._browser.close()
        finally:
            if self._playwright is not None:
                self._playwright.stop()


class SourceDownloadOrchestrator:
    """Runs source download/capture pipeline for a completed extraction job."""

    def __init__(
        self,
        job_id: str,
        store: FileStore,
        rerun_failed_only: bool = False,
        use_llm: bool = False,
        llm_backend: LLMBackendConfig | None = None,
        research_purpose: str = "",
        fetch_delay: float = 2.0,
        run_download: bool = True,
        run_llm_cleanup: bool = False,
        run_llm_summary: bool = True,
        run_llm_rating: bool = False,
        force_redownload: bool = False,
        force_llm_cleanup: bool = False,
        force_summary: bool = False,
        force_rating: bool = False,
        project_profile_yaml: str = "",
        output_options: SourceOutputOptions | None = None,
    ):
        self.job_id = job_id
        self.store = store
        self.rerun_failed_only = rerun_failed_only
        self.use_llm = use_llm
        self.llm_backend = llm_backend or LLMBackendConfig()
        self.research_purpose = (research_purpose or "").strip()
        self.fetch_delay = max(1.0, min(10.0, fetch_delay))
        self.run_download = bool(run_download)
        self.run_llm_cleanup = bool(run_llm_cleanup)
        self.run_llm_summary = bool(run_llm_summary)
        self.run_llm_rating = bool(run_llm_rating)
        self.force_redownload = bool(force_redownload)
        self.force_llm_cleanup = bool(force_llm_cleanup)
        self.force_summary = bool(force_summary)
        self.force_rating = bool(force_rating)
        self.project_profile_yaml = (project_profile_yaml or "").strip()
        self.output_options = output_options or SourceOutputOptions()
        self.output_dir = self.store.get_sources_output_dir(job_id)
        self.logs_dir = self.output_dir / "logs"
        self.log_file = self.logs_dir / "source_download.jsonl"
        self.status: SourceDownloadStatus | None = None
        self._status_items: dict[str, SourceItemStatus] = {}
        self.duplicate_urls_removed = 0
        self._cancel_event = threading.Event()
        self.runtime_capabilities = RuntimeCapabilities(
            trafilatura_available=trafilatura is not None,
            playwright_python_available=False,
            playwright_browser_available=False,
            textutil_available=check_textutil_available(),
            tesseract_available=check_tesseract_available(),
            llm_vision_enabled=False,
            runtime_notes=[],
            runtime_guidance=[],
        )

    def request_cancel(self) -> None:
        self._cancel_event.set()

    def run(self) -> None:
        """Execute source downloads sequentially with per-URL fault isolation."""
        try:
            if not (self.run_download or self.run_llm_cleanup or self.run_llm_summary or self.run_llm_rating):
                raise RuntimeError("Select at least one phase to run")
            if self.run_download and not any(
                [
                    self.output_options.include_raw_file,
                    self.output_options.include_rendered_html,
                    self.output_options.include_rendered_pdf,
                    self.output_options.include_markdown,
                ]
            ):
                raise RuntimeError("Select at least one download output type")

            targets = self._build_targets()
            previous_rows = self._load_previous_rows()
            if not self.run_download and not previous_rows:
                raise RuntimeError(
                    "No existing downloaded sources found. Run download phase first."
                )
            rows_by_id = {r.id: r for r in previous_rows}
            self._ensure_output_dirs()
            self.runtime_capabilities = detect_runtime_capabilities(
                use_llm=self.use_llm,
                llm_backend=self.llm_backend,
            )
            self._initialize_status(
                targets=targets,
                runtime_capabilities=self.runtime_capabilities,
                existing_rows=previous_rows,
            )
            self._append_log(
                {
                    "event": "started",
                    "job_id": self.job_id,
                    "total_urls": len(targets),
                    "rerun_failed_only": self.rerun_failed_only,
                    "run_download": self.run_download,
                    "run_llm_cleanup": self.run_llm_cleanup,
                    "run_llm_summary": self.run_llm_summary,
                    "run_llm_rating": self.run_llm_rating,
                    "force_redownload": self.force_redownload,
                    "force_llm_cleanup": self.force_llm_cleanup,
                    "force_summary": self.force_summary,
                    "force_rating": self.force_rating,
                    "output_options": self.output_options.model_dump(mode="json"),
                    "runtime_notes": self.runtime_capabilities.runtime_notes,
                    "runtime_guidance": self.runtime_capabilities.runtime_guidance,
                    "timestamp": _utc_now_iso(),
                }
            )

            timeout = httpx.Timeout(HTTP_TIMEOUT_SECONDS, connect=12.0)
            renderer_startup_error = ""
            if not self.runtime_capabilities.playwright_browser_available:
                renderer_startup_error = (
                    f"playwright_not_installed: run `{INSTALL_PLAYWRIGHT_BROWSER_COMMAND}`"
                )
                if not self.runtime_capabilities.playwright_python_available:
                    renderer_startup_error = (
                        f"playwright_not_installed: run `{INSTALL_BOOTSTRAP_COMMAND}`"
                    )
            renderer = PlaywrightRenderer(startup_error=renderer_startup_error)

            cancelled = False
            try:
                with httpx.Client(
                    timeout=timeout,
                    follow_redirects=True,
                    headers={
                        "User-Agent": "ResearchAssistant/0.1 (+local source downloader)",
                        "Accept": "*/*",
                    },
                ) as client:
                    last_idx = len(targets) - 1
                    for idx, target in enumerate(targets):
                        if self._cancel_event.is_set():
                            cancelled = True
                            break
                        existing_row = rows_by_id.get(target.id)

                        if self.rerun_failed_only and self._should_skip_successful_target(
                            target, rows_by_id
                        ):
                            if self._should_run_llm_postprocess(existing_row):
                                self._mark_item_running(target)
                                row = self._process_existing_row(target, existing_row)
                                rows_by_id[row.id] = row
                                self._mark_item_finished(row)
                            else:
                                self._mark_item_skipped(target.id, existing_row)
                            continue

                        if self.run_download:
                            if self._should_skip_download_target(existing_row):
                                if self._should_run_llm_postprocess(existing_row):
                                    self._mark_item_running(target)
                                    row = self._process_existing_row(target, existing_row)
                                    rows_by_id[row.id] = row
                                    self._mark_item_finished(row)
                                else:
                                    self._mark_item_skipped(target.id, existing_row)
                                continue

                            self._mark_item_running(target)
                            row = self._process_target(
                                target=target,
                                client=client,
                                renderer=renderer,
                                existing_row=existing_row,
                            )
                            rows_by_id[row.id] = row
                            self._mark_item_finished(row)
                        else:
                            if existing_row is None:
                                self._mark_item_skipped(target.id, existing_row)
                                continue
                            self._mark_item_running(target)
                            row = self._process_existing_row(target, existing_row)
                            rows_by_id[row.id] = row
                            self._mark_item_finished(row)

                        if idx < last_idx and not self._cancel_event.is_set():
                            self._cancel_event.wait(self.fetch_delay)
            finally:
                renderer.close()

            final_rows = [rows_by_id[t.id] for t in targets if t.id in rows_by_id]
            counts = _count_fetch_outcomes(final_rows)
            artifact = SourceManifestArtifact(
                rows=final_rows,
                total_urls=len(final_rows),
                success_count=counts["success"],
                failed_count=counts["failed"],
                partial_count=counts["partial"],
            )
            output_summary = summarize_output_rows(final_rows)

            csv_content = build_manifest_csv(final_rows)
            xlsx_bytes = build_manifest_xlsx(final_rows)
            self.store.save_sources_manifest_csv(self.job_id, csv_content)
            self.store.save_sources_manifest_xlsx(self.job_id, xlsx_bytes)
            self.store.save_artifact(
                self.job_id, "06_sources_manifest", artifact.model_dump()
            )
            bundle_path = self.store.build_sources_bundle(self.job_id)

            if cancelled:
                self._mark_status_cancelled(artifact, bundle_path, output_summary)
                self._append_log(
                    {
                        "event": "cancelled",
                        "job_id": self.job_id,
                        "total_urls": artifact.total_urls,
                        "success_count": artifact.success_count,
                        "failed_count": artifact.failed_count,
                        "partial_count": artifact.partial_count,
                        "output_summary": output_summary.model_dump(mode="json"),
                        "bundle_file": str(bundle_path),
                        "timestamp": _utc_now_iso(),
                    }
                )
            else:
                self._mark_status_completed(artifact, bundle_path, output_summary)
                self._append_log(
                    {
                        "event": "completed",
                        "job_id": self.job_id,
                        "total_urls": artifact.total_urls,
                        "success_count": artifact.success_count,
                        "failed_count": artifact.failed_count,
                        "partial_count": artifact.partial_count,
                        "output_summary": output_summary.model_dump(mode="json"),
                        "bundle_file": str(bundle_path),
                        "timestamp": _utc_now_iso(),
                    }
                )
        except Exception as exc:
            logger.exception("Source download failed for job %s", self.job_id)
            self._mark_status_failed(f"{type(exc).__name__}: {exc}")
            self._append_log(
                {
                    "event": "failed",
                    "job_id": self.job_id,
                    "error": f"{type(exc).__name__}: {exc}",
                    "timestamp": _utc_now_iso(),
                }
            )

    def _build_targets(self) -> list[SourceTarget]:
        bib = self.store.load_artifact(self.job_id, "03_bibliography")
        if bib is None:
            raise RuntimeError("Bibliography artifact not found")

        ingestion = self.store.load_artifact(self.job_id, "01_ingestion") or {}
        docs = ingestion.get("documents", [])
        source_doc_name = docs[0].get("filename", "") if docs else ""

        deduped_targets: list[SourceTarget] = []
        seen_keys: set[str] = set()
        duplicate_count = 0
        row_num = 0
        for entry in bib.get("entries", []):
            url = clean_url_candidate(str(entry.get("url") or ""))
            doi = clean_url_candidate(str(entry.get("doi") or ""))
            if not url and doi:
                url = f"https://doi.org/{doi}"
            if not url:
                continue

            normalized_url, _ = normalize_url(url)
            url = normalized_url or url

            dedupe_key = dedupe_url_key(url)
            if dedupe_key in seen_keys:
                duplicate_count += 1
                continue
            seen_keys.add(dedupe_key)

            row_num += 1
            entry_source_doc = str(entry.get("source_document_name") or "").strip()
            deduped_targets.append(
                SourceTarget(
                    id=f"{row_num:06d}",
                    source_document_name=entry_source_doc or source_doc_name,
                    citation_number=str(entry.get("ref_number") or ""),
                    original_url=url,
                )
            )

        self.duplicate_urls_removed = duplicate_count

        if not deduped_targets:
            raise RuntimeError("No citation URLs found in bibliography entries")
        return deduped_targets

    def _load_previous_rows(self) -> list[SourceManifestRow]:
        previous = self.store.load_artifact(self.job_id, "06_sources_manifest")
        if not previous:
            return []
        rows: list[SourceManifestRow] = []
        for raw in previous.get("rows", []):
            try:
                rows.append(SourceManifestRow.model_validate(raw))
            except Exception:
                continue
        return rows

    def _initialize_status(
        self,
        targets: list[SourceTarget],
        runtime_capabilities: RuntimeCapabilities,
        existing_rows: list[SourceManifestRow],
    ) -> None:
        items = [
            SourceItemStatus(
                id=t.id,
                original_url=t.original_url,
                citation_number=t.citation_number,
            )
            for t in targets
        ]
        phase_bits: list[str] = []
        if self.run_download:
            phase_bits.append("download")
        if self.run_llm_cleanup:
            phase_bits.append("llm cleanup")
        if self.run_llm_summary:
            phase_bits.append("summary")
        phase_text = ", ".join(phase_bits) if phase_bits else "none"
        base_message = self._compose_status_message(f"Running phases: {phase_text}")
        self._status_items = {i.id: i for i in items}
        self.status = SourceDownloadStatus(
            job_id=self.job_id,
            state="running",
            total_urls=len(targets),
            processed_urls=0,
            success_count=0,
            failed_count=0,
            partial_count=0,
            skipped_count=0,
            duplicate_urls_removed=self.duplicate_urls_removed,
            started_at=_utc_now_iso(),
            completed_at=None,
            current_item_id="",
            current_url="",
            message=base_message,
            runtime_notes=runtime_capabilities.runtime_notes,
            runtime_guidance=runtime_capabilities.runtime_guidance,
            rerun_failed_only=self.rerun_failed_only,
            run_download=self.run_download,
            run_llm_cleanup=self.run_llm_cleanup,
            run_llm_summary=self.run_llm_summary,
            force_redownload=self.force_redownload,
            force_llm_cleanup=self.force_llm_cleanup,
            force_summary=self.force_summary,
            output_options=self.output_options,
            output_summary=summarize_output_rows(existing_rows),
            output_dir="output_run",
            manifest_csv="output_run/manifest.csv",
            manifest_xlsx="output_run/manifest.xlsx",
            bundle_file="output_run.zip",
            items=items,
        )
        self._save_status()

    def _ensure_output_dirs(self) -> None:
        for sub in ["originals", "rendered", "markdown", "summaries", "metadata", "logs"]:
            (self.output_dir / sub).mkdir(parents=True, exist_ok=True)

    def _compose_status_message(self, message: str) -> str:
        details: list[str] = []
        if self.duplicate_urls_removed > 0:
            details.append(f"removed {self.duplicate_urls_removed} duplicate URLs")
        if self.runtime_capabilities.runtime_notes:
            details.append(
                f"{len(self.runtime_capabilities.runtime_notes)} runtime warnings"
            )
        if not details:
            return message
        return f"{message} | {' | '.join(details)}"

    def _save_status(self) -> None:
        if not self.status:
            return
        self.store.save_source_status(self.job_id, self.status.model_dump(mode="json"))

    def _append_log(self, event: dict) -> None:
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        with self.log_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    def _should_skip_successful_target(
        self, target: SourceTarget, rows_by_id: dict[str, SourceManifestRow]
    ) -> bool:
        previous = rows_by_id.get(target.id)
        return bool(previous and previous.fetch_status == "success")

    def _should_skip_download_target(self, existing_row: SourceManifestRow | None) -> bool:
        if not self.run_download:
            return True
        if existing_row is None:
            return False
        if self.force_redownload:
            return False
        if existing_row.fetch_status == "failed":
            return False
        if existing_row.fetch_status in {"", "queued"}:
            return False

        checks: list[bool] = []
        if self.output_options.include_raw_file:
            checks.append(bool(existing_row.raw_file))
        if self.output_options.include_rendered_html:
            checks.append(bool(existing_row.rendered_file))
        if self.output_options.include_rendered_pdf:
            checks.append(bool(existing_row.rendered_pdf_file))
        if self.output_options.include_markdown:
            checks.append(bool(existing_row.markdown_file))

        if not checks:
            return True
        return all(checks)

    def _should_run_llm_postprocess(self, existing_row: SourceManifestRow | None) -> bool:
        if existing_row is None:
            return False
        return self.run_llm_cleanup or self.run_llm_summary

    def _process_existing_row(
        self,
        target: SourceTarget,
        existing_row: SourceManifestRow,
    ) -> SourceManifestRow:
        row = existing_row.model_copy(deep=True)
        row.id = target.id
        row.source_document_name = target.source_document_name or row.source_document_name
        row.citation_number = target.citation_number or row.citation_number
        row.original_url = target.original_url or row.original_url
        notes = parse_notes(row.notes)
        event: dict = {
            "event": "source_postprocess",
            "job_id": self.job_id,
            "id": row.id,
            "original_url": row.original_url,
            "started_at": _utc_now_iso(),
        }
        return self._finalize_row(
            row=row,
            notes=notes,
            event=event,
            update_fetched_at=False,
        )

    def _mark_item_skipped(
        self,
        item_id: str,
        existing_row: SourceManifestRow | None,
    ) -> None:
        if not self.status:
            return
        item = self._status_items.get(item_id)
        if item:
            item.status = "skipped"
            item.fetch_status = existing_row.fetch_status if existing_row else "skipped"
            item.llm_cleanup_status = (
                existing_row.llm_cleanup_status if existing_row else item.llm_cleanup_status
            )
            item.summary_status = (
                existing_row.summary_status if existing_row else item.summary_status
            )
            item.rating_status = (
                existing_row.rating_status if existing_row else item.rating_status
            )
        self.status.skipped_count += 1
        self.status.processed_urls += 1
        self.status.current_item_id = item_id
        self.status.current_url = item.original_url if item else ""
        self.status.message = self._compose_status_message("Skipped (already complete)")
        self._save_status()

    def _mark_item_running(self, target: SourceTarget) -> None:
        if not self.status:
            return
        item = self._status_items.get(target.id)
        if item:
            item.status = "running"
            item.error_message = ""
        self.status.current_item_id = target.id
        self.status.current_url = target.original_url
        self.status.message = self._compose_status_message(f"Processing {target.id}")
        self._save_status()

    def _mark_item_finished(self, row: SourceManifestRow) -> None:
        if not self.status:
            return
        item = self._status_items.get(row.id)
        if item:
            item.fetch_status = row.fetch_status
            item.llm_cleanup_status = row.llm_cleanup_status
            item.summary_status = row.summary_status
            item.rating_status = row.rating_status
            item.error_message = row.error_message
            if self.run_download and row.fetch_status == "failed":
                item.status = "failed"
            else:
                item.status = "completed"

        self.status.processed_urls += 1
        if row.fetch_status == "success":
            self.status.success_count += 1
        elif row.fetch_status == "partial":
            self.status.partial_count += 1
        else:
            self.status.failed_count += 1
        self.status.message = self._compose_status_message(
            f"Processed {self.status.processed_urls}/{self.status.total_urls} URLs"
        )
        self._save_status()

    def _mark_status_failed(self, error_message: str) -> None:
        if not self.status:
            self.status = SourceDownloadStatus(
                job_id=self.job_id,
                state="failed",
                message=self._compose_status_message(error_message),
                runtime_notes=self.runtime_capabilities.runtime_notes,
                runtime_guidance=self.runtime_capabilities.runtime_guidance,
                completed_at=_utc_now_iso(),
            )
        else:
            self.status.state = "failed"
            self.status.message = self._compose_status_message(error_message)
            self.status.completed_at = _utc_now_iso()
        self._save_status()

    def _mark_status_completed(
        self,
        artifact: SourceManifestArtifact,
        bundle_path: Path,
        output_summary: SourceOutputSummary,
    ) -> None:
        if not self.status:
            return
        self.status.state = "completed"
        self.status.processed_urls = artifact.total_urls
        self.status.success_count = artifact.success_count
        self.status.failed_count = artifact.failed_count
        self.status.partial_count = artifact.partial_count
        self.status.completed_at = _utc_now_iso()
        self.status.current_item_id = ""
        self.status.current_url = ""
        self.status.message = self._compose_status_message(
            f"Completed: {artifact.success_count} success, "
            f"{artifact.partial_count} partial, {artifact.failed_count} failed"
        )
        self.status.bundle_file = bundle_path.name
        self.status.output_summary = output_summary
        self._save_status()

    def _mark_status_cancelled(
        self,
        artifact: SourceManifestArtifact,
        bundle_path: Path,
        output_summary: SourceOutputSummary,
    ) -> None:
        if not self.status:
            return

        for item in self.status.items:
            if item.status in {"pending", "running"}:
                item.status = "cancelled"
                if not item.fetch_status:
                    item.fetch_status = "cancelled"

        self.status.state = "cancelled"
        self.status.completed_at = _utc_now_iso()
        self.status.current_item_id = ""
        self.status.current_url = ""
        self.status.bundle_file = bundle_path.name
        self.status.success_count = artifact.success_count
        self.status.failed_count = artifact.failed_count
        self.status.partial_count = artifact.partial_count
        self.status.processed_urls = min(self.status.processed_urls, self.status.total_urls)
        self.status.message = self._compose_status_message(
            f"Cancelled after {self.status.processed_urls}/{self.status.total_urls} URLs"
        )
        self.status.output_summary = output_summary
        self._save_status()

    def _process_target(
        self,
        target: SourceTarget,
        client: httpx.Client,
        renderer: PlaywrightRenderer,
        existing_row: SourceManifestRow | None = None,
    ) -> SourceManifestRow:
        if existing_row is not None:
            row = existing_row.model_copy(deep=True)
            row.id = target.id
            row.source_document_name = target.source_document_name or row.source_document_name
            row.citation_number = target.citation_number or row.citation_number
            row.original_url = target.original_url
        else:
            row = SourceManifestRow(
                id=target.id,
                source_document_name=target.source_document_name,
                citation_number=target.citation_number,
                original_url=target.original_url,
            )
        notes: list[str] = parse_notes(row.notes)
        event: dict = {
            "event": "source_processed",
            "job_id": self.job_id,
            "id": target.id,
            "original_url": target.original_url,
            "started_at": _utc_now_iso(),
        }

        normalized_url, url_error = normalize_url(target.original_url)
        if url_error:
            row.fetch_status = "failed"
            row.error_message = f"invalid_url: {url_error}"
            notes.append("invalid_url")
            return self._finalize_row(row, notes, event)

        try:
            response = client.get(normalized_url)
        except httpx.TimeoutException as exc:
            row.fetch_status = "failed"
            row.final_url = normalized_url
            row.error_message = f"timeout: {exc}"
            notes.append("timeout")
            return self._finalize_row(row, notes, event)
        except httpx.RequestError as exc:
            row.fetch_status = "failed"
            row.final_url = normalized_url
            row.error_message = f"network_failure: {type(exc).__name__}: {exc}"
            notes.append("network_failure")
            return self._finalize_row(row, notes, event)

        row.final_url = str(response.url)
        row.http_status = response.status_code
        row.content_type = response.headers.get("content-type", "")
        row.detected_type = detect_source_type(
            content_type=row.content_type,
            final_url=row.final_url,
            body=response.content,
        )
        row.fetch_method = "http"
        row.error_message = ""

        if row.detected_type == "pdf":
            self._handle_pdf_response(row, response, notes)
        elif row.detected_type == "html":
            self._handle_html_response(row, response, normalized_url, renderer, notes)
        elif row.detected_type == "document":
            self._handle_document_response(row, response, notes)
        else:
            self._handle_unsupported_response(row, response, notes)

        return self._finalize_row(row, notes, event)

    def _handle_pdf_response(
        self,
        row: SourceManifestRow,
        response: httpx.Response,
        notes: list[str],
    ) -> None:
        if self.output_options.include_raw_file:
            rel_path = Path("originals") / f"{row.id}_source.pdf"
            row.raw_file = rel_path.as_posix()
            self._write_binary(rel_path, response.content)
        row.sha256 = hashlib.sha256(response.content).hexdigest()
        if response.status_code >= 400:
            row.fetch_status = "failed"
            reason = classify_http_status(response.status_code)
            notes.append(reason)
            row.error_message = f"{reason}: http_status_{response.status_code}"
        else:
            if not self.output_options.include_markdown:
                row.fetch_status = "success"
                return

            markdown_text, extraction_method, conversion_notes = self._convert_pdf_to_markdown(
                response.content
            )
            notes.extend(conversion_notes)

            if markdown_text:
                markdown_rel = Path("markdown") / f"{row.id}_clean.md"
                self._write_text(markdown_rel, markdown_text)
                row.markdown_file = markdown_rel.as_posix()
                row.markdown_char_count = len(markdown_text)
                row.extraction_method = extraction_method
                row.fetch_status = "success"
            else:
                row.fetch_status = "partial"
                row.error_message = "extraction_failure: markdown not generated"
                notes.append(NOTE_EXTRACTION_FAILURE)

    def _handle_html_response(
        self,
        row: SourceManifestRow,
        response: httpx.Response,
        normalized_url: str,
        renderer: PlaywrightRenderer,
        notes: list[str],
    ) -> None:
        raw_html = decode_html(response)
        if raw_html:
            if self.output_options.include_raw_file:
                raw_rel = Path("originals") / f"{row.id}_source.html"
                self._write_text(raw_rel, raw_html)
                row.raw_file = raw_rel.as_posix()
            row.sha256 = hashlib.sha256(raw_html.encode("utf-8")).hexdigest()
            row.title = extract_title(raw_html)
            row.canonical_url = extract_canonical_url(raw_html)

        if self.output_options.include_rendered_pdf:
            self._cancel_event.wait(self.fetch_delay)
            rendered_pdf, rendered_pdf_error, rendered_pdf_notes = renderer.capture_visual_pdf(
                normalized_url
            )
            notes.extend(rendered_pdf_notes)
            if rendered_pdf:
                rendered_pdf_rel = Path("rendered") / f"{row.id}_rendered.pdf"
                self._write_binary(rendered_pdf_rel, rendered_pdf)
                row.rendered_pdf_file = rendered_pdf_rel.as_posix()
            elif rendered_pdf_error:
                notes.append(NOTE_VISUAL_CAPTURE_FAILED)
                notes.append(normalize_render_error_note(rendered_pdf_error))

        blocked_by_challenge = detect_blocked_page(
            html_text=raw_html,
            title=row.title,
            final_url=row.final_url,
        )

        raw_markdown = ""
        raw_used_fallback = False
        raw_score = 0
        if self.output_options.include_markdown:
            raw_markdown, raw_used_fallback, raw_notes = extract_markdown_with_fallback(
                raw_html,
                self.runtime_capabilities,
            )
            notes.extend(raw_notes)
            raw_score = markdown_score(raw_markdown)

        rendered_html = ""
        rendered_markdown = ""
        rendered_used_fallback = False
        rendered_score = 0

        should_render = False
        if not blocked_by_challenge:
            should_render = self.output_options.include_rendered_html or (
                self.output_options.include_markdown
                and (response.status_code >= 400 or raw_score < MIN_MARKDOWN_SCORE)
            )
        if should_render:
            self._cancel_event.wait(self.fetch_delay)
            rendered_html, render_error = renderer.render(normalized_url)
            if render_error:
                notes.append(normalize_render_error_note(render_error))
            elif rendered_html:
                if self.output_options.include_rendered_html:
                    rendered_rel = Path("rendered") / f"{row.id}_rendered.html"
                    self._write_text(rendered_rel, rendered_html)
                    row.rendered_file = rendered_rel.as_posix()

                if not row.title:
                    row.title = extract_title(rendered_html)
                if not row.canonical_url:
                    row.canonical_url = extract_canonical_url(rendered_html)

                if self.output_options.include_markdown:
                    rendered_markdown, rendered_used_fallback, rendered_notes = (
                        extract_markdown_with_fallback(
                            rendered_html,
                            self.runtime_capabilities,
                        )
                    )
                    notes.extend(rendered_notes)
                    rendered_score = markdown_score(rendered_markdown)

        if self.output_options.include_markdown:
            markdown_to_write = raw_markdown
            used_fallback = raw_used_fallback
            extraction_method = "raw_html" if raw_markdown else ""
            if rendered_score > raw_score:
                markdown_to_write = rendered_markdown
                used_fallback = rendered_used_fallback
                extraction_method = "rendered_html"
                row.fetch_method = "playwright"

            if markdown_to_write:
                markdown_rel = Path("markdown") / f"{row.id}_clean.md"
                self._write_text(markdown_rel, markdown_to_write)
                row.markdown_file = markdown_rel.as_posix()
                row.markdown_char_count = len(markdown_to_write)
                row.extraction_method = (
                    f"{extraction_method}_fallback" if used_fallback else extraction_method
                )

        if blocked_by_challenge:
            notes.append(NOTE_BLOCKED_REQUEST)
            row.fetch_status = "failed"
            row.error_message = blocked_error_message(response.status_code)
            return

        if response.status_code >= 400:
            reason = classify_http_status(response.status_code)
            notes.append(reason)
            row.fetch_status = "failed"
            row.error_message = f"{reason}: http_status_{response.status_code}"
            return

        if not self.output_options.include_markdown:
            row.fetch_status = "success"
            return

        if row.markdown_file:
            row.fetch_status = "success"
            return

        if row.raw_file:
            row.fetch_status = "partial"
            notes.append(NOTE_EXTRACTION_FAILURE)
            row.error_message = "extraction_failure: markdown not generated"
            return

        row.fetch_status = "failed"
        row.error_message = "network_failure: empty_html_response"

    def _handle_document_response(
        self,
        row: SourceManifestRow,
        response: httpx.Response,
        notes: list[str],
    ) -> None:
        extension = infer_document_extension(
            final_url=row.final_url,
            content_type=row.content_type,
            content_disposition=response.headers.get("content-disposition", ""),
        )
        if self.output_options.include_raw_file:
            rel_path = Path("originals") / f"{row.id}_source{extension}"
            row.raw_file = rel_path.as_posix()
            self._write_binary(rel_path, response.content)
        row.sha256 = hashlib.sha256(response.content).hexdigest()

        if response.status_code >= 400:
            row.fetch_status = "failed"
            reason = classify_http_status(response.status_code)
            notes.append(reason)
            row.error_message = f"{reason}: http_status_{response.status_code}"
            return

        notes.append("download_only")
        if not self.output_options.include_markdown:
            row.fetch_status = "success"
            return

        markdown_text, extraction_method, conversion_notes = self._convert_document_to_markdown(
            extension=extension,
            binary_content=response.content,
        )
        notes.extend(conversion_notes)

        if markdown_text:
            markdown_rel = Path("markdown") / f"{row.id}_clean.md"
            self._write_text(markdown_rel, markdown_text)
            row.markdown_file = markdown_rel.as_posix()
            row.markdown_char_count = len(markdown_text)
            row.extraction_method = extraction_method
            row.title = row.title or first_nonempty_line(markdown_text)[:500]
            row.fetch_status = "success"
            return

        if extension in {".txt", ".md", ".docx", ".doc", ".dot", ".dotx"}:
            row.fetch_status = "partial"
            row.error_message = "extraction_failure: markdown not generated"
            notes.append(NOTE_EXTRACTION_FAILURE)
            return

        row.fetch_status = "success"

    def _handle_unsupported_response(
        self,
        row: SourceManifestRow,
        response: httpx.Response,
        notes: list[str],
    ) -> None:
        if self.output_options.include_raw_file:
            rel_path = Path("originals") / f"{row.id}_source.bin"
            self._write_binary(rel_path, response.content)
            row.raw_file = rel_path.as_posix()
        row.sha256 = hashlib.sha256(response.content).hexdigest()
        row.fetch_status = "failed"
        notes.append("unsupported_content")
        row.error_message = (
            f"unsupported_content: content_type={row.content_type or 'unknown'}"
        )

    def _finalize_row(
        self,
        row: SourceManifestRow,
        notes: list[str],
        event: dict,
        update_fetched_at: bool = True,
    ) -> SourceManifestRow:
        if update_fetched_at or not row.fetched_at:
            row.fetched_at = _utc_now_iso()

        self._generate_markdown_cleanup(row, notes)
        self._generate_source_summary(row, notes)
        self._generate_source_rating(row, notes)
        row.notes = "; ".join(dict.fromkeys(n for n in notes if n))
        metadata_rel = Path("metadata") / f"{row.id}.json"
        row.metadata_file = metadata_rel.as_posix()

        metadata_payload = {
            "id": row.id,
            "timestamp": row.fetched_at,
            "original_url": row.original_url,
            "final_url": row.final_url,
            "http_status": row.http_status,
            "content_type": row.content_type,
            "detected_type": row.detected_type,
            "fetch_method": row.fetch_method,
            "fetch_status": row.fetch_status,
            "error_message": row.error_message,
            "output_files": {
                "raw_file": row.raw_file,
                "rendered_file": row.rendered_file,
                "rendered_pdf_file": row.rendered_pdf_file,
                "markdown_file": row.markdown_file,
                "llm_cleanup_file": row.llm_cleanup_file,
                "summary_file": row.summary_file,
                "rating_file": row.rating_file,
                "metadata_file": row.metadata_file,
            },
            "notes": row.notes,
            "sha256": row.sha256,
            "title": row.title,
            "canonical_url": row.canonical_url,
            "extraction_method": row.extraction_method,
            "markdown_char_count": row.markdown_char_count,
            "llm_cleanup_needed": row.llm_cleanup_needed,
            "llm_cleanup_status": row.llm_cleanup_status,
            "summary_status": row.summary_status,
            "rating_status": row.rating_status,
        }
        self._write_text(
            metadata_rel,
            json.dumps(metadata_payload, ensure_ascii=False, indent=2),
        )

        event.update(metadata_payload)
        event["completed_at"] = _utc_now_iso()
        self._append_log(event)
        return row

    def _generate_markdown_cleanup(
        self,
        row: SourceManifestRow,
        notes: list[str],
    ) -> None:
        if not self.run_llm_cleanup:
            if row.llm_cleanup_file and _has_output_file(self.output_dir, row.llm_cleanup_file):
                row.llm_cleanup_status = row.llm_cleanup_status or "existing"
            elif not row.llm_cleanup_status:
                row.llm_cleanup_status = "not_requested"
            return

        if not row.markdown_file:
            row.llm_cleanup_status = "missing_markdown"
            row.llm_cleanup_needed = False
            return
        if not self.use_llm:
            row.llm_cleanup_status = "skipped_llm_disabled"
            row.llm_cleanup_needed = False
            return
        if not llm_backend_ready_for_chat(self.llm_backend):
            row.llm_cleanup_status = "skipped_llm_not_configured"
            row.llm_cleanup_needed = False
            notes.append(NOTE_LLM_CLEANUP_SKIPPED_LLM_NOT_CONFIGURED)
            return

        if (
            row.llm_cleanup_file
            and not self.force_llm_cleanup
            and _has_output_file(self.output_dir, row.llm_cleanup_file)
        ):
            row.llm_cleanup_status = row.llm_cleanup_status or "existing"
            return

        markdown_text = self._read_text(Path(row.markdown_file))
        if not markdown_text.strip():
            row.llm_cleanup_status = "missing_markdown"
            row.llm_cleanup_needed = False
            return

        source_text = markdown_text.strip()
        if len(source_text) > MAX_CLEANUP_SOURCE_CHARS:
            source_text = source_text[:MAX_CLEANUP_SOURCE_CHARS]

        research_purpose = self.research_purpose or (
            "No explicit research purpose was provided. Preserve factual content and clarity."
        )
        user_prompt = SOURCE_MARKDOWN_CLEANUP_USER.format(
            research_purpose=research_purpose,
            source_markdown=source_text,
        )

        try:
            cleanup_response = run_async_in_sync(
                self._run_llm_cleanup_completion,
                system_prompt=SOURCE_MARKDOWN_CLEANUP_SYSTEM,
                user_prompt=user_prompt,
            ).strip()
            needs_cleanup, cleaned_markdown = parse_cleanup_response(cleanup_response)
            row.llm_cleanup_needed = needs_cleanup

            if needs_cleanup:
                normalized = normalize_cleaned_markdown(cleaned_markdown)
                if not normalized:
                    row.llm_cleanup_status = "failed"
                    notes.append(NOTE_LLM_CLEANUP_FAILED)
                    return
                cleanup_rel = Path("markdown") / f"{row.id}_llm_clean.md"
                self._write_text(cleanup_rel, normalized)
                row.llm_cleanup_file = cleanup_rel.as_posix()
                row.llm_cleanup_status = "cleaned"
            else:
                row.llm_cleanup_status = "not_needed"
        except Exception as exc:
            row.llm_cleanup_status = "failed"
            row.llm_cleanup_needed = False
            notes.append(NOTE_LLM_CLEANUP_FAILED)
            logger.warning("Markdown cleanup failed for %s: %s", row.id, exc)

    def _generate_source_summary(
        self,
        row: SourceManifestRow,
        notes: list[str],
    ) -> None:
        if not self.run_llm_summary:
            if row.summary_file and _has_output_file(self.output_dir, row.summary_file):
                row.summary_status = row.summary_status or "existing"
            elif not row.summary_status:
                row.summary_status = "not_requested"
            return

        summary_source_path = ""
        if row.llm_cleanup_file and _has_output_file(self.output_dir, row.llm_cleanup_file):
            summary_source_path = row.llm_cleanup_file
        elif row.markdown_file:
            summary_source_path = row.markdown_file

        if not summary_source_path:
            row.summary_status = "missing_markdown"
            return
        if not self.use_llm:
            row.summary_status = "skipped_llm_disabled"
            return
        if not llm_backend_ready_for_chat(self.llm_backend):
            row.summary_status = "skipped_llm_not_configured"
            notes.append(NOTE_SUMMARY_SKIPPED_LLM_NOT_CONFIGURED)
            return
        if (
            row.summary_file
            and not self.force_summary
            and _has_output_file(self.output_dir, row.summary_file)
        ):
            row.summary_status = "existing"
            return

        markdown_text = self._read_text(Path(summary_source_path))
        if not markdown_text.strip():
            row.summary_status = "missing_markdown"
            return

        source_text = markdown_text.strip()
        if len(source_text) > MAX_SUMMARY_SOURCE_CHARS:
            source_text = source_text[:MAX_SUMMARY_SOURCE_CHARS]

        research_purpose = self.research_purpose or (
            "No explicit research purpose was provided. "
            "Focus on high-impact findings, methods, limitations, and relevance."
        )
        user_prompt = SOURCE_SUMMARY_USER.format(
            research_purpose=research_purpose,
            source_markdown=source_text,
        )

        try:
            summary = run_async_in_sync(
                self._run_llm_summary,
                system_prompt=SOURCE_SUMMARY_SYSTEM,
                user_prompt=user_prompt,
            ).strip()
            summary = normalize_summary_paragraph(summary)
            if not summary:
                notes.append(NOTE_SUMMARY_GENERATION_FAILED)
                row.summary_status = "failed"
                return

            summary_rel = Path("summaries") / f"{row.id}_summary.md"
            self._write_text(summary_rel, summary + "\n")
            row.summary_file = summary_rel.as_posix()
            row.summary_status = "generated"
        except Exception as exc:
            notes.append(NOTE_SUMMARY_GENERATION_FAILED)
            row.summary_status = "failed"
            logger.warning("Summary generation failed for %s: %s", row.id, exc)

    async def _run_llm_cleanup_completion(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> str:
        client = UnifiedLLMClient(self.llm_backend)
        try:
            return await client.chat_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_format=None,
            )
        finally:
            await client.close()

    async def _run_llm_summary(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> str:
        client = UnifiedLLMClient(self.llm_backend)
        try:
            return await client.chat_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_format=None,
            )
        finally:
            await client.close()

    def _generate_source_rating(
        self,
        row: SourceManifestRow,
        notes: list[str],
    ) -> None:
        if not self.run_llm_rating:
            if row.rating_file and _has_output_file(self.output_dir, row.rating_file):
                row.rating_status = row.rating_status or "existing"
            elif not row.rating_status:
                row.rating_status = "not_requested"
            return

        if not self.project_profile_yaml:
            row.rating_status = "skipped_no_profile"
            return

        rating_source_path = ""
        if row.llm_cleanup_file and _has_output_file(self.output_dir, row.llm_cleanup_file):
            rating_source_path = row.llm_cleanup_file
        elif row.markdown_file:
            rating_source_path = row.markdown_file

        if not rating_source_path:
            row.rating_status = "missing_markdown"
            return
        if not self.use_llm:
            row.rating_status = "skipped_llm_disabled"
            return
        if not llm_backend_ready_for_chat(self.llm_backend):
            row.rating_status = "skipped_llm_not_configured"
            notes.append(NOTE_RATING_SKIPPED_LLM_NOT_CONFIGURED)
            return
        if (
            row.rating_file
            and not self.force_rating
            and _has_output_file(self.output_dir, row.rating_file)
        ):
            row.rating_status = "existing"
            return

        markdown_text = self._read_text(Path(rating_source_path))
        if not markdown_text.strip():
            row.rating_status = "missing_markdown"
            return

        source_text = markdown_text.strip()
        if len(source_text) > MAX_SUMMARY_SOURCE_CHARS:
            source_text = source_text[:MAX_SUMMARY_SOURCE_CHARS]

        research_purpose = self.research_purpose or (
            "No explicit research purpose was provided. "
            "Evaluate the source based on the project profile dimensions."
        )
        system_prompt = SOURCE_RATING_SYSTEM.format(
            project_profile_yaml=self.project_profile_yaml,
        )
        user_prompt = SOURCE_RATING_USER.format(
            research_purpose=research_purpose,
            source_markdown=source_text,
        )

        try:
            raw_response = run_async_in_sync(
                self._run_llm_rating,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
            ).strip()

            rating_data = json.loads(raw_response)
            if not isinstance(rating_data, dict):
                notes.append(NOTE_RATING_GENERATION_FAILED)
                row.rating_status = "failed"
                return

            rating_rel = Path("ratings") / f"{row.id}_rating.json"
            self._write_text(
                rating_rel,
                json.dumps(rating_data, ensure_ascii=False, indent=2) + "\n",
            )
            row.rating_file = rating_rel.as_posix()
            row.rating_status = "generated"
        except Exception as exc:
            notes.append(NOTE_RATING_GENERATION_FAILED)
            row.rating_status = "failed"
            logger.warning("Rating generation failed for %s: %s", row.id, exc)

    async def _run_llm_rating(
        self,
        system_prompt: str,
        user_prompt: str,
    ) -> str:
        client = UnifiedLLMClient(self.llm_backend)
        try:
            return await client.chat_completion(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                response_format="json",
            )
        finally:
            await client.close()

    def _write_binary(self, rel_path: Path, content: bytes) -> None:
        dest = self.output_dir / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)

    def _write_text(self, rel_path: Path, content: str) -> None:
        dest = self.output_dir / rel_path
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content, encoding="utf-8")

    def _read_text(self, rel_path: Path) -> str:
        dest = self.output_dir / rel_path
        if not dest.exists():
            return ""
        return dest.read_text(encoding="utf-8", errors="replace")

    def _convert_document_to_markdown(
        self,
        extension: str,
        binary_content: bytes,
    ) -> tuple[str, str, list[str]]:
        ext = extension.lower()
        if ext in {".txt", ".md"}:
            text = decode_bytes_to_text(binary_content)
            return (text.strip(), "plain_text", []) if text.strip() else ("", "", [])

        if ext in {".docx", ".dotx"}:
            markdown_text = convert_docx_bytes_to_markdown(binary_content)
            return (
                (markdown_text, "docx_local", [])
                if markdown_text
                else ("", "", [NOTE_EXTRACTION_FAILURE])
            )

        if ext in {".doc", ".dot"}:
            if not self.runtime_capabilities.textutil_available:
                return "", "", [NOTE_RUNTIME_MISSING_TEXTUTIL, NOTE_DOC_CONVERSION_FAILED]
            markdown_text = convert_doc_bytes_with_textutil(binary_content)
            if markdown_text:
                return markdown_text, "doc_textutil", []
            return "", "", [NOTE_DOC_CONVERSION_FAILED]

        return "", "", []

    def _convert_pdf_to_markdown(
        self,
        pdf_bytes: bytes,
    ) -> tuple[str, str, list[str]]:
        notes: list[str] = []
        pages = extract_pdf_pages(pdf_bytes)
        if not pages:
            return "", "", [NOTE_EXTRACTION_FAILURE]

        native_texts = [p["text"] for p in pages]
        native_metrics = [compute_text_metrics(text) for text in native_texts]
        doc_quality = evaluate_pdf_document_quality(native_metrics)
        if doc_quality["native_good"]:
            markdown_text = format_pages_as_markdown(native_texts)
            if markdown_score(markdown_text) >= MIN_FALLBACK_MARKDOWN_SCORE:
                return markdown_text, "pdf_text", notes

        page_texts = native_texts[:]
        method = "pdf_text"
        low_quality_indexes = doc_quality["low_quality_pages"][:]

        # Tier 2: local OCR
        if low_quality_indexes and self.runtime_capabilities.tesseract_available:
            ocr_applied = False
            for idx in low_quality_indexes:
                ocr_text = run_tesseract_ocr_on_pixmap(pages[idx]["pixmap"])
                if ocr_text and compute_text_metrics(ocr_text)["chars"] >= PDF_OCR_MIN_CHARS:
                    page_texts[idx] = ocr_text
                    ocr_applied = True
            if ocr_applied:
                notes.append(NOTE_OCR_LOCAL_USED)
                method = "pdf_ocr_local"

        # Tier 3: optional LLM vision fallback on remaining low-quality pages
        post_ocr_metrics = [compute_text_metrics(text) for text in page_texts]
        post_ocr_quality = evaluate_pdf_document_quality(post_ocr_metrics)
        remaining_low_quality = post_ocr_quality["low_quality_pages"]
        llm_used = False
        if (
            remaining_low_quality
            and self.use_llm
            and self.runtime_capabilities.llm_vision_enabled
        ):
            for idx in remaining_low_quality:
                llm_text = self._run_llm_vision_ocr_on_page(pages[idx]["pixmap"])
                if llm_text and compute_text_metrics(llm_text)["chars"] >= PDF_OCR_MIN_CHARS:
                    page_texts[idx] = llm_text
                    llm_used = True
            if llm_used:
                notes.append(NOTE_OCR_LLM_FALLBACK_USED)
                method = "pdf_ocr_llm_vision"

        markdown_text = format_pages_as_markdown(page_texts)
        if markdown_score(markdown_text) >= MIN_FALLBACK_MARKDOWN_SCORE:
            return markdown_text, method, notes

        if not self.runtime_capabilities.tesseract_available:
            notes.append(NOTE_RUNTIME_MISSING_TESSERACT)
        if self.use_llm and not self.runtime_capabilities.llm_vision_enabled:
            notes.append(NOTE_RUNTIME_MISSING_LLM_VISION)

        notes.append(NOTE_EXTRACTION_FAILURE)
        return "", "", notes

    def _run_llm_vision_ocr_on_page(self, pixmap: fitz.Pixmap) -> str:
        if not self.runtime_capabilities.llm_vision_enabled:
            return ""
        try:
            image_bytes = pixmap.tobytes("png")
        except Exception:
            return ""

        prompt = (
            "Extract all readable text from this page image. "
            "Return plain text only. Do not summarize, do not add commentary."
        )
        try:
            return run_async_in_sync(
                self._run_llm_vision,
                prompt,
                image_bytes,
            ).strip()
        except Exception:
            return ""

    async def _run_llm_vision(self, prompt: str, image_bytes: bytes) -> str:
        client = UnifiedLLMClient(self.llm_backend)
        try:
            return await client.vision_ocr(prompt=prompt, image_bytes=image_bytes)
        finally:
            await client.close()


def build_manifest_csv(rows: list[SourceManifestRow]) -> str:
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=SOURCE_MANIFEST_COLUMNS)
    writer.writeheader()
    for row in rows:
        writer.writerow(_serialize_manifest_row(row))
    return output.getvalue()


def build_manifest_xlsx(rows: list[SourceManifestRow]) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.title = "manifest"
    ws.append(SOURCE_MANIFEST_COLUMNS)
    ws.freeze_panes = "A2"

    link_columns = {
        "raw_file",
        "rendered_file",
        "rendered_pdf_file",
        "markdown_file",
        "llm_cleanup_file",
        "summary_file",
        "metadata_file",
    }
    col_idx = {name: idx + 1 for idx, name in enumerate(SOURCE_MANIFEST_COLUMNS)}

    for row in rows:
        data = _serialize_manifest_row(row)
        ws.append([data[c] for c in SOURCE_MANIFEST_COLUMNS])
        row_idx = ws.max_row
        for link_col in link_columns:
            value = data.get(link_col, "")
            if not value:
                continue
            cell = ws.cell(row=row_idx, column=col_idx[link_col])
            cell.value = value
            cell.hyperlink = value
            cell.style = "Hyperlink"

    for idx, column in enumerate(SOURCE_MANIFEST_COLUMNS, start=1):
        width = 40 if column.endswith("_url") else 24
        ws.column_dimensions[get_column_letter(idx)].width = width

    stream = io.BytesIO()
    wb.save(stream)
    return stream.getvalue()


def _serialize_manifest_row(row: SourceManifestRow) -> dict[str, str | int | bool]:
    data = row.model_dump()
    serialized: dict[str, str | int | bool] = {}
    for column in SOURCE_MANIFEST_COLUMNS:
        value = data.get(column)
        if value is None:
            serialized[column] = ""
        else:
            serialized[column] = value
    return serialized


def _count_fetch_outcomes(rows: list[SourceManifestRow]) -> dict[str, int]:
    counts = {"success": 0, "failed": 0, "partial": 0}
    for row in rows:
        if row.fetch_status == "success":
            counts["success"] += 1
        elif row.fetch_status == "partial":
            counts["partial"] += 1
        else:
            counts["failed"] += 1
    return counts


def summarize_output_rows(rows: list[SourceManifestRow]) -> SourceOutputSummary:
    markdown_ready = 0
    summary_missing = 0
    summary_failed = 0
    rating_missing = 0
    rating_failed = 0
    llm_cleanup_failed = 0

    for row in rows:
        if row.markdown_file or row.llm_cleanup_file:
            markdown_ready += 1
        if (row.summary_status or "").strip().lower() == "failed":
            summary_failed += 1
        if (row.rating_status or "").strip().lower() == "failed":
            rating_failed += 1
        if (row.llm_cleanup_status or "").strip().lower() == "failed":
            llm_cleanup_failed += 1

        has_summary = bool(row.summary_file)
        if (row.markdown_file or row.llm_cleanup_file) and not has_summary:
            summary_missing += 1

        has_rating = bool(row.rating_file)
        if (row.markdown_file or row.llm_cleanup_file) and not has_rating:
            rating_missing += 1

    return SourceOutputSummary(
        total_rows=len(rows),
        raw_file_count=sum(1 for row in rows if row.raw_file),
        rendered_html_count=sum(1 for row in rows if row.rendered_file),
        rendered_pdf_count=sum(1 for row in rows if row.rendered_pdf_file),
        markdown_count=sum(1 for row in rows if row.markdown_file),
        llm_cleanup_file_count=sum(1 for row in rows if row.llm_cleanup_file),
        llm_cleanup_needed_count=sum(1 for row in rows if row.llm_cleanup_needed),
        llm_cleanup_failed_count=llm_cleanup_failed,
        summary_file_count=sum(1 for row in rows if row.summary_file),
        summary_missing_count=summary_missing,
        summary_failed_count=summary_failed,
        rating_file_count=sum(1 for row in rows if row.rating_file),
        rating_missing_count=rating_missing,
        rating_failed_count=rating_failed,
    )


def parse_notes(value: str) -> list[str]:
    if not value:
        return []
    return [part.strip() for part in value.split(";") if part.strip()]


def _has_output_file(output_dir: Path, rel_path: str) -> bool:
    if not rel_path:
        return False
    return (output_dir / rel_path).exists()


def png_images_to_pdf_bytes(images: list[bytes]) -> bytes:
    if not images:
        return b""

    doc = fitz.open()
    try:
        for image_bytes in images:
            if not image_bytes:
                continue
            image_doc = fitz.open(stream=image_bytes, filetype="png")
            try:
                rect = image_doc[0].rect
            finally:
                image_doc.close()

            page = doc.new_page(width=rect.width, height=rect.height)
            page.insert_image(page.rect, stream=image_bytes)

        if doc.page_count == 0:
            return b""
        return doc.tobytes(deflate=True, garbage=3)
    finally:
        doc.close()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def detect_runtime_capabilities(
    use_llm: bool,
    llm_backend: LLMBackendConfig,
) -> RuntimeCapabilities:
    runtime_notes: list[str] = []
    runtime_guidance: list[dict[str, str]] = []

    trafilatura_available = trafilatura is not None
    if not trafilatura_available:
        runtime_notes.append(NOTE_RUNTIME_MISSING_TRAFILATURA)
        runtime_guidance.append(
            {
                "code": NOTE_RUNTIME_MISSING_TRAFILATURA,
                "title": "Install missing parser dependency",
                "detail": (
                    "Python package `trafilatura` is not available, so HTML-to-markdown "
                    "extraction uses a fallback path. Run from the project root."
                ),
                "command": INSTALL_BOOTSTRAP_COMMAND,
            }
        )

    playwright_python_available, playwright_browser_available, playwright_error = (
        check_playwright_runtime()
    )
    if not playwright_browser_available:
        runtime_notes.append(NOTE_RUNTIME_MISSING_PLAYWRIGHT)
        if not playwright_python_available:
            runtime_guidance.append(
                {
                    "code": NOTE_RUNTIME_MISSING_PLAYWRIGHT,
                    "title": "Install Playwright Python package",
                    "detail": (
                        "Playwright is not importable from the Python environment, so "
                        "rendered HTML and visual captures are unavailable. "
                        "Run from the project root."
                    ),
                    "command": INSTALL_BOOTSTRAP_COMMAND,
                }
            )
        else:
            runtime_guidance.append(
                {
                    "code": NOTE_RUNTIME_MISSING_PLAYWRIGHT,
                    "title": "Install Playwright Chromium browser",
                    "detail": (
                        "Playwright is installed but Chromium is missing, so rendered HTML "
                        "and screenshot-based PDF capture are unavailable."
                    ),
                    "command": INSTALL_PLAYWRIGHT_BROWSER_COMMAND,
                }
            )
            if playwright_error:
                runtime_guidance[-1]["detail"] += f" ({playwright_error})"

    textutil_available = check_textutil_available()
    if not textutil_available:
        runtime_notes.append(NOTE_RUNTIME_MISSING_TEXTUTIL)

    tesseract_available = check_tesseract_available()
    if not tesseract_available:
        runtime_notes.append(NOTE_RUNTIME_MISSING_TESSERACT)

    llm_vision_enabled = False
    if use_llm:
        llm_vision_enabled = llm_backend_ready_for_vision(llm_backend)
        if not llm_vision_enabled:
            runtime_notes.append(NOTE_RUNTIME_MISSING_LLM_VISION)

    return RuntimeCapabilities(
        trafilatura_available=trafilatura_available,
        playwright_python_available=playwright_python_available,
        playwright_browser_available=playwright_browser_available,
        textutil_available=textutil_available,
        tesseract_available=tesseract_available,
        llm_vision_enabled=llm_vision_enabled,
        runtime_notes=runtime_notes,
        runtime_guidance=runtime_guidance,
    )


def check_playwright_browser_available() -> bool:
    _, browser_available, _ = check_playwright_runtime()
    return browser_available


def check_playwright_runtime() -> tuple[bool, bool, str]:
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return False, False, f"import_failure: {exc}"

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            browser.close()
            return True, True, ""
    except Exception as exc:
        normalized = _normalize_playwright_error(exc)
        return True, False, normalized


def check_textutil_available() -> bool:
    return shutil.which("textutil") is not None


def check_tesseract_available() -> bool:
    return shutil.which("tesseract") is not None


def llm_backend_ready_for_vision(config: LLMBackendConfig) -> bool:
    return llm_backend_ready_for_chat(config)


def llm_backend_ready_for_chat(config: LLMBackendConfig) -> bool:
    if not config.model.strip():
        return False
    if not config.base_url.strip():
        return False
    return config.kind in {"openai", "ollama"}


def parse_cleanup_response(response_text: str) -> tuple[bool, str]:
    text = (response_text or "").strip()
    if not text:
        return False, ""

    needs_match = re.search(r"NEEDS_CLEANUP:\s*(yes|no)", text, flags=re.IGNORECASE)
    needs_cleanup = False
    if needs_match:
        needs_cleanup = needs_match.group(1).strip().lower() == "yes"

    cleaned_markdown = ""
    marker_match = re.search(r"CLEANED_MARKDOWN:\s*", text, flags=re.IGNORECASE)
    if marker_match:
        cleaned_markdown = text[marker_match.end() :].strip()
    elif needs_cleanup:
        # Fallback: if the model ignored the format, treat full text as cleaned output.
        cleaned_markdown = text

    return needs_cleanup, cleaned_markdown


def normalize_cleaned_markdown(text: str) -> str:
    cleaned = (text or "").strip()
    if not cleaned:
        return ""
    return cleaned + "\n"


def normalize_summary_paragraph(text: str) -> str:
    cleaned = re.sub(r"\s+", " ", (text or "")).strip()
    if not cleaned:
        return ""

    sentences = split_sentences(cleaned)
    if not sentences:
        return ""
    if len(sentences) > 4:
        sentences = sentences[:4]
    return " ".join(sentences).strip()


def split_sentences(text: str) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [p.strip() for p in parts if p.strip()]


def clean_url_candidate(url: str) -> str:
    candidate = html.unescape((url or "").strip())
    if not candidate:
        return ""

    # Trim common wrappers added by markdown/table formatting.
    if candidate.startswith("<") and candidate.endswith(">"):
        candidate = candidate[1:-1].strip()

    candidate = candidate.strip("\"'`")
    candidate = _trim_unbalanced_trailing_closers(candidate)
    return candidate


def _trim_unbalanced_trailing_closers(candidate: str) -> str:
    value = (candidate or "").strip()
    if not value:
        return ""

    matching_openers = {")": "(", "]": "[", "}": "{", ">": "<"}
    while value:
        last = value[-1]
        if last in "\"'`":
            value = value[:-1].rstrip()
            continue
        opener = matching_openers.get(last)
        if not opener:
            break
        if value.count(last) <= value.count(opener):
            break
        value = value[:-1].rstrip()
    return value


def normalize_url(url: str) -> tuple[str, str]:
    candidate = clean_url_candidate(url)
    if not candidate:
        return "", "empty_url"
    if "://" not in candidate:
        candidate = f"https://{candidate}"

    try:
        parsed = urlsplit(candidate)
    except Exception as exc:
        return "", str(exc)

    if parsed.scheme not in {"http", "https"}:
        return "", f"unsupported_scheme:{parsed.scheme}"
    if not parsed.netloc:
        return "", "missing_hostname"
    normalized = urlunsplit(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path or "/",
            parsed.query,
            parsed.fragment,
        )
    )
    return normalized, ""


def dedupe_url_key(url: str) -> str:
    normalized_url, _ = normalize_url(url)
    candidate = clean_url_candidate(normalized_url or url or "")
    if not candidate:
        return ""
    try:
        parsed = urlsplit(candidate)
    except Exception:
        return candidate.lower()

    if not parsed.netloc:
        return candidate.lower()

    filtered_params: list[tuple[str, str]] = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        key_lower = key.lower()
        if key_lower in TRACKING_PARAM_EXACT:
            continue
        if any(key_lower.startswith(prefix) for prefix in TRACKING_PARAM_PREFIXES):
            continue
        filtered_params.append((key, value))

    filtered_params.sort(key=lambda item: (item[0].lower(), item[1]))
    query = urlencode(filtered_params, doseq=True)
    canonical_path = quote(unquote(parsed.path or "/"), safe="/:@!$&'()*+,;=-._~")

    return urlunsplit(
        (
            parsed.scheme.lower() or "https",
            parsed.netloc.lower(),
            canonical_path,
            query,
            "",
        )
    )


def classify_http_status(status_code: int) -> str:
    if status_code in {401, 403, 407, 429}:
        return "blocked_request"
    if 400 <= status_code < 500:
        return "network_failure"
    if status_code >= 500:
        return "network_failure"
    return "ok"


def detect_source_type(content_type: str, final_url: str, body: bytes) -> str:
    ct = (content_type or "").lower()
    ct_base = ct.split(";", 1)[0].strip()
    final = (final_url or "").lower()
    body_head = (body or b"")[:1024].lstrip()

    if "application/pdf" in ct:
        return "pdf"
    if final.endswith(".pdf"):
        return "pdf"
    if body_head.startswith(b"%PDF-"):
        return "pdf"

    if "text/html" in ct or "application/xhtml+xml" in ct:
        return "html"
    if body_head.startswith(b"<!doctype html") or body_head.startswith(b"<html"):
        return "html"

    if is_document_source(ct_base, final):
        return "document"
    return "unsupported"


def is_document_source(content_type: str, final_url: str) -> bool:
    suffix = Path(urlsplit(final_url).path).suffix.lower()
    if suffix in DOCUMENT_EXTENSIONS:
        return True
    if content_type in DOCUMENT_CONTENT_TYPE_EXT:
        return True
    # Treat remaining text-like responses as document text if not HTML.
    return content_type.startswith("text/")


def infer_document_extension(
    final_url: str,
    content_type: str,
    content_disposition: str,
) -> str:
    url_ext = Path(urlsplit(final_url).path).suffix.lower()
    if url_ext in DOCUMENT_EXTENSIONS:
        return url_ext

    disposition_ext = _extract_extension_from_content_disposition(content_disposition)
    if disposition_ext:
        return disposition_ext

    ct_base = (content_type or "").lower().split(";", 1)[0].strip()
    if ct_base in DOCUMENT_CONTENT_TYPE_EXT:
        return DOCUMENT_CONTENT_TYPE_EXT[ct_base]
    if ct_base.startswith("text/"):
        return ".txt"
    return ".bin"


def _extract_extension_from_content_disposition(content_disposition: str) -> str:
    if not content_disposition:
        return ""
    # Matches filename=report.docx or filename*=UTF-8''report.docx
    match = re.search(
        r"filename\*?=(?:UTF-8''|\"|')?([^\"';]+)",
        content_disposition,
        re.IGNORECASE,
    )
    if not match:
        return ""
    filename = match.group(1).strip()
    suffix = Path(filename).suffix.lower()
    if suffix in DOCUMENT_EXTENSIONS:
        return suffix
    return ""


def decode_document_text(response: httpx.Response) -> str:
    if not response.content:
        return ""
    try:
        return response.text
    except Exception:
        for encoding in ("utf-8", "latin-1"):
            try:
                return response.content.decode(encoding, errors="replace")
            except Exception:
                continue
    return ""


def first_nonempty_line(text: str) -> str:
    for line in (text or "").splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return ""


def _normalize_playwright_error(exc: Exception) -> str:
    details = f"{type(exc).__name__}: {exc}"
    lowered = details.lower()
    if "no module named 'playwright'" in lowered or "modulenotfounderror" in lowered:
        return f"playwright_not_installed: run `{INSTALL_BOOTSTRAP_COMMAND}`"
    if "executable doesn't exist" in lowered or "playwright install" in lowered:
        return (
            f"playwright_not_installed: run `{INSTALL_PLAYWRIGHT_BROWSER_COMMAND}`"
        )
    return f"rendering_failure: {details}"


def normalize_render_error_note(render_error: str) -> str:
    lowered = (render_error or "").lower()
    if "playwright_not_installed" in lowered or "playwright install chromium" in lowered:
        return NOTE_RUNTIME_MISSING_PLAYWRIGHT
    return "rendering_failure"


def blocked_error_message(status_code: int) -> str:
    if status_code >= 400:
        return f"{NOTE_BLOCKED_REQUEST}: http_status_{status_code}"
    return f"{NOTE_BLOCKED_REQUEST}: challenge_page_detected"


def detect_blocked_page(html_text: str, title: str, final_url: str) -> bool:
    sample = (html_text or "")[:20000]
    title_lower = (title or "").strip().lower()
    url_lower = (final_url or "").strip().lower()

    if "cdn-cgi/challenge-platform" in url_lower:
        return True
    if "just a moment" in title_lower:
        return True

    signals = 0
    for pattern in BLOCKED_PAGE_PATTERNS:
        if pattern.search(sample):
            signals += 1

    if "cf-ray" in sample.lower() or "__cf_bm" in sample.lower():
        signals += 1
    if "hcaptcha" in sample.lower() or "g-recaptcha" in sample.lower():
        signals += 1

    return signals >= 2


def decode_bytes_to_text(data: bytes) -> str:
    if not data:
        return ""
    for encoding in ("utf-8", "utf-16", "latin-1"):
        try:
            return data.decode(encoding)
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")


def convert_docx_bytes_to_markdown(data: bytes) -> str:
    try:
        from docx import Document

        doc = Document(io.BytesIO(data))
    except Exception:
        return ""

    lines: list[str] = []
    for para in doc.paragraphs:
        text = re.sub(r"\s+", " ", (para.text or "").strip())
        if not text:
            continue
        style_name = (para.style.name or "").lower()
        if "heading" in style_name:
            level = 2
            for ch in style_name:
                if ch.isdigit():
                    level = max(1, min(int(ch), 6))
                    break
            lines.append(f"{'#' * level} {text}")
        else:
            lines.append(text)
        lines.append("")

    markdown_text = "\n".join(lines).strip()
    return markdown_text


def convert_doc_bytes_with_textutil(data: bytes) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        doc_path = Path(tmpdir) / "input.doc"
        doc_path.write_bytes(data)
        cmd = ["textutil", "-convert", "txt", "-stdout", str(doc_path)]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=False,
                check=False,
                timeout=30,
            )
        except Exception:
            return ""
        if result.returncode != 0:
            return ""
        return decode_bytes_to_text(result.stdout).strip()


def extract_pdf_pages(pdf_bytes: bytes) -> list[dict]:
    try:
        with suppress_mupdf_messages():
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")

            pages: list[dict] = []
            try:
                for page_index in range(doc.page_count):
                    page = doc[page_index]
                    text = page.get_text("text")
                    pixmap = page.get_pixmap(matrix=fitz.Matrix(2.0, 2.0), alpha=False)
                    pages.append(
                        {
                            "index": page_index + 1,
                            "text": text.strip(),
                            "pixmap": pixmap,
                        }
                    )
            finally:
                doc.close()
        return pages
    except Exception:
        return []


def compute_text_metrics(text: str) -> dict[str, float]:
    compact = re.sub(r"\s+", " ", text or "").strip()
    chars = len(compact)
    alpha_chars = sum(1 for ch in compact if ch.isalpha())
    alpha_ratio = (alpha_chars / chars) if chars else 0.0
    return {
        "chars": float(chars),
        "alpha_ratio": alpha_ratio,
    }


def evaluate_pdf_document_quality(page_metrics: list[dict[str, float]]) -> dict:
    if not page_metrics:
        return {"native_good": False, "low_quality_pages": []}

    low_quality_pages: list[int] = []
    total_chars = 0.0
    for idx, metrics in enumerate(page_metrics):
        chars = metrics.get("chars", 0.0)
        alpha_ratio = metrics.get("alpha_ratio", 0.0)
        total_chars += chars
        if chars < PDF_NATIVE_PAGE_MIN_CHARS or alpha_ratio < PDF_TEXT_ALPHA_MIN_RATIO:
            low_quality_pages.append(idx)

    avg_chars = total_chars / len(page_metrics)
    native_good = (
        avg_chars >= PDF_NATIVE_DOC_MIN_AVG_CHARS
        and len(low_quality_pages) <= max(1, int(len(page_metrics) * 0.25))
    )
    return {
        "native_good": native_good,
        "low_quality_pages": low_quality_pages,
        "avg_chars": avg_chars,
    }


def run_tesseract_ocr_on_pixmap(pixmap: fitz.Pixmap) -> str:
    with tempfile.TemporaryDirectory() as tmpdir:
        image_path = Path(tmpdir) / "page.png"
        image_path.write_bytes(pixmap.tobytes("png"))
        cmd = [
            "tesseract",
            str(image_path),
            "stdout",
            "--psm",
            "3",
            "-l",
            "eng",
        ]
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                timeout=45,
            )
        except Exception:
            return ""
        if result.returncode != 0:
            return ""
        return (result.stdout or "").strip()


def format_pages_as_markdown(page_texts: list[str]) -> str:
    lines: list[str] = []
    for idx, page_text in enumerate(page_texts, start=1):
        cleaned = re.sub(r"\n{3,}", "\n\n", (page_text or "").strip())
        if not cleaned:
            continue
        lines.append(f"## Page {idx}")
        lines.append("")
        lines.append(cleaned)
        lines.append("")
    return "\n".join(lines).strip()


def decode_html(response: httpx.Response) -> str:
    if not response.content:
        return ""
    try:
        return response.text
    except Exception:
        for encoding in ("utf-8", "latin-1"):
            try:
                return response.content.decode(encoding, errors="replace")
            except Exception:
                continue
    return ""


def extract_markdown(html: str) -> tuple[str, str]:
    if not html:
        return "", "extraction_failure"
    if trafilatura is None:
        return "", "extraction_failure"
    try:
        md = trafilatura.extract(
            html,
            output_format="markdown",
            include_links=True,
            include_images=False,
            include_tables=True,
            favor_recall=True,
            deduplicate=True,
        )
    except Exception:
        return "", "extraction_failure"

    if not md:
        return "", "extraction_failure"
    return md.strip(), ""


def extract_markdown_with_fallback(
    html_text: str,
    runtime_capabilities: RuntimeCapabilities,
) -> tuple[str, bool, list[str]]:
    notes: list[str] = []
    if not html_text:
        notes.append(NOTE_EXTRACTION_FAILURE)
        return "", False, notes

    if runtime_capabilities.trafilatura_available and trafilatura is not None:
        markdown_text, _ = extract_markdown(html_text)
        if markdown_text:
            return markdown_text, False, notes
    else:
        notes.append(NOTE_RUNTIME_MISSING_TRAFILATURA)

    fallback_markdown = extract_markdown_fallback(html_text)
    if fallback_markdown:
        return fallback_markdown, True, notes

    notes.append(NOTE_EXTRACTION_FAILURE)
    return "", False, notes


def extract_markdown_fallback(html_text: str) -> str:
    if not html_text:
        return ""

    candidate = select_main_content_fragment(html_text)

    # Drop high-noise page chrome and scripts before text conversion.
    candidate = re.sub(
        r"(?is)<(script|style|noscript|svg|canvas|iframe|form|button|header|footer|nav|aside)[^>]*>.*?</\1>",
        " ",
        candidate,
    )
    candidate = re.sub(r"(?is)<br\s*/?>", "\n", candidate)
    candidate = re.sub(
        r"(?is)<h([1-6])[^>]*>(.*?)</h\1>",
        _heading_replacer,
        candidate,
    )
    candidate = re.sub(r"(?is)<li[^>]*>(.*?)</li>", _list_item_replacer, candidate)
    candidate = re.sub(r"(?is)</(p|div|section|article|main|blockquote|tr|table)>", "\n\n", candidate)
    candidate = re.sub(r"(?is)<(p|div|section|article|main|blockquote|tr|table)[^>]*>", "\n", candidate)

    text = re.sub(r"(?is)<[^>]+>", " ", candidate)
    text = html.unescape(text)
    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    cleaned_lines: list[str] = []
    seen: set[str] = set()
    for raw_line in text.splitlines():
        line = re.sub(r"\s+", " ", raw_line).strip()
        if not line:
            if cleaned_lines and cleaned_lines[-1] != "":
                cleaned_lines.append("")
            continue

        if is_boilerplate_line(line):
            continue

        dedupe_key = line.lower()
        if dedupe_key in seen and len(line) < 120:
            continue
        seen.add(dedupe_key)
        cleaned_lines.append(line)

    markdown_text = "\n".join(cleaned_lines).strip()
    markdown_text = re.sub(r"\n{3,}", "\n\n", markdown_text)
    if markdown_score(markdown_text) < MIN_FALLBACK_MARKDOWN_SCORE:
        return ""
    return markdown_text


def select_main_content_fragment(html_text: str) -> str:
    for pattern in [
        r"(?is)<main[^>]*>(.*?)</main>",
        r"(?is)<article[^>]*>(.*?)</article>",
        r'(?is)<div[^>]+(?:id|class)=["\'][^"\']*(?:content|article|post|main)[^"\']*["\'][^>]*>(.*?)</div>',
        r'(?is)<section[^>]+(?:id|class)=["\'][^"\']*(?:content|article|post|main)[^"\']*["\'][^>]*>(.*?)</section>',
    ]:
        match = re.search(pattern, html_text)
        if match:
            return match.group(1)
    return html_text


def _heading_replacer(match: re.Match) -> str:
    level = int(match.group(1))
    content = strip_tags(match.group(2))
    if not content:
        return "\n"
    return f"\n{'#' * level} {content}\n"


def _list_item_replacer(match: re.Match) -> str:
    content = strip_tags(match.group(1))
    if not content:
        return "\n"
    return f"\n- {content}"


def strip_tags(text: str) -> str:
    cleaned = re.sub(r"(?is)<[^>]+>", " ", text or "")
    cleaned = html.unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def is_boilerplate_line(line: str) -> bool:
    normalized = line.strip().lower()
    if not normalized:
        return True

    if normalized in {"menu", "navigation", "skip to content", "back to top"}:
        return True

    boilerplate_tokens = [
        "all rights reserved",
        "privacy policy",
        "cookie policy",
        "terms of use",
        "accept all cookies",
        "manage cookies",
        "sign in",
        "subscribe",
    ]
    return any(token in normalized for token in boilerplate_tokens)


def markdown_score(markdown_text: str) -> int:
    if not markdown_text:
        return 0
    compact = re.sub(r"\s+", " ", markdown_text).strip()
    return len(compact)


def extract_title(html: str) -> str:
    if not html:
        return ""
    match = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    title = re.sub(r"\s+", " ", match.group(1)).strip()
    return title[:500]


def extract_canonical_url(html: str) -> str:
    if not html:
        return ""
    match = re.search(
        r'<link[^>]+rel=["\'][^"\']*canonical[^"\']*["\'][^>]*href=["\']([^"\']+)["\']',
        html,
        re.IGNORECASE,
    )
    if match:
        return match.group(1).strip()
    return ""
