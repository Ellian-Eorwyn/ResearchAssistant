"""Artifacts captured for an existing source outside the normal download run.

A blocked source cannot be re-fetched by the downloader — that just reproduces
the wall. Instead the page is obtained some other way (a person driving a real
browser through the challenge, or saving the page by hand) and the resulting
files are written into the *existing* source id, in place, under the exact
filenames the downloader would have used.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

# Only the fetch phase is replayed by a capture. Catalog, summary and rating are
# deliberately left for the user to re-run from the browser once the row is no
# longer blocked.
CAPTURE_FETCH_METHOD = "manual_capture"
UPLOAD_FETCH_METHOD = "manual_upload"

# What a person can hand us for a source they collected themselves.
SUPPORTED_UPLOAD_EXTENSIONS = {".html", ".htm", ".xhtml", ".pdf", ".md", ".markdown", ".txt"}
REJECTED_ARCHIVE_EXTENSIONS = {".mhtml", ".mht"}
MAX_UPLOAD_BYTES = 200 * 1024 * 1024


@dataclass
class CapturedArtifacts:
    """What a capture produced, before it is written into the repository."""

    raw_html: str = ""
    rendered_html: str = ""
    rendered_pdf: bytes = b""
    raw_pdf: bytes = b""
    ocr_pdf: bytes = b""
    markdown: str = ""

    final_url: str = ""
    title: str = ""
    canonical_url: str = ""
    content_type: str = ""
    http_status: int | None = None
    detected_type: str = "html"
    fetch_method: str = CAPTURE_FETCH_METHOD
    extraction_method: str = ""
    ocr_status: str = ""
    notes: list[str] = field(default_factory=list)

    def has_content(self) -> bool:
        return bool(
            self.raw_html
            or self.rendered_html
            or self.rendered_pdf
            or self.raw_pdf
            or self.markdown
        )


class UnsupportedManualUploadError(ValueError):
    """The file a person handed us is not something we can read."""


def artifacts_from_uploaded_bytes(
    *,
    content: bytes,
    filename: str,
    final_url: str = "",
) -> CapturedArtifacts:
    """Turn a file a person collected by hand into capturable artifacts.

    Shared by every manual route — the multipart upload, the watch-folder
    attach — so a page saved from a real browser is read and scored identically
    however it reaches us.
    """
    # Imported here rather than at module scope: source_downloader pulls in the
    # whole extraction stack, and this module is otherwise cheap to import.
    from backend.pipeline.fetch_verification import verify_fetch
    from backend.pipeline.source_downloader import (
        build_searchable_pdf,
        decode_bytes_to_text,
        detect_runtime_capabilities,
        extract_canonical_url,
        extract_markdown_with_fallback,
        extract_pdf_pages,
        extract_title,
    )

    if not content:
        raise UnsupportedManualUploadError("The uploaded file is empty.")
    if len(content) > MAX_UPLOAD_BYTES:
        raise UnsupportedManualUploadError(
            f"That file is {len(content) // (1024 * 1024)} MB, over the "
            f"{MAX_UPLOAD_BYTES // (1024 * 1024)} MB limit."
        )

    suffix = Path(filename or "").suffix.lower()
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
    elif suffix in REJECTED_ARCHIVE_EXTENSIONS:
        raise UnsupportedManualUploadError(
            "MHTML archives are not supported. Save the page as HTML "
            "(or print it to PDF) and upload that instead."
        )
    else:
        raise UnsupportedManualUploadError(
            f"Unsupported file type `{suffix or filename}`. Upload HTML, PDF, or Markdown."
        )

    # A file handed to us has no HTTP evidence, so the verifier only sees the
    # text; judging it the same way keeps a saved block page from counting as a
    # fix. The authoritative verdict is recomputed when the artifacts are
    # written, but scoring here keeps the note trail identical on every route.
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

    return artifacts
