"""Artifacts captured for an existing source outside the normal download run.

A blocked source cannot be re-fetched by the downloader — that just reproduces
the wall. Instead the page is obtained some other way (a person driving a real
browser through the challenge, or saving the page by hand) and the resulting
files are written into the *existing* source id, in place, under the exact
filenames the downloader would have used.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# Only the fetch phase is replayed by a capture. Catalog, summary and rating are
# deliberately left for the user to re-run from the browser once the row is no
# longer blocked.
CAPTURE_FETCH_METHOD = "manual_capture"
UPLOAD_FETCH_METHOD = "manual_upload"


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
