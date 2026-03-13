"""PDF text extraction using PyMuPDF (fitz)."""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
import threading

import fitz  # PyMuPDF

from backend.models.ingestion import IngestedDocument, TextBlock
from backend.parsers.text_utils import clean_text_block, unwrap_lines

_MUPDF_MESSAGE_LOCK = threading.Lock()


@contextmanager
def _suppress_mupdf_messages() -> Iterator[None]:
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


def extract_pdf(file_path: Path) -> IngestedDocument:
    """Extract structured text from a PDF file using PyMuPDF."""
    with _suppress_mupdf_messages():
        doc = fitz.open(str(file_path))
    warnings: list[str] = []
    raw_page_blocks: list[list[dict]] = []

    with _suppress_mupdf_messages():
        for page_num in range(len(doc)):
            page = doc[page_num]
            page_dict = page.get_text("dict", flags=fitz.TEXT_PRESERVE_WHITESPACE)
            raw_page_blocks.append(page_dict.get("blocks", []))

    # Detect recurring headers/footers
    header_texts, footer_texts = _detect_recurring_header_footer(
        raw_page_blocks, doc[0].rect.height if len(doc) > 0 else 800
    )

    # Build text blocks
    blocks: list[TextBlock] = []
    full_text_parts: list[str] = []
    block_index = 0
    char_offset = 0

    for page_num, page_blocks in enumerate(raw_page_blocks):
        page_had_text = False
        for raw_block in page_blocks:
            if raw_block.get("type") != 0:  # 0 = text block
                continue

            # Extract text from spans within lines
            block_text_parts: list[str] = []
            for line in raw_block.get("lines", []):
                line_text = ""
                for span in line.get("spans", []):
                    line_text += span.get("text", "")
                if line_text.strip():
                    block_text_parts.append(line_text)

            if not block_text_parts:
                continue

            raw_text = "\n".join(block_text_parts)
            cleaned = clean_text_block(raw_text)
            if not cleaned:
                continue

            # Skip headers/footers
            if cleaned.strip() in header_texts or cleaned.strip() in footer_texts:
                continue

            page_had_text = True

            # Detect if this is a heading (short text, likely bold/larger font)
            is_heading = False
            heading_level = None
            if len(cleaned) < 100 and len(block_text_parts) == 1:
                # Check if font size is larger than typical body text
                spans = []
                for line in raw_block.get("lines", []):
                    spans.extend(line.get("spans", []))
                if spans:
                    avg_size = sum(s.get("size", 12) for s in spans) / len(spans)
                    is_bold = any("bold" in s.get("font", "").lower() for s in spans)
                    if avg_size > 13 or is_bold:
                        is_heading = True
                        if avg_size >= 18:
                            heading_level = 1
                        elif avg_size >= 15:
                            heading_level = 2
                        else:
                            heading_level = 3

            # Unwrap lines within this block
            cleaned = unwrap_lines(cleaned)

            tb = TextBlock(
                text=cleaned,
                page_number=page_num + 1,
                block_index=block_index,
                is_heading=is_heading,
                heading_level=heading_level,
                char_offset_start=char_offset,
                char_offset_end=char_offset + len(cleaned),
            )
            blocks.append(tb)
            full_text_parts.append(cleaned)
            char_offset += len(cleaned) + 1  # +1 for the newline separator
            block_index += 1

        if not page_had_text:
            warnings.append(f"Page {page_num + 1}: no text extracted (may be scanned/image)")

    doc.close()
    full_text = "\n".join(full_text_parts)

    return IngestedDocument(
        filename=file_path.name,
        file_type="pdf",
        total_pages=len(raw_page_blocks),
        blocks=blocks,
        full_text=full_text,
        warnings=warnings,
    )


def _detect_recurring_header_footer(
    pages: list[list[dict]], page_height: float
) -> tuple[set[str], set[str]]:
    """Detect text that recurs at the top/bottom of pages across the document."""
    if len(pages) < 3:
        return set(), set()

    header_margin = page_height * 0.08
    footer_margin = page_height * 0.92
    top_texts: Counter[str] = Counter()
    bottom_texts: Counter[str] = Counter()

    for page_blocks in pages:
        page_top_seen: set[str] = set()
        page_bottom_seen: set[str] = set()
        for block in page_blocks:
            if block.get("type") != 0:
                continue
            y0 = block.get("bbox", [0, 0, 0, 0])[1]
            y1 = block.get("bbox", [0, 0, 0, 0])[3]

            text_parts = []
            for line in block.get("lines", []):
                for span in line.get("spans", []):
                    text_parts.append(span.get("text", ""))
            text = " ".join(text_parts).strip()
            if not text or len(text) > 200:
                continue

            if y0 < header_margin and text not in page_top_seen:
                top_texts[text] += 1
                page_top_seen.add(text)
            if y1 > footer_margin and text not in page_bottom_seen:
                bottom_texts[text] += 1
                page_bottom_seen.add(text)

    threshold = len(pages) * 0.4
    headers = {t for t, c in top_texts.items() if c >= threshold}
    footers = {t for t, c in bottom_texts.items() if c >= threshold}
    return headers, footers
