"""Markdown text extraction."""

from __future__ import annotations

import re
from pathlib import Path

from backend.models.ingestion import IngestedDocument, TextBlock
from backend.parsers.text_utils import clean_text_block

# Markdown inline citation links: [\[1\]](url) or [[1]](url)
MD_CITATION_LINK = re.compile(
    r"\[\\?\[(\d{1,4})\\?\]\]\((https?://[^\s\)]+)\)"
)

# Remaining escaped bracket citations without URL: \[\[N\]\] or \[N\]
MD_ESCAPED_CITATION = re.compile(r"\\?\[\\?(\d{1,4})\\?\]")

# Bare superscript citations: text.N where N is a citation number
# Matches period (not preceded by a digit) followed by 1-3 digit number
# Examples: "disciplines.1", "Climate Zones.12", "short-cycling.14"
BARE_SUPERSCRIPT = re.compile(r"(?<!\d)\.(\d{1,3})(?=[\s,;:\n]|$)")

# Standard Markdown links in bibliography: [text](url)
MD_LINK = re.compile(r"\[([^\]]*)\]\((https?://[^\s\)]+)\)")


def extract_md(file_path: Path) -> IngestedDocument:
    """Extract structured text from a Markdown file.

    Handles Markdown-style inline citation links where citation markers
    contain URLs: [\\[1\\]](https://url.com)
    """
    raw = file_path.read_text(encoding="utf-8", errors="replace")

    # Phase 1: Extract all inline citation URLs from the raw text
    inline_urls: dict[int, str] = {}
    # Style A: [\[N\]](url) — inline citation links
    for match in MD_CITATION_LINK.finditer(raw):
        ref_num = int(match.group(1))
        url = match.group(2).rstrip(".,;:)>]")
        if ref_num not in inline_urls:
            inline_urls[ref_num] = url

    # Phase 2: Normalize the text
    # Replace [\[N\]](url) → [N]
    normalized = MD_CITATION_LINK.sub(lambda m: f"[{m.group(1)}]", raw)
    # Normalize remaining escaped brackets: \[N\] → [N]
    normalized = MD_ESCAPED_CITATION.sub(lambda m: f"[{m.group(1)}]", normalized)
    # Normalize bare superscript citations: "text.12 " → "text.[12] "
    normalized = BARE_SUPERSCRIPT.sub(lambda m: f".[{m.group(1)}]", normalized)
    # Normalize Markdown links to plain text: [text](url) → text
    # (keeps URLs accessible via URL_PATTERN in bibliography parser)
    normalized = MD_LINK.sub(lambda m: m.group(2) if m.group(1) == m.group(2) or m.group(1).startswith("http") else m.group(1), normalized)
    # Strip Markdown escaped characters: \- → -, \. → .
    normalized = re.sub(r"\\([_*\-\[\](){}#.!])", r"\1", normalized)
    # Clean Markdown angle-bracket URLs: <https://...> → https://...
    normalized = re.sub(r"<(https?://[^\s>]+)>", r"\1", normalized)
    # Strip markdown bold/italic markers for cleaner text
    # Handle bold (**text**) first, then italic (*text*)
    normalized = re.sub(r"\*\*(.+?)\*\*", r"\1", normalized)
    normalized = re.sub(r"\*(.+?)\*", r"\1", normalized)

    # Phase 3: Split into paragraphs and build blocks
    paragraphs = re.split(r"\n{2,}", normalized)

    blocks: list[TextBlock] = []
    full_text_parts: list[str] = []
    block_index = 0
    char_offset = 0

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        cleaned = clean_text_block(para)
        if not cleaned:
            continue

        # Detect headings
        is_heading = False
        heading_level = None
        heading_match = re.match(r"^(#{1,6})\s+", cleaned)
        if heading_match:
            is_heading = True
            heading_level = len(heading_match.group(1))
            cleaned = cleaned[heading_match.end():]

        tb = TextBlock(
            text=cleaned,
            page_number=None,
            block_index=block_index,
            is_heading=is_heading,
            heading_level=heading_level,
            char_offset_start=char_offset,
            char_offset_end=char_offset + len(cleaned),
        )
        blocks.append(tb)
        full_text_parts.append(cleaned)
        char_offset += len(cleaned) + 1
        block_index += 1

    full_text = "\n".join(full_text_parts)

    return IngestedDocument(
        filename=file_path.name,
        file_type="md",
        total_pages=None,
        blocks=blocks,
        full_text=full_text,
        warnings=[],
        inline_citation_urls=inline_urls,
    )
