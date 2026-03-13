"""Stage 4: Detect in-text numeric citation markers."""

from __future__ import annotations

import re

from backend.models.citations import InTextCitation
from backend.models.ingestion import IngestedDocument

# Pattern for figure/table/equation references to exclude
FIGURE_TABLE_PATTERN = re.compile(
    r"(?:Fig(?:ure)?|Table|Eq(?:uation)?|Scheme|Chart|Plate|Panel)\s*\.?\s*$",
    re.IGNORECASE,
)

# Bracket citation patterns, ordered by specificity
# These are applied to the full_text string
CITATION_PATTERNS = [
    # Mixed: [1, 3-5, 7] — most general, handles everything
    re.compile(
        r"\["
        r"(\d{1,4}"
        r"(?:\s*[-\u2013\u2014]\s*\d{1,4})?"
        r"(?:\s*,\s*\d{1,4}(?:\s*[-\u2013\u2014]\s*\d{1,4})?)*"
        r")\]"
    ),
]


def detect_citations(
    doc: IngestedDocument,
    refs_start_offset: int | None = None,
) -> list[InTextCitation]:
    """Detect all in-text numeric citation markers in the document body.

    Args:
        doc: The ingested document.
        refs_start_offset: Character offset where the references section starts.
            Citations after this point are excluded.
    """
    text = doc.full_text
    # Limit search to body text (before references section)
    if refs_start_offset is not None:
        search_text = text[:refs_start_offset]
    else:
        search_text = text

    citations: list[InTextCitation] = []
    seen_offsets: set[int] = set()
    cit_counter = 0

    for pattern in CITATION_PATTERNS:
        for match in pattern.finditer(search_text):
            start = match.start()
            if start in seen_offsets:
                continue

            # Check if this is a figure/table reference
            prefix = search_text[max(0, start - 30) : start]
            if FIGURE_TABLE_PATTERN.search(prefix):
                continue

            inner = match.group(1)
            ref_numbers = _parse_citation_inner(inner)
            if not ref_numbers:
                continue

            # Sanity check: skip unreasonably large reference numbers
            if any(n > 999 for n in ref_numbers):
                continue

            seen_offsets.add(start)
            cit_counter += 1
            page = _offset_to_page(doc, start)

            citations.append(
                InTextCitation(
                    citation_id=f"{doc.filename}_cit_{cit_counter}",
                    document_filename=doc.filename,
                    raw_marker=match.group(0),
                    ref_numbers=ref_numbers,
                    page_number=page,
                    char_offset_start=start,
                    char_offset_end=match.end(),
                    style="bracket",
                )
            )

    # Sort by position in document
    citations.sort(key=lambda c: c.char_offset_start)
    return citations


def _parse_citation_inner(inner: str) -> list[int]:
    """Parse the inner text of a citation bracket.

    Examples:
        "1" -> [1]
        "2, 3" -> [2, 3]
        "4-7" -> [4, 5, 6, 7]
        "1, 3-5, 7" -> [1, 3, 4, 5, 7]
    """
    result: list[int] = []
    parts = re.split(r"\s*,\s*", inner.strip())

    for part in parts:
        part = part.strip()
        range_match = re.match(r"(\d+)\s*[-\u2013\u2014]\s*(\d+)", part)
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            if end < start or end - start > 50:
                # Unreasonable range, skip
                continue
            result.extend(range(start, end + 1))
        else:
            try:
                result.append(int(part))
            except ValueError:
                continue

    return result


def _offset_to_page(doc: IngestedDocument, offset: int) -> int | None:
    """Map a character offset in full_text to a page number."""
    for block in doc.blocks:
        if block.char_offset_start <= offset <= block.char_offset_end:
            return block.page_number
    return None
