"""Stage 4: Detect in-text numeric citation markers."""

from __future__ import annotations

import re

from backend.models.citations import InTextCitation
from backend.models.ingestion import IngestedDocument, TextBlock

# Pattern for figure/table/equation references to exclude
FIGURE_TABLE_PATTERN = re.compile(
    r"(?:Fig(?:ure)?|Table|Eq(?:uation)?|Scheme|Chart|Plate|Panel)\s*\.?\s*$",
    re.IGNORECASE,
)

# Bracket citation patterns, ordered by specificity
# These are applied to the full_text string
CITATION_PATTERNS = [
    re.compile(
        r"\["
        r"(\d{1,4}"
        r"(?:\s*[-\u2013\u2014]\s*\d{1,4})?"
        r"(?:\s*[,;]\s*\d{1,4}(?:\s*[-\u2013\u2014]\s*\d{1,4})?)*"
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
    citations: list[InTextCitation] = []
    seen_offsets: set[tuple[int, int]] = set()
    cit_counter = 0

    for block in _iter_body_blocks(doc, refs_start_offset):
        for pattern in CITATION_PATTERNS:
            for match in pattern.finditer(block.text):
                block_local_start = match.start()
                global_start = block.char_offset_start + block_local_start
                seen_key = (block.block_index, block_local_start)
                if seen_key in seen_offsets:
                    continue

                prefix = block.text[max(0, block_local_start - 30) : block_local_start]
                if FIGURE_TABLE_PATTERN.search(prefix):
                    continue

                inner = match.group(1)
                ref_numbers = _parse_citation_inner(inner)
                if not ref_numbers:
                    continue

                if any(n > 999 for n in ref_numbers):
                    continue

                seen_offsets.add(seen_key)
                cit_counter += 1

                citations.append(
                    InTextCitation(
                        citation_id=f"{doc.filename}_cit_{cit_counter}",
                        document_filename=doc.filename,
                        raw_marker=match.group(0),
                        ref_numbers=ref_numbers,
                        page_number=block.page_number,
                        block_index=block.block_index,
                        char_offset_start=global_start,
                        char_offset_end=global_start + len(match.group(0)),
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


def _iter_body_blocks(
    doc: IngestedDocument,
    refs_start_offset: int | None,
) -> list[TextBlock]:
    if refs_start_offset is None:
        return doc.blocks
    return [
        block
        for block in doc.blocks
        if block.char_offset_start < refs_start_offset
    ]
