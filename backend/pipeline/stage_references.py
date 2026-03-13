"""Stage 2: Detect the references / bibliography section in a document."""

from __future__ import annotations

import re

from backend.models.bibliography import ReferencesSection
from backend.models.ingestion import IngestedDocument

REFERENCE_HEADING_PATTERNS = [
    re.compile(r"^\s*references?\s*$", re.IGNORECASE),
    re.compile(r"^\s*bibliography\s*$", re.IGNORECASE),
    re.compile(r"^\s*works?\s+cited\s*$", re.IGNORECASE),
    re.compile(r"^\s*literature\s+cited\s*$", re.IGNORECASE),
    re.compile(r"^\s*cited\s+references?\s*$", re.IGNORECASE),
    re.compile(r"^\s*notes?\s+and\s+references?\s*$", re.IGNORECASE),
    re.compile(r"^\s*references?\s+cited\s*$", re.IGNORECASE),
    re.compile(r"^\s*curated\s+url\s+list\s*$", re.IGNORECASE),
    re.compile(r"^\s*source\s+links?\s*$", re.IGNORECASE),
    re.compile(r"^\s*endnotes?\s*$", re.IGNORECASE),
]

# Pattern for detecting numbered reference entries
NUMBERED_REF_PATTERN = re.compile(r"^\s*\[?\s*(\d{1,4})\s*\]?[\.\)]\s*[A-Z0-9\"]", re.MULTILINE)


def detect_references_section(doc: IngestedDocument) -> ReferencesSection | None:
    """Detect the references section in a document.

    Strategy 1: Match heading blocks against known reference heading patterns.
    Strategy 2: Find clusters of numbered reference entries near the end.
    """
    # Strategy 1: Heading-based detection (take the LAST match)
    last_heading_match = None
    for i, block in enumerate(doc.blocks):
        if not block.is_heading and len(block.text.split()) > 5:
            continue
        text = block.text.strip()
        for pattern in REFERENCE_HEADING_PATTERNS:
            if pattern.match(text):
                last_heading_match = (i, text)
                break

    if last_heading_match is not None:
        start_idx, heading_text = last_heading_match
        end_idx = _find_section_end(doc, start_idx)
        raw_text = _extract_section_text(doc, start_idx + 1, end_idx)
        return ReferencesSection(
            document_filename=doc.filename,
            start_block_index=start_idx,
            end_block_index=end_idx,
            heading_text=heading_text,
            raw_text=raw_text,
            detection_method="heading_match",
            confidence=1.0,
        )

    # Strategy 2: Pattern-based detection — find dense clusters of [N] entries
    result = _detect_by_numbered_pattern(doc)
    if result is not None:
        return result

    return None


def _find_section_end(doc: IngestedDocument, heading_idx: int) -> int:
    """Find the end of the references section.

    The section ends at:
    - A new heading that is NOT a references heading (e.g., "Appendix")
    - The end of the document
    """
    for i in range(heading_idx + 1, len(doc.blocks)):
        block = doc.blocks[i]
        if block.is_heading:
            text = block.text.strip().lower()
            # Check if this heading is part of the references (e.g. "Additional References")
            is_ref_heading = any(p.match(text) for p in REFERENCE_HEADING_PATTERNS)
            if not is_ref_heading:
                return i - 1

    return len(doc.blocks) - 1


def _extract_section_text(doc: IngestedDocument, start_idx: int, end_idx: int) -> str:
    """Extract the raw text of blocks in the given range."""
    parts = []
    for i in range(start_idx, min(end_idx + 1, len(doc.blocks))):
        parts.append(doc.blocks[i].text)
    return "\n".join(parts)


def _detect_by_numbered_pattern(doc: IngestedDocument) -> ReferencesSection | None:
    """Fallback: detect references section by finding a cluster of numbered entries."""
    if not doc.blocks:
        return None

    # Scan the last 40% of the document
    start_scan = max(0, int(len(doc.blocks) * 0.6))
    best_run_start = None
    best_run_length = 0
    current_run_start = None
    current_run_length = 0

    for i in range(start_scan, len(doc.blocks)):
        block = doc.blocks[i]
        if NUMBERED_REF_PATTERN.search(block.text):
            if current_run_start is None:
                current_run_start = i
                current_run_length = 1
            else:
                current_run_length += 1
        else:
            if current_run_length > best_run_length:
                best_run_start = current_run_start
                best_run_length = current_run_length
            current_run_start = None
            current_run_length = 0

    # Check final run
    if current_run_length > best_run_length:
        best_run_start = current_run_start
        best_run_length = current_run_length

    if best_run_start is not None and best_run_length >= 3:
        end_idx = min(best_run_start + best_run_length - 1, len(doc.blocks) - 1)
        # Extend to end of document (references typically go to the end)
        end_idx = len(doc.blocks) - 1
        raw_text = _extract_section_text(doc, best_run_start, end_idx)
        return ReferencesSection(
            document_filename=doc.filename,
            start_block_index=best_run_start,
            end_block_index=end_idx,
            heading_text="(detected by pattern)",
            raw_text=raw_text,
            detection_method="pattern_cluster",
            confidence=0.7,
        )

    return None
