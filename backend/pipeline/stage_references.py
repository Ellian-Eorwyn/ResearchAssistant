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
    # Strategy 1: Heading-based detection (choose the strongest candidate section)
    heading_candidates: list[tuple[int, str]] = []
    for i, block in enumerate(doc.blocks):
        if not block.is_heading and len(block.text.split()) > 5:
            continue
        text = block.text.strip()
        for pattern in REFERENCE_HEADING_PATTERNS:
            if pattern.match(text):
                heading_candidates.append((i, text))
                break

    if heading_candidates:
        scored_candidates = []
        for start_idx, heading_text in heading_candidates:
            end_idx = _find_section_end(doc, start_idx)
            score = _score_section_candidate(doc, start_idx, end_idx, heading_text)
            scored_candidates.append((score, start_idx, end_idx, heading_text))
        score, start_idx, end_idx, heading_text = max(
            scored_candidates,
            key=lambda item: (item[0], item[1]),
        )
        raw_text = _extract_section_text(doc, start_idx + 1, end_idx)
        return ReferencesSection(
            document_filename=doc.filename,
            start_block_index=start_idx,
            end_block_index=end_idx,
            heading_text=heading_text,
            raw_text=raw_text,
            detection_method="heading_match",
            confidence=min(max(score, 0.55), 1.0),
        )

    # Strategy 2: Pattern-based detection — find dense clusters of [N] entries
    result = _detect_by_numbered_pattern(doc)
    if result is not None:
        return result

    return None


def _find_section_end(doc: IngestedDocument, heading_idx: int) -> int:
    """Find the end of the references section.

    The section ends at:
    - The next heading block
    - The end of the document
    """
    for i in range(heading_idx + 1, len(doc.blocks)):
        block = doc.blocks[i]
        if _is_section_separator(block.text):
            return i - 1
        if block.is_heading:
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


def _score_section_candidate(
    doc: IngestedDocument,
    start_idx: int,
    end_idx: int,
    heading_text: str,
) -> float:
    score = 0.35
    heading = (heading_text or "").strip().lower()
    if "works cited" in heading:
        score += 0.3
    elif "bibliography" in heading:
        score += 0.2
    elif "references" in heading:
        score += 0.1

    sample_blocks = doc.blocks[start_idx + 1 : min(end_idx + 1, start_idx + 13)]
    if not sample_blocks:
        return score

    for block in sample_blocks:
        text = block.text.strip()
        if not text:
            continue
        if "http://" in text or "https://" in text:
            score += 0.05
        if '"' in text or "“" in text:
            score += 0.03
        if re.search(r"\b(?:19|20)\d{2}\b", text):
            score += 0.02
        if NUMBERED_REF_PATTERN.search(text):
            score += 0.04
        if "..." in text:
            score -= 0.06
        if "provided as inline citations" in text.lower():
            score += 0.04

    return score


def _is_section_separator(text: str) -> bool:
    candidate = (text or "").strip()
    return bool(candidate) and len(candidate) >= 8 and len(set(candidate)) == 1 and candidate[0] in "-_=*"
