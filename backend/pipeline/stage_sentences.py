"""Stage 5: Extract sentences containing in-text citations."""

from __future__ import annotations

import re

from backend.models.citations import CitingSentence, InTextCitation
from backend.models.ingestion import IngestedDocument

# Abbreviations that should NOT be treated as sentence boundaries
ABBREVIATIONS = re.compile(
    r"\b(?:et\s+al|e\.g|i\.e|viz|cf|Dr|Mr|Mrs|Ms|Prof|Jr|Sr|Fig|Eq|Vol|No|pp|vs|Dept|Inc|Ltd|Corp|Univ|approx|ca|etc)\.\s*$",
    re.IGNORECASE,
)

# Sentence-ending punctuation followed by space and uppercase letter or end of text
SENTENCE_BOUNDARY = re.compile(
    r"(?<=[.!?])\s+(?=[A-Z\[\(\"'])"
)

# Newlines also serve as sentence boundaries (block breaks, heading transitions)
NEWLINE_BOUNDARY = re.compile(r"\n")


def extract_citing_sentences(
    doc: IngestedDocument,
    citations: list[InTextCitation],
) -> list[CitingSentence]:
    """Extract sentences that contain citation markers."""
    if not citations:
        return []

    text = doc.full_text
    sentences = _split_into_sentences(text)
    if not sentences:
        return []

    result: list[CitingSentence] = []
    sent_counter = 0
    # Index: sentence_idx -> list of citation_ids
    sentence_citations: dict[int, list[str]] = {}

    for cit in citations:
        # Find which sentence contains this citation
        sent_idx = _find_sentence_for_offset(sentences, cit.char_offset_start)
        if sent_idx is None:
            continue
        if sent_idx not in sentence_citations:
            sentence_citations[sent_idx] = []
        sentence_citations[sent_idx].append(cit.citation_id)

    for sent_idx, cit_ids in sorted(sentence_citations.items()):
        start, end, sent_text = sentences[sent_idx]
        sent_counter += 1

        # Get context
        context_before = ""
        if sent_idx > 0:
            context_before = sentences[sent_idx - 1][2]
        context_after = ""
        if sent_idx + 1 < len(sentences):
            context_after = sentences[sent_idx + 1][2]

        page = _offset_to_page(doc, start)
        paragraph = _offset_to_paragraph(doc, start)

        result.append(
            CitingSentence(
                sentence_id=f"{doc.filename}_sent_{sent_counter}",
                document_filename=doc.filename,
                text=sent_text.strip(),
                paragraph=paragraph,
                page_number=page,
                citation_ids=cit_ids,
                context_before=context_before.strip(),
                context_after=context_after.strip(),
            )
        )

    return result


def _split_into_sentences(text: str) -> list[tuple[int, int, str]]:
    """Split text into sentences with offset tracking.

    Returns list of (start_offset, end_offset, sentence_text).
    """
    if not text:
        return []

    # Split on sentence boundaries but protect abbreviations
    # First, mark abbreviation periods to protect them
    protected = text
    abbrev_markers: list[tuple[int, int]] = []

    for match in ABBREVIATIONS.finditer(text):
        abbrev_markers.append((match.start(), match.end()))

    # Find all potential sentence boundaries
    boundary_set: set[int] = {0}

    # Newlines are hard boundaries (block/heading transitions)
    for match in NEWLINE_BOUNDARY.finditer(text):
        next_pos = match.end()
        if next_pos < len(text):
            boundary_set.add(next_pos)

    # Punctuation-based sentence boundaries
    for match in SENTENCE_BOUNDARY.finditer(text):
        boundary_pos = match.start()
        # Check if this boundary falls within a protected abbreviation
        is_abbreviation = False
        for ab_start, ab_end in abbrev_markers:
            if ab_start <= boundary_pos <= ab_end:
                is_abbreviation = True
                break

        # Check for decimal numbers: "3.14"
        if boundary_pos > 0 and text[boundary_pos - 1] == "." and boundary_pos >= 2:
            before_dot = text[boundary_pos - 2]
            if before_dot.isdigit():
                is_abbreviation = True

        if not is_abbreviation:
            # The boundary is at the space between sentences
            boundary_set.add(match.start() + 1)  # start of next sentence

    boundaries = sorted(boundary_set)
    boundaries.append(len(text))

    sentences: list[tuple[int, int, str]] = []
    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        end = boundaries[i + 1]
        sent_text = text[start:end].strip()
        if sent_text:
            sentences.append((start, end, sent_text))

    return sentences


def _find_sentence_for_offset(
    sentences: list[tuple[int, int, str]],
    offset: int,
) -> int | None:
    """Find the sentence index that contains the given character offset."""
    for i, (start, end, _) in enumerate(sentences):
        if start <= offset < end:
            return i
    return None


def _offset_to_page(doc: IngestedDocument, offset: int) -> int | None:
    """Map a character offset to a page number."""
    for block in doc.blocks:
        if block.char_offset_start <= offset <= block.char_offset_end:
            return block.page_number
    return None


def _offset_to_paragraph(doc: IngestedDocument, offset: int) -> str:
    """Get the full paragraph (TextBlock) text containing the given offset."""
    for block in doc.blocks:
        if block.char_offset_start <= offset <= block.char_offset_end:
            return block.text
    return ""
