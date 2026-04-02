"""Stage 5: Extract paragraph contexts for in-text citations."""

from __future__ import annotations

from backend.models.citations import CitingSentence, InTextCitation
from backend.models.ingestion import IngestedDocument


def extract_citing_sentences(
    doc: IngestedDocument,
    citations: list[InTextCitation],
) -> list[CitingSentence]:
    """Extract paragraph contexts that contain citation markers.

    The pipeline keeps the legacy artifact shape for compatibility, but the
    `paragraph` field is now the primary context field and `text` is left blank
    so downstream exports stop implying sentence-level precision.
    """
    if not citations:
        return []

    result: list[CitingSentence] = []
    sent_counter = 0
    block_citations: dict[int, list[str]] = {}

    for cit in citations:
        block_index = cit.block_index
        if block_index is None:
            block_index = _find_block_index_for_offset(doc, cit.char_offset_start)
        if block_index is None:
            continue
        block_citations.setdefault(block_index, []).append(cit.citation_id)

    block_lookup = {block.block_index: block for block in doc.blocks}
    for block_index, cit_ids in sorted(block_citations.items()):
        block = block_lookup.get(block_index)
        if block is None:
            continue
        sent_counter += 1

        result.append(
            CitingSentence(
                sentence_id=f"{doc.filename}_sent_{sent_counter}",
                document_filename=doc.filename,
                text="",
                paragraph=block.text.strip(),
                page_number=block.page_number,
                citation_ids=cit_ids,
                context_before="",
                context_after="",
            )
        )

    return result


def _find_block_index_for_offset(doc: IngestedDocument, offset: int) -> int | None:
    """Find the text block index containing the given character offset."""
    for block in doc.blocks:
        if block.char_offset_start <= offset <= block.char_offset_end:
            return block.block_index
    return None
