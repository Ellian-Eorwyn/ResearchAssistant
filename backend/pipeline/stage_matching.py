"""Stage 6: Match in-text citations to bibliography entries."""

from __future__ import annotations

from backend.models.bibliography import BibliographyEntry
from backend.models.citations import CitationMatch, CitingSentence, InTextCitation


def match_citations(
    citations: list[InTextCitation],
    sentences: list[CitingSentence],
    bib_entries: list[BibliographyEntry],
) -> tuple[list[CitationMatch], int, int]:
    """Match each in-text citation to its bibliography entry.

    Returns:
        (matches, unmatched_citations_count, unmatched_bib_entries_count)
    """
    # Build ref_number -> bib entry index lookup
    ref_to_bib: dict[int, int] = {}
    for i, entry in enumerate(bib_entries):
        if entry.ref_number is not None:
            ref_to_bib[entry.ref_number] = i

    # Build citation_id -> sentence_id lookup
    cit_to_sentence: dict[str, str] = {}
    for sent in sentences:
        for cid in sent.citation_ids:
            cit_to_sentence[cid] = sent.sentence_id

    matches: list[CitationMatch] = []
    matched_bib_indices: set[int] = set()
    unmatched_count = 0

    for cit in citations:
        sentence_id = cit_to_sentence.get(cit.citation_id, "")
        for ref_num in cit.ref_numbers:
            bib_idx = ref_to_bib.get(ref_num)
            if bib_idx is not None:
                matches.append(
                    CitationMatch(
                        citation_id=cit.citation_id,
                        ref_number=ref_num,
                        sentence_id=sentence_id,
                        matched_bib_entry_index=bib_idx,
                        match_confidence=1.0,
                        match_method="ref_number",
                    )
                )
                matched_bib_indices.add(bib_idx)
            else:
                matches.append(
                    CitationMatch(
                        citation_id=cit.citation_id,
                        ref_number=ref_num,
                        sentence_id=sentence_id,
                        matched_bib_entry_index=None,
                        match_confidence=0.0,
                        match_method="unmatched",
                        unmatched_reason=f"No bibliography entry with ref_number={ref_num}",
                    )
                )
                unmatched_count += 1

    unmatched_bib = len(bib_entries) - len(matched_bib_indices)
    return matches, unmatched_count, unmatched_bib
