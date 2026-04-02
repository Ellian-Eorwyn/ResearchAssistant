"""Profile-driven document normalization into standardized markdown."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from backend.llm.client import UnifiedLLMClient
from backend.llm.prompts import (
    DOCUMENT_NORMALIZATION_SYSTEM,
    DOCUMENT_NORMALIZATION_USER,
)
from backend.models.bibliography import BibliographyEntry, ReferencesSection
from backend.models.ingestion import IngestedDocument, TextBlock
from backend.models.ingestion_profiles import (
    DocumentNormalizationResult,
    IngestionProfile,
    IngestionProfileSuggestion,
)
from backend.models.settings import LLMBackendConfig

_NUMERIC_CITATION_PATTERN = re.compile(
    r"\["
    r"(\d{1,4}"
    r"(?:\s*[-\u2013\u2014]\s*\d{1,4})?"
    r"(?:\s*,\s*\d{1,4}(?:\s*[-\u2013\u2014]\s*\d{1,4})?)*"
    r")\]"
)
_DOT_NUMBER_PATTERN = re.compile(r"(?<=[.!?])\s*(\d{1,3})(?=(?:\s|$))")
_SUPERSCRIPT_MARKDOWN_PATTERN = re.compile(r"(?<!\d)\.(\d{1,3})(?=(?:\s|$))")
_FOOTNOTE_MARKER_PATTERN = re.compile(r"(?:\[\^(\d{1,4})\]|\^(\d{1,4}))")
_AUTHOR_YEAR_PATTERN = re.compile(
    r"\(([^)]*\b(?:19|20)\d{2}[a-z]?\b[^)]*)\)"
)
_URL_PATTERN = re.compile(r"https?://\S+")
_SETEXT_HEADING = re.compile(r"^(?P<title>.+)\n(?P<rule>=+|-+)\s*$", re.MULTILINE)
_SENTENCE_BOUNDARY = re.compile(r"(?<=[.!?])\s+(?=[A-Z\[\(\"'])")
_WHITESPACE = re.compile(r"\s+")
_YEAR_PATTERN = re.compile(r"\b((?:19|20)\d{2}[a-z]?)\b")
_BLOCK_TABLE_LINE = re.compile(r"^\s*\|.+\|\s*$")
_BLOCK_BULLET_LINE = re.compile(r"^\s*(?:[-*+]|\d+\.)\s+")
_REFERENCE_HEADINGS = [
    r"references?",
    r"bibliography",
    r"works?\s+cited",
    r"literature\s+cited",
    r"endnotes?",
    r"notes?\s+and\s+references?",
]


@dataclass
class NormalizedDocumentOutput:
    markdown_text: str
    result: DocumentNormalizationResult
    suggestion: IngestionProfileSuggestion | None = None


def standardized_markdown_filename(filename: str) -> str:
    path = Path(filename or "document")
    stem = path.stem or "document"
    return f"{stem}.standardized.md"


def standardized_metadata_filename(filename: str) -> str:
    path = Path(filename or "document")
    stem = path.stem or "document"
    return f"{stem}.standardized.json"


def builtin_ingestion_profiles() -> list[IngestionProfile]:
    return [
        IngestionProfile(
            profile_id="generic_numeric_academic",
            label="Generic Numeric Academic",
            description="Bracketed or superscript numeric citation styles with numbered references.",
            built_in=True,
            file_type_hints=["pdf", "docx", "md"],
            reference_heading_patterns=_REFERENCE_HEADINGS,
            citation_marker_patterns=[r"\[\d+\]", r"(?<=[.!?])\s*\d+$", r"\[\^\d+\]"],
            bibliography_split_patterns=[r"^\s*\[\d+\]", r"^\s*\d+\."],
            llm_guidance="Normalize numeric or superscript citations into sentence-end bracketed references.",
            confidence_threshold=0.62,
        ),
        IngestionProfile(
            profile_id="generic_author_year_academic",
            label="Generic Author-Year Academic",
            description="APA/Chicago-like author-year parenthetical citations with bibliography matching.",
            built_in=True,
            file_type_hints=["pdf", "docx", "md"],
            reference_heading_patterns=_REFERENCE_HEADINGS,
            citation_marker_patterns=[r"\([^)]*\b(?:19|20)\d{2}[a-z]?\b[^)]*\)"],
            bibliography_split_patterns=[r"^\s*[A-Z][A-Za-z\-']+.*\(\d{4}"],
            llm_guidance="Resolve author-year parentheticals to stable numeric references when bibliography matching is possible.",
            confidence_threshold=0.66,
        ),
        IngestionProfile(
            profile_id="footnote_endnote_report",
            label="Footnote / Endnote Report",
            description="Reports with footnote markers and notes/endnotes sections.",
            built_in=True,
            file_type_hints=["pdf", "docx", "md"],
            reference_heading_patterns=[*_REFERENCE_HEADINGS, r"footnotes?"],
            citation_marker_patterns=[r"\[\^\d+\]", r"(?<=[.!?])\s*\d+$"],
            bibliography_split_patterns=[r"^\s*\d+\.", r"^\s*\[\d+\]"],
            llm_guidance="Treat footnotes/endnotes as bibliography entries and normalize body markers into bracketed numeric citations.",
            confidence_threshold=0.64,
        ),
        IngestionProfile(
            profile_id="llm_deep_research_markdown",
            label="LLM Deep Research Markdown",
            description="Markdown research reports with numbered citations and a final works-cited list.",
            built_in=True,
            file_type_hints=["md"],
            reference_heading_patterns=[r"works?\s+cited", r"references?"],
            citation_marker_patterns=[r"\[\d+\]", r"(?<=[.!?])\s*\d+$", r"(?<!\d)\.\d+"],
            bibliography_split_patterns=[r"^\s*\d+\.", r"^\s*\[\d+\]"],
            llm_guidance="Preserve markdown structure and normalize in-text citations to sentence-end bracketed numbers.",
            confidence_threshold=0.68,
        ),
    ]


def normalize_document_to_standardized_markdown(
    document: IngestedDocument,
    bibliography_entries: list[BibliographyEntry],
    references_section: ReferencesSection | None = None,
    *,
    builtin_profiles: list[IngestionProfile] | None = None,
    custom_profiles: list[IngestionProfile] | None = None,
    profile_override: str = "",
    use_llm: bool = False,
    llm_backend: LLMBackendConfig | None = None,
    research_purpose: str = "",
) -> NormalizedDocumentOutput:
    profiles = [*(builtin_profiles or builtin_ingestion_profiles()), *(custom_profiles or [])]
    selected_profile, profile_confidence = _select_profile(
        document=document,
        references_section=references_section,
        bibliography_entries=bibliography_entries,
        profiles=profiles,
        profile_override=profile_override,
    )
    canonical_entries = _canonicalize_entries(bibliography_entries)
    body_blocks = _select_body_blocks(document.blocks, references_section)
    normalized_blocks, citation_stats = _normalize_blocks(
        body_blocks=body_blocks,
        entries=canonical_entries,
        profile=selected_profile,
    )
    ordering = _build_citation_ordering(canonical_entries, citation_stats)
    rendered_body = _render_blocks(normalized_blocks, ordering)
    works_cited_text, linked_entries = _render_works_cited(canonical_entries, ordering)
    warnings = list(document.warnings)
    warnings.extend(citation_stats["warnings"])

    confidence_score = _calculate_confidence(
        profile_confidence=profile_confidence,
        references_section_detected=references_section is not None,
        entry_count=len(canonical_entries),
        linked_entry_count=linked_entries,
        total_markers=citation_stats["total_markers"],
        matched_markers=citation_stats["matched_markers"],
        unresolved_markers=citation_stats["unresolved_markers"],
        structure_score=_estimate_structure_quality(body_blocks),
    )

    used_llm_fallback = False
    suggestion: IngestionProfileSuggestion | None = None
    llm_note = ""
    if _should_use_llm_fallback(
        use_llm=use_llm,
        llm_backend=llm_backend,
        confidence_score=confidence_score,
        threshold=selected_profile.confidence_threshold,
        citation_stats=citation_stats,
    ):
        llm_output = _run_llm_normalization(
            document=document,
            body_blocks=body_blocks,
            bibliography_entries=canonical_entries,
            profile=selected_profile,
            llm_backend=llm_backend or LLMBackendConfig(),
            research_purpose=research_purpose,
            deterministic_analysis={
                "confidence_score": confidence_score,
                "citation_stats": citation_stats,
                "linked_entries": linked_entries,
                "references_section_detected": references_section is not None,
            },
        )
        if llm_output is not None:
            llm_blocks = llm_output.get("blocks", [])
            if llm_blocks:
                rendered_body = _render_llm_blocks(llm_blocks)
            llm_works_cited = llm_output.get("works_cited", [])
            if llm_works_cited:
                works_cited_text, linked_entries = _render_llm_works_cited(llm_works_cited)
            used_llm_fallback = True
            warnings.extend(_string_list(llm_output.get("warnings")))
            unresolved_from_llm = _string_list(llm_output.get("unresolved_markers"))
            citation_stats["unresolved_markers"] = len(unresolved_from_llm)
            citation_stats["matched_markers"] = max(
                citation_stats["matched_markers"],
                max(citation_stats["total_markers"] - len(unresolved_from_llm), 0),
            )
            confidence_score = max(confidence_score, selected_profile.confidence_threshold)
            llm_note = "LLM fallback used for normalization."
            suggestion = _build_profile_suggestion_from_llm(
                document=document,
                profile=selected_profile,
                raw_suggestion=llm_output.get("profile_suggestion"),
            )
        else:
            warnings.append("LLM fallback unavailable or failed; kept deterministic normalization.")

    markdown_parts = [part for part in [rendered_body, works_cited_text] if part]
    markdown_text = "\n\n".join(markdown_parts).strip()
    if markdown_text:
        markdown_text += "\n"

    result_status = "normalized"
    error_message = ""
    if not markdown_text:
        result_status = "failed"
        error_message = "No normalized markdown could be generated."
    elif citation_stats["unresolved_markers"] > 0:
        result_status = "partial"
    if llm_note:
        warnings.append(llm_note)
    if suggestion is not None:
        warnings.append("Suggested ingestion profile update available for review.")

    result = DocumentNormalizationResult(
        filename=document.filename,
        selected_profile_id=selected_profile.profile_id,
        selected_profile_label=selected_profile.label,
        status=result_status,
        confidence_score=round(confidence_score, 2),
        used_llm_fallback=used_llm_fallback,
        bibliography_entry_count=len(canonical_entries),
        total_citation_markers=citation_stats["total_markers"],
        matched_citation_markers=citation_stats["matched_markers"],
        unresolved_citation_markers=citation_stats["unresolved_markers"],
        reference_section_detected=references_section is not None,
        works_cited_linked_entries=linked_entries,
        suggestion_ids=[suggestion.suggestion_id] if suggestion is not None else [],
        warnings=_dedupe_strings(warnings),
        error_message=error_message,
    )
    return NormalizedDocumentOutput(markdown_text=markdown_text, result=result, suggestion=suggestion)


def extract_markdown_title_candidate(markdown_text: str) -> str:
    text = (markdown_text or "").strip()
    if not text:
        return ""

    front_matter = _extract_front_matter(text)
    if front_matter:
        title = _extract_front_matter_title(front_matter)
        if title:
            return title

    heading_match = re.search(r"^\s{0,3}#\s+(.+?)\s*$", text, flags=re.MULTILINE)
    if heading_match:
        return _normalize_title_text(heading_match.group(1))

    setext_match = _SETEXT_HEADING.search(text)
    if setext_match:
        return _normalize_title_text(setext_match.group("title"))

    return ""


def _select_profile(
    *,
    document: IngestedDocument,
    references_section: ReferencesSection | None,
    bibliography_entries: list[BibliographyEntry],
    profiles: list[IngestionProfile],
    profile_override: str,
) -> tuple[IngestionProfile, float]:
    if profile_override:
        for profile in profiles:
            if profile.profile_id == profile_override:
                return profile, 1.0

    best_profile = profiles[0]
    best_score = -1.0
    body_text = document.full_text or ""
    heading_text = references_section.heading_text if references_section is not None else ""
    bibliography_text = references_section.raw_text if references_section is not None else "\n".join(
        entry.raw_text for entry in bibliography_entries[:10]
    )
    for profile in profiles:
        score = 0.0
        if document.file_type in profile.file_type_hints:
            score += 0.3
        for pattern in profile.reference_heading_patterns:
            try:
                if heading_text and re.search(pattern, heading_text, flags=re.IGNORECASE):
                    score += 0.2
                    break
            except re.error:
                continue
        marker_hits = 0
        for pattern in profile.citation_marker_patterns:
            try:
                if re.search(pattern, body_text):
                    marker_hits += 1
            except re.error:
                continue
        score += min(marker_hits * 0.15, 0.3)
        split_hits = 0
        for pattern in profile.bibliography_split_patterns:
            try:
                if bibliography_text and re.search(pattern, bibliography_text, flags=re.MULTILINE):
                    split_hits += 1
            except re.error:
                continue
        score += min(split_hits * 0.1, 0.2)
        if score > best_score:
            best_profile = profile
            best_score = score
    return best_profile, max(0.25, min(best_score, 1.0))


def _canonicalize_entries(entries: list[BibliographyEntry]) -> list[BibliographyEntry]:
    canonical: list[BibliographyEntry] = []
    for index, entry in enumerate(entries, start=1):
        copy = entry.model_copy(deep=True)
        if not copy.ref_number:
            copy.ref_number = index
        if copy.doi and not copy.url:
            copy.url = f"https://doi.org/{copy.doi.strip()}"
        canonical.append(copy)
    canonical.sort(key=lambda item: (item.ref_number or 10**9, item.title or item.raw_text))
    return canonical


def _select_body_blocks(
    blocks: list[TextBlock],
    references_section: ReferencesSection | None,
) -> list[TextBlock]:
    if references_section is None:
        return list(blocks)
    cutoff = max(0, int(references_section.start_block_index))
    return list(blocks[:cutoff])


def _normalize_blocks(
    *,
    body_blocks: list[TextBlock],
    entries: list[BibliographyEntry],
    profile: IngestionProfile,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    numeric_ref_map = {int(entry.ref_number or 0): entry for entry in entries if entry.ref_number}
    author_year_index = _build_author_year_index(entries)
    blocks: list[dict[str, Any]] = []
    total_markers = 0
    matched_markers = 0
    unresolved_markers = 0
    warnings: list[str] = []

    for block in body_blocks:
        raw_text = (block.text or "").strip()
        if not raw_text:
            continue
        if block.is_heading:
            blocks.append({"kind": "heading", "level": min(max(block.heading_level or 1, 1), 6), "text": raw_text, "citations": []})
            continue

        lines = [line.rstrip() for line in raw_text.splitlines() if line.strip()]
        if lines and all(_BLOCK_TABLE_LINE.match(line) for line in lines):
            for line in lines:
                blocks.append({"kind": "table_row", "text": line, "citations": []})
            continue
        if lines and all(_BLOCK_BULLET_LINE.match(line) for line in lines):
            for line in lines:
                text, citations, stats = _normalize_line(line, numeric_ref_map, author_year_index, profile)
                total_markers += stats["total"]
                matched_markers += stats["matched"]
                unresolved_markers += stats["unresolved"]
                warnings.extend(stats["warnings"])
                blocks.append({"kind": "list_item", "text": text, "citations": citations})
            continue

        normalized_text, citations, stats = _normalize_line(raw_text, numeric_ref_map, author_year_index, profile)
        total_markers += stats["total"]
        matched_markers += stats["matched"]
        unresolved_markers += stats["unresolved"]
        warnings.extend(stats["warnings"])
        blocks.append({"kind": "paragraph", "text": normalized_text, "citations": citations})

    return blocks, {
        "total_markers": total_markers,
        "matched_markers": matched_markers,
        "unresolved_markers": unresolved_markers,
        "warnings": _dedupe_strings(warnings),
    }


def _normalize_line(
    text: str,
    numeric_ref_map: dict[int, BibliographyEntry],
    author_year_index: dict[tuple[str, str], int],
    profile: IngestionProfile,
) -> tuple[str, list[int], dict[str, Any]]:
    citations: list[int] = []
    warnings: list[str] = []
    total_markers = 0
    matched_markers = 0
    unresolved_markers = 0

    def _replace_numeric(match: re.Match[str]) -> str:
        nonlocal total_markers, matched_markers, unresolved_markers
        total_markers += 1
        numbers = _parse_citation_numbers(match.group(1))
        matched_here = [number for number in numbers if number in numeric_ref_map]
        if matched_here:
            citations.extend(matched_here)
            matched_markers += 1
        else:
            unresolved_markers += 1
        return ""

    normalized = _NUMERIC_CITATION_PATTERN.sub(_replace_numeric, text or "")

    def _replace_superscript(match: re.Match[str]) -> str:
        nonlocal total_markers, matched_markers, unresolved_markers
        number_text = match.group(1) or match.group(2) or ""
        if not number_text.isdigit():
            return ""
        number = int(number_text)
        total_markers += 1
        if number in numeric_ref_map:
            citations.append(number)
            matched_markers += 1
        else:
            unresolved_markers += 1
        return ""

    normalized = _FOOTNOTE_MARKER_PATTERN.sub(_replace_superscript, normalized)
    normalized = _SUPERSCRIPT_MARKDOWN_PATTERN.sub(_replace_superscript, normalized)
    normalized = _DOT_NUMBER_PATTERN.sub(_replace_superscript, normalized)

    if profile.profile_id == "generic_author_year_academic":
        normalized = _AUTHOR_YEAR_PATTERN.sub(
            lambda match: _replace_author_year_parenthetical(
                match,
                citations=citations,
                author_year_index=author_year_index,
                counters={
                    "total": lambda: total_markers,
                    "matched": lambda: matched_markers,
                    "unresolved": lambda: unresolved_markers,
                },
                warnings=warnings,
            ),
            normalized,
        )
        # Counters are mutated inside helper via list wrapper.
        # Recompute from captured values encoded in warnings footer is awkward; do it directly.
        author_year_counts = _count_author_year_markers(text, author_year_index)
        total_markers += author_year_counts["total"]
        matched_markers += author_year_counts["matched"]
        unresolved_markers += author_year_counts["unresolved"]
        citations.extend(author_year_counts["citations"])

    deduped_citations = sorted({value for value in citations if value > 0})
    cleaned = _collapse_whitespace(normalized)
    if deduped_citations:
        citation_text = "[" + ", ".join(str(value) for value in deduped_citations) + "]"
        cleaned = f"{cleaned} {citation_text}".strip() if cleaned else citation_text
    return cleaned, deduped_citations, {
        "total": total_markers,
        "matched": matched_markers,
        "unresolved": unresolved_markers,
        "warnings": warnings,
    }


def _replace_author_year_parenthetical(
    match: re.Match[str],
    *,
    citations: list[int],
    author_year_index: dict[tuple[str, str], int],
    counters: dict[str, Any],
    warnings: list[str],
) -> str:
    del counters, warnings
    # Counts are computed in _count_author_year_markers to keep the substitution pure.
    content = match.group(1)
    resolved = _resolve_author_year_segment(content, author_year_index)
    citations.extend(resolved)
    return ""


def _count_author_year_markers(
    text: str,
    author_year_index: dict[tuple[str, str], int],
) -> dict[str, Any]:
    total = 0
    matched = 0
    unresolved = 0
    citations: list[int] = []
    for match in _AUTHOR_YEAR_PATTERN.finditer(text or ""):
        total += 1
        resolved = _resolve_author_year_segment(match.group(1), author_year_index)
        if resolved:
            matched += 1
            citations.extend(resolved)
        else:
            unresolved += 1
    return {"total": total, "matched": matched, "unresolved": unresolved, "citations": citations}


def _resolve_author_year_segment(
    content: str,
    author_year_index: dict[tuple[str, str], int],
) -> list[int]:
    resolved: list[int] = []
    for part in re.split(r";\s*", content or ""):
        year_match = _YEAR_PATTERN.search(part)
        if not year_match:
            continue
        year = year_match.group(1)
        surnames = re.findall(r"\b([A-Z][A-Za-z'\-]+)\b", part)
        local_matches: list[int] = []
        for surname in surnames:
            key = (surname.lower(), year)
            if key in author_year_index:
                local_matches.append(author_year_index[key])
        if local_matches:
            resolved.append(local_matches[0])
    return resolved


def _build_author_year_index(entries: list[BibliographyEntry]) -> dict[tuple[str, str], int]:
    index: dict[tuple[str, str], int] = {}
    for entry in entries:
        year = (entry.year or "").strip()
        if not year:
            continue
        for author in entry.authors:
            surname = author.strip().split(",")[0].split()[-1].lower() if author.strip() else ""
            if surname:
                index[(surname, year)] = int(entry.ref_number or 0)
    return index


def _build_citation_ordering(
    entries: list[BibliographyEntry],
    citation_stats: dict[str, Any],
) -> dict[int, int]:
    del citation_stats
    ordering: dict[int, int] = {}
    for position, entry in enumerate(entries, start=1):
        if entry.ref_number:
            ordering[int(entry.ref_number)] = position
    return ordering


def _render_blocks(blocks: list[dict[str, Any]], ordering: dict[int, int]) -> str:
    rendered: list[str] = []
    for block in blocks:
        kind = block.get("kind")
        text = str(block.get("text") or "").strip()
        if not text:
            continue
        citations = [ordering.get(value, value) for value in block.get("citations", []) if value]
        citations = sorted({value for value in citations if value})
        if kind == "heading":
            level = min(max(int(block.get("level") or 1), 1), 6)
            rendered.append(f"{'#' * level} {text}")
            continue
        if kind == "list_item":
            rendered.append(_append_citations_to_text(text, citations, prefix="- "))
            continue
        if kind == "table_row":
            rendered.append(text)
            continue
        rendered.append(_append_citations_to_text(text, citations))
    return "\n\n".join(rendered).strip()


def _append_citations_to_text(text: str, citations: list[int], prefix: str = "") -> str:
    rendered = f"{prefix}{text}".strip()
    if not citations:
        return rendered
    citation_text = "[" + ", ".join(str(value) for value in citations) + "]"
    if rendered.endswith((".", "!", "?")):
        return f"{rendered} {citation_text}"
    return f"{rendered} {citation_text}".strip()


def _render_works_cited(
    entries: list[BibliographyEntry],
    ordering: dict[int, int],
) -> tuple[str, int]:
    if not entries:
        return "", 0
    lines = ["## Works Cited", ""]
    linked_entries = 0
    ordered_entries = sorted(entries, key=lambda entry: ordering.get(int(entry.ref_number or 0), 10**9))
    for index, entry in enumerate(ordered_entries, start=1):
        citation_text = _format_bibliography_entry(entry)
        link = _entry_link(entry)
        if link:
            linked_entries += 1
            lines.append(f"{index}. {citation_text} [Source]({link})")
        else:
            lines.append(f"{index}. {citation_text}")
    return "\n".join(lines).strip(), linked_entries


def _render_llm_blocks(blocks: list[dict[str, Any]]) -> str:
    rendered: list[str] = []
    for raw_block in blocks:
        kind = str(raw_block.get("kind") or "paragraph")
        text = _collapse_whitespace(str(raw_block.get("text") or ""))
        if not text:
            continue
        citations = [int(value) for value in raw_block.get("citations", []) if str(value).isdigit()]
        if kind == "heading":
            level = min(max(int(raw_block.get("level") or 1), 1), 6)
            rendered.append(f"{'#' * level} {text}")
        elif kind == "list_item":
            rendered.append(_append_citations_to_text(text, citations, prefix="- "))
        elif kind == "table_row":
            rendered.append(text)
        else:
            rendered.append(_append_citations_to_text(text, citations))
    return "\n\n".join(rendered).strip()


def _render_llm_works_cited(entries: list[dict[str, Any]]) -> tuple[str, int]:
    if not entries:
        return "", 0
    lines = ["## Works Cited", ""]
    linked_entries = 0
    sorted_entries = sorted(entries, key=lambda item: int(item.get("number") or 10**9))
    for index, entry in enumerate(sorted_entries, start=1):
        citation_text = _collapse_whitespace(str(entry.get("citation_text") or "Unresolved citation."))
        link = str(entry.get("url") or "").strip()
        doi = str(entry.get("doi") or "").strip()
        if not link and doi:
            link = f"https://doi.org/{doi}"
        if link:
            linked_entries += 1
            lines.append(f"{index}. {citation_text} [Source]({link})")
        else:
            lines.append(f"{index}. {citation_text}")
    return "\n".join(lines).strip(), linked_entries


def _calculate_confidence(
    *,
    profile_confidence: float,
    references_section_detected: bool,
    entry_count: int,
    linked_entry_count: int,
    total_markers: int,
    matched_markers: int,
    unresolved_markers: int,
    structure_score: float,
) -> float:
    score = 0.0
    score += min(max(profile_confidence, 0.0), 1.0) * 0.15
    if references_section_detected:
        score += 0.15
    if entry_count > 0:
        score += 0.15
    if entry_count > 0:
        score += min(linked_entry_count / max(entry_count, 1), 1.0) * 0.15
    if total_markers > 0:
        score += min(matched_markers / total_markers, 1.0) * 0.25
        score -= min(unresolved_markers / total_markers, 1.0) * 0.1
    else:
        score += 0.1
    score += min(max(structure_score, 0.0), 1.0) * 0.15
    return max(0.0, min(score, 1.0))


def _estimate_structure_quality(blocks: list[TextBlock]) -> float:
    if not blocks:
        return 0.0
    heading_count = sum(1 for block in blocks if block.is_heading)
    structured_count = sum(
        1
        for block in blocks
        if _BLOCK_TABLE_LINE.search(block.text or "") or _BLOCK_BULLET_LINE.search(block.text or "")
    )
    score = 0.45
    if heading_count > 0:
        score += 0.3
    if structured_count > 0:
        score += 0.25
    return min(score, 1.0)


def _should_use_llm_fallback(
    *,
    use_llm: bool,
    llm_backend: LLMBackendConfig | None,
    confidence_score: float,
    threshold: float,
    citation_stats: dict[str, Any],
) -> bool:
    if not use_llm or llm_backend is None:
        return False
    if not _llm_backend_ready_for_chat(llm_backend):
        return False
    if confidence_score < threshold:
        return True
    if citation_stats["total_markers"] > 0 and citation_stats["matched_markers"] < citation_stats["total_markers"]:
        return True
    return False


def _run_llm_normalization(
    *,
    document: IngestedDocument,
    body_blocks: list[TextBlock],
    bibliography_entries: list[BibliographyEntry],
    profile: IngestionProfile,
    llm_backend: LLMBackendConfig,
    research_purpose: str,
    deterministic_analysis: dict[str, Any],
) -> dict[str, Any] | None:
    try:
        client = UnifiedLLMClient(llm_backend)
        try:
            profile_json = json.dumps(profile.model_dump(mode="json"), ensure_ascii=False, indent=2)
            analysis_json = json.dumps(deterministic_analysis, ensure_ascii=False, indent=2)
            bibliography_context = _format_bibliography_context(bibliography_entries)
            body_text = _serialize_blocks_for_llm(body_blocks) or (document.full_text or "")

            max_chars = max(4000, min(llm_backend.max_source_chars or 30000, 90000))
            prompt_overhead = len(profile_json) + len(analysis_json) + len(bibliography_context) + 2500
            chunk_target = max(2500, max_chars - prompt_overhead)
            chunks = _chunk_text_for_llm(body_blocks, chunk_target) if len(body_text) > chunk_target else [body_text]

            if len(chunks) == 1:
                return _run_single_llm_normalization_request(
                    client=client,
                    profile_json=profile_json,
                    filename=document.filename,
                    research_purpose=research_purpose,
                    analysis_json=analysis_json,
                    bibliography_context=bibliography_context,
                    document_scope="full document",
                    document_text=chunks[0],
                )

            merged_blocks: list[dict[str, Any]] = []
            warnings: list[str] = []
            unresolved_markers: list[str] = []
            profile_suggestion: dict[str, Any] | None = None

            for index, chunk_text in enumerate(chunks, start=1):
                payload = _run_single_llm_normalization_request(
                    client=client,
                    profile_json=profile_json,
                    filename=document.filename,
                    research_purpose=research_purpose,
                    analysis_json=analysis_json,
                    bibliography_context=bibliography_context,
                    document_scope=f"body chunk {index} of {len(chunks)}",
                    document_text=chunk_text,
                )
                if payload is None:
                    return None
                merged_blocks.extend(payload.get("blocks", []))
                warnings.extend(_string_list(payload.get("warnings")))
                unresolved_markers.extend(_string_list(payload.get("unresolved_markers")))
                if profile_suggestion is None and isinstance(payload.get("profile_suggestion"), dict):
                    profile_suggestion = payload.get("profile_suggestion")

            return {
                "blocks": merged_blocks,
                "works_cited": [],
                "warnings": _dedupe_strings(warnings),
                "unresolved_markers": _dedupe_strings(unresolved_markers),
                "profile_suggestion": profile_suggestion or {},
            }
        finally:
            client.sync_close()
    except Exception:
        return None


def _run_single_llm_normalization_request(
    *,
    client: UnifiedLLMClient,
    profile_json: str,
    filename: str,
    research_purpose: str,
    analysis_json: str,
    bibliography_context: str,
    document_scope: str,
    document_text: str,
) -> dict[str, Any] | None:
    user_prompt = DOCUMENT_NORMALIZATION_USER.format(
        profile_json=profile_json,
        filename=filename,
        research_purpose=research_purpose or "No explicit research purpose provided.",
        analysis_json=analysis_json,
        bibliography_context=bibliography_context or "[]",
        document_scope=document_scope,
        document_text=document_text,
    )
    response = client.sync_chat_completion(
        system_prompt=DOCUMENT_NORMALIZATION_SYSTEM,
        user_prompt=user_prompt,
        response_format="json",
    ).strip()
    payload = json.loads(response)
    return payload if isinstance(payload, dict) else None


def _format_bibliography_context(entries: list[BibliographyEntry]) -> str:
    if not entries:
        return "[]"
    lines: list[str] = []
    for entry in sorted(entries, key=lambda item: int(item.ref_number or 10**9)):
        parts: list[str] = [f"[{int(entry.ref_number or 0)}]"]
        author_text = "; ".join(author.strip() for author in entry.authors if author.strip())
        if author_text:
            parts.append(author_text)
        if entry.title.strip():
            parts.append(entry.title.strip())
        if entry.year.strip():
            parts.append(entry.year.strip())
        link = _entry_link(entry)
        if link:
            parts.append(link)
        elif entry.journal_or_source.strip():
            parts.append(entry.journal_or_source.strip())
        lines.append(" | ".join(parts))
    return "\n".join(lines)


def _serialize_blocks_for_llm(blocks: list[TextBlock]) -> str:
    rendered: list[str] = []
    for block in blocks:
        text = str(block.text or "").strip()
        if not text:
            continue
        if block.is_heading:
            level = min(max(int(block.heading_level or 1), 1), 6)
            rendered.append(f"{'#' * level} {text}")
        else:
            rendered.append(text)
    return "\n\n".join(rendered).strip()


def _chunk_text_for_llm(blocks: list[TextBlock], chunk_target: int) -> list[str]:
    if not blocks:
        return [""]
    safe_target = max(2500, chunk_target)
    chunks: list[str] = []
    current_blocks: list[TextBlock] = []
    current_length = 0

    for block in blocks:
        block_text = _serialize_blocks_for_llm([block])
        block_length = len(block_text) + (2 if current_blocks else 0)
        if current_blocks and current_length + block_length > safe_target:
            chunks.append(_serialize_blocks_for_llm(current_blocks))
            current_blocks = [block]
            current_length = len(block_text)
            continue
        current_blocks.append(block)
        current_length += block_length

    if current_blocks:
        chunks.append(_serialize_blocks_for_llm(current_blocks))
    return [chunk for chunk in chunks if chunk]


def _build_profile_suggestion_from_llm(
    *,
    document: IngestedDocument,
    profile: IngestionProfile,
    raw_suggestion: Any,
) -> IngestionProfileSuggestion | None:
    if not isinstance(raw_suggestion, dict):
        return None
    label = _collapse_whitespace(str(raw_suggestion.get("label") or ""))
    if not label:
        return None
    proposed = IngestionProfile(
        profile_id=f"custom_{_slug(label)}",
        label=label,
        description=_collapse_whitespace(str(raw_suggestion.get("description") or "")),
        built_in=False,
        file_type_hints=[document.file_type],
        reference_heading_patterns=_string_list(raw_suggestion.get("reference_heading_patterns")),
        citation_marker_patterns=_string_list(raw_suggestion.get("citation_marker_patterns")),
        bibliography_split_patterns=_string_list(raw_suggestion.get("bibliography_split_patterns")),
        llm_guidance=_collapse_whitespace(str(raw_suggestion.get("llm_guidance") or "")),
        confidence_threshold=profile.confidence_threshold,
        notes=["Suggested from successful LLM fallback normalization."],
    )
    return IngestionProfileSuggestion(
        suggestion_id=f"suggest_{uuid.uuid4().hex[:10]}",
        proposed_profile=proposed,
        source_profile_id=profile.profile_id,
        reason="LLM fallback identified formatting guidance that may help similar documents.",
        example_filename=document.filename,
        example_excerpt=_collapse_whitespace((document.full_text or "")[:280]),
    )


def _parse_citation_numbers(inner: str) -> list[int]:
    numbers: list[int] = []
    for part in re.split(r"\s*,\s*", inner.strip()):
        range_match = re.match(r"(\d+)\s*[-\u2013\u2014]\s*(\d+)", part)
        if range_match:
            start = int(range_match.group(1))
            end = int(range_match.group(2))
            if end < start or end - start > 50:
                continue
            numbers.extend(range(start, end + 1))
            continue
        try:
            numbers.append(int(part))
        except ValueError:
            continue
    return numbers


def _format_bibliography_entry(entry: BibliographyEntry) -> str:
    segments: list[str] = []
    authors = "; ".join(author.strip() for author in entry.authors if author.strip())
    if authors:
        segments.append(authors)
    if entry.title.strip():
        segments.append(entry.title.strip())
    if entry.journal_or_source.strip():
        segments.append(entry.journal_or_source.strip())
    publication_bits = [value.strip() for value in [entry.volume, entry.issue, entry.pages] if value.strip()]
    if publication_bits:
        segments.append(", ".join(publication_bits))
    if entry.year.strip():
        segments.append(entry.year.strip())
    if entry.doi.strip():
        segments.append(f"DOI: {entry.doi.strip()}")
    if not segments:
        raw_text = _URL_PATTERN.sub("", entry.raw_text or "").strip()
        raw_text = _WHITESPACE.sub(" ", raw_text).strip(" .;")
        return (raw_text + ".") if raw_text else "Unresolved citation."
    return ". ".join(segment.strip().strip(" .;") for segment in segments if segment.strip()) + "."


def _entry_link(entry: BibliographyEntry) -> str:
    if entry.url.strip():
        return entry.url.strip()
    if entry.doi.strip():
        return f"https://doi.org/{entry.doi.strip()}"
    return ""


def _collapse_whitespace(text: str) -> str:
    compact = _WHITESPACE.sub(" ", (text or "")).strip()
    compact = re.sub(r"\s+([,.;:!?])", r"\1", compact)
    compact = re.sub(r"([(\[]) ", r"\1", compact)
    compact = re.sub(r" ([)\]])", r"\1", compact)
    return compact.strip(" ,")


def _truncate_for_llm(text: str, max_chars: int) -> str:
    limit = max(4000, min(max_chars or 30000, 90000))
    if len(text) <= limit:
        return text
    head = int(limit * 0.65)
    tail = int(limit * 0.25)
    return text[:head] + "\n\n[... truncated ...]\n\n" + text[-tail:]


def _llm_backend_ready_for_chat(config: LLMBackendConfig) -> bool:
    if not config.model.strip():
        return False
    if not config.base_url.strip():
        return False
    return config.kind in {"openai", "ollama"}


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


def _dedupe_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        cleaned = str(value or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        ordered.append(cleaned)
    return ordered


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_") or "profile"


def _extract_front_matter(text: str) -> str:
    if not text.startswith("---"):
        return ""
    lines = text.splitlines()
    if len(lines) < 3:
        return ""
    collected: list[str] = []
    for line in lines[1:]:
        if line.strip() == "---":
            return "\n".join(collected)
        collected.append(line)
    return ""


def _extract_front_matter_title(front_matter: str) -> str:
    for line in front_matter.splitlines():
        match = re.match(r"^\s*title\s*:\s*(.+?)\s*$", line, flags=re.IGNORECASE)
        if match:
            return _normalize_title_text(match.group(1))
    return ""


def _normalize_title_text(value: str) -> str:
    cleaned = (value or "").strip().strip("\"'`")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    cleaned = re.sub(r"\s+#.*$", "", cleaned).strip()
    return cleaned
