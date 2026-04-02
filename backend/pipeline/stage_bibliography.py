"""Stage 3: Parse the references section into individual bibliography entries."""

from __future__ import annotations

import json
import re

from backend.llm.client import UnifiedLLMClient
from backend.llm.prompts import BIBLIOGRAPHY_REPAIR_SYSTEM, BIBLIOGRAPHY_REPAIR_USER
from backend.models.bibliography import BibliographyArtifact, BibliographyEntry, ReferencesSection
from backend.models.settings import LLMBackendConfig

# Patterns for splitting entries
NUMBERED_ENTRY_PATTERN = re.compile(
    r"^\s*\[\s*(\d{1,4})\s*\]",
    re.MULTILINE,
)
NUMBERED_DOT_PATTERN = re.compile(
    r"^\s*(\d{1,4})\.\s+(?=\S)",
    re.MULTILINE,
)

# DOI extraction patterns
DOI_PATTERNS = [
    re.compile(r"(?:doi[:\s]+)(10\.\d{4,}/\S+)", re.IGNORECASE),
    re.compile(r"(https?://doi\.org/(10\.\d{4,}/\S+))", re.IGNORECASE),
    re.compile(r"(https?://dx\.doi\.org/(10\.\d{4,}/\S+))", re.IGNORECASE),
    re.compile(r"\b(10\.\d{4,}/\S+)"),  # bare DOI
]

# URL pattern
URL_PATTERN = re.compile(r"(https?://\S+)")
QUOTED_TITLE_PATTERN = re.compile(r"[\"“](.+?)[\"”]")
PERSON_AUTHOR_PATTERN = re.compile(
    r"[A-Z][A-Za-z'`\-]+,\s*(?:[A-Z][A-Za-z'`\-]+(?:\s+[A-Z][A-Za-z'`\-]+)*|[A-Z](?:\.[A-Z])*\.?)"
)
GIVEN_FAMILY_AUTHOR_PATTERN = re.compile(
    r"^[A-Z][A-Za-z'`\-]+(?:\s+(?:[A-Z][A-Za-z'`\-]+|[A-Z]\.)){1,3}$"
)

# Year pattern
YEAR_PATTERN = re.compile(r"\b((?:19|20)\d{2}[a-z]?)\b")

WHITESPACE_PATTERN = re.compile(r"\s+")
AUTHOR_CLAUSE_END_PATTERN = re.compile(r"(?<!\b[A-Z])\.(?=\s+[A-Z0-9\"(]|$)")
TITLE_HINT_WORDS = {
    "analysis",
    "article",
    "assessment",
    "buildings",
    "case",
    "criteria",
    "document",
    "efficiency",
    "equipment",
    "effect",
    "evidence",
    "framework",
    "impact",
    "incentives",
    "installation",
    "performance",
    "policy",
    "practice",
    "proceedings",
    "programs",
    "pump",
    "pumps",
    "ratings",
    "report",
    "review",
    "standards",
    "study",
    "systems",
    "testing",
    "tips",
}
CORPORATE_AUTHOR_KEYWORDS = {
    "agency",
    "association",
    "board",
    "bureau",
    "calnext",
    "center",
    "centre",
    "commission",
    "committee",
    "corporation",
    "council",
    "county",
    "city",
    "department",
    "district",
    "electric",
    "energy",
    "group",
    "institute",
    "laboratory",
    "lab",
    "league",
    "ministry",
    "national",
    "office",
    "program",
    "services",
    "society",
    "state",
    "team",
    "university",
}


def parse_bibliography(
    sections: list[ReferencesSection],
    *,
    use_llm: bool = False,
    llm_backend: LLMBackendConfig | None = None,
) -> BibliographyArtifact:
    """Parse all reference sections into bibliography entries."""
    all_entries: list[BibliographyEntry] = []
    total_raw = 0
    parse_failures = 0
    llm_client: UnifiedLLMClient | None = None

    if use_llm and llm_backend is not None and _llm_backend_ready_for_chat(llm_backend):
        llm_client = UnifiedLLMClient(llm_backend)

    try:
        for section in sections:
            raw_entries = _split_entries(section.raw_text)
            total_raw += len(raw_entries)

            for ref_num, raw_text in raw_entries:
                entry = _parse_single_entry(ref_num, raw_text)
                if llm_client is not None:
                    entry = _repair_entry_with_llm(entry, llm_client)
                if entry.parse_confidence < 0.2:
                    parse_failures += 1
                all_entries.append(entry)
    finally:
        if llm_client is not None:
            llm_client.sync_close()

    return BibliographyArtifact(
        sections=sections,
        entries=all_entries,
        total_raw_entries=total_raw,
        parse_failures=parse_failures,
    )


def _split_entries(raw_text: str) -> list[tuple[int | None, str]]:
    """Split the references text into individual entries.

    Returns list of (ref_number, entry_text) tuples.
    """
    # Try numbered bracket pattern first: [1], [2], ...
    entries = _split_by_pattern(raw_text, NUMBERED_ENTRY_PATTERN, bracket_style=True)
    if entries:
        return entries

    # Try numbered dot pattern: 1. Author, 2. Author, ...
    entries = _split_by_pattern(raw_text, NUMBERED_DOT_PATTERN, bracket_style=False)
    if entries:
        return entries

    # Try line-based splitting for markdown-style works-cited sections where
    # each reference is already a standalone line/paragraph.
    entries = _split_by_lines(raw_text)
    if entries:
        return entries

    # Fallback: split on blank lines
    return _split_by_blank_lines(raw_text)


def _split_by_pattern(
    raw_text: str,
    pattern: re.Pattern,
    bracket_style: bool,
) -> list[tuple[int | None, str]]:
    """Split entries using a numbered pattern."""
    matches = list(pattern.finditer(raw_text))
    if len(matches) < 2:
        return []

    entries: list[tuple[int | None, str]] = []
    for i, match in enumerate(matches):
        ref_num = int(match.group(1))
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(raw_text)
        entry_text = raw_text[start:end].strip()
        if _should_skip_entry_text(entry_text):
            continue
        # Remove the leading [N] or N. prefix from the entry text for cleaner storage
        # but keep it in raw_text
        entries.append((ref_num, entry_text))

    return entries


def _split_by_blank_lines(raw_text: str) -> list[tuple[int | None, str]]:
    """Fallback: split entries by blank lines."""
    chunks = re.split(r"\n\s*\n", raw_text)
    entries: list[tuple[int | None, str]] = []
    for i, chunk in enumerate(chunks):
        chunk = chunk.strip()
        if not chunk or len(chunk) < 10:
            continue
        if _should_skip_entry_text(chunk):
            continue
        # Try to extract a leading number
        num_match = re.match(r"^\s*\[?\s*(\d{1,4})\s*[\]\.\)]\s*", chunk)
        ref_num = int(num_match.group(1)) if num_match else None
        entries.append((ref_num, chunk))
    return entries


def _split_by_lines(raw_text: str) -> list[tuple[int | None, str]]:
    lines = [_collapse_whitespace(line) for line in str(raw_text or "").splitlines()]
    lines = [line for line in lines if line and len(line) >= 12]
    if len(lines) < 2:
        return []

    ref_like_lines = [
        line
        for line in lines
        if (
            "http://" in line
            or "https://" in line
            or '"' in line
            or "“" in line
            or YEAR_PATTERN.search(line)
        )
    ]
    if len(ref_like_lines) < max(2, len(lines) // 2):
        return []

    entries: list[tuple[int | None, str]] = []
    for line in lines:
        lower = line.lower()
        if lower.endswith(":") or "provided as inline citations" in lower:
            continue
        if _should_skip_entry_text(line):
            continue
        num_match = re.match(r"^\s*\[?\s*(\d{1,4})\s*[\]\.\)]\s*", line)
        footnote_match = re.search(r"\[\^(\d{1,4})\]\s*$", line)
        ref_num = int(num_match.group(1)) if num_match else (
            int(footnote_match.group(1)) if footnote_match else None
        )
        entries.append((ref_num, line))
    return entries


def _parse_single_entry(ref_num: int | None, raw_text: str) -> BibliographyEntry:
    """Parse a single bibliography entry into structured fields."""
    warnings: list[str] = []

    cleaned = _clean_entry_text(raw_text)
    normalized = _normalize_entry_for_field_extraction(cleaned)
    doi = _extract_doi(raw_text)
    url = _extract_url(raw_text, doi)
    year = _extract_year(normalized)

    parsed = _parse_quoted_entry(normalized, year)
    if parsed is not None:
        authors = parsed["authors"]
        title = parsed["title"]
        journal_info = parsed["journal_info"]
    else:
        authors = _extract_authors(normalized, year)
        title = _extract_title(normalized, authors, year)
        journal_info = _extract_journal_info(normalized, title, year)

        title_first = _parse_title_first_entry(normalized, year)
        if _should_prefer_title_first_parse(
            authors=authors,
            title=title,
            title_first=title_first,
        ):
            authors = title_first["authors"]
            title = title_first["title"]
            journal_info = title_first["journal_info"]

    confidence = _assess_confidence(
        authors=authors,
        title=title,
        year=year,
        doi=doi,
        url=url,
        journal=journal_info.get("journal", ""),
        warnings=warnings,
    )

    return BibliographyEntry(
        ref_number=ref_num,
        raw_text=raw_text,
        authors=authors,
        title=title,
        year=year,
        journal_or_source=journal_info.get("journal", ""),
        volume=journal_info.get("volume", ""),
        issue=journal_info.get("issue", ""),
        pages=journal_info.get("pages", ""),
        doi=doi,
        url=url,
        parse_confidence=confidence,
        parse_warnings=warnings,
    )


def _extract_doi(text: str) -> str:
    """Extract DOI from entry text."""
    for pattern in DOI_PATTERNS:
        match = pattern.search(text)
        if match:
            doi = match.group(match.lastindex or 1)
            # If it's a URL form, extract just the DOI part
            if doi.startswith("http"):
                doi_match = re.search(r"(10\.\d{4,}/\S+)", doi)
                if doi_match:
                    doi = doi_match.group(1)
            # Clean trailing punctuation
            doi = doi.rstrip(".,;:)")
            return doi
    return ""


def _extract_url(text: str, doi: str) -> str:
    """Extract URL from entry text, excluding DOI URLs."""
    for match in URL_PATTERN.finditer(text):
        url = _strip_url_suffixes(match.group(1))
        # Skip DOI URLs (already captured as DOI)
        if "doi.org" in url:
            continue
        return url
    # If we have a DOI but no URL, construct the URL
    if doi:
        return f"https://doi.org/{doi}"
    return ""


def _extract_year(text: str) -> str:
    """Extract publication year."""
    match = _find_preferred_year_match(_strip_urls_and_labels(text))
    return match.group(1) if match else ""


def _extract_authors(text: str, year: str) -> list[str]:
    """Extract authors from the beginning of the entry."""
    cleaned = _strip_urls_and_labels(text)
    if year:
        year_idx = cleaned.find(year)
        if year_idx > 0:
            cleaned = cleaned[:year_idx]
    cleaned = cleaned.strip().rstrip(",.;:")
    if not cleaned or len(cleaned) < 3:
        return []

    clause_match = AUTHOR_CLAUSE_END_PATTERN.search(cleaned)
    if clause_match:
        candidate = cleaned[: clause_match.start()].strip().rstrip(",.;:")
        authors = _extract_authors_from_segment(
            candidate,
            allow_corporate=not _looks_like_title_or_topic(candidate),
        )
        if authors:
            return authors

    comma_authors = _extract_leading_comma_authors(cleaned)
    if comma_authors:
        return comma_authors

    authors = _extract_authors_from_segment(
        cleaned,
        allow_corporate=not _looks_like_title_or_topic(cleaned),
    )
    if authors:
        return authors

    return []


def _extract_title(text: str, authors: list[str], year: str) -> str:
    """Extract the title from the entry."""
    cleaned = _strip_urls_and_labels(text)

    quoted_title = _extract_quoted_title(cleaned)
    if quoted_title:
        return quoted_title

    start_pos = 0
    if authors:
        last_author = authors[-1]
        idx = cleaned.find(last_author)
        if idx >= 0:
            start_pos = idx + len(last_author)
    if start_pos == 0 and year:
        year_match = re.search(rf"[([]?{re.escape(year)}[a-z]?[)\]]?[\.,;\s]*", cleaned)
        if year_match and year_match.start() < max(20, len(cleaned) // 3):
            start_pos = year_match.end()

    while start_pos < len(cleaned) and cleaned[start_pos] in ".,;: \t":
        start_pos += 1

    if start_pos >= len(cleaned):
        return ""

    remaining = cleaned[start_pos:].strip()
    if not remaining:
        return ""

    if year:
        year_match = _find_preferred_year_match(remaining)
        if year_match is not None and year_match.start(1) > 0:
            remaining = remaining[: year_match.start(1)].strip().rstrip(",.;:")

    remaining = re.sub(r"\b(?:doi[:\s].*|https?://\S+)\s*$", "", remaining, flags=re.IGNORECASE)
    if not remaining:
        return ""

    title = remaining
    period_match = AUTHOR_CLAUSE_END_PATTERN.search(remaining)
    if period_match and period_match.start() > 5:
        title = remaining[: period_match.start()].strip()
    else:
        comma_idx = remaining.find(",")
        if comma_idx > 5:
            title = remaining[:comma_idx].strip()

    title = title.strip('"\' .,')
    if len(title) > 500:
        title = title[:500]

    return title


def _extract_journal_info(text: str, title: str, year: str) -> dict[str, str]:
    """Extract journal name, volume, issue, and pages."""
    result = {"journal": "", "volume": "", "issue": "", "pages": ""}
    cleaned = _strip_urls_and_labels(text)

    pages_match = re.search(r"(?:pp?\.?\s*)?(\d+)\s*[-\u2013]\s*(\d+)", text)
    if pages_match:
        result["pages"] = f"{pages_match.group(1)}-{pages_match.group(2)}"

    vol_match = re.search(r"(?:Vol\.?\s*|vol\.?\s*)(\d+)", text)
    if vol_match:
        result["volume"] = vol_match.group(1)
    else:
        vol_match2 = re.search(r",\s*(\d{1,3})\s*[\(,]", text)
        if vol_match2:
            result["volume"] = vol_match2.group(1)

    issue_match = re.search(r"(?:\((\d{1,3})\)|No\.?\s*(\d+))", text)
    if issue_match:
        result["issue"] = issue_match.group(1) or issue_match.group(2) or ""

    if title:
        title_idx = cleaned.find(title)
        if title_idx >= 0:
            journal_segment = cleaned[title_idx + len(title) :].strip().lstrip(".,;: ")
            if year:
                year_idx = journal_segment.find(year)
                if year_idx > 0:
                    journal_segment = journal_segment[:year_idx]
            journal_segment = re.sub(r"\b(?:Vol\.?|No\.?|pp?\.?)\b.*$", "", journal_segment, flags=re.IGNORECASE)
            journal_segment = re.sub(r"(?:,?\s*)\d{1,4}\s*(?:\(\d{1,3}\))?.*$", "", journal_segment)
            journal_segment = journal_segment.strip(" ,.;:")
            if 3 <= len(journal_segment) <= 160:
                result["journal"] = journal_segment

    return result


def _assess_confidence(
    authors: list[str],
    title: str,
    year: str,
    doi: str,
    url: str,
    journal: str,
    warnings: list[str],
) -> float:
    """Score confidence 0-1 based on parsed fields."""
    score = 0.0
    if authors:
        score += 0.2
    if title and len(title) > 5:
        score += 0.3
    if year:
        score += 0.2
    if doi or url or journal:
        score += 0.2
    if not warnings:
        score += 0.1
    return round(min(score, 1.0), 2)


def _repair_entry_with_llm(
    entry: BibliographyEntry,
    client: UnifiedLLMClient,
) -> BibliographyEntry:
    try:
        response = client.sync_chat_completion(
            system_prompt=BIBLIOGRAPHY_REPAIR_SYSTEM,
            user_prompt=BIBLIOGRAPHY_REPAIR_USER.format(entry_text=entry.raw_text[:4000]),
            response_format="json",
        ).strip()
        payload = json.loads(response)
    except Exception:
        return entry

    if not isinstance(payload, dict):
        return entry

    authors = _coerce_authors(payload.get("authors")) or entry.authors
    title = _collapse_whitespace(str(payload.get("title") or "")) or entry.title
    year = _clean_year(str(payload.get("year") or "")) or entry.year
    journal = _collapse_whitespace(str(payload.get("journal_or_source") or "")) or entry.journal_or_source
    volume = _collapse_whitespace(str(payload.get("volume") or "")) or entry.volume
    issue = _collapse_whitespace(str(payload.get("issue") or "")) or entry.issue
    pages = _collapse_whitespace(str(payload.get("pages") or "")) or entry.pages
    doi = entry.doi or _clean_doi(str(payload.get("doi") or ""))
    url = entry.url or _clean_url(str(payload.get("url") or "")) or (
        f"https://doi.org/{doi}" if doi else ""
    )

    changed = (
        authors != entry.authors
        or title != entry.title
        or year != entry.year
        or journal != entry.journal_or_source
        or volume != entry.volume
        or issue != entry.issue
        or pages != entry.pages
        or doi != entry.doi
        or url != entry.url
    )
    if not changed:
        return entry

    warnings = [
        warning
        for warning in entry.parse_warnings
        if warning.strip().lower() != "entry built from inline url only"
    ]
    confidence = _assess_confidence(
        authors=authors,
        title=title,
        year=year,
        doi=doi,
        url=url,
        journal=journal,
        warnings=warnings,
    )

    return entry.model_copy(
        update={
            "authors": authors,
            "title": title,
            "year": year,
            "journal_or_source": journal,
            "volume": volume,
            "issue": issue,
            "pages": pages,
            "doi": doi,
            "url": url,
            "parse_confidence": max(confidence, 0.95),
            "parse_warnings": warnings,
            "repair_method": "llm_bibliography_repair",
        }
    )


def _clean_entry_text(text: str) -> str:
    cleaned = re.sub(r"^\s*\[?\s*\d{1,4}\s*[\]\.\)]\s*", "", text or "")
    return _collapse_whitespace(cleaned)


def _normalize_entry_for_field_extraction(text: str) -> str:
    cleaned = _collapse_whitespace(text).replace("—", "-").replace("–", "-")
    cleaned = re.sub(r"`(https?://[^`]+)`", r"\1", cleaned)
    cleaned = re.sub(r"\s*\[\^\d{1,4}\]\s*$", "", cleaned)
    return cleaned.strip()


def _strip_urls_and_labels(text: str) -> str:
    cleaned = _normalize_entry_for_field_extraction(text)
    cleaned = re.sub(r"\b(?:Link|URL|Via):\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"https?://\S+", "", cleaned)
    return _collapse_whitespace(cleaned).strip(" ,.;:`")


def _collapse_whitespace(text: str) -> str:
    return WHITESPACE_PATTERN.sub(" ", str(text or "")).strip()


def _extract_quoted_title(text: str) -> str:
    match = QUOTED_TITLE_PATTERN.search(text or "")
    if not match:
        return ""
    return _collapse_whitespace(match.group(1)).strip(" ,.;:`")


def _looks_like_author_list(text: str) -> bool:
    candidate = _collapse_whitespace(text).strip(" ,.;:")
    if not candidate or len(candidate) < 3 or len(candidate) > 180:
        return False
    if "http://" in candidate.lower() or "https://" in candidate.lower():
        return False
    if ":" in candidate:
        return False
    words = re.findall(r"[A-Za-z][A-Za-z'.-]*", candidate)
    if not words or len(words) > 12:
        return False
    lower_words = [word.lower() for word in words]
    if sum(1 for word in lower_words if word in TITLE_HINT_WORDS) >= 2:
        return False
    return True


def _extract_person_authors(text: str) -> list[str]:
    candidate = _collapse_whitespace(text).strip(" ,.;:")
    if not candidate:
        return []

    normalized = re.sub(r"\bet al\.?$", "", candidate, flags=re.IGNORECASE).strip(" ,.;:")
    matches = [match.group(0).strip(" ,.;:") for match in PERSON_AUTHOR_PATTERN.finditer(normalized)]
    if matches and sum(len(match) for match in matches) >= max(len(normalized) // 2, 8):
        return matches
    return []


def _extract_given_family_author(text: str) -> str:
    segments = [
        segment.strip(" ,.;:")
        for segment in re.split(r"\s*(?:,|;)\s*", _collapse_whitespace(text))
        if segment.strip(" ,.;:")
    ]
    if not segments:
        return ""

    candidate = segments[0]
    words = re.findall(r"[A-Za-z][A-Za-z'.-]*", candidate)
    lower_words = [word.lower() for word in words]
    if len(words) < 2 or len(words) > 4:
        return ""
    if any(word in TITLE_HINT_WORDS or word in CORPORATE_AUTHOR_KEYWORDS for word in lower_words):
        return ""
    if not GIVEN_FAMILY_AUTHOR_PATTERN.fullmatch(candidate):
        return ""
    return candidate


def _looks_like_corporate_author(text: str) -> bool:
    candidate = _collapse_whitespace(text).strip(" ,.;:")
    if not candidate or len(candidate) < 3 or len(candidate) > 140:
        return False
    if "http://" in candidate.lower() or "https://" in candidate.lower():
        return False
    words = re.findall(r"[A-Za-z][A-Za-z'.&/\-]*", candidate)
    if not words or len(words) > 14:
        return False
    if any(word.lower() in {"accessed", "retrieved"} for word in words):
        return False
    lower_words = [word.lower().strip(".") for word in words]
    if any(word in CORPORATE_AUTHOR_KEYWORDS for word in lower_words):
        return True
    if "/" in candidate or "&" in candidate:
        return True
    return len(words) <= 3


def _clean_corporate_author(text: str) -> str:
    cleaned = _collapse_whitespace(text).strip(" ,.;:")
    cleaned = re.sub(r"\([^)]*\d[^)]*\)", "", cleaned).strip(" ,.;:")
    return cleaned


def _extract_authors_from_segment(
    text: str,
    *,
    allow_corporate: bool,
) -> list[str]:
    candidate = _collapse_whitespace(text).strip(" ,.;:")
    if not candidate:
        return []

    given_family = _extract_given_family_author(candidate)
    if given_family:
        return [given_family]

    authors = _extract_person_authors(candidate)
    if authors:
        return authors

    if allow_corporate and _looks_like_corporate_author(candidate):
        corporate = _clean_corporate_author(candidate)
        if corporate:
            return [corporate]
    return []


def _split_author_names(text: str) -> list[str]:
    candidate = _collapse_whitespace(text).strip(" ,.;:")
    if not candidate:
        return []

    if ";" in candidate:
        raw_parts = [part.strip() for part in candidate.split(";")]
    elif " and " in candidate.lower():
        raw_parts = re.split(r"\s+(?:and|&)\s+", candidate, flags=re.IGNORECASE)
    else:
        raw_parts = [candidate]

    authors = [part.strip(" ,.;:") for part in raw_parts if part.strip(" ,.;:")]
    return authors if all(len(author) > 1 for author in authors) else []


def _coerce_authors(value: object) -> list[str]:
    if isinstance(value, list):
        authors = [_collapse_whitespace(str(item or "")).strip(" ,.;:") for item in value]
        authors = [author for author in authors if author]
        if authors:
            return authors
    if isinstance(value, str):
        authors = _split_author_names(value)
        if authors:
            return authors
    return []


def _extract_leading_comma_authors(text: str) -> list[str]:
    parts = [part.strip(" ,.;:") for part in str(text or "").split(",") if part.strip(" ,.;:")]
    if len(parts) < 2:
        return []
    if _looks_like_title_or_topic(parts[0]):
        return []

    authors: list[str] = []
    idx = 0
    while idx < len(parts):
        if idx + 1 < len(parts):
            combined = f"{parts[idx]}, {parts[idx + 1]}".strip()
            if _looks_like_single_author(combined):
                authors.append(combined)
                idx += 2
                continue
        if _looks_like_single_author(parts[idx]):
            authors.append(parts[idx])
            idx += 1
            continue
        break

    return authors if authors and idx < len(parts) else []


def _looks_like_single_author(text: str) -> bool:
    candidate = _collapse_whitespace(text).strip(" ,.;:")
    if not candidate or len(candidate) > 80 or ":" in candidate:
        return False
    words = re.findall(r"[A-Za-z][A-Za-z'.-]*\.?", candidate)
    if not words or len(words) > 5:
        return False
    lower_words = [word.rstrip(".").lower() for word in words]
    if any(word in TITLE_HINT_WORDS for word in lower_words):
        return False
    if any(word.isupper() and len(word) > 1 for word in words):
        return False
    return all(
        word[0].isupper() or word.rstrip(".").lower() in {"de", "del", "der", "di", "la", "van", "von"}
        for word in words
    )


def _clean_year(value: str) -> str:
    match = YEAR_PATTERN.search(value or "")
    return match.group(1) if match else ""


def _clean_doi(value: str) -> str:
    if not value:
        return ""
    return _extract_doi(value)


def _clean_url(value: str) -> str:
    if not value:
        return ""
    match = URL_PATTERN.search(value)
    if not match:
        return ""
    return _strip_url_suffixes(match.group(1))


def _find_preferred_year_match(text: str) -> re.Match[str] | None:
    cleaned = str(text or "")
    candidates: list[tuple[int, int, re.Match[str]]] = []
    all_matches = list(YEAR_PATTERN.finditer(cleaned))
    for match in all_matches:
        score = _score_year_match(
            cleaned,
            match.start(1),
            match.end(1),
            has_later_candidate=any(other.start(1) > match.start(1) for other in all_matches),
        )
        if score is None:
            continue
        candidates.append((score, match.start(1), match))
    if not candidates:
        return None
    return max(candidates, key=lambda item: (item[0], item[1]))[2]


def _score_year_match(text: str, start: int, end: int, *, has_later_candidate: bool) -> int | None:
    if not _is_preferred_year_match(text, start, end):
        return None

    before = text[max(0, start - 32) : start]
    after = text[end : min(len(text), end + 32)]
    prev_char = text[start - 1] if start > 0 else ""
    next_char = text[end] if end < len(text) else ""
    score = 0

    if prev_char in "([" and next_char in ")]":
        score += 4
    if prev_char in ",.;:)]":
        score += 2
    elif prev_char.isspace():
        score += 1

    if next_char in ",.;:)]":
        score += 2
    elif next_char == "-" and re.match(r"-\d{2}(?:-\d{2})?", text[end : min(len(text), end + 8)]):
        score += 1

    if re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s*$",
        before,
        flags=re.IGNORECASE,
    ):
        score += 2

    if start <= 4 and has_later_candidate:
        score -= 3
    if re.search(r"^\s+[A-Z][A-Za-z'-]+(?:\s+[A-Z][A-Za-z'-]+){1,3}", after):
        score -= 1

    return score


def _is_preferred_year_match(text: str, start: int, end: int) -> bool:
    before = text[max(0, start - 24) : start].lower()
    prev_char = text[start - 1] if start > 0 else ""
    next_char = text[end] if end < len(text) else ""
    if prev_char == "-":
        return False
    if next_char == "-" and not re.match(r"-\d{2}(?:-\d{2})?", text[end : min(len(text), end + 8)]):
        return False
    excluded_before_patterns = (
        r"(?:accessed|retrieved|updated)\s*$",
        r"post-\s*$",
        r"project year\s*$",
        r"effective\s*$",
        r"permits(?:\s+on/after)?\s*$",
        r"on/after\s*$",
    )
    return not any(re.search(pattern, before) for pattern in excluded_before_patterns)


def _parse_quoted_entry(text: str, year: str) -> dict[str, object] | None:
    title = _extract_quoted_title(text)
    if not title:
        return None

    quote_match = QUOTED_TITLE_PATTERN.search(text)
    if quote_match is None:
        return None

    author_segment = text[: quote_match.start()].strip(" ,.;:")
    remainder = text[quote_match.end() :].strip(" ,.;:")
    authors = _extract_authors_from_segment(author_segment, allow_corporate=True)
    journal = _extract_source_segment(remainder, year)
    return {
        "authors": authors,
        "title": title,
        "journal_info": {"journal": journal, "volume": "", "issue": "", "pages": ""},
    }


def _parse_title_first_entry(text: str, year: str) -> dict[str, object] | None:
    working = _strip_urls_and_labels(text)
    prefix = _prefix_before_year(working, year)
    if not prefix:
        return None

    period_segments = [segment.strip(" ,.;:") for segment in re.split(r"\.\s+", prefix) if segment.strip(" ,.;:")]
    if len(period_segments) >= 2 and _looks_like_title_segment(period_segments[0]):
        title = period_segments[0]
        source = _extract_source_segment(". ".join(period_segments[1:]), year)
        authors = _extract_authors_from_segment(source, allow_corporate=True)
        if not authors and source and not _looks_like_report_code_segment(source):
            authors = [_clean_corporate_author(source)]
        return {
            "authors": authors,
            "title": title,
            "journal_info": {"journal": source, "volume": "", "issue": "", "pages": ""},
        }

    comma_segments = [segment.strip(" ,.;:") for segment in prefix.split(",") if segment.strip(" ,.;:")]
    if len(comma_segments) < 2:
        return None

    title_candidate = ", ".join(comma_segments[:-1]).strip(" ,.;:")
    if not title_candidate or not _looks_like_title_or_topic(title_candidate):
        return None
    if _extract_person_authors(comma_segments[0]):
        return None

    source = _extract_source_segment(comma_segments[-1], year)
    authors = _extract_authors_from_segment(source, allow_corporate=True)
    if not authors and source and not _looks_like_report_code_segment(source):
        authors = [_clean_corporate_author(source)]
    return {
        "authors": authors,
        "title": title_candidate,
        "journal_info": {"journal": source, "volume": "", "issue": "", "pages": ""},
    }


def _prefix_before_year(text: str, year: str) -> str:
    working = _collapse_whitespace(text).strip(" ,.;:")
    if not year:
        return working
    match = _find_preferred_year_match(working)
    if match is not None:
        return working[: match.start(1)].strip(" ,.;:")
    return working


def _extract_source_segment(text: str, year: str) -> str:
    working = _prefix_before_year(_strip_urls_and_labels(text), year)
    if not working:
        return ""

    working = re.sub(
        r"^(?:Report|Project|Publication)\s+[A-Z0-9\-/]+\.\s*",
        "",
        working,
        flags=re.IGNORECASE,
    ).strip(" ,.;:")
    segments = [segment.strip(" ,.;:") for segment in working.split(".") if segment.strip(" ,.;:")]
    if len(segments) >= 2 and _looks_like_report_code_segment(segments[0]):
        working = ". ".join(segments[1:]).strip(" ,.;:")

    working = re.sub(
        r",?\s*(January|February|March|April|May|June|July|August|September|October|November|December)\s*$",
        "",
        working,
        flags=re.IGNORECASE,
    ).strip(" ,.;:")
    return working


def _looks_like_report_code_segment(text: str) -> bool:
    candidate = _collapse_whitespace(text).strip(" ,.;:")
    if not candidate or len(candidate) > 50:
        return False
    if "/" in candidate or re.search(r"\b[A-Z]{2,}[-/]\d", candidate):
        return True
    return bool(re.fullmatch(r"[A-Z0-9\-/]+", candidate))


def _looks_like_title_segment(text: str) -> bool:
    candidate = _collapse_whitespace(text).strip(" ,.;:")
    if not candidate:
        return False
    if any(char in candidate for char in ":?()[]"):
        return True
    words = re.findall(r"[A-Za-z][A-Za-z'\-]*", candidate)
    lower_words = [word.lower() for word in words]
    if any(word in TITLE_HINT_WORDS for word in lower_words):
        return True
    if len(candidate) > 32 or len(words) >= 4:
        return True
    return False


def _looks_like_title_or_topic(text: str) -> bool:
    candidate = _collapse_whitespace(text).strip(" ,.;:")
    if not candidate or candidate.isdigit():
        return False
    if _looks_like_title_segment(candidate):
        return True
    return not bool(_extract_person_authors(candidate))


def _should_prefer_title_first_parse(
    *,
    authors: list[str],
    title: str,
    title_first: dict[str, object] | None,
) -> bool:
    if title_first is None:
        return False

    title_first_title = str(title_first.get("title") or "").strip()
    title_first_authors = [
        str(value).strip() for value in list(title_first.get("authors") or []) if str(value).strip()
    ]
    if not title_first_title:
        return False
    if not title or title.isdigit():
        return True
    if len(title.strip()) < 5 and len(title_first_title) > len(title.strip()):
        return True
    if not authors and title_first_authors:
        return True
    if authors and any(re.fullmatch(r"[^,]+,\s*[A-Z]$", author.strip()) for author in authors):
        return True
    if authors and _normalized_text(authors[0]) == _normalized_text(title_first_title):
        return True
    if title_first_authors and _normalized_text(title) == _normalized_text(title_first_authors[0]):
        return True
    return False


def _normalized_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())


def _should_skip_entry_text(text: str) -> bool:
    cleaned = _collapse_whitespace(text)
    if not cleaned:
        return True
    if re.fullmatch(r"[-_=*]{8,}", cleaned):
        return True
    if re.fullmatch(r"(?:\[\s*\d{1,4}\s*\]\s*)+https?://\S+", cleaned, flags=re.IGNORECASE):
        return True
    if re.fullmatch(r"(?:\d{1,4}\.\s*)?https?://\S+", cleaned, flags=re.IGNORECASE):
        return True
    return False


def _strip_url_suffixes(value: str) -> str:
    cleaned = str(value or "")
    cleaned = re.sub(r"\[\^\d{1,4}\]$", "", cleaned)
    return cleaned.rstrip("`.,;:)>]")


def _llm_backend_ready_for_chat(config: LLMBackendConfig) -> bool:
    if not config.model.strip():
        return False
    if not config.base_url.strip():
        return False
    return config.kind in {"openai", "ollama"}


def build_entries_from_inline_urls(
    inline_urls: dict[int, str],
) -> list[BibliographyEntry]:
    """Build bibliography entries from inline citation URLs (Markdown-style docs).

    When a document embeds URLs directly in citation markers like [\[1\]](url),
    we create minimal bibliography entries with the ref number and URL.
    """
    entries: list[BibliographyEntry] = []
    for ref_num in sorted(inline_urls.keys()):
        url = inline_urls[ref_num]
        doi = ""
        # Extract DOI from URL if it's a DOI URL
        doi_match = re.search(r"(10\.\d{4,}/\S+)", url)
        if doi_match:
            doi = doi_match.group(1).rstrip(".,;:)>]")

        entries.append(
            BibliographyEntry(
                ref_number=ref_num,
                raw_text=f"[{ref_num}] {url}",
                url=url,
                doi=doi,
                parse_confidence=0.5,  # URL-only, no structured metadata
                parse_warnings=["Entry built from inline URL only"],
            )
        )
    return entries


def merge_inline_urls_into_entries(
    entries: list[BibliographyEntry],
    inline_urls: dict[int, str],
) -> list[BibliographyEntry]:
    """Enrich existing bibliography entries with inline citation URLs.

    If an entry has a ref_number that maps to an inline URL, and the entry
    doesn't already have a URL, add it.
    Also creates new entries for ref numbers that have inline URLs but
    no existing bibliography entry.
    """
    existing_refs = {e.ref_number for e in entries if e.ref_number is not None}

    # Enrich existing entries
    for entry in entries:
        if entry.ref_number is not None and entry.ref_number in inline_urls:
            if not entry.url:
                entry.url = inline_urls[entry.ref_number]
            if not entry.doi:
                doi_match = re.search(r"(10\.\d{4,}/\S+)", inline_urls[entry.ref_number])
                if doi_match:
                    entry.doi = doi_match.group(1).rstrip(".,;:)>]")

    # Create entries for refs that only exist as inline URLs
    for ref_num in sorted(inline_urls.keys()):
        if ref_num not in existing_refs:
            url = inline_urls[ref_num]
            doi = ""
            doi_match = re.search(r"(10\.\d{4,}/\S+)", url)
            if doi_match:
                doi = doi_match.group(1).rstrip(".,;:)>]")
            entries.append(
                BibliographyEntry(
                    ref_number=ref_num,
                    raw_text=f"[{ref_num}] {url}",
                    url=url,
                    doi=doi,
                    parse_confidence=0.5,
                    parse_warnings=["Entry built from inline URL only"],
                )
            )

    # Sort by ref_number
    entries.sort(key=lambda e: e.ref_number or 0)
    return entries
