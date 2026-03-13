"""Stage 3: Parse the references section into individual bibliography entries."""

from __future__ import annotations

import re

from backend.models.bibliography import BibliographyArtifact, BibliographyEntry, ReferencesSection

# Patterns for splitting entries
NUMBERED_ENTRY_PATTERN = re.compile(
    r"^\s*\[\s*(\d{1,4})\s*\]",
    re.MULTILINE,
)
NUMBERED_DOT_PATTERN = re.compile(
    r"^\s*(\d{1,4})\.\s+[A-Z0-9\"]",
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

# Year pattern
YEAR_PATTERN = re.compile(r"\b((?:19|20)\d{2}[a-z]?)\b")


def parse_bibliography(sections: list[ReferencesSection]) -> BibliographyArtifact:
    """Parse all reference sections into bibliography entries."""
    all_entries: list[BibliographyEntry] = []
    total_raw = 0
    parse_failures = 0

    for section in sections:
        raw_entries = _split_entries(section.raw_text)
        total_raw += len(raw_entries)

        for ref_num, raw_text in raw_entries:
            entry = _parse_single_entry(ref_num, raw_text)
            if entry.parse_confidence < 0.2:
                parse_failures += 1
            all_entries.append(entry)

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
        # Try to extract a leading number
        num_match = re.match(r"^\s*\[?\s*(\d{1,4})\s*[\]\.\)]\s*", chunk)
        ref_num = int(num_match.group(1)) if num_match else None
        entries.append((ref_num, chunk))
    return entries


def _parse_single_entry(ref_num: int | None, raw_text: str) -> BibliographyEntry:
    """Parse a single bibliography entry into structured fields."""
    warnings: list[str] = []

    doi = _extract_doi(raw_text)
    url = _extract_url(raw_text, doi)
    year = _extract_year(raw_text)
    authors = _extract_authors(raw_text, year)
    title = _extract_title(raw_text, authors, year)
    journal_info = _extract_journal_info(raw_text, title, year)

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
        url = match.group(1).rstrip(".,;:)>]")
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
    matches = YEAR_PATTERN.findall(text)
    if not matches:
        return ""
    # Prefer years in parentheses: (2023)
    paren_years = re.findall(r"\((\d{4}[a-z]?)\)", text)
    if paren_years:
        return paren_years[0]
    return matches[0]


def _extract_authors(text: str, year: str) -> list[str]:
    """Extract authors from the beginning of the entry."""
    # Remove leading reference number
    cleaned = re.sub(r"^\s*\[?\s*\d{1,4}\s*[\]\.\)]\s*", "", text)

    if not year:
        # Without a year anchor, take text up to the first period
        period_idx = cleaned.find(".")
        if period_idx > 0:
            author_text = cleaned[:period_idx]
        else:
            return []
    else:
        # Take text before the year
        year_idx = cleaned.find(year)
        if year_idx <= 0:
            return []
        # Also check for year in parentheses
        paren_idx = cleaned.find(f"({year}")
        if paren_idx > 0:
            author_text = cleaned[:paren_idx].strip().rstrip(",.")
        else:
            author_text = cleaned[:year_idx].strip().rstrip(",.")

    if not author_text or len(author_text) < 3:
        return []

    # Split authors on common delimiters
    # Handle "and", "&", ";"
    author_text = author_text.replace(" & ", "; ")
    author_text = author_text.replace(" and ", "; ")
    author_text = re.sub(r",\s*(?=[A-Z][a-z]+\s*,)", "; ", author_text)

    authors = [a.strip().rstrip(",. ") for a in author_text.split(";")]
    authors = [a for a in authors if a and len(a) > 1]

    return authors


def _extract_title(text: str, authors: list[str], year: str) -> str:
    """Extract the title from the entry."""
    # Remove leading reference number
    cleaned = re.sub(r"^\s*\[?\s*\d{1,4}\s*[\]\.\)]\s*", "", text)

    # Title typically comes after authors and year
    # Find the position after authors+year
    start_pos = 0
    if year:
        # Find year and skip past it
        year_match = re.search(re.escape(year) + r"[a-z]?\)?[\.\,\s]*", cleaned)
        if year_match:
            start_pos = year_match.end()

    if start_pos == 0 and authors:
        # Find end of last author
        last_author = authors[-1]
        idx = cleaned.find(last_author)
        if idx >= 0:
            start_pos = idx + len(last_author)
            # Skip past delimiters
            while start_pos < len(cleaned) and cleaned[start_pos] in ".,;: \t":
                start_pos += 1

    if start_pos >= len(cleaned):
        return ""

    remaining = cleaned[start_pos:].strip()
    if not remaining:
        return ""

    # Title ends at the next period followed by a space and uppercase letter,
    # or at a journal-like indicator
    title_end = re.search(r"\.\s+[A-Z]", remaining)
    if title_end:
        title = remaining[: title_end.start() + 1].strip()
    else:
        # Take up to a comma or end
        comma_idx = remaining.find(",")
        if comma_idx > 10:
            title = remaining[:comma_idx].strip()
        else:
            title = remaining.strip()

    # Clean up
    title = title.strip('"\' .,')
    if len(title) > 500:
        title = title[:500]

    return title


def _extract_journal_info(text: str, title: str, year: str) -> dict[str, str]:
    """Extract journal name, volume, issue, and pages."""
    result = {"journal": "", "volume": "", "issue": "", "pages": ""}

    # Extract pages: common patterns like pp. 123-456, 123-456
    pages_match = re.search(r"(?:pp?\.?\s*)?(\d+)\s*[-\u2013]\s*(\d+)", text)
    if pages_match:
        result["pages"] = f"{pages_match.group(1)}-{pages_match.group(2)}"

    # Extract volume: Vol. N or bold number before issue
    vol_match = re.search(r"(?:Vol\.?\s*|vol\.?\s*)(\d+)", text)
    if vol_match:
        result["volume"] = vol_match.group(1)
    else:
        # Look for pattern: Journal Name, N(M), or Journal Name, N,
        # Avoid matching 4-digit years
        vol_match2 = re.search(r",\s*(\d{1,3})\s*[\(,]", text)
        if vol_match2:
            candidate = vol_match2.group(1)
            # Make sure it's not a page number (look for it after journal name part)
            result["volume"] = candidate

    # Extract issue: (N) or No. N — but not 4-digit years like (2023)
    issue_match = re.search(r"(?:\((\d{1,3})\)|No\.?\s*(\d+))", text)
    if issue_match:
        result["issue"] = issue_match.group(1) or issue_match.group(2) or ""

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
