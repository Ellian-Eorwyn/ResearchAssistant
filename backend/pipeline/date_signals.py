"""Deterministic publication-date signal extraction from raw HTML.

The HTML->markdown step drops datelines, `<meta>` tags, `<time>` elements and
footer copyright, so a column that only reads the cleaned text cannot find the
publication date even when the page states it plainly. This module pulls those
signals back out of the *raw* HTML and returns them as a compact, labelled
string (each candidate tagged with where it came from) so a column prompt can
weigh them: prefer a published date, treat `modified` as "updated", treat
`copyright` / a bare current-year as weak, and reject event years.

It is purely deterministic (regex + optional htmldate) -- no LLM, no network --
so the resulting `date_signals` field is fetch/ingest provenance, not another
column's coded value.
"""
from __future__ import annotations

import re
from datetime import datetime

_MONTHS = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
_YEAR_RE = re.compile(r"(19|20)\d\d")


def _year(value: str, current_year: int) -> int | None:
    m = _YEAR_RE.search(value or "")
    if not m:
        return None
    y = int(m.group(0))
    return y if 1990 <= y <= current_year else None


def _search(patterns, text, flags=re.I):
    for pat in patterns:
        m = re.search(pat, text, flags)
        if m:
            return m.group(1).strip()
    return ""


def extract_date_signals(html: str, *, current_year: int | None = None) -> str:
    """Return a labelled `; `-joined string of candidate dates found in HTML.

    Labels, most authoritative first: meta_published, jsonld_published, time,
    byline, modified, htmldate, copyright. Empty string when nothing plausible
    is found. Values are kept as they appear (so the model sees full dates, not
    just years) but only signals whose year is plausible are emitted.
    """
    if not html:
        return ""
    cy = current_year or datetime.now().year
    head = html[:8000]
    top = html[:24000]
    tail = html[-8000:]
    out: list[tuple[str, str]] = []

    def add(label: str, value: str) -> None:
        if value and _year(value, cy) and not any(l == label for l, _ in out):
            out.append((label, value.strip()))

    # meta published
    add("meta_published", _search([
        r'property=["\']article:published_time["\'][^>]*content=["\']([^"\']+)',
        r'content=["\']([^"\']+)["\'][^>]*property=["\']article:published_time["\']',
        r'name=["\'](?:date|dc\.date|dcterms\.date|publish-date|pubdate|sailthru\.date|parsely-pub-date)["\'][^>]*content=["\']([^"\']+)',
        r'itemprop=["\']datePublished["\'][^>]*content=["\']([^"\']+)',
    ], head))
    # JSON-LD published
    add("jsonld_published", _search([
        r'"datePublished"\s*:\s*"([^"]+)"',
        r'"dateCreated"\s*:\s*"([^"]+)"',
    ], html))
    # <time datetime>
    add("time", _search([r'<time[^>]+datetime=["\']((?:19|20)\d\d-\d\d-\d\d[^"\']*)'], top))
    # visible dateline near the top
    add("byline", _search([
        r'((?:' + _MONTHS + r')[a-z]*\.?\s+\d{1,2},?\s+(?:19|20)\d\d)',
        r'(\d{1,2}\s+(?:' + _MONTHS + r')[a-z]*\.?\s+(?:19|20)\d\d)',
    ], top))
    # modified / updated
    add("modified", _search([
        r'property=["\']article:modified_time["\'][^>]*content=["\']([^"\']+)',
        r'property=["\']og:updated_time["\'][^>]*content=["\']([^"\']+)',
        r'"dateModified"\s*:\s*"([^"]+)"',
    ], head + html[:40000]))
    # htmldate best guess (original date)
    try:
        from htmldate import find_date

        dt = find_date(html, original_date=True)
        add("htmldate", dt or "")
    except Exception:
        pass
    # copyright, last resort
    add("copyright", _search([r'(?:©|&copy;|copyright)\s*(?:&\w+;)?\s*((?:19|20)\d\d)'], tail))

    if not out:
        return ""
    # Lead with the collection year so a reader can recognise a crawl/copyright
    # artifact (a signal whose year == today) without knowing the current date.
    parts = [f"today={cy}"] + [f"{label}={value}" for label, value in out]
    return "; ".join(parts)
