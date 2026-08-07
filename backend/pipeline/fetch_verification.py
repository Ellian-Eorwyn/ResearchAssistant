"""Verifies that a fetched source actually contains the document we asked for.

A fetch can return HTTP 200 and still be worthless: bot walls, CAPTCHA
interstitials, cookie/JavaScript gates, login walls and paywalls all serve a
perfectly valid page that simply is not the source. This module scores the
fetched content and reports whether it is real, so the caller can mark the row
`blocked` instead of `success` and keep the block page out of the LLM phases.

Pure functions only: no I/O, no network, and no import of `source_downloader`
(that module imports this one).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum

FETCH_VERIFICATION_VERSION = "fetch_verification.v1"

# Below this many words the content-based heuristics are allowed to fire. A real
# article that merely *mentions* CAPTCHAs is long; a block page never is.
WEAK_SIGNAL_MAX_WORDS = 400
JS_WALL_MAX_WORDS = 200
HTTP_BLOCK_MAX_WORDS = 120
THIN_MAX_WORDS = 25
SHORT_MAX_WORDS = 120

# Only the head of the body is inspected for weak phrases: interstitials lead
# with them, articles bury them.
HEAD_CHARS = 1500
HTML_SCAN_CHARS = 20000

# Past this much raw HTML, a single strong phrase is no longer enough on its
# own. Before extraction there is no word count to scale by -- every page looks
# thin -- so length has to come from the markup itself, or an article *about*
# blocking is condemned by one quoted phrase. Only the certain tier below
# overrides this, because those phrases are boilerplate no article quotes.
HTML_ARTICLE_CHARS = 3000

BLOCKING_HTTP_STATUS = {401, 402, 403, 407, 429, 451}
# 2xx codes that a bot wall returns to look successful while serving a wall.
SOFT_BLOCK_HTTP_STATUS = {202, 203, 205, 206}

# Content phrases mean nothing for these: a PDF *about* Cloudflare exists, and a
# transcript can legitimately discuss CAPTCHAs.
NON_HTML_TYPES = {"pdf", "document", "video"}


class FetchVerdict(str, Enum):
    OK = "ok"
    BLOCKED_CHALLENGE = "blocked_challenge"
    BLOCKED_HTTP = "blocked_http"
    LOGIN_REQUIRED = "login_required"
    PAYWALL = "paywall"
    THIN_CONTENT = "thin_content"
    EMPTY = "empty"


BLOCKED_VERDICTS = frozenset(
    {
        FetchVerdict.BLOCKED_CHALLENGE,
        FetchVerdict.BLOCKED_HTTP,
        FetchVerdict.LOGIN_REQUIRED,
        FetchVerdict.PAYWALL,
    }
)

# Error codes written into `row.error_message` and phase metadata for blocked
# fetches, so downstream code can recognise them without re-running the checks.
BLOCKED_ERROR_CODES = frozenset(verdict.value for verdict in BLOCKED_VERDICTS)


def _compile(patterns: list[str]) -> list[re.Pattern[str]]:
    return [re.compile(pattern, re.IGNORECASE) for pattern in patterns]


# Boilerplate a block page prints and an article almost never quotes verbatim.
# These decide alone, at any length, and are searched over the whole document
# rather than the scanned prefix: a rendered page can carry 20k of scripts and
# inlined styles before its body, which is exactly how a Reddit block page
# reached the analysis stage marked `success`.
CERTAIN_CHALLENGE_PATTERNS = _compile(
    [
        r"blocked by network security",
        r"security service to protect itself",
        r"triggered the security solution",
    ]
)

# A single hit is enough to condemn the page.
STRONG_CHALLENGE_PATTERNS = _compile(
    [
        r"you'?ve been blocked",
        r"blocked by network security",
        r"unusual request pattern",
        r"\berror 418\b",
        r"just a moment(?:\.\.\.|\b)",
        r"enable javascript and cookies to continue",
        r"checking (?:if|whether) (?:the site|your browser|you are)",
        r"verif(?:y|ying) (?:that )?you are (?:a )?human",
        r"errors\.edgesuite\.net",
        r"your support id is",
        r"cloudflare ray id",
        r"performance (?:&|and) security by cloudflare",
        r"incapsula incident id",
        r"request unsuccessful\. incapsula",
        r"press (?:and|&) hold",
        r"ddos protection by",
        r"attention required!",
        r"reference #\s*\d+\.\w+",
        r"why have i been blocked",
        r"pardon our interruption",
        r"are you a robot",
        r"our systems have detected unusual traffic",
        r"additional security check is required",
        r"client challenge",
    ]
)

# Suggestive but not conclusive; only counted near the top of a short body.
WEAK_CHALLENGE_PATTERNS = _compile(
    [
        r"\bcaptcha\b",
        r"access (?:to this page has been )?denied",
        r"\bsecurity check\b",
        r"request (?:blocked|rejected)",
        r"\brate limit(?:ed)?\b",
        r"bot detection",
        r"too many requests",
        r"unusual traffic",
        r"human verification",
        r"\bakamai\b",
        r"\bperimeterx\b",
        r"\bforbidden\b",
    ]
)

TITLE_CHALLENGE_PATTERNS = _compile(
    [
        r"just a moment",
        r"access denied",
        r"attention required",
        r"unable to load",
        r"403 forbidden",
        r"security check",
        r"robot check",
        r"captcha",
        r"request rejected",
        r"unusual traffic",
        r"blocked",
        r"error 4\d\d",
        r"pardon our interruption",
        r"human verification",
        r"one moment",
    ]
)

JS_WALL_PATTERNS = _compile(
    [
        r"(?:please )?enable javascript",
        r"javascript is (?:required|disabled)",
        r"enable cookies",
        r"cookies (?:are|must be) enabled",
        r"your browser does not support javascript",
    ]
)

LOGIN_WALL_PATTERNS = _compile(
    [
        r"sign in to continue",
        r"log ?in to continue",
        r"please (?:sign|log) ?in",
        r"create a (?:free )?account to (?:continue|read)",
        r"register to (?:read|continue)",
        r"you (?:must|need to) (?:sign|log) ?in",
    ]
)

PAYWALL_PATTERNS = _compile(
    [
        r"subscribe to (?:continue|read)",
        r"subscribers only",
        r"this (?:content|article) is for subscribers",
        r"you have reached your (?:free )?article limit",
        r"unlock this article",
        r"become a member to read",
    ]
)

PASSWORD_INPUT_RE = re.compile(r"""<input[^>]+type=["']password""", re.IGNORECASE)
# schema.org paywall marker — very precise when present.
NOT_FREE_RE = re.compile(r""""isAccessibleForFree"\s*:\s*(?:false|"False")""", re.IGNORECASE)

HEADING_RE = re.compile(r"(?m)^#{1,6}\s")
PARAGRAPH_SPLIT_RE = re.compile(r"\n\s*\n")
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class FetchVerification:
    """The outcome of checking one fetched source."""

    verdict: FetchVerdict
    reason: str
    suggested_status: str
    message: str
    signals: tuple[str, ...] = ()
    score: int = 0
    word_count: int = 0

    @property
    def is_blocked(self) -> bool:
        return self.verdict in BLOCKED_VERDICTS


@dataclass
class _Evidence:
    """Signals gathered before the decision ladder runs."""

    words: int = 0
    text: str = ""
    head: str = ""
    html_head: str = ""
    title: str = ""
    structure_score: int = 0
    score: int = 0
    signals: list[str] = field(default_factory=list)
    certain: list[str] = field(default_factory=list)
    strong_lead: list[str] = field(default_factory=list)
    strong_body: list[str] = field(default_factory=list)
    weak: list[str] = field(default_factory=list)
    title_hits: list[str] = field(default_factory=list)
    http_block: bool = False
    soft_2xx: bool = False
    js_wall: bool = False
    login_wall: bool = False
    paywall: bool = False


def _matches(patterns: list[re.Pattern[str]], haystack: str) -> list[str]:
    if not haystack:
        return []
    found: list[str] = []
    for pattern in patterns:
        match = pattern.search(haystack)
        if match:
            found.append(match.group(0).strip().lower())
    return found


def _structure_score(markdown: str) -> tuple[int, int, int, int]:
    """Counter-signals that a page is a real document despite being short.

    A 432-character marketing homepage with five headings is legitimate; a
    465-character bot wall has none. Structure, not length, separates them.
    """
    headings = len(HEADING_RE.findall(markdown))
    links = markdown.count("](")
    paragraphs = len([block for block in PARAGRAPH_SPLIT_RE.split(markdown) if len(block.strip()) > 40])

    score = 0
    if headings >= 3:
        score -= 1
    if links >= 5:
        score -= 1
    if paragraphs >= 3:
        score -= 1
    return score, headings, links, paragraphs


def _gather(
    *,
    http_status: int | None,
    title: str,
    raw_html: str,
    extracted_text: str,
    detected_type: str,
) -> _Evidence:
    markdown = extracted_text or ""
    text = WHITESPACE_RE.sub(" ", markdown).strip()
    words = len(text.split()) if text else 0
    head = text[:HEAD_CHARS]
    html_head = (raw_html or "")[:HTML_SCAN_CHARS]
    title_l = (title or "").strip().lower()

    ev = _Evidence(words=words, text=text, head=head, html_head=html_head, title=title_l)
    ev.structure_score, headings, links, paragraphs = _structure_score(markdown)

    # Certain phrases decide alone, searched over the whole document rather than
    # the scanned prefix. Where extraction has already run, its word count is the
    # authority on whether this is a real article, and a long one is exempt --
    # it may legitimately quote the phrase. Before extraction there is no such
    # count and a block page is the whole document, so the phrase is decisive.
    if not text or words < WEAK_SIGNAL_MAX_WORDS:
        ev.certain = _matches(CERTAIN_CHALLENGE_PATTERNS, raw_html or "")
        ev.certain += _matches(CERTAIN_CHALLENGE_PATTERNS, text)

    # Strong phrases: always trusted in the lead (or title); trusted deeper in
    # the body, or in the raw HTML, only while the body is short enough that it
    # cannot be a real article quoting one of these phrases. With nothing
    # extracted yet the word count is 0 for every page, so the raw HTML's own
    # length stands in -- otherwise one quoted phrase condemns a long article.
    html_reads_as_article = not text and len((raw_html or "").strip()) > HTML_ARTICLE_CHARS
    ev.strong_lead = _matches(STRONG_CHALLENGE_PATTERNS, head)
    ev.strong_lead += _matches(STRONG_CHALLENGE_PATTERNS, title_l)
    ev.strong_body = _matches(STRONG_CHALLENGE_PATTERNS, text)
    if words < WEAK_SIGNAL_MAX_WORDS and not html_reads_as_article:
        ev.strong_body += _matches(STRONG_CHALLENGE_PATTERNS, html_head)

    if words < WEAK_SIGNAL_MAX_WORDS:
        ev.weak = _matches(WEAK_CHALLENGE_PATTERNS, head)
    ev.title_hits = _matches(TITLE_CHALLENGE_PATTERNS, title_l)

    ev.http_block = bool(http_status) and http_status in BLOCKING_HTTP_STATUS
    ev.soft_2xx = (
        detected_type == "html" and bool(http_status) and http_status in SOFT_BLOCK_HTTP_STATUS
    )

    if words < JS_WALL_MAX_WORDS:
        ev.js_wall = bool(_matches(JS_WALL_PATTERNS, head))

    # Login and paywall markers are structural and live in the HTML of plenty of
    # perfectly good pages (a collapsed sign-in modal, a mixed-access site), so
    # they only count once extraction has confirmed the body is also thin.
    if text and words < WEAK_SIGNAL_MAX_WORDS:
        ev.login_wall = bool(_matches(LOGIN_WALL_PATTERNS, head)) or bool(
            PASSWORD_INPUT_RE.search(html_head)
        )
        ev.paywall = bool(_matches(PAYWALL_PATTERNS, head)) or bool(NOT_FREE_RE.search(html_head))
    if http_status == 402:
        ev.paywall = True

    # Score, and record human-readable signals in the same pass. The certain and
    # strong tiers overlap, so they are deduped together rather than separately.
    for phrase in dict.fromkeys(ev.certain + ev.strong_lead + ev.strong_body):
        ev.score += 3
        ev.signals.append(f"body: {phrase!r}")
    for phrase in dict.fromkeys(ev.weak):
        ev.score += 1
        ev.signals.append(f"body: {phrase!r}")
    for phrase in dict.fromkeys(ev.title_hits):
        ev.score += 2
        ev.signals.append(f"title: {phrase!r}")
    if ev.http_block:
        ev.score += 3
        ev.signals.append(f"http_status_{http_status}")
    if ev.soft_2xx:
        ev.score += 2
        ev.signals.append(f"soft_http_status_{http_status}")
    if ev.js_wall:
        ev.score += 2
        ev.signals.append("javascript/cookie gate")
    if ev.login_wall:
        ev.score += 2
        ev.signals.append("login form")
    if ev.paywall:
        ev.score += 2
        ev.signals.append("paywall marker")
    # Shortness only counts when something was actually extracted. With no text
    # at all there is nothing to be suspicious *about* — that is the EMPTY case,
    # and scoring it here would let a single title match tip the ladder over.
    if text:
        if words < THIN_MAX_WORDS:
            ev.score += 2
            ev.signals.append(f"only {words} words extracted")
        elif words < SHORT_MAX_WORDS:
            ev.score += 1
            ev.signals.append(f"only {words} words extracted")
    if ev.structure_score:
        ev.score += ev.structure_score
        ev.signals.append(
            f"document structure ({headings} headings, {links} links, {paragraphs} paragraphs)"
        )

    return ev


def _result(
    verdict: FetchVerdict,
    suggested_status: str,
    summary: str,
    ev: _Evidence,
) -> FetchVerification:
    message = summary
    if ev.signals:
        message = f"{summary} ({'; '.join(ev.signals[:4])})"
    return FetchVerification(
        verdict=verdict,
        reason=verdict.value,
        suggested_status=suggested_status,
        message=message,
        signals=tuple(ev.signals),
        score=ev.score,
        word_count=ev.words,
    )


def verify_fetch(
    *,
    http_status: int | None = None,
    final_url: str = "",
    title: str = "",
    raw_html: str = "",
    extracted_text: str = "",
    content_type: str = "",
    detected_type: str = "html",
    source_kind: str = "url",
) -> FetchVerification:
    """Judge whether a fetched source holds the real document.

    `suggested_status` is the `fetch_status` the caller should apply, or `""`
    for `OK`, which means "no objection — keep your own success/partial ladder".
    """
    ev = _gather(
        http_status=http_status,
        title=title,
        raw_html=raw_html,
        extracted_text=extracted_text,
        detected_type=detected_type,
    )

    url_l = (final_url or "").strip().lower()
    if "cdn-cgi/challenge-platform" in url_l:
        ev.signals.insert(0, "challenge redirect in final URL")
        return _result(
            FetchVerdict.BLOCKED_CHALLENGE,
            "blocked",
            "Redirected to a bot-challenge platform",
            ev,
        )

    # Binary and transcript sources carry no reliable textual tells, so only
    # HTTP evidence and outright emptiness count against them.
    if source_kind == "uploaded_document" or detected_type in NON_HTML_TYPES:
        if ev.http_block:
            return _result(
                FetchVerdict.BLOCKED_HTTP,
                "blocked",
                f"Server refused the request with HTTP {http_status}",
                ev,
            )
        if not ev.text and detected_type != "video":
            return _result(FetchVerdict.EMPTY, "failed", "No content was extracted", ev)
        return _result(FetchVerdict.OK, "", "Content looks genuine", ev)

    # Boilerplate no article quotes verbatim, so length and corroboration are
    # both beside the point. Deliberately after the binary-type guard above: a
    # PDF may legitimately be *about* one of these phrases.
    if ev.certain:
        return _result(
            FetchVerdict.BLOCKED_CHALLENGE,
            "blocked",
            "Challenge or interstitial page detected",
            ev,
        )

    if ev.paywall and (ev.words < WEAK_SIGNAL_MAX_WORDS or http_status == 402):
        return _result(FetchVerdict.PAYWALL, "blocked", "Paywall or subscriber gate", ev)

    if ev.login_wall and ev.words < WEAK_SIGNAL_MAX_WORDS:
        return _result(FetchVerdict.LOGIN_REQUIRED, "blocked", "Sign-in required", ev)

    if ev.strong_lead or (ev.strong_body and ev.words < WEAK_SIGNAL_MAX_WORDS):
        return _result(
            FetchVerdict.BLOCKED_CHALLENGE,
            "blocked",
            "Challenge or interstitial page detected",
            ev,
        )

    if ev.score >= 3 and (ev.weak or ev.title_hits or ev.soft_2xx):
        return _result(
            FetchVerdict.BLOCKED_CHALLENGE,
            "blocked",
            "Challenge or interstitial page detected",
            ev,
        )

    if ev.http_block and ev.words < HTTP_BLOCK_MAX_WORDS:
        return _result(
            FetchVerdict.BLOCKED_HTTP,
            "blocked",
            f"Server refused the request with HTTP {http_status}",
            ev,
        )

    if ev.js_wall:
        return _result(
            FetchVerdict.BLOCKED_CHALLENGE,
            "blocked",
            "Page requires JavaScript or cookies to render",
            ev,
        )

    # Binary types already returned above, so anything still here (html,
    # unsupported, or a type we never got far enough to detect) should have text.
    if not ev.text:
        return _result(FetchVerdict.EMPTY, "failed", "No content was extracted", ev)

    if ev.words < THIN_MAX_WORDS and ev.structure_score == 0 and detected_type == "html":
        return _result(
            FetchVerdict.THIN_CONTENT,
            "partial",
            "Almost no content was extracted",
            ev,
        )

    return _result(FetchVerdict.OK, "", "Content looks genuine", ev)


def looks_blocked(*, raw_html: str, title: str, final_url: str) -> bool:
    """Cheap pre-render check against raw HTML alone.

    Used before Playwright is invoked: a decisively blocked page is not worth a
    20-second render.
    """
    verification = verify_fetch(
        raw_html=raw_html,
        title=title,
        final_url=final_url,
        extracted_text="",
        detected_type="html",
    )
    # Only challenge and HTTP evidence survive without extracted text; the login
    # and paywall signals need a body to judge and never fire here anyway.
    return verification.verdict in {FetchVerdict.BLOCKED_CHALLENGE, FetchVerdict.BLOCKED_HTTP}
