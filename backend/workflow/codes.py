"""What each pipeline error code means, and what to do about it.

This is the single place a code's meaning is written down. The skills used to
carry their own copy of this table in prose, and it drifted: they listed
`http_status_5xx` and `http_status_429`, neither of which is a code — the HTTP
number is the *detail after* the colon, and the code is `network_failure`.

Two subtleties that make hand-written guidance unreliable and are handled here:

* `classify_http_status` maps 401/403/407/429 to `blocked_request` and **every
  other 4xx and 5xx** to `network_failure`. So a 404 and a 503 arrive under the
  same code and must be split on the detail. A 503 is worth retrying; a 404 is
  a wrong URL.
* `blocked_request` already means the pipeline tried its headless browser and
  lost. Retrying it unchanged cannot help, so it routes to a manual download
  rather than another attempt.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from .models import Classification

_HTTP_DETAIL = re.compile(r"http_status_(\d{3})")

# httpx's connection-level failures, which arrive with no HTTP status at all.
# `nodename nor servname` is what macOS returns for a name that did not resolve.
_CONNECTION_ERROR = re.compile(
    r"ConnectError|ConnectTimeout|nodename nor servname|Name or service not known"
    r"|Temporary failure in name resolution|Connection refused",
    re.IGNORECASE,
)

# HTTP statuses worth another attempt: server-side faults and rate limits.
RETRYABLE_HTTP = frozenset({408, 425, 429, 500, 502, 503, 504, 507, 509})
# Statuses that mean the URL itself is wrong.
BROKEN_HTTP = frozenset({400, 404, 405, 410, 414, 451})


@dataclass(frozen=True)
class CodeMeaning:
    classification: Classification
    explanation: str


CODE_TABLE: dict[str, CodeMeaning] = {
    "timeout": CodeMeaning(
        "retryable",
        "The site did not respond in time. Often transient, especially under load.",
    ),
    "network_failure": CodeMeaning(
        "unknown",
        "An HTTP error. Whether it is worth retrying depends on the status code "
        "in the detail, so this is split further.",
    ),
    "internal_error": CodeMeaning(
        "retryable",
        "The pipeline hit an error it did not expect and failed this one source "
        "rather than ending the whole run. The exception is in the detail. A "
        "retry is worth trying, since some causes are transient -- but if it "
        "repeats, this is a bug worth reporting rather than a source to fix.",
    ),
    "blocked_request": CodeMeaning(
        "needs_manual_document",
        "The site refused the request. The pipeline already retried it in a headless "
        "browser and was refused again, so another attempt will not help. Download the "
        "document by hand and attach it.",
    ),
    # The four verdicts `fetch_verification` records when the bytes that came
    # back are a wall rather than the document. They arrive after the pipeline
    # has already retried in a headless browser, so none of them is retryable --
    # what is needed is the document itself.
    "blocked_challenge": CodeMeaning(
        "needs_manual_document",
        "What came back is a bot-check or interstitial page, not the document -- a "
        "CDN 'access denied', a Cloudflare challenge, or similar. The browser retry "
        "was refused too. Save the page by hand and attach it.",
    ),
    "blocked_http": CodeMeaning(
        "needs_manual_document",
        "The site answered with a status that means refusal (401, 402, 403, 407, 429 "
        "or 451) and the page body confirmed it. Save the page by hand and attach it.",
    ),
    "login_required": CodeMeaning(
        "needs_manual_document",
        "The page is behind a sign-in. The pipeline does not hold credentials and "
        "will not enter any. Sign in yourself, save the page, and attach it.",
    ),
    "paywall": CodeMeaning(
        "needs_manual_document",
        "What came back is a subscription prompt rather than the article. If you have "
        "access, open it yourself, save the page, and attach it.",
    ),
    "blocked_fetch": CodeMeaning(
        "needs_manual_document",
        "The phase did not run because the stored content is a block or challenge "
        "page rather than the document, and analysing it would describe the wall. "
        "Get the real document -- by hand, or through Resolve Fetches -- and the "
        "phase will run against it.",
    ),
    "invalid_url": CodeMeaning(
        "broken_url",
        "The address is not usable. Fix the cell in the source spreadsheet.",
    ),
    "extraction_failure": CodeMeaning(
        "needs_manual_document",
        "The page was fetched but no readable text came out of it. Usually a viewer "
        "or an app shell. Save the document by hand and attach it.",
    ),
    "media_download_failed": CodeMeaning(
        "needs_manual_document",
        "The video or audio could not be downloaded. It may be private, removed, or "
        "region-locked.",
    ),
    "download_failure": CodeMeaning(
        "retryable",
        "The download produced no result. Worth one more attempt.",
    ),
    "convert_missing_prerequisite": CodeMeaning(
        "retryable_convert",
        "There is nothing to convert because the fetched file is missing. Fetch first, "
        "or attach the document by hand.",
    ),
    "missing_markdown": CodeMeaning(
        "retryable_convert",
        "A later phase needed the converted text and it was not there. Run convert.",
    ),
    "runtime_missing_yt_dlp": CodeMeaning(
        "environment",
        "yt-dlp is not installed, so video sources cannot be downloaded.",
    ),
    "playwright_not_installed": CodeMeaning(
        "environment",
        "The headless browser is not installed, so pages needing rendering will fail.",
    ),
    "rendering_failure": CodeMeaning(
        "environment",
        "The headless browser failed to render the page.",
    ),
    "llm_disabled": CodeMeaning(
        "environment",
        "The LLM is switched off in Settings, so this phase cannot run.",
    ),
    "llm_not_configured": CodeMeaning(
        "environment",
        "No usable LLM backend is configured. Set one in Settings.",
    ),
    "missing_project_profile": CodeMeaning(
        "environment",
        "The phase needs a project profile and none was selected.",
    ),
    "summary_generation_failed": CodeMeaning(
        "retryable",
        "The model call for the summary failed. Often transient.",
    ),
    "rating_generation_failed": CodeMeaning(
        "retryable",
        "The model call for the rating failed. Often transient.",
    ),
    "citation_verification_failed": CodeMeaning(
        "retryable",
        "Citation verification failed. Often transient.",
    ),
    "unsupported_content": CodeMeaning(
        "ignore",
        "The response was a type the pipeline does not process.",
    ),
    "import_failure": CodeMeaning(
        "broken_url",
        "The row could not be imported.",
    ),
    "not_applicable": CodeMeaning(
        "ignore",
        "This phase does not apply to this source.",
    ),
}

UNKNOWN = CodeMeaning(
    "unknown",
    "No classification is recorded for this code. Report it to the user rather than guessing.",
)


def http_status_from_detail(detail: str) -> int | None:
    match = _HTTP_DETAIL.search(detail or "")
    return int(match.group(1)) if match else None


def classify(error_code: str, detail: str = "") -> tuple[Classification, str, str]:
    """Return `(classification, explanation, detail_pattern)` for one failure.

    `detail_pattern` is the sub-group key: `network_failure` alone is not
    actionable, but `network_failure` + `http_status_503` is.
    """
    code = (error_code or "").strip()
    meaning = CODE_TABLE.get(code, UNKNOWN)

    if code == "network_failure":
        status = http_status_from_detail(detail)
        if status is None:
            # No status means the request never got far enough to receive one:
            # DNS did not resolve, or the connection was refused or timed out.
            # Calling that "an HTTP error" sends the user looking at the site
            # when the thing to look at is usually their own network.
            if _CONNECTION_ERROR.search(detail or ""):
                return (
                    "retryable",
                    "The connection never got as far as an HTTP response -- the name did "
                    "not resolve, or the host refused or timed out. Usually the local "
                    "network or DNS rather than the site. Worth a retry.",
                    "no_connection",
                )
            return (
                "retryable",
                "A network error with no status recorded. Worth one retry.",
                "",
            )
        pattern = f"http_status_{status}"
        if status in RETRYABLE_HTTP:
            return (
                "retryable",
                f"HTTP {status} is a server-side or rate-limit failure. Retrying later, "
                "and one site at a time, usually works.",
                pattern,
            )
        if status in BROKEN_HTTP:
            return (
                "broken_url",
                f"HTTP {status} means the page is not there. The URL in the spreadsheet "
                "is probably wrong or the page has been removed.",
                pattern,
            )
        return (
            "needs_manual_document",
            f"HTTP {status}. Not something a retry will change; get the document by hand.",
            pattern,
        )

    return meaning.classification, meaning.explanation, ""


# How each classification is acted on. `{ids}` is filled with a comma-separated
# id list so the model copies a complete command rather than composing one.
REMEDY_TEMPLATES: dict[str, str] = {
    "retryable": "ra fetch --ids {ids} --wait",
    "retryable_convert": "ra convert --ids {ids} --force --wait",
    # Deliberately the dry run: `ra attach` without `--apply` reports what it
    # would do, which is what the attach skill asks the user to see first.
    "needs_manual_document": (
        "Download these by hand, put them in the repository, then: ra attach <files>"
    ),
    "broken_url": "Fix the URL in the spreadsheet, then: ra plan-sheet <file>",
    "environment": "ra doctor",
    "ignore": "",
    "unknown": "",
}


def remedy_for(classification: str, source_ids: list[str]) -> str:
    template = REMEDY_TEMPLATES.get(classification, "")
    if not template:
        return ""
    return template.format(ids=",".join(source_ids[:50]))
