"""Read a column's allowed answers out of its own prompt.

The two bad cells in the first real column run — one empty, one holding the
literal string `insufficient_evidence` — had a single cause: `create_columns`
never populated `output_constraint`, so every column stored `allowed_values: []`
and `fallback_value: ""`. With the values present, `_coerce_column_output_value`
already restores canonical spelling on a case-insensitive match and substitutes
the fallback on anything else, so both cells would have been caught. Nothing in
the run path needs to change; the values just have to be there.

This is **extraction, not inference.** It fires only behind an explicit anchor
phrase the prompt author wrote to be read literally:

    Return exactly one of these values, spelled exactly as shown:

    Utility
    For-profit
    ...

Anything the slightest bit ambiguous returns `None`, and `create_columns` shows
what was extracted in its dry run before a single value is written. A prompt
that says "return a semicolon-separated list" does not match the anchor and so
is left alone — a multi-token answer must not be constrained to one value.
"""

from __future__ import annotations

import re

# Only phrasings that promise a literal enumeration follows. Deliberately not
# generous: a miss costs nothing, a false positive silently rewrites answers to
# the fallback.
_ANCHORS = (
    re.compile(
        r"return\s+exactly\s+one\s+of\s+(?:these|the\s+following)\s+values"
        r"(?:\s*,\s*spelled\s+exactly\s+as\s+shown)?\s*:",
        re.IGNORECASE,
    ),
    re.compile(r"return\s+exactly\s+one\s+of\s+the\s+following\s*:", re.IGNORECASE),
)

MIN_VALUES = 2
MAX_VALUES = 30
MAX_WORDS_PER_VALUE = 6
MAX_CHARS_PER_VALUE = 60


def _looks_like_prose(line: str) -> bool:
    """Is this explanation rather than an allowed value?"""
    if line.startswith(("-", "*", "•", ">", "#")):
        return True
    if re.match(r"^\d+[.)]\s", line):  # a numbered instruction, not a value
        return True
    if line.endswith((".", ":", ";", ",")):
        return True
    if len(line) > MAX_CHARS_PER_VALUE or len(line.split()) > MAX_WORDS_PER_VALUE:
        return True
    return False


def extract_allowed_values(prompt: str) -> list[str]:
    """The literal enumeration a prompt lists, or `[]` if it does not list one."""
    text = (prompt or "").replace("\r\n", "\n").replace("\r", "\n")
    match = None
    for anchor in _ANCHORS:
        match = anchor.search(text)
        if match:
            break
    if not match:
        return []

    values: list[str] = []
    seen: set[str] = set()
    started = False
    for raw in text[match.end() :].split("\n"):
        line = raw.strip()
        if not line:
            # Blank lines before the list are the author's formatting; a blank
            # line after it ends the list.
            if started:
                break
            continue
        if _looks_like_prose(line):
            break
        started = True
        key = line.casefold()
        if key in seen:  # a repeat means this is no longer a clean list
            return []
        seen.add(key)
        values.append(line)
        if len(values) > MAX_VALUES:
            return []

    return values if len(values) >= MIN_VALUES else []


def derive_constraint(prompt: str) -> dict | None:
    """Build an `output_constraint` for a prompt, or `None` to leave it alone."""
    values = extract_allowed_values(prompt)
    if not values:
        return None

    # The prompts end their list with their own "no answer" option. Using it as
    # the fallback keeps the author's exact spelling ("Not sure", "Not Sure",
    # "not sure" all occur) instead of leaving the cell blank.
    fallback = next((v for v in values if re.fullmatch(r"not\s+sure", v, re.IGNORECASE)), "")
    return {
        "kind": "text",
        "allowed_values": values,
        "max_words": None,
        "fallback_value": fallback,
        "format_hint": "",
    }
