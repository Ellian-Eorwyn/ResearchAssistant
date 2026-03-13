"""Text normalization and line unwrapping utilities."""

from __future__ import annotations

import re
import unicodedata


def normalize_whitespace(text: str) -> str:
    """Collapse runs of whitespace (except newlines) to single spaces."""
    text = re.sub(r"[^\S\n]+", " ", text)
    return text.strip()


def normalize_unicode(text: str) -> str:
    """Normalize common unicode variants to ASCII equivalents."""
    # Dashes
    text = text.replace("\u2013", "-")  # en-dash
    text = text.replace("\u2014", "-")  # em-dash
    text = text.replace("\u2012", "-")  # figure dash
    # Quotes
    text = text.replace("\u201c", '"')
    text = text.replace("\u201d", '"')
    text = text.replace("\u2018", "'")
    text = text.replace("\u2019", "'")
    # Spaces
    text = text.replace("\u00a0", " ")  # non-breaking space
    text = text.replace("\u200b", "")  # zero-width space
    # Ligatures
    text = unicodedata.normalize("NFKD", text)
    return text


def unwrap_lines(text: str) -> str:
    """Rejoin lines broken by PDF column wrapping.

    Heuristic: if a line ends without sentence-terminal punctuation
    and the next line starts with a lowercase letter or a continuation
    character, join the lines.
    """
    lines = text.split("\n")
    if len(lines) <= 1:
        return text

    result: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i].rstrip()
        if not line:
            result.append("")
            i += 1
            continue

        # Check if line should be joined with the next
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if next_line and _should_join(line, next_line):
                # Handle hyphenated breaks
                if line.endswith("-"):
                    line = line[:-1]  # remove trailing hyphen
                    result.append(line)
                else:
                    result.append(line + " ")
                i += 1
                continue

        result.append(line + "\n")
        i += 1

    return "".join(result).strip()


def _should_join(current_line: str, next_line: str) -> bool:
    """Determine whether two lines should be joined."""
    if not next_line:
        return False

    # If current line ends with sentence-terminal punctuation, don't join
    terminal_chars = ".!?:;"
    stripped = current_line.rstrip()
    if stripped and stripped[-1] in terminal_chars:
        # Exception: abbreviations like "et al." or "e.g."
        if re.search(r"\b(?:et al|e\.g|i\.e|vs|Fig|Eq|No|Vol)\.\s*$", stripped):
            return True
        return False

    # If next line starts with lowercase, likely continuation
    if next_line[0].islower():
        return True

    # If current line ends with hyphen, join
    if stripped.endswith("-"):
        return True

    # If current line ends with a comma or conjunction, join
    if stripped[-1] in ",(&" or stripped.endswith(" and") or stripped.endswith(" or"):
        return True

    return False


def clean_text_block(text: str) -> str:
    """Apply all cleaning steps to a text block."""
    text = normalize_unicode(text)
    text = normalize_whitespace(text)
    return text.strip()
