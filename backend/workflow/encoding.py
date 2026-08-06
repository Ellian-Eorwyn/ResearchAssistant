"""Repair text that was written as UTF-8 and read back as a legacy codepage.

The failure looks like `‚Äî` where an em dash belongs. It happens whenever a
spreadsheet is exported on one platform and opened on another: the UTF-8 bytes
for `—` (E2 80 94) get decoded as MacRoman, producing three separate characters.
Windows-1252 produces the same class of damage (`â€"`) and is more common still.

Repairing is a round trip: encode the mangled text back to the codepage that
produced it, then decode those bytes as UTF-8. That is only safe when it is
lossless, so a repair is accepted only when it round-trips exactly *and*
demonstrably reduces the damage. Otherwise it is refused, with a reason, and the
caller reports it rather than guessing.
"""

from __future__ import annotations

from .models import EncodingRepair

# Codecs that map every byte 0x00-0xFF to a character, so a round trip can be
# attempted at all. Ordered by how often the damage is seen in the wild.
CANDIDATE_CODECS: tuple[str, ...] = ("cp1252", "mac_roman")

# Sequences that only appear when UTF-8 has been misread. Each is the start of a
# mangled multi-byte character rather than a whole one, so counting them
# approximates "how much damage is in this text".
SUSPICIOUS_MARKERS: tuple[str, ...] = (
    "‚Ä",  # MacRoman reading of E2 80 xx -- dashes, curly quotes, ellipsis
    "‚Ü",  # MacRoman reading of E2 86 xx -- arrows
    "â€",  # cp1252 reading of the same E2 80 xx range
    "Ã¢",  # cp1252 reading of a UTF-8 lead byte
    "Ã©",  # cp1252 reading of e-acute
    "Ã¨",
    "Ã±",
    "Ã¼",
    "Â ",  # cp1252 reading of a non-breaking space
    "Â·",
    "ï¿½",  # a replacement character that was itself re-encoded
    "√©",  # MacRoman equivalents
    "√±",
    "√º",
)


def count_suspicious(text: str) -> int:
    """How many mojibake markers appear in this text."""
    if not text:
        return 0
    return sum(text.count(marker) for marker in SUSPICIOUS_MARKERS)


def looks_mangled(text: str) -> bool:
    return count_suspicious(text) > 0


def repair_text(text: str, *, codecs: tuple[str, ...] = CANDIDATE_CODECS) -> tuple[str, str]:
    """Return `(repaired_text, codec_used)`, or `(text, "")` when not repairable.

    A candidate is accepted only if the round trip is exact and the damage
    strictly decreases. Both conditions matter: the round trip proves we are
    reversing a real mis-decode rather than inventing characters, and the
    decrease proves the result is actually better.
    """
    if not text or not looks_mangled(text):
        return text, ""

    before = count_suspicious(text)
    for codec in codecs:
        try:
            repaired = text.encode(codec).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError, LookupError):
            continue
        if repaired == text:
            continue
        try:
            # Reversing the repair must reproduce the original exactly, or we
            # are corrupting something rather than fixing it.
            if repaired.encode("utf-8").decode(codec) != text:
                continue
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        if count_suspicious(repaired) >= before:
            continue
        return repaired, codec

    return text, ""


def repair_grid(
    grid: list[list[str]],
    *,
    mode: str = "auto",
) -> tuple[list[list[str]], EncodingRepair]:
    """Repair a whole sheet, choosing one codec for the entire grid.

    Picking a single codec rather than deciding cell by cell matters: a
    spreadsheet was mangled once, by one program, so a per-cell choice would
    only ever mean one of the choices is wrong.
    """
    report = EncodingRepair()
    if mode == "never":
        report.refused_reason = "disabled"
        return grid, report

    cells = [cell for row in grid for cell in row if cell]
    report.cells_examined = len(cells)
    report.suspicious_before = sum(count_suspicious(cell) for cell in cells)

    if report.suspicious_before == 0 and mode != "force":
        report.suspicious_after = 0
        return grid, report

    # Score each codec over the whole grid and take the one that removes most.
    best_codec = ""
    best_removed = 0
    for codec in CANDIDATE_CODECS:
        removed = 0
        usable = True
        for cell in cells:
            repaired, used = repair_text(cell, codecs=(codec,))
            if used:
                removed += count_suspicious(cell) - count_suspicious(repaired)
            elif looks_mangled(cell):
                # This codec cannot explain a damaged cell; it is the wrong one.
                usable = False
                break
        if usable and removed > best_removed:
            best_codec, best_removed = codec, removed

    if not best_codec:
        report.refused_reason = (
            "no codec round-tripped cleanly; the text may be damaged in a way that "
            "cannot be reversed automatically"
        )
        report.suspicious_after = report.suspicious_before
        return grid, report

    repaired_grid: list[list[str]] = []
    changed = 0
    for row in grid:
        new_row = []
        for cell in row:
            repaired, used = repair_text(cell, codecs=(best_codec,))
            if used and repaired != cell:
                changed += 1
            new_row.append(repaired)
        repaired_grid.append(new_row)

    report.applied = True
    report.codec = best_codec
    report.cells_changed = changed
    report.suspicious_after = sum(
        count_suspicious(cell) for row in repaired_grid for cell in row if cell
    )
    return repaired_grid, report
