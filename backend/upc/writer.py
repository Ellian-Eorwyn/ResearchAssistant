"""Corpus write primitives: atomic files, JSONL, and zero-copy byte placement.

UPC §10 requires every write to be atomic and every projection to be regenerable,
so a crash leaves a readable corpus rather than a half-written one. These helpers
are the only place this integration touches the output tree.
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any, Iterable, Sequence


def atomic_write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)


def atomic_write_text(path: Path, text: str) -> None:
    atomic_write_bytes(path, text.encode("utf-8"))


def write_json(path: Path, obj: Any) -> None:
    """Pretty JSON with a trailing newline, matching the reference tooling's output
    so a corpus written here and one written by `upc regen` diff cleanly."""
    atomic_write_text(path, json.dumps(obj, indent=2, ensure_ascii=False) + "\n")


def write_jsonl(path: Path, records: Sequence[dict[str, Any]]) -> None:
    """One compact object per line (§10). An empty collection still writes an empty
    file, so the corpus declares "no records here" rather than "file missing"."""
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
    atomic_write_text(path, body + "\n" if body else "")


def append_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> None:
    recs = list(records)
    if not recs:
        return
    prior = read_jsonl(path) if path.is_file() else []
    write_jsonl(path, prior + recs)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        try:
            out.append(json.loads(s))
        except json.JSONDecodeError:
            # A torn final line is tolerated by §10; anything else the validator
            # will report as jsonl_invalid once it reads the file.
            continue
    return out


def place_bytes(src: Path, dst: Path, *, copy: bool = False) -> str:
    """Put a source file into the corpus without duplicating it when possible.

    Hardlink by default: the corpus then contains a real directory entry whose
    realpath is inside the corpus, which is what §08 rule 0.3 requires. A **symlink
    would fail** that rule (`symlink_escape`), so it is never used. Across
    filesystems a hardlink is impossible and we fall back to copying.

    Returns "link", "copy", or "exists".
    """
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        return "exists"
    if not copy:
        try:
            os.link(src, dst)
            return "link"
        except OSError:
            pass  # cross-device, or a filesystem without hardlinks
    shutil.copyfile(src, dst)
    return "copy"
