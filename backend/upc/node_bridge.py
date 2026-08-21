"""The only place in ResearchAssistant that spawns Node.

UPC's reference implementation is zero-dependency Node. Everything whose output
must be bit-identical across implementations -- canonical JSON, URL
normalization, slugs, and every content-addressed id recipe -- is reached through
here rather than reimplemented in Python. That is not stylistic caution: running
UPC's real ``canonicalUrl`` over one repository's 101 source URLs changes 36 of
them (trailing-slash stripping, ``gbraid``/``gclid``/``utm_*`` removal), and RA's
own ``dedupe_url_key`` strips a much smaller set, so a Python port would mint 36
wrong ``src-`` ids and the validator would reject every one.

What Python *does* own is reading and verifying: JSON/JSONL parsing, hashing file
bytes, and the hop-B gate itself, which is ``text[start:end] == quote`` -- Python
``str`` indexing is codepoint indexing, which is exactly what UPC specifies.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Sequence

HERE = Path(__file__).resolve().parent
BRIDGE_SCRIPT = HERE / "_bridge.mjs"

# The minor version this integration is written against. `upc check-compat`
# enforces it; a corpus written by a newer minor still reads (must-ignore-unknown),
# but an older library cannot be trusted to mint or validate what we emit.
REQUIRES = "^1.6"

# Searched in order. The vendored copy wins so a deployment is self-contained and
# pinned; the source checkout is the development convenience.
_DEFAULT_HOMES = (
    HERE.parent / "vendor" / "upc",
    Path.home() / "provenance",
)


class UpcUnavailable(RuntimeError):
    """Node or the UPC package could not be located, or is the wrong version."""


@dataclass(frozen=True)
class UpcInfo:
    home: Path
    node: str
    spec_version: str
    schema_hash: str | None


def _looks_like_upc_home(p: Path) -> bool:
    return (p / "schemas").is_dir() and (p / "vocab" / "vocab.json").is_file() and (
        p / "skill" / "universal-provenance" / "scripts" / "upc_common.mjs"
    ).is_file()


def resolve_home(explicit: str | os.PathLike[str] | None = None) -> Path:
    """Locate the UPC package root."""
    candidates: list[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    env = os.environ.get("UPC_HOME")
    if env:
        candidates.append(Path(env))
    candidates.extend(_DEFAULT_HOMES)
    for c in candidates:
        c = c.expanduser()
        if _looks_like_upc_home(c):
            return c.resolve()
    tried = ", ".join(str(c) for c in candidates)
    raise UpcUnavailable(
        "could not locate the UPC package (needs schemas/, vocab/, and "
        f"skill/universal-provenance/scripts/). Tried: {tried}. "
        "Set UPC_HOME or pass --upc-home."
    )


def resolve_node() -> str:
    node = os.environ.get("UPC_NODE") or shutil.which("node")
    if not node:
        raise UpcUnavailable(
            "node was not found on PATH. RA already requires npm to build the "
            "frontend; the UPC bridge needs the node runtime too."
        )
    return node


class NodeBridge:
    """Batched access to the UPC reference library.

    Every call spawns one process for the whole batch, so a conversion that mints
    thousands of ids costs a handful of spawns rather than thousands.
    """

    def __init__(self, home: str | os.PathLike[str] | None = None, *, check: bool = True) -> None:
        self.home = resolve_home(home)
        self.node = resolve_node()
        self._cli = self.home / "skill" / "universal-provenance" / "scripts" / "upc.mjs"
        self.info = self._handshake() if check else None

    # -- process plumbing -------------------------------------------------

    def _env(self) -> dict[str, str]:
        env = dict(os.environ)
        env["UPC_HOME"] = str(self.home)
        # Pin schema discovery rather than relying on the walk-up heuristic, which
        # can find the wrong sibling directory in a vendored layout.
        env.setdefault("UPC_SCHEMA_DIR", str(self.home / "schemas"))
        env.setdefault("UPC_VOCAB_DIR", str(self.home / "vocab"))
        return env

    def cli(self, args: Sequence[str], stdin: str = "") -> subprocess.CompletedProcess[str]:
        """Run the ``upc`` CLI. Returns the completed process; callers decide about
        the exit code, because several commands exit non-zero to report findings
        rather than failure."""
        return subprocess.run(
            [self.node, str(self._cli), *args],
            input=stdin,
            capture_output=True,
            text=True,
            env=self._env(),
        )

    def cli_json(self, args: Sequence[str], stdin: str = "") -> Any:
        proc = self.cli(args, stdin)
        if not proc.stdout.strip():
            raise UpcUnavailable(f"upc {' '.join(args)} produced no output: {proc.stderr.strip()}")
        try:
            return json.loads(proc.stdout)
        except json.JSONDecodeError as exc:  # pragma: no cover - defensive
            raise UpcUnavailable(
                f"upc {' '.join(args)} did not return JSON: {proc.stdout[:200]}"
            ) from exc

    # -- the handshake ----------------------------------------------------

    def _handshake(self) -> UpcInfo:
        info = self.cli_json(["version", "--json"])
        compat = self.cli_json(["check-compat", "--requires", REQUIRES])
        if compat.get("status") != "ok":
            raise UpcUnavailable(
                f"UPC at {self.home} is {info.get('spec_version')}, which does not satisfy "
                f"{REQUIRES}. {compat.get('reason', '')}".strip()
            )
        return UpcInfo(
            home=self.home,
            node=self.node,
            spec_version=info["spec_version"],
            schema_hash=info.get("schema_hash"),
        )

    # -- the batched library surface --------------------------------------

    def call_many(self, calls: Iterable[tuple[str, dict[str, Any]]]) -> list[Any]:
        """Run many library functions in one process, preserving order."""
        payload = "\n".join(json.dumps({"fn": fn, "arg": arg}) for fn, arg in calls)
        if not payload:
            return []
        proc = subprocess.run(
            [self.node, str(BRIDGE_SCRIPT)],
            input=payload,
            capture_output=True,
            text=True,
            env=self._env(),
        )
        if proc.returncode != 0:
            raise UpcUnavailable(f"upc bridge failed: {proc.stderr.strip()}")
        out: list[Any] = []
        for line in proc.stdout.splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            if not rec.get("ok"):
                raise UpcUnavailable(f"upc bridge call {rec.get('i')} failed: {rec.get('error')}")
            out.append(rec["result"])
        return out

    def call(self, fn: str, arg: dict[str, Any]) -> Any:
        return self.call_many([(fn, arg)])[0]

    # Convenience wrappers for the calls the converter makes in bulk.

    def canonical_urls(self, urls: Sequence[str]) -> list[str | None]:
        return self.call_many(("canonicalUrl", {"url": u}) for u in urls)

    def slugs(self, texts: Sequence[str], max_len: int = 72) -> list[str]:
        return self.call_many(("slugify", {"text": t, "maxLen": max_len}) for t in texts)

    def mint(self, kind: str, objs: Sequence[dict[str, Any]]) -> list[str]:
        fn = {
            "src": "mintSrcId",
            "rep": "mintRepId",
            "img": "mintImgId",
            "ext": "mintExtId",
            "gen": "mintGenId",
            "syn": "mintSynId",
            "cbk": "mintCbkId",
            "cod": "mintCodId",
        }[kind]
        return self.call_many((fn, o) for o in objs)
