#!/usr/bin/env python3
"""Generate the error-code reference from the code that emits it.

Written because the hand-maintained version drifted badly: skills documented a
blocker that no longer existed, told agents to read column counters that were
never in the response, and listed `http_status_5xx` as an error code when the
real code is `network_failure` and the HTTP number is the detail after the
colon. A model that follows those literally does the wrong thing.

The extraction is AST-based, not regex. A regex over `verify.py` happily
collects `"attach"`, `"sources"` and `"citations"`, which are subjects, not
codes.

    python scripts/generate_code_tables.py           # write the file
    python scripts/generate_code_tables.py --check   # fail if it is stale
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OPERATIONS_DIR = ROOT / "backend" / "storage" / "repo_operations"
DOWNLOADER = ROOT / "backend" / "pipeline" / "source_downloader.py"
OUTPUT = ROOT / "data" / "agent_skills" / "ra-reference" / "references" / "codes.md"

# Helpers each operation module defines locally; their first positional argument
# is the code. Collecting these is what a naive `PlanIssue(code=...)` scan misses.
LOCAL_EMITTERS = {"block", "warn", "add"}


def _string(node: ast.AST) -> str | None:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def codes_from_module(path: Path) -> tuple[set[str], set[str]]:
    """Return `(blockers, warnings)` emitted by one operation module."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    blockers: set[str] = set()
    warnings: set[str] = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        name = ""
        if isinstance(node.func, ast.Name):
            name = node.func.id
        elif isinstance(node.func, ast.Attribute):
            name = node.func.attr

        if name in {"PlanIssue", "VerifyIssue"}:
            for keyword in node.keywords:
                if keyword.arg == "code" and (value := _string(keyword.value)):
                    # Which bucket depends on the surrounding call; the caller
                    # sorts that out below via the local emitters.
                    blockers.add(value)
        elif name in LOCAL_EMITTERS and node.args and (value := _string(node.args[0])):
            (warnings if name == "warn" else blockers).add(value)

    # A module can declare codes it raises through a variable, which no source
    # scan can see. Without this the tables would be quietly incomplete, which
    # is the exact failure the generator exists to prevent.
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if not isinstance(target, ast.Name):
                continue
            bucket = (
                blockers
                if target.id == "EXTRA_BLOCKER_CODES"
                else warnings
                if target.id == "EXTRA_WARNING_CODES"
                else None
            )
            if bucket is None or not isinstance(node.value, (ast.Tuple, ast.List)):
                continue
            for element in node.value.elts:
                if value := _string(element):
                    bucket.add(value)

    return blockers, warnings


def _module_constants(tree: ast.Module) -> dict[str, str]:
    """Module-level `NAME = "value"` bindings, so f-strings can be resolved."""
    constants: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        value = _string(node.value)
        if value is None:
            continue
        for target in node.targets:
            if isinstance(target, ast.Name):
                constants[target.id] = value
    return constants


def _leading_code(node: ast.AST, constants: dict[str, str]) -> str | None:
    """The code at the front of an error message, literal or interpolated.

    `row.error_message = f"{NOTE_BLOCKED_REQUEST}: {detail}"` is the common
    shape, and a naive scan misses it entirely -- which would leave
    `blocked_request`, the code that tells a user to fetch a page by hand,
    undocumented.
    """
    text = _string(node)
    if text:
        return text.split(":", 1)[0].strip() if ":" in text else None

    if not isinstance(node, ast.JoinedStr) or not node.values:
        return None
    first = node.values[0]
    if isinstance(first, ast.Constant) and isinstance(first.value, str):
        return first.value.split(":", 1)[0].strip() if ":" in first.value else None
    if isinstance(first, ast.FormattedValue) and isinstance(first.value, ast.Name):
        resolved = constants.get(first.value.id)
        # The interpolation is the whole code; the colon follows it.
        if resolved and len(node.values) > 1:
            nxt = _string(node.values[1])
            if nxt and nxt.lstrip().startswith(":"):
                return resolved
    return None


def phase_error_codes() -> set[str]:
    """Codes the download pipeline records against a phase."""
    tree = ast.parse(DOWNLOADER.read_text(encoding="utf-8"))
    constants = _module_constants(tree)
    codes: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            for keyword in node.keywords:
                if keyword.arg == "error_code" and (value := _string(keyword.value)):
                    codes.add(value)
        if isinstance(node, ast.Return):
            # `blocked_error_message` and friends return the joined string.
            head = _leading_code(node.value, constants) if node.value else None
            if head and head.replace("_", "").isalnum() and head.islower():
                codes.add(head)
        if isinstance(node, ast.Assign):
            target = node.targets[0]
            if not (isinstance(target, ast.Attribute) and target.attr == "error_message"):
                continue
            head = _leading_code(node.value, constants)
            if head and head.replace("_", "").isalnum() and head.islower():
                codes.add(head)
    return codes


def sheet_anomaly_codes() -> set[str]:
    """Codes the spreadsheet parser reports, so the skills may name them."""
    path = ROOT / "backend" / "workflow" / "sheet.py"
    tree = ast.parse(path.read_text(encoding="utf-8"))
    codes: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "SheetAnomaly":
            for keyword in node.keywords:
                if keyword.arg == "code" and (value := _string(keyword.value)):
                    codes.add(value)
    return codes


def collect() -> dict:
    from backend.storage.repo_operations.verify import verify_repository_locked  # noqa: F401

    per_operation: dict[str, tuple[set[str], set[str]]] = {}
    for path in sorted(OPERATIONS_DIR.glob("*.py")):
        if path.stem in {"__init__", "base", "context", "journal", "verify"}:
            continue
        per_operation[path.stem] = codes_from_module(path)

    engine_blockers, _ = codes_from_module(OPERATIONS_DIR / "__init__.py")
    verify_codes, _ = codes_from_module(OPERATIONS_DIR / "verify.py")

    return {
        "operations": per_operation,
        "engine": engine_blockers,
        "verify": verify_codes,
        "phases": phase_error_codes(),
        "sheet": sheet_anomaly_codes(),
    }


def render(data: dict) -> str:
    from backend.workflow.codes import CODE_TABLE

    lines = [
        "# Error codes",
        "",
        "**Generated from the code by `scripts/generate_code_tables.py`.**",
        "Do not edit by hand — regenerate instead, or the next change will silently",
        "contradict it.",
        "",
        "Every code below is emitted somewhere in the app. If you meet one that is",
        "not here, report it to the user rather than guessing what it means.",
        "",
        "## Fetch and convert failures",
        "",
        "Read these from `phase_metadata.<phase>.error_code`; the code is already",
        "split out for you. `ra triage` groups by them and gives you the remedy.",
        "",
        "| code | what it means | worth retrying? |",
        "|---|---|---|",
    ]

    for code in sorted(data["phases"]):
        meaning = CODE_TABLE.get(code)
        explanation = meaning.explanation if meaning else "Not classified."
        classification = meaning.classification if meaning else "unknown"
        retry = {
            "retryable": "yes",
            "retryable_convert": "yes, with convert",
            "needs_manual_document": "no — get the document by hand",
            "broken_url": "no — the URL is wrong",
            "environment": "no — fix the environment",
            "ignore": "not a problem",
        }.get(classification, "unknown")
        lines.append(f"| `{code}` | {explanation} | {retry} |")

    lines += [
        "",
        "`network_failure` covers every HTTP error that is not a 401/403/407/429,",
        "so a 404 and a 503 arrive under the same code. The status is in the detail",
        "as `http_status_<n>`, and `ra triage` splits them for you.",
        "",
        "## Operation blockers and warnings",
        "",
        "A blocker means nothing was changed. Fix it and run the command again.",
        "",
    ]

    for name, (blockers, warnings) in sorted(data["operations"].items()):
        lines.append(f"### `{name}`")
        lines.append("")
        if blockers:
            lines.append("Blockers: " + ", ".join(f"`{c}`" for c in sorted(blockers)))
            lines.append("")
        if warnings:
            lines.append("Warnings: " + ", ".join(f"`{c}`" for c in sorted(warnings)))
            lines.append("")

    lines += [
        "## Spreadsheet notes",
        "",
        "Reported by `ra plan-sheet`. None of these stop you on their own; they",
        "tell the user what the sheet looks like and what was skipped.",
        "",
        ", ".join(f"`{c}`" for c in sorted(data["sheet"])),
        "",
        "### Any operation",
        "",
        "Blockers: " + ", ".join(f"`{c}`" for c in sorted(data["engine"])),
        "",
        "## Integrity checks",
        "",
        "These run after every change. If one appears, the change was undone and",
        "the repository is exactly as it was.",
        "",
        ", ".join(f"`{c}`" for c in sorted(data["verify"])),
        "",
    ]
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail if the file is stale.")
    args = parser.parse_args()

    sys.path.insert(0, str(ROOT))
    content = render(collect())

    if args.check:
        current = OUTPUT.read_text(encoding="utf-8") if OUTPUT.is_file() else ""
        if current != content:
            print(
                "data/agent_skills/ra-reference/references/codes.md is out of date.\n"
                "Regenerate it with: python scripts/generate_code_tables.py",
                file=sys.stderr,
            )
            return 1
        print("codes.md is current.")
        return 0

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(content, encoding="utf-8")
    print(f"Wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
