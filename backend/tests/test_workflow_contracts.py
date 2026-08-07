"""Make documentation drift a failing test.

The skills shipped earlier contradicted the code in six places — a blocker that
no longer existed, column counters that were never in the response, an
error-code taxonomy that was invented rather than observed. A frontier model
works around that. A small local model follows it literally and does the wrong
thing, so these are the tests that keep the written word honest.
"""

from __future__ import annotations

import re
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SKILLS_DIR = ROOT / "data" / "agent_skills"
GENERATOR = ROOT / "scripts" / "generate_code_tables.py"
CODES_MD = SKILLS_DIR / "ra-reference" / "references" / "codes.md"

sys.path.insert(0, str(ROOT))


def all_known_codes() -> set[str]:
    """Every code the app can emit, extracted from the code itself."""
    import importlib.util

    spec = importlib.util.spec_from_file_location("gen_code_tables", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    data = module.collect()
    codes: set[str] = (
        set(data["engine"]) | set(data["verify"]) | set(data["phases"]) | set(data["sheet"])
    )
    for blockers, warnings in data["operations"].values():
        codes |= blockers | warnings
    return codes


class GeneratedTableTests(unittest.TestCase):
    def test_generated_tables_are_current(self) -> None:
        result = subprocess.run(
            [sys.executable, str(GENERATOR), "--check"],
            capture_output=True, text=True, cwd=str(ROOT), timeout=60,
        )
        self.assertEqual(
            result.returncode, 0,
            "codes.md is stale. Regenerate: python scripts/generate_code_tables.py\n"
            + result.stderr,
        )

    def test_every_documented_code_exists_in_the_code(self) -> None:
        known = all_known_codes()
        documented = set(re.findall(r"`([a-z][a-z0-9_]{4,})`", CODES_MD.read_text(encoding="utf-8")))
        # The file also backticks field names and phases in its prose.
        # Section headings name the operations, and the prose names fields.
        from backend.storage.repo_operations import OPERATIONS

        prose = set(OPERATIONS.names()) | {
            "phase_metadata", "error_code", "http_status", "network_failure",
            "scripts", "generate_code_tables", "codes", "triage",
        }
        stale = {c for c in documented if c not in known and c not in prose}
        self.assertEqual(stale, set(), f"codes.md documents codes that no longer exist: {stale}")

    def test_every_classified_code_still_exists(self) -> None:
        """`codes.py` must not carry entries for codes nothing emits."""
        from backend.workflow.codes import CODE_TABLE

        known = all_known_codes()
        # A few are emitted from attached_repository rather than the downloader.
        emitted_elsewhere = {"download_failure", "import_failure", "citation_verification_failed"}
        stale = set(CODE_TABLE) - known - emitted_elsewhere
        self.assertEqual(stale, set(), f"codes.py classifies codes nothing emits: {stale}")


class CheckpointTests(unittest.TestCase):
    """The two checkpoints must live in the response, not in a skill's prose.

    The skills said "never start a fetch or a column run without asking", and
    `next_actions` handed the model `ra fetch --wait` with nothing to mark it.
    A small model follows the field over remembered prose, so it chained past
    both checkpoints. These tests make that a build failure.
    """

    # Anything that downloads for a long time, spends model calls, or writes.
    DANGEROUS = re.compile(
        r"--confirm-overwrite\b|--apply\b|\bra (fetch|retry|convert|run-column|set-constraints)\b"
    )

    def _orientations(self):
        """One report per state the workflow can be in."""
        from backend.workflow.models import ColumnSummary, Orientation

        prompted = ColumnSummary(
            id="custom_a", label="Org Type", has_prompt=True, allowed_values=["Yes", "No"]
        )
        free_text = ColumnSummary(id="custom_b", label="Citation", has_prompt=True)
        yield "empty", Orientation(attached=True, total_sources=0)
        yield "queued", Orientation(
            attached=True, total_sources=42, sources_by_fetch_status={"queued": 42}
        )
        yield "failures", Orientation(
            attached=True,
            total_sources=42,
            sources_by_fetch_status={"failed": 4, "success": 38},
            failures_by_code={"network_failure": 4},
        )
        yield "no_columns", Orientation(
            attached=True, total_sources=42, sources_by_fetch_status={"success": 42}
        )
        yield "columns_never_run", Orientation(
            attached=True,
            total_sources=42,
            sources_by_fetch_status={"success": 42},
            columns=[free_text, prompted],
        )

    def test_no_dangerous_next_action_is_ever_ungated(self) -> None:
        from backend.workflow.orientation import _next_actions

        offenders: list[str] = []
        for name, report in self._orientations():
            for action in _next_actions(report):
                if self.DANGEROUS.search(action.command) and action.gate != "ask_user":
                    offenders.append(f"{name}: {action.command!r} is gate={action.gate!r}")
        self.assertEqual(
            offenders,
            [],
            "next actions that start work must be gate='ask_user':\n  " + "\n  ".join(offenders),
        )

    def test_a_gated_action_explains_itself(self) -> None:
        """`why` is what the model reads out to the user, so it cannot be blank."""
        from backend.workflow.orientation import _next_actions

        for name, report in self._orientations():
            for action in _next_actions(report):
                if action.gate == "ask_user":
                    self.assertTrue(action.why, f"{name}: {action.command!r} has no `why`")

    def test_the_first_column_offered_is_one_with_allowed_values(self) -> None:
        """Running a constrained column first is what catches a misread prompt."""
        from backend.workflow.orientation import _next_actions

        report = dict(self._orientations())["columns_never_run"]
        commands = [a.command for a in _next_actions(report)]
        self.assertIn("ra run-column custom_a --wait", commands)

    def test_a_column_that_ran_and_produced_nothing_is_not_re_offered(self) -> None:
        """Otherwise `filled_rows == 0` recommends it forever."""
        from backend.workflow.models import ColumnSummary, Orientation
        from backend.workflow.orientation import _next_actions

        report = Orientation(
            attached=True,
            total_sources=42,
            sources_by_fetch_status={"success": 42},
            columns=[
                ColumnSummary(
                    id="custom_a", label="Org Type", has_prompt=True, last_run_status="completed"
                )
            ],
        )
        self.assertEqual([a.command for a in _next_actions(report)], ["ra where"])

    def test_the_cli_agrees_with_the_server_about_what_is_gated(self) -> None:
        """`ra` gates bare strings itself; the two rules must not drift apart."""
        from backend.workflow.models import gate_for

        source = (ROOT / "data" / "agent_cli" / "ra").read_text(encoding="utf-8")
        pattern = re.search(r'^GATED = re\.compile\(\n\s+r"(.+)"\n\)', source, re.MULTILINE)
        self.assertIsNotNone(pattern, "ra no longer defines a GATED pattern")
        cli_gate = re.compile(pattern.group(1))

        for command in (
            "ra fetch --wait",
            "ra retry --wait",
            "ra convert --force",
            "ra run-column custom_a --wait",
            "ra create-sources --apply",
            "ra set-constraints --apply",
            "ra run-column custom_a --scope all --confirm-overwrite --wait",
        ):
            self.assertEqual(gate_for(command)[0], "ask_user", command)
            self.assertTrue(cli_gate.search(command), f"ra would not gate {command!r}")

        for command in ("ra where", "ra doctor", "ra triage", "ra columns", "ra watch abc123"):
            self.assertEqual(gate_for(command)[0], "go", command)
            self.assertIsNone(cli_gate.search(command), f"ra would wrongly gate {command!r}")

    def test_stale_values_are_offered_before_any_new_column(self) -> None:
        """A stale cell is filled, so nothing else would ever mention it again.

        The case: a blocked fetch stored an error page, the column ran against
        it, and the real document was attached afterwards.
        """
        from backend.workflow.models import ColumnSummary, Orientation
        from backend.workflow.orientation import _next_actions

        report = Orientation(
            attached=True,
            total_sources=42,
            sources_by_fetch_status={"success": 42},
            columns=[
                ColumnSummary(
                    id="custom_a",
                    label="Org Type",
                    has_prompt=True,
                    filled_rows=42,
                    last_run_status="completed",
                    stale_source_ids=["000024", "000029"],
                ),
                ColumnSummary(id="custom_b", label="Sector", has_prompt=True),
            ],
        )
        actions = _next_actions(report)
        first = actions[0]
        self.assertIn("--scope selected --ids 000024,000029", first.command)
        self.assertIn("--confirm-overwrite", first.command)
        self.assertEqual(first.gate, "ask_user")
        self.assertIn("rebuilt", first.why)

    def test_run_column_can_target_the_ids_the_remedy_names(self) -> None:
        """The remedy is worthless if `ra` cannot accept the ids it prints."""
        from test_workflow_cli import load_cli_module

        args = load_cli_module().build_parser().parse_args(
            ["run-column", "custom_a", "--scope", "selected", "--ids", "000024,000029",
             "--confirm-overwrite", "--wait"]
        )
        self.assertEqual(args.ids, "000024,000029")
        self.assertEqual(args.scope, "selected")

    def test_wait_follows_a_run_to_the_end(self) -> None:
        """`--wait` must not hand back a half-finished run.

        The server's budget is bounded because it holds an HTTP connection, so
        `--wait` returned mid-run with `next: ra watch <id>`. Anything that then
        wanted the result had to write a polling loop -- and a loop that skipped
        `next` started the next column into "already running".
        """
        source = (ROOT / "data" / "agent_cli" / "ra").read_text(encoding="utf-8")
        for command in ("def cmd_fetch", "def cmd_run_column"):
            body = source.split(command)[1].split("\ndef ")[0]
            self.assertIn(
                "follow(",
                body,
                f"{command} no longer follows a run to completion under --wait.",
            )

    def test_a_busy_rejection_says_what_to_watch(self) -> None:
        """"Already running" without an id leaves the only sensible retry failing."""
        source = (ROOT / "data" / "agent_cli" / "ra").read_text(encoding="utf-8")
        self.assertIn("def raise_if_busy", source)
        for command in ("def cmd_fetch", "def cmd_run_column"):
            body = source.split(command)[1].split("\ndef ")[0]
            self.assertIn("raise_if_busy(", body, f"{command} does not explain a busy repository.")

    def test_the_contract_version_was_bumped_together(self) -> None:
        from backend.workflow import WORKFLOW_CONTRACT_VERSION

        source = (ROOT / "data" / "agent_cli" / "ra").read_text(encoding="utf-8")
        declared = int(re.search(r"^CLI_CONTRACT_VERSION = (\d+)", source, re.MULTILINE).group(1))
        self.assertEqual(
            declared,
            WORKFLOW_CONTRACT_VERSION,
            "Bump CLI_CONTRACT_VERSION and WORKFLOW_CONTRACT_VERSION together.",
        )


class SkillDocumentTests(unittest.TestCase):
    """The skills a small model reads must not name codes that do not exist."""

    # Words that look like codes but are prose, filenames, fields or commands.
    ALLOWED = {
        "instruction_prompt", "fetch_status", "error_message", "phase_metadata",
        "error_code", "source_id", "source_ids", "new_id", "column_id", "job_id",
        "run_id", "state_fingerprint", "idempotency_key", "confirm_overwrite",
        "empty_only", "raw_file", "rendered_pdf_file", "markdown_file",
        "summary_file", "llm_cleanup_file", "catalog_file", "rating_file",
        "ocr_pdf_file", "video_file", "audio_file", "thumbnail_file",
        "metadata_file", "rendered_file", "create_sources", "create_columns",
        "attach_files", "remap_source_ids", "repository_state", "manifest",
        "agent_tokens", "bundled_skills", "next_actions", "total_rows",
        "processed_rows", "succeeded_rows", "failed_rows", "row_errors",
        "row_errors_truncated", "confirmation_required", "ok_to_fetch",
        "ok_to_run_columns", "include_source_text", "include_row_context",
        "output_constraint", "allowed_values", "skip_existing", "scan_inbox",
        "verify_issues", "terminal", "counts", "problems", "summary", "params",
        "apply", "plan", "blockers", "warnings", "changes", "result", "status",
        "workflow", "sources", "columns", "prompts", "header", "python",
        # Operation and run statuses, not codes.
        "rolled_back", "state_changed", "no_prompts_row_found",
        # `next` gate values, not codes.
        "ask_user", "set_column_constraints", "overwrite_existing",
    }

    def test_skills_name_no_stale_codes(self) -> None:
        known = all_known_codes()
        problems: list[str] = []
        for path in sorted(SKILLS_DIR.rglob("*.md")):
            if path == CODES_MD:
                continue  # generated, checked above
            for token in set(re.findall(r"`([a-z][a-z0-9_]*_[a-z0-9_]+)`", path.read_text(encoding="utf-8"))):
                if token in known or token in self.ALLOWED:
                    continue
                problems.append(f"{path.relative_to(ROOT)}: `{token}`")
        self.assertEqual(
            problems, [],
            "Skill documents name codes or fields that do not exist:\n  " + "\n  ".join(problems),
        )

    def test_every_skill_satisfies_the_spec(self) -> None:
        for skill_dir in sorted(SKILLS_DIR.iterdir()):
            if not skill_dir.is_dir():
                continue
            skill = skill_dir / "SKILL.md"
            self.assertTrue(skill.is_file(), f"{skill_dir.name} has no SKILL.md")
            text = skill.read_text(encoding="utf-8")
            self.assertTrue(text.startswith("---\n"), skill_dir.name)

            front = text[4:].split("\n---", 1)[0]
            fields = dict(
                (line.split(":", 1)[0].strip(), line.split(":", 1)[1].strip())
                for line in front.splitlines()
                if ":" in line and not line.startswith(" ")
            )
            self.assertEqual(fields.get("name"), skill_dir.name)
            self.assertRegex(fields["name"], r"^[a-z0-9]+(-[a-z0-9]+)*$")
            self.assertLessEqual(len(fields["name"]), 64)
            self.assertTrue(fields.get("description"))
            self.assertLessEqual(len(fields["description"]), 1024)

    def test_skills_do_not_tell_the_model_to_run_next_unconditionally(self) -> None:
        """The instruction that overrode every checkpoint the same file set."""
        offenders = [
            str(path.relative_to(ROOT))
            for path in sorted(SKILLS_DIR.rglob("*.md"))
            if re.search(
                r"`next`[^.]{0,40}\band run what it says\b",
                path.read_text(encoding="utf-8"),
                re.IGNORECASE,
            )
        ]
        self.assertEqual(
            offenders,
            [],
            "A skill tells the model to run `next` without checking its gate: "
            + ", ".join(offenders),
        )

    def test_skills_do_not_ask_the_model_to_build_requests(self) -> None:
        """A small model should run commands, not compose HTTP calls."""
        offenders: list[str] = []
        for path in sorted(SKILLS_DIR.rglob("*.md")):
            if path == CODES_MD:
                continue
            text = path.read_text(encoding="utf-8")
            for pattern, why in (
                (r"\bcurl\b", "hand-built curl"),
                (r"<a fresh uuid>|<uuid>", "asks the model to invent a uuid"),
                (r"\.\.\.same as the plan\.\.\.", "elided payload"),
            ):
                if re.search(pattern, text):
                    offenders.append(f"{path.relative_to(ROOT)}: {why}")
        self.assertEqual(offenders, [], "\n  ".join(["Skills regressed to hand-built requests:"] + offenders))


if __name__ == "__main__":
    unittest.main()
