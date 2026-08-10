"""Tests for the bundled `ra` CLI and the workflow API it talks to.

The most valuable test here is `test_cli_response_field_assumptions`: the CLI
declares which model fields it reads, and the test resolves each against the
real pydantic model. That is the check that would have caught a skill telling
agents to report column counters the response never had.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import agent as agent_router
from backend.routers import workflow as workflow_router
from backend.storage.agent_skills import bundled_agent_cli_dir
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore
from backend.workflow import WORKFLOW_CONTRACT_VERSION

RA_PATH = bundled_agent_cli_dir() / "ra"


def load_cli_module():
    """Import `ra` even though it has no .py extension."""
    import importlib.util

    spec = importlib.util.spec_from_loader(
        "ra_cli", loader=None, origin=str(RA_PATH)
    )
    module = importlib.util.module_from_spec(spec)
    module.__file__ = str(RA_PATH)
    exec(compile(RA_PATH.read_text(encoding="utf-8"), str(RA_PATH), "exec"), module.__dict__)
    return module


class CliContractTests(unittest.TestCase):
    def setUp(self) -> None:
        self.cli = load_cli_module()

    def test_contract_version_matches_the_server(self) -> None:
        """A stale CLI or a stale server must announce itself, not misbehave."""
        self.assertEqual(self.cli.CLI_CONTRACT_VERSION, WORKFLOW_CONTRACT_VERSION)

    def test_cli_response_field_assumptions(self) -> None:
        """Every field the CLI reads must still exist on the real model."""
        from backend.workflow import models as workflow_models

        for model_name, fields in self.cli.RESPONSE_FIELDS.items():
            model = getattr(workflow_models, model_name, None)
            self.assertIsNotNone(model, f"{model_name} is not a workflow model")
            available = set(model.model_fields)
            for field in fields:
                self.assertIn(
                    field,
                    available,
                    f"{model_name}.{field} is read by ra but no longer exists",
                )

    def test_every_subcommand_is_registered(self) -> None:
        parser = self.cli.build_parser()
        actions = [a for a in parser._actions if hasattr(a, "choices") and a.choices]
        names = set(actions[0].choices) if actions else set()
        expected = {
            "version", "doctor", "where", "plan-sheet", "create-sources",
            "create-columns", "set-constraints", "set-values", "columns", "fetch",
            "convert", "triage", "retry", "attach", "run-column", "remap", "watch",
        }
        self.assertEqual(names, expected)

    def test_global_flag_works_before_and_after_the_subcommand(self) -> None:
        """`ra doctor --json-only` failing on argument order is a trap."""
        parser = self.cli.build_parser()
        self.assertTrue(parser.parse_args(["--json-only", "where"]).json_only)
        self.assertTrue(parser.parse_args(["where", "--json-only"]).json_only)

    def test_exit_codes_are_distinct(self) -> None:
        codes = {
            self.cli.EXIT_OK,
            self.cli.EXIT_PROBLEM,
            self.cli.EXIT_USAGE,
            self.cli.EXIT_CONTRACT,
            self.cli.EXIT_UNREACHABLE,
        }
        self.assertEqual(len(codes), 5)
        self.assertEqual(self.cli.EXIT_OK, 0)

    def test_repo_root_is_found_by_walking_up(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "repo"
            (root / ".ra_repo").mkdir(parents=True)
            nested = root / "a" / "b"
            nested.mkdir(parents=True)
            self.assertEqual(self.cli.find_repo_root(nested), root.resolve())
            self.assertIsNone(self.cli.find_repo_root(Path(tmp)))

    def test_token_is_never_echoed(self) -> None:
        source = RA_PATH.read_text(encoding="utf-8")
        self.assertNotIn("print(token", source)
        self.assertIn("The token itself is never printed", source)

    def test_runs_as_a_script_without_the_app_installed(self) -> None:
        """Stdlib only, so it works wherever the repository does."""
        result = subprocess.run(
            [sys.executable, str(RA_PATH), "--help"],
            capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("ResearchAssistant", result.stdout)


class WorkflowApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="workflow-api-tests-")
        self.tmp = Path(self._tmp.name)
        self.service = AttachedRepositoryService(store=FileStore(base_dir=self.tmp / "app"))
        self.repo = self.tmp / "repo"
        self.repo.mkdir()
        self.service.attach(str(self.repo))

        app = FastAPI()
        app.state.file_store = self.service.store
        app.state.repository_service = self.service
        app.state.source_download_jobs = {}
        app.state.source_download_lock = threading.Lock()
        app.include_router(agent_router.router, prefix="/api")
        app.include_router(workflow_router.router, prefix="/api")
        self.client = TestClient(app)

        tokens = self.service.load_agent_tokens()
        self.read = {"Authorization": f"Bearer {tokens['read_token']}"}
        self.write = {"Authorization": f"Bearer {tokens['write_token']}"}

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_version_needs_no_token(self) -> None:
        response = self.client.get("/api/workflow/v1/version")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["data"]["contract_version"], WORKFLOW_CONTRACT_VERSION
        )

    def test_orientation_reports_an_empty_repository(self) -> None:
        response = self.client.get("/api/workflow/v1/orientation", headers=self.read)
        self.assertEqual(response.status_code, 200)
        data = response.json()["data"]
        self.assertEqual(data["total_sources"], 0)
        self.assertTrue(data["next_actions"])
        self.assertIn("plan-sheet", data["next_actions"][0]["command"])

    def test_triage_on_a_clean_repository(self) -> None:
        response = self.client.get("/api/workflow/v1/triage", headers=self.read)
        data = response.json()["data"]
        self.assertEqual(data["total_failed"], 0)
        self.assertTrue(data["next"])

    def test_sheet_parse_end_to_end(self) -> None:
        sheet = self.tmp / "plan.csv"
        sheet.write_text(
            "\n".join(
                [
                    ",,,,Group heading,",
                    "ID#,URL,Notes,Citation",
                    "Prompts:,,,"
                    + "Write one citation for this source and return only that string, "
                    "using exactly the punctuation shown and nothing else at all.",
                    "20,https://example.com/a,,",
                    "21,https://example.com/b,,",
                ]
            ),
            encoding="utf-8-sig",
        )
        response = self.client.post(
            "/api/workflow/v1/sheet/parse", headers=self.read, json={"path": str(sheet)}
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()["data"]
        self.assertEqual(len(data["sources"]), 2)
        self.assertEqual(data["sources"][0]["id"], "20")
        self.assertEqual(len(data["create_sources_params"]["sources"]), 2)

    def test_unreadable_sheet_is_a_clean_error(self) -> None:
        response = self.client.post(
            "/api/workflow/v1/sheet/parse", headers=self.read,
            json={"path": str(self.tmp / "nope.csv")},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["error"]["code"], "sheet_unreadable")

    def test_operation_plans_without_applying_then_applies(self) -> None:
        params = {"sources": [{"url": "https://example.com/a", "id": "20"}]}

        planned = self.client.post(
            "/api/workflow/v1/operations/create_sources",
            headers=self.read, json={"params": params},
        )
        self.assertEqual(planned.status_code, 200)
        self.assertFalse(planned.json()["data"]["applied"])
        self.assertEqual(self.service.get_status().total_sources, 0)

        applied = self.client.post(
            "/api/workflow/v1/operations/create_sources",
            headers=self.write, json={"params": params, "apply": True},
        )
        self.assertEqual(applied.status_code, 200)
        self.assertTrue(applied.json()["data"]["applied"])
        self.assertEqual(self.service.get_status().total_sources, 1)

    def test_applying_needs_the_write_token(self) -> None:
        response = self.client.post(
            "/api/workflow/v1/operations/create_sources",
            headers=self.read,
            json={"params": {"sources": [{"url": "https://example.com/a"}]}, "apply": True},
        )
        self.assertEqual(response.status_code, 403)

    def test_blocked_operation_returns_409_with_the_blockers(self) -> None:
        response = self.client.post(
            "/api/workflow/v1/operations/create_sources",
            headers=self.write,
            json={"params": {"sources": [{"url": "not a url", "id": "1"}]}, "apply": True},
        )
        self.assertEqual(response.status_code, 409)
        blockers = response.json()["data"]["plan"]["blockers"]
        self.assertEqual([b["code"] for b in blockers], ["url_invalid"])

    def test_unknown_operation_is_404(self) -> None:
        response = self.client.post(
            "/api/workflow/v1/operations/nope", headers=self.read, json={"params": {}}
        )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"]["code"], "unknown_operation")

    def test_orientation_points_at_the_next_step_as_state_changes(self) -> None:
        """The substitute for stored progress: derived, never stale."""
        self.client.post(
            "/api/workflow/v1/operations/create_sources",
            headers=self.write,
            json={
                "params": {"sources": [{"url": "https://example.com/a", "id": "20"}]},
                "apply": True,
            },
        )
        data = self.client.get("/api/workflow/v1/orientation", headers=self.read).json()["data"]
        self.assertEqual(data["total_sources"], 1)
        # A queued source means the next thing to do is fetch it -- but only
        # once the user has said so, so the entry arrives gated.
        fetch = [a for a in data["next_actions"] if "fetch" in a["command"]]
        self.assertTrue(fetch)
        self.assertEqual(fetch[0]["gate"], "ask_user")
        self.assertTrue(fetch[0]["why"])


if __name__ == "__main__":
    unittest.main()
