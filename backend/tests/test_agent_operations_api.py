"""Tests for the operations REST routes and the MCP tool surface."""

from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.models.sources import SourceManifestRow
from backend.routers import agent as agent_router
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore
from backend.storage.repo_operations import OPERATIONS


class _AgentApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="agent-ops-api-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.service = AttachedRepositoryService(store=FileStore(base_dir=self.tmp_path / "app"))
        self.repo = self.tmp_path / "repo"
        self.repo.mkdir()
        self.service.attach(str(self.repo))
        self.seed()

        app = FastAPI()
        app.state.file_store = self.service.store
        app.state.repository_service = self.service
        app.state.source_download_jobs = {}
        app.state.source_download_lock = threading.Lock()
        app.include_router(agent_router.router, prefix="/api")
        self.client = TestClient(app)

        tokens = self.service.load_agent_tokens()
        self.read_headers = {"Authorization": f"Bearer {tokens['read_token']}"}
        self.write_headers = {"Authorization": f"Bearer {tokens['write_token']}"}

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def seed(self) -> None:
        rows = []
        for source_id, url in (("000001", "https://a.test/one"), ("000002", "https://a.test/two")):
            directory = self.repo / "sources" / source_id
            directory.mkdir(parents=True, exist_ok=True)
            (directory / f"{source_id}_source.html").write_text("<html/>", encoding="utf-8")
            rows.append(
                SourceManifestRow(
                    id=source_id,
                    repository_source_id=source_id,
                    source_kind="url",
                    import_type="seed",
                    original_url=url,
                    fetch_status="success",
                    raw_file=f"sources/{source_id}/{source_id}_source.html",
                )
            )
        with self.service._writer_lock():
            for row in rows:
                self.service._write_repository_source_metadata(row)
            self.service._save_state_locked(sources=rows, citations=[], imports=[])
            self.service._save_meta_locked(
                {**self.service._load_meta_locked(), "next_source_id": 3}
            )
            self.service._rebuild_outputs_locked(rows, [])

    # -- helpers ---------------------------------------------------------

    def plan(self, params: dict, operation: str = "remap_source_ids"):
        return self.client.post(
            f"/api/agent/v1/operations/{operation}/plan",
            headers=self.read_headers,
            json={"params": params},
        )

    def apply(self, body: dict, operation: str = "remap_source_ids", headers=None):
        return self.client.post(
            f"/api/agent/v1/operations/{operation}/apply",
            headers=headers or self.write_headers,
            json=body,
        )

    def rpc(self, method: str, params: dict | None = None, headers=None):
        return self.client.post(
            "/api/agent/v1/mcp",
            headers=headers or self.write_headers,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}},
        ).json()

    def call_tool(self, name: str, arguments: dict, headers=None):
        return self.rpc("tools/call", {"name": name, "arguments": arguments}, headers=headers)


class OperationsRestTests(_AgentApiTestCase):
    def test_lists_operations_with_schemas(self) -> None:
        response = self.client.get("/api/agent/v1/operations", headers=self.read_headers)
        self.assertEqual(response.status_code, 200)
        items = response.json()["data"]["items"]
        self.assertEqual({item["name"] for item in items}, set(OPERATIONS.names()))
        for item in items:
            self.assertEqual(item["input_schema"]["type"], "object")

    def test_plan_accepts_the_read_token(self) -> None:
        response = self.plan({"pairs": [{"url": "https://a.test/one", "new_id": "9"}]})
        self.assertEqual(response.status_code, 200)
        data = response.json()["data"]
        self.assertTrue(data["changes"])
        self.assertEqual(data["blockers"], [])
        self.assertTrue(data["state_fingerprint"])

    def test_apply_rejects_the_read_token(self) -> None:
        response = self.apply(
            {"params": {"pairs": [{"url": "https://a.test/one", "new_id": "9"}]}},
            headers=self.read_headers,
        )
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["error"]["code"], "agent_auth_invalid")

    def test_apply_succeeds_with_the_write_token(self) -> None:
        params = {"pairs": [{"url": "https://a.test/one", "new_id": "9"}]}
        fingerprint = self.plan(params).json()["data"]["state_fingerprint"]

        response = self.apply({"params": params, "state_fingerprint": fingerprint})

        self.assertEqual(response.status_code, 200)
        data = response.json()["data"]
        self.assertEqual(data["status"], "applied")
        self.assertTrue(data["verify_passed"])

    def test_blocked_apply_returns_409_with_the_full_plan(self) -> None:
        response = self.apply(
            {"params": {"pairs": [{"url": "https://nope.invalid/", "new_id": "9"}]}}
        )
        self.assertEqual(response.status_code, 409)
        data = response.json()["data"]
        self.assertEqual(data["status"], "blocked")
        self.assertEqual([b["code"] for b in data["plan"]["blockers"]], ["url_not_found"])

    def test_stale_fingerprint_is_refused(self) -> None:
        response = self.apply(
            {
                "params": {"pairs": [{"url": "https://a.test/one", "new_id": "9"}]},
                "state_fingerprint": "stale",
            }
        )
        self.assertEqual(response.status_code, 409)
        data = response.json()["data"]
        self.assertEqual([b["code"] for b in data["plan"]["blockers"]], ["state_changed"])
        # Nothing moved.
        self.assertTrue((self.repo / "sources" / "000001").is_dir())

    def test_idempotency_key_replays_the_original_result(self) -> None:
        params = {"pairs": [{"url": "https://a.test/one", "new_id": "9"}]}
        first = self.apply({"params": params, "idempotency_key": "abc"}).json()["data"]
        second = self.apply({"params": params, "idempotency_key": "abc"}).json()["data"]

        self.assertEqual(first["status"], "applied")
        self.assertEqual(first["run_id"], second["run_id"])
        # The replay must not renumber a second time.
        self.assertTrue((self.repo / "sources" / "000009").is_dir())

    def test_reused_key_with_different_params_conflicts(self) -> None:
        self.apply(
            {
                "params": {"pairs": [{"url": "https://a.test/one", "new_id": "9"}]},
                "idempotency_key": "abc",
            }
        )
        response = self.apply(
            {
                "params": {"pairs": [{"url": "https://a.test/two", "new_id": "12"}]},
                "idempotency_key": "abc",
            }
        )
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "idempotency_conflict")

    def test_run_result_can_be_fetched_later(self) -> None:
        run_id = self.apply(
            {"params": {"pairs": [{"url": "https://a.test/one", "new_id": "9"}]}}
        ).json()["data"]["run_id"]

        response = self.client.get(
            f"/api/agent/v1/operations/runs/{run_id}", headers=self.read_headers
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["status"], "applied")

        missing = self.client.get(
            "/api/agent/v1/operations/runs/nope", headers=self.read_headers
        )
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.json()["error"]["code"], "unknown_run")

    def test_unknown_operation_is_404(self) -> None:
        response = self.plan({}, operation="does_not_exist")
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"]["code"], "unknown_operation")

    def test_apply_is_audited(self) -> None:
        self.apply({"params": {"pairs": [{"url": "https://a.test/one", "new_id": "9"}]}})
        lines = (self.repo / ".ra_repo" / "agent_audit.jsonl").read_text().strip().splitlines()
        actions = [json.loads(line)["action"] for line in lines]
        self.assertIn("operation:remap_source_ids", actions)

    def test_operations_require_authentication(self) -> None:
        self.assertEqual(self.client.get("/api/agent/v1/operations").status_code, 401)


class OperationsMcpTests(_AgentApiTestCase):
    def test_tool_surface_covers_the_registry(self) -> None:
        names = {tool["name"] for tool in self.rpc("tools/list")["result"]["tools"]}
        self.assertTrue(
            {"list_operations", "plan_operation", "apply_operation"} <= names, names
        )
        self.assertTrue(
            {
                "list_columns",
                "create_column",
                "update_column_prompt",
                "run_column",
                "get_column_run_status",
            }
            <= names,
            names,
        )

        # Adding an operation without updating the enum should fail loudly here.
        definitions = {tool["name"]: tool for tool in agent_router._mcp_tool_definitions()}
        for tool_name in ("plan_operation", "apply_operation"):
            enum = definitions[tool_name]["inputSchema"]["properties"]["operation"]["enum"]
            self.assertEqual(set(enum), set(OPERATIONS.names()), tool_name)

    def test_mcp_plan_matches_the_rest_plan(self) -> None:
        params = {"pairs": [{"url": "https://a.test/one", "new_id": "9"}]}
        mcp = self.call_tool(
            "plan_operation", {"operation": "remap_source_ids", "params": params}
        )["result"]["structuredContent"]
        rest = self.plan(params).json()["data"]

        volatile = {"plan_id", "created_at"}
        self.assertEqual(
            {k: v for k, v in mcp.items() if k not in volatile},
            {k: v for k, v in rest.items() if k not in volatile},
        )

    def test_write_tools_reject_the_read_token(self) -> None:
        for name in sorted(agent_router.MCP_WRITE_TOOLS):
            response = self.call_tool(name, {}, headers=self.read_headers)
            self.assertIn("error", response, name)
            self.assertEqual(response["error"]["code"], -32001, name)

    def test_read_tools_accept_the_read_token(self) -> None:
        response = self.call_tool("list_operations", {}, headers=self.read_headers)
        self.assertNotIn("error", response)
        self.assertEqual(
            {item["name"] for item in response["result"]["structuredContent"]["items"]},
            set(OPERATIONS.names()),
        )

    def test_blocked_apply_reaches_the_model_as_data(self) -> None:
        """Blockers are the useful answer, not a failed tool call."""
        response = self.call_tool(
            "apply_operation",
            {
                "operation": "remap_source_ids",
                "params": {"pairs": [{"url": "https://nope.invalid/", "new_id": "9"}]},
            },
        )
        self.assertNotIn("error", response)
        data = response["result"]["structuredContent"]
        self.assertEqual(data["status"], "blocked")
        self.assertEqual([b["code"] for b in data["plan"]["blockers"]], ["url_not_found"])

    def test_unknown_operation_through_mcp_is_an_error(self) -> None:
        response = self.call_tool(
            "apply_operation", {"operation": "does_not_exist", "params": {}}
        )
        self.assertIn("error", response)

    def test_apply_through_mcp_changes_the_repository(self) -> None:
        response = self.call_tool(
            "apply_operation",
            {
                "operation": "remap_source_ids",
                "params": {"pairs": [{"url": "https://a.test/one", "new_id": "9"}]},
            },
        )
        self.assertEqual(response["result"]["structuredContent"]["status"], "applied")
        self.assertTrue((self.repo / "sources" / "000009").is_dir())


class ColumnMcpTests(_AgentApiTestCase):
    def test_list_columns_reports_prompts_and_runnability(self) -> None:
        columns = self.call_tool("list_columns", {})["result"]["structuredContent"]["items"]
        self.assertTrue(columns)
        by_key = {column["key"]: column for column in columns}
        self.assertIn("title", by_key)
        self.assertIn("processable", by_key["title"])
        self.assertIn("instruction_prompt", by_key["title"])

    def test_create_then_prompt_a_custom_column(self) -> None:
        created = self.call_tool("create_column", {"label": "Method"})["result"][
            "structuredContent"
        ]
        self.assertEqual(created["label"], "Method")
        self.assertEqual(created["kind"], "custom")

        updated = self.call_tool(
            "update_column_prompt",
            {
                "column_id": created["id"],
                "instruction_prompt": "What method did the study use?",
                "include_source_text": True,
            },
        )["result"]["structuredContent"]
        self.assertEqual(updated["instruction_prompt"], "What method did the study use?")
        self.assertTrue(updated["include_source_text"])

        keys = {
            column["key"]
            for column in self.call_tool("list_columns", {})["result"]["structuredContent"][
                "items"
            ]
        }
        self.assertIn(created["id"], keys)

    def test_run_column_is_blocked_while_a_job_is_running(self) -> None:
        created = self.call_tool("create_column", {"label": "Method"})["result"][
            "structuredContent"
        ]

        class _AliveThread:
            def is_alive(self) -> bool:
                return True

        self.service._download_thread = _AliveThread()
        try:
            response = self.call_tool(
                "run_column", {"column_id": created["id"], "scope": "all"}
            )
        finally:
            self.service._download_thread = None

        self.assertIn("error", response)
        # Transient, so it gets the retryable code rather than the generic one.
        self.assertEqual(response["error"]["code"], -32003)

    def test_unknown_column_is_an_error(self) -> None:
        response = self.call_tool(
            "update_column_prompt", {"column_id": "nope", "instruction_prompt": "x"}
        )
        self.assertIn("error", response)


if __name__ == "__main__":
    unittest.main()
