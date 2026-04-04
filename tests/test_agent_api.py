from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import agent
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


class AgentApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="agent-api-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.service = AttachedRepositoryService(store=self.store)

        self.repo_dir = self.tmp_path / "repo"
        self.repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(self.repo_dir))

        first_import = self.service.import_source_list(
            filename="sources-1.csv",
            content=(
                "URL,Title\n"
                "https://example.com/a,Alpha Source\n"
                "https://example.com/b,Beta Source\n"
            ).encode("utf-8"),
        )
        second_import = self.service.import_source_list(
            filename="sources-2.csv",
            content=("URL,Title\nhttps://example.com/c,Gamma Source\n").encode("utf-8"),
        )
        self.first_import_id = first_import.import_id
        self.second_import_id = second_import.import_id

        self._write_repo_resource_files()
        self._seed_repository_sources()

        app = FastAPI()
        app.state.file_store = self.store
        app.state.repository_service = self.service
        app.state.source_download_jobs = {}
        app.state.source_download_lock = threading.Lock()
        app.include_router(agent.router, prefix="/api")
        self.app = app
        self.client = TestClient(app)

        self.tokens = self.service.load_agent_tokens()
        self.read_headers = {"Authorization": f"Bearer {self.tokens['read_token']}"}
        self.write_headers = {"Authorization": f"Bearer {self.tokens['write_token']}"}

    def tearDown(self):
        self._tmp.cleanup()

    def _write_repo_resource_files(self) -> None:
        (self.repo_dir / "CLAUDE.md").write_text(
            "# Repo Memory\n\nTags: repo, memory\n\nHigh-signal repository memory.",
            encoding="utf-8",
        )
        skill_dir = self.repo_dir / ".claude" / "agents"
        skill_dir.mkdir(parents=True, exist_ok=True)
        (skill_dir / "reviewer.md").write_text(
            "# Reviewer Skill\n\nTags: skill, review\n\nUse summaries before markdown.",
            encoding="utf-8",
        )
        memory_dir = self.repo_dir / ".researchassistant" / "memory"
        memory_dir.mkdir(parents=True, exist_ok=True)
        (memory_dir / "workflow.md").write_text(
            "---\n"
            "title: Workflow Memory\n"
            "tags: [workflow, repo]\n"
            "description: Shared workflow notes.\n"
            "---\n\n"
            "Workflow details.\n",
            encoding="utf-8",
        )
        (self.repo_dir / "project_profiles" / "custom_profile.yaml").write_text(
            "name: Curriculum Rubric\n"
            "description: Score sources for curriculum relevance.\n",
            encoding="utf-8",
        )

    def _seed_repository_sources(self) -> None:
        state_path = self.repo_dir / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        rows = state["sources"]
        self.assertEqual(len(rows), 3)

        alpha_dir = self.repo_dir / "sources" / "000001"
        beta_dir = self.repo_dir / "sources" / "000002"
        gamma_dir = self.repo_dir / "sources" / "000003"
        alpha_dir.mkdir(parents=True, exist_ok=True)
        beta_dir.mkdir(parents=True, exist_ok=True)
        gamma_dir.mkdir(parents=True, exist_ok=True)

        alpha_markdown = "# Alpha Source\n\nAlpha evidence on apprenticeship outcomes.\n"
        alpha_clean = "# Alpha Source\n\nCleaned alpha evidence with key findings.\n"
        alpha_summary = "Alpha summary with strongest evidence."
        alpha_rating = {
            "overall_score": 0.94,
            "confidence": 0.88,
            "rationale": "Direct evidence for curriculum updates.",
            "ratings": {"overall_relevance": 0.96, "depth_score": 0.9},
        }
        alpha_markdown_path = alpha_dir / "000001_clean.md"
        alpha_clean_path = alpha_dir / "000001_llm_clean.md"
        alpha_summary_path = alpha_dir / "000001_summary.md"
        alpha_rating_path = alpha_dir / "000001_rating.json"
        alpha_markdown_path.write_text(alpha_markdown, encoding="utf-8")
        alpha_clean_path.write_text(alpha_clean, encoding="utf-8")
        alpha_summary_path.write_text(alpha_summary, encoding="utf-8")
        alpha_rating_path.write_text(
            json.dumps(alpha_rating, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        alpha_clean_digest = self._sha256(alpha_clean)

        beta_markdown = "# Beta Source\n\nOlder beta material.\n"
        beta_summary = "Beta summary text."
        beta_rating = {
            "overall_score": 0.42,
            "confidence": 0.5,
            "rationale": "Some relevance but stale.",
            "ratings": {"overall_relevance": 0.4, "depth_score": 0.3},
        }
        beta_markdown_path = beta_dir / "000002_clean.md"
        beta_summary_path = beta_dir / "000002_summary.md"
        beta_rating_path = beta_dir / "000002_rating.json"
        beta_markdown_path.write_text(beta_markdown, encoding="utf-8")
        beta_summary_path.write_text(beta_summary, encoding="utf-8")
        beta_rating_path.write_text(
            json.dumps(beta_rating, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        rows[0].update(
            {
                "title": "Alpha Source",
                "fetch_status": "success",
                "detected_type": "html",
                "final_url": "https://example.com/a/final",
                "markdown_file": "sources/000001/000001_clean.md",
                "llm_cleanup_file": "sources/000001/000001_llm_clean.md",
                "llm_cleanup_status": "cleaned",
                "summary_file": "sources/000001/000001_summary.md",
                "summary_status": "generated",
                "rating_file": "sources/000001/000001_rating.json",
                "rating_status": "generated",
                "metadata_file": "sources/000001/000001_metadata.json",
                "sha256": "fetch-alpha",
                "fetched_at": "2026-04-01T12:00:00+00:00",
                "phase_metadata": {
                    "fetch": {
                        "phase": "fetch",
                        "status": "completed",
                        "content_digest": "fetch-alpha",
                    },
                    "convert": {
                        "phase": "convert",
                        "status": "completed",
                        "content_digest": alpha_clean_digest,
                    },
                    "summarize": {
                        "phase": "summarize",
                        "status": "completed",
                        "content_digest": alpha_clean_digest,
                        "model": "test-model",
                    },
                    "tag": {
                        "phase": "tag",
                        "status": "completed",
                        "content_digest": alpha_clean_digest,
                        "model": "test-model",
                        "profile_name": "custom_profile.yaml",
                    },
                },
            }
        )

        rows[1].update(
            {
                "title": "Beta Source",
                "fetch_status": "success",
                "detected_type": "pdf",
                "final_url": "https://example.com/b/final",
                "markdown_file": "sources/000002/000002_clean.md",
                "summary_file": "sources/000002/000002_summary.md",
                "summary_status": "generated",
                "rating_file": "sources/000002/000002_rating.json",
                "rating_status": "stale",
                "metadata_file": "sources/000002/000002_metadata.json",
                "sha256": "fetch-beta",
                "fetched_at": "2026-04-01T10:00:00+00:00",
                "phase_metadata": {
                    "fetch": {
                        "phase": "fetch",
                        "status": "completed",
                        "content_digest": "fetch-beta",
                    },
                    "convert": {
                        "phase": "convert",
                        "status": "completed",
                        "content_digest": self._sha256(beta_markdown),
                    },
                    "summarize": {
                        "phase": "summarize",
                        "status": "completed",
                        "content_digest": self._sha256(beta_markdown),
                    },
                    "tag": {
                        "phase": "tag",
                        "status": "stale",
                        "content_digest": "old-beta-digest",
                        "stale": True,
                    },
                },
            }
        )

        rows[2].update(
            {
                "title": "Gamma Source",
                "fetch_status": "queued",
                "detected_type": "html",
                "final_url": "",
                "sha256": "",
                "fetched_at": "",
                "metadata_file": "sources/000003/000003_metadata.json",
                "phase_metadata": {
                    "fetch": {"phase": "fetch", "status": "pending"},
                },
            }
        )

        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        self.service.rebuild()

    def _sha256(self, content: str) -> str:
        import hashlib

        return hashlib.sha256(content.encode("utf-8")).hexdigest()

    def test_agent_sources_require_bearer_token(self):
        response = self.client.get("/api/agent/v1/sources")
        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.json()["error"]["code"], "agent_auth_required")

    def test_list_sources_supports_cursor_and_relevance_filters(self):
        response = self.client.get(
            "/api/agent/v1/sources?sort_by=rating_overall&sort_dir=desc&limit=1",
            headers=self.read_headers,
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["data"]["items"][0]["source_id"], "000001")
        next_cursor = payload["data"]["next_cursor"]
        self.assertTrue(next_cursor)

        next_response = self.client.get(
            f"/api/agent/v1/sources?sort_by=rating_overall&sort_dir=desc&limit=1&cursor={next_cursor}",
            headers=self.read_headers,
        )
        self.assertEqual(next_response.status_code, 200)
        next_payload = next_response.json()
        self.assertEqual(next_payload["data"]["items"][0]["source_id"], "000002")

        filtered = self.client.get(
            "/api/agent/v1/sources?min_relevance=0.9",
            headers=self.read_headers,
        )
        self.assertEqual(filtered.status_code, 200)
        items = filtered.json()["data"]["items"]
        self.assertEqual([item["source_id"] for item in items], ["000001"])

    def test_get_source_exposes_freshness_and_artifact_uris(self):
        response = self.client.get("/api/agent/v1/sources/000002", headers=self.read_headers)
        self.assertEqual(response.status_code, 200)
        data = response.json()["data"]
        self.assertEqual(data["source_id"], "000002")
        self.assertTrue(data["freshness"]["rating_stale"])
        self.assertEqual(data["artifact_uris"]["rating"], "repo://sources/000002/rating")
        self.assertEqual(data["provenance"]["import_id"], self.first_import_id)

    def test_source_content_chunks_with_offsets(self):
        response = self.client.get(
            "/api/agent/v1/sources/000001/content?kind=summary&chunk_size=10&include_offsets=true",
            headers=self.read_headers,
        )
        self.assertEqual(response.status_code, 200)
        data = response.json()["data"]
        self.assertEqual(data["offset_start"], 0)
        self.assertEqual(data["offset_end"], 10)
        self.assertTrue(data["next_cursor"])

        response_2 = self.client.get(
            f"/api/agent/v1/sources/000001/content?kind=summary&chunk_size=50&cursor={data['next_cursor']}",
            headers=self.read_headers,
        )
        self.assertEqual(response_2.status_code, 200)
        self.assertIn("strongest", response_2.json()["data"]["content"])

    def test_resources_index_memory_skill_and_rubric_files(self):
        response = self.client.get("/api/agent/v1/resources", headers=self.read_headers)
        self.assertEqual(response.status_code, 200)
        items = response.json()["data"]["items"]
        kinds = {item["kind"] for item in items}
        self.assertIn("memory", kinds)
        self.assertIn("skill", kinds)
        self.assertIn("rubric", kinds)

        memory_item = next(item for item in items if item["kind"] == "memory")
        detail = self.client.get(
            f"/api/agent/v1/resources/{memory_item['resource_id']}",
            headers=self.read_headers,
        )
        self.assertEqual(detail.status_code, 200)
        self.assertIn("Repo Memory", detail.json()["data"]["content"])

    def test_run_source_phases_supports_idempotency_cancel_and_audit(self):
        started = threading.Event()
        release = threading.Event()

        def fake_run(self):
            started.set()
            release.wait(timeout=2)

        with patch(
            "backend.storage.attached_repository.SourceDownloadOrchestrator.run",
            new=fake_run,
        ):
            response = self.client.post(
                "/api/agent/v1/runs/source-phases",
                headers=self.write_headers,
                json={
                    "scope": "all",
                    "source_ids": ["000001"],
                    "phases": ["convert"],
                    "idempotency_key": "same-run",
                },
            )
            self.assertEqual(response.status_code, 202)
            run_id = response.json()["data"]["run_id"]
            self.assertTrue(started.wait(timeout=1))

            duplicate = self.client.post(
                "/api/agent/v1/runs/source-phases",
                headers=self.write_headers,
                json={
                    "scope": "all",
                    "source_ids": ["000001"],
                    "phases": ["convert"],
                    "idempotency_key": "same-run",
                },
            )
            self.assertEqual(duplicate.status_code, 202)
            self.assertEqual(duplicate.json()["data"]["run_id"], run_id)

            status = self.client.get(
                f"/api/agent/v1/runs/{run_id}",
                headers=self.read_headers,
            )
            self.assertEqual(status.status_code, 200)
            self.assertEqual(status.json()["data"]["scope"], "source_ids")
            self.assertEqual(status.json()["data"]["selected_source_ids"], ["000001"])

            cancel = self.client.post(
                f"/api/agent/v1/runs/{run_id}/cancel",
                headers=self.write_headers,
            )
            self.assertEqual(cancel.status_code, 202)

            with self.app.state.source_download_lock:
                orchestrator = self.app.state.source_download_jobs.get(run_id)
            self.assertIsNotNone(orchestrator)
            self.assertTrue(orchestrator.cancel_requested)

            release.set()
            if self.service._download_thread is not None:
                self.service._download_thread.join(timeout=1)

        audit_path = self.repo_dir / ".ra_repo" / "agent_audit.jsonl"
        audit_lines = [
            json.loads(line)
            for line in audit_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        actions = [item["action"] for item in audit_lines]
        self.assertIn("run_source_phases", actions)
        self.assertIn("cancel_run", actions)

    def test_mcp_tools_and_resources_match_rest_surface(self):
        init = self.client.post(
            "/api/agent/v1/mcp",
            headers=self.read_headers,
            json={"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}},
        )
        self.assertEqual(init.status_code, 200)
        self.assertEqual(init.json()["result"]["serverInfo"]["name"], "ResearchAssistant Agent Surface")

        rest_sources = self.client.get(
            "/api/agent/v1/sources?sort_by=rating_overall&sort_dir=desc&limit=1",
            headers=self.read_headers,
        ).json()["data"]
        mcp_sources = self.client.post(
            "/api/agent/v1/mcp",
            headers=self.read_headers,
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "search_sources",
                    "arguments": {
                        "sort_by": "rating_overall",
                        "sort_dir": "desc",
                        "limit": 1,
                    },
                },
            },
        ).json()["result"]["structuredContent"]
        self.assertEqual(
            mcp_sources["items"][0]["source_id"],
            rest_sources["items"][0]["source_id"],
        )

        resource_list = self.client.get(
            "/api/agent/v1/resources",
            headers=self.read_headers,
        ).json()["data"]["items"]
        memory_item = next(item for item in resource_list if item["kind"] == "memory")
        rest_resource = self.client.get(
            f"/api/agent/v1/resources/{memory_item['resource_id']}",
            headers=self.read_headers,
        ).json()["data"]["content"]
        mcp_resource = self.client.post(
            "/api/agent/v1/mcp",
            headers=self.read_headers,
            json={
                "jsonrpc": "2.0",
                "id": 3,
                "method": "resources/read",
                "params": {"uri": f"repo://memory/{memory_item['resource_id']}"},
            },
        ).json()["result"]["contents"][0]["text"]
        self.assertEqual(mcp_resource, rest_resource)


if __name__ == "__main__":
    unittest.main()
