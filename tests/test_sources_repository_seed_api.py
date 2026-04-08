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

from backend.routers import repository, sources
from backend.models.settings import RepoSettings
from backend.pipeline.source_downloader import SourceDownloadOrchestrator
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore
from backend.storage.project_profiles import DEFAULT_PROJECT_PROFILE_FILENAME


class SourcesRepositorySeedApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="sources-repo-seed-api-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.service = AttachedRepositoryService(store=self.store)

        self.repo_dir = self.tmp_path / "repo"
        self.repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(self.repo_dir))
        self.service.import_source_list(
            filename="sources.csv",
            content=("URL\nhttps://example.com/a\n").encode("utf-8"),
        )

        state_path = self.repo_dir / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["sources"][0]["fetch_status"] = "success"
        state["sources"][0]["markdown_file"] = "markdown/000001.md"
        state_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        markdown_path = self.repo_dir / "markdown" / "000001.md"
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text("# Existing markdown\n", encoding="utf-8")

        app = FastAPI()
        app.state.file_store = self.store
        app.state.repository_service = self.service
        app.state.source_download_jobs = {}
        app.state.source_download_lock = threading.Lock()
        app.include_router(sources.router, prefix="/api")
        app.include_router(repository.router, prefix="/api")
        self.app = app
        self.client = TestClient(app)

    def tearDown(self):
        self._tmp.cleanup()

    def _update_source_row(self, **fields):
        state_path = self.repo_dir / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["sources"][0].update(fields)
        state_path.write_text(
            json.dumps(state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _start_empty_only_job(self, **payload):
        started = threading.Event()
        release = threading.Event()

        def fake_run(self: SourceDownloadOrchestrator):
            started.set()
            release.wait(timeout=2)

        with patch(
            "backend.storage.attached_repository.SourceDownloadOrchestrator.run",
            new=fake_run,
        ):
            response = self.client.post(
                "/api/repository/source-tasks",
                json={
                    "scope": "empty_only",
                    "import_id": "",
                    "rerun_failed_only": False,
                    "run_download": False,
                    "run_convert": False,
                    "run_catalog": False,
                    "run_citation_verify": False,
                    "run_llm_cleanup": False,
                    "run_llm_title": False,
                    "run_llm_summary": False,
                    "run_llm_rating": False,
                    "force_redownload": False,
                    "force_convert": False,
                    "force_catalog": False,
                    "force_citation_verify": False,
                    "force_llm_cleanup": False,
                    "force_title": False,
                    "force_summary": False,
                    "force_rating": False,
                    "project_profile_name": "",
                    "include_raw_file": False,
                    "include_rendered_html": False,
                    "include_rendered_pdf": False,
                    "include_markdown": False,
                    **payload,
                },
            )

            if response.status_code != 200:
                return response, None

            job_id = response.json()["job_id"]
            self.assertTrue(started.wait(timeout=1))
            with self.app.state.source_download_lock:
                orchestrator = self.app.state.source_download_jobs.get(job_id)
            self.assertIsNotNone(orchestrator)

            release.set()
            if self.service._download_thread is not None:
                self.service._download_thread.join(timeout=1)
            return response, orchestrator

    def test_summary_cleanup_only_run_bootstraps_existing_repository_output(self):
        export_job = self.service.create_export_job(scope="all")
        job_store = self.service.job_store_for(export_job.job_id)
        self.assertIsNone(job_store.load_artifact(export_job.job_id, "06_sources_manifest"))

        response = self.client.post(
            f"/api/sources/{export_job.job_id}/download",
            json={
                "run_download": False,
                "run_llm_cleanup": True,
                "run_llm_summary": False,
                "run_llm_rating": False,
                "include_raw_file": True,
                "include_rendered_html": True,
                "include_rendered_pdf": True,
                "include_markdown": True,
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "started")
        self.assertEqual(payload["run_download"], False)

        seeded_manifest = job_store.load_artifact(export_job.job_id, "06_sources_manifest")
        self.assertIsNotNone(seeded_manifest)
        rows = seeded_manifest.get("rows", [])
        self.assertGreaterEqual(len(rows), 1)
        self.assertEqual(rows[0].get("markdown_file"), "markdown/000001.md")

        copied_markdown = job_store.get_sources_output_dir(export_job.job_id) / "markdown" / "000001.md"
        self.assertTrue(copied_markdown.exists())

    def test_status_reports_running_when_orchestrator_exists_without_persisted_status(self):
        export_job = self.service.create_export_job(scope="all")

        class DummyOrchestrator:
            status = None
            run_download = False
            run_llm_cleanup = True
            run_llm_summary = False
            run_llm_rating = False
            force_redownload = False
            force_llm_cleanup = False
            force_summary = False
            force_rating = False

            def request_cancel(self):
                return None

        with self.app.state.source_download_lock:
            self.app.state.source_download_jobs[export_job.job_id] = DummyOrchestrator()

        response = self.client.get(f"/api/sources/{export_job.job_id}/status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["state"], "running")
        self.assertIn("Preparing source task run", payload["message"])

    def test_cancel_can_stop_running_orchestrator_without_persisted_status(self):
        export_job = self.service.create_export_job(scope="all")

        class DummyOrchestrator:
            cancelled = False

            def request_cancel(self):
                self.cancelled = True

        orchestrator = DummyOrchestrator()
        with self.app.state.source_download_lock:
            self.app.state.source_download_jobs[export_job.job_id] = orchestrator

        response = self.client.post(f"/api/sources/{export_job.job_id}/cancel")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "cancelling")
        self.assertTrue(orchestrator.cancelled)

    def test_status_reports_cancelling_when_orchestrator_has_pending_stop(self):
        export_job = self.service.create_export_job(scope="all")

        class DummyOrchestrator:
            status = None
            cancel_requested = True
            run_download = False
            run_llm_cleanup = True
            run_llm_summary = False
            run_llm_rating = False
            force_redownload = False
            force_llm_cleanup = False
            force_summary = False
            force_rating = False

            def request_cancel(self):
                return None

        with self.app.state.source_download_lock:
            self.app.state.source_download_jobs[export_job.job_id] = DummyOrchestrator()

        response = self.client.get(f"/api/sources/{export_job.job_id}/status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["state"], "cancelling")
        self.assertTrue(payload["cancel_requested"])
        self.assertIn("Stop requested", payload["message"])

    def test_repository_source_task_cancel_uses_live_registry(self):
        started = threading.Event()
        release = threading.Event()

        def fake_run(self: SourceDownloadOrchestrator):
            started.set()
            release.wait(timeout=2)

        with patch(
            "backend.storage.attached_repository.SourceDownloadOrchestrator.run",
            new=fake_run,
        ):
            response = self.client.post(
                "/api/repository/source-tasks",
                json={
                    "scope": "all",
                    "import_id": "",
                    "rerun_failed_only": False,
                    "run_download": False,
                    "run_llm_cleanup": True,
                    "run_llm_summary": False,
                    "run_llm_rating": False,
                    "force_redownload": False,
                    "force_llm_cleanup": False,
                    "force_summary": False,
                    "force_rating": False,
                    "project_profile_name": "",
                    "include_raw_file": True,
                    "include_rendered_html": True,
                    "include_rendered_pdf": True,
                    "include_markdown": True,
                },
            )

            self.assertEqual(response.status_code, 200)
            payload = response.json()
            job_id = payload["job_id"]
            self.assertTrue(started.wait(timeout=1))

            with self.app.state.source_download_lock:
                orchestrator = self.app.state.source_download_jobs.get(job_id)
            self.assertIsNotNone(orchestrator)

            cancel_response = self.client.post(f"/api/sources/{job_id}/cancel")
            self.assertEqual(cancel_response.status_code, 200)
            cancel_payload = cancel_response.json()
            self.assertEqual(cancel_payload["status"], "cancelling")
            self.assertIn("Stop requested", cancel_payload["message"])

            with self.app.state.source_download_lock:
                orchestrator = self.app.state.source_download_jobs.get(job_id)
            self.assertIsNotNone(orchestrator)
            self.assertTrue(orchestrator.cancel_requested)

            release.set()

    def test_repository_source_tasks_force_title_implies_title_phase(self):
        started = threading.Event()
        release = threading.Event()

        def fake_run(self: SourceDownloadOrchestrator):
            started.set()
            release.wait(timeout=2)

        with patch(
            "backend.storage.attached_repository.SourceDownloadOrchestrator.run",
            new=fake_run,
        ):
            response = self.client.post(
                "/api/repository/source-tasks",
                json={
                    "scope": "all",
                    "import_id": "",
                    "rerun_failed_only": False,
                    "run_download": False,
                    "run_llm_cleanup": False,
                    "run_llm_title": False,
                    "run_llm_summary": False,
                    "run_llm_rating": False,
                    "force_redownload": False,
                    "force_llm_cleanup": False,
                    "force_title": True,
                    "force_summary": False,
                    "force_rating": False,
                    "project_profile_name": "",
                    "include_raw_file": True,
                    "include_rendered_html": True,
                    "include_rendered_pdf": True,
                    "include_markdown": True,
                },
            )

            self.assertEqual(response.status_code, 200)
            job_id = response.json()["job_id"]
            self.assertTrue(started.wait(timeout=1))

            with self.app.state.source_download_lock:
                orchestrator = self.app.state.source_download_jobs.get(job_id)
            self.assertIsNotNone(orchestrator)
            self.assertTrue(orchestrator.run_llm_title)
            self.assertFalse(orchestrator.run_catalog)
            self.assertFalse(orchestrator.run_download)

            release.set()
            if self.service._download_thread is not None:
                self.service._download_thread.join(timeout=1)

            deadline = time.time() + 1
            while time.time() < deadline:
                with self.app.state.source_download_lock:
                    if job_id not in self.app.state.source_download_jobs:
                        break
                time.sleep(0.01)

            with self.app.state.source_download_lock:
                self.assertNotIn(job_id, self.app.state.source_download_jobs)

    def test_repository_source_tasks_blank_profile_uses_default_profile(self):
        started = threading.Event()
        release = threading.Event()

        self.service.save_repo_settings(
            RepoSettings(
                use_llm=False,
                research_purpose="Find evidence to support an HVAC apprenticeship curriculum update.",
            )
        )

        def fake_run(self: SourceDownloadOrchestrator):
            started.set()
            release.wait(timeout=2)

        with patch(
            "backend.storage.attached_repository.SourceDownloadOrchestrator.run",
            new=fake_run,
        ):
            response = self.client.post(
                "/api/repository/source-tasks",
                json={
                    "scope": "all",
                    "import_id": "",
                    "rerun_failed_only": False,
                    "run_download": False,
                    "run_llm_cleanup": False,
                    "run_llm_title": False,
                    "run_llm_summary": False,
                    "run_llm_rating": True,
                    "force_redownload": False,
                    "force_llm_cleanup": False,
                    "force_title": False,
                    "force_summary": False,
                    "force_rating": False,
                    "project_profile_name": "",
                    "include_raw_file": True,
                    "include_rendered_html": True,
                    "include_rendered_pdf": True,
                    "include_markdown": True,
                },
            )

            self.assertEqual(response.status_code, 200)
            job_id = response.json()["job_id"]
            self.assertTrue(started.wait(timeout=1))

            with self.app.state.source_download_lock:
                orchestrator = self.app.state.source_download_jobs.get(job_id)
            self.assertIsNotNone(orchestrator)
            self.assertEqual(orchestrator.project_profile_name, DEFAULT_PROJECT_PROFILE_FILENAME)
            self.assertIn(
                "Find evidence to support an HVAC apprenticeship curriculum update.",
                orchestrator.project_profile_yaml,
            )
            self.assertNotIn("{{research_purpose}}", orchestrator.project_profile_yaml)

            release.set()
            if self.service._download_thread is not None:
                self.service._download_thread.join(timeout=1)

    def test_empty_only_scope_selects_rows_missing_markdown_outputs(self):
        self._update_source_row(markdown_file="")

        response, orchestrator = self._start_empty_only_job(
            run_download=True,
            run_convert=True,
            include_markdown=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["scope"], "empty_only")
        self.assertEqual([row.id for row in orchestrator.target_rows], ["000001"])

    def test_empty_only_scope_selects_rows_missing_rendered_pdf_outputs(self):
        self._update_source_row(rendered_pdf_file="")

        response, orchestrator = self._start_empty_only_job(
            run_download=True,
            run_convert=False,
            include_rendered_pdf=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["scope"], "empty_only")
        self.assertEqual([row.id for row in orchestrator.target_rows], ["000001"])
        self.assertFalse(orchestrator.run_convert)

    def test_import_scope_duplicate_seed_batch_selects_existing_rows(self):
        duplicate_import = self.service.import_source_list(
            filename="sources-dup.csv",
            content=("URL\nhttps://example.com/a\n").encode("utf-8"),
        )
        self.assertEqual(duplicate_import.accepted_new, 0)

        started = threading.Event()
        release = threading.Event()

        def fake_run(self: SourceDownloadOrchestrator):
            started.set()
            release.wait(timeout=2)

        with patch(
            "backend.storage.attached_repository.SourceDownloadOrchestrator.run",
            new=fake_run,
        ):
            response = self.client.post(
                "/api/repository/source-tasks",
                json={
                    "scope": "import",
                    "import_id": duplicate_import.import_id,
                    "rerun_failed_only": False,
                    "run_download": True,
                    "run_convert": True,
                    "run_catalog": False,
                    "run_citation_verify": False,
                    "run_llm_cleanup": False,
                    "run_llm_title": False,
                    "run_llm_summary": False,
                    "run_llm_rating": False,
                    "force_redownload": False,
                    "force_convert": False,
                    "force_catalog": False,
                    "force_citation_verify": False,
                    "force_llm_cleanup": False,
                    "force_title": False,
                    "force_summary": False,
                    "force_rating": False,
                    "project_profile_name": "",
                    "include_raw_file": True,
                    "include_rendered_html": True,
                    "include_rendered_pdf": True,
                    "include_markdown": True,
                    "source_ids": [],
                },
            )

            self.assertEqual(response.status_code, 200)
            job_id = response.json()["job_id"]
            self.assertTrue(started.wait(timeout=1))

            with self.app.state.source_download_lock:
                orchestrator = self.app.state.source_download_jobs.get(job_id)
            self.assertIsNotNone(orchestrator)
            self.assertEqual([row.id for row in orchestrator.target_rows], ["000001"])

            release.set()
            if self.service._download_thread is not None:
                self.service._download_thread.join(timeout=1)

    def test_empty_only_scope_selects_rows_missing_catalog_artifacts(self):
        self._update_source_row(catalog_file="", catalog_status="")

        response, orchestrator = self._start_empty_only_job(
            run_catalog=True,
            include_markdown=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual([row.id for row in orchestrator.target_rows], ["000001"])

    def test_empty_only_scope_selects_rows_missing_cleanup_artifacts(self):
        self._update_source_row(llm_cleanup_file="", llm_cleanup_status="")

        response, orchestrator = self._start_empty_only_job(
            run_llm_cleanup=True,
            include_markdown=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual([row.id for row in orchestrator.target_rows], ["000001"])

    def test_empty_only_scope_selects_rows_missing_title_values(self):
        self._update_source_row(title="", title_status="")

        response, orchestrator = self._start_empty_only_job(
            run_llm_title=True,
            include_markdown=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual([row.id for row in orchestrator.target_rows], ["000001"])

    def test_empty_only_scope_selects_rows_missing_citation_verification(self):
        self._update_source_row(catalog_file="", catalog_status="")

        response, orchestrator = self._start_empty_only_job(
            run_citation_verify=True,
            include_markdown=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual([row.id for row in orchestrator.target_rows], ["000001"])

    def test_empty_only_scope_selects_rows_with_non_ready_citation_verification(self):
        catalog_path = self.repo_dir / "metadata" / "000001_catalog.json"
        catalog_path.parent.mkdir(parents=True, exist_ok=True)
        catalog_path.write_text(
            json.dumps(
                {
                    "citation": {
                        "title": "Existing Source",
                        "issued": "2024",
                        "url": "https://example.com/a",
                        "verification_status": "blocked",
                        "missing_fields": ["authors"],
                        "ready_for_ris": False,
                    }
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        self._update_source_row(catalog_file="metadata/000001_catalog.json", catalog_status="generated")

        response, orchestrator = self._start_empty_only_job(
            run_citation_verify=True,
            include_markdown=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual([row.id for row in orchestrator.target_rows], ["000001"])

    def test_empty_only_scope_selects_rows_missing_summary_artifacts(self):
        self._update_source_row(summary_file="", summary_status="")

        response, orchestrator = self._start_empty_only_job(
            run_llm_summary=True,
            include_markdown=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual([row.id for row in orchestrator.target_rows], ["000001"])

    def test_empty_only_scope_selects_rows_missing_rating_artifacts(self):
        self._update_source_row(rating_file="", rating_status="")

        response, orchestrator = self._start_empty_only_job(
            run_llm_rating=True,
            include_markdown=True,
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual([row.id for row in orchestrator.target_rows], ["000001"])


if __name__ == "__main__":
    unittest.main()
