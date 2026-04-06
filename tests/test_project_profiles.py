from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.models.settings import RepoSettings
from backend.routers import results
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore
from backend.storage.project_profiles import DEFAULT_PROJECT_PROFILE_FILENAME


class ProjectProfileTests(unittest.TestCase):
    def test_list_profiles_includes_default_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FileStore(Path(tmp))
            profiles = store.list_project_profiles()
            filenames = {profile["filename"] for profile in profiles}
            self.assertIn(DEFAULT_PROJECT_PROFILE_FILENAME, filenames)

    def test_list_profiles_finds_yaml_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FileStore(Path(tmp))
            (store.project_profiles_dir / "test_profile.yaml").write_text("version: '1.0'")
            (store.project_profiles_dir / "another.yml").write_text("version: '1.0'")
            profiles = store.list_project_profiles()
            names = {p["name"] for p in profiles}
            self.assertIn("test_profile", names)
            self.assertIn("another", names)

    def test_load_project_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FileStore(Path(tmp))
            content = "version: '1.0'\nprofile_name: test"
            (store.project_profiles_dir / "my_profile.yaml").write_text(content)
            loaded = store.load_project_profile("my_profile.yaml")
            self.assertEqual(loaded, content)

    def test_resolve_default_project_profile_renders_research_purpose(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FileStore(Path(tmp))
            filename, loaded = store.resolve_project_profile(
                "",
                research_purpose="Find implementation evidence for EV charging workforce training.",
                default_when_blank=True,
            )
            self.assertEqual(filename, DEFAULT_PROJECT_PROFILE_FILENAME)
            self.assertIn(
                "Find implementation evidence for EV charging workforce training.",
                loaded,
            )
            self.assertNotIn("{{research_purpose}}", loaded)
            self.assertIn('id: "overall_relevance"', loaded)

    def test_load_project_profile_path_traversal(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FileStore(Path(tmp))
            with self.assertRaises(ValueError):
                store.load_project_profile("../../../etc/passwd")

    def test_load_project_profile_not_found(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FileStore(Path(tmp))
            with self.assertRaises(ValueError):
                store.load_project_profile("nonexistent.yaml")


class ProjectProfileApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="project-profile-api-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(self.tmp_path / "data")
        self.service = AttachedRepositoryService(store=self.store)
        self.repo_dir = self.tmp_path / "repo"
        self.repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.create(str(self.repo_dir))

        app = FastAPI()
        app.state.file_store = self.store
        app.state.repository_service = self.service
        app.include_router(results.router, prefix="/api")
        self.client = TestClient(app)

    def tearDown(self):
        self._tmp.cleanup()

    def test_generate_project_profile_endpoint_returns_yaml_draft(self):
        self.service.save_repo_settings(
            RepoSettings(
                use_llm=True,
                research_purpose="Find evidence for housing retrofit workforce training.",
                llm_backend={
                    "kind": "ollama",
                    "base_url": "http://localhost:11434",
                    "api_key": "",
                    "model": "test-model",
                    "temperature": 0,
                    "think_mode": "default",
                    "num_ctx": 8192,
                    "max_source_chars": 0,
                    "llm_timeout": 300,
                },
            )
        )

        generated_yaml = """
version: "1.0"
profile_name: "Housing Retrofit Review"
outputs:
  ratings:
    dimensions:
      - id: "overall_relevance"
      - id: "depth_score"
      - id: "relevant_detail_score"
""".strip()

        class DummyClient:
            def __init__(self, *_args, **_kwargs):
                pass

            def sync_chat_completion(self, **_kwargs):
                return json.dumps({"yaml": generated_yaml})

            def sync_close(self):
                return None

        with patch("backend.storage.attached_repository.UnifiedLLMClient", DummyClient):
            response = self.client.post(
                "/api/project-profiles/generate",
                json={
                    "research_purpose": "Find evidence for housing retrofit workforce training.",
                    "profile_name": "Housing Retrofit Review",
                    "filename": "housing_retrofit_review.yaml",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["filename"], "housing_retrofit_review.yaml")
        self.assertIn('id: "overall_relevance"', payload["content"])
        self.assertIn('id: "depth_score"', payload["content"])
        self.assertIn('id: "relevant_detail_score"', payload["content"])

    def test_save_project_profile_endpoint_writes_repo_local_yaml(self):
        response = self.client.put(
            "/api/project-profiles/housing_retrofit_review.yaml",
            json={
                "content": (
                    'version: "1.0"\n'
                    'profile_name: "Housing Retrofit Review"\n'
                    "outputs:\n"
                    "  ratings:\n"
                    "    dimensions:\n"
                    '      - id: "overall_relevance"\n'
                )
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        target = self.repo_dir / "project_profiles" / "housing_retrofit_review.yaml"
        self.assertTrue(target.exists())
        self.assertEqual(payload["filename"], "housing_retrofit_review.yaml")
        self.assertIn('id: "overall_relevance"', target.read_text(encoding="utf-8"))

    def test_save_project_profile_endpoint_rejects_invalid_extension(self):
        response = self.client.put(
            "/api/project-profiles/escape.txt",
            json={"content": 'version: "1.0"\nprofile_name: "Escape"\n'},
        )
        self.assertEqual(response.status_code, 400)


if __name__ == "__main__":
    unittest.main()
