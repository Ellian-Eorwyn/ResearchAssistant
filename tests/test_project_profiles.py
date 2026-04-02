from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
