from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from backend.storage.file_store import FileStore


class ProjectProfileTests(unittest.TestCase):
    def test_list_empty_profiles(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = FileStore(Path(tmp))
            profiles = store.list_project_profiles()
            self.assertEqual(profiles, [])

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
