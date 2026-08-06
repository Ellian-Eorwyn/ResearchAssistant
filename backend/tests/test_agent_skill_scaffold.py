"""Tests for shipping agent skills into every research repository."""

from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend.storage import agent_skills
from backend.storage.agent_skills import (
    STATUS_DIVERGED,
    STATUS_INSTALLED,
    STATUS_REFRESHED,
    STATUS_UNCHANGED,
    STATUS_UNKNOWN_PROVENANCE,
    skill_tree_hash,
)
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


class _ScaffoldTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="skill-scaffold-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.service = AttachedRepositoryService(store=FileStore(base_dir=self.tmp_path / "app"))
        self.repo = self.tmp_path / "repo"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def create(self) -> None:
        self.service.create(str(self.repo))

    @staticmethod
    def shipped_skill_names() -> list[str]:
        """Derived from the bundle, so adding a skill does not break tests."""
        bundle = agent_skills.bundled_agent_skills_dir()
        return sorted(
            path.name
            for path in bundle.iterdir()
            if path.is_dir() and (path / "SKILL.md").is_file()
        )

    def manifest(self) -> dict:
        return json.loads((self.repo / ".ra_repo" / "bundled_skills.json").read_text())

    def report_status(self, name: str) -> str:
        for record in self.manifest().get("last_report", []):
            if record.get("name") == name:
                return record.get("status", "")
        return ""

    def make_fake_bundle(self, body: str = "original") -> Path:
        """A stand-in bundle so tests do not depend on the shipped skills."""
        bundle = self.tmp_path / "bundle"
        skill = bundle / "demo-skill"
        (skill / "references").mkdir(parents=True, exist_ok=True)
        (skill / "SKILL.md").write_text(
            f"---\nname: demo-skill\ndescription: A demo skill for tests.\n---\n\n{body}\n",
            encoding="utf-8",
        )
        (skill / "references" / "notes.md").write_text(f"{body} notes\n", encoding="utf-8")
        return bundle


class BundledSkillScaffoldTests(_ScaffoldTestCase):
    def test_create_installs_the_shipped_skills(self) -> None:
        self.create()

        skills_root = self.repo / ".agents" / "skills"
        self.assertTrue((skills_root / "repo-remap-source-ids" / "SKILL.md").is_file())
        self.assertTrue((skills_root / "repo-attach-files" / "SKILL.md").is_file())
        self.assertTrue((self.repo / "AGENTS.md").is_file())
        self.assertTrue((self.repo / ".mcp.json").is_file())
        self.assertTrue((self.repo / ".ra_repo" / "inbox").is_dir())
        self.assertTrue((self.repo / ".ra_repo" / "operations").is_dir())

        recorded = self.manifest()["skills"]
        self.assertIn("repo-remap-source-ids", recorded)
        self.assertTrue(recorded["repo-remap-source-ids"]["shipped_hash"])

    def test_shipped_skills_satisfy_the_spec(self) -> None:
        """name must be lowercase-hyphenated and match its directory."""
        self.create()
        skills_root = self.repo / ".agents" / "skills"
        found = []
        for skill_dir in sorted(skills_root.iterdir()):
            text = (skill_dir / "SKILL.md").read_text(encoding="utf-8")
            self.assertTrue(text.startswith("---\n"), skill_dir.name)
            front, _, _ = text[4:].partition("\n---")
            fields = dict(
                (line.split(":", 1)[0].strip(), line.split(":", 1)[1].strip())
                for line in front.splitlines()
                if ":" in line
            )
            self.assertEqual(fields.get("name"), skill_dir.name)
            self.assertRegex(fields["name"], r"^[a-z0-9]+(-[a-z0-9]+)*$")
            self.assertLessEqual(len(fields["name"]), 64)
            self.assertTrue(fields.get("description"))
            # The description is a single line: the frontmatter parser is
            # line-based and does not understand folded YAML scalars.
            self.assertLessEqual(len(fields["description"]), 1024)
            found.append(skill_dir.name)
        self.assertEqual(found, self.shipped_skill_names())

    def test_claude_skills_link_to_the_canonical_copies(self) -> None:
        self.create()
        for name in ("repo-remap-source-ids", "repo-attach-files"):
            link = self.repo / ".claude" / "skills" / name
            canonical = self.repo / ".agents" / "skills" / name
            self.assertTrue(link.exists() or link.is_symlink(), name)
            self.assertEqual(link.resolve(), canonical.resolve())

    def test_scaffold_is_idempotent(self) -> None:
        self.create()
        skill = self.repo / ".agents" / "skills" / "repo-attach-files"
        first = skill_tree_hash(skill)

        for _ in range(3):
            self.service.attach(str(self.repo))

        self.assertEqual(skill_tree_hash(skill), first)
        self.assertEqual(self.report_status("repo-attach-files"), STATUS_UNCHANGED)

    def test_resource_index_lists_each_skill_once(self) -> None:
        self.create()
        resources = self.service.list_agent_resources()
        skills = [item for item in resources if item.kind == "skill"]
        expected = self.shipped_skill_names()

        self.assertEqual(len(skills), len(expected))
        self.assertEqual(len({item.resource_id for item in skills}), len(expected))
        for item in skills:
            # `.claude/skills` symlinks to these; indexing both would produce
            # two resource ids for one skill.
            self.assertTrue(item.path.startswith(".agents/skills/"), item.path)
            self.assertEqual(item.provenance, "bundled")
            self.assertTrue(item.short_description)

        # The title comes from the spec's `name` field, which equals the
        # directory name -- SKILL.md's own stem would be useless here.
        self.assertEqual(sorted(item.title for item in skills), expected)

    def test_editing_a_skill_marks_it_modified(self) -> None:
        self.create()
        target = self.repo / ".agents" / "skills" / "repo-attach-files" / "SKILL.md"
        target.write_text(
            target.read_text(encoding="utf-8") + "\n\nLocal note.\n", encoding="utf-8"
        )

        self.service.attach(str(self.repo))

        skills = {
            item.title: item for item in self.service.list_agent_resources() if item.kind == "skill"
        }
        self.assertEqual(skills["repo-attach-files"].provenance, "bundled_modified")
        self.assertIn("Local note.", target.read_text(encoding="utf-8"))

    def test_mcp_config_uses_an_env_var_not_a_literal_token(self) -> None:
        self.create()
        config = json.loads((self.repo / ".mcp.json").read_text())
        header = config["mcpServers"]["researchassistant"]["headers"]["Authorization"]
        self.assertEqual(header, "Bearer ${RA_AGENT_WRITE_TOKEN}")

        real_token = self.service.load_agent_tokens()["write_token"]
        self.assertNotIn(real_token, (self.repo / ".mcp.json").read_text())

    def test_mcp_config_is_not_overwritten(self) -> None:
        self.create()
        path = self.repo / ".mcp.json"
        path.write_text('{"mcpServers": {"mine": {}}}', encoding="utf-8")
        self.service.attach(str(self.repo))
        self.assertEqual(json.loads(path.read_text()), {"mcpServers": {"mine": {}}})

    def test_gitignore_protects_the_agent_token(self) -> None:
        self.create()
        text = (self.repo / ".gitignore").read_text()
        self.assertIn(".ra_repo/", text)

    def test_agents_md_managed_block_is_refreshed_not_duplicated(self) -> None:
        self.create()
        path = self.repo / "AGENTS.md"
        path.write_text(
            "# My own notes\n\nKeep this.\n\n" + path.read_text(encoding="utf-8"),
            encoding="utf-8",
        )

        self.service.attach(str(self.repo))

        text = path.read_text(encoding="utf-8")
        self.assertIn("Keep this.", text)
        self.assertEqual(text.count(agent_skills.MANAGED_BEGIN), 1)

    def test_agents_md_is_indexed_as_memory(self) -> None:
        self.create()
        memories = [i for i in self.service.list_agent_resources() if i.kind == "memory"]
        self.assertIn("AGENTS.md", {item.path for item in memories})


class SkillUpdatePolicyTests(_ScaffoldTestCase):
    def test_unmodified_skill_is_refreshed_when_the_app_ships_a_new_version(self) -> None:
        bundle = self.make_fake_bundle("original")
        with mock.patch.object(agent_skills, "bundled_agent_skills_dir", return_value=bundle):
            self.create()
            installed = self.repo / ".agents" / "skills" / "demo-skill" / "SKILL.md"
            self.assertIn("original", installed.read_text(encoding="utf-8"))
            # `create` scaffolds and then attaches, so the second pass reports
            # `unchanged` rather than `installed`. Either means it is ours.
            self.assertIn(
                self.report_status("demo-skill"), {STATUS_INSTALLED, STATUS_UNCHANGED}
            )

            self.make_fake_bundle("updated")
            self.service.attach(str(self.repo))

            self.assertIn("updated", installed.read_text(encoding="utf-8"))
            self.assertEqual(self.report_status("demo-skill"), STATUS_REFRESHED)

    def test_user_edited_skill_is_left_alone(self) -> None:
        bundle = self.make_fake_bundle("original")
        with mock.patch.object(agent_skills, "bundled_agent_skills_dir", return_value=bundle):
            self.create()
            installed = self.repo / ".agents" / "skills" / "demo-skill" / "SKILL.md"
            installed.write_text("---\nname: demo-skill\ndescription: Mine.\n---\n\nmine\n", encoding="utf-8")

            self.make_fake_bundle("updated")
            self.service.attach(str(self.repo))

            self.assertIn("mine", installed.read_text(encoding="utf-8"))
            self.assertNotIn("updated", installed.read_text(encoding="utf-8"))
            self.assertEqual(self.report_status("demo-skill"), STATUS_DIVERGED)

    def test_an_edit_to_a_reference_file_also_counts_as_divergence(self) -> None:
        bundle = self.make_fake_bundle("original")
        with mock.patch.object(agent_skills, "bundled_agent_skills_dir", return_value=bundle):
            self.create()
            notes = self.repo / ".agents" / "skills" / "demo-skill" / "references" / "notes.md"
            notes.write_text("my own notes\n", encoding="utf-8")

            self.make_fake_bundle("updated")
            self.service.attach(str(self.repo))

            self.assertEqual(notes.read_text(encoding="utf-8"), "my own notes\n")
            self.assertEqual(self.report_status("demo-skill"), STATUS_DIVERGED)

    def test_a_pre_existing_skill_of_the_same_name_is_never_replaced(self) -> None:
        bundle = self.make_fake_bundle("original")
        pre_existing = self.repo / ".agents" / "skills" / "demo-skill"
        pre_existing.mkdir(parents=True)
        (pre_existing / "SKILL.md").write_text(
            "---\nname: demo-skill\ndescription: Theirs.\n---\n\ntheirs\n", encoding="utf-8"
        )

        with mock.patch.object(agent_skills, "bundled_agent_skills_dir", return_value=bundle):
            self.service.create(str(self.repo))

        self.assertIn("theirs", (pre_existing / "SKILL.md").read_text(encoding="utf-8"))
        self.assertEqual(self.report_status("demo-skill"), STATUS_UNKNOWN_PROVENANCE)


class SymlinkFallbackTests(_ScaffoldTestCase):
    def test_falls_back_to_copying_when_symlinks_are_unavailable(self) -> None:
        with mock.patch.object(os, "symlink", side_effect=OSError("no symlinks here")):
            self.create()

        expected = self.shipped_skill_names()
        for name in expected:
            link = self.repo / ".claude" / "skills" / name
            canonical = self.repo / ".agents" / "skills" / name
            self.assertTrue(link.is_dir(), name)
            self.assertFalse(link.is_symlink(), name)
            self.assertEqual(skill_tree_hash(link), skill_tree_hash(canonical))

        # Still exactly one resource per skill, from the canonical location.
        skills = [i for i in self.service.list_agent_resources() if i.kind == "skill"]
        self.assertEqual(len(skills), len(expected))

    def test_attach_still_succeeds_when_scaffolding_fails_entirely(self) -> None:
        with mock.patch.object(
            agent_skills, "sync_bundled_agent_skills", side_effect=OSError("disk full")
        ):
            status = self.service.create(str(self.repo))
        self.assertTrue(status.attached)
        self.assertTrue((self.repo / ".ra_repo" / "repository_state.json").is_file())

    def test_a_broken_symlink_is_repaired(self) -> None:
        self.create()
        link = self.repo / ".claude" / "skills" / "repo-attach-files"
        if not link.is_symlink():
            self.skipTest("symlinks unavailable on this platform")
        link.unlink()
        link.symlink_to(Path("..") / ".." / "nowhere", target_is_directory=True)
        self.assertFalse(link.exists())

        self.service.attach(str(self.repo))

        self.assertTrue(link.exists())
        self.assertEqual(
            link.resolve(), (self.repo / ".agents" / "skills" / "repo-attach-files").resolve()
        )


if __name__ == "__main__":
    unittest.main()
