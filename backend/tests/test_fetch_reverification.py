from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from backend.models.sources import SourceManifestRow, SourcePhaseMetadata
from backend.pipeline.fetch_verification import FETCH_VERIFICATION_VERSION
from backend.pipeline.source_downloader import (
    mark_downstream_stale,
    mark_downstream_stale_for_blocked,
)
from backend.storage.attached_repository import AttachedRepositoryService, _load_source_rows
from backend.storage.file_store import FileStore

REDDIT_BLOCK_MD = (
    "You've been blocked by network security.\n"
    "If you think you've been blocked by mistake, file a ticket below and we'll "
    "look into it.\n"
    "File a ticket"
)

REAL_ARTICLE_MD = (
    "# Virtual Power Plants Explained\n\n"
    "## What They Are\n\n"
    + ("A virtual power plant aggregates distributed energy resources. " * 30)
    + "\n\n## How They Work\n\n"
    + ("Operators dispatch batteries and flexible loads together. " * 30)
    + "\n\n## Why It Matters\n\n"
    + ("Grid operators gain flexibility without new peaker plants. " * 30)
)


class MarkDownstreamStaleForBlockedTest(unittest.TestCase):
    def test_stales_phases_even_when_the_digest_is_unchanged(self):
        """The bytes never move during retro re-verification — only our reading of them."""
        digest = "abc123"
        row = SourceManifestRow(id="000001", rating_status="generated")
        row.phase_metadata["rating"] = SourcePhaseMetadata(
            phase="rating",
            status="completed",
            content_digest=digest,
            completed_at="2026-01-01T00:00:00+00:00",
        )

        # The digest-comparing version would clear staleness here, not set it.
        mark_downstream_stale(row, digest)
        self.assertFalse(row.phase_metadata["rating"].stale)

        staled = mark_downstream_stale_for_blocked(row)
        self.assertEqual(staled, ["rating"])
        self.assertTrue(row.phase_metadata["rating"].stale)
        self.assertEqual(row.phase_metadata["rating"].status, "stale")
        self.assertEqual(row.rating_status, "stale")

    def test_stales_a_phase_recorded_only_in_its_status_field(self):
        """Older rows carry a completed status with no phase metadata at all."""
        row = SourceManifestRow(id="000001", catalog_status="existing")
        self.assertNotIn("catalog", row.phase_metadata)

        staled = mark_downstream_stale_for_blocked(row)
        self.assertEqual(staled, ["catalog"])
        self.assertEqual(row.catalog_status, "stale")
        self.assertTrue(row.phase_metadata["catalog"].stale)

    def test_leaves_phases_that_never_produced_anything(self):
        row = SourceManifestRow(
            id="000001",
            catalog_status="not_requested",
            summary_status="",
            rating_status="skipped_llm_disabled",
        )
        self.assertEqual(mark_downstream_stale_for_blocked(row), [])
        self.assertEqual(row.catalog_status, "not_requested")
        self.assertEqual(row.rating_status, "skipped_llm_disabled")


class ReverifyFetchesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="reverify-test-")
        root = Path(self.temp_dir.name)
        self.store = FileStore(base_dir=root / "data", sync_project_profiles=False)
        self.repository = AttachedRepositoryService(store=self.store)
        self.repository.create(str(root / "repo"))
        self.repo_path = self.repository.path

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _seed(self, rows: list[SourceManifestRow], markdown: dict[str, str]) -> None:
        for source_id, text in markdown.items():
            target = self.repo_path / "sources" / source_id
            target.mkdir(parents=True, exist_ok=True)
            (target / f"{source_id}_clean.md").write_text(text, encoding="utf-8")

        state_path = self.repo_path / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["sources"] = [row.model_dump(mode="json") for row in rows]
        state_path.write_text(json.dumps(state), encoding="utf-8")

    def _reload(self) -> dict[str, SourceManifestRow]:
        state_path = self.repo_path / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        return {row.id: row for row in _load_source_rows(state.get("sources", []))}

    def test_flips_a_false_success_to_blocked_and_stales_its_llm_output(self):
        blocked = SourceManifestRow(
            id="000045",
            original_url="https://www.reddit.com/r/explainlikeimfive/comments/1j3k95m/",
            fetch_status="success",
            http_status=200,
            detected_type="html",
            title="Reddit",
            markdown_file="sources/000045/000045_clean.md",
            catalog_status="existing",
            rating_status="generated",
        )
        good = SourceManifestRow(
            id="000046",
            original_url="https://example.com/article",
            fetch_status="success",
            http_status=200,
            detected_type="html",
            title="Virtual Power Plants Explained",
            markdown_file="sources/000046/000046_clean.md",
            catalog_status="existing",
            rating_status="generated",
        )
        self._seed(
            [blocked, good],
            {"000045": REDDIT_BLOCK_MD, "000046": REAL_ARTICLE_MD},
        )

        result = self.repository.reverify_fetches(scope="all")

        self.assertEqual(result.checked_count, 2)
        self.assertEqual(result.blocked_count, 1)
        self.assertEqual([change.source_id for change in result.changes], ["000045"])

        rows = self._reload()
        self.assertEqual(rows["000045"].fetch_status, "blocked")
        self.assertEqual(rows["000045"].fetch_verification, "blocked_challenge")
        self.assertEqual(rows["000045"].catalog_status, "stale")
        self.assertEqual(rows["000045"].rating_status, "stale")

        # The genuine article must be scored but otherwise left alone.
        self.assertEqual(rows["000046"].fetch_status, "success")
        self.assertEqual(rows["000046"].fetch_verification, "ok")
        self.assertEqual(rows["000046"].catalog_status, "existing")
        self.assertEqual(rows["000046"].rating_status, "generated")

    def test_second_pass_skips_rows_already_scored(self):
        row = SourceManifestRow(
            id="000045",
            fetch_status="success",
            http_status=200,
            detected_type="html",
            markdown_file="sources/000045/000045_clean.md",
        )
        self._seed([row], {"000045": REDDIT_BLOCK_MD})

        self.assertFalse(self.repository.fetch_verification_is_current())
        first = self.repository.reverify_fetches(scope="all")
        self.assertEqual(first.checked_count, 1)
        self.assertTrue(self.repository.fetch_verification_is_current())

        second = self.repository.reverify_fetches(scope="all")
        self.assertEqual(second.checked_count, 0)

        forced = self.repository.reverify_fetches(scope="all", force=True)
        self.assertEqual(forced.checked_count, 1)

    def test_records_the_verifier_version_on_the_fetch_phase(self):
        row = SourceManifestRow(
            id="000045",
            fetch_status="success",
            http_status=200,
            detected_type="html",
            markdown_file="sources/000045/000045_clean.md",
        )
        self._seed([row], {"000045": REDDIT_BLOCK_MD})
        self.repository.reverify_fetches(scope="all")

        metadata = self._reload()["000045"].phase_metadata["fetch"]
        self.assertEqual(metadata.prompt_version, FETCH_VERIFICATION_VERSION)
        self.assertEqual(metadata.status, "failed")
        self.assertEqual(metadata.error_code, "blocked_challenge")

    def test_scope_selected_only_touches_the_named_rows(self):
        rows = [
            SourceManifestRow(
                id=source_id,
                fetch_status="success",
                http_status=200,
                detected_type="html",
                markdown_file=f"sources/{source_id}/{source_id}_clean.md",
            )
            for source_id in ("000045", "000046")
        ]
        self._seed(rows, {"000045": REDDIT_BLOCK_MD, "000046": REDDIT_BLOCK_MD})

        result = self.repository.reverify_fetches(scope="selected", source_ids=["000045"])
        self.assertEqual(result.checked_count, 1)

        reloaded = self._reload()
        self.assertEqual(reloaded["000045"].fetch_status, "blocked")
        self.assertEqual(reloaded["000046"].fetch_status, "success")

    def test_selected_scope_requires_ids(self):
        with self.assertRaises(ValueError):
            self.repository.reverify_fetches(scope="selected", source_ids=[])

    def test_manifest_gains_the_verification_column(self):
        row = SourceManifestRow(
            id="000045",
            fetch_status="success",
            http_status=200,
            detected_type="html",
            markdown_file="sources/000045/000045_clean.md",
        )
        self._seed([row], {"000045": REDDIT_BLOCK_MD})
        self.repository.reverify_fetches(scope="all")

        header = self.repository.manifest_csv_path().read_text(encoding="utf-8-sig").splitlines()[0]
        self.assertIn("fetch_verification", header)


class ManualFetchStatusOverrideTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="fetch-override-test-")
        root = Path(self.temp_dir.name)
        self.store = FileStore(base_dir=root / "data", sync_project_profiles=False)
        self.repository = AttachedRepositoryService(store=self.store)
        self.repository.create(str(root / "repo"))

        state_path = self.repository.path / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["sources"] = [
            SourceManifestRow(
                id="000045",
                fetch_status="blocked",
                fetch_verification="blocked_challenge",
            ).model_dump(mode="json")
        ]
        state_path.write_text(json.dumps(state), encoding="utf-8")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_a_false_positive_can_be_forced_back_to_success(self):
        self.repository.update_source(
            "000045",
            patch={"fetch_status": "success", "fetch_verification": "ok"},
        )
        state_path = self.repository.path / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        row = _load_source_rows(state["sources"])[0]
        self.assertEqual(row.fetch_status, "success")
        self.assertEqual(row.fetch_verification, "ok")

    def test_rejects_an_unknown_status(self):
        with self.assertRaises(ValueError):
            self.repository.update_source("000045", patch={"fetch_status": "nonsense"})


if __name__ == "__main__":
    unittest.main()
