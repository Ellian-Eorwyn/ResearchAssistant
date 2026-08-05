from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path

from backend.models.sources import SourceManifestRow, SourcePhaseMetadata
from backend.pipeline.fetch_verification import FETCH_VERIFICATION_VERSION
from backend.pipeline.source_capture import CapturedArtifacts
from backend.storage.attached_repository import AttachedRepositoryService, _load_source_rows
from backend.storage.file_store import FileStore

REAL_PAGE_MD = (
    "# What Is A Virtual Power Plant\n\n"
    "## Definition\n\n"
    + ("A virtual power plant aggregates distributed energy resources. " * 25)
    + "\n\n## Operation\n\n"
    + ("Operators dispatch batteries and flexible loads together. " * 25)
    + "\n\n## Outlook\n\n"
    + ("Grid operators gain flexibility without new peaker plants. " * 25)
)

BLOCK_PAGE_MD = "Just a moment...\n\nVerifying you are human. This may take a few seconds."


class CaptureSourceArtifactsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="capture-test-")
        root = Path(self.temp_dir.name)
        self.store = FileStore(base_dir=root / "data", sync_project_profiles=False)
        self.repository = AttachedRepositoryService(store=self.store)
        self.repository.create(str(root / "repo"))
        self.repo_path = self.repository.path
        self._seed_blocked_row()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _seed_blocked_row(self) -> None:
        row = SourceManifestRow(
            id="000045",
            original_url="https://www.reddit.com/r/explainlikeimfive/comments/1j3k95m/",
            fetch_status="blocked",
            fetch_verification="blocked_challenge",
            error_message="blocked_challenge: Challenge or interstitial page detected",
            http_status=200,
            detected_type="html",
            title="Reddit",
            notes="queued_for_download; blocked_request; verify_blocked_challenge",
            markdown_file="sources/000045/000045_clean.md",
            markdown_char_count=143,
            catalog_status="existing",
            rating_status="generated",
        )
        row.phase_metadata["rating"] = SourcePhaseMetadata(
            phase="rating",
            status="completed",
            content_digest="old-digest",
            completed_at="2026-01-01T00:00:00+00:00",
        )
        target = self.repo_path / "sources" / "000045"
        target.mkdir(parents=True, exist_ok=True)
        (target / "000045_clean.md").write_text("You've been blocked.", encoding="utf-8")

        state_path = self.repo_path / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        state["sources"] = [row.model_dump(mode="json")]
        state_path.write_text(json.dumps(state), encoding="utf-8")

    def _row(self) -> SourceManifestRow:
        state_path = self.repo_path / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        return _load_source_rows(state["sources"])[0]

    def _good_capture(self) -> CapturedArtifacts:
        return CapturedArtifacts(
            raw_html="<html><title>ELI5: What is a VPP</title><body>real</body></html>",
            rendered_html="<html><title>ELI5: What is a VPP</title><body>real</body></html>",
            rendered_pdf=b"%PDF-1.4 fake",
            markdown=REAL_PAGE_MD,
            final_url="https://www.reddit.com/r/explainlikeimfive/comments/1j3k95m/",
            title="ELI5: What is a VPP",
            content_type="text/html",
            http_status=200,
            detected_type="html",
            extraction_method="rendered_html_manual",
        )

    def test_writes_artifacts_in_place_under_the_same_source_id(self):
        result = self.repository.capture_source_artifacts(
            source_id="000045", artifacts=self._good_capture()
        )

        self.assertEqual(result.status, "captured")
        self.assertEqual(result.source_id, "000045")
        for expected in (
            "sources/000045/000045_source.html",
            "sources/000045/000045_rendered.html",
            "sources/000045/000045_rendered.pdf",
            "sources/000045/000045_clean.md",
        ):
            self.assertIn(expected, result.written_files)
            self.assertTrue((self.repo_path / expected).exists(), expected)

        row = self._row()
        self.assertEqual(row.id, "000045")
        self.assertEqual(row.fetch_status, "success")
        self.assertEqual(row.fetch_verification, "ok")
        self.assertEqual(row.fetch_method, "manual_capture")
        self.assertEqual(row.title, "ELI5: What is a VPP")
        self.assertEqual(row.error_message, "")
        self.assertGreater(row.markdown_char_count, 1000)

        # The stored markdown really is the captured page, not the old wall.
        stored = (self.repo_path / "sources/000045/000045_clean.md").read_text(encoding="utf-8")
        self.assertIn("virtual power plant aggregates", stored)

    def test_records_the_fetch_phase_and_clears_the_block_notes(self):
        self.repository.capture_source_artifacts(
            source_id="000045", artifacts=self._good_capture()
        )
        row = self._row()

        metadata = row.phase_metadata["fetch"]
        self.assertEqual(metadata.status, "completed")
        self.assertEqual(metadata.error, "")
        self.assertEqual(metadata.prompt_version, FETCH_VERIFICATION_VERSION)

        self.assertNotIn("blocked_request", row.notes)
        self.assertNotIn("verify_blocked_challenge", row.notes)
        self.assertIn("manual_capture", row.notes)

    def test_stales_llm_output_derived_from_the_block_page(self):
        result = self.repository.capture_source_artifacts(
            source_id="000045", artifacts=self._good_capture()
        )
        self.assertIn("rating", result.staled_phases)
        row = self._row()
        self.assertEqual(row.rating_status, "stale")
        self.assertTrue(row.phase_metadata["rating"].stale)

    def test_a_captured_block_page_does_not_count_as_a_fix(self):
        artifacts = self._good_capture()
        artifacts.markdown = BLOCK_PAGE_MD
        artifacts.title = "Just a moment..."

        result = self.repository.capture_source_artifacts(
            source_id="000045", artifacts=artifacts
        )

        self.assertEqual(result.status, "still_blocked")
        row = self._row()
        self.assertEqual(row.fetch_status, "blocked")
        self.assertEqual(row.fetch_verification, "blocked_challenge")
        # A wall's title must not overwrite whatever we had.
        self.assertEqual(row.title, "Reddit")

    def test_refuses_while_a_repository_job_is_running(self):
        """A live job holds a stale row snapshot and would overwrite the capture."""
        stop = threading.Event()
        worker = threading.Thread(target=stop.wait, daemon=True)
        worker.start()
        self.repository._download_thread = worker
        try:
            with self.assertRaises(RuntimeError):
                self.repository.capture_source_artifacts(
                    source_id="000045", artifacts=self._good_capture()
                )
        finally:
            stop.set()
            worker.join(timeout=5)
            self.repository._download_thread = None

    def test_rejects_an_unknown_source(self):
        with self.assertRaises(ValueError):
            self.repository.capture_source_artifacts(
                source_id="999999", artifacts=self._good_capture()
            )

    def test_rejects_an_empty_capture(self):
        with self.assertRaises(ValueError):
            self.repository.capture_source_artifacts(
                source_id="000045", artifacts=CapturedArtifacts()
            )

    def test_manifest_is_rebuilt_with_the_new_values(self):
        self.repository.capture_source_artifacts(
            source_id="000045", artifacts=self._good_capture()
        )
        manifest = self.repository.manifest_csv_path().read_text(encoding="utf-8-sig")
        self.assertIn("fetch_verification", manifest.splitlines()[0])
        self.assertIn("manual_capture", manifest)


if __name__ == "__main__":
    unittest.main()
