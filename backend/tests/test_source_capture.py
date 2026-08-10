from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path

from backend.models.sources import SourceManifestRow, SourcePhaseMetadata
from backend.pipeline.fetch_verification import FETCH_VERIFICATION_VERSION
from backend.pipeline.source_capture import (
    MAX_UPLOAD_BYTES,
    CapturedArtifacts,
    UnsupportedManualUploadError,
    artifacts_from_uploaded_bytes,
)
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


class ReleasesBlockedPhaseSkipsTest(unittest.TestCase):
    """A capture that resolves a block must let the held-back phases run."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="capture-unblock-")
        root = Path(self.temp_dir.name)
        self.store = FileStore(base_dir=root / "data", sync_project_profiles=False)
        self.repository = AttachedRepositoryService(store=self.store)
        self.repository.create(str(root / "repo"))

        row = SourceManifestRow(
            id="000045",
            original_url="https://example.com/walled",
            title="Attention Required! | Cloudflare",
            fetch_status="blocked",
            fetch_verification="blocked_challenge",
            error_message="blocked_challenge: bot wall",
            notes="blocked_request",
        )
        # Exactly what `_skip_phase_for_blocked_fetch` leaves behind.
        for phase, field in (
            ("cleanup", "llm_cleanup_status"),
            ("title", "title_status"),
            ("catalog", "catalog_status"),
            ("summary", "summary_status"),
            ("rating", "rating_status"),
        ):
            setattr(row, field, "skipped_blocked_fetch")
            row.phase_metadata[phase] = SourcePhaseMetadata(
                phase=phase, status="skipped", error_code="blocked_fetch"
            )
        with self.repository._writer_lock():
            self.repository._save_state_locked(sources=[row], citations=[], imports=[])
            self.repository._rebuild_outputs_locked([row], [])

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _real_page(self) -> CapturedArtifacts:
        html = (
            "<html><head><title>Real Article</title></head><body><article><p>"
            + ("Real content about distributed energy resources. " * 60)
            + "</p></article></body></html>"
        )
        return artifacts_from_uploaded_bytes(
            content=html.encode("utf-8"),
            filename="saved.html",
            final_url="https://example.com/walled",
        )

    def _row(self) -> SourceManifestRow:
        return _load_source_rows(
            json.loads(
                (self.repository.path / ".ra_repo" / "repository_state.json").read_text(
                    encoding="utf-8"
                )
            )["sources"]
        )[0]

    def test_the_held_phases_are_reset_to_never_run(self) -> None:
        self.repository.capture_source_artifacts(source_id="000045", artifacts=self._real_page())
        row = self._row()
        self.assertEqual(row.fetch_status, "success")
        for field in ("llm_cleanup_status", "catalog_status", "summary_status", "rating_status"):
            self.assertEqual(getattr(row, field), "", f"{field} still holds the block's skip")
        for phase in ("cleanup", "catalog", "summary", "rating"):
            self.assertNotIn(phase, row.phase_metadata)

    def test_the_released_phases_are_reported_to_the_caller(self) -> None:
        """Silence here would tell the user nothing needs running."""
        result = self.repository.capture_source_artifacts(
            source_id="000045", artifacts=self._real_page()
        )
        self.assertEqual(
            sorted(result.staled_phases),
            ["catalog", "cleanup", "rating", "summary", "title"],
        )

    def test_the_extracted_title_replaces_the_walls_title(self) -> None:
        self.repository.capture_source_artifacts(source_id="000045", artifacts=self._real_page())
        row = self._row()
        self.assertEqual(row.title, "Real Article")
        self.assertEqual(row.title_status, "extracted")

    def test_a_capture_that_is_still_blocked_keeps_the_phases_held(self) -> None:
        html = (
            "<html><head><title>Just a moment...</title></head><body>"
            "<p>Verifying you are human. This may take a few seconds.</p>"
            "<p>Enable JavaScript and cookies to continue.</p></body></html>"
        )
        artifacts = artifacts_from_uploaded_bytes(
            content=html.encode("utf-8"), filename="wall.html"
        )
        result = self.repository.capture_source_artifacts(
            source_id="000045", artifacts=artifacts
        )
        self.assertEqual(result.status, "still_blocked")
        row = self._row()
        self.assertEqual(row.fetch_status, "blocked")
        self.assertEqual(row.summary_status, "skipped_blocked_fetch")


class ArtifactsFromUploadedBytesTest(unittest.TestCase):
    """The bytes -> artifacts step every manual route shares."""

    def test_html_is_extracted_with_its_title_and_canonical_url(self):
        html = (
            "<html><head><title>What Is A Virtual Power Plant</title>"
            '<link rel="canonical" href="https://example.com/vpp"></head>'
            "<body><article><p>"
            + ("A virtual power plant aggregates distributed resources. " * 60)
            + "</p></article></body></html>"
        )
        artifacts = artifacts_from_uploaded_bytes(
            content=html.encode("utf-8"), filename="saved.html", final_url="https://example.com/vpp"
        )
        self.assertEqual(artifacts.detected_type, "html")
        self.assertEqual(artifacts.content_type, "text/html")
        self.assertEqual(artifacts.title, "What Is A Virtual Power Plant")
        self.assertEqual(artifacts.canonical_url, "https://example.com/vpp")
        self.assertEqual(artifacts.fetch_method, "manual_upload")
        self.assertIn("virtual power plant", artifacts.markdown.lower())

    def test_markdown_is_taken_as_the_text_itself(self):
        artifacts = artifacts_from_uploaded_bytes(
            content=REAL_PAGE_MD.encode("utf-8"), filename="notes.md"
        )
        self.assertEqual(artifacts.detected_type, "document")
        self.assertEqual(artifacts.extraction_method, "manual_markdown")
        self.assertEqual(artifacts.markdown, REAL_PAGE_MD)

    def test_a_txt_file_is_treated_as_markdown(self):
        artifacts = artifacts_from_uploaded_bytes(
            content=REAL_PAGE_MD.encode("utf-8"), filename="notes.txt"
        )
        self.assertEqual(artifacts.detected_type, "document")
        self.assertTrue(artifacts.has_content())

    def test_a_saved_block_page_is_still_scored_as_blocked(self):
        """The regression that matters: saving the wall must not count as a fix."""
        html = (
            "<html><head><title>Just a moment...</title></head><body>"
            "<p>Verifying you are human. This may take a few seconds.</p>"
            "<p>Enable JavaScript and cookies to continue.</p>"
            "</body></html>"
        )
        artifacts = artifacts_from_uploaded_bytes(
            content=html.encode("utf-8"), filename="wall.html"
        )
        self.assertTrue(
            any(note.startswith("verify_") and note != "verify_ok" for note in artifacts.notes),
            artifacts.notes,
        )

    def test_a_real_page_is_noted_as_verified(self):
        html = "<html><head><title>Real</title></head><body><article><p>" + (
            "Substantial article text that reads like a document. " * 80
        ) + "</p></article></body></html>"
        artifacts = artifacts_from_uploaded_bytes(
            content=html.encode("utf-8"), filename="real.html"
        )
        self.assertIn("verify_ok", artifacts.notes)

    def test_mhtml_is_refused_with_its_own_guidance(self):
        with self.assertRaises(UnsupportedManualUploadError) as caught:
            artifacts_from_uploaded_bytes(content=b"From: <saved>", filename="page.mhtml")
        self.assertIn("MHTML", str(caught.exception))

    def test_an_unknown_extension_is_refused(self):
        with self.assertRaises(UnsupportedManualUploadError) as caught:
            artifacts_from_uploaded_bytes(content=b"MZ", filename="installer.exe")
        self.assertIn("Unsupported file type", str(caught.exception))

    def test_an_empty_file_is_refused(self):
        with self.assertRaises(UnsupportedManualUploadError):
            artifacts_from_uploaded_bytes(content=b"", filename="page.html")

    def test_an_oversized_file_is_refused(self):
        with self.assertRaises(UnsupportedManualUploadError) as caught:
            artifacts_from_uploaded_bytes(
                content=b"x" * (MAX_UPLOAD_BYTES + 1), filename="page.html"
            )
        self.assertIn("limit", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
