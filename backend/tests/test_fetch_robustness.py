"""One bad source must not end a run, and page options must be context options.

Both cases here cost a real 101-source fetch. `capture_visual_pdf` asked for a
page with a `viewport`, which is a `Browser.new_page` option and not a
`BrowserContext.new_page` one, so every HTML source raised `TypeError` -- and
because the run loop had no guard around a single source, the first one took
the whole job down with it, reporting neither a success nor a failure for any
of the hundred behind it.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from backend.models.sources import SourceManifestRow
from backend.pipeline.source_downloader import (
    PlaywrightRenderer,
    SourceDownloadOrchestrator,
    SourceTarget,
)
from backend.storage.file_store import FileStore


class _FakePage:
    def __init__(self) -> None:
        self.closed = False
        # Set by the context, exactly as a real page inherits it.
        self.viewport_size = {"width": 1280, "height": 1024}

    def goto(self, url, **kwargs):
        return None

    def wait_for_timeout(self, ms):
        return None

    def emulate_media(self, **kwargs):
        return None

    def evaluate(self, script):
        return 2048

    def pdf(self, **kwargs):
        return b"%PDF-1.4 fake"

    def close(self):
        self.closed = True


class _FakeContext:
    """Mirrors the real signature: `BrowserContext.new_page()` takes no options."""

    def __init__(self) -> None:
        self.pages: list[_FakePage] = []

    def new_page(self):
        page = _FakePage()
        self.pages.append(page)
        return page


class VisualCaptureTests(unittest.TestCase):
    def _renderer(self) -> tuple[PlaywrightRenderer, _FakeContext]:
        renderer = PlaywrightRenderer()
        context = _FakeContext()
        # Non-None browser short-circuits `_ensure_started`, so no real Chromium.
        renderer._browser = object()
        renderer._context = context
        return renderer, context

    def test_capture_asks_the_context_for_a_page_with_no_options(self) -> None:
        renderer, context = self._renderer()

        pdf_bytes, error, _ = renderer.capture_visual_pdf("https://example.com/a")

        self.assertEqual(error, "")
        self.assertTrue(pdf_bytes.startswith(b"%PDF"))
        self.assertEqual(len(context.pages), 1)
        self.assertTrue(context.pages[0].closed)

    def test_render_and_capture_use_the_same_page_call(self) -> None:
        # The viewport belongs to the context, so neither caller may pass one.
        import inspect

        self.assertEqual(
            list(inspect.signature(PlaywrightRenderer._new_page).parameters),
            ["self"],
        )


class FailedRowTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="fetch-robustness-tests-")
        self.store = FileStore(base_dir=Path(self._tmp.name) / "app_data")
        self.job_id = self.store.create_job()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_an_unexpected_error_fails_one_source_and_names_the_cause(self) -> None:
        orchestrator = SourceDownloadOrchestrator(job_id=self.job_id, store=self.store)
        target = SourceTarget(
            id="000042",
            source_document_name="",
            citation_number="",
            original_url="https://example.com/boom",
        )

        row = orchestrator._failed_row_for(target, None, TypeError("viewport"))

        self.assertEqual(row.id, "000042")
        self.assertEqual(row.fetch_status, "failed")
        # The exception type and message have to survive: an internal_error is a
        # bug in the pipeline, and the manifest is where it gets diagnosed.
        self.assertIn("internal_error", row.error_message)
        self.assertIn("TypeError", row.error_message)
        self.assertIn("viewport", row.error_message)
        self.assertEqual(row.phase_metadata["fetch"].error_code, "internal_error")

    def test_an_existing_row_keeps_its_data_when_it_fails(self) -> None:
        orchestrator = SourceDownloadOrchestrator(job_id=self.job_id, store=self.store)
        target = SourceTarget(
            id="000042",
            source_document_name="",
            citation_number="",
            original_url="https://example.com/boom",
        )
        existing = SourceManifestRow(
            id="000042",
            original_url="https://example.com/boom",
            custom_fields={"custom_1": "Perplexity"},
        )

        row = orchestrator._failed_row_for(target, existing, RuntimeError("crash"))

        self.assertEqual(row.fetch_status, "failed")
        # A failed fetch must not discard values imported alongside the source.
        self.assertEqual(row.custom_fields, {"custom_1": "Perplexity"})

    def test_the_code_is_explained_by_triage(self) -> None:
        from backend.workflow.codes import CODE_TABLE

        meaning = CODE_TABLE["internal_error"]
        self.assertEqual(meaning.classification, "retryable")
        self.assertTrue(meaning.explanation)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
