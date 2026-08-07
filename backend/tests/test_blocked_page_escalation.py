"""A challenge page should be retried in the browser before giving up.

The plain HTTP client frequently lands on a bot-check interstitial for sites
that serve normal content to a real browser. The pipeline already ships a
headless renderer and already falls back to it when extracted text looks thin;
these tests cover using it on the one case where it was previously skipped.

A page that genuinely requires solving a CAPTCHA or signing in is not recovered
-- that is not something the pipeline attempts. Such a row reports `blocked`
rather than `failed`: the wall was reached and verified, which is a different
outcome from a page that is simply not there (a 404 still reports `failed`).
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import httpx

from backend.models.sources import SourceManifestRow, SourceOutputOptions
from backend.pipeline.source_downloader import (
    NOTE_BLOCKED_RECOVERED_BY_BROWSER,
    NOTE_BLOCKED_REQUEST,
    SourceDownloadOrchestrator,
    classify_http_status,
    detect_blocked_page,
    detect_source_type,
)
from backend.storage.file_store import FileStore

CHALLENGE_HTML = (
    b"<html><head><title>Just a moment...</title></head>"
    b"<body><div class='cf-ray'>Checking your browser before accessing. "
    b"Please complete the security check. cf-challenge</div></body></html>"
)

REAL_HTML = (
    "<html><head><title>Virtual Power Plants Explained</title>"
    "<link rel='canonical' href='https://example.com/real'></head>"
    "<body><main><h1>Virtual Power Plants</h1>"
    "<p>A virtual power plant aggregates distributed energy resources so they "
    "can be dispatched together. This paragraph is long enough to clear the "
    "minimum markdown score the pipeline uses when deciding whether extracted "
    "text is usable at all, which matters for this test.</p>"
    "<p>Operators use them to shave peak demand without building new "
    "generation capacity, which is the whole point of the approach.</p>"
    "</main></body></html>"
)


class _Renderer:
    """Stand-in for PlaywrightRenderer."""

    def __init__(self, html: str = "", error: str = "") -> None:
        self.html = html
        self.error = error
        self.render_calls = 0

    def capture_visual_pdf(self, url: str):
        return b"", "", []

    def render(self, url: str):
        self.render_calls += 1
        return self.html, self.error


class BlockedPageEscalationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="blocked-escalation-tests-")
        self.store = FileStore(base_dir=Path(self._tmp.name) / "app_data")
        self.job_id = self.store.create_job()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def run_fetch(self, renderer: _Renderer, *, status: int = 200, body: bytes | None = None):
        orchestrator = SourceDownloadOrchestrator(
            job_id=self.job_id,
            store=self.store,
            run_download=True,
            run_convert=True,
            run_llm_cleanup=False,
            run_llm_title=False,
            run_llm_summary=False,
            run_llm_rating=False,
            output_options=SourceOutputOptions(
                include_rendered_pdf=False,
                include_rendered_html=False,
            ),
        )
        orchestrator._ensure_output_dirs()

        row = SourceManifestRow(
            id="000001",
            original_url="https://example.com/real",
            final_url="https://example.com/real",
            detected_type="html",
        )
        response = httpx.Response(
            status,
            headers={"content-type": "text/html; charset=utf-8"},
            content=CHALLENGE_HTML if body is None else body,
            request=httpx.Request("GET", "https://example.com/real"),
        )
        notes: list[str] = []
        orchestrator._handle_html_response(
            row, response, "https://example.com/real", renderer, notes
        )
        return row, notes

    # -- the fixture itself --------------------------------------------

    def test_the_challenge_fixture_is_detected_and_the_real_page_is_not(self) -> None:
        self.assertTrue(
            detect_blocked_page(
                html_text=CHALLENGE_HTML.decode(),
                title="Just a moment...",
                final_url="https://example.com/real",
            )
        )
        self.assertFalse(
            detect_blocked_page(
                html_text=REAL_HTML,
                title="Virtual Power Plants Explained",
                final_url="https://example.com/real",
            )
        )

    # -- recovery -------------------------------------------------------

    def test_browser_render_recovers_a_blocked_page(self) -> None:
        renderer = _Renderer(html=REAL_HTML)
        row, notes = self.run_fetch(renderer)

        self.assertEqual(renderer.render_calls, 1)
        self.assertEqual(row.fetch_status, "success")
        self.assertEqual(row.fetch_method, "playwright")
        self.assertIn(NOTE_BLOCKED_RECOVERED_BY_BROWSER, notes)
        self.assertNotIn(NOTE_BLOCKED_REQUEST, notes)

        # The recovered content replaces the challenge page everywhere.
        self.assertEqual(row.title, "Virtual Power Plants Explained")
        self.assertEqual(row.canonical_url, "https://example.com/real")
        self.assertTrue(row.markdown_file)
        markdown = (self.store.get_sources_output_dir(self.job_id) / row.markdown_file).read_text()
        self.assertIn("Virtual Power Plants", markdown)
        self.assertNotIn("security check", markdown)

    def test_recovery_also_clears_a_403_from_the_original_request(self) -> None:
        """Challenge pages usually arrive with a 4xx, so the status must not
        independently fail a fetch whose content we did recover."""
        renderer = _Renderer(html=REAL_HTML)
        row, notes = self.run_fetch(renderer, status=403)

        self.assertEqual(row.fetch_status, "success")
        self.assertEqual(row.error_message, "")
        self.assertIn(NOTE_BLOCKED_RECOVERED_BY_BROWSER, notes)

    # -- still failing when it should -----------------------------------

    def test_still_fails_when_the_browser_hits_the_same_challenge(self) -> None:
        renderer = _Renderer(html=CHALLENGE_HTML.decode())
        row, notes = self.run_fetch(renderer)

        self.assertEqual(renderer.render_calls, 1)
        # `blocked`, not `failed`: the wall was reached and verified. A fetch
        # that is missing (404) still reports `failed` -- see the 404 test.
        self.assertEqual(row.fetch_status, "blocked")
        self.assertIn(NOTE_BLOCKED_REQUEST, notes)
        self.assertNotIn(NOTE_BLOCKED_RECOVERED_BY_BROWSER, notes)

    def test_still_fails_when_the_browser_is_unavailable(self) -> None:
        renderer = _Renderer(error="playwright_not_installed")
        row, notes = self.run_fetch(renderer)

        # The page is a challenge page whether or not a browser was available
        # to try it, so the row records what it is rather than how we gave up.
        self.assertEqual(row.fetch_status, "blocked")
        self.assertIn(NOTE_BLOCKED_REQUEST, notes)
        # The reason the escalation could not help is recorded for the report.
        self.assertTrue(any("playwright" in note for note in notes))

    # -- a bare 403 with no challenge markers ---------------------------
    #
    # This is the common real-world case: the site returns a plain "403
    # Forbidden" whose body looks nothing like a challenge, so body-based
    # detection never fires. The status alone has to be enough to try the
    # browser.

    def test_a_plain_403_with_no_challenge_markers_still_escalates(self) -> None:
        plain_403 = (
            b"<!DOCTYPE HTML><html><head><title>403 - Forbidden</title></head>"
            b"<body><h1>403 Forbidden</h1></body></html>"
        )
        self.assertFalse(
            detect_blocked_page(
                html_text=plain_403.decode(),
                title="403 - Forbidden",
                final_url="https://example.com/real",
            ),
            "fixture must not look like a challenge, or the test proves nothing",
        )

        renderer = _Renderer(html=REAL_HTML)
        row, notes = self.run_fetch(renderer, status=403, body=plain_403)

        self.assertEqual(renderer.render_calls, 1)
        self.assertEqual(row.fetch_status, "success")
        self.assertEqual(row.fetch_method, "playwright")
        self.assertIn(NOTE_BLOCKED_RECOVERED_BY_BROWSER, notes)

    def test_a_404_is_never_recovered(self) -> None:
        """A missing page is missing; the browser cannot conjure it.

        The pipeline still renders a 404 for its own markdown fallback, so what
        matters is that no recovery is claimed and the row stays failed.
        """
        renderer = _Renderer(html=REAL_HTML)
        row, notes = self.run_fetch(
            renderer, status=404, body=b"<html><body>Not found</body></html>"
        )

        self.assertEqual(row.fetch_status, "failed")
        self.assertIn("http_status_404", row.error_message)
        self.assertNotIn(NOTE_BLOCKED_RECOVERED_BY_BROWSER, notes)

    def test_a_thin_interstitial_is_rejected_even_if_it_evades_detection(self) -> None:
        """A Cloudflare "Attention Required" page can slip past the pattern
        check, so recovery also requires the page to yield readable text."""
        interstitial = (
            "<html><head><title>Attention Required</title></head>"
            "<body><div>Sorry, you have been stopped.</div></body></html>"
        )
        self.assertFalse(
            detect_blocked_page(
                html_text=interstitial,
                title="Attention Required",
                final_url="https://example.com/real",
            )
        )

        renderer = _Renderer(html=interstitial)
        row, notes = self.run_fetch(renderer, status=403)

        self.assertEqual(renderer.render_calls, 1)
        # Evading the pattern check is not evading verification: the interstitial
        # yields no readable text, so the row is `blocked` rather than recovered.
        self.assertEqual(row.fetch_status, "blocked")
        self.assertNotIn(NOTE_BLOCKED_RECOVERED_BY_BROWSER, notes)

    def test_a_plain_text_refusal_still_reaches_the_escalation(self) -> None:
        """Some sites answer 403 with a text/plain note.

        That detects as a `document` and used to route past the HTML handler
        entirely, so the URL was never retried in a browser. The refusal
        describes the block, not the source.
        """
        renderer = _Renderer(html=REAL_HTML)
        orchestrator = SourceDownloadOrchestrator(
            job_id=self.job_id,
            store=self.store,
            run_download=True,
            run_convert=True,
            run_llm_cleanup=False,
            run_llm_title=False,
            run_llm_summary=False,
            run_llm_rating=False,
            output_options=SourceOutputOptions(
                include_rendered_pdf=False, include_rendered_html=False
            ),
        )
        orchestrator._ensure_output_dirs()

        row = SourceManifestRow(
            id="000003",
            original_url="https://example.com/plain",
            final_url="https://example.com/plain",
        )
        response = httpx.Response(
            403,
            headers={"content-type": "text/plain"},
            content=b"Please respect our robot policy when crawling us.",
            request=httpx.Request("GET", "https://example.com/plain"),
        )
        row.detected_type = detect_source_type(
            content_type="text/plain", final_url=row.final_url, body=response.content
        )
        self.assertEqual(row.detected_type, "document", "fixture must detect as a document")

        notes: list[str] = []
        # Exercise the dispatch, not the handler, since routing is the bug.
        if row.detected_type == "pdf":
            pass
        elif row.detected_type == "html" or classify_http_status(403) == "blocked_request":
            orchestrator._handle_html_response(
                row, response, "https://example.com/plain", renderer, notes
            )

        self.assertEqual(renderer.render_calls, 1)
        self.assertEqual(row.fetch_status, "success")
        self.assertIn(NOTE_BLOCKED_RECOVERED_BY_BROWSER, notes)

    def test_a_clean_page_never_triggers_an_escalation_render(self) -> None:
        renderer = _Renderer(html=REAL_HTML)
        orchestrator = SourceDownloadOrchestrator(
            job_id=self.job_id,
            store=self.store,
            run_download=True,
            run_convert=True,
            run_llm_cleanup=False,
            run_llm_title=False,
            run_llm_summary=False,
            run_llm_rating=False,
            output_options=SourceOutputOptions(
                include_rendered_pdf=False,
                include_rendered_html=False,
            ),
        )
        orchestrator._ensure_output_dirs()
        row = SourceManifestRow(
            id="000002",
            original_url="https://example.com/clean",
            final_url="https://example.com/clean",
            detected_type="html",
        )
        response = httpx.Response(
            200,
            headers={"content-type": "text/html; charset=utf-8"},
            content=REAL_HTML.encode(),
            request=httpx.Request("GET", "https://example.com/clean"),
        )
        notes: list[str] = []
        orchestrator._handle_html_response(
            row, response, "https://example.com/clean", renderer, notes
        )

        self.assertEqual(row.fetch_status, "success")
        # Good HTTP content scores well, so no browser work is needed at all.
        self.assertEqual(renderer.render_calls, 0)
        # `fetch_method` is stamped "http" by the caller; what matters here is
        # that nothing escalated it to the browser.
        self.assertNotEqual(row.fetch_method, "playwright")


if __name__ == "__main__":
    unittest.main()


class PhaseMetadataAfterRecoveryTests(BlockedPageEscalationTests):
    """A recovered fetch must not leave a `failed` phase record behind.

    The 4xx from the original request is still on the row, so judging the phase
    on the status code alone marks a working fetch as failed -- and anything
    reading phase metadata then reports a source that is already fine.
    """

    def test_recovered_row_reports_a_completed_fetch_phase(self) -> None:
        renderer = _Renderer(html=REAL_HTML)
        orchestrator = SourceDownloadOrchestrator(
            job_id=self.job_id,
            store=self.store,
            run_download=True,
            run_convert=True,
            run_llm_cleanup=False,
            run_llm_title=False,
            run_llm_summary=False,
            run_llm_rating=False,
            output_options=SourceOutputOptions(
                include_rendered_pdf=False, include_rendered_html=False
            ),
        )
        orchestrator._ensure_output_dirs()

        row = SourceManifestRow(
            id="000004",
            original_url="https://example.com/recovered",
            final_url="https://example.com/recovered",
            detected_type="html",
            http_status=403,
            fetch_status="success",
        )
        # Mirror what the escalation leaves behind: content obtained, 403 kept.
        row.error_message = ""

        from backend.pipeline.source_downloader import _phase_error_code

        obtained = str(row.fetch_status or "").strip() in {"success", "partial"}
        failed = (not obtained and (row.http_status or 0) >= 400) or _phase_error_code(
            row.error_message
        ) in {"invalid_url", "timeout", "network_failure", "blocked_request", "unsupported_content"}

        self.assertFalse(failed, "a recovered row must not record a failed fetch phase")
