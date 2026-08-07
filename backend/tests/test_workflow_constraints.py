"""Reading allowed answers out of a prompt must be literal, never clever.

A false positive here is worse than a miss: a value wrongly ruled out is
silently replaced by the fallback, so a column fills with plausible wrong
answers. Every negative case below is a real prompt from the planning
spreadsheet that must be left alone.
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from backend.workflow.constraints import derive_constraint, extract_allowed_values

ORG_TYPE = """Classify the type of organization that published this source. \
Return exactly one of these values, spelled exactly as shown:

Utility
For-profit
Government
Academic
Non-profit
Public-private-partnership
News/media
Private individuals
Not sure

Critical distinction: code the SOURCE, not its subject matter. Many of these
pages describe a VPP program run by some other organization.
"""

YES_NO = """Record whether this source includes any static visual.
Return exactly one of these values, spelled exactly as shown:

Yes
No
Not Sure

Important limitation: the extracted document text does not carry images.
"""

SEMICOLON_LIST = """Flag whether this source meets any of the exclusion criteria. \
Return a semicolon-separated list of the flags that apply, using exactly these
tokens and this order:

no-vpp-mention
course-or-training-ad
paywalled
url-unresponsive

If none apply, return exactly: none
"""

FREE_TEXT = """Write one citation for this source and return only that citation string.

Use exactly this shape, including the punctuation:
Author. (Year, Month if provided). "Title." Source/Publication. URL.
"""

NUMBERED_STEPS = """Classify the dominant format of this source.
Return exactly one of these values, spelled exactly as shown:

report
video
not sure

Work through these tests in order and stop at the first that fits:
1. video — the source is a video with no substantial written content.
"""


class ExtractionTests(unittest.TestCase):
    def test_reads_a_literal_list(self) -> None:
        self.assertEqual(
            extract_allowed_values(ORG_TYPE),
            [
                "Utility",
                "For-profit",
                "Government",
                "Academic",
                "Non-profit",
                "Public-private-partnership",
                "News/media",
                "Private individuals",
                "Not sure",
            ],
        )

    def test_stops_at_the_prose_that_follows(self) -> None:
        self.assertEqual(extract_allowed_values(YES_NO), ["Yes", "No", "Not Sure"])

    def test_stops_before_numbered_instructions(self) -> None:
        self.assertEqual(extract_allowed_values(NUMBERED_STEPS), ["report", "video", "not sure"])

    def test_a_multi_answer_prompt_is_left_alone(self) -> None:
        """A semicolon-separated list must never be pinned to one value."""
        self.assertEqual(extract_allowed_values(SEMICOLON_LIST), [])
        self.assertIsNone(derive_constraint(SEMICOLON_LIST))

    def test_free_text_is_left_alone(self) -> None:
        self.assertEqual(extract_allowed_values(FREE_TEXT), [])
        self.assertIsNone(derive_constraint(FREE_TEXT))

    def test_no_anchor_means_no_constraint(self) -> None:
        self.assertIsNone(derive_constraint("Return the publication year, e.g. 2024."))
        self.assertIsNone(derive_constraint(""))

    def test_a_single_value_is_not_a_list(self) -> None:
        prompt = "Return exactly one of these values, spelled exactly as shown:\n\nYes\n\nDone.\n"
        self.assertEqual(extract_allowed_values(prompt), [])


class FallbackTests(unittest.TestCase):
    def test_fallback_keeps_the_authors_own_spelling(self) -> None:
        """`Not sure`, `Not Sure` and `not sure` all occur across the sheet."""
        self.assertEqual(derive_constraint(ORG_TYPE)["fallback_value"], "Not sure")
        self.assertEqual(derive_constraint(YES_NO)["fallback_value"], "Not Sure")
        self.assertEqual(derive_constraint(NUMBERED_STEPS)["fallback_value"], "not sure")

    def test_a_list_without_a_not_sure_option_gets_no_fallback(self) -> None:
        prompt = "Return exactly one of these values, spelled exactly as shown:\n\nYes\nNo\n"
        self.assertEqual(derive_constraint(prompt)["fallback_value"], "")

    def test_constraint_is_the_shape_the_repository_stores(self) -> None:
        from backend.models.repository import RepositoryColumnOutputConstraint

        constraint = RepositoryColumnOutputConstraint.model_validate(derive_constraint(ORG_TYPE))
        self.assertIn("Utility", constraint.allowed_values)
        self.assertEqual(constraint.kind, "text")


class CoercionTests(unittest.TestCase):
    """The two cells the first real column run got wrong, as regression tests."""

    def _coerce(self, value: str):
        from backend.models.repository import RepositoryColumnOutputConstraint
        from backend.storage.attached_repository import _coerce_column_output_value

        constraint = RepositoryColumnOutputConstraint.model_validate(derive_constraint(ORG_TYPE))
        return _coerce_column_output_value(value, constraint)

    def test_the_status_marker_no_longer_reaches_the_cell(self) -> None:
        """A model that puts its status in `value` used to have it stored verbatim."""
        self.assertEqual(self._coerce("insufficient_evidence"), "Not sure")

    def test_an_empty_answer_becomes_the_fallback(self) -> None:
        self.assertEqual(self._coerce(""), "Not sure")

    def test_canonical_spelling_is_restored(self) -> None:
        self.assertEqual(self._coerce("non-profit"), "Non-profit")

    def test_a_valid_answer_is_kept(self) -> None:
        self.assertEqual(self._coerce("Utility"), "Utility")


class RowContextTests(unittest.TestCase):
    """A prompt that reads row metadata must be sent row metadata.

    12 of the 14 prompts in the first real spreadsheet told the model to use
    fields like `discovered_media_urls`, while `include_row_context` defaulted
    to False and the metadata block was empty -- so those instructions referred
    to nothing, and six columns could not work at all.
    """

    def test_the_phrase_itself_is_enough(self) -> None:
        from backend.workflow.constraints import needs_row_context

        self.assertTrue(needs_row_context("Prefer the value in the row metadata."))

    def test_a_named_manifest_field_is_enough(self) -> None:
        from backend.workflow.constraints import needs_row_context

        self.assertTrue(needs_row_context("Return `ocr_pdf_file` exactly as given."))
        self.assertTrue(needs_row_context("If `discovered_media_count` is 1 or greater, answer Yes."))

    def test_a_backticked_word_that_is_not_a_field_is_not(self) -> None:
        from backend.workflow.constraints import needs_row_context

        self.assertFalse(needs_row_context("Answer `Yes` or `No`, spelled exactly."))
        self.assertFalse(needs_row_context(ORG_TYPE.replace("row metadata", "the document")))

    def test_file_paths_reach_a_prompt_that_asks_for_them(self) -> None:
        """`Source PDF` asks for raw_file and rendered_pdf_file by name."""
        import inspect

        from backend.storage.attached_repository import (
            AttachedRepositoryService as Service,
        )

        source = inspect.getsource(Service._generate_column_value_for_row)
        excluded = source.split("row_metadata = {")[1].split("}")[0]
        for field in ("raw_file", "rendered_pdf_file", "ocr_pdf_file"):
            self.assertNotIn(
                f'"{field}"',
                excluded,
                f"{field} is stripped from row metadata, so a prompt naming it cannot work.",
            )


class BlockedPageTests(unittest.TestCase):
    """Two block pages returned HTTP 200 and were stored as successful fetches.

    Their text then went on to be analysed as though it were the document, and
    the column dutifully answered "Not sure".
    """

    def _detect(self, text: str) -> bool:
        from backend.pipeline.source_downloader import detect_blocked_page

        return detect_blocked_page(text, "", "")

    def test_catches_the_two_that_got_through(self) -> None:
        self.assertTrue(
            self._detect(
                "You've been blocked by network security.\nIf you think you've been "
                "blocked by mistake, file a ticket below.\nFile a ticket"
            )
        )
        self.assertTrue(
            self._detect(
                "This website is using a security service to protect itself from online "
                "attacks. The action you just performed triggered the security solution. "
                "Cloudflare Ray ID: 8ab12"
            )
        )

    def test_a_block_page_is_caught_in_its_raw_html(self) -> None:
        """This runs on raw HTML, not extracted text.

        A first attempt made a long page need an extra signal, which read
        sensibly against extracted text and silently stopped catching real block
        pages -- they are full HTML documents, scripts and all.
        """
        html = (
            "<html><head><title>Blocked</title></head><body>"
            + "<div>nav</div>" * 300
            + "You've been blocked by network security. File a ticket below."
            + "<script>x</script>" * 200
            + "</body></html>"
        )
        self.assertTrue(self._detect(html))

    def test_block_text_past_the_sample_window_is_still_caught(self) -> None:
        """A rendered page can carry 20k of scripts before its body.

        That is exactly how a Reddit block page reached the analysis stage
        marked `success`: the phrase sat past the sampled prefix.
        """
        html = "<script>" + ("x" * 60000) + "</script>You've been blocked by network security."
        self.assertTrue(self._detect(html))

    def test_a_long_article_about_blocking_is_not_a_block_page(self) -> None:
        """The reason length raises the bar rather than lowering it."""
        text = (
            ("An article about web security infrastructure. " * 40)
            + " A Cloudflare Ray ID appears when you have been blocked by a WAF. "
            + ("More prose about the topic. " * 200)
        )
        self.assertFalse(self._detect(text))

    def test_a_short_ordinary_page_is_not_a_block_page(self) -> None:
        self.assertFalse(self._detect("A short page about solar batteries in Victoria."))

    def test_reddit_falls_back_to_its_own_legacy_interface(self) -> None:
        from backend.pipeline.source_downloader import alternate_urls

        self.assertEqual(
            alternate_urls("https://www.reddit.com/r/explainlikeimfive/comments/1j3k95m/eli5/"),
            ["https://old.reddit.com/r/explainlikeimfive/comments/1j3k95m/eli5/"],
        )
        self.assertEqual(
            alternate_urls("http://reddit.com/r/x/"), ["https://old.reddit.com/r/x/"]
        )

    def test_a_site_with_no_published_alternate_gets_none(self) -> None:
        from backend.pipeline.source_downloader import alternate_urls

        for url in (
            "https://emp.lbl.gov/publications/virtual-power-plants-insights",
            "https://old.reddit.com/r/x/",  # already the alternate
            "",
        ):
            self.assertEqual(alternate_urls(url), [], url)

    def test_alternates_are_first_party_addresses_only(self) -> None:
        """The line this must not cross.

        An alternate is a public address the site serves itself. A third-party
        mirror, proxy, or cache would be an evasion, which this project does not
        implement -- a refused alternate goes to manual download like anything
        else.
        """
        from urllib.parse import urlsplit

        from backend.pipeline.source_downloader import ALTERNATE_URL_FORMS

        for pattern, replacement in ALTERNATE_URL_FORMS:
            host = urlsplit(replacement).hostname or ""
            registrable = ".".join(host.split(".")[-2:])
            matched = pattern.pattern.replace("\\", "")  # the pattern escapes its dots
            self.assertIn(
                registrable,
                matched,
                f"{replacement} is not on the same site as the address it replaces.",
            )

    def test_the_fallback_render_is_checked_for_a_block_page(self) -> None:
        """A JavaScript-gated block only appears once a real browser loads it.

        The plain fetch returns an empty shell that reads as merely thin, so
        without this the refusal is stored as a successful fetch. Detection
        alone is not enough -- it has to be called on the rendered page.
        """
        import inspect

        from backend.pipeline import source_downloader

        body = inspect.getsource(source_downloader)
        rendered_branch = body.split("elif rendered_html:")[1].split("self._collect_media_links")[0]
        self.assertIn(
            "detect_blocked_page",
            rendered_branch,
            "The fallback render is no longer checked for a block page.",
        )


class SubstitutionReportingTests(unittest.TestCase):
    """A stored fallback must be distinguishable from a chosen answer.

    Both write the same string into the cell. The first real run of this column
    produced 14 'Not sure' values out of 42 with no way to tell how many the
    model actually chose, which is the difference between a coded column and one
    that only looks coded.
    """

    def _constraint(self):
        from backend.models.repository import RepositoryColumnOutputConstraint

        return RepositoryColumnOutputConstraint.model_validate(derive_constraint(ORG_TYPE))

    def test_an_answer_outside_the_list_is_reported(self) -> None:
        from backend.storage.attached_repository import _coerce_column_output_value

        notes: list[str] = []
        value = _coerce_column_output_value("Public Private Partnership", self._constraint(), notes)
        self.assertEqual(value, "Not sure")
        self.assertEqual(notes, ["Public Private Partnership"])

    def test_a_chosen_answer_is_not_reported(self) -> None:
        from backend.storage.attached_repository import _coerce_column_output_value

        notes: list[str] = []
        self.assertEqual(_coerce_column_output_value("Not sure", self._constraint(), notes), "Not sure")
        self.assertEqual(notes, [], "the model chose this value; it was not substituted")

    def test_an_empty_answer_is_reported_when_a_fallback_replaces_it(self) -> None:
        from backend.storage.attached_repository import _coerce_column_output_value

        notes: list[str] = []
        self.assertEqual(_coerce_column_output_value("", self._constraint(), notes), "Not sure")
        self.assertEqual(notes, [""])

    def test_an_empty_answer_is_not_reported_when_empty_is_the_answer(self) -> None:
        """`Video link (URL)` says to return an empty value when there is no video.

        27 of its 42 rows were correctly blank, and counting each as a
        substitution made a column with zero misses and zero inventions look
        like it needed checking.
        """
        from backend.models.repository import RepositoryColumnOutputConstraint
        from backend.storage.attached_repository import _coerce_column_output_value

        free_text = RepositoryColumnOutputConstraint(kind="text", fallback_value="")
        notes: list[str] = []
        self.assertEqual(_coerce_column_output_value("", free_text, notes), "")
        self.assertEqual(notes, [], "empty is this column's correct answer, not a decline")

    def test_update_source_cannot_carry_the_stale_mark(self) -> None:
        """Why the mark is cleared in the run's own state write, not via a patch.

        `update_source` applies only its whitelisted field groups and silently
        drops anything else, so clearing the mark through it looked correct and
        did nothing.
        """
        import inspect

        from backend.storage.attached_repository import AttachedRepositoryService

        source = inspect.getsource(AttachedRepositoryService.update_source)
        self.assertNotIn(
            "stale_column_ids",
            source,
            "update_source now handles stale_column_ids; clear it there instead.",
        )

    def test_the_run_outcome_carries_the_count(self) -> None:
        from backend.workflow.models import RunOutcome

        outcome = RunOutcome(kind="column", coerced_rows=3)
        self.assertEqual(outcome.coerced_rows, 3)
        self.assertEqual(outcome.coercions, [])


if __name__ == "__main__":
    unittest.main()
