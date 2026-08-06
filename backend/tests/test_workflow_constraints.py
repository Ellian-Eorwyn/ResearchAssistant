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

    def test_an_empty_answer_is_reported(self) -> None:
        """An empty answer also becomes the fallback, so it counts too."""
        from backend.storage.attached_repository import _coerce_column_output_value

        notes: list[str] = []
        self.assertEqual(_coerce_column_output_value("", self._constraint(), notes), "Not sure")
        self.assertEqual(notes, [""])

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
