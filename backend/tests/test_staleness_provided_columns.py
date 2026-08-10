"""A column the user filled in themselves can never go stale.

Staleness exists for values a model computed from a source's text: when a
blocked fetch's error page is later replaced by the real document, those answers
describe a Cloudflare notice and have to be recomputed.

A provided column is the opposite case. Its values were imported from the user's
own spreadsheet by `set_column_values` -- a collection date, the channel a link
came from -- and no amount of rebuilding the source text makes them wrong. The
danger is not the false alarm: it is the remedy. `ra where` hands back a
`run-column --scope all --confirm-overwrite`, so acting on a stale mark against
a provided column replaces the user's data with whatever the model invents for a
column that has no prompt to guide it.

Seen for real: importing three provided columns across 106 sources, then letting
the fetch's convert phase rebuild the text, left `ra where` reporting values
"computed from text that has since been rebuilt" before any column had ever run.
"""

from __future__ import annotations

import unittest

from backend.models.repository import RepositoryColumnConfig
from backend.workflow.staleness import computed_column_ids


def _column(column_id: str, prompt: str) -> RepositoryColumnConfig:
    return RepositoryColumnConfig(
        id=column_id, label=column_id, kind="custom", instruction_prompt=prompt
    )


class ComputedColumnTests(unittest.TestCase):
    def test_a_column_with_a_prompt_can_go_stale(self) -> None:
        configs = [_column("custom_org", "Classify the organization type.")]
        self.assertEqual(computed_column_ids(configs), {"custom_org"})

    def test_a_provided_column_never_goes_stale(self) -> None:
        configs = [_column("custom_origin", "")]
        self.assertEqual(computed_column_ids(configs), set())

    def test_a_whitespace_only_prompt_is_still_no_prompt(self) -> None:
        configs = [_column("custom_year", "   \n  ")]
        self.assertEqual(computed_column_ids(configs), set())

    def test_the_two_kinds_are_separated(self) -> None:
        configs = [
            _column("custom_org", "Classify the organization type."),
            _column("custom_origin", ""),
            _column("custom_year", ""),
            _column("custom_sector", "Which sector?"),
        ]
        self.assertEqual(computed_column_ids(configs), {"custom_org", "custom_sector"})

    def test_no_configs_marks_nothing(self) -> None:
        # A repository with no columns must not somehow mark everything stale.
        self.assertEqual(computed_column_ids([]), set())
        self.assertEqual(computed_column_ids(None), set())

    def test_a_mark_already_written_is_purged(self) -> None:
        """Existing repositories heal, rather than only new ones staying clean.

        The 7 bad marks this was found with were already on disk; a fix that
        only stopped new ones would have left `ra where` nagging about them and
        offering a remedy that overwrites imported data.
        """
        import inspect

        from backend.workflow import staleness

        source = inspect.getsource(staleness.mark_stale)
        self.assertIn("existing & computed", source)

    def test_snapshot_only_records_computed_columns(self) -> None:
        """The filter has to be in `snapshot`, not only in `mark_stale`.

        `snapshot` is what runs *before* a convert; if it records a provided
        column there, the comparison afterwards has already lost the
        distinction.
        """
        import inspect

        from backend.workflow import staleness

        source = inspect.getsource(staleness.snapshot)
        self.assertIn("computed_column_ids", source)
        self.assertIn("computed_column_ids", inspect.getsource(staleness.mark_stale))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
