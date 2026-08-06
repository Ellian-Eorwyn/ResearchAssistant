"""Tests for planning-spreadsheet parsing and encoding repair.

The fixture reproduces the awkward parts of a real exported sheet: a merged
banner row above the header, a `Prompts:` row between the header and the data,
data rows carrying only an id and a URL, and UTF-8 text that was read back as a
legacy codepage.
"""

from __future__ import annotations

import csv
import tempfile
import unittest
from pathlib import Path

from backend.workflow.encoding import count_suspicious, repair_grid, repair_text
from backend.workflow.sheet import (
    SheetReadError,
    parse_planning_sheet,
    read_grid,
    sheet_plan_to_create_columns_params,
    sheet_plan_to_create_sources_params,
)

# UTF-8 em dash read back as MacRoman, exactly as seen in the real sheet.
MOJIBAKE_EM_DASH = "‚Äî"

BANNER = ["", "", "", "", "Source attributes to scrape", "", "", ""]
HEADER = ["ID#", "URL", "Data Collection Year", "Source Origin", "Citation",
          "Year Published", "Org Type", "Location"]
PROMPTS = [
    "Prompts:", "", "", "",
    "Write one citation for this source and return only that citation string. "
    "Use exactly this shape, including the punctuation, and nothing else at all.",
    "Return the calendar year this document was published, as four characters, "
    "for example 2024, using the evidence in priority order given below.",
    "Classify the type of organization that published this source. Return "
    "exactly one of these values, spelled exactly as shown, and nothing else.",
    "Identify whether this source focuses on a particular geography, returning "
    "exactly one of the listed values and nothing else whatsoever.",
]


def data_row(source_id: str, url: str) -> list[str]:
    return [source_id, url, "", "", "", "", "", ""]


class _SheetTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="workflow-sheet-tests-")
        self.tmp = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def write_csv(self, rows: list[list[str]], name: str = "plan.csv") -> Path:
        path = self.tmp / name
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            csv.writer(handle).writerows(rows)
        return path

    def standard_sheet(self, *, extra_rows: list[list[str]] | None = None) -> Path:
        rows = [BANNER, HEADER, PROMPTS]
        rows += [
            data_row("20", "https://example.com/alpha"),
            data_row("21", "https://example.com/beta"),
            data_row("22", "https://www.youtube.com/watch?v=abc"),
        ]
        rows += extra_rows or []
        return self.write_csv(rows)


class LayoutDetectionTests(_SheetTestCase):
    def test_finds_header_prompts_and_data_past_a_banner_row(self) -> None:
        plan = parse_planning_sheet(self.standard_sheet())

        self.assertEqual(plan.layout.header_row, 1)
        self.assertEqual(plan.layout.prompts_row, 2)
        self.assertEqual(plan.layout.first_data_row, 3)
        self.assertEqual(plan.layout.id_column, 0)
        self.assertEqual(plan.layout.url_column, 1)
        self.assertIn("Prompts", plan.layout.detection["prompts"])

    def test_extracts_sources_and_columns(self) -> None:
        plan = parse_planning_sheet(self.standard_sheet())

        self.assertEqual([s.id for s in plan.sources], ["000020".lstrip("0"), "21", "22"])
        self.assertEqual(plan.sources[0].url, "https://example.com/alpha")
        # Id and URL columns never become repository columns.
        labels = [c.label for c in plan.columns]
        self.assertNotIn("ID#", labels)
        self.assertNotIn("URL", labels)
        self.assertEqual(labels, ["Citation", "Year Published", "Org Type", "Location"])
        self.assertTrue(all(c.prompt for c in plan.columns))

    def test_headings_without_prompts_are_skipped_and_reported(self) -> None:
        plan = parse_planning_sheet(self.standard_sheet())
        skipped = {c.label for c in plan.skipped_columns}
        self.assertEqual(skipped, {"Data Collection Year", "Source Origin"})
        self.assertIn("column_without_prompt", {a.code for a in plan.anomalies})

    def test_reports_candidates_so_a_wrong_guess_is_correctable(self) -> None:
        plan = parse_planning_sheet(self.standard_sheet())
        self.assertTrue(plan.layout.header_candidates)
        self.assertIn(1, [c.row for c in plan.layout.header_candidates])

    def test_header_row_can_be_overridden(self) -> None:
        plan = parse_planning_sheet(self.standard_sheet(), header_row=1, prompts_row=2)
        self.assertEqual(plan.layout.header_row, 1)
        self.assertEqual(plan.layout.detection["header"], "explicit override")

    def test_a_sheet_with_no_prompts_row_still_yields_sources(self) -> None:
        path = self.write_csv([HEADER, data_row("1", "https://example.com/a")])
        plan = parse_planning_sheet(path)

        self.assertEqual(len(plan.sources), 1)
        self.assertEqual(plan.columns, [])
        self.assertIn("no_prompts_row_found", {a.code for a in plan.anomalies})
        # Missing prompts is a note, not a blocker -- sources are still usable.
        self.assertEqual(plan.blocking_anomalies, [])

    def test_a_sheet_with_no_urls_blocks(self) -> None:
        path = self.write_csv([["Name", "Notes"], ["alpha", "some text"]])
        plan = parse_planning_sheet(path)
        codes = {a.code for a in plan.blocking_anomalies}
        self.assertIn("no_url_column_found", codes)


class AnomalyTests(_SheetTestCase):
    def test_duplicate_ids_and_urls_are_reported(self) -> None:
        path = self.standard_sheet(
            extra_rows=[
                data_row("21", "https://example.com/gamma"),
                data_row("23", "https://example.com/alpha"),
            ]
        )
        plan = parse_planning_sheet(path)
        codes = {a.code for a in plan.anomalies}
        self.assertIn("duplicate_source_id", codes)
        self.assertIn("duplicate_url", codes)

    def test_a_non_url_cell_is_reported_but_the_row_is_kept(self) -> None:
        path = self.standard_sheet(extra_rows=[data_row("24", "not a web address")])
        plan = parse_planning_sheet(path)

        self.assertIn("cell_not_a_url", {a.code for a in plan.anomalies})
        # Kept deliberately: create_sources blocks on it with a precise code,
        # which is a better place for the user to see the problem.
        self.assertIn("not a web address", [s.url for s in plan.sources])

    def test_a_non_numeric_id_is_blanked_so_one_is_allocated(self) -> None:
        path = self.standard_sheet(extra_rows=[data_row("TBD", "https://example.com/delta")])
        plan = parse_planning_sheet(path)

        self.assertIn("non_numeric_id", {a.code for a in plan.anomalies})
        delta = next(s for s in plan.sources if s.url.endswith("delta"))
        self.assertEqual(delta.id, "")

    def test_gaps_in_the_id_range_are_noted_not_blocked(self) -> None:
        path = self.standard_sheet(extra_rows=[data_row("99", "https://example.com/far")])
        plan = parse_planning_sheet(path)
        self.assertIn("non_contiguous_ids", {a.code for a in plan.anomalies})
        self.assertEqual(plan.blocking_anomalies, [])

    def test_blank_rows_are_ignored(self) -> None:
        path = self.standard_sheet(extra_rows=[["", "", "", "", "", "", "", ""]])
        plan = parse_planning_sheet(path)
        self.assertEqual(len(plan.sources), 3)


class EncodingTests(_SheetTestCase):
    def test_repairs_the_macroman_case_seen_in_the_real_sheet(self) -> None:
        mangled = f"a byline such as Staff {MOJIBAKE_EM_DASH} use the organization"
        repaired, codec = repair_text(mangled)

        self.assertEqual(repaired, "a byline such as Staff — use the organization")
        self.assertEqual(codec, "mac_roman")

    def test_leaves_clean_text_alone(self) -> None:
        clean = "an em dash — and a curly quote “here”"
        repaired, codec = repair_text(clean)
        self.assertEqual(repaired, clean)
        self.assertEqual(codec, "")

    def test_refuses_when_no_codec_round_trips(self) -> None:
        # A lone replacement character is damage that cannot be reversed.
        grid = [["text with � a lost character"]]
        _, report = repair_grid(grid)
        self.assertFalse(report.applied)

    def test_repairs_a_whole_sheet_and_reports_it(self) -> None:
        prompts = list(PROMPTS)
        prompts[4] = f"Write a citation {MOJIBAKE_EM_DASH} exactly as shown."
        prompts[5] = f"Return the year {MOJIBAKE_EM_DASH} four characters."
        path = self.write_csv([BANNER, HEADER, prompts, data_row("20", "https://example.com/a")])

        plan = parse_planning_sheet(path)

        self.assertTrue(plan.encoding_repair.applied)
        self.assertEqual(plan.encoding_repair.codec, "mac_roman")
        self.assertEqual(plan.encoding_repair.cells_changed, 2)
        self.assertEqual(plan.encoding_repair.suspicious_after, 0)
        self.assertIn("mojibake_repaired", {a.code for a in plan.anomalies})
        self.assertIn("—", plan.columns[0].prompt)
        self.assertNotIn(MOJIBAKE_EM_DASH, plan.columns[0].prompt)

    def test_repair_can_be_disabled(self) -> None:
        prompts = list(PROMPTS)
        prompts[4] = f"Write a citation {MOJIBAKE_EM_DASH} exactly as shown."
        path = self.write_csv([BANNER, HEADER, prompts, data_row("20", "https://example.com/a")])

        plan = parse_planning_sheet(path, repair_encoding="never")

        self.assertFalse(plan.encoding_repair.applied)
        self.assertIn(MOJIBAKE_EM_DASH, plan.columns[0].prompt)

    def test_counts_damage(self) -> None:
        self.assertEqual(count_suspicious(f"a {MOJIBAKE_EM_DASH} b {MOJIBAKE_EM_DASH} c"), 2)
        self.assertEqual(count_suspicious("clean text"), 0)


class ParamConversionTests(_SheetTestCase):
    def test_converts_to_operation_params(self) -> None:
        plan = parse_planning_sheet(self.standard_sheet())

        sources = sheet_plan_to_create_sources_params(plan)
        self.assertEqual(len(sources["sources"]), 3)
        self.assertEqual(sources["sources"][0], {"url": "https://example.com/alpha", "id": "20"})
        self.assertTrue(sources["skip_existing"])

        columns = sheet_plan_to_create_columns_params(plan)
        self.assertEqual(len(columns["columns"]), 4)
        self.assertEqual(columns["columns"][0]["label"], "Citation")
        self.assertTrue(columns["columns"][0]["instruction_prompt"])
        self.assertTrue(columns["columns"][0]["include_source_text"])


class ReadGridTests(_SheetTestCase):
    def test_rejects_unsupported_types(self) -> None:
        path = self.tmp / "notes.docx"
        path.write_text("x", encoding="utf-8")
        with self.assertRaises(SheetReadError):
            read_grid(path)

    def test_rejects_a_missing_file(self) -> None:
        with self.assertRaises(SheetReadError):
            read_grid(self.tmp / "nope.csv")

    def test_pads_ragged_rows_to_a_rectangle(self) -> None:
        path = self.write_csv([["a", "b", "c"], ["1"]])
        grid, fmt = read_grid(path)
        self.assertEqual(fmt, "csv")
        self.assertEqual(len(grid[1]), 3)


if __name__ == "__main__":
    unittest.main()
