"""End-to-end conversion tests over a small synthetic RA repository.

These build their own fixture rather than depending on a real repository, so they
run anywhere. They need Node and the UPC package (``UPC_HOME``, or a vendored copy
under ``backend/vendor/upc``); when it is absent the tests skip rather than fail,
because the bridge is an integration dependency, not a unit under test.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from backend.upc.convert_repo import Converter, load_ra_repo, plan
from backend.upc.node_bridge import NodeBridge, UpcUnavailable


def _bridge_or_skip() -> NodeBridge:
    try:
        return NodeBridge()
    except UpcUnavailable as exc:  # pragma: no cover - environment dependent
        raise unittest.SkipTest(f"UPC bridge unavailable: {exc}")


CLEAN_MD = (
    "# Virtual Power Plants\n\n"
    "A virtual power plant is a cloud-based network of distributed resources.\n"
    "A utility may operate one directly, or contract an aggregator to run it.\n"
)
# The "tidied" copy silently rewords the source -- the failure mode measured across
# the reference repository, where 68% of substantive lines diverge like this.
LLM_MD = (
    "# Virtual Power Plants\n\n"
    "A virtual power plant is a cloud based network of distributed resources.\n"
    "A utility may operate one directly, or contract an aggregator to run it.\n"
)
RAW_HTML = "<html><head><title>VPP Explainer for Grid Operators | Example</title></head><body><p>Body.</p></body></html>\n"


def build_ra_repo(root: Path) -> None:
    """A minimal but realistic attached RA repository."""
    (root / ".ra_repo").mkdir(parents=True)
    s1 = root / "sources" / "000001"
    s1.mkdir(parents=True)
    (s1 / "000001_source.html").write_text(RAW_HTML, encoding="utf-8")
    (s1 / "000001_clean.md").write_text(CLEAN_MD, encoding="utf-8")
    (s1 / "000001_llm_clean.md").write_text(LLM_MD, encoding="utf-8")
    (s1 / "000001_catalog.json").write_text(
        json.dumps(
            {
                "title": "VPP Explainer",
                "evidence_snippets": [
                    # anchors uniquely in the deterministic markdown
                    "A utility may operate one directly, or contract an aggregator to run it.",
                    # exists ONLY in the model rewrite -> must not be written as verified
                    "A virtual power plant is a cloud based network of distributed resources.",
                    # lives in the HTML head, not the prose
                    "VPP Explainer for Grid Operators | Example",
                    # too short to be evidence
                    "VPP",
                ],
            }
        ),
        encoding="utf-8",
    )
    # A failed fetch whose only bytes are its metadata sidecar (UPC rule 0.4).
    s2 = root / "sources" / "000002"
    s2.mkdir(parents=True)
    (s2 / "000002_metadata.json").write_text(json.dumps({"id": "000002"}), encoding="utf-8")

    rows = [
        {
            "id": "000001",
            "source_kind": "url",
            "original_url": "https://example.org/vpp?utm_source=news",
            "final_url": "https://example.org/vpp",
            "fetch_status": "success",
            "fetch_method": "http",
            "detected_type": "html",
            "fetched_at": "2026-01-02T03:04:05Z",
            "title": "VPP Explainer",
            "author_names": "Smith, Jane",
            "publication_year": "2026",
            "raw_file": "sources/000001/000001_source.html",
            "markdown_file": "sources/000001/000001_clean.md",
            "llm_cleanup_file": "sources/000001/000001_llm_clean.md",
            "catalog_file": "sources/000001/000001_catalog.json",
            "custom_fields": {"c_org": "Utility", "c_note": "grid services"},
            "phase_metadata": {
                "fetch": {"status": "completed", "started_at": "2026-01-02T03:04:00Z",
                          "completed_at": "2026-01-02T03:04:05Z"},
                "cleanup": {"status": "completed", "started_at": "2026-01-02T03:05:00Z",
                            "completed_at": "2026-01-02T03:05:09Z", "model": "chat2",
                            "prompt_version": "source_markdown_cleanup.v1"},
            },
        },
        {
            "id": "000002",
            "source_kind": "url",
            "original_url": "https://example.org/missing",
            "fetch_status": "failed",
            "metadata_file": "sources/000002/000002_metadata.json",
            "custom_fields": {},
            "phase_metadata": {},
        },
    ]
    columns = [
        {"id": "c_org", "label": "Org Type",
         "output_constraint": {"allowed_values": ["Utility", "NGO"], "fallback_value": "Not sure"}},
        {"id": "c_note", "label": "Assoc. Orgs", "output_constraint": {}},
        {"id": "c_url", "label": "Video link (URL)", "output_constraint": {}},
    ]
    (root / ".ra_repo" / "repository_state.json").write_text(
        json.dumps({"sources": rows, "citations": [], "imports": [], "column_configs": columns}),
        encoding="utf-8",
    )
    (root / ".ra_repo" / "repository.json").write_text(
        json.dumps({"schema_version": 5, "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-03T00:00:00Z"}),
        encoding="utf-8",
    )
    # Must never be read, let alone copied into a corpus meant to be shared.
    (root / ".ra_repo" / "agent_tokens.json").write_text(
        json.dumps({"read_token": "SEKRET-READ-0123456789", "write_token": "SEKRET-WRITE-0123456789"}),
        encoding="utf-8",
    )


class ConvertTests(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        cls.bridge = _bridge_or_skip()
        cls.tmp = Path(tempfile.mkdtemp(prefix="upc-convert-"))
        cls.inp = cls.tmp / "repo"
        cls.out = cls.tmp / "corpus"
        build_ra_repo(cls.inp)
        cls.repo = load_ra_repo(cls.inp)
        cls.report = Converter(cls.repo, cls.out, cls.bridge).run()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tmp, ignore_errors=True)

    # -- the contract ----------------------------------------------------

    def test_corpus_validates_with_no_errors(self):
        proc = self.bridge.cli(["validate", str(self.out)])
        report = json.loads(proc.stdout)
        self.assertEqual(report["status"], "passed", report["errors"][:5])
        self.assertEqual(report["errors"], [])

    def test_input_is_untouched(self):
        """The converter is read-only with respect to its input, by construction."""
        before = {p: p.stat().st_mtime_ns for p in self.inp.rglob("*") if p.is_file()}
        Converter(load_ra_repo(self.inp), self.out, self.bridge).run()
        after = {p: p.stat().st_mtime_ns for p in self.inp.rglob("*") if p.is_file()}
        self.assertEqual(before, after)

    def test_agent_tokens_never_reach_the_corpus(self):
        secrets = json.loads((self.inp / ".ra_repo" / "agent_tokens.json").read_text())
        blob = b"".join(
            p.read_bytes() for p in self.out.rglob("*") if p.is_file() and p.suffix in
            {".json", ".jsonl", ".csv", ".html", ".md"}
        )
        for value in secrets.values():
            self.assertNotIn(value.encode(), blob)

    def test_conversion_is_deterministic_except_its_own_timestamp(self):
        other = self.tmp / "corpus2"
        Converter(load_ra_repo(self.inp), other, self.bridge).run()
        for rel in ("corpus.json", "sources.csv", "extractions.csv"):
            self.assertEqual((self.out / rel).read_bytes(), (other / rel).read_bytes(), rel)
        a = [json.loads(l) for l in (self.out / "provenance" / "events.jsonl").read_text().splitlines() if l.strip()]
        b = [json.loads(l) for l in (other / "provenance" / "events.jsonl").read_text().splitlines() if l.strip()]
        self.assertEqual(len(a), len(b))
        differing = [x["activity_type"] for x, y in zip(a, b) if x != y]
        self.assertEqual(differing, ["import"])

    # -- the mapping -----------------------------------------------------

    def test_failed_fetch_still_satisfies_rule_0_4(self):
        src = self._source_by_alias("000002")
        roles = [r["role"] for r in src["representations"]]
        self.assertEqual(roles, ["metadata"])

    def test_ra_id_survives_as_an_alias(self):
        self.assertEqual(self._source_by_alias("000001")["aliases"]["researchassistant"], "000001")

    def test_the_rewrite_declares_its_model_derivation(self):
        src = self._source_by_alias("000001")
        rewrite = next(r for r in src["representations"]
                       if r.get("ext", {}).get("researchassistant", {}).get("ra_field") == "llm_cleanup_file")
        deterministic = next(r for r in src["representations"]
                             if r.get("ext", {}).get("researchassistant", {}).get("ra_field") == "markdown_file")
        self.assertEqual(rewrite["produced_by"], "model")
        self.assertEqual(rewrite["parent_representation_ref"], deterministic["representation_id"])

    def test_a_quote_only_in_the_rewrite_is_not_written_as_verified(self):
        """The whole point of the default: a sentence the source never wrote must not
        end up anchored as if it did."""
        exts = self._extractions()
        quotes = {e["direct_quote"] for e in exts}
        self.assertIn("A utility may operate one directly, or contract an aggregator to run it.", quotes)
        self.assertNotIn("A virtual power plant is a cloud based network of distributed resources.", quotes)

    def test_html_head_metadata_is_an_entity_not_a_quote(self):
        """A value found only in the markup is a metadata value, not a quotation of
        the document's prose."""
        title = next((e for e in self._extractions() if e["direct_quote"] == "VPP Explainer for Grid Operators | Example"), None)
        self.assertIsNotNone(title)
        self.assertEqual(title["type"], "entity")
        self.assertEqual(title["secondary_locators"][0]["type"], "css_selector")

    def test_short_values_never_become_extractions(self):
        """RA's stored "evidence" mixes real prose with one-word field values.
        Minting a needs_review extraction for the word "VPP" is noise, not
        provenance, so anything under the threshold is skipped outright."""
        from backend.upc.convert_repo import MIN_EVIDENCE_CODEPOINTS

        quotes = {e["direct_quote"] for e in self._extractions()}
        self.assertNotIn("VPP", quotes)
        self.assertTrue(all(len(q) >= MIN_EVIDENCE_CODEPOINTS for q in quotes))

    def test_every_extraction_passes_the_gate_by_construction(self):
        for ext in self._extractions():
            rep = self._rep_path(ext["representation_ref"])
            text = (self.out / rep).read_text(encoding="utf-8")
            v = ext["locator"]["value"]
            self.assertEqual(text[v["start"]:v["end"]], ext["direct_quote"])

    def test_columns_are_triaged_not_blindly_coded(self):
        books = [json.loads(p.read_text()) for p in (self.out / "codebooks").glob("*.json")]
        by_slug = {b["slug"]: b for b in books}
        self.assertIn("org-type", by_slug)
        self.assertTrue(by_slug["org-type"]["closed"])
        self.assertIn("assoc-orgs", by_slug)
        self.assertFalse(by_slug["assoc-orgs"]["closed"])
        # A URL column is a reference, not a coding scheme.
        self.assertNotIn("video-link-url", by_slug)

    def test_codings_are_document_level_and_carry_no_locator(self):
        codings = self._codings()
        self.assertTrue(codings)
        for c in codings:
            self.assertEqual(c["target"]["kind"], "source")
            self.assertNotIn("locator", c)
            self.assertEqual(c["coder"], "ra-column-v1")

    def test_open_and_closed_codebooks_use_the_right_form(self):
        books = {json.loads(p.read_text())["codebook_id"]: json.loads(p.read_text())
                 for p in (self.out / "codebooks").glob("*.json")}
        for c in self._codings():
            closed = books[c["codebook_ref"]]["closed"]
            self.assertEqual("code" in c, closed, c)
            self.assertEqual("value" in c, not closed, c)

    def test_journal_uses_the_phases_real_timestamps(self):
        events = [json.loads(l) for l in
                  (self.out / "provenance" / "events.jsonl").read_text().splitlines() if l.strip()]
        fetch = next(e for e in events if e["activity_type"] == "fetch")
        self.assertEqual(fetch["started_at"], "2026-01-02T03:04:00Z")
        self.assertEqual(events[-1]["activity_type"], "import")

    def test_plan_writes_nothing(self):
        scratch = self.tmp / "planonly"
        before = sorted(p.name for p in self.tmp.iterdir())
        plan(load_ra_repo(self.inp), self.bridge)
        self.assertFalse(scratch.exists())
        self.assertEqual(sorted(p.name for p in self.tmp.iterdir()), before)

    # -- helpers ---------------------------------------------------------

    def _sources(self) -> list[dict]:
        return [json.loads(p.read_text()) for p in sorted((self.out / "sources").glob("*/source.json"))]

    def _source_by_alias(self, ra_id: str) -> dict:
        for s in self._sources():
            if s.get("aliases", {}).get("researchassistant") == ra_id:
                return s
        raise AssertionError(f"no source with RA id {ra_id}")

    def _rep_path(self, rep_id: str) -> str:
        for s in self._sources():
            for r in s["representations"]:
                if r["representation_id"] == rep_id:
                    return r["path"]
        raise AssertionError(f"no representation {rep_id}")

    def _extractions(self) -> list[dict]:
        out: list[dict] = []
        for p in (self.out / "sources").glob("*/extractions.jsonl"):
            out.extend(json.loads(l) for l in p.read_text().splitlines() if l.strip())
        return out

    def _codings(self) -> list[dict]:
        out: list[dict] = []
        for p in (self.out / "codings").glob("*/items.jsonl"):
            out.extend(json.loads(l) for l in p.read_text().splitlines() if l.strip())
        return out


class OutputSafetyTests(unittest.TestCase):
    def test_output_inside_input_is_refused(self):
        from backend.upc.convert_repo import _assert_output_outside_input

        with tempfile.TemporaryDirectory() as td:
            inp = Path(td) / "repo"
            inp.mkdir()
            with self.assertRaises(SystemExit):
                _assert_output_outside_input(inp, inp / "corpus")
            _assert_output_outside_input(inp, Path(td) / "corpus")  # sibling is fine


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
