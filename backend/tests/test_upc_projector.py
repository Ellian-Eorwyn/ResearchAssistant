"""Unit tests for the pure RA -> UPC mapping.

These never touch Node or the filesystem: `projector` is deliberately side-effect
free so the offline converter and the live in-app projection can share it exactly.
Everything asserted here is a mapping decision that would otherwise be easy to
"tidy" into something wrong.
"""

from __future__ import annotations

import unittest

from backend.upc import projector as P


class RepresentationRoleTests(unittest.TestCase):
    def test_raw_file_is_polymorphic(self):
        """`raw_file` means HTML, a PDF, or a transcript depending on the source.

        Keying the role off the field name alone -- which the published crosswalk
        does -- silently mislabels 6 of the 7 video sources in the reference
        repository as raw_html.
        """
        html = P.plan_representations(
            {"raw_file": "sources/1/1_source.html", "detected_type": "html", "fetch_method": "http"}
        )
        self.assertEqual(html[0].role, "raw_html")
        self.assertEqual(html[0].media_type, "text/html")

        pdf = P.plan_representations(
            {"raw_file": "sources/2/2_doc.pdf", "detected_type": "pdf", "fetch_method": "local_upload"}
        )
        self.assertEqual(pdf[0].role, "document_pdf")
        self.assertEqual(pdf[0].produced_by, "manual")

        vtt = P.plan_representations(
            {"raw_file": "sources/3/3_source.vtt", "detected_type": "video", "fetch_method": "yt_dlp"}
        )
        self.assertEqual(vtt[0].role, "transcript")

    def test_llm_cleanup_declares_a_model_derivation(self):
        """The AI-tidied copy must declare model + a textual parent.

        That pair is what earns `verified-to-rewrite` instead of plain `verified`.
        Measured across the reference repository, 68% of substantive lines in these
        files are not verbatim in their parent, so a quote taken from one is the
        model's wording, not the source's.
        """
        plans = {
            p.field: p
            for p in P.plan_representations(
                {
                    "markdown_file": "sources/1/1_clean.md",
                    "llm_cleanup_file": "sources/1/1_llm_clean.md",
                }
            )
        }
        rewrite = plans["llm_cleanup_file"]
        self.assertEqual(rewrite.produced_by, "model")
        self.assertEqual(rewrite.parent_field, "markdown_file")
        self.assertEqual(plans["markdown_file"].produced_by, "conversion")

    def test_metadata_sidecar_is_a_representation(self):
        """A failed fetch has no captured bytes but still needs one representation,
        or UPC rule 0.4 rejects the source outright."""
        plans = P.plan_representations(
            {"metadata_file": "sources/182/182_metadata.json", "fetch_status": "failed"}
        )
        self.assertEqual([p.role for p in plans], ["metadata"])

    def test_identical_bytes_collapse_to_one_representation(self):
        """A rep- id is the file's byte hash, so two fields over the same bytes are
        one representation; the suppressed role is recorded rather than dropped."""
        plans = P.plan_representations(
            {
                "raw_file": "a.html",
                "rendered_file": "b.html",
                "markdown_file": "c.md",
                "detected_type": "html",
            }
        )
        same = {"a.html": "hh", "b.html": "hh", "c.md": "mm"}
        kept, suppressed = P.dedupe_representations(plans, same)
        self.assertEqual(len(kept), 2)
        roles = {k.role for k in kept}
        self.assertEqual(roles, {"raw_html", "clean_markdown"})
        self.assertEqual(suppressed["a.html"][0]["role"], "rendered_html")

    def test_missing_files_are_not_planned_as_representations(self):
        plans = P.plan_representations({"raw_file": "a.html", "detected_type": "html"})
        kept, _ = P.dedupe_representations(plans, {})
        self.assertEqual(kept, [])


class ColumnTriageTests(unittest.TestCase):
    """RA's columns are heterogeneous; the naive "closed value set -> codebook,
    else generation" rule over-produces junk."""

    def test_closed_value_set_is_a_codebook(self):
        self.assertEqual(
            P.triage_column({"label": "Org Type", "output_constraint": {"allowed_values": ["Utility", "NGO"]}}),
            P.COLUMN_CODEBOOK_CLOSED,
        )

    def test_free_text_is_an_open_codebook(self):
        self.assertEqual(
            P.triage_column({"label": "Assoc. Orgs", "output_constraint": {}}),
            P.COLUMN_CODEBOOK_OPEN,
        )

    def test_a_url_column_is_not_a_code(self):
        """Coding a document *with a URL* is a category error: the value is a
        reference to another object, not a label drawn from a scheme."""
        for label in ("Video link (URL)", "Source PDF", "if video embedded, linked to what source ID"):
            self.assertEqual(P.triage_column({"label": label, "output_constraint": {}}),
                             P.COLUMN_REFERENCE, label)

    def test_workflow_flags_stay_private(self):
        for label in ("Flag for possible deletion", "Flag as possible duplicate from 2025 or 2026 Dataset"):
            self.assertEqual(P.triage_column({"label": label, "output_constraint": {}}),
                             P.COLUMN_WORKFLOW, label)

    def test_bibliographic_columns_are_metadata(self):
        for label in ("Citation", "Year Published"):
            self.assertEqual(P.triage_column({"label": label, "output_constraint": {}}),
                             P.COLUMN_BIBLIOGRAPHIC, label)

    def test_fallback_value_becomes_a_declared_code(self):
        """RA substitutes the fallback when a model answer fails its constraint, so
        the value really occurs in the data and must be legal -- but it means "the
        coder declined", which the definition has to say out loud."""
        cbk = P.codebook_for_column(
            {
                "id": "custom_1",
                "label": "Sector",
                "output_constraint": {"allowed_values": ["Public", "Private"], "fallback_value": "not sure"},
            },
            "researchassistant",
            "sector",
        )
        self.assertTrue(cbk["closed"])
        tokens = {c["code"] for c in cbk["codes"]}
        self.assertEqual(tokens, {"public", "private", "not-sure"})
        declined = next(c for c in cbk["codes"] if c["code"] == "not-sure")
        self.assertIn("No determination", declined["definition"])
        self.assertEqual(cbk["aliases"], {"researchassistant": "custom_1"})

    def test_code_token_is_stable_under_label_edits(self):
        """The token, not the label, enters the cod- identity recipe, so re-casing
        or re-punctuating a label must not re-mint every coding."""
        self.assertEqual(P.code_token("Investor-Owned Utility"), P.code_token("investor owned utility"))
        self.assertEqual(P.code_token("  Not Sure  "), "not-sure")
        self.assertEqual(P.code_token("!!!"), "unlabelled")


class ProvenanceTests(unittest.TestCase):
    def test_profile_name_does_not_masquerade_as_prompt_version(self):
        """Two different things -- which rubric ran vs. which prompt text ran.
        Collapsing them, as the published crosswalk suggests, makes the stamp lie."""
        stamp = P.provenance_stamp(
            {"model": "chat2", "prompt_version": "source_title.v1", "profile_name": "yolovpp.yaml",
             "completed_at": "2026-08-20T16:52:39Z"},
            tool="researchassistant", fallback_created_at="1970-01-01T00:00:00Z",
        )
        self.assertEqual(stamp["produced_by"]["prompt_version"], "source_title.v1")
        self.assertEqual(stamp["produced_by"]["model"], "chat2")
        self.assertEqual(stamp["produced_by"]["method"], "model")
        self.assertEqual(stamp["ext"]["researchassistant"]["profile_name"], "yolovpp.yaml")
        self.assertEqual(stamp["created_at"], "2026-08-20T16:52:39Z")

    def test_stamp_falls_back_when_a_phase_never_ran(self):
        stamp = P.provenance_stamp(None, tool="t", fallback_created_at="2026-01-01T00:00:00Z")
        self.assertEqual(stamp["created_at"], "2026-01-01T00:00:00Z")
        self.assertEqual(stamp["produced_by"], {"tool": "t", "method": "import"})

    def test_events_use_the_phase_s_real_timestamps(self):
        """So the journal reads as a history rather than a wall of "now"."""
        ev = P.phase_event(
            "title",
            {"status": "completed", "started_at": "2026-08-20T16:52:36Z",
             "completed_at": "2026-08-20T16:52:39Z", "model": "chat2"},
            tool="researchassistant", inputs={"source_ids": ["src-abc"]},
        )
        self.assertEqual(ev["activity_type"], "generate")
        self.assertEqual(ev["status"], "success")
        self.assertEqual(ev["started_at"], "2026-08-20T16:52:36Z")
        self.assertEqual(ev["ended_at"], "2026-08-20T16:52:39Z")

    def test_a_phase_that_never_started_produces_no_event(self):
        self.assertIsNone(P.phase_event("title", {"status": "completed"}, tool="t"))
        self.assertIsNone(P.phase_event("title", {"status": "", "started_at": "x"}, tool="t"))

    def test_every_phase_maps_onto_a_closed_activity_vocab(self):
        """`activity_type` has no x- escape, so an unmapped phase must not invent
        a value -- it falls back to `generate`."""
        allowed = {"search", "fetch", "render", "extract", "embed", "generate",
                   "synthesize", "validate", "import", "export", "modify",
                   "reanchor", "retract", "remove", "redact"}
        self.assertTrue(set(P.PHASE_ACTIVITY.values()) <= allowed)
        ev = P.phase_event("some_future_phase", {"status": "completed", "started_at": "t"}, tool="t")
        self.assertEqual(ev["activity_type"], "generate")


class BibliographicTests(unittest.TestCase):
    def test_authors_split_into_csl_shapes(self):
        got = P.bibliographic_block({"author_names": "Smith, Jane; International Energy Agency"})
        self.assertEqual(got["authors"], [{"family": "Smith", "given": "Jane"},
                                          {"literal": "International Energy Agency"}])

    def test_issued_prefers_a_full_date_then_a_year(self):
        self.assertEqual(P.bibliographic_block({"publication_date": "2023-04-13"})["issued"],
                         {"date_parts": [[2023, 4, 13]]})
        self.assertEqual(P.bibliographic_block({"publication_year": "2026"})["issued"],
                         {"date_parts": [[2026]]})
        self.assertNotIn("issued", P.bibliographic_block({"publication_year": "n.d."}))

    def test_item_type_follows_the_document_then_the_kind(self):
        self.assertEqual(P.bibliographic_block({"document_type": "Journal article"})["item_type"],
                         "article-journal")
        self.assertEqual(P.bibliographic_block({"source_kind": "video"})["item_type"], "motion_picture")
        self.assertEqual(P.bibliographic_block({"source_kind": "url"})["item_type"], "webpage")

    def test_retrieval_maps_ra_vocab_onto_upc_vocab(self):
        ret = P.retrieval_block({
            "original_url": "https://x.test/a", "fetch_status": "blocked",
            "fetch_method": "yt_dlp", "http_status": "403", "sha256": "ab" * 32,
        })
        self.assertEqual(ret["fetch_status"], "blocked")
        self.assertEqual(ret["fetch_method"], "x-yt-dlp")   # not in the closed enum
        self.assertEqual(ret["http_status"], 403)
        self.assertTrue(ret["sha256"].startswith("sha256:"))

    def test_unknown_fetch_method_gets_an_x_escape_not_an_invalid_value(self):
        ret = P.retrieval_block({"fetch_method": "some new method"})
        self.assertEqual(ret["fetch_method"], "x-some-new-method")

    def test_slug_seed_reads_like_a_bibliography_entry(self):
        seed = P.slug_seed({"author_names": "Smith, Jane", "publication_year": "2024",
                            "title": "Virtual Power Plants"})
        self.assertEqual(seed, "Smith 2024 Virtual Power Plants")
        self.assertTrue(P.slug_seed({"title": "T"}).startswith("n-d"))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
