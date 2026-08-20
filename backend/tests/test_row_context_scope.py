"""Row-context scope: fetch-metadata-only mode must not leak other columns.

Covers the per-column `row_context_scope` added so a column's LLM run sees only
the fetched document plus deterministic fetch/ingest provenance -- never another
column's extracted value.
"""
import unittest

from backend.models.repository import RepositoryColumnConfig
from backend.models.sources import FETCH_METADATA_FIELDS
from backend.pipeline.source_downloader import MANIFEST_DERIVED_COLUMNS
from backend.storage.attached_repository import (
    _effective_column_include_row_context,
    _effective_column_row_context_scope,
    _row_metadata_for_scope,
)


def _cfg(**kw) -> RepositoryColumnConfig:
    return RepositoryColumnConfig(id="c", label="C", kind="custom", **kw)


# A manifest record mixing deterministic provenance with LLM-derived values and
# another custom column's answer.
MANIFEST = {
    "final_url": "https://example.org/vpp",
    "original_url": "https://example.org/vpp",
    "source_kind": "web",
    "detected_type": "html",
    "http_status": 200,
    "fetch_status": "fetched",
    "discovered_media_urls": "https://youtu.be/x",
    "relevant_image_count": 2,
    "image_status": "analyzed",
    "ocr_pdf_file": "sources/1/ocr.pdf",
    # LLM catalog / derived -- must be withheld under fetch_metadata:
    "title": "A guessed title",
    "organization_name": "Guessed Org",
    "publication_date": "2024-03-01",
    "summary_text": "an LLM summary",
    "rating_overall": 0.8,
    "citation_title": "coded citation title",
    # another custom column's stored answer:
    "custom_abc12345": "commercial",
    # big blobs withheld even under 'all':
    "rating_raw_json": "{...}",
    "citation_field_evidence_json": "{...}",
}


class ScopeResolutionTests(unittest.TestCase):
    def test_legacy_boolean_fallback(self) -> None:
        self.assertEqual(_effective_column_row_context_scope(_cfg(include_row_context=True)), "all")
        self.assertEqual(_effective_column_row_context_scope(_cfg(include_row_context=False)), "none")
        self.assertEqual(_effective_column_row_context_scope(None), "none")

    def test_explicit_scope_overrides_boolean(self) -> None:
        self.assertEqual(
            _effective_column_row_context_scope(
                _cfg(include_row_context=True, row_context_scope="none")
            ),
            "none",
        )
        self.assertEqual(
            _effective_column_row_context_scope(
                _cfg(include_row_context=False, row_context_scope="fetch_metadata")
            ),
            "fetch_metadata",
        )

    def test_include_flag_derived_from_scope(self) -> None:
        self.assertTrue(_effective_column_include_row_context(_cfg(row_context_scope="fetch_metadata")))
        self.assertFalse(_effective_column_include_row_context(_cfg(row_context_scope="none")))


class MetadataFilteringTests(unittest.TestCase):
    def test_none_sends_nothing(self) -> None:
        self.assertEqual(_row_metadata_for_scope(MANIFEST, "none"), {})

    def test_fetch_metadata_keeps_provenance_only(self) -> None:
        out = _row_metadata_for_scope(MANIFEST, "fetch_metadata")
        # deterministic provenance the prompts rely on is present
        for key in ("final_url", "source_kind", "detected_type", "http_status",
                    "discovered_media_urls", "relevant_image_count", "ocr_pdf_file"):
            self.assertIn(key, out)
        # no LLM catalog / derived / other-column value leaks through
        for leaked in ("title", "organization_name", "publication_date",
                       "summary_text", "rating_overall", "citation_title",
                       "custom_abc12345"):
            self.assertNotIn(leaked, out)

    def test_all_keeps_everything_but_the_blobs(self) -> None:
        out = _row_metadata_for_scope(MANIFEST, "all")
        self.assertIn("custom_abc12345", out)  # legacy behaviour unchanged
        self.assertIn("title", out)
        self.assertNotIn("rating_raw_json", out)
        self.assertNotIn("citation_field_evidence_json", out)

    def test_allowlist_excludes_all_llm_derived_columns(self) -> None:
        self.assertFalse(FETCH_METADATA_FIELDS & set(MANIFEST_DERIVED_COLUMNS))


if __name__ == "__main__":
    unittest.main()
