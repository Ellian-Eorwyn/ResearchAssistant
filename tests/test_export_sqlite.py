from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from backend.models.export import ExportRow
from backend.models.sources import SourceManifestRow
from backend.pipeline.stage_export_sqlite import build_wikiclaude_sqlite_db


class ExportSqliteTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="sqlite-export-tests-")
        self.tmp_path = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_builds_wikiclaude_compatible_sqlite_export(self):
        rows = [
            ExportRow(
                repository_source_id="000001",
                source_document="paper-a.pdf",
                import_type="document",
                citation_ref_numbers="1",
                citing_sentence="Sentence A",
                cited_title="A Useful Source",
                cited_url="https://example.com/a",
                match_confidence=0.91,
                match_method="ref_number",
            ),
            ExportRow(
                repository_source_id="000001",
                source_document="paper-a.pdf",
                import_type="document",
                citation_ref_numbers="1",
                citing_sentence="Sentence B",
                cited_title="A Useful Source",
                cited_url="https://example.com/a",
                match_confidence=0.63,
                match_method="ref_number",
            ),
        ]
        source_rows = [
            SourceManifestRow(
                id="000001",
                repository_source_id="000001",
                original_url="https://example.com/a",
                title="Source Title From Manifest",
                summary_file="summaries/000001_summary.md",
            )
        ]

        db_path = self.tmp_path / "wikiclaude_export.db"
        build_wikiclaude_sqlite_db(db_path=db_path, export_rows=rows, source_rows=source_rows)
        self.assertTrue(db_path.exists())

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            tables = {
                r["name"]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            self.assertIn("articles", tables)
            self.assertIn("domains", tables)
            self.assertIn("subdomains", tables)
            self.assertIn("classifications", tables)
            self.assertIn("ingest_state", tables)

            article_count = conn.execute("SELECT COUNT(*) AS cnt FROM articles").fetchone()["cnt"]
            class_count = conn.execute("SELECT COUNT(*) AS cnt FROM classifications").fetchone()["cnt"]
            self.assertEqual(article_count, 1)
            self.assertEqual(class_count, 1)

            domain = conn.execute("SELECT slug FROM domains LIMIT 1").fetchone()
            self.assertIsNotNone(domain)
            self.assertEqual(domain["slug"], "research-assistant")

            article = conn.execute("SELECT * FROM articles LIMIT 1").fetchone()
            self.assertEqual(article["title"], "A Useful Source")
            self.assertGreater(article["text_length"], 0)
            categories = json.loads(article["categories"])
            self.assertIn("import_type:document", categories)
            self.assertIn("source_document:paper-a.pdf", categories)

            cls = conn.execute("SELECT signals FROM classifications LIMIT 1").fetchone()
            self.assertIsNotNone(cls)
            signals = json.loads(cls["signals"])
            self.assertEqual(signals["method"], "researchassistant_export")

            state_row = conn.execute(
                "SELECT value FROM ingest_state WHERE key = 'classification_complete'"
            ).fetchone()
            self.assertIsNotNone(state_row)
            self.assertEqual(json.loads(state_row["value"]), True)
        finally:
            conn.close()

    def test_builds_taxonomy_sqlite_export_when_config_provided(self):
        try:
            import yaml  # noqa: F401
        except Exception:
            self.skipTest("PyYAML is not installed")

        rows = [
            ExportRow(
                repository_source_id="000007",
                source_document="heat-pumps.md",
                import_type="document",
                citation_ref_numbers="7",
                citing_sentence="Heat pump policy adoption is increasing.",
                cited_title="Heat Pump Adoption Review",
                cited_summary="Heat pump deployment and incentives.",
                cited_url="https://example.com/heat-pump",
                match_confidence=0.82,
                match_method="title_url",
            )
        ]
        source_rows = [
            SourceManifestRow(
                id="000007",
                repository_source_id="000007",
                original_url="https://example.com/heat-pump",
                title="Heat Pump Adoption Review",
            )
        ]

        taxonomy_path = self.tmp_path / "domains.yaml"
        taxonomy_path.write_text(
            """
classification:
  min_confidence: 1.0
  max_subdomains_per_article: 2
domains:
  climate:
    name: Climate
    description: Climate topics
    subdomains:
      heat_pumps:
        name: Heat Pumps
        description: Heat pump systems
        keywords:
          - heat
          - pump
        category_patterns:
          - import_type:document
""".strip()
            + "\n",
            encoding="utf-8",
        )

        db_path = self.tmp_path / "wikiclaude_export_taxonomy.db"
        build_wikiclaude_sqlite_db(
            db_path=db_path,
            export_rows=rows,
            source_rows=source_rows,
            taxonomy_config_path=taxonomy_path,
        )
        self.assertTrue(db_path.exists())

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            domain = conn.execute("SELECT id FROM domains WHERE slug = 'climate'").fetchone()
            self.assertIsNotNone(domain)
            subdomain = conn.execute(
                "SELECT id FROM subdomains WHERE slug = 'heat_pumps'"
            ).fetchone()
            self.assertIsNotNone(subdomain)
            classification = conn.execute(
                """
                SELECT c.signals
                FROM classifications c
                JOIN subdomains s ON s.id = c.subdomain_id
                WHERE s.slug = 'heat_pumps'
                LIMIT 1
                """
            ).fetchone()
            self.assertIsNotNone(classification)
            signals = json.loads(classification["signals"])
            self.assertEqual(signals["method"], "researchassistant_taxonomy_export")
        finally:
            conn.close()

    def test_embeds_markdown_in_wikitext_for_reader_compatibility(self):
        markdown_text = "# Sample Heading\n\nThis is full markdown body text."
        rows = [
            ExportRow(
                repository_source_id="000009",
                source_document="sample.md",
                import_type="document",
                citation_ref_numbers="9",
                citing_sentence="Citation sentence",
                cited_title="Markdown Source",
                cited_url="https://example.com/sample",
                match_confidence=0.77,
                match_method="title_url",
            )
        ]
        source_rows = [
            SourceManifestRow(
                id="000009",
                repository_source_id="000009",
                original_url="https://example.com/sample",
                title="Markdown Source",
                markdown_file="markdown/000009_clean.md",
            )
        ]

        db_path = self.tmp_path / "wikiclaude_export_markdown.db"
        build_wikiclaude_sqlite_db(
            db_path=db_path,
            export_rows=rows,
            source_rows=source_rows,
            markdown_by_source_id={"000009": markdown_text},
        )

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            article = conn.execute(
                "SELECT wikitext, markdown_content, text_length FROM articles LIMIT 1"
            ).fetchone()
            self.assertIsNotNone(article)
            self.assertEqual(article["wikitext"], markdown_text)
            self.assertEqual(article["markdown_content"], markdown_text)
            self.assertEqual(article["text_length"], len(markdown_text))
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
