from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.models.export import ExportArtifact, ExportRow
from backend.models.sources import SourceManifestRow
from backend.routers import results
from backend.storage.file_store import FileStore


class ResultsSqliteExportTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="results-export-sqlite-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(self.tmp_path / "data")
        app = FastAPI()
        app.state.file_store = self.store
        app.include_router(results.router, prefix="/api")
        self.client = TestClient(app)

    def tearDown(self):
        self._tmp.cleanup()

    def _seed_job_with_markdown(self) -> tuple[str, str]:
        job_id = self.store.create_job()
        markdown_text = "# Exported Markdown\n\nThis markdown should be embedded in the SQLite export."
        export_rows = [
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
            )
        ]
        export_artifact = ExportArtifact(
            rows=export_rows,
            total_citations_found=1,
            total_bib_entries=1,
            matched_count=1,
            unmatched_count=0,
        )
        self.store.save_artifact(job_id, "05_export", export_artifact.model_dump(mode="json"))

        source_row = SourceManifestRow(
            id="000001",
            repository_source_id="000001",
            original_url="https://example.com/a",
            title="A Useful Source",
            markdown_file="markdown/000001_clean.md",
        )
        self.store.save_artifact(
            job_id,
            "06_sources_manifest",
            {
                "rows": [source_row.model_dump(mode="json")],
                "total_urls": 1,
                "success_count": 1,
                "failed_count": 0,
                "partial_count": 0,
            },
        )

        output_dir = self.store.get_sources_output_dir(job_id)
        markdown_path = output_dir / "markdown" / "000001_clean.md"
        markdown_path.parent.mkdir(parents=True, exist_ok=True)
        markdown_path.write_text(markdown_text, encoding="utf-8")
        return job_id, markdown_text

    def test_export_sqlite_embeds_source_markdown(self):
        job_id, markdown_text = self._seed_job_with_markdown()
        response = self.client.get(f"/api/export/{job_id}/sqlite")
        self.assertEqual(response.status_code, 200)

        downloaded_db = self.tmp_path / "downloaded_export.db"
        downloaded_db.write_bytes(response.content)

        conn = sqlite3.connect(downloaded_db)
        try:
            row = conn.execute(
                "SELECT wikitext, markdown_content FROM articles WHERE page_id = 1"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], markdown_text)
            self.assertEqual(row[1], markdown_text)
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
