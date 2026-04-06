from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import repository
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


class RepositoryCitationRisExportApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="repo-citation-ris-api-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.service = AttachedRepositoryService(store=self.store)

        self.repo_dir = self.tmp_path / "repo"
        self.repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(self.repo_dir))
        self.service.import_source_list(
            filename="sources.csv",
            content=(
                "URL,Title\n"
                "https://example.com/a,Alpha Source\n"
                "https://example.com/b,Beta Source\n"
                "https://example.com/c,Gamma Source\n"
            ).encode("utf-8"),
        )

        state_path = self.repo_dir / ".ra_repo" / "repository_state.json"
        state = json.loads(state_path.read_text(encoding="utf-8"))
        rows = state.get("sources", [])

        rows[0]["title"] = "Alpha Source"
        rows[0]["author_names"] = "Jane Doe"
        rows[0]["publication_date"] = "2024-03-15"
        rows[0]["document_type"] = "report"
        rows[0]["organization_name"] = "Alpha Agency"
        rows[0]["catalog_file"] = "catalogs/000001_catalog.json"
        rows[0]["catalog_status"] = "generated"
        rows[0]["rating_file"] = "ratings/000001_rating.json"
        rows[0]["rating_status"] = "generated"

        rows[1]["title"] = "Beta Source"
        rows[1]["author_names"] = "Beta Team"
        rows[1]["publication_date"] = "2023-09-01"
        rows[1]["document_type"] = "web page"
        rows[1]["organization_name"] = "Beta Blog"
        rows[1]["catalog_file"] = "catalogs/000002_catalog.json"
        rows[1]["catalog_status"] = "generated"
        rows[1]["rating_file"] = "ratings/000002_rating.json"
        rows[1]["rating_status"] = "generated"

        rows[2]["title"] = "Gamma Source"
        rows[2]["author_names"] = ""
        rows[2]["publication_date"] = ""
        rows[2]["document_type"] = "report"
        rows[2]["organization_name"] = "Gamma Lab"
        rows[2]["catalog_file"] = "catalogs/000003_catalog.json"
        rows[2]["catalog_status"] = "generated"

        state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")

        (self.repo_dir / "catalogs").mkdir(parents=True, exist_ok=True)
        (self.repo_dir / "ratings").mkdir(parents=True, exist_ok=True)
        (self.repo_dir / "catalogs" / "000001_catalog.json").write_text(
            json.dumps(
                {
                    "title": "Alpha Source",
                    "author_names": "Jane Doe",
                    "publication_date": "2024-03-15",
                    "publication_year": "2024",
                    "document_type": "report",
                    "organization_name": "Alpha Agency",
                    "citation": {
                        "item_type": "report",
                        "title": "Alpha Source",
                        "authors": [{"family": "Doe", "given": "Jane", "literal": ""}],
                        "issued": "2024-03-15",
                        "publisher": "Alpha Agency",
                        "doi": "10.1234/alpha",
                        "url": "https://example.com/a",
                        "report_number": "CEC-500-2025-029",
                        "confidence": 0.95,
                        "verification_status": "verified",
                        "verification_confidence": 0.95,
                        "missing_fields": [],
                        "ready_for_ris": True,
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (self.repo_dir / "catalogs" / "000002_catalog.json").write_text(
            json.dumps(
                {
                    "title": "Beta Source",
                    "author_names": "Beta Team",
                    "publication_date": "2023-09-01",
                    "publication_year": "2023",
                    "document_type": "web page",
                    "organization_name": "Beta Blog",
                    "citation": {
                        "item_type": "",
                        "title": "Beta Source",
                        "authors": [{"family": "", "given": "", "literal": "Beta Team"}],
                        "issued": "2023-09-01",
                        "publisher": "Beta Blog",
                        "url": "https://example.com/b",
                        "confidence": 0.9,
                        "verification_status": "verified",
                        "verification_confidence": 0.9,
                        "missing_fields": [],
                        "ready_for_ris": True,
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (self.repo_dir / "catalogs" / "000003_catalog.json").write_text(
            json.dumps(
                {
                    "title": "Gamma Source",
                    "document_type": "report",
                    "organization_name": "Gamma Lab",
                    "citation": {
                        "item_type": "report",
                        "title": "Gamma Source",
                        "publisher": "Gamma Lab",
                        "url": "https://example.com/c",
                        "missing_fields": ["authors", "publication_year"],
                        "confidence": 0.5,
                        "verification_status": "blocked",
                        "verification_confidence": 0.5,
                        "ready_for_ris": False,
                    },
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (self.repo_dir / "ratings" / "000001_rating.json").write_text(
            json.dumps(
                {
                    "overall_score": 0.9,
                    "ratings": {"overall_relevance": 0.93},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        (self.repo_dir / "ratings" / "000002_rating.json").write_text(
            json.dumps(
                {
                    "overall_score": 0.4,
                    "ratings": {"overall_relevance": 0.2},
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        app = FastAPI()
        app.state.file_store = self.store
        app.state.repository_service = self.service
        app.include_router(repository.router, prefix="/api")
        self.client = TestClient(app)

    def tearDown(self):
        self._tmp.cleanup()

    def test_exports_ris_for_whole_manifest_and_skips_incomplete_rows(self):
        response = self.client.post(
            "/api/repository/citations/export-ris",
            json={"scope": "all", "source_ids": [], "filters": {}},
        )

        self.assertEqual(response.status_code, 200)
        text = response.content.decode("utf-8")
        self.assertIn("TI  - Alpha Source", text)
        self.assertIn("TI  - Beta Source", text)
        self.assertNotIn("TI  - Gamma Source", text)
        self.assertEqual(response.headers["x-researchassistant-requested-count"], "3")
        self.assertEqual(response.headers["x-researchassistant-exported-count"], "2")
        self.assertEqual(response.headers["x-researchassistant-skipped-count"], "1")

    def test_exports_ris_for_selected_rows(self):
        response = self.client.post(
            "/api/repository/citations/export-ris",
            json={"scope": "selected", "source_ids": ["000001"], "filters": {}},
        )

        self.assertEqual(response.status_code, 200)
        text = response.content.decode("utf-8")
        self.assertIn("TI  - Alpha Source", text)
        self.assertNotIn("TI  - Beta Source", text)
        self.assertEqual(response.headers["x-researchassistant-exported-count"], "1")

    def test_exports_filtered_ris_using_relevance_threshold(self):
        response = self.client.post(
            "/api/repository/citations/export-ris",
            json={
                "scope": "filtered",
                "source_ids": [],
                "filters": {"rating_overall_relevance_min": 0.9},
            },
        )

        self.assertEqual(response.status_code, 200)
        text = response.content.decode("utf-8")
        self.assertIn("TI  - Alpha Source", text)
        self.assertNotIn("TI  - Beta Source", text)
        self.assertEqual(response.headers["x-researchassistant-requested-count"], "1")
        self.assertEqual(response.headers["x-researchassistant-exported-count"], "1")

    def test_exports_empty_ris_when_no_rows_match_filter(self):
        response = self.client.post(
            "/api/repository/citations/export-ris",
            json={
                "scope": "filtered",
                "source_ids": [],
                "filters": {"rating_overall_relevance_min": 0.99},
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content.decode("utf-8"), "")
        self.assertEqual(response.headers["x-researchassistant-requested-count"], "0")
        self.assertEqual(response.headers["x-researchassistant-exported-count"], "0")
        self.assertEqual(response.headers["x-researchassistant-skipped-count"], "0")


if __name__ == "__main__":
    unittest.main()
