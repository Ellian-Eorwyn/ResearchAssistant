from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.models.ingestion_profiles import IngestionProfile, IngestionProfileSuggestion
from backend.routers import repository
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore


class RepositoryIngestionProfilesApiTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="repo-ingestion-profiles-api-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.service = AttachedRepositoryService(store=self.store)

        self.repo_dir = self.tmp_path / "repo"
        self.repo_dir.mkdir(parents=True, exist_ok=True)
        self.service.attach(str(self.repo_dir))

        app = FastAPI()
        app.state.file_store = self.store
        app.state.repository_service = self.service
        app.include_router(repository.router, prefix="/api")
        self.client = TestClient(app)

    def tearDown(self):
        self._tmp.cleanup()

    def test_ingestion_profiles_support_crud(self):
        list_response = self.client.get("/api/repository/ingestion-profiles")
        self.assertEqual(list_response.status_code, 200)
        payload = list_response.json()
        self.assertEqual(payload["default_profile_id"], "generic_numeric_academic")
        self.assertTrue(any(item["built_in"] for item in payload["profiles"]))

        profile = {
            "profile_id": "custom_vendor_report",
            "label": "Vendor Report",
            "description": "Rules for vendor deep research exports.",
            "built_in": False,
            "file_type_hints": ["md"],
            "reference_heading_patterns": ["^Sources$"],
            "citation_marker_patterns": [r"\[\d+\]"],
            "bibliography_split_patterns": [r"^\d+\."],
            "llm_guidance": "Repair vendor report formatting.",
            "confidence_threshold": 0.7,
            "notes": ["Custom profile"],
        }

        create_response = self.client.post(
            "/api/repository/ingestion-profiles",
            json=profile,
        )
        self.assertEqual(create_response.status_code, 200)
        self.assertEqual(create_response.json()["profile"]["profile_id"], "custom_vendor_report")

        update_response = self.client.put(
            "/api/repository/ingestion-profiles/custom_vendor_report",
            json={**profile, "label": "Vendor Report Updated"},
        )
        self.assertEqual(update_response.status_code, 200)
        self.assertEqual(update_response.json()["profile"]["label"], "Vendor Report Updated")

        refreshed = self.client.get("/api/repository/ingestion-profiles").json()
        self.assertTrue(
            any(
                item["profile_id"] == "custom_vendor_report"
                and item["label"] == "Vendor Report Updated"
                for item in refreshed["profiles"]
            )
        )

        delete_response = self.client.delete(
            "/api/repository/ingestion-profiles/custom_vendor_report"
        )
        self.assertEqual(delete_response.status_code, 200)

        after_delete = self.client.get("/api/repository/ingestion-profiles").json()
        self.assertFalse(
            any(item["profile_id"] == "custom_vendor_report" for item in after_delete["profiles"])
        )

    @patch.object(AttachedRepositoryService, "_process_documents_worker", autospec=True)
    def test_process_documents_accepts_profile_override(self, mock_process_worker):
        response = self.client.post(
            "/api/repository/process-documents",
            data={"profile_override": "generic_author_year_academic"},
            files=[
                (
                    "files",
                    (
                        "report.md",
                        b"# Report\n\nAgency findings improved outcomes (Smith, 2024).\n",
                        "text/markdown",
                    ),
                )
            ],
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()

        self.assertEqual(payload["selected_profile_id"], "generic_author_year_academic")
        self.assertEqual(payload["document_normalization"][0]["selected_profile_id"], "generic_author_year_academic")

        job_store = self.service.job_store_for(payload["job_id"])
        context = job_store.load_artifact(payload["job_id"], "repo_processing_context")
        self.assertIsNotNone(context)
        self.assertEqual(context["profile_override"], "generic_author_year_academic")
        status = job_store.get_job_status(payload["job_id"])
        self.assertEqual(status["repository_preprocess_state"], "pending")
        self.assertTrue(mock_process_worker.called)

    def test_ingestion_profile_suggestions_can_be_accepted_and_rejected(self):
        accept_suggestion = IngestionProfileSuggestion(
            suggestion_id="suggest_accept",
            source_profile_id="generic_numeric_academic",
            proposed_profile=IngestionProfile(
                profile_id="custom_accept_profile",
                label="Accepted Profile",
                description="Accepted from test suggestion.",
                built_in=False,
                file_type_hints=["md"],
                reference_heading_patterns=["^Works Cited$"],
                citation_marker_patterns=[r"\[\d+\]"],
                bibliography_split_patterns=[r"^\d+\."],
                llm_guidance="Accepted guidance.",
                confidence_threshold=0.7,
                notes=["Accepted"],
            ),
            reason="Fallback succeeded on a recurring format.",
            example_filename="accept.md",
            example_excerpt="Example accept excerpt",
        )
        reject_suggestion = IngestionProfileSuggestion(
            suggestion_id="suggest_reject",
            source_profile_id="generic_numeric_academic",
            proposed_profile=IngestionProfile(
                profile_id="custom_reject_profile",
                label="Rejected Profile",
                description="Rejected from test suggestion.",
                built_in=False,
                file_type_hints=["pdf"],
                reference_heading_patterns=["^References$"],
                citation_marker_patterns=[r"\(\w+, \d{4}\)"],
                bibliography_split_patterns=[r"^[A-Z]"],
                llm_guidance="Rejected guidance.",
                confidence_threshold=0.65,
                notes=["Rejected"],
            ),
            reason="Should remain a pending suggestion until reviewed.",
            example_filename="reject.md",
            example_excerpt="Example reject excerpt",
        )
        self.service._save_ingestion_profile_suggestions(
            [accept_suggestion, reject_suggestion]
        )

        accept_response = self.client.post(
            "/api/repository/ingestion-profile-suggestions/suggest_accept/accept"
        )
        self.assertEqual(accept_response.status_code, 200)
        accept_payload = accept_response.json()
        self.assertEqual(
            accept_payload["accepted_profile"]["profile_id"],
            "custom_accept_profile",
        )

        profiles = self.client.get("/api/repository/ingestion-profiles").json()["profiles"]
        self.assertTrue(
            any(item["profile_id"] == "custom_accept_profile" for item in profiles)
        )

        reject_response = self.client.post(
            "/api/repository/ingestion-profile-suggestions/suggest_reject/reject"
        )
        self.assertEqual(reject_response.status_code, 200)
        reject_payload = reject_response.json()
        self.assertEqual(reject_payload["suggestion"]["status"], "rejected")

        suggestions = self.client.get("/api/repository/ingestion-profile-suggestions").json()[
            "suggestions"
        ]
        statuses = {item["suggestion_id"]: item["status"] for item in suggestions}
        self.assertEqual(statuses["suggest_accept"], "accepted")
        self.assertEqual(statuses["suggest_reject"], "rejected")


if __name__ == "__main__":
    unittest.main()
