"""File storage management for uploads, artifacts, exports, and settings."""

from __future__ import annotations

import json
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path

from backend.models.common import PipelineStage, StageStatus
from backend.storage.project_profiles import (
    list_project_profiles_in_dir,
    resolve_project_profile_yaml,
    sync_bundled_project_profiles,
)


class FileStore:
    def __init__(self, base_dir: Path, *, sync_project_profiles: bool = True):
        self.base_dir = base_dir
        self.uploads_dir = base_dir / "uploads"
        self.artifacts_dir = base_dir / "artifacts"
        self.exports_dir = base_dir / "exports"
        self.settings_path = base_dir / "settings.json"
        self.project_profiles_dir = base_dir / "project_profiles"
        for d in [self.uploads_dir, self.artifacts_dir, self.exports_dir, self.project_profiles_dir]:
            d.mkdir(parents=True, exist_ok=True)
        if sync_project_profiles:
            sync_bundled_project_profiles(self.project_profiles_dir)

    def create_job(self, *, prefix: str = "") -> str:
        normalized_prefix = str(prefix or "").strip()
        job_id = uuid.uuid4().hex[:12]
        if normalized_prefix:
            safe_prefix = normalized_prefix.rstrip("_")
            job_id = f"{safe_prefix}_{job_id}"
        (self.uploads_dir / job_id).mkdir(exist_ok=True)
        (self.artifacts_dir / job_id).mkdir(exist_ok=True)
        (self.exports_dir / job_id).mkdir(exist_ok=True)
        # Initialize status
        self.save_job_status(
            job_id,
            {
                "job_id": job_id,
                "current_stage": PipelineStage.PENDING.value,
                "stages": [
                    StageStatus(stage=s).model_dump(mode="json")
                    for s in [
                        PipelineStage.INGESTING,
                        PipelineStage.DETECTING_REFERENCES,
                        PipelineStage.PARSING_BIBLIOGRAPHY,
                        PipelineStage.DETECTING_CITATIONS,
                        PipelineStage.EXTRACTING_SENTENCES,
                        PipelineStage.MATCHING_CITATIONS,
                        PipelineStage.EXPORTING,
                    ]
                ],
                "progress_pct": 0.0,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": None,
            },
        )
        return job_id

    def save_upload(self, job_id: str, filename: str, content: bytes) -> Path:
        dest = self.uploads_dir / job_id / filename
        dest.write_bytes(content)
        return dest

    def get_upload_dir(self, job_id: str) -> Path:
        return self.uploads_dir / job_id

    def save_artifact(self, job_id: str, stage: str, data: dict) -> Path:
        dest = self.artifacts_dir / job_id / f"{stage}.json"
        dest.write_text(json.dumps(data, default=str, ensure_ascii=False, indent=2))
        return dest

    def load_artifact(self, job_id: str, stage: str) -> dict | None:
        path = self.artifacts_dir / job_id / f"{stage}.json"
        if not path.exists():
            return None
        return json.loads(path.read_text())

    def save_export(self, job_id: str, csv_content: str) -> Path:
        dest = self.exports_dir / job_id / "citations.csv"
        dest.write_text(csv_content, encoding="utf-8-sig")
        return dest

    def get_export_path(self, job_id: str) -> Path:
        return self.exports_dir / job_id / "citations.csv"

    def get_export_dir(self, job_id: str) -> Path:
        path = self.exports_dir / job_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_sources_output_dir(self, job_id: str) -> Path:
        path = self.get_export_dir(job_id) / "output_run"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def get_sources_manifest_csv_path(self, job_id: str) -> Path:
        return self.get_sources_output_dir(job_id) / "manifest.csv"

    def get_sources_manifest_xlsx_path(self, job_id: str) -> Path:
        return self.get_sources_output_dir(job_id) / "manifest.xlsx"

    def get_sources_bundle_path(self, job_id: str) -> Path:
        return self.get_export_dir(job_id) / "output_run.zip"

    def save_sources_manifest_csv(self, job_id: str, csv_content: str) -> Path:
        dest = self.get_sources_manifest_csv_path(job_id)
        dest.write_text(csv_content, encoding="utf-8-sig")
        return dest

    def save_sources_manifest_xlsx(self, job_id: str, content: bytes) -> Path:
        dest = self.get_sources_manifest_xlsx_path(job_id)
        dest.write_bytes(content)
        return dest

    def build_sources_bundle(self, job_id: str) -> Path:
        output_dir = self.get_sources_output_dir(job_id)
        bundle_path = self.get_sources_bundle_path(job_id)
        base_without_ext = bundle_path.with_suffix("")
        if bundle_path.exists():
            bundle_path.unlink()
        created = shutil.make_archive(
            str(base_without_ext),
            "zip",
            root_dir=output_dir,
        )
        return Path(created)

    def load_settings(self) -> dict:
        if not self.settings_path.exists():
            return {}
        try:
            return json.loads(self.settings_path.read_text())
        except (json.JSONDecodeError, OSError):
            return {}

    def save_settings(self, settings: dict) -> None:
        self.settings_path.write_text(
            json.dumps(settings, default=str, ensure_ascii=False, indent=2)
        )

    def delete_settings(self) -> None:
        self.settings_path.unlink(missing_ok=True)

    def get_job_status(self, job_id: str) -> dict | None:
        path = self.artifacts_dir / job_id / "_status.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return None

    def save_job_status(self, job_id: str, status: dict) -> None:
        path = self.artifacts_dir / job_id / "_status.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(status, default=str, ensure_ascii=False, indent=2))

    def get_source_status(self, job_id: str) -> dict | None:
        path = self.artifacts_dir / job_id / "_sources_status.json"
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            return None

    def save_source_status(self, job_id: str, status: dict) -> None:
        path = self.artifacts_dir / job_id / "_sources_status.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(status, default=str, ensure_ascii=False, indent=2))

    def list_project_profiles(self) -> list[dict]:
        """List available project profile YAML files."""
        return list_project_profiles_in_dir(self.project_profiles_dir)

    def load_project_profile(self, filename: str) -> str:
        """Read and return raw YAML text for a project profile.

        Raises ValueError if the file is not found or path traversal is attempted.
        """
        _, profile_yaml = resolve_project_profile_yaml(self.project_profiles_dir, filename)
        return profile_yaml

    def resolve_project_profile(
        self,
        filename: str,
        *,
        research_purpose: str = "",
        default_when_blank: bool = False,
    ) -> tuple[str, str]:
        return resolve_project_profile_yaml(
            self.project_profiles_dir,
            filename,
            research_purpose=research_purpose,
            default_when_blank=default_when_blank,
        )

    def sync_project_profiles_to(self, target_dir: Path) -> None:
        sync_bundled_project_profiles(self.project_profiles_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        try:
            if self.project_profiles_dir.resolve() == target_dir.resolve():
                return
        except OSError:
            pass

        for profile in list_project_profiles_in_dir(self.project_profiles_dir):
            source_path = self.project_profiles_dir / profile["filename"]
            destination = target_dir / profile["filename"]
            if destination.exists():
                continue
            shutil.copy2(source_path, destination)

    def job_exists(self, job_id: str) -> bool:
        return (self.artifacts_dir / job_id).is_dir()
