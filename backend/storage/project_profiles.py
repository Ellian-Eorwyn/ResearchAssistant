"""Helpers for bundled and runtime project profile YAML files."""

from __future__ import annotations

import shutil
from pathlib import Path

DEFAULT_PROJECT_PROFILE_FILENAME = "default_project_profile.yaml"
PROJECT_PROFILE_PATTERNS = ("*.yaml", "*.yml")
_RESEARCH_PURPOSE_TOKENS = ("{{research_purpose}}", "{{ research_purpose }}")
_DEFAULT_RESEARCH_PURPOSE = "No explicit research purpose was provided."


def bundled_project_profiles_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "project_profiles"


def sync_bundled_project_profiles(target_dir: Path) -> None:
    """Copy bundled profile YAML files into the target directory when missing."""
    target_dir.mkdir(parents=True, exist_ok=True)
    source_dir = bundled_project_profiles_dir()
    if not source_dir.is_dir():
        return

    try:
        if source_dir.resolve() == target_dir.resolve():
            return
    except OSError:
        pass

    for pattern in PROJECT_PROFILE_PATTERNS:
        for source_path in sorted(source_dir.glob(pattern)):
            if not source_path.is_file():
                continue
            destination = target_dir / source_path.name
            if destination.exists():
                continue
            shutil.copy2(source_path, destination)


def list_project_profiles_in_dir(profiles_dir: Path) -> list[dict[str, str]]:
    if not profiles_dir.is_dir():
        return []

    profiles_by_filename: dict[str, dict[str, str]] = {}
    for pattern in PROJECT_PROFILE_PATTERNS:
        for path in sorted(profiles_dir.glob(pattern)):
            if not path.is_file():
                continue
            profiles_by_filename[path.name] = {"name": path.stem, "filename": path.name}

    return sorted(
        profiles_by_filename.values(),
        key=lambda item: (
            item["filename"] != DEFAULT_PROJECT_PROFILE_FILENAME,
            item["name"].lower(),
            item["filename"].lower(),
        ),
    )


def render_project_profile_yaml(profile_yaml: str, research_purpose: str = "") -> str:
    purpose = " ".join((research_purpose or "").split()) or _DEFAULT_RESEARCH_PURPOSE
    rendered = profile_yaml
    for token in _RESEARCH_PURPOSE_TOKENS:
        rendered = rendered.replace(token, purpose)
    return rendered


def resolve_project_profile_yaml(
    profiles_dir: Path,
    filename: str,
    *,
    research_purpose: str = "",
    default_when_blank: bool = False,
) -> tuple[str, str]:
    requested_name = filename or ""
    safe_name = Path(requested_name).name
    if safe_name != requested_name:
        raise ValueError(f"Invalid project profile filename: {filename}")
    if not safe_name and default_when_blank:
        safe_name = DEFAULT_PROJECT_PROFILE_FILENAME
    if not safe_name:
        return "", ""

    profile_path = profiles_dir / safe_name
    if not profile_path.is_file():
        raise ValueError(f"Project profile not found: {safe_name}")

    try:
        raw_yaml = profile_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(str(exc)) from exc

    return safe_name, render_project_profile_yaml(
        raw_yaml,
        research_purpose=research_purpose,
    )
