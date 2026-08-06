"""Ship agent skills into every research repository.

Skills follow the Agent Skills open standard: a directory containing a
`SKILL.md` with `name`/`description` frontmatter, optionally bundling
`references/`, `scripts/`, and `assets/`.

They are written to `.agents/skills/<name>/` as the canonical, vendor-neutral
location, and `.claude/skills/<name>` is symlinked to each so Claude Code and
similar clients discover them without a second copy to keep in sync. Where
symlinks are unavailable (Windows without Developer Mode) we fall back to
copying.

Updates are provenance-tracked: a skill whose on-disk content still matches
what the app last shipped is refreshed in place, while one the user has edited
is left alone and reported as diverged.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

AGENTS_DIR_NAME = ".agents"
SKILLS_DIR_NAME = "skills"
CLAUDE_DIR_NAME = ".claude"
BIN_DIR_NAME = "bin"
SKILL_FILE_NAME = "SKILL.md"
AGENTS_MD_NAME = "AGENTS.md"
MCP_CONFIG_NAME = ".mcp.json"

MANAGED_BEGIN = "<!-- ra:agents:begin -->"
MANAGED_END = "<!-- ra:agents:end -->"

STATUS_INSTALLED = "installed"
STATUS_REFRESHED = "refreshed"
STATUS_UNCHANGED = "unchanged"
STATUS_DIVERGED = "diverged"
STATUS_UNKNOWN_PROVENANCE = "unknown_provenance"
STATUS_ORPHANED = "orphaned"
STATUS_FAILED = "failed"

LINK_SYMLINKED = "symlinked"
LINK_COPIED = "copied"


@dataclass
class SkillSyncRecord:
    name: str
    status: str
    shipped_hash: str = ""
    on_disk_hash: str = ""
    link_mode: str = ""
    message: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            "name": self.name,
            "status": self.status,
            "shipped_hash": self.shipped_hash,
            "on_disk_hash": self.on_disk_hash,
            "link_mode": self.link_mode,
            "message": self.message,
        }


@dataclass
class SkillSyncReport:
    records: list[SkillSyncRecord] = field(default_factory=list)

    @property
    def names(self) -> list[str]:
        return [record.name for record in self.records if record.status != STATUS_ORPHANED]

    @property
    def diverged(self) -> list[str]:
        return [
            record.name
            for record in self.records
            if record.status in {STATUS_DIVERGED, STATUS_UNKNOWN_PROVENANCE}
        ]


def bundled_agent_skills_dir() -> Path:
    """Where the app keeps its shipped skills (tracked in version control)."""
    return Path(__file__).resolve().parents[2] / "data" / "agent_skills"


def bundled_agent_cli_dir() -> Path:
    """Where the app keeps the `ra` CLI it ships into every repository."""
    return Path(__file__).resolve().parents[2] / "data" / "agent_cli"


def sync_bundled_agent_cli(repo_root: Path, *, manifest_path: Path) -> list[SkillSyncRecord]:
    """Install `.agents/bin/ra` with the same provenance rules as a skill.

    One shared copy rather than one per skill: every skill references the same
    command, and an agent that reads `AGENTS.md` finds it in one place.
    """
    repo_root = Path(repo_root)
    source_dir = bundled_agent_cli_dir()
    target_dir = repo_root / AGENTS_DIR_NAME / BIN_DIR_NAME
    records: list[SkillSyncRecord] = []

    if not source_dir.is_dir():
        return records

    manifest = _load_manifest(manifest_path)
    recorded: dict = manifest.setdefault("bin", {})

    try:
        target_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return [SkillSyncRecord(name="ra", status=STATUS_FAILED, message=str(exc))]

    for source in sorted(source_dir.iterdir()):
        if not source.is_file() or source.name.startswith("."):
            continue
        record = _sync_one_file(source, target_dir / source.name, recorded)
        records.append(record)

    manifest["bin"] = recorded
    manifest["bin_report"] = [record.to_dict() for record in records]
    _save_manifest(manifest_path, manifest)
    return records


def _sync_one_file(source: Path, dest: Path, recorded: dict) -> SkillSyncRecord:
    name = source.name
    bundle_hash = _file_hash(source)
    entry = recorded.get(name) if isinstance(recorded.get(name), dict) else None
    shipped_hash = str((entry or {}).get("shipped_hash") or "")

    try:
        if not dest.exists():
            shutil.copy2(source, dest)
            dest.chmod(0o755)
            recorded[name] = {"shipped_hash": bundle_hash, "synced_at": _now()}
            return SkillSyncRecord(name=name, status=STATUS_INSTALLED, shipped_hash=bundle_hash)

        on_disk = _file_hash(dest)
        if not shipped_hash:
            return SkillSyncRecord(
                name=name,
                status=STATUS_UNKNOWN_PROVENANCE,
                on_disk_hash=on_disk,
                message="A file of this name already existed; left untouched.",
            )
        if on_disk != shipped_hash:
            return SkillSyncRecord(
                name=name,
                status=STATUS_DIVERGED,
                shipped_hash=shipped_hash,
                on_disk_hash=on_disk,
                message="Edited locally; the app's newer version was not installed.",
            )
        if on_disk == bundle_hash:
            dest.chmod(0o755)
            return SkillSyncRecord(name=name, status=STATUS_UNCHANGED, shipped_hash=bundle_hash)

        shutil.copy2(source, dest)
        dest.chmod(0o755)
        recorded[name] = {"shipped_hash": bundle_hash, "synced_at": _now()}
        return SkillSyncRecord(name=name, status=STATUS_REFRESHED, shipped_hash=bundle_hash)
    except OSError as exc:
        return SkillSyncRecord(name=name, status=STATUS_FAILED, message=str(exc))


def _file_hash(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ""


def repo_skills_dir(repo_root: Path) -> Path:
    return Path(repo_root) / AGENTS_DIR_NAME / SKILLS_DIR_NAME


def skill_tree_hash(skill_dir: Path) -> str:
    """Hash the whole skill directory, so edits to references/ count too."""
    digest = hashlib.sha256()
    skill_dir = Path(skill_dir)
    if not skill_dir.is_dir():
        return ""
    for path in sorted(skill_dir.rglob("*")):
        if not path.is_file():
            continue
        digest.update(path.relative_to(skill_dir).as_posix().encode("utf-8"))
        digest.update(b"\x00")
        try:
            digest.update(path.read_bytes())
        except OSError:
            digest.update(b"<unreadable>")
        digest.update(b"\x00")
    return digest.hexdigest()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_manifest(manifest_path: Path) -> dict:
    if not manifest_path.is_file():
        return {"version": 1, "skills": {}, "last_report": []}
    try:
        data = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "skills": {}, "last_report": []}
    if not isinstance(data, dict):
        return {"version": 1, "skills": {}, "last_report": []}
    data.setdefault("version", 1)
    if not isinstance(data.get("skills"), dict):
        data["skills"] = {}
    return data


def _save_manifest(manifest_path: Path, data: dict) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def sync_bundled_agent_skills(repo_root: Path, *, manifest_path: Path) -> SkillSyncReport:
    """Install or refresh the app's skills in `<repo>/.agents/skills/`."""
    repo_root = Path(repo_root)
    bundle_root = bundled_agent_skills_dir()
    target_root = repo_skills_dir(repo_root)
    target_root.mkdir(parents=True, exist_ok=True)

    manifest = _load_manifest(manifest_path)
    recorded: dict = manifest["skills"]
    report = SkillSyncReport()

    bundled_names: set[str] = set()
    if bundle_root.is_dir():
        for source_dir in sorted(bundle_root.iterdir()):
            if not source_dir.is_dir() or not (source_dir / SKILL_FILE_NAME).is_file():
                continue
            name = source_dir.name
            bundled_names.add(name)
            report.records.append(
                _sync_one_skill(source_dir, target_root / name, name, recorded)
            )

    # A skill dropped from the bundle stays on disk -- it may be in use, and
    # deleting user-visible files is never worth the tidiness.
    for name in sorted(set(recorded) - bundled_names):
        if (target_root / name).is_dir():
            report.records.append(
                SkillSyncRecord(
                    name=name,
                    status=STATUS_ORPHANED,
                    message="No longer shipped with the app; left in place.",
                )
            )

    manifest["skills"] = recorded
    manifest["last_report"] = [record.to_dict() for record in report.records]
    manifest["synced_at"] = _now()
    _save_manifest(manifest_path, manifest)
    return report


def _sync_one_skill(
    source_dir: Path,
    dest_dir: Path,
    name: str,
    recorded: dict,
) -> SkillSyncRecord:
    bundle_hash = skill_tree_hash(source_dir)
    entry = recorded.get(name) if isinstance(recorded.get(name), dict) else None
    shipped_hash = str((entry or {}).get("shipped_hash") or "")

    try:
        if not dest_dir.exists():
            shutil.copytree(source_dir, dest_dir)
            recorded[name] = {"shipped_hash": bundle_hash, "synced_at": _now()}
            return SkillSyncRecord(
                name=name,
                status=STATUS_INSTALLED,
                shipped_hash=bundle_hash,
                on_disk_hash=bundle_hash,
            )

        on_disk_hash = skill_tree_hash(dest_dir)

        if not shipped_hash:
            # A directory we never wrote. Could be the user's own skill that
            # happens to share our name; do not touch it.
            return SkillSyncRecord(
                name=name,
                status=STATUS_UNKNOWN_PROVENANCE,
                shipped_hash=bundle_hash,
                on_disk_hash=on_disk_hash,
                message="A skill of this name already existed; left untouched.",
            )

        if on_disk_hash != shipped_hash:
            return SkillSyncRecord(
                name=name,
                status=STATUS_DIVERGED,
                shipped_hash=shipped_hash,
                on_disk_hash=on_disk_hash,
                message="Edited locally; the app's newer version was not installed.",
            )

        if on_disk_hash == bundle_hash:
            return SkillSyncRecord(
                name=name,
                status=STATUS_UNCHANGED,
                shipped_hash=bundle_hash,
                on_disk_hash=on_disk_hash,
            )

        shutil.rmtree(dest_dir)
        shutil.copytree(source_dir, dest_dir)
        recorded[name] = {"shipped_hash": bundle_hash, "synced_at": _now()}
        return SkillSyncRecord(
            name=name,
            status=STATUS_REFRESHED,
            shipped_hash=bundle_hash,
            on_disk_hash=bundle_hash,
        )
    except OSError as exc:
        return SkillSyncRecord(name=name, status=STATUS_FAILED, message=str(exc))


def ensure_claude_skill_links(repo_root: Path, names: list[str]) -> list[SkillSyncRecord]:
    """Point `.claude/skills/<name>` at the canonical `.agents/skills/<name>`."""
    repo_root = Path(repo_root)
    links_root = repo_root / CLAUDE_DIR_NAME / SKILLS_DIR_NAME
    records: list[SkillSyncRecord] = []

    try:
        links_root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return [SkillSyncRecord(name="*", status=STATUS_FAILED, message=str(exc))]

    for name in names:
        canonical = repo_skills_dir(repo_root) / name
        if not canonical.is_dir():
            continue
        link = links_root / name
        target_rel = Path("..") / ".." / AGENTS_DIR_NAME / SKILLS_DIR_NAME / name

        try:
            # `is_symlink` rather than `exists`: a broken link exists as a link
            # but reports False for `exists`, and must be repaired.
            if link.is_symlink():
                if os.readlink(link) == str(target_rel) and link.resolve() == canonical.resolve():
                    records.append(
                        SkillSyncRecord(
                            name=name, status=STATUS_UNCHANGED, link_mode=LINK_SYMLINKED
                        )
                    )
                    continue
                link.unlink()
            elif link.is_dir():
                if skill_tree_hash(link) == skill_tree_hash(canonical):
                    # Our own copy fallback from a previous run; refresh it.
                    shutil.rmtree(link)
                else:
                    records.append(
                        SkillSyncRecord(
                            name=name,
                            status=STATUS_DIVERGED,
                            link_mode=LINK_COPIED,
                            message=".claude/skills copy differs; left untouched.",
                        )
                    )
                    continue
            elif link.exists():
                link.unlink()

            os.symlink(target_rel, link, target_is_directory=True)
            records.append(
                SkillSyncRecord(name=name, status=STATUS_INSTALLED, link_mode=LINK_SYMLINKED)
            )
        except (OSError, NotImplementedError, AttributeError):
            # Windows without Developer Mode, or a filesystem with no symlink
            # support. A copy is worse but still discoverable.
            try:
                if link.exists() or link.is_symlink():
                    if link.is_dir() and not link.is_symlink():
                        shutil.rmtree(link)
                    else:
                        link.unlink()
                shutil.copytree(canonical, link)
                records.append(
                    SkillSyncRecord(name=name, status=STATUS_INSTALLED, link_mode=LINK_COPIED)
                )
            except OSError as exc:
                records.append(SkillSyncRecord(name=name, status=STATUS_FAILED, message=str(exc)))

    return records


def ensure_agents_md(repo_root: Path, skill_names: list[str]) -> bool:
    """Write or refresh the managed pointer block in the repo's AGENTS.md."""
    repo_root = Path(repo_root)
    path = repo_root / AGENTS_MD_NAME
    block = _agents_md_block(skill_names)

    try:
        if not path.exists():
            path.write_text(
                "# Agent guide\n\n"
                "This is a ResearchAssistant repository.\n\n" + block + "\n",
                encoding="utf-8",
            )
            return True

        existing = path.read_text(encoding="utf-8", errors="replace")
        if MANAGED_BEGIN in existing and MANAGED_END in existing:
            head, _, rest = existing.partition(MANAGED_BEGIN)
            _, _, tail = rest.partition(MANAGED_END)
            updated = f"{head}{block}{tail}"
        else:
            updated = existing.rstrip() + "\n\n" + block + "\n"

        if updated != existing:
            path.write_text(updated, encoding="utf-8")
            return True
        return False
    except OSError:
        return False


def _agents_md_block(skill_names: list[str]) -> str:
    listed = "\n".join(f"- `.agents/skills/{name}/SKILL.md`" for name in sorted(skill_names))
    return (
        f"{MANAGED_BEGIN}\n"
        "## ResearchAssistant skills\n\n"
        "This repository ships agent skills describing how to edit its data safely.\n"
        "Read the relevant `SKILL.md` before changing anything under `sources/`,\n"
        "`manifest.csv`, or `.ra_repo/`.\n\n"
        f"{listed}\n\n"
        "Run repository work through the bundled command rather than by hand:\n\n"
        "```bash\n"
        ".agents/bin/ra where      # what is the state, and what should I do next?\n"
        ".agents/bin/ra doctor     # is everything ready for a long job?\n"
        "```\n\n"
        "Every `ra` command prints a summary, then `--- json ---`, then JSON. Read the\n"
        "`next` field and run what it says.\n\n"
        "`.claude/skills/` symlinks to these, so Claude Code loads them automatically.\n\n"
        "**Never edit `.ra_repo/repository_state.json`, `manifest.csv`, or anything under\n"
        "`sources/` by hand.** Those files are cross-referenced, and a direct edit is\n"
        "silently undone -- or worse, half-applied -- the next time the app opens this\n"
        "repository. Use the operations API the skills describe; it plans, verifies, and\n"
        "rolls back.\n"
        f"{MANAGED_END}"
    )


def ensure_mcp_config(repo_root: Path, *, port: str = "7995") -> bool:
    """Drop a `.mcp.json` so Claude Code can reach the app's MCP endpoint."""
    path = Path(repo_root) / MCP_CONFIG_NAME
    if path.exists():
        return False
    payload = {
        "mcpServers": {
            "researchassistant": {
                "type": "http",
                "url": f"http://127.0.0.1:{port}/api/agent/v1/mcp",
                # The literal token lives in `.ra_repo/agent_tokens.json`. Keep
                # it out of a repo-root file that is far likelier to be
                # committed; the server reads this env var too.
                "headers": {"Authorization": "Bearer ${RA_AGENT_WRITE_TOKEN}"},
            }
        }
    }
    try:
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return True
    except OSError:
        return False


def ensure_repo_gitignore(repo_root: Path) -> bool:
    """Keep `.ra_repo/` out of git -- it holds the agent write token."""
    path = Path(repo_root) / ".gitignore"
    required = [".ra_repo/", ".mcp.json"]
    try:
        existing = path.read_text(encoding="utf-8", errors="replace") if path.is_file() else ""
        lines = {line.strip() for line in existing.splitlines()}
        missing = [item for item in required if item not in lines]
        if not missing:
            return False
        prefix = existing if not existing or existing.endswith("\n") else existing + "\n"
        if not existing:
            prefix = "# ResearchAssistant local state and agent credentials\n"
        path.write_text(prefix + "\n".join(missing) + "\n", encoding="utf-8")
        return True
    except OSError:
        return False
