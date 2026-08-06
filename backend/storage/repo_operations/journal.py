"""Reversible file moves for repository operations.

`_create_backup_snapshot_locked` copies the five state files, which is enough
to undo a state edit but nothing at all for the file renames a source-id remap
performs. `MoveJournal` covers that gap: every move is recorded to disk
*after* it succeeds, so a crash at any point leaves a journal that describes
exactly what to undo.

The journal and its staging directory live under `.ra_repo/operations/<run_id>/`
-- inside the repository, so `os.replace` is a rename rather than a copy, and
excluded from `_iter_paths_named`, so staged files can never be picked up by
the scan-merge on the next attach.
"""

from __future__ import annotations

import errno
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

OPERATIONS_DIR_NAME = "operations"
JOURNAL_FILE_NAME = "journal.json"
RESULT_FILE_NAME = "result.json"
STAGING_DIR_NAME = "staging"
BACKUP_DIR_NAME = "backup"

STATUS_OPEN = "open"
STATUS_APPLYING = "applying"
STATUS_COMMITTED = "committed"
STATUS_ROLLED_BACK = "rolled_back"


def operations_dir(repo_root: Path) -> Path:
    return repo_root / ".ra_repo" / OPERATIONS_DIR_NAME


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class JournalError(RuntimeError):
    """Raised when a journaled filesystem action cannot be completed."""


class MoveJournal:
    """An append-only, crash-safe record of file moves and overwrites."""

    def __init__(self, repo_root: Path, run_id: str) -> None:
        self.repo_root = repo_root
        self.run_id = run_id
        self.root = operations_dir(repo_root) / run_id
        self.staging_dir = self.root / STAGING_DIR_NAME
        self.backup_dir = self.root / BACKUP_DIR_NAME
        self.journal_path = self.root / JOURNAL_FILE_NAME
        self.result_path = self.root / RESULT_FILE_NAME
        self._data: dict[str, Any] = {
            "run_id": run_id,
            "operation": "",
            "status": STATUS_OPEN,
            "started_at": _now(),
            "finished_at": "",
            "state_backup_dir": "",
            "entries": [],
        }

    # -- lifecycle --------------------------------------------------------

    def begin(self, *, operation: str, state_backup_dir: Path | None) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self._data["operation"] = operation
        self._data["status"] = STATUS_APPLYING
        self._data["state_backup_dir"] = str(state_backup_dir or "")
        self._flush()

    def commit(self) -> None:
        self._data["status"] = STATUS_COMMITTED
        self._data["finished_at"] = _now()
        self._flush()
        shutil.rmtree(self.staging_dir, ignore_errors=True)

    def _flush(self) -> None:
        """Write the journal atomically, so a crash never truncates it."""
        self.root.mkdir(parents=True, exist_ok=True)
        tmp = self.journal_path.with_suffix(".json.tmp")
        tmp.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(tmp, self.journal_path)

    # -- recorded actions -------------------------------------------------

    def move(self, src: Path, dst: Path) -> None:
        """Move a file or directory, recording it so it can be undone."""
        src = Path(src)
        dst = Path(dst)
        if not src.exists():
            raise JournalError(f"Cannot move missing path: {src}")
        if dst.exists():
            raise JournalError(f"Refusing to move onto an existing path: {dst}")

        dst.parent.mkdir(parents=True, exist_ok=True)
        copied = False
        try:
            os.replace(src, dst)
        except OSError as exc:
            if exc.errno != errno.EXDEV:
                raise
            # `.ra_repo` on a different mount: fall back to copy-then-delete and
            # record both halves so rollback can still reverse it.
            if src.is_dir():
                shutil.copytree(src, dst)
                shutil.rmtree(src)
            else:
                shutil.copy2(src, dst)
                src.unlink()
            copied = True

        self._append({"action": "move", "src": str(src), "dst": str(dst), "copied": copied})

    def stash(self, path: Path) -> Path:
        """Back up a file that is about to be overwritten or removed."""
        path = Path(path)
        if not path.is_file():
            raise JournalError(f"Cannot stash missing file: {path}")
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        target = self.backup_dir / f"{len(self._data['entries']):04d}_{path.name}"
        shutil.copy2(path, target)
        self._append({"action": "stash", "src": str(path), "dst": str(target)})
        return target

    def record_created(self, path: Path) -> None:
        """Record a path this operation created, so rollback can remove it."""
        self._append({"action": "create", "src": "", "dst": str(path)})

    def protect(self, path: Path) -> None:
        """Make an about-to-be-written path reversible.

        Call before any direct write that bypasses `move` -- notably
        `_write_repository_source_metadata`, which rewrites a file in place.
        Without this, rollback restores the file at its old *location* but with
        the operation's mutated *content*.
        """
        path = Path(path)
        if path.is_file():
            self.stash(path)
        else:
            self.record_created(path)

    def _append(self, entry: dict[str, Any]) -> None:
        entry["at"] = _now()
        self._data["entries"].append(entry)
        self._flush()

    # -- undo -------------------------------------------------------------

    def rollback(self) -> bool:
        """Reverse every recorded action, newest first. Idempotent.

        Returns True when everything was reversed cleanly.
        """
        ok = True
        for entry in reversed(list(self._data.get("entries", []))):
            action = entry.get("action")
            src = Path(entry["src"]) if entry.get("src") else None
            dst = Path(entry["dst"]) if entry.get("dst") else None
            try:
                if action == "move" and src is not None and dst is not None:
                    if dst.exists() and not src.exists():
                        src.parent.mkdir(parents=True, exist_ok=True)
                        os.replace(dst, src)
                elif action == "stash" and src is not None and dst is not None:
                    if dst.is_file():
                        src.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copy2(dst, src)
                elif action == "create" and dst is not None:
                    if dst.is_dir():
                        shutil.rmtree(dst, ignore_errors=True)
                    elif dst.exists():
                        dst.unlink()
            except OSError:
                ok = False

        self._data["status"] = STATUS_ROLLED_BACK
        self._data["finished_at"] = _now()
        self._data["rollback_ok"] = ok
        self._flush()
        shutil.rmtree(self.staging_dir, ignore_errors=True)
        return ok

    def restore_state_files(self) -> bool:
        """Copy the five state files back from the pre-operation snapshot."""
        backup = str(self._data.get("state_backup_dir") or "")
        if not backup:
            return False
        backup_dir = Path(backup)
        if not backup_dir.is_dir():
            return False

        ok = True
        for snapshot in sorted(backup_dir.iterdir()):
            if not snapshot.is_file():
                continue
            target = _state_file_destination(self.repo_root, snapshot.name)
            if target is None:
                continue
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(snapshot, target)
            except OSError:
                ok = False
        return ok

    # -- crash recovery ---------------------------------------------------

    @classmethod
    def load(cls, repo_root: Path, run_id: str) -> "MoveJournal | None":
        journal = cls(repo_root, run_id)
        if not journal.journal_path.is_file():
            return None
        try:
            data = json.loads(journal.journal_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        if not isinstance(data, dict):
            return None
        journal._data = data
        return journal

    @classmethod
    def find_incomplete(cls, repo_root: Path) -> list["MoveJournal"]:
        """Every journal left mid-apply by a crash, oldest first."""
        base = operations_dir(repo_root)
        if not base.is_dir():
            return []
        found: list[MoveJournal] = []
        for child in sorted(base.iterdir()):
            if not child.is_dir():
                continue
            journal = cls.load(repo_root, child.name)
            if journal is None:
                continue
            if journal._data.get("status") == STATUS_APPLYING:
                found.append(journal)
        return found

    @property
    def operation(self) -> str:
        return str(self._data.get("operation") or "")

    @property
    def status(self) -> str:
        return str(self._data.get("status") or "")


def _state_file_destination(repo_root: Path, name: str) -> Path | None:
    """Map a snapshot filename back to where it belongs in the repository."""
    from backend.storage.attached_repository import (
        CITATIONS_CSV_NAME,
        INTERNAL_DIR_NAME,
        MANIFEST_CSV_NAME,
        MANIFEST_XLSX_NAME,
        META_FILE_NAME,
        STATE_FILE_NAME,
    )

    if name in {MANIFEST_CSV_NAME, MANIFEST_XLSX_NAME, CITATIONS_CSV_NAME}:
        return repo_root / name
    if name in {META_FILE_NAME, STATE_FILE_NAME}:
        return repo_root / INTERNAL_DIR_NAME / name
    return None
