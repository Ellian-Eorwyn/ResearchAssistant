"""Tests for the transactional repository operations engine."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend.models.export import ExportRow
from backend.models.operations import VerifyIssue
from backend.models.sources import SourceManifestRow, SourcePhaseMetadata
from backend.storage.attached_repository import AttachedRepositoryService
from backend.storage.file_store import FileStore
from backend.storage.repo_operations import (
    OPERATIONS,
    apply_operation_locked,
    plan_operation_locked,
    recover_incomplete_operations_locked,
)
from backend.storage.repo_operations.journal import MoveJournal
from backend.storage.repo_operations.verify import verify_repository_locked
from backend.storage.repo_operations.context import load_context_locked

SKIP_PREFIXES = (".ra_repo/operations", ".ra_repo/backups")


def _load_rows(service):
    """Current source rows, for tests that need to seed an awkward state."""
    from backend.storage.attached_repository import _load_source_rows

    return _load_source_rows(service._load_state_locked().get("sources", []))


class _OperationsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory(prefix="repo-ops-tests-")
        self.tmp_path = Path(self._tmp.name)
        self.store = FileStore(base_dir=self.tmp_path / "app_data")
        self.service = AttachedRepositoryService(store=self.store)
        self.repo = self.tmp_path / "repo"
        self.repo.mkdir()
        self.service.attach(str(self.repo))

    def tearDown(self) -> None:
        self._tmp.cleanup()

    # -- helpers ---------------------------------------------------------

    def seed_source(
        self,
        source_id: str,
        url: str,
        *,
        parent: str = "",
        children: str = "",
        fetch_status: str = "success",
    ) -> SourceManifestRow:
        directory = self.repo / "sources" / source_id
        directory.mkdir(parents=True, exist_ok=True)
        (directory / f"{source_id}_source.html").write_text(
            f"<html>{source_id}</html>", encoding="utf-8"
        )
        (directory / f"{source_id}_clean.md").write_text(f"# {source_id}\n", encoding="utf-8")
        return SourceManifestRow(
            id=source_id,
            repository_source_id=source_id,
            source_kind="url",
            import_type="seed",
            original_url=url,
            fetch_status=fetch_status,
            raw_file=f"sources/{source_id}/{source_id}_source.html",
            markdown_file=f"sources/{source_id}/{source_id}_clean.md",
            discovered_from=parent,
            discovered_source_ids=children,
        )

    def commit(
        self,
        rows: list[SourceManifestRow],
        citations: list[ExportRow] | None = None,
        imports: list[dict] | None = None,
    ) -> None:
        citations = citations or []
        imports = imports or []
        with self.service._writer_lock():
            for row in rows:
                self.service._write_repository_source_metadata(row)
            self.service._save_state_locked(
                sources=rows, citations=citations, imports=imports
            )
            self.service._save_meta_locked(
                {
                    **self.service._load_meta_locked(),
                    "next_source_id": max((int(r.id) for r in rows), default=0) + 1,
                }
            )
            self.service._rebuild_outputs_locked(rows, citations)

    def plan(self, operation: str, params: dict):
        with self.service._writer_lock():
            return plan_operation_locked(self.service, operation, params)

    def apply(self, operation: str, params: dict, **kwargs):
        with self.service._writer_lock():
            return apply_operation_locked(self.service, operation, params, **kwargs)

    def state(self) -> dict:
        return json.loads((self.repo / ".ra_repo" / "repository_state.json").read_text())

    def rows_by_id(self) -> dict[str, dict]:
        return {row["id"]: row for row in self.state()["sources"]}

    def snapshot(self) -> dict[str, bytes]:
        """Every repository file, excluding operation scratch and backups.

        `manifest.xlsx` is skipped: the writer embeds a creation timestamp, so
        a regenerated copy is never byte-identical even when its content is
        unchanged. `manifest.csv` covers the same data and does compare.
        """
        result: dict[str, bytes] = {}
        for path in self.repo.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(self.repo).as_posix()
            if rel.startswith(SKIP_PREFIXES) or rel == "manifest.xlsx":
                continue
            result[rel] = path.read_bytes()
        return result

    def inbox(self) -> Path:
        path = self.repo / ".ra_repo" / "inbox"
        path.mkdir(parents=True, exist_ok=True)
        return path


class RemapSourceIdsTests(_OperationsTestCase):
    def test_plan_is_read_only(self) -> None:
        self.commit([self.seed_source("000003", "https://example.com/a")])
        before = self.snapshot()

        plan = self.plan("remap_source_ids", {"pairs": [{"url": "https://example.com/a", "new_id": "9"}]})

        self.assertEqual(self.snapshot(), before)
        self.assertTrue(plan.state_fingerprint)
        self.assertTrue(plan.changes)
        self.assertEqual(plan.blockers, [])

    def test_swap_two_ids(self) -> None:
        rows = [
            self.seed_source("000003", "https://example.com/a", children="000005"),
            self.seed_source("000005", "https://example.com/b", parent="000003"),
        ]
        citations = [
            ExportRow(repository_source_id="000003", cited_title="cite a"),
            ExportRow(repository_source_id="000005", cited_title="cite b"),
        ]
        imports = [{"import_id": "imp1", "source_ids": ["000003", "000005"]}]
        self.commit(rows, citations, imports)

        result = self.apply(
            "remap_source_ids",
            {
                "pairs": [
                    {"url": "https://example.com/a", "new_id": "5"},
                    {"url": "https://example.com/b", "new_id": "3"},
                ]
            },
        )

        self.assertEqual(result.status, "applied")
        self.assertTrue(result.verify_passed)

        by_id = self.rows_by_id()
        self.assertEqual(by_id["000005"]["original_url"], "https://example.com/a")
        self.assertEqual(by_id["000003"]["original_url"], "https://example.com/b")

        # Discovery links follow the sources, on both sides.
        self.assertEqual(by_id["000005"]["discovered_source_ids"], "000003")
        self.assertEqual(by_id["000003"]["discovered_from"], "000005")
        self.assertEqual(by_id["000005"]["discovered_from"], "")

        # Artifact paths and the files themselves.
        for source_id in ("000003", "000005"):
            self.assertEqual(
                by_id[source_id]["raw_file"],
                f"sources/{source_id}/{source_id}_source.html",
            )
            names = sorted(p.name for p in (self.repo / "sources" / source_id).iterdir())
            self.assertEqual(
                names,
                [
                    f"{source_id}_clean.md",
                    f"{source_id}_metadata.json",
                    f"{source_id}_source.html",
                ],
            )
            metadata = json.loads(
                (self.repo / "sources" / source_id / f"{source_id}_metadata.json").read_text()
            )
            self.assertEqual(metadata["id"], source_id)
            self.assertEqual(metadata["original_url"], by_id[source_id]["original_url"])

        state = self.state()
        self.assertEqual(
            {(c["repository_source_id"], c["cited_title"]) for c in state["citations"]},
            {("000005", "cite a"), ("000003", "cite b")},
        )
        self.assertEqual(sorted(state["imports"][0]["source_ids"]), ["000003", "000005"])

    def test_three_way_cycle(self) -> None:
        self.commit(
            [
                self.seed_source("000001", "https://example.com/1"),
                self.seed_source("000002", "https://example.com/2"),
                self.seed_source("000003", "https://example.com/3"),
            ]
        )

        result = self.apply(
            "remap_source_ids",
            {
                "pairs": [
                    {"source_id": "000001", "new_id": "2"},
                    {"source_id": "000002", "new_id": "3"},
                    {"source_id": "000003", "new_id": "1"},
                ]
            },
        )

        self.assertEqual(result.status, "applied", result.message)
        by_id = self.rows_by_id()
        self.assertEqual(by_id["000002"]["original_url"], "https://example.com/1")
        self.assertEqual(by_id["000003"]["original_url"], "https://example.com/2")
        self.assertEqual(by_id["000001"]["original_url"], "https://example.com/3")

    def test_survives_reattach(self) -> None:
        """The regression this whole design guards against."""
        self.commit(
            [
                self.seed_source("000003", "https://example.com/a"),
                self.seed_source("000005", "https://example.com/b"),
            ]
        )
        self.apply(
            "remap_source_ids",
            {
                "pairs": [
                    {"url": "https://example.com/a", "new_id": "5"},
                    {"url": "https://example.com/b", "new_id": "3"},
                ]
            },
        )
        after_apply = self.rows_by_id()

        self.service.attach(str(self.repo))

        after_reattach = self.rows_by_id()
        self.assertEqual(
            {i: r["original_url"] for i, r in after_reattach.items()},
            {i: r["original_url"] for i, r in after_apply.items()},
        )

    def test_rolls_back_on_verify_failure(self) -> None:
        self.commit([self.seed_source("000003", "https://example.com/a")])
        before_files = self.snapshot()

        calls = {"n": 0}

        def only_second_call_fails(*_args, **_kwargs):
            calls["n"] += 1
            # The first call is the pre-operation baseline and must stay clean,
            # or the issue is treated as pre-existing and correctly ignored.
            if calls["n"] == 1:
                return []
            return [VerifyIssue(code="synthetic", message="forced", subject="test")]

        with mock.patch(
            "backend.storage.repo_operations.verify_repository_locked",
            side_effect=only_second_call_fails,
        ):
            result = self.apply(
                "remap_source_ids",
                {"pairs": [{"url": "https://example.com/a", "new_id": "9"}]},
            )

        self.assertEqual(result.status, "rolled_back")
        self.assertTrue(result.rollback_performed)
        self.assertTrue(result.rollback_ok)
        self.assertEqual([i.code for i in result.verify_issues], ["synthetic"])
        self.assertEqual(self.snapshot(), before_files)

    def test_rolls_back_when_a_file_move_fails(self) -> None:
        self.commit(
            [
                self.seed_source("000001", "https://example.com/1"),
                self.seed_source("000002", "https://example.com/2"),
            ]
        )
        before_files = self.snapshot()

        real_replace = os.replace
        calls = {"n": 0}

        def fail_on_third(src, dst, *args, **kwargs):
            # Let the journal's own bookkeeping through; only break real moves.
            if str(dst).endswith(".json"):
                return real_replace(src, dst, *args, **kwargs)
            calls["n"] += 1
            if calls["n"] == 3:
                raise OSError("simulated disk failure")
            return real_replace(src, dst, *args, **kwargs)

        with mock.patch(
            "backend.storage.repo_operations.journal.os.replace", side_effect=fail_on_third
        ):
            result = self.apply(
                "remap_source_ids",
                {
                    "pairs": [
                        {"source_id": "000001", "new_id": "8"},
                        {"source_id": "000002", "new_id": "9"},
                    ]
                },
            )

        self.assertEqual(result.status, "rolled_back")
        self.assertIn("simulated disk failure", result.message)
        self.assertEqual(self.snapshot(), before_files)

    def test_baseline_tolerates_pre_existing_damage(self) -> None:
        """A repo that is already imperfect must not be permanently frozen."""
        rows = [self.seed_source("000001", "https://example.com/1")]
        rows[0].summary_file = "sources/000001/000001_summary.md"  # never written
        self.commit(rows)

        with self.service._writer_lock():
            issues = verify_repository_locked(self.service, load_context_locked(self.service))
        self.assertIn("missing_artifact", {issue.code for issue in issues})

        result = self.apply(
            "remap_source_ids", {"pairs": [{"source_id": "000001", "new_id": "7"}]}
        )

        self.assertEqual(result.status, "applied", result.message)

    def test_blocks_on_unknown_and_ambiguous_urls(self) -> None:
        self.commit(
            [
                self.seed_source("000001", "https://example.com/same"),
                self.seed_source("000002", "https://example.com/same?utm_source=x"),
            ]
        )
        before = self.snapshot()

        missing = self.plan(
            "remap_source_ids", {"pairs": [{"url": "https://nope.invalid/", "new_id": "5"}]}
        )
        self.assertEqual([i.code for i in missing.blockers], ["url_not_found"])

        ambiguous = self.plan(
            "remap_source_ids", {"pairs": [{"url": "https://example.com/same", "new_id": "5"}]}
        )
        self.assertEqual([i.code for i in ambiguous.blockers], ["url_ambiguous"])

        result = self.apply(
            "remap_source_ids", {"pairs": [{"url": "https://nope.invalid/", "new_id": "5"}]}
        )
        self.assertEqual(result.status, "blocked")
        self.assertEqual(self.snapshot(), before)

    def test_blocks_on_colliding_new_id(self) -> None:
        self.commit(
            [
                self.seed_source("000001", "https://example.com/1"),
                self.seed_source("000002", "https://example.com/2"),
            ]
        )
        plan = self.plan(
            "remap_source_ids", {"pairs": [{"source_id": "000001", "new_id": "2"}]}
        )
        self.assertEqual([i.code for i in plan.blockers], ["new_id_collides"])

    def test_blocks_on_duplicate_new_ids(self) -> None:
        self.commit(
            [
                self.seed_source("000001", "https://example.com/1"),
                self.seed_source("000002", "https://example.com/2"),
            ]
        )
        plan = self.plan(
            "remap_source_ids",
            {
                "pairs": [
                    {"source_id": "000001", "new_id": "7"},
                    {"source_id": "000002", "new_id": "7"},
                ]
            },
        )
        self.assertEqual([i.code for i in plan.blockers], ["new_id_duplicate"])

    def test_rejects_invalid_new_id(self) -> None:
        self.commit([self.seed_source("000001", "https://example.com/1")])
        plan = self.plan(
            "remap_source_ids", {"pairs": [{"source_id": "000001", "new_id": "abc"}]}
        )
        self.assertEqual([i.code for i in plan.blockers], ["new_id_invalid"])

    def test_noop_when_id_already_matches(self) -> None:
        self.commit([self.seed_source("000001", "https://example.com/1")])
        result = self.apply(
            "remap_source_ids", {"pairs": [{"source_id": "000001", "new_id": "1"}]}
        )
        self.assertEqual(result.status, "noop")
        self.assertIn("remap_noop", {i.code for i in result.plan.warnings})

    def test_stray_manifest_blocks_only_when_it_would_win(self) -> None:
        self.commit(
            [self.seed_source("000001", "https://example.com/a", fetch_status="queued")]
        )
        stray_dir = self.repo / "old_export"
        stray_dir.mkdir()
        shutil.copy2(self.repo / "manifest.csv", stray_dir / "manifest.csv")

        # The stray copy claims `success` against our `queued` row, so the
        # scan-merge on the next attach would prefer it and restore the old id.
        text = (stray_dir / "manifest.csv").read_text(encoding="utf-8-sig")
        (stray_dir / "manifest.csv").write_text(
            text.replace(",queued,", ",success,"), encoding="utf-8-sig"
        )

        plan = self.plan(
            "remap_source_ids", {"pairs": [{"source_id": "000001", "new_id": "4"}]}
        )
        self.assertIn("stray_manifest_would_override", {i.code for i in plan.blockers})

        # Same stray file, but now our row is at least as good: a warning only.
        self.commit(
            [self.seed_source("000001", "https://example.com/a", fetch_status="success")]
        )
        plan = self.plan(
            "remap_source_ids", {"pairs": [{"source_id": "000001", "new_id": "4"}]}
        )
        self.assertEqual(plan.blockers, [])
        self.assertIn("stray_manifest_present", {i.code for i in plan.warnings})

    def test_blocked_while_a_job_is_running(self) -> None:
        self.commit([self.seed_source("000001", "https://example.com/1")])

        class _AliveThread:
            def is_alive(self) -> bool:
                return True

        self.service._download_thread = _AliveThread()
        try:
            result = self.apply(
                "remap_source_ids", {"pairs": [{"source_id": "000001", "new_id": "4"}]}
            )
        finally:
            self.service._download_thread = None

        self.assertEqual(result.status, "blocked")
        self.assertEqual([i.code for i in result.plan.blockers], ["repository_busy"])

    def test_stale_fingerprint_is_refused(self) -> None:
        self.commit([self.seed_source("000001", "https://example.com/1")])
        before = self.snapshot()

        result = self.apply(
            "remap_source_ids",
            {"pairs": [{"source_id": "000001", "new_id": "4"}]},
            expected_fingerprint="not-the-current-one",
        )

        self.assertEqual(result.status, "blocked")
        self.assertEqual([i.code for i in result.plan.blockers], ["state_changed"])
        self.assertEqual(self.snapshot(), before)

    def test_next_source_id_is_raised(self) -> None:
        self.commit([self.seed_source("000001", "https://example.com/1")])
        self.apply("remap_source_ids", {"pairs": [{"source_id": "000001", "new_id": "50"}]})
        meta = json.loads((self.repo / ".ra_repo" / "repository.json").read_text())
        self.assertGreater(meta["next_source_id"], 50)


class AttachFilesTests(_OperationsTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.row = self.seed_source("000001", "https://example.com/paper", fetch_status="queued")
        self.row.raw_file = ""
        self.row.markdown_file = ""
        shutil.rmtree(self.repo / "sources" / "000001")
        self.commit([self.row])

    def test_attaches_to_existing_source_by_filename_prefix(self) -> None:
        (self.inbox() / "000001_clean.md").write_text("# Recovered\n", encoding="utf-8")

        result = self.apply("attach_files", {})

        self.assertEqual(result.status, "applied", result.message)
        row = self.rows_by_id()["000001"]
        self.assertEqual(row["markdown_file"], "sources/000001/000001_clean.md")
        self.assertEqual(row["phase_metadata"]["convert"]["status"], "completed")
        # A blank or queued status would keep re-queueing the failed download.
        self.assertEqual(row["fetch_status"], "success")
        self.assertIn("manual_attach", row["notes"])
        # `sha256` identifies uploaded documents; a markdown file must not set it.
        self.assertEqual(row["sha256"], "")
        # Files are moved, so a successful apply empties the inbox.
        self.assertEqual(list(self.inbox().iterdir()), [])

    def test_ambiguous_extension_requires_a_role(self) -> None:
        (self.inbox() / "paper.pdf").write_bytes(b"%PDF-1.4 x")

        plan = self.plan(
            "attach_files", {"hints": [{"path": "paper.pdf", "source_id": "000001"}]}
        )
        self.assertEqual([i.code for i in plan.blockers], ["ambiguous_slot"])

        plan = self.plan(
            "attach_files",
            {"hints": [{"path": "paper.pdf", "source_id": "000001", "role": "raw_file"}]},
        )
        self.assertEqual(plan.blockers, [])

    def test_slot_collision_needs_overwrite(self) -> None:
        (self.inbox() / "000001_clean.md").write_text("# First\n", encoding="utf-8")
        self.apply("attach_files", {})

        (self.inbox() / "000001_clean.md").write_text("# Second\n", encoding="utf-8")
        plan = self.plan("attach_files", {})
        self.assertEqual([i.code for i in plan.blockers], ["slot_occupied"])

        stored = self.repo / "sources" / "000001" / "000001_clean.md"
        self.assertEqual(stored.read_text(), "# First\n")

        result = self.apply(
            "attach_files",
            {"hints": [{"path": "000001_clean.md", "overwrite": True}]},
        )
        self.assertEqual(result.status, "applied", result.message)
        self.assertEqual(stored.read_text(), "# Second\n")

    def test_identical_content_is_skipped(self) -> None:
        (self.inbox() / "000001_clean.md").write_text("# Same\n", encoding="utf-8")
        self.apply("attach_files", {})

        (self.inbox() / "000001_clean.md").write_text("# Same\n", encoding="utf-8")
        plan = self.plan("attach_files", {})
        self.assertEqual(plan.blockers, [])
        self.assertIn("already_attached", {i.code for i in plan.warnings})
        self.assertEqual(plan.changes, [])

    def test_rejects_paths_outside_the_repository(self) -> None:
        outside = self.tmp_path / "outside.md"
        outside.write_text("nope", encoding="utf-8")

        plan = self.plan("attach_files", {"scan_inbox": False, "paths": [str(outside)]})
        self.assertEqual([i.code for i in plan.blockers], ["path_outside_repository"])

        # inbox -> .ra_repo -> repo -> tmp, so this genuinely escapes.
        plan = self.plan(
            "attach_files", {"scan_inbox": False, "paths": ["../../../outside.md"]}
        )
        self.assertEqual([i.code for i in plan.blockers], ["path_outside_repository"])

        # A traversal that lands back inside the repo is not a containment
        # problem -- it is simply a file that is not there.
        plan = self.plan("attach_files", {"scan_inbox": False, "paths": ["../../nope.md"]})
        self.assertEqual([i.code for i in plan.blockers], ["file_not_found"])

    def test_accepts_any_folder_inside_the_repository(self) -> None:
        """A folder the user made in their own repo is a fine place to stage."""
        folder = self.repo / "user-provided-documents"
        folder.mkdir()
        (folder / "000001_clean.md").write_text("# Hand saved\n", encoding="utf-8")

        result = self.apply("attach_files", {"scan_inbox": False, "paths": [str(folder)]})

        self.assertEqual(result.status, "applied", result.message)
        self.assertEqual(
            self.rows_by_id()["000001"]["markdown_file"], "sources/000001/000001_clean.md"
        )

    def test_a_directory_is_expanded_to_its_files(self) -> None:
        folder = self.repo / "batch"
        (folder / "nested").mkdir(parents=True)
        (folder / "000001_summary.md").write_text("# Summary\n", encoding="utf-8")
        (folder / "nested" / "000001_rating.json").write_text("{}", encoding="utf-8")
        (folder / ".hidden.md").write_text("ignore me", encoding="utf-8")

        plan = self.plan("attach_files", {"scan_inbox": False, "paths": [str(folder)]})

        self.assertEqual(plan.blockers, [])
        targets = {c.field for c in plan.changes if c.kind == "row_field"}
        self.assertIn("summary_file", targets)
        self.assertIn("rating_file", targets)
        # Dotfiles are skipped rather than becoming mystery sources.
        self.assertNotIn(".hidden.md", " ".join(c.before for c in plan.changes))

    def test_rejects_managed_and_internal_paths(self) -> None:
        managed = self.repo / "sources" / "000001"
        managed.mkdir(parents=True, exist_ok=True)
        (managed / "000001_source.html").write_text("<html/>", encoding="utf-8")
        plan = self.plan(
            "attach_files",
            {"scan_inbox": False, "paths": [str(managed / "000001_source.html")]},
        )
        self.assertEqual([i.code for i in plan.blockers], ["path_already_managed"])

        internal = self.repo / ".ra_repo" / "repository_state.json"
        plan = self.plan("attach_files", {"scan_inbox": False, "paths": [str(internal)]})
        self.assertEqual([i.code for i in plan.blockers], ["path_is_internal_state"])

    def test_rejects_symlinks(self) -> None:
        target = self.tmp_path / "real.md"
        target.write_text("payload", encoding="utf-8")
        link = self.inbox() / "000001_clean.md"
        try:
            link.symlink_to(target)
        except (OSError, NotImplementedError):
            self.skipTest("symlinks unavailable on this platform")

        plan = self.plan("attach_files", {})
        self.assertEqual([i.code for i in plan.blockers], ["symlink_not_allowed"])

    def test_creates_new_source_like_manual_import(self) -> None:
        (self.inbox() / "brand_new.pdf").write_bytes(b"%PDF-1.4 new")

        result = self.apply("attach_files", {})
        self.assertEqual(result.status, "applied", result.message)

        created = self.rows_by_id()["000002"]
        # Compare field for field against a row the app's own import produced.
        self.service.import_manual_documents([("reference.pdf", b"%PDF-1.4 ref")])
        imported = next(
            row
            for row in self.state()["sources"]
            if row["source_document_name"] == "reference.pdf"
        )

        for field in (
            "source_kind",
            "import_type",
            "fetch_status",
            "fetch_method",
            "detected_type",
            "title_status",
        ):
            self.assertEqual(created[field], imported[field], f"{field} differs")
        self.assertEqual(created["raw_file"], "sources/000002/000002_brand_new.pdf")
        self.assertTrue(created["sha256"])

        state = self.state()
        manual = [i for i in state["imports"] if i.get("import_type") == "manual_attach"]
        self.assertEqual(manual[0]["source_ids"], ["000002"])

    def test_an_id_filename_creates_the_source_it_names(self) -> None:
        # How a planning sheet's document rows arrive: the user saves the file
        # under the id their sheet gave it, for a source that does not exist yet.
        (self.inbox() / "ID#109-ocr.pdf").write_bytes(b"%PDF-1.4 claude")

        result = self.apply("attach_files", {})

        self.assertEqual(result.status, "applied", result.message)
        rows = self.rows_by_id()
        self.assertIn("000109", rows)
        self.assertEqual(rows["000109"]["raw_file"], "sources/000109/000109_ID#109-ocr.pdf")
        # An uploaded document was never going to be fetched, so it is not a
        # fetch that succeeded -- it is a fetch that does not apply.
        self.assertEqual(rows["000109"]["fetch_status"], "not_applicable")
        self.assertEqual(rows["000109"]["source_kind"], "uploaded_document")

    def test_id_filename_variants_all_name_the_same_source(self) -> None:
        for name in ("ID-109.pdf", "id 109 copy.pdf", "ID#109.pdf", "ID109.pdf"):
            with self.subTest(name=name):
                candidate = self._candidate_for(name)
                self.assertEqual(candidate, "000109")

    def test_a_filename_starting_with_a_year_is_not_read_as_an_id(self) -> None:
        # `2024-report.pdf` is a normal name; reading it as source 2024 would put
        # the file somewhere the user never asked for.
        self.assertEqual(self._candidate_for("2024-report.pdf"), "")

    def _candidate_for(self, name: str) -> str:
        from backend.storage.repo_operations.attach_files import _ID_LABEL_RE

        match = _ID_LABEL_RE.match(name)
        return f"{int(match.group(1)):06d}" if match else ""

    def test_two_files_naming_one_id_is_a_blocker(self) -> None:
        (self.inbox() / "ID#109-a.pdf").write_bytes(b"%PDF-1.4 first")
        (self.inbox() / "ID#109-b.pdf").write_bytes(b"%PDF-1.4 second")

        plan = self.plan("attach_files", {})
        self.assertEqual([i.code for i in plan.blockers], ["id_claimed_twice"])

    def test_an_id_filename_for_an_existing_source_attaches_to_it(self) -> None:
        (self.inbox() / "ID#1-saved.pdf").write_bytes(b"%PDF-1.4 saved")

        result = self.apply(
            "attach_files", {"hints": [{"path": "ID#1-saved.pdf", "role": "raw_file"}]}
        )

        self.assertEqual(result.status, "applied", result.message)
        rows = self.rows_by_id()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows["000001"]["raw_file"], "sources/000001/000001_ID#1-saved.pdf")

    def test_can_refuse_to_create_new_sources(self) -> None:
        (self.inbox() / "orphan.pdf").write_bytes(b"%PDF-1.4 orphan")
        plan = self.plan("attach_files", {"allow_new_sources": False})
        self.assertEqual([i.code for i in plan.blockers], ["no_target_for_file"])

    def test_unknown_source_id_hint_is_a_blocker(self) -> None:
        (self.inbox() / "thing.md").write_text("x", encoding="utf-8")
        plan = self.plan(
            "attach_files",
            {"hints": [{"path": "thing.md", "source_id": "999999", "role": "summary_file"}]},
        )
        self.assertEqual([i.code for i in plan.blockers], ["unknown_source_id"])

    def test_metadata_file_cannot_be_attached(self) -> None:
        (self.inbox() / "thing.json").write_text("{}", encoding="utf-8")
        plan = self.plan(
            "attach_files",
            {"hints": [{"path": "thing.json", "source_id": "000001", "role": "metadata_file"}]},
        )
        self.assertEqual([i.code for i in plan.blockers], ["slot_not_writable"])

    def test_plan_is_read_only(self) -> None:
        (self.inbox() / "000001_clean.md").write_text("# x\n", encoding="utf-8")
        before = self.snapshot()
        self.plan("attach_files", {})
        self.assertEqual(self.snapshot(), before)

    def test_attaching_a_document_clears_a_failed_fetch(self) -> None:
        """The point of attaching by hand is to resolve a fetch that failed."""
        row = self.rows_by_id()["000001"]
        self.assertEqual(row["fetch_status"], "queued")

        with self.service._writer_lock():
            rows = _load_rows(self.service)
            rows[0].fetch_status = "failed"
            rows[0].error_message = "blocked_request: http_status_403"
            self.service._save_state_locked(sources=rows, citations=[], imports=[])
            self.service._rebuild_outputs_locked(rows, [])

        (self.inbox() / "saved-page.html").write_text(
            "<html><body><p>Real content.</p></body></html>", encoding="utf-8"
        )
        result = self.apply(
            "attach_files",
            {"hints": [{"path": "saved-page.html", "source_id": "000001", "role": "raw_file"}]},
        )

        self.assertEqual(result.status, "applied", result.message)
        updated = self.rows_by_id()["000001"]
        self.assertEqual(updated["fetch_status"], "success")
        self.assertEqual(updated["error_message"], "")
        self.assertEqual(updated["fetch_method"], "manual_attach")

    def _block_source(self, source_id: str = "000001") -> None:
        """Leave the row exactly as a blocked run does: held phases and all."""
        with self.service._writer_lock():
            rows = _load_rows(self.service)
            row = next(item for item in rows if item.id == source_id)
            row.fetch_status = "blocked"
            row.fetch_verification = "blocked_challenge"
            row.error_message = "blocked_challenge: bot wall"
            row.notes = "blocked_request"
            row.title = "Attention Required! | Cloudflare"
            for phase, field in (
                ("cleanup", "llm_cleanup_status"),
                ("title", "title_status"),
                ("catalog", "catalog_status"),
                ("summary", "summary_status"),
                ("rating", "rating_status"),
            ):
                setattr(row, field, "skipped_blocked_fetch")
                row.phase_metadata[phase] = SourcePhaseMetadata(
                    phase=phase, status="skipped", error_code="blocked_fetch"
                )
            self.service._save_state_locked(sources=rows, citations=[], imports=[])
            self.service._rebuild_outputs_locked(rows, [])

    def test_attaching_a_document_clears_a_blocked_fetch(self) -> None:
        """A bot wall is the case this operation exists to rescue."""
        self._block_source()
        (self.inbox() / "saved-page.html").write_text(
            "<html><head><title>Real Article</title></head><body><article><p>"
            + ("Real content about distributed energy resources. " * 60)
            + "</p></article></body></html>",
            encoding="utf-8",
        )
        result = self.apply(
            "attach_files",
            {"hints": [{"path": "saved-page.html", "source_id": "000001", "role": "raw_file"}]},
        )

        self.assertEqual(result.status, "applied", result.message)
        updated = self.rows_by_id()["000001"]
        self.assertEqual(updated["fetch_status"], "success")
        self.assertEqual(updated["error_message"], "")
        self.assertEqual(updated["fetch_verification"], "")
        self.assertEqual(updated["fetch_method"], "manual_attach")
        self.assertNotIn("blocked_request", updated["notes"])

    def test_resolving_a_block_releases_the_phases_it_held_back(self) -> None:
        """Skipped phases produced nothing, so staling cannot reach them."""
        self._block_source()
        (self.inbox() / "saved-page.html").write_text(
            "<html><head><title>Real Article</title></head><body><article><p>"
            + ("Real content about distributed energy resources. " * 60)
            + "</p></article></body></html>",
            encoding="utf-8",
        )
        self.apply(
            "attach_files",
            {"hints": [{"path": "saved-page.html", "source_id": "000001", "role": "raw_file"}]},
        )

        updated = self.rows_by_id()["000001"]
        for field in (
            "llm_cleanup_status",
            "catalog_status",
            "summary_status",
            "rating_status",
        ):
            self.assertEqual(updated[field], "", f"{field} still holds the block's skip")
        for phase in ("cleanup", "catalog", "summary", "rating"):
            self.assertNotIn(phase, updated.get("phase_metadata") or {})

    def test_the_plan_names_the_phases_a_block_was_holding(self) -> None:
        self._block_source()
        (self.inbox() / "saved-page.html").write_text(
            "<html><head><title>Real Article</title></head><body><article><p>"
            + ("Real content about distributed energy resources. " * 60)
            + "</p></article></body></html>",
            encoding="utf-8",
        )
        plan = self.plan(
            "attach_files",
            {"hints": [{"path": "saved-page.html", "source_id": "000001", "role": "raw_file"}]},
        )

        status_change = next(
            change for change in plan.changes if change.field == "fetch_status"
        )
        self.assertEqual(status_change.before, "blocked")
        self.assertEqual(status_change.after, "success")

        released = next(
            change
            for change in plan.changes
            if change.field == "phase_metadata.skipped_blocked_fetch"
        )
        self.assertIn("summary", released.before)
        self.assertEqual(released.after, "cleared")

    def test_attaching_the_saved_bot_wall_does_not_count_as_a_fix(self) -> None:
        """Saving the challenge screen must not silently mark the source good."""
        self._block_source()
        (self.inbox() / "saved-page.html").write_text(
            "<html><head><title>Just a moment...</title></head><body>"
            "<p>Verifying you are human. This may take a few seconds.</p>"
            "<p>Enable JavaScript and cookies to continue.</p></body></html>",
            encoding="utf-8",
        )
        plan = self.plan(
            "attach_files",
            {"hints": [{"path": "saved-page.html", "source_id": "000001", "role": "raw_file"}]},
        )
        self.assertIn(
            "attached_file_is_a_block_page",
            [issue.code for issue in plan.warnings],
        )
        self.assertEqual(
            [change for change in plan.changes if change.field == "fetch_status"], []
        )

        self.apply(
            "attach_files",
            {"hints": [{"path": "saved-page.html", "source_id": "000001", "role": "raw_file"}]},
        )
        updated = self.rows_by_id()["000001"]
        self.assertEqual(updated["fetch_status"], "blocked")
        self.assertEqual(updated["summary_status"], "skipped_blocked_fetch")
        # Stored anyway: it is evidence, and stranding it helps nobody.
        self.assertTrue(updated["raw_file"])

    def test_a_non_fetch_artifact_does_not_clear_a_blocked_fetch(self) -> None:
        """A summary says nothing about whether the page was ever retrieved."""
        self._block_source()
        (self.inbox() / "000001_summary.md").write_text("# Summary\n", encoding="utf-8")
        self.apply("attach_files", {})

        updated = self.rows_by_id()["000001"]
        self.assertEqual(updated["fetch_status"], "blocked")
        self.assertTrue(updated["summary_file"])

    def test_a_non_fetch_artifact_does_not_clear_a_failed_fetch(self) -> None:
        """A summary says nothing about whether the page was ever retrieved."""
        with self.service._writer_lock():
            rows = _load_rows(self.service)
            rows[0].fetch_status = "failed"
            rows[0].error_message = "blocked_request: http_status_403"
            self.service._save_state_locked(sources=rows, citations=[], imports=[])
            self.service._rebuild_outputs_locked(rows, [])

        (self.inbox() / "000001_summary.md").write_text("# Summary\n", encoding="utf-8")
        self.apply("attach_files", {})

        updated = self.rows_by_id()["000001"]
        self.assertEqual(updated["fetch_status"], "failed")
        self.assertTrue(updated["summary_file"])

    def test_reattaching_identical_content_converges_a_stale_status(self) -> None:
        """Re-running must fix the row, not skip because the bytes match."""
        (self.inbox() / "page.html").write_text("<html><p>Real.</p></html>", encoding="utf-8")
        self.apply(
            "attach_files",
            {"hints": [{"path": "page.html", "source_id": "000001", "role": "raw_file"}]},
        )

        # Simulate the row being left in a wrong state after the attach.
        with self.service._writer_lock():
            rows = _load_rows(self.service)
            rows[0].fetch_status = "failed"
            rows[0].error_message = "blocked_request: http_status_403"
            self.service._save_state_locked(sources=rows, citations=[], imports=[])
            self.service._rebuild_outputs_locked(rows, [])

        staged = self.inbox() / "page.html"
        staged.write_text("<html><p>Real.</p></html>", encoding="utf-8")
        result = self.apply(
            "attach_files",
            {"hints": [{"path": "page.html", "source_id": "000001", "role": "raw_file"}]},
        )

        self.assertEqual(result.status, "applied", result.message)
        updated = self.rows_by_id()["000001"]
        self.assertEqual(updated["fetch_status"], "success")
        self.assertEqual(updated["error_message"], "")
        # Identical content means no file was moved, so the staged copy stays.
        self.assertTrue(staged.is_file())
        self.assertTrue((self.repo / updated["raw_file"]).is_file())

    def test_identical_content_on_a_healthy_row_is_still_skipped(self) -> None:
        (self.inbox() / "000001_clean.md").write_text("# Same\n", encoding="utf-8")
        self.apply("attach_files", {})

        (self.inbox() / "000001_clean.md").write_text("# Same\n", encoding="utf-8")
        plan = self.plan("attach_files", {})
        self.assertIn("already_attached", {i.code for i in plan.warnings})
        self.assertEqual(plan.changes, [])

    def test_unsafe_filenames_are_sanitized(self) -> None:
        """A browser "Save page as" name must not make the repo unportable.

        Colons and pipes are illegal in Windows filenames, so storing them
        verbatim would produce a repository that cannot be checked out there.
        """
        from backend.storage.repo_operations.attach_files import safe_source_name

        self.assertEqual(
            safe_source_name("Explainer: What is a VPP? | Reuters.html"),
            "Explainer- What is a VPP- - Reuters.html",
        )
        self.assertNotIn(":", safe_source_name("a:b.html"))
        self.assertNotIn("|", safe_source_name("a|b.html"))
        # Long names still have to leave room for the id prefix.
        self.assertLessEqual(len(safe_source_name("z" * 300 + ".pdf")), 70)
        # Ordinary names are left alone.
        self.assertEqual(safe_source_name("000001_clean.md"), "000001_clean.md")

        source = self.inbox() / "Report: Q1 | Final.html"
        source.write_text("<html><body>hi</body></html>", encoding="utf-8")
        result = self.apply(
            "attach_files",
            {"hints": [{"path": source.name, "source_id": "000001", "role": "rendered_file"}]},
        )
        self.assertEqual(result.status, "applied", result.message)

        stored = self.rows_by_id()["000001"]["rendered_file"]
        self.assertNotIn(":", stored)
        self.assertNotIn("|", stored)
        self.assertTrue((self.repo / stored).is_file())


class VerifyTests(_OperationsTestCase):
    def issues_for(self, rows, citations=None, imports=None) -> set[str]:
        self.commit(rows, citations, imports)
        with self.service._writer_lock():
            return {
                issue.code
                for issue in verify_repository_locked(
                    self.service, load_context_locked(self.service)
                )
            }

    def test_clean_repository_has_no_issues(self) -> None:
        self.assertEqual(self.issues_for([self.seed_source("000001", "https://a.test/")]), set())

    def test_detects_missing_artifact(self) -> None:
        row = self.seed_source("000001", "https://a.test/")
        row.summary_file = "sources/000001/000001_summary.md"
        self.assertIn("missing_artifact", self.issues_for([row]))

    def test_detects_orphan_citation(self) -> None:
        codes = self.issues_for(
            [self.seed_source("000001", "https://a.test/")],
            [ExportRow(repository_source_id="000404", cited_title="ghost")],
        )
        self.assertIn("orphan_citation", codes)

    def test_detects_orphan_discovery_link(self) -> None:
        row = self.seed_source("000001", "https://a.test/", parent="000404")
        self.assertIn("orphan_discovery_link", self.issues_for([row]))

    def test_detects_orphan_import_reference(self) -> None:
        codes = self.issues_for(
            [self.seed_source("000001", "https://a.test/")],
            None,
            [{"import_id": "imp", "source_ids": ["000404"]}],
        )
        self.assertIn("orphan_import_ref", codes)

    def test_detects_id_mismatch(self) -> None:
        row = self.seed_source("000001", "https://a.test/")
        row.repository_source_id = "000002"
        self.assertIn("id_mismatch", self.issues_for([row]))

    def test_detects_duplicate_dedupe_key(self) -> None:
        codes = self.issues_for(
            [
                self.seed_source("000001", "https://a.test/page"),
                self.seed_source("000002", "https://a.test/page?utm_source=x"),
            ]
        )
        self.assertIn("dedupe_key_collision", codes)

    def test_detects_stray_source_directory(self) -> None:
        self.commit([self.seed_source("000001", "https://a.test/")])
        (self.repo / "sources" / "000099").mkdir(parents=True)
        with self.service._writer_lock():
            codes = {
                i.code
                for i in verify_repository_locked(
                    self.service, load_context_locked(self.service)
                )
            }
        self.assertIn("stray_source_dir", codes)

    def test_detects_stale_metadata_file(self) -> None:
        self.commit([self.seed_source("000001", "https://a.test/")])
        path = self.repo / "sources" / "000001" / "000001_metadata.json"
        payload = json.loads(path.read_text())
        payload["id"] = "000777"
        path.write_text(json.dumps(payload), encoding="utf-8")
        with self.service._writer_lock():
            codes = {
                i.code
                for i in verify_repository_locked(
                    self.service, load_context_locked(self.service)
                )
            }
        self.assertIn("stale_metadata_file", codes)

    def test_detects_next_source_id_too_low(self) -> None:
        self.commit([self.seed_source("000010", "https://a.test/")])
        with self.service._writer_lock():
            self.service._save_meta_locked(
                {**self.service._load_meta_locked(), "next_source_id": 2}
            )
            codes = {
                i.code
                for i in verify_repository_locked(
                    self.service, load_context_locked(self.service)
                )
            }
        self.assertIn("next_source_id_too_low", codes)


class JournalRecoveryTests(_OperationsTestCase):
    def test_incomplete_journal_is_rolled_back_on_attach(self) -> None:
        self.commit([self.seed_source("000001", "https://a.test/")])
        source_dir = self.repo / "sources" / "000001"
        before_files = sorted(p.name for p in source_dir.iterdir())

        # Simulate a crash midway through an apply: files moved, journal open.
        journal = MoveJournal(self.repo, "crashed")
        with self.service._writer_lock():
            backup = self.service._create_backup_snapshot_locked("pre_test")
        journal.begin(operation="remap_source_ids", state_backup_dir=backup)
        journal.move(source_dir, journal.staging_dir / "000001")
        self.assertFalse(source_dir.exists())
        self.assertEqual([j.run_id for j in MoveJournal.find_incomplete(self.repo)], ["crashed"])

        self.service.attach(str(self.repo))

        # Note: `attach` also backfills fields like `imported_at`, so compare
        # what recovery is responsible for rather than the whole tree.
        self.assertTrue(source_dir.is_dir())
        self.assertEqual(sorted(p.name for p in source_dir.iterdir()), before_files)
        self.assertEqual(MoveJournal.find_incomplete(self.repo), [])
        self.assertFalse(journal.staging_dir.exists())

        row = self.rows_by_id()["000001"]
        self.assertEqual(row["original_url"], "https://a.test/")
        self.assertEqual(row["raw_file"], "sources/000001/000001_source.html")

        with self.service._writer_lock():
            issues = verify_repository_locked(self.service, load_context_locked(self.service))
        self.assertEqual([i.code for i in issues], [])

    def test_recovery_is_idempotent(self) -> None:
        self.commit([self.seed_source("000001", "https://a.test/")])
        with self.service._writer_lock():
            first = recover_incomplete_operations_locked(self.service)
            second = recover_incomplete_operations_locked(self.service)
        self.assertEqual(first, [])
        self.assertEqual(second, [])

    def test_move_refuses_to_clobber(self) -> None:
        journal = MoveJournal(self.repo, "test")
        journal.begin(operation="test", state_backup_dir=None)
        a = self.tmp_path / "a.txt"
        b = self.tmp_path / "b.txt"
        a.write_text("a", encoding="utf-8")
        b.write_text("b", encoding="utf-8")
        with self.assertRaises(Exception):
            journal.move(a, b)


class RegistryTests(_OperationsTestCase):
    def test_registry_exposes_every_operation_with_schemas(self) -> None:
        descriptors = {item.name: item for item in self.service.list_repo_operations()}
        # Derived from the registry, so adding an operation does not need a test
        # edit -- what matters is that every registered one is fully described.
        self.assertEqual(set(descriptors), set(OPERATIONS.names()))
        self.assertGreaterEqual(len(descriptors), 4)
        for descriptor in descriptors.values():
            self.assertTrue(descriptor.description, descriptor.name)
            self.assertTrue(descriptor.title, descriptor.name)
            self.assertEqual(descriptor.input_schema.get("type"), "object", descriptor.name)

    def test_unknown_operation_raises(self) -> None:
        with self.assertRaises(ValueError):
            self.service.plan_repo_operation("nope", {})

    def test_invalid_params_become_blockers(self) -> None:
        plan = self.plan("remap_source_ids", {"pairs": "not-a-list"})
        self.assertEqual([i.code for i in plan.blockers], ["invalid_params"])

    def test_operation_names_are_sorted_and_resolvable(self) -> None:
        names = OPERATIONS.names()
        self.assertEqual(names, sorted(names))
        for name in names:
            self.assertIsNotNone(OPERATIONS.get(name))
        self.assertIsNone(OPERATIONS.get("does_not_exist"))


class CreateColumnsTests(_OperationsTestCase):
    def spec(self, label: str, prompt: str = "Say something useful.") -> dict:
        return {"label": label, "instruction_prompt": prompt}

    def test_creates_columns_in_one_transaction(self) -> None:
        result = self.apply(
            "create_columns",
            {"columns": [self.spec("Org Type"), self.spec("Year Published")]},
        )

        self.assertEqual(result.status, "applied", result.message)
        self.assertTrue(result.verify_passed)

        configs = self.state()["column_configs"]
        self.assertEqual([c["label"] for c in configs], ["Org Type", "Year Published"])
        self.assertTrue(all(c["kind"] == "custom" for c in configs))
        self.assertTrue(all(c["instruction_prompt"] for c in configs))
        self.assertTrue(all(c["include_source_text"] for c in configs))

    def test_existing_labels_are_skipped_on_a_re_run(self) -> None:
        params = {"columns": [self.spec("Org Type"), self.spec("Sector")]}
        self.apply("create_columns", params)

        plan = self.plan("create_columns", params)
        self.assertEqual(plan.blockers, [])
        self.assertEqual({i.code for i in plan.warnings}, {"column_label_exists"})
        self.assertEqual(plan.changes, [])

        self.assertEqual(len(self.state()["column_configs"]), 2)

    def test_label_matching_ignores_case_and_spacing(self) -> None:
        self.apply("create_columns", {"columns": [self.spec("Org Type")]})
        plan = self.plan("create_columns", {"columns": [self.spec("  org   type ")]})
        self.assertIn("column_label_exists", {i.code for i in plan.warnings})

    def test_blocks_on_missing_label_or_prompt(self) -> None:
        plan = self.plan("create_columns", {"columns": [{"label": "", "instruction_prompt": "x"}]})
        self.assertEqual([i.code for i in plan.blockers], ["label_required"])

        plan = self.plan("create_columns", {"columns": [{"label": "A", "instruction_prompt": " "}]})
        self.assertEqual([i.code for i in plan.blockers], ["prompt_required"])

    def test_blocks_on_duplicate_labels_within_the_request(self) -> None:
        plan = self.plan(
            "create_columns", {"columns": [self.spec("Sector"), self.spec("Sector")]}
        )
        self.assertEqual([i.code for i in plan.blockers], ["label_duplicate_in_request"])

    def test_output_constraint_is_stored(self) -> None:
        self.apply(
            "create_columns",
            {
                "columns": [
                    {
                        "label": "Sector",
                        "instruction_prompt": "Pick one.",
                        "output_constraint": {"kind": "text", "allowed_values": ["A", "B"]},
                    }
                ]
            },
        )
        config = self.state()["column_configs"][0]
        self.assertEqual(config["output_constraint"]["allowed_values"], ["A", "B"])

    def test_columns_survive_reattach(self) -> None:
        self.apply("create_columns", {"columns": [self.spec("Org Type")]})
        self.service.attach(str(self.repo))
        self.assertEqual([c["label"] for c in self.state()["column_configs"]], ["Org Type"])

    def test_plan_is_read_only(self) -> None:
        before = self.snapshot()
        self.plan("create_columns", {"columns": [self.spec("Org Type")]})
        self.assertEqual(self.snapshot(), before)


class CreateSourcesTests(_OperationsTestCase):
    def test_creates_sources_with_the_requested_ids(self) -> None:
        result = self.apply(
            "create_sources",
            {
                "sources": [
                    {"url": "https://example.com/a", "id": "20"},
                    {"url": "https://example.com/b", "id": "21"},
                ]
            },
        )

        self.assertEqual(result.status, "applied", result.message)
        self.assertTrue(result.verify_passed)

        by_id = self.rows_by_id()
        self.assertEqual(set(by_id), {"000020", "000021"})
        self.assertEqual(by_id["000020"]["original_url"], "https://example.com/a")
        # Queued is what the fetch phase selects on.
        self.assertEqual(by_id["000020"]["fetch_status"], "queued")
        self.assertEqual(by_id["000020"]["source_kind"], "url")

        meta = json.loads((self.repo / ".ra_repo" / "repository.json").read_text())
        self.assertEqual(meta["next_source_id"], 22)

    def test_auto_assigns_ids_around_requested_ones(self) -> None:
        result = self.apply(
            "create_sources",
            {
                "sources": [
                    {"url": "https://example.com/auto1"},
                    {"url": "https://example.com/pinned", "id": "1"},
                    {"url": "https://example.com/auto2"},
                ]
            },
        )
        self.assertEqual(result.status, "applied", result.message)

        by_url = {r["original_url"]: r["id"] for r in self.state()["sources"]}
        # The pinned id must not be stolen by an auto-assigned row.
        self.assertEqual(by_url["https://example.com/pinned"], "000001")
        self.assertEqual(len(set(by_url.values())), 3)

    def test_url_is_normalized(self) -> None:
        self.apply("create_sources", {"sources": [{"url": "example.com/thing", "id": "5"}]})
        self.assertEqual(
            self.rows_by_id()["000005"]["original_url"], "https://example.com/thing"
        )

    def test_existing_url_is_skipped_by_default(self) -> None:
        self.commit([self.seed_source("000001", "https://example.com/already")])

        plan = self.plan(
            "create_sources",
            {"sources": [{"url": "https://example.com/already", "id": "30"}]},
        )
        self.assertEqual(plan.blockers, [])
        self.assertIn("url_already_present", {i.code for i in plan.warnings})
        self.assertEqual(plan.changes, [])

    def test_existing_url_can_be_a_blocker_instead(self) -> None:
        self.commit([self.seed_source("000001", "https://example.com/already")])
        plan = self.plan(
            "create_sources",
            {
                "sources": [{"url": "https://example.com/already", "id": "30"}],
                "skip_existing": False,
            },
        )
        self.assertEqual([i.code for i in plan.blockers], ["url_already_present"])

    def test_blocks_on_taken_id(self) -> None:
        self.commit([self.seed_source("000007", "https://example.com/existing")])
        plan = self.plan(
            "create_sources", {"sources": [{"url": "https://example.com/new", "id": "7"}]}
        )
        self.assertEqual([i.code for i in plan.blockers], ["id_taken"])

    def test_blocks_on_duplicates_within_the_request(self) -> None:
        plan = self.plan(
            "create_sources",
            {
                "sources": [
                    {"url": "https://example.com/x", "id": "1"},
                    {"url": "https://example.com/y", "id": "1"},
                ]
            },
        )
        self.assertEqual([i.code for i in plan.blockers], ["id_duplicate_in_request"])

        plan = self.plan(
            "create_sources",
            {
                "sources": [
                    {"url": "https://example.com/x", "id": "1"},
                    {"url": "https://example.com/x?utm_source=n", "id": "2"},
                ]
            },
        )
        self.assertEqual([i.code for i in plan.blockers], ["duplicate_url_in_request"])

    def test_blocks_on_bad_input(self) -> None:
        plan = self.plan(
            "create_sources", {"sources": [{"url": "not a url at all", "id": "1"}]}
        )
        self.assertEqual([i.code for i in plan.blockers], ["url_invalid"])

        plan = self.plan(
            "create_sources", {"sources": [{"url": "https://example.com/x", "id": "abc"}]}
        )
        self.assertEqual([i.code for i in plan.blockers], ["id_invalid"])

        plan = self.plan("create_sources", {"sources": [{"url": "  ", "id": "1"}]})
        self.assertEqual([i.code for i in plan.blockers], ["url_required"])

    def test_plan_is_read_only(self) -> None:
        before = self.snapshot()
        self.plan("create_sources", {"sources": [{"url": "https://example.com/a", "id": "20"}]})
        self.assertEqual(self.snapshot(), before)

    def test_rolls_back_cleanly(self) -> None:
        self.commit([self.seed_source("000001", "https://example.com/seed")])
        before = self.snapshot()

        calls = {"n": 0}

        def only_second_call_fails(*_args, **_kwargs):
            calls["n"] += 1
            return (
                []
                if calls["n"] == 1
                else [VerifyIssue(code="synthetic", message="forced", subject="test")]
            )

        with mock.patch(
            "backend.storage.repo_operations.verify_repository_locked",
            side_effect=only_second_call_fails,
        ):
            result = self.apply(
                "create_sources", {"sources": [{"url": "https://example.com/new", "id": "40"}]}
            )

        self.assertEqual(result.status, "rolled_back")
        self.assertTrue(result.rollback_ok)
        self.assertFalse((self.repo / "sources" / "000040").exists())
        self.assertEqual(self.snapshot(), before)

    def test_created_sources_survive_reattach(self) -> None:
        self.apply(
            "create_sources",
            {
                "sources": [
                    {"url": "https://example.com/a", "id": "20"},
                    {"url": "https://example.com/b", "id": "21"},
                ]
            },
        )
        self.service.attach(str(self.repo))
        self.assertEqual(set(self.rows_by_id()), {"000020", "000021"})

    def test_records_an_import_for_provenance(self) -> None:
        self.apply(
            "create_sources", {"sources": [{"url": "https://example.com/a", "id": "20"}]}
        )
        imports = self.state()["imports"]
        self.assertEqual(imports[0]["import_type"], "agent_source_list")
        self.assertEqual(imports[0]["source_ids"], ["000020"])


if __name__ == "__main__":
    unittest.main()
