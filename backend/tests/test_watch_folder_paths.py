"""The watch-folder path guard.

The attach-by-path endpoint takes a server-side filesystem path from the client,
so this guard is a security boundary rather than a convenience check. These
tests are deliberately about escapes, not happy paths.
"""

from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path

from backend.pipeline.source_capture import MAX_UPLOAD_BYTES
from backend.routers.capture import _scan_watch_folder, _watch_path_refusal


def _code(refusal: tuple[str, str] | None) -> str:
    return "" if refusal is None else refusal[0]


class WatchPathRefusalTest(unittest.TestCase):
    def setUp(self) -> None:
        # Under macOS `tempfile` lands in /var, which is a symlink to
        # /private/var. That makes this a live test of resolving *both* sides.
        self.temp_dir = tempfile.TemporaryDirectory(prefix="watch-guard-")
        self.root = Path(self.temp_dir.name)
        self.outside_dir = tempfile.TemporaryDirectory(prefix="watch-outside-")
        self.outside = Path(self.outside_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()
        self.outside_dir.cleanup()

    def _write(self, name: str, text: str = "hello") -> Path:
        target = self.root / name
        target.write_text(text, encoding="utf-8")
        return target

    def test_accepts_a_real_file_in_the_watch_folder(self) -> None:
        self.assertIsNone(_watch_path_refusal(self._write("page.html"), self.root))

    def test_accepts_a_watch_root_that_itself_needs_resolving(self) -> None:
        # An implementation that compares unresolved strings fails here on
        # macOS, where the temp root is reached through a symlink.
        path = self._write("paper.pdf", "%PDF-1.4")
        self.assertIsNone(_watch_path_refusal(path, Path(str(self.root))))

    def test_rejects_a_symlinked_leaf(self) -> None:
        link = self.root / "sneaky.html"
        os.symlink("/etc/passwd", link)
        self.assertEqual(_code(_watch_path_refusal(link, self.root)), "symlink_not_allowed")

    def test_rejects_escape_through_a_symlinked_parent(self) -> None:
        # The leaf is an ordinary file; only a directory partway up is a link.
        # A `startswith` check on the unresolved path misses this entirely.
        (self.outside / "evil.html").write_text("x", encoding="utf-8")
        os.symlink(self.outside, self.root / "viadir")
        refusal = _watch_path_refusal(self.root / "viadir" / "evil.html", self.root)
        self.assertEqual(_code(refusal), "path_outside_watch_folder")

    def test_traversal_reports_as_traversal_not_as_missing(self) -> None:
        # Ordering matters: containment is checked before existence, so a probe
        # for a real file outside the folder cannot be used to test for its
        # presence.
        refusal = _watch_path_refusal(self.root / ".." / ".." / "etc" / "passwd", self.root)
        self.assertEqual(_code(refusal), "path_outside_watch_folder")

    def test_rejects_a_missing_file_inside_the_folder(self) -> None:
        refusal = _watch_path_refusal(self.root / "nope.html", self.root)
        self.assertEqual(_code(refusal), "file_not_found")

    def test_rejects_a_disallowed_extension(self) -> None:
        refusal = _watch_path_refusal(self._write("payload.exe"), self.root)
        self.assertEqual(_code(refusal), "unsupported_file_type")

    def test_rejects_mhtml_with_its_own_guidance(self) -> None:
        refusal = _watch_path_refusal(self._write("saved.mhtml"), self.root)
        assert refusal is not None
        self.assertEqual(refusal[0], "unsupported_file_type")
        self.assertIn("MHTML", refusal[1])

    def test_extension_is_judged_on_the_resolved_name(self) -> None:
        # A caller cannot smuggle a disallowed file past the allowlist by
        # asking for it under an allowed-looking name.
        real = self._write("payload.exe")
        link = self.root / "innocent.html"
        os.symlink(real, link)
        self.assertEqual(_code(_watch_path_refusal(link, self.root)), "symlink_not_allowed")

    def test_rejects_an_empty_file(self) -> None:
        refusal = _watch_path_refusal(self._write("partial.html", ""), self.root)
        self.assertEqual(_code(refusal), "empty_file")

    def test_rejects_an_oversized_file(self) -> None:
        big = self.root / "huge.pdf"
        with big.open("wb") as handle:
            handle.truncate(MAX_UPLOAD_BYTES + 1)
        self.assertEqual(_code(_watch_path_refusal(big, self.root)), "file_too_large")


class ScanWatchFolderTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="watch-scan-")
        self.root = Path(self.temp_dir.name)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write(self, name: str, text: str = "hello") -> Path:
        target = self.root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        return target

    def _scan(self, **kwargs):
        options = {"since_ms": 0, "max_age_minutes": 1440, "limit": 40}
        options.update(kwargs)
        return _scan_watch_folder(self.root, **options)

    def test_lists_only_attachable_files(self) -> None:
        self._write("page.html")
        self._write("paper.pdf", "%PDF-1.4")
        self._write("notes.md")
        self._write("installer.exe")
        self._write("partial.html", "")
        names = {file.name for file in self._scan().files}
        self.assertEqual(names, {"page.html", "paper.pdf", "notes.md"})

    def test_does_not_recurse_into_a_complete_page_save(self) -> None:
        # "Save page as > Complete" writes a sibling folder of assets; the page
        # must not be buried under its own stylesheets.
        self._write("page.html")
        self._write("page_files/style.html")
        self.assertEqual([file.name for file in self._scan().files], ["page.html"])

    def test_skips_symlinks_so_the_list_matches_what_can_be_attached(self) -> None:
        self._write("page.html")
        os.symlink("/etc/hosts", self.root / "linked.html")
        self.assertEqual([file.name for file in self._scan().files], ["page.html"])

    def _age(self, path: Path, seconds_ago: float) -> None:
        stamp = time.time() - seconds_ago
        os.utime(path, (stamp, stamp))

    def test_orders_newest_first_and_honours_the_limit(self) -> None:
        for index in range(5):
            self._age(self._write(f"page{index}.html"), (5 - index) * 60)
        files = self._scan(limit=3).files
        self.assertEqual([file.name for file in files], ["page4.html", "page3.html", "page2.html"])

    def test_max_age_excludes_older_downloads(self) -> None:
        self._write("fresh.html")
        self._age(self._write("stale.html"), 7200)
        names = [file.name for file in self._scan(max_age_minutes=60).files]
        self.assertEqual(names, ["fresh.html"])

    def test_is_new_marks_only_files_since_the_given_moment(self) -> None:
        self._age(self._write("old.html"), 3600)
        self._write("just_saved.html")
        # The user opened the source in their browser ten minutes ago.
        since_ms = int((time.time() - 600) * 1000)
        files = {file.name: file.is_new for file in self._scan(since_ms=since_ms).files}
        self.assertTrue(files["just_saved.html"])
        self.assertFalse(files["old.html"])

    def test_missing_folder_is_reported_not_raised(self) -> None:
        response = _scan_watch_folder(
            self.root / "nonexistent", since_ms=0, max_age_minutes=1440, limit=40
        )
        self.assertFalse(response.configured)
        self.assertEqual(response.files, [])
        self.assertIn("Settings", response.error)


if __name__ == "__main__":
    unittest.main()
