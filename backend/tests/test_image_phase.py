"""End-to-end tests for the image phase.

Hermetic: image downloads and the vision model are both mocked, so the test
exercises extraction, classification, description, idempotency and the readiness
gates without a network. Modeled on ``test_media_recursion_guard.py``.
"""

from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path

from PIL import Image

from backend.models.settings import LLMBackendConfig
from backend.models.sources import SourceManifestRow, SourceOutputOptions
from backend.pipeline import source_downloader as sd
from backend.pipeline.source_downloader import (
    RuntimeCapabilities,
    SourceDownloadOrchestrator,
    _parse_image_analysis,
)
from backend.storage.file_store import FileStore

SOURCE_ID = "000042"
PAGE_URL = "https://example.com/vpp"

# Two images with no declared dimensions (so the attribute pre-filter passes and
# they are downloaded), plus one tiny icon and one that declares a small size.
RENDERED_HTML = """<html><head><title>VPP explainer</title>
<meta property="og:image" content="https://example.com/hero.png"></head><body>
<h1>Virtual Power Plants</h1>
<img src="/hero.png" alt="A diagram of a virtual power plant">
<img src="/chart.png" alt="A small chart">
<img src="/diagram.svg" alt="vector diagram">
<img src="/logo.png" width="24" height="24" alt="site logo">
<img src="/tiny.png" alt="spacer">
</body></html>"""

DUMMY_BACKEND = LLMBackendConfig(kind="openai", base_url="http://vision.local", model="chat")


def _png(width: int, height: int) -> bytes:
    buf = io.BytesIO()
    Image.new("RGB", (width, height), (30, 120, 200)).save(buf, format="PNG")
    return buf.getvalue()


# Bytes each mocked URL returns. logo/tiny are only reached if the pre-filter lets
# them through; hero/chart are the real content candidates.
_DOWNLOADS = {
    "https://example.com/hero.png": _png(900, 640),
    "https://example.com/chart.png": _png(300, 220),
    "https://example.com/diagram.svg": (
        b"<svg xmlns='http://www.w3.org/2000/svg' width='500' height='400'>"
        b"<rect width='500' height='400' fill='blue'/></svg>"
    ),
    "https://example.com/logo.png": _png(24, 24),
    "https://example.com/tiny.png": _png(40, 40),
}


class ImagePhaseTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.out = Path(self._tmp.name)
        (self.out / "sources" / SOURCE_ID).mkdir(parents=True)
        (self.out / "sources" / SOURCE_ID / f"{SOURCE_ID}_rendered.html").write_text(
            RENDERED_HTML, encoding="utf-8"
        )
        self.vision_calls = 0

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _row(self) -> SourceManifestRow:
        return SourceManifestRow(
            id=SOURCE_ID,
            detected_type="html",
            fetch_status="success",
            rendered_file=f"sources/{SOURCE_ID}/{SOURCE_ID}_rendered.html",
            final_url=PAGE_URL,
            title="VPP explainer",
        )

    def _orchestrator(self, *, vision_enabled=True, **flags) -> SourceDownloadOrchestrator:
        test = self

        def fake_download(self, http, url, max_bytes):  # noqa: ANN001
            data = _DOWNLOADS.get(url)
            return (data, "image/png") if data else None

        async def fake_vision(self, system, user, image_bytes, mime):  # noqa: ANN001
            test.vision_calls += 1
            with Image.open(io.BytesIO(image_bytes)) as im:
                edge = min(im.size)
            if edge >= 400:
                return json.dumps({
                    "classification": "relevant", "category": "diagram", "confidence": 0.92,
                    "reason": "Explains a VPP.", "description": "A VPP schematic.",
                    "relevance": "Central to the study.",
                })
            return json.dumps({
                "classification": "incidental", "category": "photo",
                "confidence": 0.8, "reason": "Not subject matter.",
            })

        orch = SourceDownloadOrchestrator(
            job_id="imgtest",
            store=FileStore(self.out / ".jobs"),
            use_llm=True,
            llm_backend=DUMMY_BACKEND,
            vision_backend=DUMMY_BACKEND,
            project_profile_name="test_profile",
            project_profile_yaml="purpose: study VPP visuals",
            research_purpose="How VPPs are explained to the public.",
            output_options=SourceOutputOptions(image_max_count=20),
            output_dir=self.out,
            writes_to_repository=True,
            **flags,
        )
        orch._download_image_bytes = fake_download.__get__(orch, SourceDownloadOrchestrator)
        orch._run_image_vision = fake_vision.__get__(orch, SourceDownloadOrchestrator)
        orch.runtime_capabilities = RuntimeCapabilities(
            trafilatura_available=False, playwright_python_available=False,
            playwright_browser_available=False, textutil_available=False,
            tesseract_available=False, yt_dlp_available=False, ffmpeg_available=False,
            llm_vision_enabled=vision_enabled, runtime_notes=[], runtime_guidance=[],
        )
        return orch

    def _index(self) -> dict:
        path = self.out / "sources" / SOURCE_ID / "images" / f"{SOURCE_ID}_images.json"
        return json.loads(path.read_text(encoding="utf-8"))

    # -- tests ------------------------------------------------------------

    def test_extracts_classifies_and_describes(self):
        row = self._row()
        self._orchestrator(run_images=True)._generate_source_images(row, [])

        self.assertEqual(row.image_status, "analyzed")
        # hero + chart + diagram.svg reach storage. The 24x24 logo is dropped by
        # the declared-dimension pre-filter; the 40x40 spacer is dropped after
        # decode (below the content size floor).
        self.assertEqual(row.image_count, 3)
        self.assertEqual(row.relevant_image_count, 1)

        index = self._index()
        classifications = sorted(im["classification"] for im in index["images"])
        self.assertEqual(classifications, ["incidental", "incidental", "relevant"])
        # The SVG is classified deterministically (no vision call for vectors).
        svg = next(im for im in index["images"] if im["mime"] == "image/svg+xml")
        self.assertEqual(svg["analysis"], "deterministic")
        self.assertEqual(svg["category"], "vector_graphic")
        self.assertEqual(svg["classification"], "incidental")
        # Only the two raster images above the size floor went to the model.
        self.assertEqual(self.vision_calls, 2)

        # Descriptions: per-source canonical + top-level mirror + index.
        per_source = self.out / "sources" / SOURCE_ID / f"{SOURCE_ID}_image_descriptions.md"
        mirror = self.out / "image_descriptions" / f"{SOURCE_ID}.md"
        self.assertTrue(per_source.is_file())
        self.assertTrue(mirror.is_file())
        self.assertIn("A VPP schematic.", per_source.read_text(encoding="utf-8"))
        self.assertIn("images/", per_source.read_text(encoding="utf-8"))
        self.assertIn(f"../sources/{SOURCE_ID}/images/", mirror.read_text(encoding="utf-8"))
        self.assertTrue((self.out / "image_descriptions" / "index.md").is_file())
        self.assertEqual(row.image_descriptions_status, "generated")
        self.assertEqual(row.phase_metadata["images"].status, "completed")

    def test_reanalysis_is_idempotent_until_forced(self):
        self._orchestrator(run_images=True)._generate_source_images(self._row(), [])
        first = self.vision_calls
        self.assertEqual(first, 2)

        self.vision_calls = 0
        self._orchestrator(run_images=True)._generate_source_images(self._row(), [])
        self.assertEqual(self.vision_calls, 0)  # nothing changed -> no re-spend

        self.vision_calls = 0
        row = self._row()
        self._orchestrator(run_images=True, force_images=True)._generate_source_images(row, [])
        self.assertEqual(self.vision_calls, 2)  # force re-analyzes
        self.assertEqual(row.relevant_image_count, 1)

    def test_skips_cleanly_when_vision_unavailable(self):
        row = self._row()
        notes: list[str] = []
        self._orchestrator(run_images=True, vision_enabled=False)._generate_source_images(row, notes)
        # Extraction still happened; analysis was skipped, not failed.
        self.assertEqual(row.image_status, "skipped_vision_unavailable")
        self.assertEqual(row.phase_metadata["images"].status, "skipped")
        self.assertEqual(self.vision_calls, 0)
        self.assertTrue((self.out / "sources" / SOURCE_ID / "images").is_dir())
        self.assertTrue(row.image_count > 0)

    def test_convert_only_run_does_not_touch_images(self):
        # No fetch, no images phase: extraction must not run (convert stays
        # network-free) and no images directory is created.
        row = self._row()
        orch = self._orchestrator(run_download=False, run_convert=True, run_images=False)
        orch._generate_source_images(row, [])
        self.assertEqual(row.image_status, "not_requested")
        self.assertEqual(self.vision_calls, 0)
        self.assertFalse((self.out / "sources" / SOURCE_ID / "images").exists())

    def test_auto_extract_during_fetch_without_vision(self):
        # A fetch run (run_download) auto-extracts but does not classify.
        row = self._row()
        orch = self._orchestrator(run_download=True, run_images=False)
        orch._generate_source_images(row, [])
        self.assertEqual(row.image_status, "extracted")
        self.assertEqual(self.vision_calls, 0)
        self.assertTrue(row.image_count > 0)
        # No classifications yet.
        self.assertTrue(all(im["classification"] == "" for im in self._index()["images"]))


class ParseImageAnalysisTests(unittest.TestCase):
    def test_parses_relevant_with_description(self):
        out = _parse_image_analysis(
            '{"classification":"relevant","category":"chart","confidence":0.9,'
            '"reason":"r","description":"d","relevance":"why"}',
            describe=True,
        )
        self.assertEqual(out["classification"], "relevant")
        self.assertEqual(out["description"], "d")
        self.assertEqual(out["relevance"], "why")

    def test_incidental_clears_description_fields(self):
        out = _parse_image_analysis(
            '{"classification":"incidental","description":"x","relevance":"y"}', describe=True
        )
        self.assertEqual(out["description"], "")
        self.assertEqual(out["relevance"], "")

    def test_classify_only_omits_description(self):
        out = _parse_image_analysis(
            '{"classification":"relevant","description":"d"}', describe=False
        )
        self.assertEqual(out["description"], "")

    def test_recovers_json_wrapped_in_prose(self):
        out = _parse_image_analysis(
            'Here you go:\n{"classification":"relevant","confidence":1.5}\nThanks!', describe=True
        )
        self.assertEqual(out["classification"], "relevant")
        self.assertEqual(out["confidence"], 1.0)  # clamped

    def test_coerces_content_synonym_and_bad_confidence(self):
        out = _parse_image_analysis(
            '{"classification":"content","confidence":"high"}', describe=True
        )
        self.assertEqual(out["classification"], "relevant")
        self.assertEqual(out["confidence"], 0.0)

    def test_returns_none_on_unparseable(self):
        self.assertIsNone(_parse_image_analysis("not json at all", describe=True))
        self.assertIsNone(_parse_image_analysis("", describe=True))


if __name__ == "__main__":
    unittest.main()
