"""Tests for the deterministic image-reference extractor.

Pure and network-free: it parses HTML and normalizes URLs, mirroring the shape
of ``test_media_link_extraction.py``. Byte download and the vision calls live in
the orchestrator and are covered by ``test_image_phase.py``.
"""

from __future__ import annotations

import base64
import unittest

from backend.pipeline import image_extraction as ie

BASE = "https://example.com/articles/energy"


class ExtractImageRefsTests(unittest.TestCase):
    def _urls(self, refs):
        return [r.resolved_url for r in refs]

    def test_resolves_absolute_relative_and_protocol_relative(self):
        html = """
        <img src="https://cdn.example.com/a.jpg">
        <img src="/static/b.png">
        <img src="../c.gif">
        <img src="//img.example.net/d.webp">
        """
        refs = ie.extract_image_refs(html, base_url=BASE)
        urls = self._urls(refs)
        self.assertIn("https://cdn.example.com/a.jpg", urls)
        self.assertIn("https://example.com/static/b.png", urls)
        # `energy` is a resource, not a dir, so ../ resolves against /articles/.
        self.assertIn("https://example.com/c.gif", urls)
        self.assertIn("https://img.example.net/d.webp", urls)

    def test_picks_largest_srcset_candidate(self):
        html = '<img srcset="a.jpg 200w, b.jpg 800w, c.jpg 400w">'
        refs = ie.extract_image_refs(html, base_url=BASE)
        self.assertEqual(len(refs), 1)
        self.assertTrue(refs[0].resolved_url.endswith("/b.jpg"))

    def test_reads_picture_source_and_lazy_attrs(self):
        html = """
        <picture><source type="image/webp" srcset="hero.webp 1x, hero@2x.webp 2x"></picture>
        <img data-src="/lazy/late.jpg">
        """
        refs = ie.extract_image_refs(html, base_url=BASE)
        urls = self._urls(refs)
        self.assertTrue(any(u.endswith("hero@2x.webp") for u in urls))
        self.assertIn("https://example.com/lazy/late.jpg", urls)

    def test_reads_og_and_twitter_image_meta(self):
        html = """
        <meta property="og:image" content="https://example.com/og.png">
        <meta name="twitter:image" content="/tw.jpg">
        """
        refs = ie.extract_image_refs(html, base_url=BASE)
        origins = {r.origin for r in refs}
        self.assertIn("meta", origins)
        self.assertIn("https://example.com/og.png", self._urls(refs))
        self.assertIn("https://example.com/tw.jpg", self._urls(refs))

    def test_deduplicates_by_resolved_url_in_document_order(self):
        html = """
        <img src="/x.jpg" alt="first">
        <img src="/x.jpg" alt="dup">
        <img src="/y.jpg">
        """
        refs = ie.extract_image_refs(html, base_url=BASE)
        self.assertEqual(self._urls(refs), [
            "https://example.com/x.jpg",
            "https://example.com/y.jpg",
        ])
        self.assertEqual(refs[0].alt, "first")

    def test_captures_alt_and_dimension_attrs(self):
        html = '<img src="/p.jpg" alt="A chart" width="640" height="480">'
        ref = ie.extract_image_refs(html, base_url=BASE)[0]
        self.assertEqual(ref.alt, "A chart")
        self.assertEqual(ref.width_attr, 640)
        self.assertEqual(ref.height_attr, 480)

    def test_data_uri_is_flagged_and_not_resolved(self):
        html = '<img src="data:image/gif;base64,R0lGODlhAQABAAAAACw=">'
        ref = ie.extract_image_refs(html, base_url=BASE)[0]
        self.assertTrue(ref.is_data_uri)
        self.assertEqual(ref.resolved_url, "")

    def test_regex_fallback_when_tree_parse_finds_nothing(self):
        # Angle brackets are broken, so lxml yields no <img>; the regex sweep
        # still recovers the src that ends in an image extension.
        html = "junk <IMG SRC='/broken/pic.jpeg'> more junk without closing"
        refs = ie.extract_image_refs("<<" + html, base_url=BASE)
        self.assertTrue(any(u.endswith("/broken/pic.jpeg") for u in self._urls(refs)))

    def test_limit_caps_results(self):
        html = "".join(f'<img src="/{i}.jpg">' for i in range(10))
        refs = ie.extract_image_refs(html, base_url=BASE, limit=3)
        self.assertEqual(len(refs), 3)


class HelperTests(unittest.TestCase):
    def test_pick_srcset_handles_density_and_bare(self):
        self.assertEqual(ie.pick_srcset_url("only.jpg"), "only.jpg")
        self.assertEqual(ie.pick_srcset_url("a.jpg 1x, b.jpg 3x, c.jpg 2x"), "b.jpg")

    def test_parse_data_uri_base64_and_plain(self):
        raw, mime = ie.parse_data_uri("data:image/png;base64,iVBORw0KGgo=")
        self.assertEqual(mime, "image/png")
        self.assertEqual(raw, base64.b64decode("iVBORw0KGgo="))
        self.assertIsNone(ie.parse_data_uri("https://example.com/not-a-data-uri.png"))

    def test_dimension_prefilter_drops_small_keeps_unknown(self):
        self.assertFalse(ie.passes_dimension_prefilter(16, 16, 64))
        self.assertFalse(ie.passes_dimension_prefilter(300, 20, 64))
        self.assertTrue(ie.passes_dimension_prefilter(300, 300, 64))
        self.assertTrue(ie.passes_dimension_prefilter(None, None, 64))

    def test_sniff_mime_reads_magic_numbers(self):
        png = base64.b64decode(
            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
        )
        self.assertEqual(ie.sniff_mime(png), "image/png")
        self.assertEqual(ie.sniff_mime(b"\xff\xd8\xff\xe0blah"), "image/jpeg")
        self.assertEqual(ie.sniff_mime(b"GIF89a..."), "image/gif")
        self.assertEqual(ie.sniff_mime(b"<svg xmlns='...'>"), "image/svg+xml")

    def test_extension_for_mime_and_sha256_stable(self):
        self.assertEqual(ie.extension_for_mime("image/jpeg"), ".jpg")
        self.assertEqual(ie.extension_for_mime("image/svg+xml"), ".svg")
        self.assertEqual(ie.sha256_bytes(b"abc"), ie.sha256_bytes(b"abc"))
        self.assertNotEqual(ie.sha256_bytes(b"abc"), ie.sha256_bytes(b"abd"))

    def test_chrome_url_heuristic(self):
        self.assertTrue(ie.looks_like_chrome_url("https://x.com/assets/logo.png"))
        self.assertTrue(ie.looks_like_chrome_url("https://x.com/sprite-sheet.png"))
        self.assertFalse(ie.looks_like_chrome_url("https://x.com/figures/vpp-diagram.png"))


if __name__ == "__main__":
    unittest.main()
