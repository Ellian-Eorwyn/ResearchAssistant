"""Deterministic date-signal extraction from raw HTML."""
import unittest

from backend.pipeline.date_signals import extract_date_signals


class DateSignalTests(unittest.TestCase):
    def test_empty_html_returns_empty(self) -> None:
        self.assertEqual(extract_date_signals("", current_year=2026), "")
        self.assertEqual(extract_date_signals("<html><body>no dates</body></html>", current_year=2026), "")

    def test_meta_and_jsonld_published_win(self) -> None:
        html = (
            '<html><head>'
            '<meta property="article:published_time" content="2023-04-13T10:00:00Z">'
            '<script type="application/ld+json">{"datePublished":"2023-04-13"}</script>'
            '</head><body>x</body></html>'
        )
        sig = extract_date_signals(html, current_year=2026)
        self.assertIn("today=2026", sig)
        self.assertIn("meta_published=2023-04-13", sig)
        self.assertIn("jsonld_published=2023-04-13", sig)
        # authoritative labels lead the crawl-year htmldate/copyright
        self.assertLess(sig.index("meta_published"), sig.index("today") + len(sig))

    def test_visible_byline(self) -> None:
        html = "<html><body><p>Published Apr 13, 2023 by staff</p>" + "z" * 100 + "</body></html>"
        sig = extract_date_signals(html, current_year=2026)
        self.assertIn("byline=Apr 13, 2023", sig)

    def test_future_years_are_rejected(self) -> None:
        html = '<meta property="article:published_time" content="2099-01-01">'
        self.assertEqual(extract_date_signals(html, current_year=2026), "")

    def test_copyright_is_last_resort_and_labelled(self) -> None:
        html = "<html><body>" + "x" * 200 + "<footer>&copy; 2000 Example</footer></body></html>"
        sig = extract_date_signals(html, current_year=2026)
        self.assertIn("copyright=2000", sig)

    def test_only_today_never_emitted_alone(self) -> None:
        # No real signal -> no "today=" noise
        self.assertNotIn("today=", extract_date_signals("<html>nothing</html>", current_year=2026))


if __name__ == "__main__":
    unittest.main()
