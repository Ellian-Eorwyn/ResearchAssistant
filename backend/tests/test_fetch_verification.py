from __future__ import annotations

import unittest

from backend.pipeline.fetch_verification import (
    FetchVerdict,
    looks_blocked,
    verify_fetch,
)

# Verbatim content captured from real fetches. These are the pages the previous
# heuristic let through as `success`, so they are the regression corpus.

REDDIT_BLOCK_MD = (
    "You've been blocked by network security.\n"
    "If you think you've been blocked by mistake, file a ticket below and we'll "
    "look into it.\n"
    "File a ticket"
)

IEEE_BLOCK_MD = (
    "# Unusual Traffic Detected (Error 418)\n\n"
    "IEEE Xplore has detected an unusual request pattern. To protect site "
    "performance, this action has been restricted.\n\n"
    "If you require high-volume access to IEEE content, please request an "
    "[IEEE API key](https://developer.ieee.org/).\n\n"
    "If you have questions or believe you received this in error, please "
    "[contact us](https://xploreqa.ieee.org/xpl/contact?reason=UAA%202306051630986019219).\n\n"
    "Your support ID is: 2306051630986019219"
)

AKAMAI_BLOCK_MD = (
    "Reference #18.912d3e17.1785942796.78399597\n\n"
    "https://errors.edgesuite.net/18.912d3e17.1785942796.78399597"
)

CLOUDFLARE_BLOCK_MD = (
    "This website is using a security service to protect itself from online "
    "attacks. The action you just performed triggered the security solution. "
    "There are several actions that could trigger this block including "
    "submitting a certain word or phrase, a SQL command or malformed data.\n\n"
    "You can email the site owner to let them know you were blocked. Please "
    "include what you were doing when this page came up and the Cloudflare Ray "
    "ID found at the bottom of this page."
)

SUBSTACK_BLOCK_MD = "Enable JavaScript and cookies to continue"

# A legitimate but short marketing homepage. 66 words — *fewer* than the IEEE
# block page above — so only its structure distinguishes it. This is the
# false-positive guard.
SONNEN_OK_MD = """Energy storage system

# The future of energy begins here

**flexible, Weather-resilient and VPP-ready outdoor storage**

## The new sonnenHome Battery 11

Virtual Power Plant

## Support the grid and earn financial rewards with the sonnenVPP

Commercial Solutions

## Optimize energy costs with a commercial storage

About sonnen

## Clean and affordable energy for everyone

Smart at home

Smarter on the grid

Stronger in the VPP
"""


def _long_article(topic_sentence: str, words: int = 1200) -> str:
    """A structurally real article of `words` words containing `topic_sentence`."""
    filler = " ".join(["grid"] * 40)
    paragraphs = [
        "# Bot Walls And The Open Web\n",
        f"{topic_sentence} {filler}\n",
    ]
    while sum(len(p.split()) for p in paragraphs) < words:
        paragraphs.append(f"## Section\n\nDistributed energy resources {filler}\n")
    return "\n\n".join(paragraphs)


class VerifyBlockedPagesTest(unittest.TestCase):
    def test_reddit_network_security_block(self):
        result = verify_fetch(
            http_status=200,
            final_url="https://www.reddit.com/r/explainlikeimfive/comments/1j3k95m/",
            title="Reddit",
            raw_html="<html><body>You've been blocked by network security.</body></html>",
            extracted_text=REDDIT_BLOCK_MD,
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.BLOCKED_CHALLENGE)
        self.assertEqual(result.suggested_status, "blocked")
        self.assertTrue(result.is_blocked)

    def test_ieee_soft_202_error_418(self):
        result = verify_fetch(
            http_status=202,
            final_url="https://ieeexplore.ieee.org/document/5749026",
            title="IEEE Xplore - Unable to Load Page",
            raw_html="",
            extracted_text=IEEE_BLOCK_MD,
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.BLOCKED_CHALLENGE)
        self.assertEqual(result.suggested_status, "blocked")

    def test_akamai_reference_block(self):
        result = verify_fetch(
            http_status=403,
            final_url="https://www.tesla.com/support/energy/powerwall/virtual-power-plant",
            title="Access Denied",
            raw_html="",
            extracted_text=AKAMAI_BLOCK_MD,
            detected_type="html",
        )
        self.assertTrue(result.is_blocked)
        self.assertEqual(result.suggested_status, "blocked")

    def test_cloudflare_security_service_block(self):
        result = verify_fetch(
            http_status=403,
            final_url="https://emp.lbl.gov/publications/virtual-power-plants-insights",
            title="Attention Required! | Cloudflare",
            raw_html="",
            extracted_text=CLOUDFLARE_BLOCK_MD,
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.BLOCKED_CHALLENGE)

    def test_cloudflare_just_a_moment(self):
        result = verify_fetch(
            http_status=403,
            final_url="https://www.energymining.sa.gov.au/consumers/solar-and-batteries",
            title="Just a moment...",
            raw_html="<html><title>Just a moment...</title></html>",
            extracted_text="Verifying you are human. This may take a few seconds.",
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.BLOCKED_CHALLENGE)

    def test_substack_javascript_cookie_wall(self):
        result = verify_fetch(
            http_status=403,
            final_url="https://stephenheins.substack.com/p/headline",
            title="",
            raw_html="",
            extracted_text=SUBSTACK_BLOCK_MD,
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.BLOCKED_CHALLENGE)

    def test_reuters_401_with_no_body(self):
        result = verify_fetch(
            http_status=401,
            final_url="https://www.reuters.com/business/sustainable-business/",
            title="reuters.com",
            raw_html="",
            extracted_text="Please enable JS and disable any ad blocker",
            detected_type="html",
        )
        self.assertTrue(result.is_blocked)

    def test_challenge_platform_redirect(self):
        result = verify_fetch(
            http_status=200,
            final_url="https://example.com/cdn-cgi/challenge-platform/h/b/orchestrate",
            title="",
            raw_html="",
            extracted_text="",
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.BLOCKED_CHALLENGE)


class VerifyGoodPagesTest(unittest.TestCase):
    def test_short_marketing_homepage_is_ok(self):
        """66 words, but five headings. Must survive."""
        result = verify_fetch(
            http_status=200,
            final_url="https://www.sonnenusa.com/",
            title="The Future of Energy | sonnen",
            raw_html="<html><body>...</body></html>",
            extracted_text=SONNEN_OK_MD,
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.OK)
        self.assertEqual(result.suggested_status, "")
        self.assertFalse(result.is_blocked)

    def test_long_article_mentioning_captcha_is_ok(self):
        result = verify_fetch(
            http_status=200,
            final_url="https://example.com/article",
            title="How CAPTCHAs Work",
            raw_html="",
            extracted_text=_long_article(
                "Sites deploy a captcha and return access denied to suspected bots."
            ),
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.OK)

    def test_long_article_quoting_a_block_page_is_ok(self):
        """A strong phrase deep in a real article must not condemn it."""
        text = _long_article("Distributed energy resources coordinate across homes.")
        text += "\n\nOne user reported seeing \"you've been blocked by network security\" instead.\n"
        result = verify_fetch(
            http_status=200,
            final_url="https://example.com/article",
            title="Bot Walls And Research",
            raw_html="",
            extracted_text=text,
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.OK)

    def test_page_with_hidden_login_form_is_ok(self):
        """A collapsed sign-in modal is present on countless normal pages."""
        result = verify_fetch(
            http_status=200,
            final_url="https://example.com/article",
            title="Virtual Power Plants Explained",
            raw_html='<html><form><input type="password" name="pw"></form></html>',
            extracted_text=_long_article("Virtual power plants aggregate distributed resources."),
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.OK)


class VerifyWallsTest(unittest.TestCase):
    def test_login_wall(self):
        result = verify_fetch(
            http_status=200,
            final_url="https://example.com/report",
            title="Members Area",
            raw_html='<html><form><input type="password"></form></html>',
            extracted_text="Please sign in to continue reading this report.",
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.LOGIN_REQUIRED)
        self.assertEqual(result.suggested_status, "blocked")

    def test_paywall_is_accessible_for_free_false(self):
        result = verify_fetch(
            http_status=200,
            final_url="https://example.com/story",
            title="A Story",
            raw_html='<script type="application/ld+json">{"isAccessibleForFree": false}</script>',
            extracted_text="Subscribe to continue reading. Subscribers only.",
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.PAYWALL)
        self.assertEqual(result.suggested_status, "blocked")

    def test_http_402_is_paywall(self):
        result = verify_fetch(
            http_status=402,
            final_url="https://example.com/story",
            title="A Story",
            raw_html="",
            extracted_text="Payment required.",
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.PAYWALL)


class VerifyThinAndEmptyTest(unittest.TestCase):
    def test_empty_body(self):
        result = verify_fetch(
            http_status=200,
            final_url="https://example.com/",
            title="Example",
            raw_html="<html><body></body></html>",
            extracted_text="",
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.EMPTY)
        self.assertEqual(result.suggested_status, "failed")

    def test_no_response_at_all_is_empty(self):
        """detected_type is blank when the fetch never got far enough."""
        result = verify_fetch(
            http_status=None,
            final_url="",
            title="",
            raw_html="",
            extracted_text="",
            detected_type="",
        )
        self.assertEqual(result.verdict, FetchVerdict.EMPTY)

    def test_thin_html_becomes_partial_not_blocked(self):
        result = verify_fetch(
            http_status=200,
            final_url="https://example.com/stub",
            title="Stub",
            raw_html="<html><body>Coming soon</body></html>",
            extracted_text="Coming soon",
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.THIN_CONTENT)
        self.assertEqual(result.suggested_status, "partial")
        self.assertFalse(result.is_blocked)

    def test_thin_video_transcript_is_not_blocked(self):
        """Videos legitimately yield almost no text; they must stay untouched."""
        result = verify_fetch(
            http_status=None,
            final_url="https://www.youtube.com/watch?v=7IbZF5ZOpZ4",
            title="A Short Clip",
            raw_html="",
            extracted_text="- **Channel:** Example",
            detected_type="video",
        )
        self.assertEqual(result.verdict, FetchVerdict.OK)
        self.assertEqual(result.suggested_status, "")


class VerifyNonHtmlTest(unittest.TestCase):
    def test_pdf_content_phrases_are_ignored(self):
        """A paper *about* bot walls is not itself a bot wall."""
        result = verify_fetch(
            http_status=200,
            final_url="https://example.com/paper.pdf",
            title="Just a moment: a study of CAPTCHA interstitials",
            raw_html="",
            extracted_text="You've been blocked by network security is a common message.",
            detected_type="pdf",
        )
        self.assertEqual(result.verdict, FetchVerdict.OK)

    def test_pdf_http_block_still_counts(self):
        result = verify_fetch(
            http_status=403,
            final_url="https://example.com/paper.pdf",
            title="",
            raw_html="",
            extracted_text="",
            detected_type="pdf",
        )
        self.assertEqual(result.verdict, FetchVerdict.BLOCKED_HTTP)

    def test_uploaded_document_is_never_challenge_blocked(self):
        result = verify_fetch(
            http_status=None,
            final_url="",
            title="Access Denied",
            raw_html="",
            extracted_text="Just a moment...",
            detected_type="html",
            source_kind="uploaded_document",
        )
        self.assertEqual(result.verdict, FetchVerdict.OK)

    def test_html_interstitial_served_at_a_pdf_url(self):
        """Callers re-route these through the html path; verify it lands right."""
        result = verify_fetch(
            http_status=200,
            final_url="https://example.com/download/paper.pdf",
            title="Just a moment...",
            raw_html="<html><title>Just a moment...</title></html>",
            extracted_text="Enable JavaScript and cookies to continue",
            detected_type="html",
        )
        self.assertEqual(result.verdict, FetchVerdict.BLOCKED_CHALLENGE)


class LooksBlockedTest(unittest.TestCase):
    """The pre-render check runs on raw HTML alone, before any extraction."""

    def test_detects_challenge_before_rendering(self):
        self.assertTrue(
            looks_blocked(
                raw_html="<html><body>Just a moment... checking your browser</body></html>",
                title="Just a moment...",
                final_url="https://example.com/",
            )
        )

    def test_normal_page_with_login_form_is_not_blocked(self):
        self.assertFalse(
            looks_blocked(
                raw_html='<html><form><input type="password"></form>'
                '<p>Virtual power plants aggregate resources.</p></html>',
                title="Virtual Power Plants",
                final_url="https://example.com/",
            )
        )

    def test_title_alone_does_not_block(self):
        """'blocked' in a headline must not condemn a page with no body evidence."""
        self.assertFalse(
            looks_blocked(
                raw_html="<html><body><p>An account was suspended last week.</p></body></html>",
                title="Why my account was blocked",
                final_url="https://example.com/",
            )
        )


if __name__ == "__main__":
    unittest.main()
