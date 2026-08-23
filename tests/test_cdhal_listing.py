from __future__ import annotations

import unittest
from unittest.mock import patch

import requests

from scripts.scrapers.usf.jobs.refresh_cdhal_listing_prices import fetch_listing_html


class Response:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class Session:
    def __init__(self, response: Response) -> None:
        self.response = response
        self.calls = 0

    def get(self, *_args, **_kwargs) -> Response:
        self.calls += 1
        return self.response


class CdHalListingTransportTests(unittest.TestCase):
    def test_403_uses_browser_fallback(self):
        session = Session(Response(403))
        with patch(
            "scripts.scrapers.usf.jobs.refresh_cdhal_listing_prices.fetch_html_with_playwright",
            return_value="<html>browser</html>",
        ) as fallback:
            html, status, transport = fetch_listing_html(
                session,
                "https://www.cdhal.nl/vinyl",
            )

        self.assertEqual(session.calls, 1)
        self.assertEqual(html, "<html>browser</html>")
        self.assertEqual(status, 200)
        self.assertEqual(transport, "playwright")
        fallback.assert_called_once_with(
            "https://www.cdhal.nl/vinyl",
            referer=None,
        )

    def test_successful_requests_response_does_not_start_browser(self):
        session = Session(Response(200, "<html>requests</html>"))
        with patch(
            "scripts.scrapers.usf.jobs.refresh_cdhal_listing_prices.fetch_html_with_playwright"
        ) as fallback:
            html, status, transport = fetch_listing_html(
                session,
                "https://www.cdhal.nl/vinyl",
            )

        self.assertEqual(html, "<html>requests</html>")
        self.assertEqual(status, 200)
        self.assertEqual(transport, "requests")
        fallback.assert_not_called()


if __name__ == "__main__":
    unittest.main()
