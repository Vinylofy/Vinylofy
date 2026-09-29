from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts.scrapers.usf.jobs.detail_cdhal import request_detail_page


class Response:
    def __init__(self, status_code: int, text: str = "") -> None:
        self.status_code = status_code
        self.text = text
        self.headers: dict[str, str] = {}


class Session:
    def __init__(self, response: Response) -> None:
        self.response = response

    def get(self, *_args, **_kwargs) -> Response:
        return self.response


class CdHalDetailTransportTests(unittest.TestCase):
    def test_403_uses_browser_for_detail_html(self) -> None:
        url = "https://www.cdhal.nl/example-lp"
        with patch(
            "scripts.scrapers.usf.jobs.detail_cdhal.fetch_html_with_playwright",
            return_value="<html>product</html>",
        ) as browser:
            page = request_detail_page(Session(Response(403)), url)

        self.assertEqual(page.status_code, 200)
        self.assertEqual(page.text, "<html>product</html>")
        self.assertEqual(page.transport, "playwright")
        browser.assert_called_once_with(url)

    def test_successful_request_keeps_requests_transport(self) -> None:
        with patch(
            "scripts.scrapers.usf.jobs.detail_cdhal.fetch_html_with_playwright"
        ) as browser:
            page = request_detail_page(
                Session(Response(200, "<html>requests</html>")),
                "https://www.cdhal.nl/example-lp",
            )

        self.assertEqual(page.status_code, 200)
        self.assertEqual(page.text, "<html>requests</html>")
        self.assertEqual(page.transport, "requests")
        browser.assert_not_called()


if __name__ == "__main__":
    unittest.main()
