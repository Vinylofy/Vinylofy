import unittest
from unittest.mock import patch

import requests

from scripts.scrapers.usf.jobs import (
    refresh_shop3345_listing_prices as shop3345,
)


VALID_METRICS = {
    "product_cards": 98,
    "price_blocks": 96,
    "contains_euro": True,
}


class FakeResponse:
    def __init__(
        self,
        *,
        status_code=200,
        headers=None,
        url=(
            "https://3345.nl/nl/"
            "collections/all?page=32"
        ),
    ):
        self.status_code = status_code
        self.headers = dict(headers or {})
        self.url = url
        self.text = "<html>listing</html>"
        self.history = []
        self.cookies = (
            requests.cookies.RequestsCookieJar()
        )

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(
                f"HTTP {self.status_code}",
                response=self,
            )


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.get_calls = []
        self.cookies = (
            requests.cookies.RequestsCookieJar()
        )

    def get(self, url, **kwargs):
        self.get_calls.append(
            {
                "url": url,
                "kwargs": kwargs,
            }
        )

        if not self.responses:
            raise AssertionError(
                f"Onverwachte GET-request: {url}"
            )

        return self.responses.pop(0)


class Shop3345ListingRateLimitTests(
    unittest.TestCase
):
    def fetch(self, session):
        with (
            patch.object(
                shop3345,
                "_3345_log_response",
                return_value=VALID_METRICS,
            ),
            patch.object(
                shop3345,
                "_3345_save_html",
            ),
        ):
            return shop3345.fetch_3345_listing(
                session,
                (
                    "https://3345.nl/nl/"
                    "collections/all?page=32"
                ),
                page=32,
                debug=False,
            )

    def test_429_retries_and_then_succeeds(self):
        session = FakeSession(
            [
                FakeResponse(
                    status_code=429,
                    headers={
                        "Retry-After": "3",
                    },
                ),
                FakeResponse(status_code=200),
            ]
        )

        with patch.object(
            shop3345.time,
            "sleep",
        ) as mocked_sleep:
            response = self.fetch(session)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(session.get_calls), 2)
        mocked_sleep.assert_called_once_with(3.0)

    def test_repeated_429_stops_after_three_attempts(
        self,
    ):
        session = FakeSession(
            [
                FakeResponse(
                    status_code=429,
                    headers={"Retry-After": "1"},
                ),
                FakeResponse(
                    status_code=429,
                    headers={"Retry-After": "1"},
                ),
                FakeResponse(
                    status_code=429,
                    headers={"Retry-After": "1"},
                ),
            ]
        )

        with (
            patch.object(
                shop3345.time,
                "sleep",
            ) as mocked_sleep,
            self.assertRaises(requests.HTTPError),
        ):
            self.fetch(session)

        self.assertEqual(len(session.get_calls), 3)
        self.assertEqual(
            mocked_sleep.call_count,
            2,
        )

    def test_non_429_http_error_is_not_retried(
        self,
    ):
        session = FakeSession(
            [
                FakeResponse(status_code=500),
            ]
        )

        with (
            patch.object(
                shop3345.time,
                "sleep",
            ) as mocked_sleep,
            self.assertRaises(requests.HTTPError),
        ):
            self.fetch(session)

        self.assertEqual(len(session.get_calls), 1)
        mocked_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
