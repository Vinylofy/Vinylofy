import unittest
from unittest.mock import patch

import requests

from scripts.scrapers.usf.jobs import (
    refresh_shop3345_listing_prices as shop3345,
)


class FakeResponse:
    def __init__(
        self,
        *,
        payload=None,
        status_code=200,
        headers=None,
    ):
        self._payload = payload
        self.status_code = status_code
        self.headers = dict(headers or {})
        self.url = (
            "https://3345.nl/products/"
            "rate-limited-product.js"
        )
        self.text = ""

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(
                f"HTTP {self.status_code}",
                response=self,
            )

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, get_responses):
        self.get_responses = list(get_responses)
        self.get_calls = []
        self.post_calls = []

    def post(self, url, **kwargs):
        self.post_calls.append(
            {
                "url": url,
                "kwargs": kwargs,
            }
        )

        return FakeResponse(
            payload={
                "data": {
                    "p0": None,
                },
            }
        )

    def get(self, url, **kwargs):
        self.get_calls.append(
            {
                "url": url,
                "kwargs": kwargs,
            }
        )

        if not self.get_responses:
            raise AssertionError(
                f"Onverwachte GET-request: {url}"
            )

        return self.get_responses.pop(0)


class Shop3345AjaxRateLimitTests(unittest.TestCase):
    def fetch(self, session):
        return (
            shop3345.fetch_variant_availability_for_handles(
                session,
                handles=["rate-limited-product"],
                api_version="2024-04",
                public_access_token="public-token",
            )
        )

    def test_429_retries_and_then_succeeds(self):
        session = FakeSession(
            [
                FakeResponse(
                    status_code=429,
                    headers={
                        "Retry-After": "2",
                    },
                ),
                FakeResponse(
                    payload={
                        "handle": "rate-limited-product",
                        "available": True,
                        "tags": [],
                        "variants": [
                            {
                                "available": True,
                            },
                        ],
                    },
                ),
            ]
        )

        with patch.object(
            shop3345.time,
            "sleep",
        ) as mocked_sleep:
            result = self.fetch(session)

        self.assertEqual(
            result,
            {
                "rate-limited-product": "in_stock",
            },
        )
        self.assertEqual(len(session.get_calls), 2)
        mocked_sleep.assert_called_once_with(2.0)

    def test_repeated_429_stops_after_three_attempts(self):
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

    def test_non_429_http_error_is_not_retried(self):
        session = FakeSession(
            [
                FakeResponse(
                    status_code=500,
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

        self.assertEqual(len(session.get_calls), 1)
        mocked_sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
