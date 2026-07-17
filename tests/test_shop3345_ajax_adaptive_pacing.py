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
        url="https://3345.nl/products/test.js",
    ):
        self._payload = payload
        self.status_code = status_code
        self.headers = dict(headers or {})
        self.url = url
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
    def __init__(self, *, graph_data, get_responses):
        self.graph_data = graph_data
        self.get_responses = list(get_responses)
        self.get_calls = []

    def post(self, url, **kwargs):
        return FakeResponse(
            payload={
                "data": self.graph_data,
            },
            url=url,
        )

    def get(self, url, **kwargs):
        self.get_calls.append(url)

        if not self.get_responses:
            raise AssertionError(
                f"Onverwachte GET-request: {url}"
            )

        return self.get_responses.pop(0)


def available_product(handle):
    return {
        "handle": handle,
        "available": True,
        "tags": [],
        "variants": [
            {
                "available": True,
            },
        ],
    }


class Shop3345AjaxAdaptivePacingTests(
    unittest.TestCase
):
    def fetch(self, session, handles):
        return (
            shop3345.fetch_variant_availability_for_handles(
                session,
                handles=handles,
                api_version="2024-04",
                public_access_token="public-token",
            )
        )

    def test_missing_retry_after_uses_ten_seconds_first(
        self,
    ):
        session = FakeSession(
            graph_data={
                "p0": None,
            },
            get_responses=[
                FakeResponse(status_code=429),
                FakeResponse(
                    payload=available_product(
                        "product-one"
                    ),
                ),
            ],
        )

        with patch.object(
            shop3345.time,
            "sleep",
        ) as mocked_sleep:
            result = self.fetch(
                session,
                ["product-one"],
            )

        self.assertEqual(
            result["product-one"],
            "in_stock",
        )
        mocked_sleep.assert_called_once_with(10.0)

    def test_second_missing_retry_after_uses_thirty_seconds(
        self,
    ):
        session = FakeSession(
            graph_data={
                "p0": None,
            },
            get_responses=[
                FakeResponse(status_code=429),
                FakeResponse(status_code=429),
                FakeResponse(
                    payload=available_product(
                        "product-one"
                    ),
                ),
            ],
        )

        with patch.object(
            shop3345.time,
            "sleep",
        ) as mocked_sleep:
            result = self.fetch(
                session,
                ["product-one"],
            )

        self.assertEqual(
            result["product-one"],
            "in_stock",
        )
        self.assertEqual(
            mocked_sleep.call_args_list,
            [
                unittest.mock.call(10.0),
                unittest.mock.call(30.0),
            ],
        )

    def test_requests_after_first_429_are_paced(
        self,
    ):
        session = FakeSession(
            graph_data={
                "p0": None,
                "p1": None,
            },
            get_responses=[
                FakeResponse(status_code=429),
                FakeResponse(
                    payload=available_product(
                        "product-one"
                    ),
                ),
                FakeResponse(
                    payload=available_product(
                        "product-two"
                    ),
                ),
            ],
        )

        with patch.object(
            shop3345.time,
            "sleep",
        ) as mocked_sleep:
            result = self.fetch(
                session,
                [
                    "product-one",
                    "product-two",
                ],
            )

        self.assertEqual(
            result,
            {
                "product-one": "in_stock",
                "product-two": "in_stock",
            },
        )
        self.assertEqual(len(session.get_calls), 3)

        # Eerst de 429-backoff, daarna pacing vóór het
        # volgende afzonderlijke Ajax-productrequest.
        self.assertEqual(
            mocked_sleep.call_args_list,
            [
                unittest.mock.call(10.0),
                unittest.mock.call(1.0),
            ],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
