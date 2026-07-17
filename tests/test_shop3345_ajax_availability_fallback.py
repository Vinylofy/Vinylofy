import unittest

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
        url="https://3345.nl/test",
    ):
        self._payload = payload
        self.status_code = status_code
        self.url = url
        self.text = ""

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(
                f"HTTP {self.status_code}"
            )

    def json(self):
        return self._payload


class FakeSession:
    def __init__(
        self,
        *,
        post_responses=None,
        get_responses=None,
    ):
        self.post_responses = list(
            post_responses or []
        )
        self.get_responses = list(
            get_responses or []
        )
        self.post_calls = []
        self.get_calls = []

    def post(self, url, **kwargs):
        self.post_calls.append(
            {
                "url": url,
                "kwargs": kwargs,
            }
        )

        if not self.post_responses:
            raise AssertionError(
                f"Onverwachte POST-request: {url}"
            )

        return self.post_responses.pop(0)

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


def storefront_payload(products):
    return FakeResponse(
        payload={
            "data": {
                f"p{index}": product
                for index, product in enumerate(products)
            },
        }
    )


class Shop3345AjaxFallbackTests(unittest.TestCase):
    def fetch(
        self,
        session,
        handles,
        *,
        secondhand_by_handle=None,
    ):
        return (
            shop3345.fetch_variant_availability_for_handles(
                session,
                handles=handles,
                api_version="2024-04",
                public_access_token="public-token",
                secondhand_by_handle=secondhand_by_handle,
            )
        )

    def test_only_storefront_misses_use_ajax_fallback(self):
        secondhand = {}

        session = FakeSession(
            post_responses=[
                storefront_payload(
                    [
                        {
                            "handle": "storefront-match",
                            "tags": [],
                            "availableForSale": True,
                            "variants": {
                                "nodes": [
                                    {
                                        "availableForSale": True,
                                    },
                                ],
                            },
                        },
                        None,
                        None,
                    ]
                ),
            ],
            get_responses=[
                FakeResponse(
                    url=(
                        "https://3345.nl/products/"
                        "ajax-in-stock.js"
                    ),
                    payload={
                        "handle": "ajax-in-stock",
                        "available": True,
                        "price": 999999,
                        "compare_at_price": 1234567,
                        "tags": [],
                        "variants": [
                            {
                                "available": True,
                                "price": 999999,
                            },
                        ],
                    },
                ),
                FakeResponse(
                    url=(
                        "https://3345.nl/products/"
                        "ajax-secondhand.js"
                    ),
                    payload={
                        "handle": "ajax-secondhand",
                        "available": False,
                        "price": 1,
                        "tags": [
                            "USED Base",
                            "DISCONNECT_SYNC_STATUS",
                        ],
                        "variants": [
                            {
                                "available": False,
                                "price": 1,
                            },
                        ],
                    },
                ),
            ],
        )

        result = self.fetch(
            session,
            [
                "storefront-match",
                "ajax-in-stock",
                "ajax-secondhand",
            ],
            secondhand_by_handle=secondhand,
        )

        self.assertEqual(
            result,
            {
                "storefront-match": "in_stock",
                "ajax-in-stock": "in_stock",
                "ajax-secondhand": "out_of_stock",
            },
        )

        self.assertEqual(
            secondhand,
            {
                "storefront-match": False,
                "ajax-in-stock": False,
                "ajax-secondhand": True,
            },
        )

        self.assertEqual(len(session.post_calls), 1)
        self.assertEqual(len(session.get_calls), 2)

        requested_urls = [
            call["url"]
            for call in session.get_calls
        ]

        self.assertNotIn(
            "storefront-match.js",
            " ".join(requested_urls),
        )
        self.assertTrue(
            requested_urls[0].endswith(
                "/products/ajax-in-stock.js"
            )
        )
        self.assertTrue(
            requested_urls[1].endswith(
                "/products/ajax-secondhand.js"
            )
        )

    def test_ajax_price_fields_do_not_affect_status(self):
        session = FakeSession(
            post_responses=[
                storefront_payload([None]),
            ],
            get_responses=[
                FakeResponse(
                    payload={
                        "handle": "price-independent",
                        "available": True,
                        "price": 0,
                        "compare_at_price": 99999999,
                        "variants": [
                            {
                                "available": True,
                                "price": 0,
                            },
                        ],
                        "tags": [],
                    },
                ),
            ],
        )

        self.assertEqual(
            self.fetch(
                session,
                ["price-independent"],
            ),
            {
                "price-independent": "in_stock",
            },
        )

    def test_ajax_404_remains_unknown(self):
        session = FakeSession(
            post_responses=[
                storefront_payload([None]),
            ],
            get_responses=[
                FakeResponse(
                    status_code=404,
                    url=(
                        "https://3345.nl/products/"
                        "missing-product.js"
                    ),
                ),
            ],
        )

        self.assertEqual(
            self.fetch(
                session,
                ["missing-product"],
            ),
            {},
        )

    def test_ajax_server_error_aborts_scan(self):
        session = FakeSession(
            post_responses=[
                storefront_payload([None]),
            ],
            get_responses=[
                FakeResponse(
                    status_code=500,
                    url=(
                        "https://3345.nl/products/"
                        "server-error.js"
                    ),
                ),
            ],
        )

        with self.assertRaises(
            requests.HTTPError
        ):
            self.fetch(
                session,
                ["server-error"],
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
