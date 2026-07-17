import unittest

import requests

from scripts.scrapers.usf.jobs import (
    refresh_shop3345_listing_prices as shop3345,
)


HOMEPAGE_HTML = """
<html>
  <head>
    <script src="/cdn/shop/t/47/assets/main.js?v=123"></script>
  </head>
</html>
"""

MAIN_JAVASCRIPT = """
const client = createStorefrontClient({
  storeDomain:"3345.nl",
  apiVersion:"2024-04",
  publicAccessToken:"public-token-for-tests",
  retries:3
});
"""


class FakeResponse:
    def __init__(
        self,
        *,
        text="",
        payload=None,
        status_code=200,
        url="https://3345.nl/test",
    ):
        self.text = text
        self._payload = payload
        self.status_code = status_code
        self.url = url

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
        get_responses=None,
        post_responses=None,
    ):
        self.get_responses = list(
            get_responses or []
        )
        self.post_responses = list(
            post_responses or []
        )
        self.get_calls = []
        self.post_calls = []

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


class Shop3345StorefrontConfigTests(unittest.TestCase):
    def test_loads_public_config_once(self):
        self.assertTrue(
            hasattr(shop3345, "fetch_storefront_config"),
            "fetch_storefront_config ontbreekt",
        )

        session = FakeSession(
            get_responses=[
                FakeResponse(
                    text=HOMEPAGE_HTML,
                    url="https://3345.nl/nl",
                ),
                FakeResponse(
                    text=MAIN_JAVASCRIPT,
                    url=(
                        "https://3345.nl/cdn/shop/"
                        "assets/main.js?v=123"
                    ),
                ),
            ]
        )

        self.assertEqual(
            shop3345.fetch_storefront_config(session),
            (
                "2024-04",
                "public-token-for-tests",
            ),
        )

        self.assertEqual(len(session.get_calls), 2)


class Shop3345ListingHandleTests(unittest.TestCase):
    def test_extracts_unique_handles_from_listing_html(self):
        self.assertTrue(
            hasattr(shop3345, "extract_listing_handles"),
            "extract_listing_handles ontbreekt",
        )

        html = """
        <div>
          <a href="/products/bill-evans-example">Bill</a>
          <a href="/nl/products/aerosmith-example">Aero</a>
          <a href="/products/bill-evans-example">Bill duplicate</a>
          <a href="/collections/all">Geen product</a>
        </div>
        """

        self.assertEqual(
            shop3345.extract_listing_handles(html),
            [
                "bill-evans-example",
                "aerosmith-example",
            ],
        )


class Shop3345PerPageAvailabilityTests(unittest.TestCase):
    def fetch(self, session, handles):
        self.assertTrue(
            hasattr(
                shop3345,
                "fetch_variant_availability_for_handles",
            ),
            "fetch_variant_availability_for_handles ontbreekt",
        )

        return (
            shop3345.fetch_variant_availability_for_handles(
                session,
                handles=handles,
                api_version="2024-04",
                public_access_token="public-token-for-tests",
            )
        )

    def test_fetches_listing_handles_in_one_graphql_request(self):
        session = FakeSession(
            post_responses=[
                FakeResponse(
                    payload={
                        "data": {
                            "p0": {
                                "handle": "bill-evans-example",
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
                            "p1": {
                                "handle": "aerosmith-example",
                                "tags": [],
                                "availableForSale": False,
                                "variants": {
                                    "nodes": [
                                        {
                                            "availableForSale": False,
                                        },
                                    ],
                                },
                            },
                            "p2": None,
                        },
                    }
                ),
            ],
            get_responses=[
                FakeResponse(
                    status_code=404,
                    url=(
                        "https://3345.nl/products/"
                        "missing-example.js"
                    ),
                ),
            ],
        )

        result = self.fetch(
            session,
            [
                "bill-evans-example",
                "aerosmith-example",
                "missing-example",
                "bill-evans-example",
            ],
        )

        self.assertEqual(
            result,
            {
                "bill-evans-example": "in_stock",
                "aerosmith-example": "out_of_stock",
            },
        )

        self.assertEqual(len(session.post_calls), 1)
        self.assertEqual(len(session.get_calls), 1)
        self.assertTrue(
            session.get_calls[0]["url"].endswith(
                "/products/missing-example.js"
            )
        )

        request_json = (
            session.post_calls[0]["kwargs"]["json"]
        )
        query = request_json["query"]
        variables = request_json["variables"]

        self.assertIn("productByHandle", query)
        self.assertNotIn("products(first:", query)
        self.assertNotIn("price", query.lower())
        self.assertNotIn("amount", query.lower())

        self.assertEqual(
            sorted(variables.values()),
            [
                "aerosmith-example",
                "bill-evans-example",
                "missing-example",
            ],
        )

    def test_graphql_errors_abort(self):
        session = FakeSession(
            post_responses=[
                FakeResponse(
                    payload={
                        "errors": [
                            {
                                "message": "Storefront error",
                            },
                        ],
                    }
                ),
            ]
        )

        with self.assertRaises(RuntimeError):
            self.fetch(
                session,
                ["bill-evans-example"],
            )

    def test_empty_handle_list_performs_no_request(self):
        session = FakeSession()

        self.assertEqual(
            self.fetch(session, []),
            {},
        )
        self.assertEqual(session.post_calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
