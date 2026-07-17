import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from scripts.scrapers.usf.jobs import (
    refresh_shop3345_listing_prices as shop3345,
)


class Shop3345ListingVariantIntegrationTests(unittest.TestCase):
    def parse_product(
        self,
        *,
        handle,
        source_availability_by_handle,
        price="29.99",
    ):
        html = f"""
        <div class="product-card">
          <a href="/products/{handle}">
            Test Artist - Test Album
          </a>

          <cart-add-button
            data-id="123456789"
            data-purchase-type="instant"
          >
            Add to cart
          </cart-add-button>
        </div>
        """

        with (
            patch.object(
                shop3345,
                "extract_title",
                return_value="Test Artist - Test Album",
            ),
            patch.object(
                shop3345,
                "extract_artist",
                return_value="Test Artist",
            ),
            patch.object(
                shop3345,
                "extract_listing_format",
                return_value="LP",
            ),
            patch.object(
                shop3345,
                "extract_price",
                return_value=price,
            ),
        ):
            return shop3345.parse_listing_page(
                html,
                page=1,
                listing_url=(
                    "https://3345.nl/nl/collections/all?page=1"
                ),
                seen_at=datetime(
                    2026,
                    7,
                    17,
                    tzinfo=timezone.utc,
                ),
                debug=False,
                variant_availability_by_handle=(
                    source_availability_by_handle
                ),
            )

    def test_json_out_of_stock_overrules_html_cart_placeholder(self):
        links, offers = self.parse_product(
            handle="aerosmith-example",
            source_availability_by_handle={
                "aerosmith-example": "out_of_stock",
            },
        )

        self.assertEqual(len(links), 1)
        self.assertEqual(len(offers), 1)

        payload = links[0].payload

        self.assertEqual(
            payload["source_availability"],
            "out_of_stock",
        )
        self.assertEqual(
            payload["availability"],
            "out_of_stock",
        )
        self.assertTrue(
            payload["listing_cta_add_to_cart"]
        )
        self.assertFalse(
            payload["publish_eligible"]
        )
        self.assertEqual(
            offers[0].availability,
            "out_of_stock",
        )

    def test_json_in_stock_remains_in_stock(self):
        links, offers = self.parse_product(
            handle="bill-evans-example",
            source_availability_by_handle={
                "bill-evans-example": "in_stock",
            },
        )

        payload = links[0].payload

        self.assertEqual(
            payload["source_availability"],
            "in_stock",
        )
        self.assertEqual(
            payload["availability"],
            "in_stock",
        )
        self.assertTrue(
            payload["publish_eligible"]
        )
        self.assertEqual(
            offers[0].availability,
            "in_stock",
        )

    def test_missing_json_match_is_unknown_not_in_stock(self):
        links, offers = self.parse_product(
            handle="missing-json-example",
            source_availability_by_handle={},
        )

        payload = links[0].payload

        self.assertEqual(
            payload["source_availability"],
            "unknown",
        )
        self.assertEqual(
            payload["availability"],
            "unknown",
        )
        self.assertFalse(
            payload["publish_eligible"]
        )
        self.assertEqual(
            offers[0].availability,
            "unknown",
        )

    def test_listing_html_remains_price_authority(self):
        links, offers = self.parse_product(
            handle="html-price-example",
            source_availability_by_handle={
                "html-price-example": "in_stock",
            },
            price="37.49",
        )

        payload = links[0].payload

        self.assertEqual(payload["price"], "37.49")
        self.assertEqual(
            payload["html_listing_price"],
            "37.49",
        )
        self.assertEqual(
            payload["listing_price_transport"],
            "html",
        )
        self.assertEqual(
            payload["price_source"],
            "listing",
        )
        self.assertEqual(
            offers[0].price,
            "37.49",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
