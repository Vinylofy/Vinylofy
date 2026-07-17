import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from scripts.scrapers.usf.jobs import (
    refresh_shop3345_listing_prices as shop3345,
)


class Shop3345StorefrontSecondhandTests(unittest.TestCase):
    def detect(self, product):
        self.assertTrue(
            hasattr(
                shop3345,
                "storefront_secondhand_from_product",
            ),
            "storefront_secondhand_from_product ontbreekt",
        )

        return shop3345.storefront_secondhand_from_product(
            product
        )

    def test_used_base_tag_is_secondhand(self):
        self.assertTrue(
            self.detect(
                {
                    "handle": "lizzy-mcalpine-five-seconds-flat",
                    "title": "Lizzy McAlpine - Five Seconds Flat",
                    "tags": [
                        "DISCONNECT_SYNC_STATUS",
                        "USED Base",
                    ],
                }
            )
        )

    def test_used_recommendation_tag_is_secondhand(self):
        self.assertTrue(
            self.detect(
                {
                    "handle": "used-ornette-coleman-science-fiction",
                    "tags": [
                        "_Format_LP",
                        "_Recommended_USED",
                        "USED Base",
                    ],
                }
            )
        )

    def test_used_in_normal_album_title_is_not_secondhand(self):
        self.assertFalse(
            self.detect(
                {
                    "handle": (
                        "tate-mcrae-i-used-to-think-"
                        "i-could-fly-lp"
                    ),
                    "title": (
                        "Tate Mcrae - I Used To Think "
                        "I Could Fly (LP)"
                    ),
                    "tags": [
                        "_Format_LP",
                        "_Genre_Pop",
                        "BOLCOM",
                    ],
                }
            )
        )

    def test_second_hand_orchestra_is_not_secondhand(self):
        self.assertFalse(
            self.detect(
                {
                    "handle": (
                        "james-yorkston-nina-persson-and-the-"
                        "second-hand-orchestra-the-great-white-"
                        "sea-eagle-coloured-vinyl"
                    ),
                    "title": (
                        "James Yorkston, Nina Persson and "
                        "The Second Hand Orchestra"
                    ),
                    "tags": [
                        "_Format_LP",
                        "_Genre_Folk / Country",
                    ],
                }
            )
        )


class Shop3345SecondhandFallbackTests(unittest.TestCase):
    def test_tate_title_is_not_a_secondhand_marker(self):
        self.assertFalse(
            shop3345.detect_secondhand(
                artist="Tate Mcrae",
                title="I Used To Think I Could Fly",
                card_text=(
                    "Tate Mcrae I Used To Think I Could Fly "
                    "LP €29,99 Add to cart"
                ),
                source_url=(
                    "https://3345.nl/products/"
                    "tate-mcrae-i-used-to-think-i-could-fly-lp"
                ),
            )
        )

    def test_second_hand_orchestra_is_not_a_marker(self):
        self.assertFalse(
            shop3345.detect_secondhand(
                artist=(
                    "James Yorkston, Nina Persson and "
                    "The Second Hand Orchestra"
                ),
                title="The Great White Sea Eagle",
                card_text=(
                    "James Yorkston Nina Persson and "
                    "The Second Hand Orchestra €15,59"
                ),
                source_url=(
                    "https://3345.nl/products/"
                    "james-yorkston-nina-persson-and-the-"
                    "second-hand-orchestra-the-great-white-"
                    "sea-eagle-coloured-vinyl"
                ),
            )
        )

    def test_used_prefix_is_still_a_marker(self):
        self.assertTrue(
            shop3345.detect_secondhand(
                artist="3345 second hand",
                title="USED - Ornette Coleman - Science Fiction",
                card_text=(
                    "3345 second hand USED - Ornette Coleman"
                ),
                source_url=(
                    "https://3345.nl/products/"
                    "used-ornette-coleman-science-fiction"
                ),
            )
        )


class Shop3345SecondhandParserIntegrationTests(
    unittest.TestCase
):
    def parse(self, *, handle, title, secondhand):
        html = f"""
        <div class="product-card">
          <a href="/products/{handle}">
            {title}
          </a>
          <cart-add-button
            data-id="123"
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
                return_value=title,
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
                return_value="29.99",
            ),
        ):
            links, _ = shop3345.parse_listing_page(
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
                variant_availability_by_handle={
                    handle: "in_stock",
                },
                secondhand_by_handle={
                    handle: secondhand,
                },
            )

        self.assertEqual(len(links), 1)
        return links[0].payload

    def test_storefront_used_tag_blocks_publication(self):
        payload = self.parse(
            handle="lizzy-mcalpine-five-seconds-flat",
            title="Lizzy McAlpine - Five Seconds Flat",
            secondhand=True,
        )

        self.assertTrue(payload["is_secondhand"])
        self.assertEqual(
            payload["availability"],
            "out_of_stock",
        )
        self.assertFalse(payload["publish_eligible"])

    def test_false_positive_title_remains_publishable(self):
        payload = self.parse(
            handle=(
                "tate-mcrae-i-used-to-think-i-could-fly-lp"
            ),
            title=(
                "Tate Mcrae - I Used To Think I Could Fly"
            ),
            secondhand=False,
        )

        self.assertFalse(payload["is_secondhand"])
        self.assertEqual(
            payload["availability"],
            "in_stock",
        )
        self.assertTrue(payload["publish_eligible"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
