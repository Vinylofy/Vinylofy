import unittest

from scripts.scrapers.usf.jobs import (
    refresh_shop3345_listing_prices as shop3345,
)


class Shop3345VariantAvailabilityTests(unittest.TestCase):
    def availability(self, product):
        self.assertTrue(
            hasattr(
                shop3345,
                "variant_availability_from_product",
            ),
            "variant_availability_from_product ontbreekt",
        )

        function = shop3345.variant_availability_from_product
        return function(product)

    def test_any_available_variant_means_in_stock(self):
        product = {
            "handle": "bill-evans-example",
            "variants": [
                {"id": 1, "available": False},
                {"id": 2, "available": True},
            ],
        }

        self.assertEqual(
            self.availability(product),
            "in_stock",
        )

    def test_all_unavailable_variants_mean_out_of_stock(self):
        product = {
            "handle": "aerosmith-example",
            "variants": [
                {"id": 1, "available": False},
                {"id": 2, "available": False},
            ],
        }

        self.assertEqual(
            self.availability(product),
            "out_of_stock",
        )

    def test_coming_soon_tag_means_preorder(self):
        product = {
            "handle": "coming-soon-example",
            "tags": ["Rock", "Coming Soon"],
            "variants": [
                {"id": 1, "available": False},
            ],
        }

        self.assertEqual(
            self.availability(product),
            "preorder",
        )

    def test_pre_order_tag_means_preorder(self):
        product = {
            "handle": "pre-order-example",
            "tags": "Vinyl, Pre Order, New",
            "variants": [
                {"id": 1, "available": False},
            ],
        }

        self.assertEqual(
            self.availability(product),
            "preorder",
        )

    def test_missing_variants_mean_unknown(self):
        product = {
            "handle": "missing-variants-example",
        }

        self.assertEqual(
            self.availability(product),
            "unknown",
        )

    def test_incomplete_variant_values_mean_unknown(self):
        product = {
            "handle": "incomplete-example",
            "variants": [
                {"id": 1},
                {"id": 2, "available": False},
            ],
        }

        self.assertEqual(
            self.availability(product),
            "unknown",
        )

    def test_json_price_fields_do_not_affect_availability(self):
        product = {
            "handle": "price-is-not-authority",
            "price": 1,
            "variants": [
                {
                    "id": 1,
                    "available": False,
                    "price": "0.01",
                },
            ],
        }

        self.assertEqual(
            self.availability(product),
            "out_of_stock",
        )


class Shop3345VariantAvailabilityIndexTests(unittest.TestCase):
    def build_index(self, payload):
        self.assertTrue(
            hasattr(
                shop3345,
                "variant_availability_index_from_payload",
            ),
            "variant_availability_index_from_payload ontbreekt",
        )

        return (
            shop3345.variant_availability_index_from_payload(
                payload
            )
        )

    def test_builds_handle_to_availability_index(self):
        payload = {
            "products": [
                {
                    "handle": "bill-evans-example",
                    "variants": [
                        {"id": 1, "available": True},
                    ],
                },
                {
                    "handle": "aerosmith-example",
                    "variants": [
                        {"id": 2, "available": False},
                    ],
                },
                {
                    "handle": "coming-soon-example",
                    "tags": ["Coming Soon"],
                    "variants": [
                        {"id": 3, "available": False},
                    ],
                },
            ],
        }

        self.assertEqual(
            self.build_index(payload),
            {
                "bill-evans-example": "in_stock",
                "aerosmith-example": "out_of_stock",
                "coming-soon-example": "preorder",
            },
        )

    def test_ignores_products_without_handle(self):
        payload = {
            "products": [
                {
                    "variants": [
                        {"id": 1, "available": True},
                    ],
                },
            ],
        }

        self.assertEqual(self.build_index(payload), {})

    def test_malformed_payload_returns_empty_index(self):
        self.assertEqual(
            self.build_index({"products": "not-a-list"}),
            {},
        )

    def test_json_prices_are_not_added_to_index(self):
        payload = {
            "products": [
                {
                    "handle": "price-must-be-ignored",
                    "price": 999999,
                    "variants": [
                        {
                            "id": 1,
                            "available": False,
                            "price": "0.01",
                        },
                    ],
                },
            ],
        }

        self.assertEqual(
            self.build_index(payload),
            {
                "price-must-be-ignored": "out_of_stock",
            },
        )


class Shop3345PublicAvailabilityTests(unittest.TestCase):
    def public_status(
        self,
        source_availability,
        *,
        secondhand=False,
        add_to_cart=True,
    ):
        return shop3345.public_availability(
            source_availability=source_availability,
            secondhand=secondhand,
            add_to_cart=add_to_cart,
        )

    def test_json_in_stock_is_not_overruled_by_cart_placeholder(self):
        self.assertEqual(
            self.public_status(
                "in_stock",
                add_to_cart=False,
            ),
            "in_stock",
        )

    def test_json_out_of_stock_remains_out_of_stock(self):
        self.assertEqual(
            self.public_status("out_of_stock"),
            "out_of_stock",
        )

    def test_json_preorder_remains_preorder(self):
        self.assertEqual(
            self.public_status("preorder"),
            "preorder",
        )

    def test_missing_json_match_remains_unknown(self):
        self.assertEqual(
            self.public_status("unknown"),
            "unknown",
        )

    def test_secondhand_is_never_published_as_in_stock(self):
        self.assertEqual(
            self.public_status(
                "in_stock",
                secondhand=True,
            ),
            "out_of_stock",
        )

    def test_invalid_status_is_conservative_unknown(self):
        self.assertEqual(
            self.public_status("nonsense"),
            "unknown",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
