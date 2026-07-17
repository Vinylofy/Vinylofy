import unittest

from bs4 import BeautifulSoup

from scripts.scrapers.usf.jobs.refresh_shop3345_listing_prices import (
    detect_source_availability,
    has_active_add_to_cart,
)


class Shop3345AvailabilityTests(unittest.TestCase):
    def check_card(self, html, expected_cta, expected_availability):
        soup = BeautifulSoup(html, "html.parser")
        card = soup.select_one(".product-card, .card-wrapper")

        self.assertIsNotNone(card)
        self.assertEqual(
            has_active_add_to_cart(card),
            expected_cta,
        )
        self.assertEqual(
            detect_source_availability(card),
            expected_availability,
        )

    def test_visible_active_button_is_in_stock(self):
        self.check_card(
            """
            <div class="product-card">
              <button name="add">Add to cart</button>
            </div>
            """,
            True,
            "in_stock",
        )

    def test_disabled_button_is_out_of_stock(self):
        self.check_card(
            """
            <div class="product-card">
              <button name="add" disabled>Add to cart</button>
            </div>
            """,
            False,
            "out_of_stock",
        )

    def test_aria_disabled_button_is_out_of_stock(self):
        self.check_card(
            """
            <div class="product-card">
              <button name="add" aria-disabled="true">
                Add to cart
              </button>
            </div>
            """,
            False,
            "out_of_stock",
        )

    def test_hidden_button_is_out_of_stock(self):
        self.check_card(
            """
            <div class="product-card">
              <button name="add" hidden>Add to cart</button>
            </div>
            """,
            False,
            "out_of_stock",
        )

    def test_explicit_sold_out_overrules_active_button(self):
        self.check_card(
            """
            <div class="product-card">
              <span>Sold out</span>
              <button name="add">Add to cart</button>
            </div>
            """,
            False,
            "out_of_stock",
        )


    def test_text_outside_button_with_active_cart_form_is_in_stock(self):
        self.check_card(
            """
            <div class="product-card">
              <form action="/cart/add" method="post">
                <span>Add to cart</span>
                <button type="submit" aria-label="Quick add"></button>
              </form>
            </div>
            """,
            True,
            "in_stock",
        )


    def test_real_3345_cart_add_element_is_in_stock(self):
        self.check_card(
            """
            <div class="card-wrapper">
              <div class="card__product-bottom" data-product-bottom>
                <span>LP</span>
                <span>€23,99</span>
                <cart-add-button
                  data-id="53556008223064"
                  data-purchase-type="instant"
                  class="plp-cta-btn"
                >
                  Add to cart
                </cart-add-button>
              </div>
            </div>
            """,
            True,
            "in_stock",
        )

    def test_3345_cart_element_without_variant_id_is_out_of_stock(self):
        self.check_card(
            """
            <div class="card-wrapper">
              <div class="card__product-bottom" data-product-bottom>
                <span>LP</span>
                <span>€23,99</span>
                <cart-add-button
                  data-purchase-type="instant"
                  class="plp-cta-btn"
                >
                  Add to cart
                </cart-add-button>
              </div>
            </div>
            """,
            False,
            "out_of_stock",
        )

    def test_hidden_3345_cart_element_is_out_of_stock(self):
        self.check_card(
            """
            <div class="card-wrapper">
              <cart-add-button
                data-id="53556008223064"
                data-purchase-type="instant"
                class="plp-cta-btn"
                hidden
              >
                Add to cart
              </cart-add-button>
            </div>
            """,
            False,
            "out_of_stock",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
