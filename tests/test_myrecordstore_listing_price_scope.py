import unittest

from bs4 import BeautifulSoup

from scripts.scrapers.usf.jobs.refresh_myrecordstore_listing_prices import (
    extract_listing_card_prices,
)


class MyRecordStoreListingPriceScopeTest(unittest.TestCase):
    def test_ignores_price_from_neighbouring_product_tile(self):
        soup = BeautifulSoup(
            """
            <section>
              <article class="product-card">
                <a id="target" href="/item/pop/lp/michael-jackson/off-the-wall/4861155">
                  Michael Jackson - Off The Wall
                </a>
                <h2 class="ItemTile-module__itemTilePrice">85,-</h2>
              </article>

              <article class="product-card">
                <a href="/item/pop/lp/another/product/123">
                  Ander product
                </a>
                <h2 class="ItemTile-module__itemTilePrice">26,99</h2>
              </article>
            </section>
            """,
            "html.parser",
        )

        anchor = soup.select_one("#target")

        self.assertEqual(
            extract_listing_card_prices(anchor),
            ["85.00"],
        )

    def test_preserves_old_and_sale_price_inside_one_price_element(self):
        soup = BeautifulSoup(
            """
            <article class="product-card">
              <a id="target" href="/item/test/1">Testproduct</a>
              <p class="ItemTile-module__itemTilePrice">
                <del>159,99</del> 85,-
              </p>
            </article>
            """,
            "html.parser",
        )

        anchor = soup.select_one("#target")

        self.assertEqual(
            extract_listing_card_prices(anchor),
            ["159.99", "85.00"],
        )

    def test_rejects_scope_with_multiple_product_prices(self):
        soup = BeautifulSoup(
            """
            <section>
              <a id="target" href="/item/test/1">Testproduct</a>
              <p class="ItemTile-module__itemTilePrice">85,-</p>
              <p class="ItemTile-module__itemTilePrice">26,99</p>
            </section>
            """,
            "html.parser",
        )

        anchor = soup.select_one("#target")

        self.assertEqual(
            extract_listing_card_prices(anchor),
            [],
        )


if __name__ == "__main__":
    unittest.main()
