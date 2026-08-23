from __future__ import annotations

import unittest

from scripts.scrapers.suburban import extract_detail_page, parse_listing_page


def listing_html(price: str = "€32,99", sale: bool = False) -> str:
    price_markup = (
        f'<del><span class="amount">€39,99</span></del>'
        f'<ins><span class="amount">{price}</span></ins>'
        if sale
        else f'<span class="woocommerce-Price-amount amount">{price}</span>'
    )
    return f"""
    <ul class="products">
      <li class="product type-product instock">
        <div class="product-tile">
          <a href="/product/example-record/">
            <div class="product-tile__title uc">
              <span class="artist"><strong>Example Artist</strong></span>
              <span class="title">Example Record</span>
            </div>
            <div class="product-tile__format_price">
              <div class="product-tile__price">{price_markup}</div>
              <div class="product-tile__artist label">LP</div>
            </div>
          </a>
        </div>
      </li>
    </ul>
    """


class SuburbanScraperTest(unittest.TestCase):
    def test_listing_extracts_artist_title_current_price_and_availability(self):
        rows = parse_listing_page(listing_html())
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["artist"], "Example Artist")
        self.assertEqual(rows[0]["title"], "Example Record")
        self.assertEqual(rows[0]["price"], "32.99")
        self.assertEqual(rows[0]["standard_price"], "")
        self.assertEqual(rows[0]["availability"], "in_stock")
        self.assertEqual(rows[0]["detail_status"], "pending")

    def test_strike_through_is_standard_price_not_current_price(self):
        row = parse_listing_page(listing_html("€19,95", sale=True))[0]
        self.assertEqual(row["price"], "19.95")
        self.assertEqual(row["standard_price"], "39.99")

    def test_detail_extracts_barcode_without_replacing_listing_fields(self):
        details = extract_detail_page(
            """
            <main>
              <div class="product-info">
                <div class="price">€999,99</div>
                <p class="stock in-stock">Op voorraad</p>
                <div class="release"><div class="label">LP</div></div>
              </div>
              <div class="product-barcode"><span class="product-barcode__label">Barcode:</span>
                <span class="product-barcode__value">8718521078584</span></div>
              <p>Release: 29-05-2026</p>
            </main>
            """
        )
        self.assertEqual(details["ean"], "8718521078584")
        self.assertEqual(details["detail_status"], "ok")
        self.assertEqual(details["release_date"], "29-05-2026")
        self.assertEqual(details["format"], "LP")


if __name__ == "__main__":
    unittest.main()
