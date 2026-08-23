from __future__ import annotations

import unittest
from unittest.mock import patch

from bs4 import BeautifulSoup

from scripts.scrapers.viprecords import (
    RateLimitedClient,
    enrich_details,
    next_page_number,
    parse_detail_page,
    parse_detail_limit,
    parse_listing_page,
    scrape_listings,
)


def listing_html(*, page: int = 1, has_next: bool = True, duplicate: bool = False) -> str:
    next_button = '<button class="hook__category-load-page" data-page-number="2">Volgende</button>' if has_next else ""
    second_id = "1001" if duplicate else "1002"
    return f"""
    <html><body>
      <div class="page__small-pagination">{next_button}</div>
      <div class="cs-products">
        <div class="cs-product">
          <a class="hook_ShowProduct" data-product-id="1001" href="/Webwinkel-Product-1001/example.html"></a>
          <a class="cs-product__title" href="/Webwinkel-Product-1001/example.html">Example Artist - Example Record (LP)</a>
          <img class="cs-product__img" data-src="/Files/example.jpg" src="/fallback.jpg">
          <div class="cs-product__prices"><span class="from__price1">44,<span class="decimals">99</span></span><span class="price1">22,<span class="decimals">49</span></span><span class="discount__perc">Korting</span></div>
          <a class="hook_AddProductToCart" data-product-id="1001"></a>
        </div>
        <div class="cs-product">
          <a class="hook_ShowProduct" data-product-id="{second_id}" href="/SECOND.html"></a>
          <a class="cs-product__title" href="/SECOND.html">Regular Artist - Regular Record (2LP)</a>
          <img class="cs-product__img" src="/fallback-2.jpg">
          <div class="cs-product__prices"><span class="price1">19,95</span></div>
          <a class="hook_AddProductToCart" data-product-id="{second_id}"></a>
        </div>
      </div>
    </body></html>
    """


def detail_html() -> str:
    return """
    <html><head><script type="application/ld+json">
    {"@type":"Product","productID":"8718521078584","offers":{"price":999.99}}
    </script></head><body>
      <span class="product-title">Example Artist - Example Record</span>
      <div id="AttributeCombinationInformation"><div class="row"><p>Artikelnummer</p><p>8718521078584</p></div></div>
      <div class="page__product__short-description">Vinyl metadata</div>
      <span>Standaard levertijd : 2 werkdagen</span>
      <span class="price1">999,99</span>
      <button>Bestellen</button>
    </body></html>
    """


class VipRecordsScraperTest(unittest.TestCase):
    def test_detail_limit_supports_full_enrichment_explicitly(self):
        self.assertIsNone(parse_detail_limit("all"))
        self.assertEqual(parse_detail_limit("10"), 10)
        with self.assertRaises(ValueError):
            parse_detail_limit("-1")

    def test_listing_selectors_parse_sale_and_regular_prices(self):
        rows, next_page, skips = parse_listing_page(listing_html(), 1)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["product_id"], "1001")
        self.assertEqual(rows[0]["price"], "22.49")
        self.assertEqual(rows[0]["standard_price"], "44.99")
        self.assertEqual(rows[0]["is_sale"], "true")
        self.assertEqual(rows[0]["availability"], "in_stock")
        self.assertEqual(rows[0]["image_url"], "https://www.viprecords.nl/Files/example.jpg")
        self.assertEqual(rows[1]["price"], "19.95")
        self.assertEqual(rows[1]["standard_price"], "")
        self.assertEqual(rows[1]["is_sale"], "false")
        self.assertEqual(next_page, 2)
        self.assertFalse(skips)

    def test_next_page_requires_the_immediate_server_indicated_page(self):
        self.assertEqual(next_page_number(BeautifulSoup(listing_html(), "html.parser"), 1), 2)
        self.assertIsNone(next_page_number(BeautifulSoup(listing_html(has_next=False), "html.parser"), 1))

    def test_detail_extracts_valid_gtin_and_keeps_listing_fields(self):
        rows, _next_page, _skips = parse_listing_page(listing_html(), 1)
        before = rows[0]["price"], rows[0]["standard_price"], rows[0]["availability"]

        class FakeSession:
            def get(self, _url, **_kwargs):
                class Response:
                    status_code = 200
                    text = detail_html()

                return Response()

        with patch("scripts.scrapers.viprecords.time.sleep"):
            attempted = enrich_details(RateLimitedClient(FakeSession(), 0), rows, 1)
        self.assertEqual(attempted, 1)
        self.assertEqual(rows[0]["ean"], "8718521078584")
        self.assertEqual(rows[0]["detail_status"], "ok")
        self.assertEqual((rows[0]["price"], rows[0]["standard_price"], rows[0]["availability"]), before)
        self.assertEqual(parse_detail_page(detail_html())["detail_availability_observed"], "in_stock")

    def test_scrape_stops_on_repeated_id_set_and_deduplicates_ids(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def get(self, url):
                self.calls.append(url)
                return listing_html()

        client = FakeClient()
        rows, skips, pages = scrape_listings(client, max_pages=0)
        self.assertEqual(len(rows), 2)
        self.assertEqual(pages, 1)
        self.assertEqual(skips["repeated_product_id_set"], 1)
        self.assertEqual(client.calls, ["https://www.viprecords.nl/vinyl", "https://www.viprecords.nl/vinyl?page=2"])


if __name__ == "__main__":
    unittest.main()
