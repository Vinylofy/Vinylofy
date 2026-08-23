from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts.scrapers.getbackmusic import (
    GetBackClient,
    enrich_details,
    next_page_number,
    parse_detail_page,
    parse_detail_limit,
    merge_partial_listing_with_previous,
    parse_listing_page,
    merge_listing_with_previous,
    scrape_listings,
)
from bs4 import BeautifulSoup


def card(*, product_id="101", variant_id="1001", sale=False, available=True, second_variant=False):
    price = '<span class="sale-price">€ 19,95</span><del>€ 29,95</del>' if sale else '<span>€ 24,95</span>'
    disabled = " disabled" if not available else ""
    extra = '<input name="id" value="1002">' if second_variant else ""
    return f"""
    <article class="cardmnsy">
      <product-form-component data-product-id="{product_id}">
        <form action="/cart/add" method="post">
          <input name="id" value="{variant_id}" checked>{extra}
          <button type="submit"{disabled}>Toevoegen</button>
        </form>
      </product-form-component>
      <a class="cardmnsy__cover-link" data-url="/collections/lp-vinyl/products/example-record"></a>
      <a class="cardmnsy__artist" data-product-url="/collections/lp-vinyl/products/example-record">Example Artist</a>
      <a class="cardmnsy__title" href="/collections/lp-vinyl/products/example-record">Example Record</a>
      <img class="cardmnsy__img" src="/cdn/example.jpg">
      <span class="cardmnsy__format">LP</span>
      <span class="cardmnsy__price">{price}</span>
    </article>
    """


def listing_html(*, page=1, has_next=True, repeated=False):
    href = f'<a rel="next" href="/collections/lp-vinyl?page={page + 1}">next</a>' if has_next else ""
    first = card(sale=True, second_variant=True)
    second = card(product_id="102", variant_id="2001", available=False)
    if repeated:
        second = card(product_id="102", variant_id="2001", available=False)
    return f"<html><body>{first}{second}{href}</body></html>"


def detail_html():
    return """
    <html><body>
      <h1>Example Record</h1>
      <div class="product__description">Observed metadata</div>
      <dl><dt>EAN</dt><dd>8718521078584</dd></dl>
      <p>Release date: 2026-01-01</p>
    </body></html>
    """


class GetBackMusicScraperTest(unittest.TestCase):
    def test_listing_parses_sale_regular_variant_and_availability(self):
        rows, next_page, identities, skips = parse_listing_page(listing_html(), 1)
        self.assertEqual(len(rows), 2)
        self.assertEqual(len(identities), 2)
        self.assertEqual(rows[0]["product_id"], "101")
        self.assertEqual(rows[0]["variant_id"], "1001")
        self.assertEqual(rows[0]["price"], "19.95")
        self.assertEqual(rows[0]["standard_price"], "29.95")
        self.assertEqual(rows[0]["is_sale"], "true")
        self.assertEqual(rows[0]["availability"], "in_stock")
        self.assertEqual(rows[1]["price"], "24.95")
        self.assertEqual(rows[1]["standard_price"], "")
        self.assertEqual(rows[1]["availability"], "out_of_stock")
        self.assertEqual(next_page, 2)
        self.assertFalse(skips)

    def test_next_page_requires_immediate_server_pagination(self):
        soup = BeautifulSoup(listing_html(), "html.parser")
        self.assertEqual(next_page_number(soup, 1), 2)
        self.assertEqual(next_page_number(BeautifulSoup(listing_html(has_next=False), "html.parser"), 1), 2)

    def test_detail_extracts_direct_ean_and_does_not_overwrite_listing_fields(self):
        rows, _next, _ids, _skips = parse_listing_page(listing_html(), 1)
        before = rows[0]["price"], rows[0]["standard_price"], rows[0]["availability"]

        class FakeClient:
            def get(self, _url):
                return detail_html()

        rows[0]["ean"] = ""
        with patch("scripts.scrapers.getbackmusic.time.sleep"):
            self.assertEqual(enrich_details(FakeClient(), rows, 1), 1)
        self.assertEqual(rows[0]["ean"], "8718521078584")
        self.assertEqual(rows[0]["detail_status"], "ok")
        self.assertEqual((rows[0]["price"], rows[0]["standard_price"], rows[0]["availability"]), before)

    def test_listing_stops_on_repeated_variant_set(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def get(self, url):
                self.calls.append(url)
                return listing_html(repeated=True)

        client = FakeClient()
        rows, skips, pages = scrape_listings(client, max_pages=0)
        self.assertEqual(len(rows), 2)
        self.assertEqual(pages, 1)
        self.assertEqual(skips["repeated_product_variant_set"], 1)
        self.assertEqual(client.calls, [
            "https://www.getbackmusic.nl/collections/lp-vinyl",
            "https://www.getbackmusic.nl/collections/lp-vinyl?page=2",
        ])

    def test_missing_or_disabled_cart_action_is_not_available(self):
        html = card().replace('<button type="submit">Toevoegen</button>', "").replace('class="cardmnsy__price"', 'class="cardmnsy__price">')
        rows, _next, _ids, _skips = parse_listing_page(f"<article>{html}</article>", 1)
        self.assertEqual(rows[0]["availability"], "out_of_stock")

    def test_non_vinyl_format_is_skipped(self):
        html = card().replace('<span class="cardmnsy__format">LP</span>', '<span class="cardmnsy__format">CD</span>')
        rows, _next, _ids, skips = parse_listing_page(html, 1)
        self.assertEqual(rows, [])
        self.assertEqual(skips["non_vinyl"], 1)

    def test_shopify_collection_product_path_and_data_url_are_normalized(self):
        rows, _next, _ids, skips = parse_listing_page(card(), 1)
        self.assertEqual(rows[0]["product_url"], "https://www.getbackmusic.nl/products/example-record")
        self.assertFalse(skips)

    def test_listing_refresh_preserves_detail_fields(self):
        rows, _next, _ids, _skips = parse_listing_page(card(), 1)
        rows[0]["ean"] = ""
        previous = [{
            "product_key": rows[0]["product_key"],
            "ean": "8718521078584",
            "detail_status": "ok",
            "detail_title": "Stored detail",
        }]
        merge_listing_with_previous(rows, previous)
        self.assertEqual(rows[0]["ean"], "8718521078584")
        self.assertEqual(rows[0]["detail_status"], "ok")
        self.assertEqual(rows[0]["detail_title"], "Stored detail")

    def test_detail_limit_accepts_all_value(self):
        self.assertIsNone(parse_detail_limit("all"))
        self.assertIsNone(parse_detail_limit("0"))
        self.assertEqual(parse_detail_limit("25"), 25)

    def test_partial_listing_retains_records_not_reached_before_rate_limit(self):
        current = [{"product_key": "current", "price": "10.00"}]
        previous = [
            {"product_key": "current", "price": "9.00"},
            {"product_key": "not-reached", "price": "20.00", "ean": "8718521078584"},
        ]
        merged = merge_partial_listing_with_previous(current, previous)
        self.assertEqual([row["product_key"] for row in merged], ["current", "not-reached"])
        self.assertEqual(merged[0]["price"], "10.00")
        self.assertEqual(merged[1]["ean"], "8718521078584")


if __name__ == "__main__":
    unittest.main()
