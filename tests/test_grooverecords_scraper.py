from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts.scrapers.grooverecords import (
    Category,
    RateLimitedClient,
    discover_categories,
    enrich_details,
    normalize_url,
    parse_detail_page,
    parse_listing_page,
    scrape_listings,
)


def listing_html(*, out_of_stock: bool = False, has_next: bool = True) -> str:
    stock = "Uitverkocht" if out_of_stock else ""
    button = "<button disabled>Bestellen</button>" if out_of_stock else "<button>Bestellen</button>"
    next_link = '<a class="listforward" href="/nl/page/rock/30/1">Volgende</a>' if has_next else ""
    return f"""
    <html><body>
      <article class="product" data-aid="123">
        <a href="https://www.grooverecords.nl/nl/product/example-record?aid=123">
          <span class="articleArtist">Example Artist</span>
          <span class="articleTitle">Example Record</span>
        </a>
        <span class="articlePrice">€ 19,95</span>
        <span class="articlePricerecommended"><del>€ 29,95</del></span>
        <span class="articleAvailability">{stock}</span>
        {button}
        <img src="https://www.grooverecords.nl/images/example.jpg">
      </article>
      {next_link}
    </body></html>
    """


def detail_html() -> str:
    return """
    <html><body>
      <div class="details">
        <div>EAN: 8718521078584</div>
        <div>Label: Example Label</div>
        <div>Releasedatum: 2026</div>
      </div>
      <div class="price"><span class="articlePrice">€ 999,99</span></div>
      <button>Bestellen</button>
    </body></html>
    """


class GrooveRecordsScraperTest(unittest.TestCase):
    def test_listing_uses_current_price_and_keeps_struck_standard_price(self):
        rows, next_url, skips = parse_listing_page(listing_html(), Category("rock", "30", "Rock"), 0)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["price"], "19.95")
        self.assertEqual(rows[0]["standard_price"], "29.95")
        self.assertEqual(rows[0]["availability"], "in_stock")
        self.assertEqual(rows[0]["artist"], "Example Artist")
        self.assertEqual(rows[0]["title"], "Example Record")
        self.assertEqual(rows[0]["product_id"], "123")
        self.assertEqual(next_url, "https://grooverecords.nl/nl/page/rock/30/1")
        self.assertFalse(skips)

    def test_listing_availability_is_scoped_to_card_and_can_be_out_of_stock(self):
        rows, _next, _skips = parse_listing_page(
            listing_html(out_of_stock=True, has_next=False), Category("rock", "30", "Rock"), 0
        )
        self.assertEqual(rows[0]["availability"], "out_of_stock")

    def test_canonical_host_is_normalized_and_non_product_category_is_excluded(self):
        self.assertEqual(
            normalize_url("https://www.grooverecords.nl/nl/product/example?aid=123&utm_source=x"),
            "https://grooverecords.nl/nl/product/example?aid=123",
        )
        html = """
        <nav>
          <a href="/nl/page/rock/30/0">Rock</a>
          <a href="/nl/page/accessoires/99/0">Accessoires</a>
          <a href="/nl/page/jazz/31/0">Jazz</a>
        </nav>
        """
        categories = discover_categories(html)
        self.assertEqual([(item.slug, item.group_id) for item in categories], [("jazz", "31"), ("rock", "30")])

    def test_detail_extracts_ean_and_does_not_override_listing_fields(self):
        rows, _next, _skips = parse_listing_page(listing_html(), Category("rock", "30", "Rock"), 0)
        before = rows[0]["price"], rows[0]["availability"]

        class FakeSession:
            def get(self, _url, **_kwargs):
                class Response:
                    status_code = 200
                    text = detail_html()

                return Response()

        client = RateLimitedClient(FakeSession(), 30)
        with patch("scripts.scrapers.grooverecords.time.sleep"):
            attempted = enrich_details(client, rows, 1)
        self.assertEqual(attempted, 1)
        self.assertEqual(rows[0]["ean"], "8718521078584")
        self.assertEqual(rows[0]["detail_status"], "ok")
        self.assertEqual((rows[0]["price"], rows[0]["availability"]), before)
        self.assertEqual(parse_detail_page(detail_html())["detail_availability_observed"], "in_stock")

    def test_scrape_deduplicates_stable_product_id_between_categories(self):
        class FakeClient:
            def __init__(self):
                self.calls = []

            def get(self, url):
                self.calls.append(url)
                return listing_html(has_next=False)

        client = FakeClient()
        rows, skips, pages = scrape_listings(
            client,
            [Category("rock", "30", "Rock"), Category("jazz", "31", "Jazz")],
            1,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(skips["duplicate_between_categories"], 1)
        self.assertEqual(pages, 2)

    def test_rate_limiter_rejects_unsafe_delay(self):
        with self.assertRaises(ValueError):
            RateLimitedClient(object(), 29.99)


if __name__ == "__main__":
    unittest.main()
