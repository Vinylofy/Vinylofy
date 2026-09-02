from __future__ import annotations

import unittest

from scripts.importers.import_blackvinyl import map_blackvinyl_row
from scripts.scrapers.blackvinyl import (
    CSV_COLUMNS,
    detail_ean_from_html,
    discover_all,
    extract_valid_gtin,
    listing_row,
)


def product(product_id: int, name: str, *, price: str = "2499") -> dict:
    return {
        "id": product_id,
        "name": name,
        "permalink": f"https://www.blackvinyl.nl/product/item-{product_id}/",
        "sku": "Label 123456",
        "description": "<p>ean: 0602438614813</p><p>LP</p>",
        "prices": {
            "price": price,
            "regular_price": "2999",
            "sale_price": price,
            "currency_code": "EUR",
            "currency_minor_unit": 2,
        },
        "is_in_stock": True,
        "stock_availability": {"text": "1 op voorraad", "class": "in-stock"},
        "categories": [{"id": 15, "name": "LP Nieuw", "slug": "vinyl-nieuw"}],
        "images": [{"src": "https://www.blackvinyl.nl/wp-content/uploads/item.jpg"}],
    }


class Response:
    def __init__(self, payload, *, url="https://www.blackvinyl.nl/wp-json/wc/store/v1/products", headers=None, text=""):
        self._payload = payload
        self.url = url
        self.headers = headers or {}
        self.text = text
        self.status_code = 200

    @property
    def ok(self):
        return True

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


class Session:
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def get(self, url, *, params=None, timeout=None):
        self.calls.append((url, params, timeout))
        page = int((params or {}).get("page", 1))
        return self.responses[page]


class BlackvinylContractTests(unittest.TestCase):
    def test_extract_valid_gtin_prefers_explicit_ean_and_validates_checkdigit(self):
        result = extract_valid_gtin("ean: 0602438614813", "Label 123456")
        self.assertEqual(result, ("0602438614813", "00602438614813"))
        self.assertIsNone(extract_valid_gtin("ean: 0602438614814"))


    def test_listing_row_keeps_current_sale_price_and_identity(self):
        row = listing_row(
            product(42, "ABBA – Voyage"),
            page=1,
            category_id=15,
            category_slug="vinyl-nieuw",
            scraped_at="2026-09-02T00:00:00+00:00",
            source_api_url="https://example.test/api?page=1",
        )
        self.assertEqual(row["artist"], "ABBA")
        self.assertEqual(row["title"], "Voyage")
        self.assertEqual(row["price"], "24.99")
        self.assertEqual(row["standard_price"], "29.99")
        self.assertEqual(row["ean"], "0602438614813")
        self.assertEqual(row["availability"], "in_stock")
        self.assertEqual(row["product_id"], "42")


    def test_discover_all_uses_api_total_pages_and_stops_after_last_page(self):
        session = Session(
            {
                1: Response([product(1, "ABBA – Voyage")], headers={"X-WP-Total": "2", "X-WP-TotalPages": "2"}),
                2: Response([product(2, "AC/DC – The Razor's Edge")], headers={"X-WP-Total": "2", "X-WP-TotalPages": "2"}),
            }
        )
        rows, total, pages = discover_all(
            session,
            category_id=15,
            category_slug="vinyl-nieuw",
            per_page=100,
            timeout=1,
        )
        self.assertEqual(total, 2)
        self.assertEqual(pages, 2)
        self.assertEqual([row["product_id"] for row in rows], ["1", "2"])
        self.assertEqual(len(session.calls), 2)


    def test_detail_ean_reads_visible_description_and_jsonld(self):
        html = """
        <html><body><span class='sku'>Label 0602438614813</span>
        <script type='application/ld+json'>{"@type":"Product","gtin13":"0602438614813"}</script>
        </body></html>
        """
        self.assertEqual(detail_ean_from_html(html), ("0602438614813", "00602438614813"))


    def test_importer_rejects_missing_ean_and_accepts_strict_gtin(self):
        accepted, reason = map_blackvinyl_row(
            {
                "ean": "0602438614813",
                "product_url": "https://www.blackvinyl.nl/product/item-1/",
                "artist": "ABBA",
                "title": "Voyage",
                "price": "24.99",
                "availability": "in_stock",
                "scraped_at": "2026-09-02T00:00:00+00:00",
                "format": "LP Nieuw",
                "detail_status": "api",
            },
            2,
        )
        self.assertIsNone(reason)
        self.assertIsNotNone(accepted)
        self.assertEqual(accepted.gtin_normalized, "00602438614813")

        rejected, reject_reason = map_blackvinyl_row(
            {
                "ean": "",
                "product_url": "https://www.blackvinyl.nl/product/item-2/",
                "artist": "Artist",
                "title": "Title",
                "price": "12.50",
                "availability": "in_stock",
                "scraped_at": "2026-09-02T00:00:00+00:00",
            },
            3,
        )
        self.assertIsNone(rejected)
        self.assertEqual(reject_reason, "missing_or_invalid_ean")


    def test_csv_contract_columns_are_stable(self):
        self.assertEqual(
            CSV_COLUMNS[:7],
            (
                "scraped_at",
                "product_url",
                "product_id",
                "artist",
                "title",
                "price",
                "standard_price",
            ),
        )
