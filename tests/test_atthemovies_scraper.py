from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.scrapers.atthemovies import (
    enrich_details,
    fetch,
    merge_cached_details,
    parse_listing_page,
    scrape_listings,
)


def listing_html(
    *,
    current="€ 25,59",
    standard="€ 31,99",
    sold_out=False,
    available=True,
    product_type="Vinyl",
    variants=None,
    page=1,
    has_next=True,
):
    variant = {"id": 20, "sku": "SKU-1", "barcode": "0602567988724"}
    if available is not None:
        variant["available"] = available
    product = {
        "id": 10,
        "handle": "example-soundtrack",
        "type": product_type,
        "variants": variants if variants is not None else [variant],
    }
    badge = '<span class="product-label--sold-out">Uitverkocht</span>' if sold_out else ""
    next_link = f'<a href="/nl/collections/all-products?page={page + 1}">{page + 1}</a>' if has_next else ""
    return f"""
    <html><head><link rel="canonical" href="https://atthemoviesshop.com/nl/collections/all-products?page={page}"></head><body>
    <script>window.backInStock.productsInCollectionLiquidObject = {json.dumps([product])};</script>
    <div class="product-block">{badge}<div class="product-block__title-price">
      <a class="title" href="/nl/collections/all-products/products/example-soundtrack">
        <div class="artist">Example Album</div><div class="title">White Vinyl</div>Example Artist
      </a>
      <div class="price on-sale"><span class="amount theme-money">{current}</span><del>{standard}</del></div>
    </div></div>{next_link}
    </body></html>
    """


class AtTheMoviesScraperTest(unittest.TestCase):
    def test_listing_uses_current_white_price_and_keeps_standard_price_separate(self):
        rows, has_next, _handles, _skips = parse_listing_page(listing_html())
        self.assertTrue(has_next)
        self.assertEqual(rows[0]["price"], "25.59")
        self.assertEqual(rows[0]["standard_price"], "31.99")
        self.assertEqual(rows[0]["availability"], "in_stock")
        self.assertEqual(rows[0]["artist"], "Example Artist")
        self.assertEqual(rows[0]["ean"], "0602567988724")

    def test_listing_availability_is_scoped_to_own_card(self):
        rows, _has_next, _handles, _skips = parse_listing_page(listing_html(sold_out=True))
        self.assertEqual(rows[0]["availability"], "out_of_stock")

    def test_non_vinyl_products_are_excluded(self):
        rows, _has_next, _handles, skips = parse_listing_page(listing_html(product_type="CD"))
        self.assertEqual(rows, [])
        self.assertEqual(skips["non_vinyl"], 1)

    def test_listing_availability_is_unknown_without_positive_or_negative_evidence(self):
        rows, _has_next, _handles, _skips = parse_listing_page(listing_html(available=None))
        self.assertEqual(rows[0]["availability"], "unknown")

    def test_variant_unavailable_is_out_of_stock_without_badge(self):
        rows, _has_next, _handles, _skips = parse_listing_page(listing_html(available=False))
        self.assertEqual(rows[0]["availability"], "out_of_stock")

    def test_multi_variant_skip_is_counted(self):
        variants = [
            {"id": 20, "available": True, "barcode": "0602567988724"},
            {"id": 21, "available": True, "barcode": "4006381333931"},
        ]
        rows, _has_next, _handles, skips = parse_listing_page(listing_html(variants=variants))
        self.assertEqual(rows, [])
        self.assertEqual(skips["multi_variant"], 1)

    def test_filtered_intermediate_page_does_not_stop_pagination(self):
        pages = [
            listing_html(product_type="CD", page=1, has_next=True),
            listing_html(page=2, has_next=False).replace("example-soundtrack", "second-soundtrack"),
        ]

        class Response:
            status_code = 200

            def __init__(self, text):
                self.text = text

            def raise_for_status(self):
                return None

        class Session:
            calls = 0

            def get(self, *_args, **_kwargs):
                response = Response(pages[self.calls])
                self.calls += 1
                return response

        session = Session()
        with patch("scripts.scrapers.atthemovies.time.sleep"):
            rows = scrape_listings(session, "https://atthemoviesshop.com/nl/collections/all-products", None)
        self.assertEqual(session.calls, 2)
        self.assertEqual(len(rows), 1)

    def test_http_429_stops_without_retry(self):
        class Response:
            status_code = 429

        class Session:
            calls = 0

            def get(self, *_args, **_kwargs):
                self.calls += 1
                return Response()

        session = Session()
        with self.assertRaisesRegex(RuntimeError, "stopped safely"):
            fetch(session, "https://atthemoviesshop.com/nl/collections/all-products")
        self.assertEqual(session.calls, 1)

    def test_detail_429_keeps_completed_rows_and_writes_checkpoint(self):
        rows = [
            {
                "handle": f"item-{index}",
                "product_url": f"https://example/item-{index}",
                "variant_id": str(index),
                "ean": "",
                "sku": "",
                "detail_status": "listing",
            }
            for index in range(5)
        ]

        class Response:
            def __init__(self, status_code, variant_id=None):
                self.status_code = status_code
                self.variant_id = variant_id

            def raise_for_status(self):
                return None

            def json(self):
                return {"variants": [{"id": self.variant_id, "barcode": f"ean-{self.variant_id}"}]}

        class Session:
            calls = 0

            def get(self, _url, **_kwargs):
                self.calls += 1
                if self.calls == 4:
                    return Response(429)
                return Response(200, self.calls - 1)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "checkpoint.csv"
            session = Session()
            enrich_details(session, rows, 5, checkpoint_path=output)

            self.assertEqual(session.calls, 4)
            self.assertEqual([row["detail_status"] for row in rows[:3]], ["ok", "ok", "ok"])
            self.assertEqual([row["detail_status"] for row in rows[3:]], ["listing", "listing"])
            with output.open(encoding="utf-8", newline="") as handle:
                checkpoint_rows = list(csv.DictReader(handle))
            self.assertEqual([row["detail_status"] for row in checkpoint_rows[:3]], ["ok", "ok", "ok"])
            self.assertEqual(len(checkpoint_rows), 5)

    def test_detail_enrichment_never_overwrites_listing_price_or_availability(self):
        rows, _has_next, _handles, _skips = parse_listing_page(listing_html())
        rows[0]["ean"] = ""

        class Response:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {
                    "price": 9999,
                    "available": False,
                    "variants": [{"id": 20, "sku": "DETAIL-SKU", "barcode": "8719262046580"}],
                }

        class Session:
            def get(self, *_args, **_kwargs):
                return Response()

        before = rows[0]["price"], rows[0]["availability"]
        enrich_details(Session(), rows, 1)
        self.assertEqual((rows[0]["price"], rows[0]["availability"]), before)
        self.assertEqual(rows[0]["ean"], "8719262046580")
        self.assertEqual(rows[0]["detail_status"], "ok")

    def test_detail_limit_resumes_only_pending_rows(self):
        rows = [
            {"handle": "done", "detail_status": "ok", "product_url": "https://example/done", "variant_id": "1", "ean": "1", "sku": ""},
            {"handle": "listing-ean", "detail_status": "listing", "product_url": "https://example/listing-ean", "variant_id": "3", "ean": "0602567988724", "sku": ""},
            {"handle": "pending", "detail_status": "listing", "product_url": "https://example/pending", "variant_id": "2", "ean": "", "sku": ""},
        ]

        class Response:
            status_code = 200

            def raise_for_status(self):
                return None

            def json(self):
                return {"variants": [{"id": 2, "barcode": "0602567988725"}]}

        class Session:
            def get(self, url, **_kwargs):
                self.url = url
                return Response()

        session = Session()
        enrich_details(session, rows, 1)
        self.assertEqual(session.url, "https://example/pending.js")
        self.assertEqual(rows[0]["ean"], "1")
        self.assertEqual(rows[1]["detail_status"], "listing")
        self.assertEqual(rows[2]["detail_status"], "ok")

    def test_cached_success_restores_detail_fields_without_listing_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "atthemovies_products.csv"
            output.write_text(
                "handle,ean,sku,detail_status,price\n"
                "example-soundtrack,8719262046580,DETAIL-SKU,ok,99.99\n",
                encoding="utf-8",
            )
            rows = [{
                "handle": "example-soundtrack",
                "ean": "0602567988724",
                "sku": "SKU-1",
                "detail_status": "listing",
                "price": "25.59",
            }]

            self.assertEqual(merge_cached_details(rows, output), 1)
            self.assertEqual(rows[0]["ean"], "8719262046580")
            self.assertEqual(rows[0]["price"], "25.59")


if __name__ == "__main__":
    unittest.main()
