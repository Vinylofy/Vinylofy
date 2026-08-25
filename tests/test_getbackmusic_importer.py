from __future__ import annotations

import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.automation.pipeline_config import get_shop_config
from scripts.importers.import_getbackmusic import (
    SHOP_DEFINITION,
    apply_existing_offer_ean_matches,
    map_getbackmusic_row,
)
from scripts.importers.registry import get_shop_importer


def source_row(**overrides: str) -> dict[str, str]:
    row = {
        "scraped_at": "2026-08-23T12:00:00+00:00",
        "product_url": "https://www.getbackmusic.nl/products/example-record",
        "product_id": "101",
        "variant_id": "1001",
        "product_key": "getbackmusic:101:1001",
        "ean": "8718521078584",
        "artist": "Example Artist",
        "title": "Example Record",
        "format": "LP",
        "price": "19,95",
        "standard_price": "29,95",
        "is_sale": "true",
        "availability": "in_stock",
        "detail_status": "ok",
        "image_url": "https://www.getbackmusic.nl/cdn/example.jpg",
    }
    row.update(overrides)
    return row


class GetBackMusicImporterTest(unittest.TestCase):
    def test_mapper_uses_listing_sale_price_and_strict_ean_gatekeeper(self):
        record, error = map_getbackmusic_row(source_row(), 2)
        self.assertIsNone(error)
        self.assertEqual(record.price, 19.95)
        self.assertEqual(record.ean, "8718521078584")
        self.assertEqual(record.gtin_normalized, "08718521078584")
        self.assertEqual(record.product_handle, "101:1001")
        self.assertEqual(record.raw["standard_price"], "29,95")
        self.assertEqual(record.cover_candidate_url, source_row()["image_url"])

        rejected, error = map_getbackmusic_row(source_row(ean=""), 3)
        self.assertIsNone(rejected)
        self.assertEqual(error, "missing_or_invalid_ean")

    def test_pending_detail_is_rejected_without_publishing_listing_only_row(self):
        rejected, error = map_getbackmusic_row(source_row(ean="", detail_status="pending"), 3)
        self.assertIsNone(rejected)
        self.assertEqual(error, "detail_not_attempted")

    def test_existing_offer_url_can_reuse_ean_without_creating_new_product(self):
        rows = [source_row(ean="", detail_status="listing")]
        candidates = {
            rows[0]["product_url"]: (("existing-product", "8718521078584"),),
        }
        self.assertEqual(apply_existing_offer_ean_matches(rows, candidates), 1)
        self.assertEqual(rows[0]["ean"], "8718521078584")
        self.assertEqual(rows[0]["detail_status"], "existing_offer_ean_reused")

    def test_ambiguous_existing_offer_url_stays_ean_gated(self):
        rows = [source_row(ean="", detail_status="listing")]
        candidates = {
            rows[0]["product_url"]: (
                ("product-a", "8718521078584"),
                ("product-b", "0601234567895"),
            ),
        }
        self.assertEqual(apply_existing_offer_ean_matches(rows, candidates), 0)
        self.assertEqual(rows[0]["ean"], "")

    def test_registry_headers_pipeline_and_shipping_contract(self):
        self.assertIs(get_shop_importer("getbackmusic"), SHOP_DEFINITION)
        self.assertEqual(get_shop_config("getbackmusic").importer_module, SHOP_DEFINITION.importer_module)
        self.assertTrue(set(SHOP_DEFINITION.required_columns).issubset(SHOP_DEFINITION.all_declared_columns))
        with TemporaryDirectory() as directory:
            path = Path(directory) / "source.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=SHOP_DEFINITION.all_declared_columns)
                writer.writeheader()
                writer.writerow(source_row())
            self.assertTrue(path.exists())
        with Path("data/shipping/vinylofy_shipping_rules_nl.csv").open(encoding="utf-8", newline="") as handle:
            rows = {row["shop_slug"]: row for row in csv.DictReader(handle)}
        self.assertEqual(rows["getbackmusic"]["shipping_cost_cents"], "725")
        self.assertEqual(rows["getbackmusic"]["free_shipping_threshold_cents"], "7500")
        self.assertEqual(rows["getbackmusic"]["shipping_logic"], "threshold")


if __name__ == "__main__":
    unittest.main()
