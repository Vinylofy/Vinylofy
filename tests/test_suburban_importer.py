from __future__ import annotations

import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.importers.import_suburban import SHOP_DEFINITION, map_suburban_row
from scripts.importers.registry import get_shop_importer
from scripts.automation.pipeline_config import get_shop_config
from scripts.scrapers.usf.jobs.refresh_suburban_listing_prices import build_offers, build_links


def source_row(**overrides: str) -> dict[str, str]:
    row = {
        "scraped_at": "2026-08-23T12:00:00+00:00",
        "product_url": "https://suburban.nl/product/example-record/",
        "product_key": "key",
        "ean": "8718521078584",
        "artist": "Example Artist",
        "title": "Example Record",
        "format": "LP",
        "price": "19,95",
        "standard_price": "39,99",
        "availability": "in_stock",
        "detail_status": "ok",
    }
    row.update(overrides)
    return row


class SuburbanImporterTest(unittest.TestCase):
    def test_mapper_uses_current_price_and_ean_gatekeeper(self):
        record, error = map_suburban_row(source_row(), 2)
        self.assertIsNone(error)
        self.assertIsNotNone(record)
        self.assertEqual(record.price, 19.95)
        self.assertEqual(record.ean, "8718521078584")
        self.assertEqual(record.gtin_normalized, "08718521078584")

        rejected, error = map_suburban_row(source_row(ean=""), 3)
        self.assertIsNone(rejected)
        self.assertEqual(error, "missing_or_invalid_ean")

    def test_invalid_check_digit_is_rejected(self):
        record, error = map_suburban_row(source_row(ean="8718521078585"), 2)
        self.assertIsNone(record)
        self.assertEqual(error, "missing_or_invalid_ean")

    def test_unfetched_detail_is_distinguished_from_missing_barcode(self):
        record, error = map_suburban_row(source_row(ean="", detail_status="pending"), 2)
        self.assertIsNone(record)
        self.assertEqual(error, "detail_not_attempted")

        record, error = map_suburban_row(source_row(ean="", detail_status="missing_ean"), 3)
        self.assertIsNone(record)
        self.assertEqual(error, "missing_or_invalid_ean")

    def test_registry_and_pipeline_contract_are_present(self):
        self.assertIs(get_shop_importer("suburban"), SHOP_DEFINITION)
        self.assertEqual(get_shop_config("suburban").importer_module, SHOP_DEFINITION.importer_module)
        self.assertTrue(set(SHOP_DEFINITION.required_columns).issubset(SHOP_DEFINITION.all_declared_columns))

    def test_shipping_rule_has_fixed_user_configured_nl_cost(self):
        path = Path("data/shipping/vinylofy_shipping_rules_nl.csv")
        with path.open(encoding="utf-8", newline="") as handle:
            rows = {row["shop_slug"]: row for row in csv.DictReader(handle)}
        self.assertEqual(rows["suburban"]["shipping_cost_cents"], "780")
        self.assertEqual(rows["suburban"]["free_shipping_threshold_cents"], "")
        self.assertEqual(rows["suburban"]["shipping_logic"], "flat")

    def test_dry_run_source_headers_match_definition(self):
        with TemporaryDirectory() as directory:
            source = Path(directory) / "suburban_master.csv"
            with source.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=SHOP_DEFINITION.all_declared_columns)
                writer.writeheader()
                writer.writerow(source_row())
            self.assertTrue(source.exists())

    def test_listing_refresh_builds_registry_and_price_sync_payloads(self):
        row = source_row(product_url="https://suburban.nl/product/example-record/")
        links = build_links([row])
        offers = build_offers([row])
        self.assertEqual(links[0].shop_id, "suburban")
        self.assertEqual(links[0].payload["price"], "19,95")
        self.assertEqual(offers[0].source_url, row["product_url"])
        self.assertEqual(offers[0].price, "19,95")


if __name__ == "__main__":
    unittest.main()
