from __future__ import annotations

import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.automation.pipeline_config import get_shop_config
from scripts.importers.import_grooverecords import SHOP_DEFINITION, map_grooverecords_row
from scripts.importers.registry import get_shop_importer


def source_row(**overrides: str) -> dict[str, str]:
    row = {
        "scraped_at": "2026-08-23T12:00:00+00:00",
        "product_url": "https://grooverecords.nl/nl/product/example-record?aid=123",
        "product_key": "aid:123",
        "ean": "8718521078584",
        "artist": "Example Artist",
        "title": "Example Record",
        "format": "Vinyl",
        "price": "19,95",
        "standard_price": "29,95",
        "availability": "in_stock",
        "detail_status": "ok",
    }
    row.update(overrides)
    return row


class GrooveRecordsImporterTest(unittest.TestCase):
    def test_mapper_uses_sale_price_and_ean_gatekeeper(self):
        record, error = map_grooverecords_row(source_row(), 2)
        self.assertIsNone(error)
        self.assertIsNotNone(record)
        self.assertEqual(record.price, 19.95)
        self.assertEqual(record.ean, "8718521078584")
        self.assertEqual(record.gtin_normalized, "08718521078584")

        rejected, error = map_grooverecords_row(source_row(ean=""), 3)
        self.assertIsNone(rejected)
        self.assertEqual(error, "missing_or_invalid_ean")

    def test_invalid_gtin_check_digit_is_rejected(self):
        record, error = map_grooverecords_row(source_row(ean="8718521078585"), 2)
        self.assertIsNone(record)
        self.assertEqual(error, "missing_or_invalid_ean")

    def test_unattempted_detail_is_reported_separately(self):
        record, error = map_grooverecords_row(source_row(ean="", detail_status="pending"), 2)
        self.assertIsNone(record)
        self.assertEqual(error, "detail_not_attempted")

    def test_registry_pipeline_and_declared_headers(self):
        self.assertIs(get_shop_importer("grooverecords"), SHOP_DEFINITION)
        self.assertEqual(get_shop_config("grooverecords").importer_module, SHOP_DEFINITION.importer_module)
        self.assertTrue(set(SHOP_DEFINITION.required_columns).issubset(SHOP_DEFINITION.all_declared_columns))
        with TemporaryDirectory() as directory:
            path = Path(directory) / "master.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=SHOP_DEFINITION.all_declared_columns)
                writer.writeheader()
                writer.writerow(source_row())
            self.assertTrue(path.exists())

    def test_shipping_rule_is_flat_795_without_free_threshold(self):
        path = Path("data/shipping/vinylofy_shipping_rules_nl.csv")
        with path.open(encoding="utf-8", newline="") as handle:
            rows = {row["shop_slug"]: row for row in csv.DictReader(handle)}
        self.assertEqual(rows["grooverecords"]["shipping_cost_cents"], "795")
        self.assertEqual(rows["grooverecords"]["free_shipping_threshold_cents"], "")
        self.assertEqual(rows["grooverecords"]["shipping_logic"], "flat")


if __name__ == "__main__":
    unittest.main()
