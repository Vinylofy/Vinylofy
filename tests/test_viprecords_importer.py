from __future__ import annotations

import csv
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from scripts.automation.pipeline_config import get_shop_config
from scripts.importers.import_viprecords import SHOP_DEFINITION, map_viprecords_row
from scripts.importers.registry import get_shop_importer


def source_row(**overrides: str) -> dict[str, str]:
    row = {
        "scraped_at": "2026-08-23T12:00:00+00:00",
        "product_url": "https://www.viprecords.nl/Webwinkel-Product-1001/example.html",
        "product_id": "1001",
        "ean": "8718521078584",
        "artist": "Example Artist",
        "title": "Example Record",
        "format": "LP",
        "price": "22,49",
        "standard_price": "44,99",
        "is_sale": "true",
        "availability": "in_stock",
        "detail_status": "ok",
    }
    row.update(overrides)
    return row


class VipRecordsImporterTest(unittest.TestCase):
    def test_mapper_uses_current_sale_price_and_gtin_gatekeeper(self):
        record, error = map_viprecords_row(source_row(), 2)
        self.assertIsNone(error)
        self.assertIsNotNone(record)
        self.assertEqual(record.price, 22.49)
        self.assertEqual(record.ean, "8718521078584")
        self.assertEqual(record.gtin_normalized, "08718521078584")
        self.assertEqual(record.availability, "in_stock")
        self.assertEqual(record.product_handle, "1001")

        rejected, error = map_viprecords_row(source_row(ean=""), 3)
        self.assertIsNone(rejected)
        self.assertEqual(error, "missing_or_invalid_ean")

    def test_invalid_gtin_and_unattempted_detail_are_rejected_safely(self):
        record, error = map_viprecords_row(source_row(ean="8718521078585"), 2)
        self.assertIsNone(record)
        self.assertEqual(error, "missing_or_invalid_ean")

        record, error = map_viprecords_row(source_row(ean="", detail_status="pending"), 2)
        self.assertIsNone(record)
        self.assertEqual(error, "detail_not_attempted")

    def test_registry_pipeline_headers_and_shipping_contract(self):
        self.assertIs(get_shop_importer("viprecords"), SHOP_DEFINITION)
        self.assertEqual(get_shop_config("viprecords").importer_module, SHOP_DEFINITION.importer_module)
        self.assertTrue(set(SHOP_DEFINITION.required_columns).issubset(SHOP_DEFINITION.all_declared_columns))
        with TemporaryDirectory() as directory:
            path = Path(directory) / "master.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=SHOP_DEFINITION.all_declared_columns)
                writer.writeheader()
                writer.writerow(source_row())
            self.assertTrue(path.exists())

        with Path("data/shipping/vinylofy_shipping_rules_nl.csv").open(encoding="utf-8", newline="") as handle:
            rows = {row["shop_slug"]: row for row in csv.DictReader(handle)}
        self.assertEqual(rows["viprecords"]["shipping_cost_cents"], "695")
        self.assertEqual(rows["viprecords"]["free_shipping_threshold_cents"], "")
        self.assertEqual(rows["viprecords"]["shipping_logic"], "flat")
        self.assertEqual(rows["viprecords"]["verified_at"], "2026-08-23")
        self.assertEqual(rows["viprecords"]["active"], "true")


if __name__ == "__main__":
    unittest.main()
