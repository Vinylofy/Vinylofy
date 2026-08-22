from __future__ import annotations

import unittest

from scripts.importers.import_atthemovies import SHOP_DEFINITION, map_atthemovies_row
from scripts.importers.common import strict_normalize_gtin
from scripts.importers.registry import get_shop_importer
from scripts.automation.pipeline_config import get_shop_config


def source_row(**overrides):
    row = {
        "scraped_at": "2026-08-22T12:00:00+00:00",
        "product_url": "https://atthemoviesshop.com/nl/products/example",
        "handle": "example",
        "product_type": "Vinyl",
        "ean": "0602567988724",
        "artist": "Queen",
        "title": "Bohemian Rhapsody",
        "format": "Black Vinyl",
        "price": "39.19",
        "standard_price": "48.99",
        "availability": "in_stock",
        "detail_status": "ok",
    }
    row.update(overrides)
    return row


class AtTheMoviesImporterTest(unittest.TestCase):
    def test_importer_maps_current_price_and_ean_gatekeeper(self):
        record, error = map_atthemovies_row(source_row(), 2)
        self.assertIsNone(error)
        self.assertIsNotNone(record)
        self.assertEqual(record.price, 39.19)
        self.assertEqual(record.ean, "0602567988724")
        self.assertIsNone(record.cover_url)
        self.assertFalse(record.is_secondhand)
        self.assertEqual(record.gtin_normalized, "00602567988724")

        rejected, error = map_atthemovies_row(source_row(ean=""), 3)
        self.assertIsNone(rejected)
        self.assertEqual(error, "missing_or_invalid_ean")

    def test_supported_gtin_lengths_require_valid_checkdigit(self):
        valid_identifiers = (
            "96385074",       # GTIN-8
            "036000291452",   # GTIN-12 / UPC-A
            "4006381333931",  # GTIN-13 / EAN-13
            "10012345000017", # GTIN-14
        )
        for identifier in valid_identifiers:
            with self.subTest(identifier=identifier):
                self.assertIsNotNone(strict_normalize_gtin(identifier))
                record, error = map_atthemovies_row(source_row(ean=identifier), 2)
                self.assertIsNone(error)
                self.assertIsNotNone(record)
                self.assertEqual(record.gtin_normalized, identifier.zfill(14))

    def test_invalid_checkdigit_is_rejected(self):
        self.assertIsNone(strict_normalize_gtin("0602567988725"))
        record, error = map_atthemovies_row(source_row(ean="0602567988725"), 2)
        self.assertIsNone(record)
        self.assertEqual(error, "missing_or_invalid_ean")

    def test_registry_and_declared_headers_include_atthemovies(self):
        self.assertIs(get_shop_importer("atthemovies"), SHOP_DEFINITION)
        self.assertTrue({"ean", "price", "availability"}.issubset(SHOP_DEFINITION.required_columns))
        pipeline = get_shop_config("atthemovies")
        self.assertEqual(pipeline.importer_module, "scripts.importers.import_atthemovies")
        self.assertEqual(pipeline.csv_output_path, SHOP_DEFINITION.files.csv_output_path)
        self.assertFalse(pipeline.include_in_all)


if __name__ == "__main__":
    unittest.main()
