from __future__ import annotations

import csv
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.importers.import_atthemovies import (
    AMBIGUOUS_EXISTING_PRODUCT_EANS,
    CONFIG,
    SHOP_DEFINITION,
    map_atthemovies_row,
)
from scripts.importers.common import read_and_filter, run_import, strict_normalize_gtin
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

    def test_known_ambiguous_identity_eans_are_rejected_before_record_creation(self):
        for ean in AMBIGUOUS_EXISTING_PRODUCT_EANS:
            with self.subTest(ean=ean):
                record, error = map_atthemovies_row(source_row(ean=ean), 2)
                self.assertIsNone(record)
                self.assertEqual(error, "ambiguous_existing_product_identity")

        accepted, rejects = self._read_rows(
            [source_row(ean=ean) for ean in AMBIGUOUS_EXISTING_PRODUCT_EANS]
            + [source_row(ean="0602567988724")]
        )
        self.assertEqual([record.ean for record in accepted], ["0602567988724"])
        self.assertEqual(
            [reject["reason"] for reject in rejects],
            ["ambiguous_existing_product_identity"] * len(AMBIGUOUS_EXISTING_PRODUCT_EANS),
        )

    def test_ambiguous_identity_rows_never_reach_product_write_path(self):
        rows = [source_row(ean=ean) for ean in AMBIGUOUS_EXISTING_PRODUCT_EANS]
        rows.append(source_row(ean="0602567988724"))

        with tempfile.TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            source_path = directory / "source.csv"
            self._write_rows(source_path, rows)

            connection = _FakeConnection()
            with (
                patch.dict(os.environ, {"DATABASE_URL": "postgresql://test"}, clear=False),
                patch("scripts.importers.common.load_env"),
                patch("scripts.importers.common.psycopg.connect", return_value=connection),
                patch("scripts.importers.common.ensure_shop", return_value="shop-id"),
                patch("scripts.importers.common.upsert_product", return_value=("product-id", False)) as upsert_product,
                patch("scripts.importers.common.maybe_upsert_cover_candidate", return_value=False),
                patch("scripts.importers.common.upsert_price", return_value=(True, False)),
                patch("scripts.importers.common.maybe_insert_history", return_value=False),
            ):
                run_import(
                    CONFIG,
                    str(source_path),
                    map_atthemovies_row,
                    rejects_path=str(directory / "rejects.csv"),
                    summary_path=str(directory / "summary.json"),
                )

            self.assertEqual([call.args[1].ean for call in upsert_product.call_args_list], ["0602567988724"])
            self.assertEqual(connection.commit_count, 1)

    def _read_rows(self, rows: list[dict]):
        with tempfile.TemporaryDirectory() as temporary_directory:
            source_path = Path(temporary_directory) / "source.csv"
            self._write_rows(source_path, rows)
            return read_and_filter(source_path, map_atthemovies_row)

    @staticmethod
    def _write_rows(path: Path, rows: list[dict]) -> None:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def test_registry_and_declared_headers_include_atthemovies(self):
        self.assertIs(get_shop_importer("atthemovies"), SHOP_DEFINITION)
        self.assertTrue({"ean", "price", "availability"}.issubset(SHOP_DEFINITION.required_columns))
        pipeline = get_shop_config("atthemovies")
        self.assertEqual(pipeline.importer_module, "scripts.importers.import_atthemovies")
        self.assertEqual(pipeline.csv_output_path, SHOP_DEFINITION.files.csv_output_path)
        self.assertFalse(pipeline.include_in_all)


if __name__ == "__main__":
    unittest.main()


class _FakeCursor:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _FakeConnection:
    def __init__(self):
        self.commit_count = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, *_args, **_kwargs):
        return None

    def cursor(self):
        return _FakeCursor()

    def commit(self):
        self.commit_count += 1
