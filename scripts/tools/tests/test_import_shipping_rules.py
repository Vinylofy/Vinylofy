import csv
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.tools import import_shipping_rules as importer


SHOP_ID = "458c6eeb-f71c-4320-8651-26fee8110170"


def row(*, slug="atthemovies", source_url="https://atthemoviesshop.com/nl/pages/shipping"):
    return {
        "shop_slug": slug,
        "shop_name": "At The Movies Shop",
        "country_code": "NL",
        "currency": "EUR",
        "shipping_cost_cents": "750",
        "free_shipping_threshold_cents": "8900",
        "shipping_logic": "threshold",
        "shipping_note": "test",
        "confidence": "verified",
        "source_url": source_url,
        "source_url_2": "",
        "verified_at": "2026-08-22",
        "active": "true",
    }


class Cursor:
    def __init__(self, matches_by_domain, *, fail_on_write=False):
        self.matches_by_domain = matches_by_domain
        self.fail_on_write = fail_on_write
        self.executed = []
        self.writes = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def fetchall(self):
        domains = self.executed[-1][1][0]
        if isinstance(domains, str):
            domains = [domains]
        for domain in domains:
            matches = self.matches_by_domain.get(domain)
            if matches is not None:
                return matches
        return []

    def executemany(self, sql, payload):
        if self.fail_on_write:
            raise RuntimeError("simulated write failure")
        self.writes.append((sql, list(payload)))


class Connection:
    def __init__(self, cursor):
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        if exc_type:
            self.rollback()
        return False

    def cursor(self):
        return self._cursor

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class ShippingImporterTests(unittest.TestCase):
    def run_import(self, rows, connection):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "shipping.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
            with patch.dict("os.environ", {"DATABASE_URL": "test"}), patch.object(
                sys, "argv", ["import_shipping_rules.py", "--input", str(path)]
            ), patch.object(importer.psycopg, "connect", return_value=connection):
                return importer.main()

    def test_existing_unique_shop_writes_correct_shop_id(self):
        cursor = Cursor({"atthemoviesshop.com": [(SHOP_ID,)]})
        connection = Connection(cursor)

        self.assertEqual(self.run_import([row()], connection), 0)
        self.assertEqual(cursor.writes[0][1][0]["shop_id"], SHOP_ID)
        self.assertTrue(connection.committed)

    def test_missing_shop_fails_closed_without_shipping_write(self):
        cursor = Cursor({})
        connection = Connection(cursor)

        with self.assertRaises(importer.ShopResolutionError):
            self.run_import([row()], connection)
        self.assertEqual(cursor.writes, [])
        self.assertTrue(connection.rolled_back)

    def test_ambiguous_shop_match_fails_closed_without_shipping_write(self):
        cursor = Cursor({"atthemoviesshop.com": [("shop-a",), ("shop-b",)]})
        connection = Connection(cursor)

        with self.assertRaises(importer.ShopResolutionError):
            self.run_import([row()], connection)
        self.assertEqual(cursor.writes, [])
        self.assertTrue(connection.rolled_back)

    def test_upsert_sql_updates_shop_id_for_existing_shipping_row(self):
        cursor = Cursor({"atthemoviesshop.com": [(SHOP_ID,)]})
        connection = Connection(cursor)

        self.run_import([row()], connection)
        sql = cursor.writes[0][0]
        self.assertIn("on conflict (shop_slug, country_code)", sql.lower())
        self.assertIn("shop_id = excluded.shop_id", sql.lower())

    def test_upsert_sql_inserts_shop_id_for_new_shipping_row(self):
        cursor = Cursor({"atthemoviesshop.com": [(SHOP_ID,)]})
        connection = Connection(cursor)

        self.run_import([row()], connection)
        sql, payload = cursor.writes[0]
        self.assertIn("shop_id", sql.split("values", 1)[0].lower())
        self.assertEqual(payload[0]["shop_id"], SHOP_ID)

    def test_transaction_rolls_back_when_shipping_write_fails(self):
        cursor = Cursor({"atthemoviesshop.com": [(SHOP_ID,)]}, fail_on_write=True)
        connection = Connection(cursor)

        with self.assertRaises(RuntimeError):
            self.run_import([row()], connection)
        self.assertFalse(connection.committed)
        self.assertTrue(connection.rolled_back)


if __name__ == "__main__":
    unittest.main()
