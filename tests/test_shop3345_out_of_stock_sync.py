import unittest
from contextlib import contextmanager
from unittest import mock

from scripts.scrapers.usf.jobs import (
    refresh_shop3345_listing_prices as shop3345,
)


class FakeCursor:
    def __init__(self):
        self.queries = []
        self.rowcount = 0
        self.result = (0,)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        normalized = " ".join(sql.split()).lower()
        self.queries.append((normalized, params))

        if normalized.startswith("select count"):
            self.result = (3,)
        elif normalized.startswith("update public.prices"):
            self.rowcount = 2

    def fetchone(self):
        return self.result


class FakeConnection:
    def __init__(self):
        self.fake_cursor = FakeCursor()

    def cursor(self):
        return self.fake_cursor


class Shop3345OutOfStockSyncTests(unittest.TestCase):
    def test_sync_preserves_price_and_supports_dry_run(self):
        self.assertTrue(
            hasattr(shop3345, "sync_registry_out_of_stock_prices"),
            "sync_registry_out_of_stock_prices ontbreekt",
        )

        connections = []

        @contextmanager
        def fake_db_connection():
            connection = FakeConnection()
            connections.append(connection)
            yield connection

        sync_function = shop3345.sync_registry_out_of_stock_prices

        with mock.patch.object(
            shop3345,
            "db_connection",
            fake_db_connection,
        ):
            dry_result = sync_function(write=False)
            write_result = sync_function(write=True)

        self.assertEqual(
            dry_result,
            {"candidates": 3, "updated": 0},
        )
        self.assertEqual(
            write_result,
            {"candidates": 3, "updated": 2},
        )

        dry_queries = connections[0].fake_cursor.queries
        write_queries = connections[1].fake_cursor.queries

        self.assertEqual(len(dry_queries), 1)
        self.assertEqual(len(write_queries), 2)

        update_sql = write_queries[1][0]

        self.assertIn("update public.prices pr", update_sql)
        self.assertIn(
            "availability = 'out_of_stock'",
            update_sql,
        )
        self.assertRegex(
            update_sql,
            r"greatest\(\s*pr\.last_seen_at\s*,"
            r"\s*spl\.last_seen_at\s*\)",
        )
        self.assertIn("public.shop_product_links", update_sql)

        self.assertNotIn("set price =", update_sql)
        self.assertNotIn("currency =", update_sql)
        self.assertNotIn("product_url =", update_sql)
        self.assertNotIn("is_active =", update_sql)


    def test_main_runs_out_of_stock_sync_after_listing_and_delisting(self):
        import ast
        from pathlib import Path

        scraper_path = Path(
            "scripts/scrapers/usf/jobs/"
            "refresh_shop3345_listing_prices.py"
        )
        scraper_source = scraper_path.read_text(encoding="utf-8")
        tree = ast.parse(
            scraper_source,
            filename=str(scraper_path),
        )

        main_functions = [
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "main"
        ]
        self.assertEqual(len(main_functions), 1)

        main_function = main_functions[0]

        def call_name(node):
            if not isinstance(node, ast.Call):
                return None
            if isinstance(node.func, ast.Name):
                return node.func.id
            if isinstance(node.func, ast.Attribute):
                return node.func.attr
            return None

        calls = [
            node
            for node in ast.walk(main_function)
            if isinstance(node, ast.Call)
        ]

        availability_calls = [
            node
            for node in calls
            if call_name(node)
            == "sync_registry_out_of_stock_prices"
        ]
        normal_sync_calls = [
            node
            for node in calls
            if call_name(node) == "sync_offers"
        ]
        missing_delist_calls = [
            node
            for node in calls
            if call_name(node)
            == "mark_missing_links_out_of_stock"
        ]

        self.assertEqual(
            len(availability_calls),
            1,
            "main moet de gerichte availability-sync exact één keer uitvoeren",
        )
        self.assertGreaterEqual(len(normal_sync_calls), 1)
        self.assertEqual(len(missing_delist_calls), 1)

        availability_line = availability_calls[0].lineno

        self.assertGreater(
            availability_line,
            max(node.lineno for node in normal_sync_calls),
        )
        self.assertGreater(
            availability_line,
            missing_delist_calls[0].lineno,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
