from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts.follow_the_groove import coverage_priorities as subject


class FakeConnection:
    def __init__(self, rows=()):
        self.rows = rows
        self.commands = []
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.commands.append((sql, params))
        return self

    def fetchall(self):
        return self.rows

    def rollback(self):
        self.rollbacks += 1


class CoveragePrioritiesTest(unittest.TestCase):
    def test_selection_is_bounded_and_does_not_treat_proxy_as_rendered_count(self):
        conn = FakeConnection([("artist-mbid", "Artist", 20, 2, 1, 8, 5)])
        rows = subject.select_priorities(conn, min_products=10, limit=5)
        self.assertEqual(rows, [{
            "artist_mbid": "artist-mbid", "name": "Artist", "product_links": 20,
            "proven_neighbors": 2, "product_group_neighbors": 1,
            "related_neighbors": 8, "prior_lastfm_limit": 5, "proxy_deficit": 4,
        }])
        sql, params = conn.commands[0]
        self.assertIn("ev.classification = 'allowed'", sql)
        self.assertIn("resolution_status = 'resolved'", sql)
        self.assertIn("s.status = 'proven_output'", sql)
        self.assertIn("target.entity_type = 'group'", sql)
        self.assertIn("target_products.product_count > 0", sql)
        self.assertIn("coalesce(p.lastfm_limit, 0) < %(max_lastfm_limit)s", sql)
        self.assertEqual(params, {"min_products": 10, "max_lastfm_limit": 25, "limit": 5})

    def test_run_uses_read_only_transaction_and_reports_zero_writes(self):
        conn = FakeConnection()
        with patch.object(subject.psycopg, "connect", return_value=conn):
            report = subject.run(min_products=10, limit=5, database_url="postgres://fixture")
        self.assertEqual(conn.commands[0][0], "begin isolation level repeatable read read only")
        self.assertEqual(conn.rollbacks, 1)
        self.assertEqual(report["database_writes"], 0)
        self.assertTrue(report["heuristic_only"])

    def test_rejects_unbounded_selection_before_connecting(self):
        for min_products, limit in ((0, 5), (10, 0), (10, 26)):
            with self.subTest(min_products=min_products, limit=limit):
                with patch.object(subject.psycopg, "connect") as connect:
                    with self.assertRaises(ValueError):
                        subject.run(min_products=min_products, limit=limit, database_url="postgres://fixture")
                    connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
