from __future__ import annotations

import unittest

from scripts.tools.import_shipping_rules import domain_candidates, resolve_shop_id


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.params = None

    def execute(self, _query, params):
        self.params = params

    def fetchall(self):
        return self.rows


class ShippingImporterTest(unittest.TestCase):
    def test_domain_candidates_support_www_and_non_www_shop_domains(self):
        self.assertEqual(
            domain_candidates("www.viprecords.nl"),
            ["viprecords.nl", "www.viprecords.nl"],
        )

    def test_resolve_shop_id_accepts_www_source_url_for_non_www_shop(self):
        cursor = FakeCursor([("vip-shop-id",)])

        shop_id = resolve_shop_id(
            cursor,
            {"shop_slug": "viprecords", "source_url": "https://www.viprecords.nl/vinyl"},
        )

        self.assertEqual(shop_id, "vip-shop-id")
        self.assertEqual(cursor.params, (["viprecords.nl", "www.viprecords.nl"],))


if __name__ == "__main__":
    unittest.main()
