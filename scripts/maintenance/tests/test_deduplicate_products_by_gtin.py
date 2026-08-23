import unittest

from scripts.maintenance.deduplicate_products_by_gtin import Product, find_pairs


def product(product_id: str, ean: str, gtin: str | None) -> Product:
    return Product(
        product_id=product_id,
        ean=ean,
        gtin_normalized=gtin,
        artist="Artist",
        title="Release",
        format_label="LP",
        created_at="2026-01-01T00:00:00+00:00",
    )


class DeduplicateProductsByGtinTest(unittest.TestCase):
    def test_upc_and_leading_zero_ean_form_one_supported_pair(self):
        pairs, unsupported = find_pairs(
            [
                product("canonical", "602445254286", "00602445254286"),
                product("duplicate", "0602445254286", None),
            ]
        )

        self.assertEqual(unsupported, [])
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0].gtin, "00602445254286")
        self.assertEqual(pairs[0].canonical.product_id, "canonical")
        self.assertEqual(pairs[0].duplicate.product_id, "duplicate")

    def test_ambiguous_groups_are_not_auto_supported(self):
        pairs, unsupported = find_pairs(
            [
                product("canonical-a", "602445254286", "00602445254286"),
                product("canonical-b", "0602445254286", "00602445254286"),
            ]
        )

        self.assertEqual(pairs, [])
        self.assertEqual(len(unsupported), 1)

    def test_unrelated_gtins_are_ignored(self):
        pairs, unsupported = find_pairs(
            [
                product("one", "602445254286", "00602445254286"),
                product("two", "602445254293", "00602445254293"),
            ]
        )

        self.assertEqual(pairs, [])
        self.assertEqual(unsupported, [])


if __name__ == "__main__":
    unittest.main()
