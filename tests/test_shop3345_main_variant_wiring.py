import ast
import unittest
from pathlib import Path


SOURCE_PATH = Path(
    "scripts/scrapers/usf/jobs/refresh_shop3345_listing_prices.py"
)


class Shop3345MainVariantWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = SOURCE_PATH.read_text(encoding="utf-8")
        cls.tree = ast.parse(
            cls.source,
            filename=str(SOURCE_PATH),
        )

        cls.main_function = next(
            (
                node
                for node in cls.tree.body
                if isinstance(node, ast.FunctionDef)
                and node.name == "main"
            ),
            None,
        )

        if cls.main_function is None:
            raise AssertionError("main() ontbreekt")

    def calls_named(self, name):
        return [
            node
            for node in ast.walk(self.main_function)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == name
        ]

    def test_global_catalog_fetch_is_removed(self):
        self.assertEqual(
            self.calls_named(
                "fetch_variant_availability_index"
            ),
            [],
        )

    def test_storefront_config_is_loaded_once(self):
        session_calls = self.calls_named(
            "build_3345_session"
        )
        config_calls = self.calls_named(
            "fetch_storefront_config"
        )
        listing_calls = self.calls_named(
            "fetch_3345_listing"
        )

        self.assertEqual(len(session_calls), 1)
        self.assertEqual(len(config_calls), 1)
        self.assertEqual(len(listing_calls), 1)

        self.assertLess(
            session_calls[0].lineno,
            config_calls[0].lineno,
        )
        self.assertLess(
            config_calls[0].lineno,
            listing_calls[0].lineno,
        )

    def test_page_handles_are_extracted_before_availability_fetch(self):
        listing_call = self.calls_named(
            "fetch_3345_listing"
        )[0]
        handle_calls = self.calls_named(
            "extract_listing_handles"
        )
        availability_calls = self.calls_named(
            "fetch_variant_availability_for_handles"
        )
        parser_call = self.calls_named(
            "parse_listing_page"
        )[0]

        self.assertEqual(len(handle_calls), 1)
        self.assertEqual(len(availability_calls), 1)

        self.assertLess(
            listing_call.lineno,
            handle_calls[0].lineno,
        )
        self.assertLess(
            handle_calls[0].lineno,
            availability_calls[0].lineno,
        )
        self.assertLess(
            availability_calls[0].lineno,
            parser_call.lineno,
        )

    def test_parser_receives_page_availability_map(self):
        parser_call = self.calls_named(
            "parse_listing_page"
        )[0]

        keywords = {
            keyword.arg: keyword.value
            for keyword in parser_call.keywords
            if keyword.arg is not None
        }

        self.assertIn(
            "variant_availability_by_handle",
            keywords,
        )

        value = keywords[
            "variant_availability_by_handle"
        ]

        self.assertIsInstance(value, ast.Name)
        self.assertEqual(
            value.id,
            "variant_availability_by_handle",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
