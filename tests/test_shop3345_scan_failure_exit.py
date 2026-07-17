import unittest
from types import SimpleNamespace
from unittest.mock import patch

import requests

from scripts.scrapers.usf.jobs import (
    refresh_shop3345_listing_prices as shop3345,
)


class FakeArgumentParser:
    def __init__(self, args):
        self.args = args

    def parse_args(self):
        return self.args


class FakeResponse:
    text = "<html>listing</html>"


def make_args(*, max_pages, write):
    return SimpleNamespace(
        start_page=1,
        max_pages=max_pages,
        max_page_failures=1,
        sleep=0,
        debug=False,
        write=write,
    )


def make_listing_objects():
    source_url = (
        "https://3345.nl/products/"
        "test-album"
    )

    link = SimpleNamespace(
        source_url=source_url,
        payload={
            "availability": "in_stock",
            "listing_cta_add_to_cart": True,
            "is_secondhand": False,
            "price": "29.99",
        },
    )

    offer = SimpleNamespace(
        source_url=source_url,
        price="29.99",
        raw={
            "listing_price_transport": "html",
            "artist": "Test Artist",
            "title": "Test Album",
            "html_listing_price": "29.99",
        },
    )

    return link, offer


class Shop3345ScanFailureExitTests(
    unittest.TestCase
):
    def run_main(
        self,
        *,
        max_pages,
        write,
        fail_after_first_page,
    ):
        args = make_args(
            max_pages=max_pages,
            write=write,
        )
        link, offer = make_listing_objects()

        if fail_after_first_page:
            listing_side_effect = [
                FakeResponse(),
                requests.HTTPError(
                    "429 listing failure"
                ),
            ]
        else:
            listing_side_effect = [
                FakeResponse(),
            ]

        registry_result = SimpleNamespace(
            inserted=0,
            updated=1,
            total=1,
        )

        with (
            patch.object(
                shop3345,
                "build_parser",
                return_value=FakeArgumentParser(args),
            ),
            patch.object(
                shop3345,
                "build_3345_session",
                return_value=object(),
            ),
            patch.object(
                shop3345,
                "fetch_storefront_config",
                return_value=("2024-04", "token"),
            ),
            patch.object(
                shop3345,
                "fetch_3345_listing",
                side_effect=listing_side_effect,
            ),
            patch.object(
                shop3345,
                "extract_listing_handles",
                return_value=["test-album"],
            ),
            patch.object(
                shop3345,
                "fetch_variant_availability_for_handles",
                return_value={
                    "test-album": "in_stock",
                },
            ),
            patch.object(
                shop3345,
                "parse_listing_page",
                return_value=([link], [offer]),
            ),
            patch.object(
                shop3345.time,
                "sleep",
            ),
            patch.object(
                shop3345,
                "upsert_discovered_links",
                return_value=registry_result,
            ) as upsert_mock,
            patch.object(
                shop3345,
                "sync_offers",
            ) as sync_offers_mock,
            patch.object(
                shop3345,
                "sync_registry_out_of_stock_prices",
                return_value={
                    "candidates": 0,
                    "updated": 0,
                },
            ) as out_of_stock_mock,
        ):
            result = shop3345.main()

        return (
            result,
            upsert_mock,
            sync_offers_mock,
            out_of_stock_mock,
        )

    def test_partial_full_scan_returns_nonzero_in_dry_run(
        self,
    ):
        result, _, _, _ = self.run_main(
            max_pages=0,
            write=False,
            fail_after_first_page=True,
        )

        self.assertEqual(result, 1)

    def test_partial_full_scan_blocks_all_writes(
        self,
    ):
        (
            result,
            upsert_mock,
            sync_offers_mock,
            out_of_stock_mock,
        ) = self.run_main(
            max_pages=0,
            write=True,
            fail_after_first_page=True,
        )

        self.assertEqual(result, 1)
        upsert_mock.assert_not_called()
        sync_offers_mock.assert_not_called()
        out_of_stock_mock.assert_not_called()

    def test_configured_page_cap_remains_successful_dry_run(
        self,
    ):
        result, _, _, _ = self.run_main(
            max_pages=1,
            write=False,
            fail_after_first_page=False,
        )

        self.assertEqual(result, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
