from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from scripts.scrapers.legacy.platomania_legacy import SEED_URLS
from scripts.scrapers.usf.core.listing_price_sync import (
    ListingOffer,
    ensure_product_from_offer_ean,
    offers_needing_price_reconcile,
)


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.query = None
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query, params):
        self.query = query
        self.params = params

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, rows):
        self.cursor_instance = FakeCursor(rows)

    def cursor(self):
        return self.cursor_instance


def offer(*, url: str, ean: str = "0093624827771") -> ListingOffer:
    return ListingOffer(
        shop_name="Platomania",
        shop_domain="platomania.nl",
        shop_country="NL",
        source_url=url,
        price="19,99",
        availability="in_stock",
        ean=ean,
        raw={
            "artist": "SOMBR",
            "title": "I BARELY KNOW HER -BLACK VINYL-",
            "drager": "LP (1)",
            "item_nr": "4809098",
        },
    )


def run_tests() -> None:
    assert "https://www.platomania.nl/vinyl" in SEED_URLS
    assert "https://www.platomania.nl/vinyl-aanbiedingen" in SEED_URLS

    existing_url = "https://www.platomania.nl/article/1/existing"
    new_url = "https://www.platomania.nl/article/2/new"
    connection = FakeConnection([(existing_url,)])
    selected = offers_needing_price_reconcile(
        connection,
        [offer(url=existing_url), offer(url=new_url)],
        shop_domain="platomania.nl",
    )
    assert [item.source_url for item in selected] == [new_url]

    with patch(
        "scripts.scrapers.usf.core.listing_price_sync.upsert_product",
        return_value=("product-id", True),
    ) as upsert:
        created = ensure_product_from_offer_ean(
            FakeCursor([]),
            offer=offer(url=new_url),
            price=Decimal("19.99"),
            availability="in_stock",
        )

    assert created == ("product-id", True)
    record = upsert.call_args.args[1]
    assert record.ean == "0093624827771"
    assert record.artist == "SOMBR"
    assert record.title == "I BARELY KNOW HER -BLACK VINYL-"
    assert record.product_url == new_url
    assert record.cover_url is None

    assert (
        ensure_product_from_offer_ean(
            FakeCursor([]),
            offer=ListingOffer(
                shop_name="Platomania",
                shop_domain="platomania.nl",
                shop_country="NL",
                source_url=new_url,
                price="19,99",
                ean=None,
                raw={"artist": "SOMBR", "title": "I BARELY KNOW HER"},
            ),
            price=Decimal("19.99"),
            availability="in_stock",
        )
        is None
    )

    print("[TEST-OK] Platomania seeds and new-offer reconciliation")


if __name__ == "__main__":
    run_tests()
