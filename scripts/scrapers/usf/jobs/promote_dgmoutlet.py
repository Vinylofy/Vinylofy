#!/usr/bin/env python3
from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.promotion import (
    PromotionConfig,
    preview_staged_offers,
)


CONFIG = PromotionConfig(
    shop_id="dgmoutlet",
    shop_name="DGM Outlet",
    shop_domain="dgmoutlet.nl",
    shop_country="NL",
    currency="EUR",
    cover_candidate_source_type="shop_listing_image",
    cover_candidate_queue_priority=100,
    require_artist=True,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Preview DGM staged offers als CanonicalRecord. "
            "Deze job voert nog geen publieke databasewrites uit."
        )
    )
    parser.add_argument("--limit", type=int, default=5)
    return parser


def main() -> int:
    args = build_parser().parse_args()

    items = preview_staged_offers(
        config=CONFIG,
        limit=args.limit,
    )

    print(
        f"[PROMOTE-DRY-RUN] shop={CONFIG.shop_id} "
        f"queued={len(items)}",
        flush=True,
    )

    for item in items:
        record = item.record

        print(
            "[PROMOTE-PREVIEW]",
            {
                "staged_offer_id": item.staged_offer_id,
                "ean": record.ean,
                "gtin_normalized": record.gtin_normalized,
                "artist": record.artist,
                "title": record.title,
                "format_label": record.format_label,
                "price": record.price,
                "currency": record.currency,
                "availability": record.availability,
                "product_url": record.product_url,
                "product_handle": record.product_handle,
                "cover_candidate_url": (
                    record.cover_candidate_url
                ),
                "cover_candidate_source_type": (
                    record.cover_candidate_source_type
                ),
            },
            flush=True,
        )

    print(
        "[PROMOTE-DRY-RUN] complete; "
        "geen products/prices/history writes.",
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
