#!/usr/bin/env python3
from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.promotion import (
    PromotionConfig,
    preview_staged_offers,
)
from scripts.scrapers.usf.core.promotion_writer import (
    promote_staged_offers_atomically,
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
            "Promote DGM staged offers via de centrale "
            "common.py product- en prijslogica."
        )
    )
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument(
        "--write",
        action="store_true",
        help=(
            "Voer echte products/prices/history-writes uit. "
            "Zonder deze vlag is dit een dry-run."
        ),
    )
    return parser


def run_preview(limit: int) -> int:
    items = preview_staged_offers(
        config=CONFIG,
        limit=limit,
    )

    print(
        f"[PROMOTE] shop={CONFIG.shop_id} "
        f"queued={len(items)} write=False",
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
                "cover_candidate_url": (
                    record.cover_candidate_url
                ),
            },
            flush=True,
        )

    print(
        "[PROMOTE] dry-run complete; geen databasewrites.",
        flush=True,
    )
    return 0


def run_write(limit: int) -> int:
    result = promote_staged_offers_atomically(
        config=CONFIG,
        limit=limit,
    )

    print(
        "[PROMOTE] done",
        {
            "shop": CONFIG.shop_id,
            "queued": result.queued,
            "processed": result.processed,
            "new_products": result.new_products,
            "new_prices": result.new_prices,
            "changed_prices": result.changed_prices,
            "history_rows": result.history_rows,
            "cover_candidates": result.cover_candidates,
        },
        flush=True,
    )

    for item in result.items:
        print(
            "[PROMOTED]",
            {
                "staged_offer_id": item.staged_offer_id,
                "product_id": item.product_id,
                "ean": item.ean,
                "price": item.price,
                "product_inserted": item.product_inserted,
                "price_inserted": item.price_inserted,
                "price_changed": item.price_changed,
                "history_inserted": item.history_inserted,
                "cover_candidate_inserted": (
                    item.cover_candidate_inserted
                ),
            },
            flush=True,
        )

    return 0


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    if args.write:
        return run_write(args.limit)

    return run_preview(args.limit)


if __name__ == "__main__":
    raise SystemExit(main())
