#!/usr/bin/env python3
from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.promotion import PromotionConfig, preview_staged_offers
from scripts.scrapers.usf.core.promotion_writer import promote_staged_offers_atomically

CONFIG = PromotionConfig(
    shop_id="everythingjazz",
    shop_name="Everything Jazz EU",
    shop_domain="eustore.everythingjazz.com",
    shop_country="NL",
    currency="EUR",
    require_artist=True,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote Everything Jazz staged offers.")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    if not args.write:
        items = preview_staged_offers(config=CONFIG, limit=args.limit)
        print(
            "[EVERYTHINGJAZZ-PROMOTE]",
            {"queued": len(items), "write": False},
            flush=True,
        )
        for item in items:
            record = item.record
            print(
                "[EVERYTHINGJAZZ-PROMOTE-PREVIEW]",
                {
                    "staged_offer_id": item.staged_offer_id,
                    "ean": record.ean,
                    "artist": record.artist,
                    "title": record.title,
                    "format": record.format_label,
                    "price": record.price,
                    "availability": record.availability,
                    "url": record.product_url,
                },
                flush=True,
            )
        return 0

    result = promote_staged_offers_atomically(config=CONFIG, limit=args.limit)
    print(
        "[EVERYTHINGJAZZ-PROMOTE]",
        {
            "queued": result.queued,
            "processed": result.processed,
            "failed": result.failed,
            "new_products": result.new_products,
            "new_prices": result.new_prices,
            "changed_prices": result.changed_prices,
            "history_rows": result.history_rows,
            "cover_candidates": result.cover_candidates,
            "write": True,
        },
        flush=True,
    )
    for failure in result.failures:
        print(
            "[EVERYTHINGJAZZ-PROMOTE-WARN]",
            {
                "staged_offer_id": failure.staged_offer_id,
                "reason": failure.reason,
            },
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
