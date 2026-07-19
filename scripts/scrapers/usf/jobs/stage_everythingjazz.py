#!/usr/bin/env python3
from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.staging import stage_latest_raw_snapshots

SHOP_ID = "everythingjazz"


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage Everything Jazz raw snapshots.")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    result = stage_latest_raw_snapshots(
        shop_id=SHOP_ID,
        limit=args.limit,
        write=args.write,
    )
    print(
        "[EVERYTHINGJAZZ-STAGE]",
        {
            "candidates": result.candidates,
            "inserted": result.inserted,
            "skipped": result.skipped,
            "write": args.write,
        },
        flush=True,
    )
    for item in result.items:
        print(
            "[EVERYTHINGJAZZ-STAGE-ITEM]",
            {
                "raw_scrape_id": item.raw_scrape_id,
                "staged_offer_id": item.staged_offer_id,
                "url": item.source_url,
                "ean": item.ean_normalized,
                "price": str(item.price) if item.price is not None else None,
                "availability": item.availability,
                "reason": item.stage_reason,
            },
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
