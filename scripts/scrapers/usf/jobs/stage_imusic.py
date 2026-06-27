#!/usr/bin/env python3
from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.staging import stage_latest_raw_snapshots

SHOP_ID = "imusic"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stage de nieuwste iMusic raw snapshots naar staged_offers."
    )
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    result = stage_latest_raw_snapshots(
        shop_id=SHOP_ID,
        limit=args.limit,
        write=args.write,
    )

    print(
        f"[STAGE] shop={SHOP_ID} "
        f"candidates={result.candidates} "
        f"inserted={result.inserted} "
        f"skipped={result.skipped} "
        f"write={args.write}",
        flush=True,
    )

    for item in result.items[:10]:
        print(
            "[STAGE-SAMPLE]",
            {
                "raw_scrape_id": item.raw_scrape_id,
                "staged_offer_id": item.staged_offer_id,
                "source_url": item.source_url,
                "ean_normalized": item.ean_normalized,
                "price": str(item.price) if item.price is not None else None,
                "availability": item.availability,
                "stage_reason": item.stage_reason,
            },
            flush=True,
        )

    if not args.write:
        print("[STAGE] dry-run complete; geen databasewrites.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
