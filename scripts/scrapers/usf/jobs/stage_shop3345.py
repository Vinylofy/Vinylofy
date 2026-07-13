#!/usr/bin/env python3
from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.staging import stage_latest_raw_snapshots


SHOP_ID = "shop3345"
DEFAULT_LIMIT = 25


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Stage de nieuwste 3345 raw snapshots naar staged_offers "
            "via de generieke USF-stagingcore."
        )
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Maximaal aantal nieuwste unstaged raw snapshots; standaard 25.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Schrijf staged offers. Zonder --write is dit een dry-run.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    result = stage_latest_raw_snapshots(
        shop_id=SHOP_ID,
        limit=args.limit,
        write=args.write,
    )

    print(
        "[STAGE]",
        {
            "shop": SHOP_ID,
            "candidates": result.candidates,
            "inserted": result.inserted,
            "skipped": result.skipped,
            "write": args.write,
        },
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
