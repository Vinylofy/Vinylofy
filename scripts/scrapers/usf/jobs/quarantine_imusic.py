#!/usr/bin/env python3
from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.quarantine import quarantine_promote_errors

SHOP_ID = "imusic"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Plaats iMusic promote_errors idempotent in quarantine_offers."
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    result = quarantine_promote_errors(
        shop_id=SHOP_ID,
        limit=args.limit,
        write=args.write,
    )

    print(
        f"[QUARANTINE] shop={SHOP_ID} "
        f"candidates={result.candidates} "
        f"inserted={result.inserted} "
        f"write={args.write}",
        flush=True,
    )

    for item in result.items:
        print(
            "[QUARANTINE-ITEM]",
            {
                "staged_offer_id": item.staged_offer_id,
                "quarantine_id": item.quarantine_id,
                "source_url": item.source_url,
                "issue_type": item.issue_type,
                "issue_detail": item.issue_detail,
            },
            flush=True,
        )

    if not args.write:
        print("[QUARANTINE] dry-run complete; geen databasewrites.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
