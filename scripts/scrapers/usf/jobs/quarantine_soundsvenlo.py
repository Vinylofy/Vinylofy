#!/usr/bin/env python3
from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.quarantine import quarantine_promote_errors


SHOP_ID = "soundsvenlo"
DEFAULT_LIMIT = 100


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Quarantine alleen echte Sounds Venlo promote errors. "
            "Missing EAN/detail-enrichment blijft detailqueue, geen quarantine."
        )
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    result = quarantine_promote_errors(
        shop_id=SHOP_ID,
        limit=args.limit,
        write=args.write,
    )

    print(
        "[QUARANTINE]",
        {
            "shop": SHOP_ID,
            "candidates": result.candidates,
            "inserted": result.inserted,
            "write": args.write,
        },
        flush=True,
    )

    for item in result.items[:10]:
        print(
            "[QUARANTINE-SAMPLE]",
            {
                "staged_offer_id": item.staged_offer_id,
                "source_url": item.source_url,
                "issue_type": item.issue_type,
                "issue_detail": item.issue_detail,
                "quarantine_id": item.quarantine_id,
            },
            flush=True,
        )

    if not args.write:
        print("[QUARANTINE] dry-run complete; geen databasewrites.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
