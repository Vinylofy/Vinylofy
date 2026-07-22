#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.scrapers.usf.core.staging import (
    stage_latest_raw_snapshots,
)


SHOP_ID = "colouredvinyl"
DEFAULT_LIMIT = 100


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Stage de nieuwste Coloured Vinyl "
            "raw detailsnapshots via de generieke "
            "USF-stagingcore."
        )
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=(
            "Maximumaantal nieuwste unstaged "
            "raw snapshots; standaard 100."
        ),
    )

    parser.add_argument(
        "--write",
        action="store_true",
        help=(
            "Schrijf staged_offers. Zonder "
            "--write is dit uitsluitend een dry-run."
        ),
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit(
            "[ERROR] --limit moet minimaal 1 zijn."
        )

    result = stage_latest_raw_snapshots(
        shop_id=SHOP_ID,
        limit=args.limit,
        write=args.write,
    )

    summary = {
        "shop": SHOP_ID,
        "candidates": result.candidates,
        "inserted": result.inserted,
        "skipped": result.skipped,
        "write": args.write,
    }

    items = [
        {
            "raw_scrape_id": item.raw_scrape_id,
            "staged_offer_id": (
                item.staged_offer_id
            ),
            "source_url": item.source_url,
            "ean_normalized": (
                item.ean_normalized
            ),
            "price": (
                str(item.price)
                if item.price is not None
                else None
            ),
            "availability": (
                item.availability
            ),
            "stage_reason": (
                item.stage_reason
            ),
        }
        for item in result.items
    ]

    diagnostics = {
        "summary": summary,
        "items": items,
    }

    output_dir = Path(
        "output/usf-colouredvinyl"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    diagnostics_path = (
        output_dir
        / "stage-diagnostics.json"
    )

    diagnostics_path.write_text(
        json.dumps(
            diagnostics,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(
        "[COLOUREDVINYL-STAGE-SUMMARY]",
        summary,
        flush=True,
    )

    for item in items[:10]:
        print(
            "[COLOUREDVINYL-STAGE-SAMPLE]",
            item,
            flush=True,
        )

    print(
        "[COLOUREDVINYL-STAGE-DIAGNOSTICS]",
        {
            "path": str(
                diagnostics_path
            )
        },
        flush=True,
    )

    if not args.write:
        print(
            "[COLOUREDVINYL-STAGE] "
            "dry-run compleet; "
            "geen staged_offers geschreven.",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
