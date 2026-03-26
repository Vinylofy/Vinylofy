#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve().parent
if str(CURRENT) not in sys.path:
    sys.path.insert(0, str(CURRENT))

import shop3345 as base  # noqa: E402


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Automation wrapper voor 3345 scraper")
    parser.add_argument(
        "--mode",
        choices=["links", "discovery", "refresh-known", "backfill", "both"],
        default="both",
    )
    parser.add_argument("--max-pages", type=int, default=15)
    parser.add_argument("--limit-details", type=int, default=250)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--links-file", default="data/raw/shop3345/3345_product_links.txt")
    parser.add_argument("--csv-file", default="data/raw/shop3345/3345_products.csv")
    parser.add_argument("--state-file", default="data/raw/shop3345/3345_detail_rotation_state.json")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=None,
        help="Discovery source(s), repeatable.",
    )
    return parser


def ensure_parent(path: str | Path) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)


def main() -> int:
    args = build_parser().parse_args()

    links_file = Path(args.links_file)
    csv_file = Path(args.csv_file)
    state_file = Path(args.state_file)

    ensure_parent(links_file)
    ensure_parent(csv_file)
    ensure_parent(state_file)

    session = base.build_session()

    if args.mode in {"links", "discovery", "both"}:
        new_links = base.scrape_listing_pages(
            session=session,
            links_file=links_file,
            state_file=state_file,
            source_names=args.sources or ["browse-all-music", "all", "used-records"],
            max_pages_per_source=max(1, args.max_pages),
        )
        print(f"[DISCOVERY] nieuw opgeslagen links: {len(new_links)}")
    else:
        new_links = []

    if args.mode in {"links", "discovery"}:
        print("Klaar.")
        return 0

    strategy = {
        "refresh-known": "refresh-known",
        "backfill": "backfill",
        "both": "mixed",
    }[args.mode]

    selected_links = base.select_links_for_detail_refresh(
        links_file=links_file,
        csv_file=csv_file,
        limit_details=args.limit_details,
        state_file=state_file,
        strategy=strategy,
    )

    written = base.scrape_product_details(
        session=session,
        links=selected_links,
        csv_path=csv_file,
        update_existing=True,
        workers=max(1, args.workers),
        state_file=state_file,
    )

    print(f"[DETAILS] verwerkt: {written}")
    print("Klaar.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
