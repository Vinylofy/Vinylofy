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


def main(*_args, **_kwargs) -> int:
    raise SystemExit(
        "[DISABLED] scripts.scrapers.shop3345_automation is een oud 3345-updatepad. "
        "Gebruik uitsluitend de aparte 3345-prijs- en voorraadworkflows."
    )


if __name__ == "__main__":
    raise SystemExit(main())
