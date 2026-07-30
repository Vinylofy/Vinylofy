#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "3345 discovery- en prijsrunner. Voorraad wordt uitsluitend door "
            "scripts.scrapers.usf.stock_shop3345 bijgewerkt."
        )
    )
    parser.add_argument("--listing-start-page", type=int, default=1)
    parser.add_argument("--listing-max-pages", type=int, default=0)
    parser.add_argument("--listing-sleep", type=float, default=0.35)
    parser.add_argument("--max-page-failures", type=int, default=3)
    parser.add_argument("--debug-listing", action="store_true")
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    command = [
        sys.executable,
        "-u",
        "-m",
        "scripts.scrapers.usf.jobs.refresh_shop3345_listing_prices",
        "--start-page",
        str(args.listing_start_page),
        "--max-pages",
        str(args.listing_max_pages),
        "--sleep",
        str(args.listing_sleep),
        "--max-page-failures",
        str(args.max_page_failures),
    ]
    if args.debug_listing:
        command.append("--debug")
    if args.write:
        command.append("--write")

    print("[3345-PIPELINE]", {"command": command}, flush=True)
    subprocess.run(command, check=True)
    print(
        "[3345-PIPELINE] discovery en prijzen gereed; "
        "voorraadpad niet aangeroepen.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
