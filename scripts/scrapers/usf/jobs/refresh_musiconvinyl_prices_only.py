from __future__ import annotations

import argparse
import subprocess
import sys

from scripts.scrapers.musiconvinyl import DEFAULT_COLLECTIONS


def run(command: list[str]) -> None:
    print("[MUSICONVINYL-PRICE-ONLY] CMD", " ".join(command), flush=True)
    subprocess.run(command, check=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Full Music On Vinyl listing/price refresh without detail requests."
    )
    parser.add_argument("--collections", default=",".join(DEFAULT_COLLECTIONS))
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--listing-limit", type=int, default=250)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    command = [
        sys.executable,
        "-m",
        "scripts.scrapers.usf.jobs.discover_musiconvinyl",
        "--collections",
        args.collections,
        "--max-pages",
        str(args.max_pages),
        "--limit",
        str(args.listing_limit),
    ]
    if args.write:
        command.append("--write")
    run(command)
    print(
        "[MUSICONVINYL-PRICE-ONLY] done: listing payload prices refreshed; no details fetched.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
