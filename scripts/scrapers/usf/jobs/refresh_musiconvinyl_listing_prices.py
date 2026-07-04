from __future__ import annotations

import argparse
import subprocess
import sys


def add_write(command: list[str], write: bool) -> list[str]:
    if write:
        command.append("--write")
    return command


def run(label: str, command: list[str]) -> None:
    print(f"[MUSICONVINYL-REFRESH] START {label}", flush=True)
    print("[MUSICONVINYL-REFRESH] CMD", " ".join(command), flush=True)
    subprocess.run(command, check=True)
    print(f"[MUSICONVINYL-REFRESH] DONE {label}", flush=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh Music On Vinyl listing data. Listingprijs blijft leidend; "
            "detailprijzen worden hier niet gebruikt."
        )
    )
    parser.add_argument("--collections", default="all-products,new-releases,back-in-stock")
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--listing-limit", type=int, default=250)
    parser.add_argument("--materialize-limit", type=int, default=500)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    discover = [
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
    materialize = [
        sys.executable,
        "-m",
        "scripts.scrapers.usf.jobs.materialize_musiconvinyl_listing",
        "--limit",
        str(args.materialize_limit),
    ]
    run("discover_musiconvinyl", add_write(discover, args.write))
    run("materialize_musiconvinyl_listing", add_write(materialize, args.write))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
