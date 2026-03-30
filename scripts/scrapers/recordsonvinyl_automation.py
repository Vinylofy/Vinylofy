#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve().parent
TARGET = CURRENT / "recordsonvinyl.py"


def ensure_parent(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def run_command(args: list[str]) -> None:
    proc = subprocess.run(args, text=True)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Automation wrapper voor RecordsOnVinyl scraper")
    p.add_argument("--mode", choices=["crawl", "refresh", "export"], default="refresh")
    p.add_argument("--master-csv", default="data/raw/recordsonvinyl/recordsonvinyl_master.csv")
    p.add_argument("--out", default="data/raw/recordsonvinyl/recordsonvinyl_products.csv")
    p.add_argument("--collection-url", default="https://recordsonvinyl.nl/collections/all")
    p.add_argument("--max-pages", type=int, default=999)
    p.add_argument("--limit-products", type=int, default=5000)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--delay-listing", type=float, default=0.15)
    p.add_argument("--delay-product", type=float, default=0.60)
    p.add_argument("--user-agent", default="StorkStylusPricingBot/1.0")
    p.add_argument("--ignore-robots", action="store_true")
    p.add_argument("--rescrape", action="store_true")
    return p


def main() -> int:
    args = build_parser().parse_args()
    ensure_parent(args.master_csv)
    ensure_parent(args.out)

    base = [
        sys.executable,
        "-u",
        str(TARGET),
        "--master-csv",
        args.master_csv,
        "--delay-listing",
        str(max(0.0, args.delay_listing)),
        "--delay-product",
        str(max(0.0, args.delay_product)),
        "--user-agent",
        args.user_agent,
    ]
    if args.ignore_robots:
        base.append("--ignore-robots")

    if args.mode == "crawl":
        cmd = base + [
            "--out",
            args.out,
            "--workers",
            str(max(1, args.workers)),
            "crawl",
            "--collection-url",
            args.collection_url,
            "--max-pages",
            str(max(0, args.max_pages)),
            "--limit-products",
            str(max(0, args.limit_products)),
        ]
        if args.rescrape:
            cmd.append("--rescrape")
        run_command(cmd)
        return 0

    if args.mode == "refresh":
        cmd = base + [
            "--out",
            args.out,
            "refresh-prices",
            "--collection-url",
            args.collection_url,
            "--max-pages",
            str(max(0, args.max_pages)),
            "--limit-products",
            str(max(0, args.limit_products)),
        ]
        run_command(cmd)
        return 0

    cmd = [
        sys.executable,
        "-u",
        str(TARGET),
        "--master-csv",
        args.master_csv,
        "export",
        "--out",
        args.out,
    ]
    run_command(cmd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
