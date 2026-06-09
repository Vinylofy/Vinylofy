#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from scripts.scrapers.usf.core.shop_registry import get_shop, list_enabled_shops


def run_module(module: str, args: list[str]) -> int:
    cmd = [sys.executable, "-m", module, *args]
    print("[USF]", " ".join(cmd), flush=True)
    return subprocess.call(cmd)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="USF runner for stable Vinylofy shops")
    p.add_argument("--shop", required=True, help="Shop key, or 'all'")
    p.add_argument("--phase", choices=["scrape", "import", "both"], default="both")
    p.add_argument("--scrape-args", default="", help="Extra scraper args as one string")
    p.add_argument("--import-args", default="", help="Extra importer args as one string")
    return p


def split_args(value: str) -> list[str]:
    return value.split() if value.strip() else []


def run_one(shop_key: str, phase: str, scrape_args: list[str], import_args: list[str]) -> int:
    try:
        shop = get_shop(shop_key)
    except KeyError as exc:
        print(f"[USF][ERROR] {exc}", flush=True)
        return 2

    if phase in {"scrape", "both"}:
        code = run_module(shop.scraper_module, scrape_args)
        if code != 0:
            print(f"[USF][ERROR] Scrape failed for {shop_key} with exit code {code}", flush=True)
            return code

    if phase in {"import", "both"}:
        code = run_module(shop.importer_module, import_args)
        if code != 0:
            print(f"[USF][ERROR] Import failed for {shop_key} with exit code {code}", flush=True)
            return code

    return 0


def main() -> int:
    args = build_parser().parse_args()
    scrape_args = split_args(args.scrape_args)
    import_args = split_args(args.import_args)

    shops = list_enabled_shops() if args.shop == "all" else [args.shop]

    for shop_key in shops:
        print(f"[USF] Running {args.phase} for {shop_key}", flush=True)
        code = run_one(shop_key, args.phase, scrape_args, import_args)
        if code != 0:
            return code

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
