#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Register discovered product links into Supabase")
    p.add_argument("--shop", required=True)
    p.add_argument("--file", required=True, help="Text file with one URL per line")
    return p


def main() -> int:
    args = build_parser().parse_args()
    path = Path(args.file)

    if not path.exists():
        print(f"[USF][ERROR] File not found: {path}", flush=True)
        return 1

    urls = [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]

    links = [
        DiscoveredLink(
            shop_id=args.shop,
            source_url=url,
            source_product_id=url.rstrip("/").split("/")[-1],
            payload={"source_file": str(path)},
        )
        for url in urls
    ]

    result = upsert_discovered_links(links)
    print(f"[USF] registered links: inserted={result.inserted} updated={result.updated} total={result.total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
