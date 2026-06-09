#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Register product links from a CSV into Supabase")
    p.add_argument("--shop", required=True)
    p.add_argument("--file", required=True)
    p.add_argument("--url-column", default="product_url")
    p.add_argument("--id-column", default="handle")
    return p


def main() -> int:
    args = build_parser().parse_args()
    path = Path(args.file)

    links: list[DiscoveredLink] = []

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            url = (row.get(args.url_column) or "").strip()
            if not url:
                continue

            source_product_id = (row.get(args.id_column) or "").strip() or url.rstrip("/").split("/")[-1]

            links.append(
                DiscoveredLink(
                    shop_id=args.shop,
                    source_url=url,
                    source_product_id=source_product_id,
                    payload={
                        "source_file": str(path),
                        "source": row.get("source"),
                        "variant_id": row.get("variant_id"),
                        "sku": row.get("sku"),
                    },
                )
            )

    result = upsert_discovered_links(links)
    print(f"[USF] registered links from CSV: inserted={result.inserted} updated={result.updated} total={result.total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
