from __future__ import annotations

import argparse

from scripts.scrapers.musiconvinyl import DEFAULT_COLLECTIONS, SHOP_ID, discover_products
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover Music On Vinyl products and register them in USF shop_product_links."
    )
    parser.add_argument("--collections", default=",".join(DEFAULT_COLLECTIONS))
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--limit", type=int, default=250)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--sleep", type=float, default=0.25)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    collections = tuple(c.strip() for c in args.collections.split(",") if c.strip())
    records = discover_products(
        collections=collections,
        max_pages=args.max_pages,
        limit=args.limit,
        timeout=args.timeout,
        sleep=args.sleep,
    )
    links = [
        DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=record.source_url,
            source_product_id=record.source_product_id,
            payload=record.payload,
        )
        for record in records
    ]
    print(
        f"[DISCOVER] shop={SHOP_ID} links={len(links)} collections={collections} "
        f"max_pages={args.max_pages} write={args.write}",
        flush=True,
    )
    for link in links[:10]:
        print(
            "[DISCOVER-SAMPLE]",
            {
                "source_url": link.source_url,
                "source_product_id": link.source_product_id,
                "ean": link.payload.get("ean"),
                "price": link.payload.get("chosen_price"),
                "availability": link.payload.get("availability"),
                "price_source": link.payload.get("price_source"),
            },
            flush=True,
        )
    if not links:
        raise SystemExit("[ERROR] Music On Vinyl discovery leverde geen productlinks op.")
    if not args.write:
        print("[DISCOVER] dry-run complete; geen databasewrites.", flush=True)
        return 0
    result = upsert_discovered_links(links)
    print(
        f"[DISCOVER] registered inserted={result.inserted} updated={result.updated} total={result.total}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
