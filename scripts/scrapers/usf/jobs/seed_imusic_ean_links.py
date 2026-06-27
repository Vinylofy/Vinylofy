#!/usr/bin/env python3
from __future__ import annotations

import argparse
from typing import Any

from scripts.importers.common import normalize_ean, normalize_text
from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "imusic"
BASE_URL = "https://imusic.nl/music"


def lookup_ean(ean: Any, gtin_normalized: Any) -> str | None:
    direct = normalize_ean(ean)
    if direct:
        return direct

    gtin = normalize_ean(gtin_normalized)
    if not gtin:
        return None

    if len(gtin) == 14 and gtin.startswith("00"):
        return gtin[2:]
    if len(gtin) == 14 and gtin.startswith("0"):
        return gtin[1:]

    return gtin


def fetch_product_rows(limit: int) -> list[dict[str, Any]]:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    ean,
                    gtin_normalized,
                    artist,
                    title,
                    format_label
                from public.products
                where nullif(coalesce(ean, ''), '') is not null
                   or nullif(coalesce(gtin_normalized, ''), '') is not null
                order by updated_at desc nulls last, created_at desc nulls last
                limit %s
                """,
                (limit,),
            )
            rows = cur.fetchall()

    return [
        {
            "ean": row[0],
            "gtin_normalized": row[1],
            "artist": row[2],
            "title": row[3],
            "format_label": row[4],
        }
        for row in rows
    ]


def build_links(rows: list[dict[str, Any]]) -> list[DiscoveredLink]:
    links: list[DiscoveredLink] = []
    seen: set[str] = set()

    for row in rows:
        ean = lookup_ean(row.get("ean"), row.get("gtin_normalized"))
        if not ean or ean in seen:
            continue

        seen.add(ean)
        links.append(
            DiscoveredLink(
                shop_id=SHOP_ID,
                source_url=f"{BASE_URL}/{ean}",
                source_product_id=ean,
                payload={
                    "source": "vinylofy_products_ean_seed",
                    "ean": ean,
                    "product_ean": normalize_text(row.get("ean")) or None,
                    "gtin_normalized": normalize_text(row.get("gtin_normalized")) or None,
                    "artist": normalize_text(row.get("artist")) or None,
                    "title": normalize_text(row.get("title")) or None,
                    "format_label": normalize_text(row.get("format_label")) or None,
                },
            )
        )

    return links


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Seed iMusic shop_product_links vanuit bestaande Vinylofy EAN's."
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    rows = fetch_product_rows(args.limit)
    links = build_links(rows)

    print(
        "[SEED-IMUSIC]",
        {
            "products_read": len(rows),
            "links_built": len(links),
            "write": args.write,
        },
        flush=True,
    )

    for link in links[:10]:
        print(
            "[SEED-IMUSIC-SAMPLE]",
            {
                "source_url": link.source_url,
                "source_product_id": link.source_product_id,
                "payload": link.payload,
            },
            flush=True,
        )

    if not args.write:
        print("[SEED-IMUSIC] dry-run complete; geen databasewrites.", flush=True)
        return 0

    result = upsert_discovered_links(links)
    print("[SEED-IMUSIC] registry", vars(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
