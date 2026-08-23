from __future__ import annotations

import argparse
from datetime import datetime, timezone

from scripts.scrapers.suburban import build_session, scrape_listings
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import ListingOffer, sync_listing_offers
from scripts.scrapers.usf.core.models import DiscoveredLink
from scripts.scrapers.usf.core.db import db_connection


SHOP_ID = "suburban"
SHOP_NAME = "Suburban"
SHOP_DOMAIN = "suburban.nl"
SHOP_COUNTRY = "NL"


def build_links(rows: list[dict[str, str]]) -> list[DiscoveredLink]:
    return [
        DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=row["product_url"],
            source_product_id=row.get("product_key") or None,
            payload={
                "source": "suburban_vinyl_listing",
                "artist": row.get("artist", ""),
                "title": row.get("title", ""),
                "price": row.get("price", ""),
                "standard_price": row.get("standard_price", ""),
                "currency": row.get("currency", "EUR"),
                "availability": row.get("availability", "unknown"),
                "page_found": row.get("page_found", ""),
                "scraped_at": row.get("scraped_at", ""),
            },
        )
        for row in rows
        if row.get("product_url")
    ]


def build_offers(rows: list[dict[str, str]]) -> list[ListingOffer]:
    seen_at = datetime.now(timezone.utc)
    return [
        ListingOffer(
            shop_name=SHOP_NAME,
            shop_domain=SHOP_DOMAIN,
            shop_country=SHOP_COUNTRY,
            source_url=row["product_url"],
            price=row.get("price"),
            availability=row.get("availability"),
            currency=row.get("currency") or "EUR",
            seen_at=seen_at,
            raw=dict(row),
        )
        for row in rows
        if row.get("product_url") and row.get("price")
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh Suburban prices from the vinyl collection page."
    )
    parser.add_argument("--max-pages", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=0.25)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.max_pages < 0 or args.delay_seconds < 0:
        raise SystemExit("max-pages en delay-seconds mogen niet negatief zijn")

    rows = scrape_listings(
        build_session(),
        max_pages=args.max_pages,
        delay_seconds=args.delay_seconds,
    )
    links = build_links(rows)
    offers = build_offers(rows)
    print(
        f"[SUBURBAN-LISTING] rows={len(rows)} offers={len(offers)} "
        f"write={args.write}",
        flush=True,
    )
    if not rows:
        raise SystemExit("Suburban listing refresh leverde geen producten op")
    if not args.write:
        print("[SUBURBAN-LISTING] dry-run complete; geen databasewrites.", flush=True)
        return 0

    registry = upsert_discovered_links(links)
    with db_connection() as conn:
        stats = sync_listing_offers(conn, offers, write=True)
    print(
        f"[SUBURBAN-LISTING] registry inserted={registry.inserted} "
        f"updated={registry.updated} price_sync={vars(stats)}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
