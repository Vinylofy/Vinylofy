#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import ListingOffer, sync_listing_offers
from scripts.scrapers.usf.jobs.discover_bobsvinyl import (
    DEFAULT_DELAY_SECONDS,
    DEFAULT_MAX_PAGES,
    SHOP_ID,
    discover_links,
)


SHOP_NAME = "Bob's Vinyl"
SHOP_DOMAIN = "bobsvinyl.nl"
SHOP_COUNTRY = "NL"


def clean(value: object) -> str:
    return str(value or "").strip()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh Bob's Vinyl listing prices into prices using collection pages."
    )
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help="Aantal listingpagina's; 0 = volledige catalogus.",
    )
    parser.add_argument("--delay-seconds", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.start_page < 1:
        raise SystemExit("[ERROR] --start-page moet minimaal 1 zijn.")
    if args.max_pages < 0:
        raise SystemExit("[ERROR] --max-pages mag niet negatief zijn.")
    if args.delay_seconds < 0:
        raise SystemExit("[ERROR] --delay-seconds mag niet negatief zijn.")

    max_pages = None if args.max_pages == 0 else args.max_pages

    links = discover_links(
        start_page=args.start_page,
        max_pages=max_pages,
        delay_seconds=args.delay_seconds,
    )

    seen_at = datetime.now(timezone.utc)
    offers: list[ListingOffer] = []

    for link in links:
        payload = dict(link.payload or {})
        price = clean(payload.get("price"))

        if not price:
            continue

        offers.append(
            ListingOffer(
                shop_name=SHOP_NAME,
                shop_domain=SHOP_DOMAIN,
                shop_country=SHOP_COUNTRY,
                source_url=link.source_url,
                price=price,
                availability=clean(payload.get("availability")) or "unknown",
                currency="EUR",
                seen_at=seen_at,
                raw=payload,
            )
        )

    print(
        "[LISTING-REFRESH]",
        {
            "shop": SHOP_ID,
            "links": len(links),
            "offers_with_price": len(offers),
            "write": args.write,
        },
        flush=True,
    )

    for offer in offers[:5]:
        print(
            "[LISTING-SAMPLE]",
            {
                "source_url": offer.source_url,
                "price": str(offer.price),
                "availability": offer.availability,
            },
            flush=True,
        )

    if not links:
        raise SystemExit("[ERROR] Bob's Vinyl listing refresh leverde geen links op.")

    if args.write and not offers:
        raise SystemExit("[ERROR] Bob's Vinyl listing refresh vond geen prijzen; schrijf niets weg.")

    if not args.write:
        print("[LISTING-REFRESH] dry-run complete; geen databasewrites.", flush=True)
        return 0

    result = upsert_discovered_links(links)

    print(
        "[LISTING-REFRESH] registry",
        {
            "inserted": result.inserted,
            "updated": result.updated,
            "total": result.total,
        },
        flush=True,
    )

    with db_connection() as conn:
        stats = sync_listing_offers(conn, offers, write=True)

    print("[LISTING-REFRESH] price_sync", vars(stats), flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
