from __future__ import annotations

import argparse
from datetime import datetime, timezone

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.fast_listing_price_sync import bulk_update_prices_from_link_registry
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import (
    ListingOffer,
    offers_needing_price_reconcile,
    sync_listing_offers,
)
from scripts.scrapers.usf.jobs.discover_platomania import (
    SHOP_COUNTRY,
    SHOP_DOMAIN,
    SHOP_ID,
    SHOP_NAME,
    clean,
    discover_links,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Platomania listing price refresh with fast existing-price sync "
            "and bounded new-offer reconciliation."
        )
    )
    parser.add_argument("--seed-limit", type=int, default=0, help="Aantal seedcategorieën; 0 = alle seeds.")
    parser.add_argument("--max-pages-per-seed", type=int, default=0, help="Aantal pagina's per seed; 0 = tot leeg/dubbel.")
    parser.add_argument("--delay-seconds", type=float, default=0.35)
    parser.add_argument(
        "--fast-price-sync",
        action="store_true",
        help=(
            "Gebruik snelle bulk sync voor bestaande prices en reconcile "
            "daarna nieuwe links EAN-aware."
        ),
    )
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.seed_limit < 0:
        raise SystemExit("[ERROR] --seed-limit mag niet negatief zijn.")
    if args.max_pages_per_seed < 0:
        raise SystemExit("[ERROR] --max-pages-per-seed mag niet negatief zijn.")
    if args.delay_seconds < 0:
        raise SystemExit("[ERROR] --delay-seconds mag niet negatief zijn.")

    links = discover_links(
        seed_limit=args.seed_limit,
        max_pages_per_seed=args.max_pages_per_seed,
        delay_seconds=args.delay_seconds,
    )

    seen_at = datetime.now(timezone.utc)
    offers: list[ListingOffer] = []

    for link in links:
        payload = dict(link.payload or {})
        price = clean(payload.get("price") or payload.get("prijs"))
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
                ean=clean(payload.get("ean")) or None,
                seen_at=seen_at,
                raw=payload,
            )
        )

    print(
        "[LISTING-REFRESH] summary",
        {
            "shop": SHOP_ID,
            "links": len(links),
            "offers_with_price": len(offers),
            "offers_with_ean": sum(1 for offer in offers if getattr(offer, "ean", None)),
            "fast_price_sync": bool(args.fast_price_sync),
            "write": bool(args.write),
        },
        flush=True,
    )

    for offer in offers[:5]:
        print(
            "[LISTING-REFRESH] sample",
            {
                "source_url": offer.source_url,
                "ean": getattr(offer, "ean", None),
                "price": str(offer.price),
                "availability": offer.availability,
            },
            flush=True,
        )

    if not links:
        raise SystemExit("[ERROR] Platomania listing refresh leverde geen links op.")
    if args.write and not offers:
        raise SystemExit("[ERROR] Platomania listing refresh vond geen prijzen; schrijf niets weg.")

    if not args.write:
        print("[LISTING-REFRESH] dry-run complete; geen databasewrites.", flush=True)
        return 0

    result = upsert_discovered_links(links)
    print(
        "[LISTING-REFRESH] registry",
        {"inserted": result.inserted, "updated": result.updated, "total": result.total},
        flush=True,
    )

    with db_connection() as conn:
        offers_needing_reconcile = offers_needing_price_reconcile(
            conn,
            offers,
            shop_domain=SHOP_DOMAIN,
        )

        if args.fast_price_sync:
            stats = bulk_update_prices_from_link_registry(
                conn,
                shop_registry_id=SHOP_ID,
                shop_domain=SHOP_DOMAIN,
                write=True,
                currency="EUR",
            )
            print("[LISTING-REFRESH] fast_price_sync", vars(stats), flush=True)

            if offers_needing_reconcile:
                reconcile_stats = sync_listing_offers(
                    conn,
                    offers_needing_reconcile,
                    write=True,
                    create_product_from_offer_ean=True,
                )
                print(
                    "[LISTING-REFRESH] new_offer_reconcile",
                    vars(reconcile_stats),
                    flush=True,
                )
            else:
                print(
                    "[LISTING-REFRESH] new_offer_reconcile",
                    {"offers": 0, "reason": "all discovered offers already have a price"},
                    flush=True,
                )
        else:
            stats = sync_listing_offers(
                conn,
                offers,
                write=True,
                create_product_from_offer_ean=True,
            )
            print("[LISTING-REFRESH] price_sync", vars(stats), flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
