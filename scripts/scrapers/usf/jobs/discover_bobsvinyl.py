#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from typing import Any

from scripts.scrapers.bobsvinyl import (
    COLLECTION_NAME,
    collection_page_url,
    fetch_soup,
    make_session,
    parse_listing_card,
)
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink


SHOP_ID = "bobsvinyl"
DEFAULT_MAX_PAGES = 2
DEFAULT_DELAY_SECONDS = 0.20


def clean(value: object) -> str:
    return str(value or "").strip()


def build_payload(
    row: dict[str, str],
    *,
    page: int,
    listing_position: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "discovery_source": "bobsvinyl_collection_listing",
        "collection": COLLECTION_NAME,
        "page": page,
        "listing_position": listing_position,
    }

    field_mapping = {
        "url_listing": "url_listing",
        "artist": "artist",
        "title": "title",
        "drager": "format",
        "prijs": "price",
        "price_checked_at": "price_checked_at",
        "bron_collectie": "source_collection",
        "bron_listing_urls": "source_listing_url",
    }

    for source_field, payload_field in field_mapping.items():
        value = clean(row.get(source_field))
        if value:
            payload[payload_field] = value

    return payload


def discover_links(
    *,
    start_page: int,
    max_pages: int | None,
    delay_seconds: float,
) -> list[DiscoveredLink]:
    session = make_session()
    links_by_url: dict[str, DiscoveredLink] = {}

    page = start_page
    pages_processed = 0

    while max_pages is None or pages_processed < max_pages:
        listing_url = collection_page_url(page)

        try:
            soup = fetch_soup(session, listing_url)
        except Exception as exc:
            raise RuntimeError(
                f"Bob's Vinyl listingpagina kon niet worden geladen: "
                f"page={page} url={listing_url} error={exc}"
            ) from exc

        cards = soup.select("div.card-wrapper.product-card-wrapper")
        parsed_on_page = 0

        for position, card in enumerate(cards, start=1):
            row = parse_listing_card(card, listing_url)
            if row is None:
                continue

            source_url = clean(row.get("url"))
            if not source_url:
                continue

            source_product_id = clean(row.get("product_handle")) or None

            links_by_url[source_url] = DiscoveredLink(
                shop_id=SHOP_ID,
                source_url=source_url,
                source_product_id=source_product_id,
                payload=build_payload(
                    row,
                    page=page,
                    listing_position=position,
                ),
            )
            parsed_on_page += 1

        print(
            "[DISCOVER-PAGE]",
            {
                "shop": SHOP_ID,
                "page": page,
                "listing_url": listing_url,
                "cards": len(cards),
                "parsed": parsed_on_page,
                "unique_total": len(links_by_url),
            },
            flush=True,
        )

        pages_processed += 1

        if parsed_on_page == 0:
            print(
                f"[DISCOVER] stop: pagina {page} bevat geen bruikbare producten.",
                flush=True,
            )
            break

        page += 1

        if max_pages is None or pages_processed < max_pages:
            time.sleep(delay_seconds)

    return list(links_by_url.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Discover Bob's Vinyl collection listings en registreer "
            "optioneel de productlinks in shop_product_links."
        )
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="Eerste collectionpagina; standaard 1.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=(
            "Maximaal aantal listingpagina's. Standaard 2 voor een veilige "
            "pilot; gebruik 0 om door te lopen tot de eerste lege pagina."
        ),
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help="Pauze tussen listingpagina's.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help=(
            "Schrijf links naar shop_product_links. "
            "Zonder deze vlag is dit een dry-run."
        ),
    )
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

    print(
        "[DISCOVER]",
        {
            "shop": SHOP_ID,
            "links": len(links),
            "start_page": args.start_page,
            "max_pages": args.max_pages,
            "write": args.write,
        },
        flush=True,
    )

    for link in links[:5]:
        print(
            "[SAMPLE]",
            {
                "source_url": link.source_url,
                "source_product_id": link.source_product_id,
                "artist": link.payload.get("artist"),
                "title": link.payload.get("title"),
                "price": link.payload.get("price"),
                "page": link.payload.get("page"),
            },
            flush=True,
        )

    if not links:
        raise SystemExit(
            "[ERROR] Bob's Vinyl discovery leverde geen productlinks op."
        )

    if not args.write:
        print(
            "[DISCOVER] dry-run complete; geen databasewrites.",
            flush=True,
        )
        return 0

    result = upsert_discovered_links(links)

    print(
        "[DISCOVER] registered",
        {
            "inserted": result.inserted,
            "updated": result.updated,
            "total": result.total,
        },
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
