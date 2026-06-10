from __future__ import annotations

import argparse
import json
import time
from typing import Any

from scripts.scrapers.legacy.platenzaak_legacy import (
    COLLECTION_URL,
    build_session,
    discover_last_page,
    fetch_soup,
    parse_listing_page,
    product_key_from_url,
)
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "platenzaak"


def clean(value: Any) -> str:
    return str(value or "").strip()


def build_payload(row: dict[str, Any], page: int) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "discovery_source": "platenzaak_vinyl_listing",
        "page": page,
    }

    field_mapping = {
        "source_shop": "source_shop",
        "product_key": "product_key",
        "artist": "artist",
        "title": "title",
        "price": "price",
        "currency": "currency",
        "availability": "availability",
        "page_found": "page_found",
        "scraped_at": "scraped_at",
    }

    for source_field, payload_field in field_mapping.items():
        value = clean(row.get(source_field))
        if value:
            payload[payload_field] = value

    return payload


def discover_links(max_pages: int | None, delay_seconds: float) -> tuple[list[DiscoveredLink], int, int]:
    session = build_session()

    print(f"[DISCOVER] shop={SHOP_ID} start={COLLECTION_URL}", flush=True)
    first_soup = fetch_soup(session, COLLECTION_URL)
    discovered_last_page = discover_last_page(first_soup)

    if max_pages is None or max_pages == 0:
        last_page = discovered_last_page
    else:
        last_page = min(discovered_last_page, max_pages)

    links: list[DiscoveredLink] = []
    seen_urls: set[str] = set()

    for page in range(1, last_page + 1):
        page_url = COLLECTION_URL if page == 1 else f"{COLLECTION_URL}?page={page}"
        print(f"[DISCOVER] page={page}/{last_page} url={page_url}", flush=True)

        soup = first_soup if page == 1 else fetch_soup(session, page_url)
        rows = parse_listing_page(soup, page)

        print(f"[DISCOVER] page={page} rows={len(rows)}", flush=True)

        if not rows:
            print(f"[DISCOVER] page={page} no rows, stopping safely.", flush=True)
            break

        for row in rows:
            source_url = clean(row.get("product_url"))
            if not source_url or source_url in seen_urls:
                continue

            seen_urls.add(source_url)

            source_product_id = clean(row.get("product_key")) or product_key_from_url(source_url)

            links.append(
                DiscoveredLink(
                    shop_id=SHOP_ID,
                    source_url=source_url,
                    source_product_id=source_product_id,
                    payload=build_payload(row, page),
                )
            )

        if delay_seconds > 0 and page < last_page:
            time.sleep(delay_seconds)

    return links, discovered_last_page, last_page


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover Platenzaak product links and optionally register them in USF."
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Max aantal listingpagina's. Gebruik 0 voor alle gedetecteerde pagina's.",
    )
    parser.add_argument(
        "--delay-seconds",
        type=float,
        default=0.25,
        help="Pauze tussen listingpagina's.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=5,
        help="Aantal voorbeeldrecords tonen.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Schrijf discovered links naar shop_product_links. Zonder --write is dit dry-run.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    if args.max_pages < 0:
        raise SystemExit("[ERROR] --max-pages mag niet negatief zijn.")
    if args.delay_seconds < 0:
        raise SystemExit("[ERROR] --delay-seconds mag niet negatief zijn.")
    if args.sample_size < 0:
        raise SystemExit("[ERROR] --sample-size mag niet negatief zijn.")

    links, discovered_last_page, requested_last_page = discover_links(
        max_pages=args.max_pages,
        delay_seconds=args.delay_seconds,
    )

    print(
        "[DISCOVER] summary "
        + json.dumps(
            {
                "shop": SHOP_ID,
                "links": len(links),
                "discovered_last_page": discovered_last_page,
                "requested_last_page": requested_last_page,
                "write": bool(args.write),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )

    for link in links[: args.sample_size]:
        print(
            "[DISCOVER] sample "
            + json.dumps(
                {
                    "source_url": link.source_url,
                    "source_product_id": link.source_product_id,
                    "artist": link.payload.get("artist"),
                    "title": link.payload.get("title"),
                    "price": link.payload.get("price"),
                    "availability": link.payload.get("availability"),
                    "page": link.payload.get("page"),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )

    if not links:
        raise SystemExit("[ERROR] Platenzaak discovery leverde geen productlinks op.")

    if not args.write:
        print("[DISCOVER] dry-run complete; geen databasewrites.", flush=True)
        return 0

    result = upsert_discovered_links(links)
    print(
        f"[DISCOVER] registered inserted={result.inserted} "
        f"updated={result.updated} total={result.total}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
