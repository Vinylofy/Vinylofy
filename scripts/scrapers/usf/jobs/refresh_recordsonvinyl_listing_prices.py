#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import ListingOffer, sync_listing_offers
from scripts.scrapers.usf.core.models import DiscoveredLink


SHOP_ID = "recordsonvinyl"
SHOP_NAME = "Records on Vinyl"
SHOP_DOMAIN = "recordsonvinyl.nl"
SHOP_COUNTRY = "NL"
BASE_URL = "https://recordsonvinyl.nl"


def clean(value: object) -> str:
    return str(value or "").strip()


def normalize_product_url(href: str) -> str:
    return urljoin(BASE_URL, href.split("?", 1)[0]).rstrip("/")


def extract_price(text: str) -> str | None:
    patterns = [
        r"€\s*([0-9]+(?:[.,][0-9]{1,2})?)",
        r"EUR\s*([0-9]+(?:[.,][0-9]{1,2})?)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            return match.group(1).replace(",", ".")

    return None


def extract_availability(text: str) -> str:
    lower = text.lower()

    if "uitverkocht" in lower or "sold out" in lower or "out of stock" in lower:
        return "out_of_stock"

    if "pre-order" in lower or "preorder" in lower:
        return "preorder"

    return "unknown"


def likely_product_container(anchor):
    node = anchor

    for _ in range(8):
        if node is None:
            break

        classes = " ".join(node.get("class", [])).lower() if hasattr(node, "get") else ""

        if any(
            token in classes
            for token in [
                "grid__item",
                "card-wrapper",
                "product-card",
                "product",
                "card",
            ]
        ):
            return node

        node = node.parent

    return anchor.parent or anchor


def parse_listing_page(
    html: str,
    *,
    page: int,
    listing_url: str,
    seen_at: datetime,
) -> tuple[list[DiscoveredLink], list[ListingOffer]]:
    soup = BeautifulSoup(html, "html.parser")

    links_by_url: dict[str, DiscoveredLink] = {}
    offers_by_url: dict[str, ListingOffer] = {}

    anchors = soup.select('a[href*="/products/"]')

    for position, anchor in enumerate(anchors, start=1):
        href = clean(anchor.get("href"))
        if not href:
            continue

        source_url = normalize_product_url(href)

        if "/products/" not in source_url:
            continue

        handle = source_url.rstrip("/").split("/")[-1]
        container = likely_product_container(anchor)

        text = (
            clean(container.get_text(" ", strip=True))
            if container
            else clean(anchor.get_text(" ", strip=True))
        )

        price = extract_price(text)
        availability = extract_availability(text)

        payload = {
            "discovery_source": "recordsonvinyl_collection_listing",
            "discovery_url": listing_url,
            "page": page,
            "listing_position": position,
            "price": price,
            "price_source": "listing",
            "availability": availability,
            "listing_seen_at": seen_at.isoformat(),
        }

        links_by_url[source_url] = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=handle,
            payload=payload,
        )

        if price:
            offers_by_url[source_url] = ListingOffer(
                shop_name=SHOP_NAME,
                shop_domain=SHOP_DOMAIN,
                shop_country=SHOP_COUNTRY,
                source_url=source_url,
                price=price,
                availability=availability,
                currency="EUR",
                seen_at=seen_at,
                raw=payload,
            )

    return list(links_by_url.values()), list(offers_by_url.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh Records on Vinyl listing prices into prices using collection pages."
    )
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Aantal listingpagina's; 0 = doorlopen tot lege pagina.",
    )
    parser.add_argument("--sleep", type=float, default=1.5)
    parser.add_argument(
        "--max-page-failures",
        type=int,
        default=3,
        help="Stop veilig na dit aantal opeenvolgende mislukte listingpagina's.",
    )
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.start_page < 1:
        raise SystemExit("[ERROR] --start-page moet minimaal 1 zijn.")
    if args.max_pages < 0:
        raise SystemExit("[ERROR] --max-pages mag niet negatief zijn.")
    if args.sleep < 0:
        raise SystemExit("[ERROR] --sleep mag niet negatief zijn.")
    if args.max_page_failures < 1:
        raise SystemExit("[ERROR] --max-page-failures moet minimaal 1 zijn.")

    session = requests.Session()
    seen_at = datetime.now(timezone.utc)

    all_links: list[DiscoveredLink] = []
    all_offers: list[ListingOffer] = []
    seen_page_signatures: set[tuple[str, ...]] = set()

    page = args.start_page
    pages_done = 0
    consecutive_page_failures = 0
    failed_pages: list[int] = []

    while args.max_pages == 0 or pages_done < args.max_pages:
        listing_url = f"{BASE_URL}/collections/all?page={page}"

        print(f"[LISTING-REFRESH] page={page} url={listing_url}", flush=True)

        try:
            response = session.get(listing_url, timeout=30)
        except requests.RequestException as exc:
            consecutive_page_failures += 1
            failed_pages.append(page)
            print(
                "[LISTING-REFRESH][WARN] request failed; skipping page",
                {
                    "page": page,
                    "failure": str(exc),
                    "consecutive_page_failures": consecutive_page_failures,
                    "max_page_failures": args.max_page_failures,
                },
                flush=True,
            )
            if consecutive_page_failures >= args.max_page_failures:
                print(
                    "[LISTING-REFRESH][WARN] max consecutive page failures reached; stopping safely.",
                    {"failed_pages": failed_pages[-args.max_page_failures:]},
                    flush=True,
                )
                break
            pages_done += 1
            page += 1
            if args.max_pages == 0 or pages_done < args.max_pages:
                time.sleep(args.sleep)
            continue

        if response.status_code == 429:
            print("[LISTING-REFRESH][WARN] HTTP 429, stopping safely.", flush=True)
            break

        if response.status_code >= 500:
            consecutive_page_failures += 1
            failed_pages.append(page)
            print(
                "[LISTING-REFRESH][WARN] server error; skipping page",
                {
                    "page": page,
                    "status_code": response.status_code,
                    "consecutive_page_failures": consecutive_page_failures,
                    "max_page_failures": args.max_page_failures,
                },
                flush=True,
            )
            if consecutive_page_failures >= args.max_page_failures:
                print(
                    "[LISTING-REFRESH][WARN] max consecutive server errors reached; stopping safely.",
                    {"failed_pages": failed_pages[-args.max_page_failures:]},
                    flush=True,
                )
                break
            pages_done += 1
            page += 1
            if args.max_pages == 0 or pages_done < args.max_pages:
                time.sleep(args.sleep)
            continue

        response.raise_for_status()
        consecutive_page_failures = 0

        links, offers = parse_listing_page(
            response.text,
            page=page,
            listing_url=listing_url,
            seen_at=seen_at,
        )

        page_signature = tuple(link.source_url for link in links)
        if page_signature and page_signature in seen_page_signatures:
            print(
                "[LISTING-REFRESH][WARN] duplicate listing page detected; stopping safely.",
                {
                    "page": page,
                    "links": len(links),
                    "first_link": page_signature[0],
                    "last_link": page_signature[-1],
                },
                flush=True,
            )
            break
        if page_signature:
            seen_page_signatures.add(page_signature)


        print(
            "[LISTING-REFRESH-PAGE]",
            {
                "page": page,
                "links": len(links),
                "offers_with_price": len(offers),
                "write": args.write,
            },
            flush=True,
        )

        if not links:
            print(f"[LISTING-REFRESH] page={page} no links, stopping.", flush=True)
            break

        all_links.extend(links)
        all_offers.extend(offers)

        pages_done += 1
        page += 1

        if args.max_pages == 0 or pages_done < args.max_pages:
            time.sleep(args.sleep)

    print(
        "[LISTING-REFRESH]",
        {
            "shop": SHOP_ID,
            "links": len(all_links),
            "offers_with_price": len(all_offers),
            "write": args.write,
        },
        flush=True,
    )

    for offer in all_offers[:5]:
        print(
            "[LISTING-SAMPLE]",
            {
                "source_url": offer.source_url,
                "price": str(offer.price),
                "availability": offer.availability,
            },
            flush=True,
        )

    if not all_links:
        raise SystemExit("[ERROR] Records on Vinyl listing refresh leverde geen links op.")

    if args.write and not all_offers:
        raise SystemExit("[ERROR] Records on Vinyl listing refresh vond geen prijzen; schrijf niets weg.")

    if not args.write:
        print("[LISTING-REFRESH] dry-run complete; geen databasewrites.", flush=True)
        return 0

    result = upsert_discovered_links(all_links)
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
        stats = sync_listing_offers(conn, all_offers, write=True)

    print("[LISTING-REFRESH] price_sync", vars(stats), flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
