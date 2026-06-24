from __future__ import annotations

import argparse
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.fast_listing_price_sync import bulk_update_prices_from_link_registry
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import ListingOffer, sync_listing_offers
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "soundshaarlem"
SHOP_NAME = "Sounds Haarlem"
SHOP_DOMAIN = "soundshaarlem.nl"
SHOP_COUNTRY = "NL"
BASE_URL = "https://soundshaarlem.nl"

SEED_URLS = [
    "https://soundshaarlem.nl/collections/new-arrivals",
    "https://soundshaarlem.nl/collections/pre-orders",
    "https://soundshaarlem.nl/collections/sale",
    "https://soundshaarlem.nl/collections/record-store-day",
    "https://soundshaarlem.nl/collections/sounds-favourites",
    "https://soundshaarlem.nl/collections/sounds-haarlem-likes-vinyl",
    "https://soundshaarlem.nl/collections/all",
]

VINYL_RE = re.compile(
    r"\b(?:LP|2xLP|3xLP|4xLP|VINYL|COLOURED VINYL|COLORED VINYL|7-INCH|10-INCH|12-INCH|7\"|10\"|12\")\b",
    flags=re.I,
)
NON_VINYL_RE = re.compile(r"\b(?:CD|DVD|BLU[\s-]?RAY|CASSETTE|TAPE)\b", flags=re.I)
PRICE_RE = re.compile(r"€\s*([0-9]+(?:[.,][0-9]{1,2})?)")


def clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }
    )
    return session


def with_page(url: str, page: int) -> str:
    if page <= 1:
        return url

    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["page"] = str(page)
    return urlunparse(parsed._replace(query=urlencode(query)))


def normalize_product_url(href: str) -> str:
    url = urljoin(BASE_URL, href)
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    return urlunparse(("https", "soundshaarlem.nl", path, "", "", ""))


def source_product_id_from_url(url: str) -> str | None:
    path = urlparse(url).path.rstrip("/")
    if "/products/" not in path:
        return None
    return path.rsplit("/products/", 1)[-1] or None


def extract_prices(text: str) -> tuple[str | None, str | None]:
    text = clean(text)

    sale_matches = re.findall(r"Sale price\s*€\s*([0-9]+(?:[.,][0-9]{1,2})?)", text, flags=re.I)
    regular_matches = re.findall(r"Regular price\s*€\s*([0-9]+(?:[.,][0-9]{1,2})?)", text, flags=re.I)

    if sale_matches:
        current_price = sale_matches[-1].replace(",", ".")
        old_price = regular_matches[0].replace(",", ".") if regular_matches else None
        if old_price == current_price:
            old_price = None
        return current_price, old_price

    all_prices = [price.replace(",", ".") for price in PRICE_RE.findall(text)]

    if not all_prices:
        return None, None

    # Sounds Haarlem sale cards can render as:
    # old strikethrough price first, current sale price second.
    if len(all_prices) >= 2:
        current_price = all_prices[-1]
        old_price = all_prices[0]
        if old_price == current_price:
            old_price = None
        return current_price, old_price

    return all_prices[0], None


def extract_price(text: str) -> str | None:
    current_price, _old_price = extract_prices(text)
    return current_price


def extract_availability(text: str) -> str:
    lower = clean(text).lower()
    if "in-stock" in lower or "in stock" in lower:
        return "in_stock"
    if "pre-order" in lower or "pre order" in lower or "preorder" in lower:
        return "preorder"
    if "back-order" in lower or "back order" in lower or "backorder" in lower:
        return "preorder"
    if "sold out" in lower or "uitverkocht" in lower:
        return "out_of_stock"
    return "unknown"


def extract_format(text: str) -> str | None:
    text = clean(text)
    paren_matches = re.findall(r"\(([^()]{1,80})\)", text)
    for match in paren_matches:
        if VINYL_RE.search(match):
            return clean(match)

    match = VINYL_RE.search(text)
    if match:
        return clean(match.group(0))

    return None


def is_vinyl_product(title: str, text: str) -> bool:
    haystack = f"{title} {text}"
    if VINYL_RE.search(haystack):
        return True
    if NON_VINYL_RE.search(haystack):
        return False
    return False


def likely_product_container(anchor) -> Any:
    node = anchor
    best = anchor.parent

    for _ in range(10):
        if node is None:
            break

        if hasattr(node, "get_text"):
            text = clean(node.get_text(" ", strip=True))
            if "€" in text and len(text) < 3000:
                best = node

        node = getattr(node, "parent", None)

    return best


def parse_collection_page(
    html: str,
    *,
    seed_url: str,
    listing_url: str,
    page: int,
    seen_at: datetime,
) -> tuple[list[DiscoveredLink], list[ListingOffer]]:
    soup = BeautifulSoup(html, "html.parser")

    links_by_url: dict[str, DiscoveredLink] = {}
    offers_by_url: dict[str, ListingOffer] = {}

    for position, anchor in enumerate(soup.select('a[href*="/products/"]'), start=1):
        href = clean(anchor.get("href"))
        if not href:
            continue

        source_url = normalize_product_url(href)
        source_product_id = source_product_id_from_url(source_url)
        if not source_product_id:
            continue

        title = clean(anchor.get_text(" ", strip=True))
        if not title or title.lower().startswith("image:"):
            # Product title is often duplicated; walk the card text when anchor text is an image alt.
            title = ""

        container = likely_product_container(anchor)
        card_text = clean(container.get_text(" ", strip=True)) if container else clean(anchor.get_text(" ", strip=True))

        if not title:
            candidates = []
            for sub_anchor in container.select('a[href*="/products/"]') if container else []:
                candidate = clean(sub_anchor.get_text(" ", strip=True))
                if candidate and not candidate.lower().startswith("image:") and len(candidate) > 5:
                    candidates.append(candidate)
            title = candidates[-1] if candidates else source_product_id.replace("-", " ")

        if not is_vinyl_product(title, card_text):
            continue

        price, price_old = extract_prices(card_text)
        if not price:
            continue

        availability = extract_availability(card_text)
        format_label = extract_format(f"{title} {card_text}")
        is_sale = bool(price_old and price_old != price)

        payload = {
            "discovery_source": "soundshaarlem_shopify_collection",
            "seed_url": seed_url,
            "listing_url": listing_url,
            "page": page,
            "listing_position": position,
            "title": title,
            "title_raw": title,
            "format": format_label,
            "format_label_raw": format_label,
            "price": price,
            "prijs": price,
            "price_current": price,
            "price_old": price_old,
            "is_sale": is_sale,
            "price_source": "listing_sale" if is_sale else "listing",
            "availability": availability,
            "availability_text": availability,
            "listing_seen_at": seen_at.isoformat(),
            "listing_text": card_text[:1000],
        }

        links_by_url[source_url] = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=source_product_id,
            payload=payload,
        )

        offers_by_url[source_url] = ListingOffer(
            shop_name=SHOP_NAME,
            shop_domain=SHOP_DOMAIN,
            shop_country=SHOP_COUNTRY,
            source_url=source_url,
            price=price,
            availability=availability,
            currency="EUR",
            ean=None,
            seen_at=seen_at,
            raw=payload,
        )

    return list(links_by_url.values()), list(offers_by_url.values())


def discover_listing_rows(
    *,
    seed_limit: int,
    max_pages_per_seed: int,
    delay_seconds: float,
) -> tuple[list[DiscoveredLink], list[ListingOffer]]:
    if seed_limit < 0:
        raise SystemExit("[ERROR] --seed-limit mag niet negatief zijn.")
    if max_pages_per_seed < 0:
        raise SystemExit("[ERROR] --max-pages-per-seed mag niet negatief zijn.")
    if delay_seconds < 0:
        raise SystemExit("[ERROR] --delay-seconds mag niet negatief zijn.")

    session = make_session()
    seen_at = datetime.now(timezone.utc)
    seeds = SEED_URLS[:seed_limit] if seed_limit > 0 else SEED_URLS

    all_links_by_url: dict[str, DiscoveredLink] = {}
    all_offers_by_url: dict[str, ListingOffer] = {}

    print(
        "[LISTING-REFRESH] shopify_http_start",
        {
            "shop": SHOP_ID,
            "seeds": len(seeds),
            "seed_limit": seed_limit,
            "max_pages_per_seed": max_pages_per_seed,
            "delay_seconds": delay_seconds,
        },
        flush=True,
    )

    for seed_index, seed_url in enumerate(seeds, start=1):
        seen_page_signatures: set[tuple[str, ...]] = set()
        page = 1

        while max_pages_per_seed == 0 or page <= max_pages_per_seed:
            listing_url = with_page(seed_url, page)
            print(
                "[LISTING-REFRESH] page",
                {
                    "seed_index": seed_index,
                    "seed_url": seed_url,
                    "page": page,
                    "url": listing_url,
                },
                flush=True,
            )

            response = session.get(listing_url, timeout=30)

            if response.status_code == 404:
                print("[LISTING-REFRESH] stop_404", {"url": listing_url}, flush=True)
                break

            if response.status_code == 429:
                print("[LISTING-REFRESH][WARN] HTTP 429, stopping safely.", {"url": listing_url}, flush=True)
                break

            response.raise_for_status()

            links, offers = parse_collection_page(
                response.text,
                seed_url=seed_url,
                listing_url=listing_url,
                page=page,
                seen_at=seen_at,
            )

            page_signature = tuple(link.source_url for link in links)

            if not links:
                print("[LISTING-REFRESH] stop_empty", {"seed_url": seed_url, "page": page}, flush=True)
                break

            if page_signature and page_signature in seen_page_signatures:
                print(
                    "[LISTING-REFRESH] stop_duplicate_page",
                    {
                        "seed_url": seed_url,
                        "page": page,
                        "links": len(page_signature),
                        "first_link": page_signature[0],
                        "last_link": page_signature[-1],
                    },
                    flush=True,
                )
                break

            if page_signature:
                seen_page_signatures.add(page_signature)

            new_links = 0
            for link in links:
                if link.source_url not in all_links_by_url:
                    new_links += 1
                all_links_by_url[link.source_url] = link

            for offer in offers:
                all_offers_by_url[offer.source_url] = offer

            print(
                "[LISTING-REFRESH-PAGE]",
                {
                    "seed_url": seed_url,
                    "page": page,
                    "links": len(links),
                    "offers_with_price": len(offers),
                    "new_links": new_links,
                    "catalog_links_total": len(all_links_by_url),
                },
                flush=True,
            )

            page += 1

            if delay_seconds > 0:
                time.sleep(delay_seconds)

    return list(all_links_by_url.values()), list(all_offers_by_url.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh Sounds Haarlem Shopify listing prices into USF/public prices.")
    parser.add_argument("--seed-limit", type=int, default=0, help="Aantal collectie-seeds; 0 = alle seeds.")
    parser.add_argument("--max-pages-per-seed", type=int, default=1, help="Aantal pagina's per collectie; 0 = tot leeg/dubbel.")
    parser.add_argument("--delay-seconds", type=float, default=0.35)
    parser.add_argument("--fast-price-sync", action="store_true", help="Gebruik snelle price-only bulk sync vanuit shop_product_links, zoals ROV.")
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    links, offers = discover_listing_rows(
        seed_limit=args.seed_limit,
        max_pages_per_seed=args.max_pages_per_seed,
        delay_seconds=args.delay_seconds,
    )

    print(
        "[LISTING-REFRESH] summary",
        {
            "shop": SHOP_ID,
            "links": len(links),
            "offers_with_price": len(offers),
            "fast_price_sync": bool(args.fast_price_sync),
            "write": bool(args.write),
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
        raise SystemExit("[ERROR] Sounds Haarlem listing refresh leverde geen links op.")
    if args.write and not offers:
        raise SystemExit("[ERROR] Sounds Haarlem listing refresh vond geen prijzen; schrijf niets weg.")

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
        if args.fast_price_sync:
            stats = bulk_update_prices_from_link_registry(
                conn,
                shop_registry_id=SHOP_ID,
                shop_domain=SHOP_DOMAIN,
                write=True,
                currency="EUR",
            )
            print("[LISTING-REFRESH] fast_price_sync", vars(stats), flush=True)
        else:
            stats = sync_listing_offers(conn, offers, write=True)
            print("[LISTING-REFRESH] price_sync", vars(stats), flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
