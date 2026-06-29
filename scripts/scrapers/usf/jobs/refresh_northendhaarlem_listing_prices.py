from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Iterable
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag

from scripts.scrapers.usf.core.db import db_connection, load_env
from scripts.scrapers.usf.core.fast_listing_price_sync import bulk_update_prices_from_link_registry
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import ListingOffer, sync_listing_offers
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "northendhaarlem"
SHOP_NAME = "North End Haarlem"
SHOP_DOMAIN = "northendhaarlem.nl"
BASE_URL = "https://www.northendhaarlem.nl"
DISCOVERY_SOURCE = "northendhaarlem_category_listing"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.7,en;q=0.6",
}

CATEGORY_SEEDS = [
    {"name": "Coloured vinyl", "slug": "coloured-vinyl", "category_id": "3735064"},
    {"name": "High-Quality", "slug": "high-quality", "category_id": "6844139"},
    {"name": "Pop", "slug": "pop", "category_id": "504448"},
    {"name": "Rock", "slug": "rock", "category_id": "1132140"},
    {"name": "Indie / Alternative", "slug": "indie-alternative", "category_id": "1527573"},
    {"name": "HipHop / Soul", "slug": "hiphop-soul", "category_id": "4833613"},
    {"name": "Blues / Folk", "slug": "blues-folk", "category_id": "1132146"},
    {"name": "Electronic", "slug": "electronic", "category_id": "4184147"},
    {"name": "Jazz", "slug": "jazz", "category_id": "1132141"},
    {"name": "RSD 26", "slug": "rsd-26", "category_id": "7759833"},
]

PRODUCT_PATH_RE = re.compile(r"/a-(?P<id>[0-9]+)(?:/|$)", flags=re.I)
PRICE_RE = re.compile(r"€\s*([0-9]{1,5})\s*[,.]\s*(?:\^\{?)?([0-9]{2})(?:\})?", flags=re.I)
FORMAT_RE = re.compile(
    r"\b(?:LP|2LP|3LP|4LP|5LP|7INCH|7\s?INCH|10\s?INCH|12\s?INCH|SINGLE|VINYL|BOX\s?SET)\b",
    flags=re.I,
)


@dataclass(frozen=True)
class ListingRow:
    link: DiscoveredLink
    offer: ListingOffer


def clean(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def category_url(seed: dict[str, str], page: int) -> str:
    category_id = seed["category_id"]
    slug = seed["slug"]
    if page <= 1:
        return f"{BASE_URL}/c-{category_id}/{slug}/"
    return (
        f"{BASE_URL}/c-{category_id}-{page}/{slug}/"
        "?sort_order=ascending&sort_method=by_relevance"
    )


def normalize_product_url(href: str) -> str | None:
    if not href:
        return None
    absolute = urljoin(BASE_URL, href)
    parsed = urlparse(absolute)
    if SHOP_DOMAIN not in parsed.netloc.lower():
        return None
    if not PRODUCT_PATH_RE.search(parsed.path):
        return None
    path = parsed.path.rstrip("/") + "/"
    return urlunparse(("https", SHOP_DOMAIN, path, "", "", ""))


def source_product_id_from_url(url: str) -> str | None:
    match = PRODUCT_PATH_RE.search(urlparse(url).path)
    return match.group("id") if match else None


def decimal_price(value: str | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def extract_prices(text: str) -> tuple[str | None, str | None, bool]:
    prices: list[str] = []
    for euros, cents in PRICE_RE.findall(text or ""):
        price = f"{int(euros)}.{cents}"
        if price not in prices:
            prices.append(price)
    if not prices:
        return None, None, False
    current_price = prices[-1]
    original_price = prices[0] if len(prices) >= 2 and prices[0] != current_price else None
    return current_price, original_price, bool(original_price)


def normalize_availability(text: str) -> str | None:
    value = clean(text).lower()
    if not value:
        return None
    if "uitverkocht" in value or "niet op voorraad" in value:
        return "out_of_stock"
    if "op voorraad" in value or re.search(r"\bvoorraad\s+[0-9]+\s+stuk", value):
        return "in_stock"
    if (
        "op bestelling" in value
        or "levertijd" in value
        or "pre-order" in value
        or "preorder" in value
        or "wordt verwacht" in value
        or "awaiting repress" in value
        or "awaiting re-press" in value
    ):
        return "preorder"
    return "unknown"


def extract_image_url(container: Tag) -> str | None:
    image = container.select_one("img[src], img[data-src], img[data-original], img[srcset]")
    if not image:
        return None
    for attr in ("data-src", "data-original", "src"):
        value = image.get(attr)
        if value:
            return urljoin(BASE_URL, str(value))
    srcset = image.get("srcset")
    if srcset:
        first = str(srcset).split(",", 1)[0].strip().split(" ", 1)[0]
        if first:
            return urljoin(BASE_URL, first)
    return None


def extract_title(container: Tag, source_url: str) -> str | None:
    candidates: list[str] = []
    for anchor in container.select('a[href*="/a-"]'):
        normalized = normalize_product_url(str(anchor.get("href") or ""))
        if normalized != source_url:
            continue
        text = clean(anchor.get_text(" ", strip=True))
        if text and "image:" not in text.lower() and not PRICE_RE.search(text):
            candidates.append(text)
    if candidates:
        return max(candidates, key=len)
    return None


def infer_format(title: str | None) -> str | None:
    match = FORMAT_RE.search(title or "")
    return clean(match.group(0)).upper().replace(" ", "") if match else None


def choose_product_container(anchor: Tag) -> Tag:
    best: Tag = anchor
    node = anchor
    for _ in range(12):
        parent = node.parent
        if not isinstance(parent, Tag):
            break
        text = clean(parent.get_text(" ", strip=True))
        product_link_count = len(parent.select('a[href*="/a-"]'))
        if "€" in text and 60 <= len(text) <= 2500:
            best = parent
            if product_link_count <= 4:
                break
        node = parent
    return best


def page_signature(source_urls: Iterable[str]) -> str:
    joined = "\n".join(sorted(set(source_urls)))
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def fetch_html(url: str, timeout: int = 45, attempts: int = 3) -> tuple[int, str]:
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                url,
                headers=REQUEST_HEADERS,
                timeout=(10, timeout),
            )
            return response.status_code, response.text
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt >= attempts:
                print(
                    "[NORTHEND-FETCH-ERROR]",
                    {
                        "url": url,
                        "attempt": attempt,
                        "attempts": attempts,
                        "error": repr(exc),
                    },
                    flush=True,
                )
                return 599, ""

            sleep_seconds = attempt * 5
            print(
                "[NORTHEND-FETCH-RETRY]",
                {
                    "url": url,
                    "attempt": attempt,
                    "attempts": attempts,
                    "sleep_seconds": sleep_seconds,
                    "error": repr(exc),
                },
                flush=True,
            )
            time.sleep(sleep_seconds)

    print(
        "[NORTHEND-FETCH-ERROR]",
        {"url": url, "error": repr(last_error)},
        flush=True,
    )
    return 599, ""


def parse_listing_page(
    html: str,
    *,
    discovery_url: str,
    seed: dict[str, str],
    page: int,
    seen_at: str,
) -> list[ListingRow]:
    soup = BeautifulSoup(html, "html.parser")
    rows_by_url: dict[str, ListingRow] = {}
    position = 0

    for anchor in soup.select('a[href*="/a-"]'):
        if not isinstance(anchor, Tag):
            continue
        source_url = normalize_product_url(str(anchor.get("href") or ""))
        if not source_url:
            continue

        container = choose_product_container(anchor)
        card_text = clean(container.get_text(" ", strip=True))
        price, original_price, is_sale = extract_prices(card_text)
        if not price:
            # Prevent duplicate, non-product anchors from overwriting listing payloads with prices.
            continue

        title_raw = extract_title(container, source_url)
        if not title_raw:
            continue

        source_product_id = source_product_id_from_url(source_url)
        image_url = extract_image_url(container)
        availability = normalize_availability(card_text)
        carrier = infer_format(title_raw)
        position += 1

        payload = {
            "discovery_source": DISCOVERY_SOURCE,
            "discovery_url": discovery_url,
            "category_slug": seed["slug"],
            "category_name": seed["name"],
            "category_id": seed["category_id"],
            "page": page,
            "listing_position": position,
            "source_product_id": source_product_id,
            "title_raw": title_raw,
            "artist_raw": None,
            "format": carrier,
            "carrier": carrier,
            "price": price,
            "price_source": "listing",
            "is_sale": is_sale,
            "availability": availability,
            "image_url": image_url,
            "listing_seen_at": seen_at,
            "listing_text": card_text[:1200],
        }
        if original_price:
            payload["original_price"] = original_price
            payload["compare_at_price"] = original_price

        link = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=source_product_id,
            payload=payload,
        )
        offer = ListingOffer(
            shop_name=SHOP_NAME,
            shop_domain=SHOP_DOMAIN,
            shop_country="NL",
            source_url=source_url,
            price=decimal_price(price),
            availability=availability,
            currency="EUR",
            ean=None,
            seen_at=datetime.now(timezone.utc),
            raw={
                **payload,
                "source_product_id": source_product_id,
                "title_raw": title_raw,
                "artist_raw": None,
                "image_url": image_url,
            },
        )

        existing = rows_by_url.get(source_url)
        if existing is None:
            rows_by_url[source_url] = ListingRow(link=link, offer=offer)
            continue

        existing_price = existing.link.payload.get("price") if existing.link.payload else None
        if not existing_price and price:
            rows_by_url[source_url] = ListingRow(link=link, offer=offer)

    return list(rows_by_url.values())


def discover_listing_rows(
    *,
    category_limit: int,
    max_pages_per_category: int,
    delay_seconds: float,
) -> list[ListingRow]:
    selected_seeds = CATEGORY_SEEDS[:category_limit] if category_limit > 0 else CATEGORY_SEEDS
    all_rows: list[ListingRow] = []
    global_seen_urls: set[str] = set()

    for seed in selected_seeds:
        category_rows: list[ListingRow] = []
        category_seen_urls: set[str] = set()
        signatures: set[str] = set()
        page = 1

        while True:
            if max_pages_per_category > 0 and page > max_pages_per_category:
                break

            url = category_url(seed, page)
            print("[NORTHEND-LISTING] page", {"category": seed["slug"], "page": page, "url": url}, flush=True)
            status_code, html = fetch_html(url)
            if status_code in {404, 410}:
                print(
                    "[NORTHEND-LISTING-STOP] missing_page",
                    {"category": seed["slug"], "page": page, "status_code": status_code},
                    flush=True,
                )
                break
            if status_code >= 400:
                print(
                    "[NORTHEND-LISTING-STOP] http_error",
                    {"category": seed["slug"], "page": page, "status_code": status_code},
                    flush=True,
                )
                break

            rows = parse_listing_page(
                html,
                discovery_url=url,
                seed=seed,
                page=page,
                seen_at=now_iso(),
            )
            urls = [row.link.source_url for row in rows]
            signature = page_signature(urls)
            new_rows = [row for row in rows if row.link.source_url not in category_seen_urls]

            print(
                "[NORTHEND-LISTING-PAGE]",
                {
                    "category": seed["slug"],
                    "page": page,
                    "links": len(rows),
                    "priced_links": sum(1 for row in rows if row.link.payload.get("price")),
                    "new_links": len(new_rows),
                    "total_category_links": len(category_seen_urls) + len(new_rows),
                },
                flush=True,
            )

            if not rows:
                print("[NORTHEND-LISTING-STOP] no_product_links", {"category": seed["slug"], "page": page}, flush=True)
                break
            if signature in signatures:
                print("[NORTHEND-LISTING-STOP] duplicate_signature", {"category": seed["slug"], "page": page}, flush=True)
                break
            if not new_rows:
                print("[NORTHEND-LISTING-STOP] no_new_source_urls", {"category": seed["slug"], "page": page}, flush=True)
                break

            signatures.add(signature)
            category_seen_urls.update(row.link.source_url for row in new_rows)
            category_rows.extend(new_rows)
            page += 1
            if delay_seconds > 0:
                time.sleep(delay_seconds)

        for row in category_rows:
            if row.link.source_url not in global_seen_urls:
                global_seen_urls.add(row.link.source_url)
                all_rows.append(row)

        print(
            "[NORTHEND-LISTING-CATEGORY]",
            {
                "category": seed["slug"],
                "links": len(category_rows),
                "priced_links": sum(1 for row in category_rows if row.link.payload.get("price")),
            },
            flush=True,
        )

    return all_rows


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh North End Haarlem listing prices into USF link registry.")
    parser.add_argument("--category-limit", type=int, default=0, help="Aantal categorieën; 0 = alle seeds.")
    parser.add_argument("--max-pages-per-category", type=int, default=1, help="Aantal pagina's per categorie; 0 = tot stopconditie.")
    parser.add_argument("--delay-seconds", type=float, default=0.5)
    parser.add_argument("--fast-price-sync", action="store_true", help="Update bestaande publieke prijzen vanuit link payloads.")
    parser.add_argument("--write", action="store_true", help="Werkelijke databasewrites uitvoeren.")
    args = parser.parse_args()

    load_env()
    rows = discover_listing_rows(
        category_limit=args.category_limit,
        max_pages_per_category=args.max_pages_per_category,
        delay_seconds=args.delay_seconds,
    )
    links = [row.link for row in rows]
    offers = [row.offer for row in rows]

    print(
        "[NORTHEND-LISTING] discovered",
        {
            "links": len(links),
            "offers": len(offers),
            "write": args.write,
            "fast_price_sync": args.fast_price_sync,
        },
        flush=True,
    )

    if not args.write:
        print("[NORTHEND-LISTING] dry_run_sample", json.dumps([row.link.payload for row in rows[:3]], ensure_ascii=False), flush=True)
        return 0

    registry_stats = upsert_discovered_links(links)
    print("[NORTHEND-LISTING] registry", vars(registry_stats), flush=True)

    with db_connection() as conn:
        if args.fast_price_sync:
            sync_stats = bulk_update_prices_from_link_registry(
                conn,
                shop_registry_id=SHOP_ID,
                shop_domain=SHOP_DOMAIN,
                write=True,
                currency="EUR",
            )
            print("[NORTHEND-LISTING] fast_price_sync", vars(sync_stats), flush=True)
        else:
            sync_stats = sync_listing_offers(conn, offers, write=True)
            print("[NORTHEND-LISTING] listing_offer_sync", vars(sync_stats), flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
