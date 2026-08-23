#!/usr/bin/env python3
from __future__ import annotations

import argparse
import html as html_lib
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.variaworld_fast_price_sync import bulk_update_variaworld_prices_from_link_registry
from scripts.scrapers.usf.core.listing_price_sync import ListingOffer, sync_listing_offers
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "variaworld"
SHOP_NAME = "Variaworld"
SHOP_DOMAIN = "variaworld.nl"
SHOP_COUNTRY = "NL"
BASE_URL = "https://www.variaworld.nl"

SEEDS = {
    # Variaworld's catalog router expects the filter expression as an encoded
    # path segment. Sending these values as a normal query string returns a
    # short HTTP 200 response without catalog HTML on the Actions runners.
    "lp_nieuw": "https://www.variaworld.nl/vinyl/lp-nieuw/m_ge%3D%5Bj%3Bm%5D%26m_so%3D2%26m_su%3D1%26startpagina%3D{page}",
    "12inch_nieuw": "https://www.variaworld.nl/vinyl/12-inch-nieuw/m_ge%3D%5Bj%3Bm%5D%26m_so%3D2%26m_su%3D7%26startpagina%3D{page}",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 Vinylofy Variaworld listing refresh",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}

EMPTY_PAGE_RETRIES = 2
BLOCK_PAGE_MARKERS = (
    "captcha",
    "cloudflare",
    "access denied",
    "verify you are human",
    "robot",
)


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_product_url(href: str) -> str:
    return urljoin(BASE_URL, html_lib.unescape(href)).strip()


def extract_source_product_id(href: str) -> str | None:
    match = re.search(r"[?&]at=([^&]+)", html_lib.unescape(href or ""))
    return match.group(1) if match else None


def extract_money(text: str) -> str | None:
    text = clean(text)
    if not text:
        return None
    amounts = re.findall(r"(?:€|EUR)?\s*([0-9]+(?:[.,][0-9]{2}))", text, flags=re.I)
    if not amounts:
        return None
    return amounts[-1].replace(",", ".")


def node_text(node, selector: str) -> str:
    found = node.select_one(selector)
    return clean(found.get_text(" ", strip=True)) if found else ""


def extract_item_price(item) -> str | None:
    # Actie-/saleprijs wint: overzicht_prijs is de huidige/actieprijs wanneer gevuld.
    current_price = extract_money(node_text(item, ".overzicht_prijs"))
    if current_price:
        return current_price

    base_price = extract_money(node_text(item, ".overzicht_van_prijs"))
    if base_price:
        return base_price

    return extract_money(item.get_text(" ", strip=True))


def extract_availability(item, *, price: str | None) -> str:
    text = clean(item.get_text(" ", strip=True)).lower()

    if any(token in text for token in ("uitverkocht", "niet leverbaar", "sold out", "out of stock")):
        return "out_of_stock"

    pre_order = item.select_one(".pre_order_blok_overzicht")
    if pre_order:
        style = clean(pre_order.get("style")).lower()
        if "display:none" not in style and "display: none" not in style:
            return "preorder"

    if "pre-order" in text or "preorder" in text or "leverbaar vanaf" in text:
        return "preorder"

    # Variaworld toont actieve listingitems met prijs; detail kan later verfijnen.
    if price:
        return "in_stock"

    return "unknown"


def payload_score(payload: dict[str, object]) -> int:
    return (
        10 * bool(payload.get("price"))
        + 4 * bool(payload.get("image_url"))
        + 3 * bool(payload.get("title"))
        + 2 * bool(payload.get("artist_raw"))
        + 1 * bool(payload.get("availability"))
    )


def response_diagnostics(response: requests.Response) -> dict[str, object]:
    """Return safe, compact diagnostics for an unexpected empty listing page."""
    soup = BeautifulSoup(response.text, "html.parser")
    title = clean(soup.title.get_text(" ", strip=True)) if soup.title else ""
    body_prefix = clean(soup.get_text(" ", strip=True))[:160].lower()
    markers = [marker for marker in BLOCK_PAGE_MARKERS if marker in body_prefix or marker in response.text.lower()]
    return {
        "status_code": response.status_code,
        "content_type": response.headers.get("Content-Type", ""),
        "bytes": len(response.content),
        "final_url": response.url,
        "title": title[:160],
        "possible_block_markers": markers,
    }


def parse_listing_page(
    html: str,
    *,
    seed_key: str,
    page: int,
    listing_url: str,
    seen_at: datetime,
) -> tuple[list[DiscoveredLink], list[ListingOffer]]:
    soup = BeautifulSoup(html, "html.parser")

    links_by_url: dict[str, DiscoveredLink] = {}
    offers_by_url: dict[str, ListingOffer] = {}

    items = soup.select("div#overzicht_container a.overzichtbox_2[href*='/artikel/detail.php']")

    for position, item in enumerate(items, start=1):
        href = clean(item.get("href"))
        if not href:
            continue

        source_url = normalize_product_url(href)
        source_product_id = extract_source_product_id(href)
        if not source_product_id:
            continue

        price = extract_item_price(item)
        availability = extract_availability(item, price=price)

        artist_raw = node_text(item, ".koptekst")
        texts = [clean(x.get_text(" ", strip=True)) for x in item.select(".tekst")]
        title_raw = texts[0] if texts else None
        carrier_raw = texts[-1] if texts else None
        title = clean(f"{artist_raw} - {title_raw}") if artist_raw and title_raw else clean(item.get_text(" ", strip=True))

        image_node = item.select_one(".overzichtfotobox_2 img[src]")
        image_url = urljoin(BASE_URL, image_node.get("src")) if image_node and image_node.get("src") else None

        payload = {
            "discovery_source": "variaworld_listing",
            "discovery_url": listing_url,
            "page": page,
            "startpagina": page,
            "listing_position": position,
            "price": price,
            "price_source": "listing",
            "availability": availability,
            "listing_seen_at": seen_at.isoformat(),
            "category": seed_key,
            "artist_raw": artist_raw,
            "title_raw": title_raw,
            "title": title,
            "carrier_raw": carrier_raw,
            "image_url": image_url,
            "source_product_id": source_product_id,
        }

        existing = links_by_url.get(source_url)
        if existing is not None:
            existing_price = existing.payload.get("price") if isinstance(existing.payload, dict) else None
            # Nooit een rijkere payload met prijs overschrijven door een latere variant zonder prijs.
            if existing_price and not price:
                continue
            if payload_score(payload) <= payload_score(existing.payload):
                continue

        links_by_url[source_url] = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=source_product_id,
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
    parser = argparse.ArgumentParser(description="Refresh Variaworld listing-first prices.")
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--max-pages", type=int, default=5, help="Aantal pagina's per seed; 0 = doorlopen tot stopconditie.")
    parser.add_argument("--sleep", type=float, default=1.5)
    parser.add_argument("--max-page-failures", type=int, default=3)
    parser.add_argument(
        "--empty-page-retries",
        type=int,
        default=EMPTY_PAGE_RETRIES,
        help="Retries voor een HTTP 200-response zonder listinglinks.",
    )
    parser.add_argument("--fast-price-sync", action="store_true")
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
    if args.empty_page_retries < 0:
        raise SystemExit("[ERROR] --empty-page-retries mag niet negatief zijn.")

    session = requests.Session()
    session.headers.update(HEADERS)

    seen_at = datetime.now(timezone.utc)
    all_links_by_url: dict[str, DiscoveredLink] = {}
    all_offers_by_url: dict[str, ListingOffer] = {}

    for seed_key, url_template in SEEDS.items():
        page = args.start_page
        pages_done = 0
        consecutive_page_failures = 0
        seen_page_signatures: set[tuple[str, ...]] = set()
        seen_seed_urls: set[str] = set()

        print("[LISTING-REFRESH] seed_start", {"shop": SHOP_ID, "seed": seed_key}, flush=True)

        while args.max_pages == 0 or pages_done < args.max_pages:
            listing_url = url_template.format(page=page)
            print("[LISTING-REFRESH] page", {"shop": SHOP_ID, "seed": seed_key, "page": page, "url": listing_url}, flush=True)

            try:
                response = session.get(listing_url, timeout=30)
            except requests.RequestException as exc:
                consecutive_page_failures += 1
                print("[LISTING-REFRESH][WARN] request failed", {
                    "seed": seed_key,
                    "page": page,
                    "error": str(exc),
                    "consecutive_page_failures": consecutive_page_failures,
                }, flush=True)
                if consecutive_page_failures >= args.max_page_failures:
                    break
                pages_done += 1
                page += 1
                time.sleep(args.sleep)
                continue

            if response.status_code == 429:
                print("[LISTING-REFRESH][WARN] HTTP 429, stopping safely.", {"seed": seed_key, "page": page}, flush=True)
                break

            if response.status_code >= 500:
                consecutive_page_failures += 1
                print("[LISTING-REFRESH][WARN] server error", {
                    "seed": seed_key,
                    "page": page,
                    "status_code": response.status_code,
                    "consecutive_page_failures": consecutive_page_failures,
                }, flush=True)
                if consecutive_page_failures >= args.max_page_failures:
                    break
                pages_done += 1
                page += 1
                time.sleep(args.sleep)
                continue

            response.raise_for_status()
            consecutive_page_failures = 0
            response.encoding = response.apparent_encoding or response.encoding or "utf-8"

            links, offers = parse_listing_page(
                response.text,
                seed_key=seed_key,
                page=page,
                listing_url=listing_url,
                seen_at=seen_at,
            )

            for empty_page_attempt in range(1, args.empty_page_retries + 1):
                if links:
                    break
                print("[LISTING-REFRESH][WARN] HTTP 200 zonder listinglinks; retrying.", {
                    "seed": seed_key,
                    "page": page,
                    "attempt": empty_page_attempt,
                    "max_retries": args.empty_page_retries,
                    **response_diagnostics(response),
                }, flush=True)
                time.sleep(args.sleep * empty_page_attempt)
                try:
                    response = session.get(listing_url, timeout=30)
                except requests.RequestException as exc:
                    print("[LISTING-REFRESH][WARN] empty-page retry failed", {
                        "seed": seed_key,
                        "page": page,
                        "attempt": empty_page_attempt,
                        "error": str(exc),
                    }, flush=True)
                    break
                if response.status_code == 429:
                    print("[LISTING-REFRESH][WARN] HTTP 429 during empty-page retry; stopping safely.", {
                        "seed": seed_key,
                        "page": page,
                        "attempt": empty_page_attempt,
                    }, flush=True)
                    break
                if response.status_code >= 500:
                    print("[LISTING-REFRESH][WARN] server error during empty-page retry", {
                        "seed": seed_key,
                        "page": page,
                        "attempt": empty_page_attempt,
                        "status_code": response.status_code,
                    }, flush=True)
                    break
                response.raise_for_status()
                response.encoding = response.apparent_encoding or response.encoding or "utf-8"
                links, offers = parse_listing_page(
                    response.text,
                    seed_key=seed_key,
                    page=page,
                    listing_url=listing_url,
                    seen_at=seen_at,
                )

            page_signature = tuple(link.source_url for link in links)
            if page_signature and page_signature in seen_page_signatures:
                print("[LISTING-REFRESH][WARN] duplicate listing page detected; stopping seed.", {
                    "seed": seed_key,
                    "page": page,
                    "links": len(links),
                    "first_link": page_signature[0],
                    "last_link": page_signature[-1],
                }, flush=True)
                break
            if page_signature:
                seen_page_signatures.add(page_signature)

            new_urls = {link.source_url for link in links} - seen_seed_urls

            print("[LISTING-REFRESH-PAGE]", {
                "seed": seed_key,
                "page": page,
                "links": len(links),
                "new_source_urls": len(new_urls),
                "offers_with_price": len(offers),
                "write": args.write,
            }, flush=True)

            if not links:
                print("[LISTING-REFRESH] no links, stopping seed.", {"seed": seed_key, "page": page}, flush=True)
                break

            if not new_urls:
                print("[LISTING-REFRESH][WARN] no new source_urls, stopping seed.", {"seed": seed_key, "page": page}, flush=True)
                break

            seen_seed_urls.update(new_urls)

            for link in links:
                existing = all_links_by_url.get(link.source_url)
                if existing is None:
                    all_links_by_url[link.source_url] = link
                else:
                    existing_price = existing.payload.get("price") if isinstance(existing.payload, dict) else None
                    new_price = link.payload.get("price") if isinstance(link.payload, dict) else None
                    if not (existing_price and not new_price) and payload_score(link.payload) > payload_score(existing.payload):
                        all_links_by_url[link.source_url] = link

            for offer in offers:
                all_offers_by_url[offer.source_url] = offer

            pages_done += 1
            page += 1
            if args.max_pages == 0 or pages_done < args.max_pages:
                time.sleep(args.sleep)

    all_links = list(all_links_by_url.values())
    all_offers = list(all_offers_by_url.values())

    print("[LISTING-REFRESH]", {
        "shop": SHOP_ID,
        "links": len(all_links),
        "offers_with_price": len(all_offers),
        "write": args.write,
    }, flush=True)

    for offer in all_offers[:8]:
        raw = offer.raw if isinstance(offer.raw, dict) else {}
        print("[LISTING-SAMPLE]", {
            "source_url": offer.source_url,
            "price": str(offer.price),
            "availability": offer.availability,
            "category": raw.get("category"),
        }, flush=True)

    if not all_links:
        raise SystemExit("[ERROR] Variaworld listing refresh leverde geen links op.")

    if args.write and not all_offers:
        raise SystemExit("[ERROR] Variaworld listing refresh vond geen prijzen; schrijf niets weg.")

    if not args.write:
        print("[LISTING-REFRESH] dry-run complete; geen databasewrites.", flush=True)
        return 0

    result = upsert_discovered_links(all_links)
    print("[LISTING-REFRESH] registry", {
        "inserted": result.inserted,
        "updated": result.updated,
        "total": result.total,
    }, flush=True)

    with db_connection() as conn:
        if args.fast_price_sync:
            stats = bulk_update_variaworld_prices_from_link_registry(
                conn,
                shop_registry_id=SHOP_ID,
                shop_domain=SHOP_DOMAIN,
                write=True,
                currency="EUR",
                max_matches_per_listing=3,
            )
            print("[LISTING-REFRESH] variaworld_fast_price_sync", vars(stats), flush=True)
        else:
            stats = sync_listing_offers(conn, all_offers, write=True)
            print("[LISTING-REFRESH] price_sync", vars(stats), flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
