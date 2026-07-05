#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import html as html_lib
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import ListingOffer, sync_listing_offers
from scripts.scrapers.usf.core.models import DiscoveredLink
from scripts.scrapers.usf.core.fast_listing_price_sync import bulk_update_prices_from_link_registry

SHOP_ID = "myrecordstore"
SHOP_NAME = "My Record Store"
SHOP_DOMAIN = "myrecordstore.nl"
SHOP_COUNTRY = "NL"
BASE_URL = "https://myrecordstore.nl"
SEED_URL = "https://myrecordstore.nl/item/nieuw?type=LP"

HEADERS = {
    "User-Agent": "Mozilla/5.0 Vinylofy MyRecordStore listing refresh",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}

PRICE_RE = re.compile(r"(?:€|EUR)?\s*([0-9]{1,5}(?:[.,][0-9]{2}))")
DAY_RE = re.compile(r"(?:(?:circa|ca\.?)\s*)?([0-9]{1,2})\s*(?:werk)?dagen?", re.I)
WEEK_RE = re.compile(r"(?:(?:circa|ca\.?)\s*)?([0-9]{1,2})\s*weken?", re.I)

def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()

def page_url(page: int) -> str:
    return SEED_URL if page <= 1 else f"{SEED_URL}&page={page}"

def normalize_url(href: str) -> str:
    return urljoin(BASE_URL, html_lib.unescape(href or "")).split("#", 1)[0].strip()

def source_product_id_from_url(url: str) -> str:
    slug = url.rstrip("/").rsplit("/", 1)[-1]
    return slug or hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]

def extract_money(text: str) -> str | None:
    matches = PRICE_RE.findall(clean(text))
    if not matches:
        return None
    return matches[-1].replace(",", ".")

def estimate_delivery_days(text: str) -> int | None:
    low = clean(text).lower()
    m = DAY_RE.search(low)
    if m:
        return int(m.group(1))
    m = WEEK_RE.search(low)
    if m:
        return int(m.group(1)) * 7
    if "4 weken" in low or "vier weken" in low:
        return 28
    return None

def normalize_availability(text: str) -> tuple[str, int | None, str]:
    raw = clean(text)
    low = raw.lower()
    days = estimate_delivery_days(raw)

    if "niet leverbaar" in low or "uitverkocht" in low:
        return "out_of_stock", days, raw

    if days is not None:
        return ("in_stock" if days <= 14 else "out_of_stock"), days, raw

    if "op voorraad" in low:
        return "in_stock", days, raw

    if "te bestellen" in low or "levertijd" in low or "leverancier" in low:
        return "unknown", days, raw

    return "unknown", days, raw

def split_artist_title(text: str) -> tuple[str | None, str | None]:
    text = clean(text)
    for sep in (" - ", " – ", " — "):
        if sep in text:
            a, t = text.split(sep, 1)
            return clean(a) or None, clean(t) or None
    return None, text or None

def parse_listing_page(html: str, *, listing_url: str, page: int, seen_at: datetime):
    soup = BeautifulSoup(html, "html.parser")
    links: dict[str, DiscoveredLink] = {}
    offers: dict[str, ListingOffer] = {}

    # MyRecordStore heeft productlinks als /artist/slug en /artist/title-slug.
    # Titel/detail-links zijn de links waarvan de tekst direct na een " - " in de listingregel staat.
    anchors = soup.select("a[href]")
    title_links = []

    for a in anchors:
        href = a.get("href") or ""
        text = clean(a.get_text(" ", strip=True))
        if not href or not text:
            continue
        if href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue

        url = normalize_url(href)
        if not url.startswith(BASE_URL + "/"):
            continue
        if any(skip in url for skip in (
            "/item/nieuw",
            "/nieuws",
            "/contact",
            "/cadeaubon",
            "/merch",
            "/aanbiedingen",
            "/privacy",
            "/algemene-voorwaarden",
        )):
            continue

        prev = ""
        node = a.previous_sibling
        if node is not None:
            prev = clean(str(node))
        parent_text = clean(a.parent.get_text(" ", strip=True)) if a.parent else ""

        if prev in {"-", "–", "—"} or re.search(r"\s[-–—]\s*" + re.escape(text), parent_text):
            title_links.append(a)

    position = 0
    for a in title_links:
        source_url = normalize_url(a.get("href") or "")
        if source_url in links:
            continue

        # Neem de dichtstbijzijnde parent die prijs + voorraadtekst bevat, maar stop voor globale pagina-wrapper.
        item = a
        for parent in list(a.parents)[:8]:
            txt = clean(parent.get_text(" ", strip=True))
            if re.search(r"\b[0-9]{1,5},[0-9]{2}\b|\b[0-9]{1,5},-\b", txt) and (
                "op voorraad" in txt.lower()
                or "levertijd" in txt.lower()
                or "te bestellen" in txt.lower()
                or "niet op voorraad" in txt.lower()
            ):
                item = parent
                break

        text = clean(item.get_text(" ", strip=True))
        raw_prices = re.findall(r"\b([0-9]{1,5})(?:,([0-9]{2})|,-)\b", text)
        prices = [f"{int(e)}.{c or '00'}" for e, c in raw_prices]
        if not prices:
            continue

        price = prices[-1]
        sale_price = None
        original_price = None
        if len(set(prices)) >= 2:
            original_price = prices[0]
            sale_price = prices[-1]
            price = sale_price

        title_raw = clean(a.get_text(" ", strip=True))
        artist_raw = None

        parent_text = clean(a.parent.get_text(" ", strip=True)) if a.parent else ""
        if " - " in parent_text:
            left = parent_text.split(" - ", 1)[0]
            artist_raw = clean(left)
        elif " – " in parent_text:
            left = parent_text.split(" – ", 1)[0]
            artist_raw = clean(left)

        title_text = clean(f"{artist_raw} - {title_raw}") if artist_raw else title_raw
        availability, estimated_days, availability_raw = normalize_availability(text)

        img = item.select_one("img[src], img[data-src], img[data-lazy-src]")
        image_url = None
        if img:
            image_url = normalize_url(img.get("src") or img.get("data-src") or img.get("data-lazy-src") or "")

        position += 1
        source_product_id = source_product_id_from_url(source_url)
        payload = {
            "source": "myrecordstore_listing",
            "discovery_source": "myrecordstore_listing",
            "discovery_url": listing_url,
            "page": page,
            "listing_position": position,
            "title": title_text,
            "title_raw": title_raw,
            "artist_raw": artist_raw,
            "price": price,
            "price_source": "listing",
            "sale_price": sale_price,
            "original_price": original_price,
            "availability": availability,
            "availability_raw": availability_raw,
            "estimated_delivery_days": estimated_days,
            "image_url": image_url,
            "source_product_id": source_product_id,
            "listing_seen_at": seen_at.isoformat(),
        }

        links[source_url] = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=source_product_id,
            payload=payload,
        )

        offers[source_url] = ListingOffer(
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

    return list(links.values()), list(offers.values())

def deactivate_missing_links(run_started_at: datetime, *, write: bool) -> dict[str, int]:
    if not write:
        return {"links_deactivated": 0, "prices_deactivated": 0}

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.shop_product_links
                set status = 'inactive'
                where shop_id = %s
                  and status = 'active'
                  and last_seen_at < %s
                """,
                (SHOP_ID, run_started_at),
            )
            links_count = cur.rowcount

            cur.execute(
                """
                update public.prices pr
                set is_active = false,
                    availability = 'out_of_stock',
                    updated_at = now()
                from public.shops sh
                where pr.shop_id = sh.id
                  and sh.domain = %s
                  and not exists (
                    select 1
                    from public.shop_product_links spl
                    where spl.shop_id = %s
                      and spl.status = 'active'
                      and trim(trailing '/' from split_part(spl.source_url, '?', 1))
                        = trim(trailing '/' from split_part(pr.product_url, '?', 1))
                  )
                """,
                (SHOP_DOMAIN, SHOP_ID),
            )
            prices_count = cur.rowcount

    return {"links_deactivated": links_count, "prices_deactivated": prices_count}

def build_parser():
    p = argparse.ArgumentParser(description="Refresh My Record Store listing-first prices.")
    p.add_argument("--start-page", type=int, default=1)
    p.add_argument("--max-pages", type=int, default=5, help="0 = doorlopen tot stopconditie")
    p.add_argument("--sleep", type=float, default=1.0)
    p.add_argument("--write", action="store_true")
    p.add_argument("--fast-price-sync", action="store_true")
    p.add_argument("--sync-existing-offers", action="store_true")
    p.add_argument("--delist-missing", action="store_true")
    return p

def main() -> int:
    args = build_parser().parse_args()
    run_started_at = datetime.now(timezone.utc)
    seen_at = run_started_at

    session = requests.Session()
    session.headers.update(HEADERS)

    all_links = {}
    all_offers = {}
    seen_signatures = set()
    page = args.start_page
    pages_done = 0

    while args.max_pages == 0 or pages_done < args.max_pages:
        url = page_url(page)
        print("[MYRECORDSTORE-LISTING] page", {"page": page, "url": url}, flush=True)
        response = session.get(url, timeout=45)

        if response.status_code == 429:
            print("[MYRECORDSTORE-LISTING-WARN] HTTP 429; stopping safely", {"page": page}, flush=True)
            break
        if response.status_code >= 400:
            print("[MYRECORDSTORE-LISTING-WARN] HTTP error; stopping", {"page": page, "status": response.status_code}, flush=True)
            break

        links, offers = parse_listing_page(response.text, listing_url=url, page=page, seen_at=seen_at)
        signature = tuple(sorted(link.source_url for link in links))
        if not links:
            print("[MYRECORDSTORE-LISTING] stop no_products", {"page": page}, flush=True)
            break
        if signature in seen_signatures:
            print("[MYRECORDSTORE-LISTING] stop repeated_signature", {"page": page}, flush=True)
            break
        seen_signatures.add(signature)

        for link in links:
            all_links[link.source_url] = link
        for offer in offers:
            all_offers[offer.source_url] = offer

        pages_done += 1
        page += 1
        time.sleep(args.sleep)

    print("[MYRECORDSTORE-LISTING] collected", {"links": len(all_links), "offers": len(all_offers), "write": args.write}, flush=True)

    if args.write and all_links:
        result = upsert_discovered_links(list(all_links.values()))
        print("[MYRECORDSTORE-LISTING] link_registry", vars(result), flush=True)

    if args.write and args.sync_existing_offers and all_offers:
        sync_result = sync_listing_offers(list(all_offers.values()), write=True)
        print("[MYRECORDSTORE-LISTING] listing_price_sync", vars(sync_result), flush=True)

    if args.write and args.fast_price_sync:
        with db_connection() as conn:
            stats = bulk_update_prices_from_link_registry(
                conn,
                shop_registry_id=SHOP_ID,
                shop_domain=SHOP_DOMAIN,
                write=True,
                currency="EUR",
            )
        print("[MYRECORDSTORE-FAST-SYNC]", vars(stats), flush=True)

    if args.write and args.delist_missing and pages_done > 0:
        stats = deactivate_missing_links(run_started_at, write=True)
        print("[MYRECORDSTORE-LISTING] delist_missing", stats, flush=True)

    return 0

if __name__ == "__main__":
    raise SystemExit(main())
