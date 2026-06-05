#!/usr/bin/env python3
"""
Records on Vinyl master-only price refresh.

Clean purpose
-------------
Fast and safe refresh of existing Records on Vinyl products.

This script:
1. Reads existing recordsonvinyl_master.csv.
2. Builds handle -> EAN/GTIN mapping from the master.
3. Reads Records on Vinyl listing pages.
4. Extracts price only from the product card belonging to the product link.
5. Updates Supabase public.prices for existing products matched by EAN/GTIN.
6. Writes audit and snapshot CSVs.
7. Does not create products.
8. Does not call Shopify product JSON.
9. Does not use common.py or the normal importer.

Why
---
Refresh should refresh prices for known products.
Crawl should discover/enrich new products and EANs.
Mixing those two caused rate limits and difficult-to-debug importer behavior.
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import psycopg
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


BASE_URL = "https://recordsonvinyl.nl"
DEFAULT_COLLECTION = "https://recordsonvinyl.nl/collections/all"
SHOP_DOMAIN = "recordsonvinyl.nl"
SHOP_NAME = "Records on Vinyl"
SHOP_COUNTRY = "NL"
CURRENCY = "EUR"
USER_AGENT = "VinylofyRecordsOnVinylRefresh/1.0 (+https://vinylofy.com)"


@dataclass(frozen=True)
class MasterProduct:
    handle: str
    ean: str
    gtin14: str
    master_url: str


@dataclass(frozen=True)
class ListingPrice:
    handle: str
    product_url: str
    price_offer: float
    price_list: float | None
    title: str


@dataclass(frozen=True)
class RefreshCandidate:
    handle: str
    product_url: str
    ean: str
    gtin14: str
    price: float
    price_list: float | None
    title: str
    availability: str


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def clean_text(value: object | None) -> str:
    text = "" if value is None else str(value)
    text = text.replace("\u200e", " ").replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    parsed = parsed._replace(query="", fragment="")
    return urlunparse(parsed).rstrip("/")


def set_query_param(url: str, **params: str) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query.update({k: v for k, v in params.items() if v is not None})
    parsed = parsed._replace(query=urlencode(query))
    return urlunparse(parsed)


def product_handle_from_url(url: str) -> str | None:
    path = urlparse(url).path
    match = re.search(r"/products/([^/?#]+)", path)
    return match.group(1) if match else None


def canonical_product_url(href: str) -> str:
    return normalize_url(urljoin(BASE_URL, href))


def normalize_ean(value: object | None) -> str | None:
    raw = clean_text(value)
    raw = re.sub(r"\.0$", "", raw)
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None

    if len(digits) == 11:
        digits = "0" + digits

    if len(digits) not in (8, 12, 13, 14):
        return None

    return digits


def normalize_gtin14(value: object | None) -> str | None:
    ean = normalize_ean(value)
    if not ean:
        return None
    return ean.zfill(14)


def identifier_variants(value: object | None) -> list[str]:
    variants: list[str] = []

    def add(v: object | None) -> None:
        text = clean_text(v)
        if text and text not in variants:
            variants.append(text)

    raw = clean_text(value)
    if raw:
        add(raw)

    ean = normalize_ean(value)
    if not ean:
        return variants

    add(ean)
    add(ean.zfill(14))

    if len(ean) == 14 and ean.startswith("0"):
        add(ean[1:])
    if len(ean) == 13:
        add("0" + ean)
    if len(ean) == 12:
        add("0" + ean)
        add("00" + ean)

    return variants


def parse_price_text(value: object | None) -> float | None:
    text = clean_text(value)
    if not text:
        return None

    text = text.replace("€", "").replace("EUR", "").strip()
    text = re.sub(r"[^0-9,.\-]", "", text)

    if not text or text.startswith("-"):
        return None

    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        price = float(text)
    except ValueError:
        return None

    if price <= 0 or price > 1000:
        return None

    return round(price, 2)


def db_availability_from_price(price: float | None) -> str:
    return "in_stock" if price is not None else "unknown"


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl,en;q=0.8",
        }
    )
    return session


def get_with_retry(session: requests.Session, url: str, *, retries: int = 5, timeout: int = 30) -> requests.Response:
    wait = 1.0
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=timeout)

            if response.status_code == 200:
                return response

            if response.status_code in {429, 500, 502, 503, 504}:
                print(
                    f"[WARN] temporary_http status={response.status_code} "
                    f"attempt={attempt}/{retries} url={url}",
                    flush=True,
                )
                time.sleep(wait + random.random())
                wait = min(wait * 2, 30)
                continue

            raise RuntimeError(f"HTTP {response.status_code} url={url}")

        except Exception as exc:  # noqa: BLE001
            last_error = exc
            print(f"[WARN] request_failed attempt={attempt}/{retries} url={url} error={exc}", flush=True)
            time.sleep(wait + random.random())
            wait = min(wait * 2, 30)

    if last_error:
        raise last_error

    raise RuntimeError(f"request failed url={url}")


def load_master_products(master_csv: Path) -> dict[str, MasterProduct]:
    if not master_csv.exists():
        raise SystemExit(f"Master CSV not found: {master_csv}")

    by_handle: dict[str, MasterProduct] = {}
    skipped = 0

    with master_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for row in reader:
            product_url = clean_text(row.get("product_url") or row.get("url") or row.get("link"))
            handle = clean_text(row.get("handle")) or (product_handle_from_url(product_url) if product_url else "")

            ean = normalize_ean(
                row.get("ean13")
                or row.get("ean")
                or row.get("barcode")
                or row.get("gtin")
                or row.get("gtin_normalized")
            )
            gtin14 = normalize_gtin14(ean)

            if not handle or not ean or not gtin14:
                skipped += 1
                continue

            by_handle[handle] = MasterProduct(
                handle=handle,
                ean=ean,
                gtin14=gtin14,
                master_url=normalize_url(product_url) if product_url else f"{BASE_URL}/products/{handle}",
            )

    print(f"[MASTER] usable_handles={len(by_handle)} skipped={skipped}", flush=True)
    return by_handle


def first_price_in_element(element) -> float | None:
    if element is None:
        return None
    matches = re.findall(r"(?:€\s*)?(\d{1,5}[.,]\d{2})(?:\s*€)?", element.get_text(" ", strip=True))
    for match in matches:
        price = parse_price_text(match)
        if price is not None:
            return price
    return None


def parse_listing_card(anchor) -> ListingPrice | None:
    href = anchor.get("href") or ""
    if "/products/" not in href:
        return None

    product_url = canonical_product_url(href)
    handle = product_handle_from_url(product_url)
    if not handle:
        return None

    price_box = anchor.select_one(".grid-product__price")
    if price_box is None:
        return None

    sale_el = price_box.select_one(".sale-price-emphasis")
    original_el = price_box.select_one(".grid-product__price--original")

    if sale_el is not None:
        price_offer = first_price_in_element(sale_el)
        price_list = first_price_in_element(original_el)
        if price_offer is None:
            return None
        if price_list is not None and price_list <= price_offer:
            price_list = None
    else:
        cleaned = BeautifulSoup(str(price_box), "html.parser")
        for selector in [
            "style",
            "script",
            ".visually-hidden",
            ".flair-badge-layout",
            ".flair-badge",
            ".grid-product__price--original",
            ".sale-price-emphasis",
        ]:
            for node in cleaned.select(selector):
                node.decompose()
        price_offer = first_price_in_element(cleaned)
        price_list = None
        if price_offer is None:
            return None

    title_el = anchor.select_one(".grid-product__title")
    title = clean_text(title_el.get_text(" ", strip=True) if title_el else "")

    return ListingPrice(
        handle=handle,
        product_url=product_url,
        price_offer=price_offer,
        price_list=price_list,
        title=title,
    )


def discover_listing_prices(
    session: requests.Session,
    *,
    collection_url: str,
    max_pages: int,
    limit_products: int,
    sleep_seconds: float,
) -> dict[str, ListingPrice]:
    by_handle: dict[str, ListingPrice] = {}
    page = 1
    no_new_pages = 0

    while True:
        if max_pages > 0 and page > max_pages:
            break

        if limit_products > 0 and len(by_handle) >= limit_products:
            break

        page_url = set_query_param(collection_url, page=str(page))

        try:
            response = get_with_retry(session, page_url)
        except Exception as exc:  # noqa: BLE001
            print(
                f"[WARN] listing_page_failed page={page} url={page_url} error={exc}; "
                f"continuing with discovered={len(by_handle)}",
                flush=True,
            )
            break

        soup = BeautifulSoup(response.text, "html.parser")
        anchors = soup.select('a.grid-product__link[href*="/products/"]')

        if not anchors:
            print(f"[DISCOVER] page={page} no_cards stop", flush=True)
            break

        page_new = 0
        for anchor in anchors:
            item = parse_listing_card(anchor)
            if item is None:
                continue
            if item.handle in by_handle:
                continue

            by_handle[item.handle] = item
            page_new += 1

            if limit_products > 0 and len(by_handle) >= limit_products:
                break

        print(
            f"[DISCOVER] page={page} new={page_new} total={len(by_handle)} url={page_url}",
            flush=True,
        )

        no_new_pages = no_new_pages + 1 if page_new == 0 else 0
        if no_new_pages >= 2:
            break

        page += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    return by_handle


def build_candidates(
    master_by_handle: dict[str, MasterProduct],
    listing_by_handle: dict[str, ListingPrice],
) -> tuple[list[RefreshCandidate], list[dict[str, object]]]:
    candidates: list[RefreshCandidate] = []
    audit: list[dict[str, object]] = []

    for handle, master in master_by_handle.items():
        listing = listing_by_handle.get(handle)
        if listing is None:
            audit.append(
                {
                    "reason": "handle_not_seen_on_listing",
                    "handle": handle,
                    "ean": master.ean,
                    "master_url": master.master_url,
                }
            )
            continue

        candidates.append(
            RefreshCandidate(
                handle=handle,
                product_url=listing.product_url,
                ean=master.ean,
                gtin14=master.gtin14,
                price=listing.price_offer,
                price_list=listing.price_list,
                title=listing.title,
                availability=db_availability_from_price(listing.price_offer),
            )
        )

    print(f"[BUILD] candidates={len(candidates)} audit={len(audit)}", flush=True)
    return candidates, audit


def ensure_shop(cur) -> str:
    cur.execute("select id from public.shops where domain = %s limit 1", (SHOP_DOMAIN,))
    row = cur.fetchone()
    if row:
        return str(row[0])

    cur.execute(
        """
        insert into public.shops (name, domain, country, is_active)
        values (%s, %s, %s, true)
        returning id
        """,
        (SHOP_NAME, SHOP_DOMAIN, SHOP_COUNTRY),
    )
    return str(cur.fetchone()[0])


def find_product_ids(cur, candidate: RefreshCandidate) -> list[str]:
    ean_candidates = list(dict.fromkeys(identifier_variants(candidate.ean) + identifier_variants(candidate.gtin14)))
    gtin_candidates = list(dict.fromkeys(identifier_variants(candidate.gtin14) + identifier_variants(candidate.ean)))

    cur.execute(
        """
        select id
        from public.products
        where gtin_normalized = any(%s)
           or ean = any(%s)
        order by
          case when gtin_normalized = %s then 0 else 1 end,
          case when ean = %s then 0 else 1 end,
          updated_at desc nulls last,
          created_at desc nulls last
        """,
        (gtin_candidates, ean_candidates, candidate.gtin14, candidate.ean),
    )
    return [str(row[0]) for row in cur.fetchall()]


def upsert_current_price(
    cur,
    *,
    product_id: str,
    shop_id: str,
    candidate: RefreshCandidate,
    captured_at: datetime,
) -> tuple[bool, bool]:
    cur.execute(
        """
        select price, currency, product_url, availability
        from public.prices
        where product_id = %s
          and shop_id = %s
        limit 1
        """,
        (product_id, shop_id),
    )
    existing = cur.fetchone()

    inserted = existing is None
    changed = True

    if existing is not None:
        existing_price, existing_currency, existing_url, existing_availability = existing
        changed = any(
            [
                round(float(existing_price), 2) != round(float(candidate.price), 2),
                str(existing_currency) != CURRENCY,
                clean_text(existing_url) != candidate.product_url,
                clean_text(existing_availability) != candidate.availability,
            ]
        )

    cur.execute(
        """
        insert into public.prices (
          product_id,
          shop_id,
          price,
          currency,
          product_url,
          availability,
          first_seen_at,
          last_seen_at,
          is_active,
          created_at,
          updated_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, now(), true, now(), now())
        on conflict (product_id, shop_id)
        do update set
          price = excluded.price,
          currency = excluded.currency,
          product_url = excluded.product_url,
          availability = excluded.availability,
          last_seen_at = excluded.last_seen_at,
          is_active = true,
          updated_at = now()
        """,
        (
            product_id,
            shop_id,
            candidate.price,
            CURRENCY,
            candidate.product_url,
            candidate.availability,
            captured_at,
        ),
    )

    return inserted, changed


def maybe_insert_history(
    cur,
    *,
    product_id: str,
    shop_id: str,
    candidate: RefreshCandidate,
    captured_at: datetime,
) -> bool:
    cur.execute(
        """
        select price, availability, captured_at
        from public.price_history
        where product_id = %s
          and shop_id = %s
        order by captured_at desc
        limit 1
        """,
        (product_id, shop_id),
    )
    latest = cur.fetchone()

    if latest is not None:
        latest_price, latest_availability, latest_captured_at = latest
        if (
            latest_captured_at.date() == captured_at.date()
            and round(float(latest_price), 2) == round(float(candidate.price), 2)
            and clean_text(latest_availability) == candidate.availability
        ):
            return False

    cur.execute(
        """
        insert into public.price_history (
          product_id,
          shop_id,
          price,
          currency,
          availability,
          captured_at,
          created_at
        )
        values (%s, %s, %s, %s, %s, %s, now())
        """,
        (
            product_id,
            shop_id,
            candidate.price,
            CURRENCY,
            candidate.availability,
            captured_at,
        ),
    )
    return True


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if fieldnames is None:
        fieldnames = sorted({key for row in rows for key in row.keys()}) if rows else ["empty"]

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_snapshot(path: Path, candidates: list[RefreshCandidate]) -> None:
    rows = [
        {
            "handle": c.handle,
            "ean": c.ean,
            "gtin14": c.gtin14,
            "price_offer": f"{c.price:.2f}",
            "price_list": "" if c.price_list is None else f"{c.price_list:.2f}",
            "product_url": c.product_url,
            "availability": c.availability,
            "title": c.title,
        }
        for c in candidates
    ]
    write_csv(
        path,
        rows,
        [
            "handle",
            "ean",
            "gtin14",
            "price_offer",
            "price_list",
            "product_url",
            "availability",
            "title",
        ],
    )


def apply_to_database(
    candidates: list[RefreshCandidate],
    audit: list[dict[str, object]],
    *,
    dry_run: bool,
) -> dict[str, int]:
    stats = {
        "price_inserts": 0,
        "price_updates": 0,
        "unchanged": 0,
        "history_inserts": 0,
        "unmatched": 0,
    }

    if dry_run:
        print("[DRY RUN] database writes skipped", flush=True)
        return stats

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set")

    captured_at = now_utc()

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            shop_id = ensure_shop(cur)
            print(f"[DB] shop_id={shop_id}", flush=True)

            for index, candidate in enumerate(candidates, start=1):
                product_ids = find_product_ids(cur, candidate)

                if not product_ids:
                    stats["unmatched"] += 1
                    audit.append(
                        {
                            "reason": "no_matching_product",
                            "handle": candidate.handle,
                            "ean": candidate.ean,
                            "gtin14": candidate.gtin14,
                            "price": candidate.price,
                            "product_url": candidate.product_url,
                        }
                    )
                    continue

                for product_id in product_ids:
                    inserted, changed = upsert_current_price(
                        cur,
                        product_id=product_id,
                        shop_id=shop_id,
                        candidate=candidate,
                        captured_at=captured_at,
                    )

                    if inserted:
                        stats["price_inserts"] += 1
                    elif changed:
                        stats["price_updates"] += 1
                    else:
                        stats["unchanged"] += 1

                    if maybe_insert_history(
                        cur,
                        product_id=product_id,
                        shop_id=shop_id,
                        candidate=candidate,
                        captured_at=captured_at,
                    ):
                        stats["history_inserts"] += 1

                if index % 100 == 0:
                    print(
                        f"[DB PROGRESS] {index}/{len(candidates)} "
                        f"inserts={stats['price_inserts']} "
                        f"updates={stats['price_updates']} "
                        f"unchanged={stats['unchanged']} "
                        f"history={stats['history_inserts']} "
                        f"unmatched={stats['unmatched']}",
                        flush=True,
                    )

        conn.commit()

    return stats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-csv", required=True)
    parser.add_argument("--collection-url", default=DEFAULT_COLLECTION)
    parser.add_argument("--max-pages", type=int, default=0, help="0 = until exhausted")
    parser.add_argument("--limit-products", type=int, default=0, help="0 = no limit")
    parser.add_argument("--sleep", type=float, default=0.15)
    parser.add_argument("--snapshot", default="output/recordsonvinyl_master_only_refresh_snapshot.csv")
    parser.add_argument("--audit", default="output/recordsonvinyl_master_only_refresh_audit.csv")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv(".env.local", override=True)
    load_dotenv(override=True)

    master_by_handle = load_master_products(Path(args.master_csv))

    session = make_session()
    listing_by_handle = discover_listing_prices(
        session,
        collection_url=args.collection_url,
        max_pages=args.max_pages,
        limit_products=args.limit_products,
        sleep_seconds=args.sleep,
    )

    print(f"[DISCOVER] DONE listing_handles={len(listing_by_handle)}", flush=True)

    candidates, audit = build_candidates(master_by_handle, listing_by_handle)
    write_snapshot(Path(args.snapshot), candidates)

    stats = apply_to_database(candidates, audit, dry_run=args.dry_run)
    write_csv(Path(args.audit), audit)

    print(
        "[DONE] "
        f"master_handles={len(master_by_handle)} "
        f"listing_handles={len(listing_by_handle)} "
        f"candidates={len(candidates)} "
        f"audit_rows={len(audit)} "
        f"price_inserts={stats['price_inserts']} "
        f"price_updates={stats['price_updates']} "
        f"unchanged={stats['unchanged']} "
        f"history_inserts={stats['history_inserts']} "
        f"unmatched={stats['unmatched']} "
        f"snapshot={args.snapshot} "
        f"audit={args.audit}",
        flush=True,
    )


if __name__ == "__main__":
    main()
