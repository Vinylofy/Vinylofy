#!/usr/bin/env python3
"""
Standalone Records on Vinyl price refresh for Vinylofy.

Clean replacement for the fragile refresh/import/reconcile chain.

Flow:
1. Scrape Records on Vinyl listing pages.
2. Extract price strictly from the product card belonging to the product link.
3. Resolve EAN/GTIN from existing master CSV, fallback Shopify /products/{handle}.js.
4. Match existing Supabase products by EAN/GTIN.
5. Upsert public.prices directly.
6. Insert price_history only when the latest same-day row differs.

This script does NOT create products and does NOT use common.py or run_vinylofy_pipeline.py.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import psycopg
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


BASE_URL = "https://recordsonvinyl.nl"
COLLECTION_URL = "https://recordsonvinyl.nl/collections/all"
SHOP_DOMAIN = "recordsonvinyl.nl"
SHOP_NAME = "Records on Vinyl"
SHOP_COUNTRY = "NL"
CURRENCY = "EUR"
DEFAULT_TIMEOUT = 30
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0 Safari/537.36 VinylofyBot/1.0"
)


@dataclass
class ListingEntry:
    handle: str
    product_url: str
    price_offer: float
    price_list: float | None
    title: str
    availability: str


@dataclass
class RefreshRow:
    handle: str
    product_url: str
    ean: str
    gtin14: str
    price: float
    price_list: float | None
    title: str
    availability: str
    source: str


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def clean_text(value: object | None) -> str:
    value = "" if value is None else str(value)
    return re.sub(r"\s+", " ", value.replace("\u200e", " ")).strip()


def normalize_handle_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    parts = path.split("/")
    if "products" in parts:
        idx = parts.index("products")
        if idx + 1 < len(parts):
            return parts[idx + 1].strip()
    return parts[-1].strip()


def absolute_product_url(href: str) -> str:
    return urljoin(BASE_URL, href).split("?")[0].rstrip("/")


def parse_price_text(value: object | None) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    text = text.replace("€", "").replace("EUR", "").replace("\xa0", " ")
    text = re.sub(r"[^0-9,.\-]", "", text).strip()
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


def db_availability(value: object | None, price: float | None = None) -> str:
    text = clean_text(value).lower()
    compact = text.replace(" ", "").replace("-", "_")
    if compact in {"true", "1", "yes", "ja", "y", "available", "in_stock", "instock"}:
        return "in_stock"
    if compact in {"false", "0", "no", "nee", "n", "soldout", "sold_out", "out_of_stock", "outofstock"}:
        return "out_of_stock"
    if any(token in text for token in ("uitverkocht", "sold out", "out of stock")):
        return "out_of_stock"
    if any(token in text for token in ("op voorraad", "in stock", "preorder", "bestelbaar")):
        return "in_stock"
    if text in {"in_stock", "out_of_stock", "unknown"}:
        return text
    return "in_stock" if price is not None else "unknown"


def get_with_retry(session: requests.Session, url: str, *, tries: int = 4) -> requests.Response:
    last_exc: Exception | None = None
    for attempt in range(1, tries + 1):
        try:
            response = session.get(url, timeout=DEFAULT_TIMEOUT)
            if response.status_code == 200:
                return response
            if response.status_code in {429, 500, 502, 503, 504}:
                wait = min(20, 2 ** attempt) + random.random()
                print(f"[WARN] HTTP {response.status_code} attempt={attempt}/{tries} url={url} wait={wait:.1f}s", flush=True)
                time.sleep(wait)
                continue
            raise RuntimeError(f"HTTP {response.status_code} url={url}")
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            wait = min(20, 2 ** attempt) + random.random()
            print(f"[WARN] request failed attempt={attempt}/{tries} url={url} err={exc} wait={wait:.1f}s", flush=True)
            time.sleep(wait)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed request url={url}")


def parse_listing_card(anchor) -> ListingEntry | None:
    href = anchor.get("href") or ""
    if "/products/" not in href:
        return None
    product_url = absolute_product_url(href)
    handle = normalize_handle_from_url(product_url)
    if not handle:
        return None

    title_el = anchor.select_one(".grid-product__title")
    title = clean_text(title_el.get_text(" ", strip=True) if title_el else "")
    price_box = anchor.select_one(".grid-product__price")
    if not price_box:
        return None

    original_el = price_box.select_one(".grid-product__price--original")
    sale_el = price_box.select_one(".sale-price-emphasis")
    price_list = parse_price_text(original_el.get_text(" ", strip=True) if original_el else "")

    if sale_el:
        price_offer = parse_price_text(sale_el.get_text(" ", strip=True))
    else:
        price_box_copy = BeautifulSoup(str(price_box), "html.parser")
        for bad in price_box_copy.select(".visually-hidden, .flair-badge-layout, style, script"):
            bad.decompose()
        price_offer = parse_price_text(price_box_copy.get_text(" ", strip=True))

    if price_offer is None:
        return None
    return ListingEntry(handle, product_url, price_offer, price_list, title, "in_stock")


def discover_listing_entries(session: requests.Session, *, max_pages: int, limit_products: int, sleep_seconds: float) -> list[ListingEntry]:
    entries_by_handle: dict[str, ListingEntry] = {}
    page = 1
    while True:
        if max_pages and page > max_pages:
            break
        page_url = f"{COLLECTION_URL}?page={page}"
        try:
            response = get_with_retry(session, page_url)
        except Exception as exc:  # noqa: BLE001
            print(f"[WARN] stopping listing discovery at page={page} due to {exc}", flush=True)
            break
        soup = BeautifulSoup(response.text, "html.parser")
        anchors = soup.select('a.grid-product__link[href*="/products/"]')
        if not anchors:
            print(f"[DISCOVER] page={page} no product cards; stop", flush=True)
            break
        new_count = 0
        for anchor in anchors:
            entry = parse_listing_card(anchor)
            if not entry:
                continue
            if entry.handle not in entries_by_handle:
                entries_by_handle[entry.handle] = entry
                new_count += 1
            if limit_products and len(entries_by_handle) >= limit_products:
                break
        print(f"[DISCOVER] page={page} new={new_count} total={len(entries_by_handle)} url={page_url}", flush=True)
        if limit_products and len(entries_by_handle) >= limit_products:
            break
        if new_count == 0:
            break
        page += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return list(entries_by_handle.values())


def load_master_eans(master_csv: Path | None) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if not master_csv or not master_csv.exists():
        print("[MASTER] no master CSV available; will fallback to Shopify JSON", flush=True)
        return mapping
    with master_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            handle = clean_text(row.get("handle"))
            if not handle:
                url = clean_text(row.get("product_url") or row.get("url") or row.get("link"))
                handle = normalize_handle_from_url(url) if url else ""
            if not handle:
                continue
            ean = normalize_ean(row.get("ean13") or row.get("ean") or row.get("barcode") or row.get("gtin") or row.get("gtin_normalized"))
            if ean:
                mapping[handle] = ean
    print(f"[MASTER] loaded handle->ean mappings: {len(mapping)}", flush=True)
    return mapping


def fetch_shopify_ean(session: requests.Session, handle: str) -> str | None:
    url = f"{BASE_URL}/products/{handle}.js"
    try:
        response = get_with_retry(session, url, tries=3)
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN] could not fetch product json handle={handle} err={exc}", flush=True)
        return None
    try:
        data = response.json()
    except json.JSONDecodeError:
        print(f"[WARN] invalid product json handle={handle}", flush=True)
        return None
    for variant in data.get("variants") or []:
        ean = normalize_ean(variant.get("barcode"))
        if ean:
            return ean
    return None


def build_refresh_rows(session: requests.Session, entries: list[ListingEntry], master_eans: dict[str, str], *, sleep_seconds: float) -> tuple[dict[str, RefreshRow], list[dict[str, object]]]:
    rows_by_gtin: dict[str, RefreshRow] = {}
    audit: list[dict[str, object]] = []
    for i, entry in enumerate(entries, start=1):
        ean = normalize_ean(master_eans.get(entry.handle))
        source = "master"
        if not ean:
            ean = fetch_shopify_ean(session, entry.handle)
            source = "shopify_json"
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        if not ean:
            audit.append({"reason": "missing_ean", "handle": entry.handle, "product_url": entry.product_url, "price": entry.price_offer, "title": entry.title})
            continue
        gtin14 = normalize_gtin14(ean)
        if not gtin14:
            audit.append({"reason": "invalid_ean_after_normalize", "handle": entry.handle, "ean": ean, "product_url": entry.product_url, "price": entry.price_offer})
            continue
        row = RefreshRow(entry.handle, entry.product_url, ean, gtin14, entry.price_offer, entry.price_list, entry.title, db_availability(entry.availability, entry.price_offer), source)
        existing = rows_by_gtin.get(gtin14)
        if existing is None or row.price < existing.price:
            if existing is not None:
                audit.append({"reason": "duplicate_gtin_replaced_by_lower_price", "gtin14": gtin14, "old_price": existing.price, "old_url": existing.product_url, "new_price": row.price, "new_url": row.product_url})
            rows_by_gtin[gtin14] = row
        else:
            audit.append({"reason": "duplicate_gtin_loser", "gtin14": gtin14, "price": row.price, "product_url": row.product_url, "winner_price": existing.price, "winner_url": existing.product_url})
        if i % 100 == 0:
            print(f"[RESOLVE] {i}/{len(entries)} rows_by_gtin={len(rows_by_gtin)} audit={len(audit)}", flush=True)
    return rows_by_gtin, audit


def ensure_shop(cur) -> str:
    cur.execute("select id from public.shops where domain = %s limit 1", (SHOP_DOMAIN,))
    row = cur.fetchone()
    if row:
        return str(row[0])
    cur.execute("insert into public.shops (name, domain, country, is_active) values (%s, %s, %s, true) returning id", (SHOP_NAME, SHOP_DOMAIN, SHOP_COUNTRY))
    return str(cur.fetchone()[0])


def find_product_ids(cur, row: RefreshRow) -> list[str]:
    ean_candidates = list(dict.fromkeys(identifier_variants(row.ean) + identifier_variants(row.gtin14)))
    gtin_candidates = list(dict.fromkeys(identifier_variants(row.gtin14) + identifier_variants(row.ean)))
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
        (gtin_candidates, ean_candidates, row.gtin14, row.ean),
    )
    return [str(r[0]) for r in cur.fetchall()]


def upsert_price(cur, product_id: str, shop_id: str, row: RefreshRow, captured_at: datetime) -> tuple[bool, bool]:
    cur.execute("select price, currency, product_url, availability from public.prices where product_id = %s and shop_id = %s limit 1", (product_id, shop_id))
    existing = cur.fetchone()
    inserted = existing is None
    changed = True
    if existing is not None:
        existing_price, existing_currency, existing_url, existing_availability = existing
        changed = any([
            round(float(existing_price), 2) != round(float(row.price), 2),
            str(existing_currency) != CURRENCY,
            clean_text(existing_url) != row.product_url,
            clean_text(existing_availability) != row.availability,
        ])
    cur.execute(
        """
        insert into public.prices (
          product_id, shop_id, price, currency, product_url, availability,
          first_seen_at, last_seen_at, is_active, created_at, updated_at
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
        (product_id, shop_id, row.price, CURRENCY, row.product_url, row.availability, captured_at),
    )
    return inserted, changed


def maybe_insert_history(cur, product_id: str, shop_id: str, row: RefreshRow, captured_at: datetime) -> bool:
    cur.execute("select price, availability, captured_at from public.price_history where product_id = %s and shop_id = %s order by captured_at desc limit 1", (product_id, shop_id))
    latest = cur.fetchone()
    if latest is not None:
        latest_price, latest_availability, latest_captured_at = latest
        same_day = latest_captured_at.date() == captured_at.date()
        unchanged = round(float(latest_price), 2) == round(float(row.price), 2) and clean_text(latest_availability) == row.availability
        if same_day and unchanged:
            return False
    cur.execute(
        """
        insert into public.price_history (product_id, shop_id, price, currency, availability, captured_at, created_at)
        values (%s, %s, %s, %s, %s, %s, now())
        """,
        (product_id, shop_id, row.price, CURRENCY, row.availability, captured_at),
    )
    return True


def write_audit(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_snapshot(path: Path, rows: Iterable[RefreshRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["handle", "product_url", "ean", "gtin14", "price", "price_list", "title", "availability", "source"]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                "handle": row.handle,
                "product_url": row.product_url,
                "ean": row.ean,
                "gtin14": row.gtin14,
                "price": f"{row.price:.2f}",
                "price_list": "" if row.price_list is None else f"{row.price_list:.2f}",
                "title": row.title,
                "availability": row.availability,
                "source": row.source,
            })


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--master-csv", default="", help="Optional recordsonvinyl_master.csv for handle->EAN mapping")
    parser.add_argument("--out", default="output/recordsonvinyl_direct_refresh_audit.csv")
    parser.add_argument("--snapshot", default="output/recordsonvinyl_direct_refresh_snapshot.csv")
    parser.add_argument("--max-pages", type=int, default=0, help="0 = until exhausted")
    parser.add_argument("--limit-products", type=int, default=0, help="0 = no limit")
    parser.add_argument("--sleep", type=float, default=0.15)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv(".env.local", override=True)
    load_dotenv(override=True)

    db_url = os.getenv("DATABASE_URL")
    if not db_url and not args.dry_run:
        raise SystemExit("DATABASE_URL is not set")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept": "text/html,application/json"})

    master_eans = load_master_eans(Path(args.master_csv) if args.master_csv else None)
    entries = discover_listing_entries(session, max_pages=args.max_pages, limit_products=args.limit_products, sleep_seconds=args.sleep)
    print(f"[DISCOVER] DONE entries={len(entries)}", flush=True)

    rows_by_gtin, audit = build_refresh_rows(session, entries, master_eans, sleep_seconds=args.sleep)
    rows = list(rows_by_gtin.values())
    print(f"[RESOLVE] DONE rows_by_gtin={len(rows)} audit_rows={len(audit)}", flush=True)
    write_snapshot(Path(args.snapshot), rows)

    if args.dry_run:
        print("[DRY RUN] No database writes", flush=True)
        write_audit(Path(args.out), audit)
        return

    captured_at = now_utc()
    price_inserts = price_updates = unchanged = history_inserts = unmatched = 0
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            shop_id = ensure_shop(cur)
            print(f"[DB] shop_id={shop_id}", flush=True)
            for idx, row in enumerate(rows, start=1):
                product_ids = find_product_ids(cur, row)
                if not product_ids:
                    unmatched += 1
                    audit.append({"reason": "no_matching_product", "handle": row.handle, "ean": row.ean, "gtin14": row.gtin14, "price": row.price, "product_url": row.product_url, "title": row.title})
                    continue
                for product_id in product_ids:
                    inserted, changed = upsert_price(cur, product_id, shop_id, row, captured_at)
                    if inserted:
                        price_inserts += 1
                    elif changed:
                        price_updates += 1
                    else:
                        unchanged += 1
                    if maybe_insert_history(cur, product_id, shop_id, row, captured_at):
                        history_inserts += 1
                if idx % 100 == 0:
                    print(f"[DB PROGRESS] {idx}/{len(rows)} inserts={price_inserts} updates={price_updates} unchanged={unchanged} history={history_inserts} unmatched={unmatched}", flush=True)
        conn.commit()

    write_audit(Path(args.out), audit)
    print(
        "[DONE] "
        f"entries={len(entries)} rows={len(rows)} "
        f"price_inserts={price_inserts} price_updates={price_updates} unchanged={unchanged} "
        f"history_inserts={history_inserts} unmatched={unmatched} "
        f"audit={args.out} snapshot={args.snapshot}",
        flush=True,
    )


if __name__ == "__main__":
    main()
