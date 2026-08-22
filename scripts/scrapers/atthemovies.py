#!/usr/bin/env python3
"""Bounded, listing-first scraper for atthemoviesshop.com.

Collection cards are the only source for current price and availability. Product
JSON is used for EAN and metadata enrichment and never overwrites those fields.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import re
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, NavigableString


BASE_URL = "https://atthemoviesshop.com"
DEFAULT_COLLECTION_URL = f"{BASE_URL}/nl/collections/all-products"
DEFAULT_OUTPUT = Path("data/raw/atthemovies/atthemovies_products.csv")
USER_AGENT = "VinylofyBot/1.0 (+https://vinylofy.com)"
COLLECTION_JSON_PREFIX = "window.backInStock.productsInCollectionLiquidObject = "
CSV_COLUMNS = (
    "scraped_at",
    "product_url",
    "handle",
    "product_id",
    "variant_id",
    "sku",
    "product_type",
    "ean",
    "artist",
    "title",
    "format",
    "price",
    "standard_price",
    "currency",
    "availability",
    "detail_status",
)


class RateLimitError(RuntimeError):
    """The source asked the scraper to stop requesting detail pages."""


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl,en;q=0.8",
        }
    )
    return session


def fetch(session: requests.Session, url: str, *, retries: int = 2, timeout: int = 30) -> requests.Response:
    for attempt in range(retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code == 429:
                raise RateLimitError(f"HTTP 429 for {url}; scraper stopped safely")
            if response.status_code in {500, 502, 503, 504}:
                raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
            response.raise_for_status()
            return response
        except requests.RequestException:
            if attempt == retries:
                raise
            time.sleep((2**attempt) + random.random() * 0.25)
    raise RuntimeError("unreachable")


def parse_money(value: str | None) -> str:
    text = re.sub(r"[^0-9,.]", "", value or "").replace(".", "").replace(",", ".")
    return f"{float(text):.2f}" if text else ""


def product_handle(url: str) -> str:
    match = re.search(r"/products/([^/?#]+)", urlparse(url).path)
    return match.group(1) if match else ""


def extract_collection_products(html: str) -> list[dict[str, Any]]:
    start = html.find(COLLECTION_JSON_PREFIX)
    if start < 0:
        raise ValueError("collection product JSON not found")
    payload = html[start + len(COLLECTION_JSON_PREFIX) :]
    products, _ = json.JSONDecoder().raw_decode(payload)
    if not isinstance(products, list):
        raise ValueError("collection product JSON is not a list")
    return products


def _direct_text(element: Any) -> str:
    return " ".join(
        str(child).strip()
        for child in element.children
        if isinstance(child, NavigableString) and str(child).strip()
    )


def parse_listing_page(
    html: str,
    collection_url: str = DEFAULT_COLLECTION_URL,
) -> tuple[list[dict[str, str]], bool, set[str], Counter[str]]:
    soup = BeautifulSoup(html, "html.parser")
    products = {str(item.get("handle")): item for item in extract_collection_products(html)}
    scraped_at = now_utc_iso()
    rows: list[dict[str, str]] = []
    discovered_handles: set[str] = set()
    skips: Counter[str] = Counter()

    for card in soup.select("div.product-block"):
        title_link = card.select_one("div.product-block__title-price > a.title[href*='/products/']")
        if not title_link:
            skips["missing_product_link"] += 1
            continue
        url = urljoin(collection_url, str(title_link.get("href", "")))
        handle = product_handle(url)
        if not handle:
            skips["invalid_product_url"] += 1
            continue
        discovered_handles.add(handle)

        price_box = card.select_one("div.product-block__title-price > div.price")
        current_price = price_box.select_one(":scope > span.amount") if price_box else None
        if not current_price:
            skips["missing_listing_price"] += 1
            continue

        product = products.get(handle)
        if not product:
            skips["missing_product_json"] += 1
            continue
        if str(product.get("type") or "").strip().lower() != "vinyl":
            skips["non_vinyl"] += 1
            continue
        variants = product.get("variants") or []
        # A single card price cannot safely identify multiple variant offers.
        if len(variants) != 1:
            skips["multi_variant"] += 1
            continue
        variant = variants[0]
        badge_text = " ".join(item.get_text(" ", strip=True) for item in card.select("[class*='product-label']"))
        if "uitverkocht" in badge_text.lower() or variant.get("available") is False:
            availability = "out_of_stock"
        elif variant.get("available") is True:
            availability = "in_stock"
        else:
            availability = "unknown"
        standard = price_box.select_one("del") if price_box else None
        nested_title = title_link.select_one(":scope > div.artist")
        format_node = title_link.select_one(":scope > div.title")
        artist = _direct_text(title_link)

        rows.append(
            {
                "scraped_at": scraped_at,
                "product_url": f"{BASE_URL}/nl/products/{handle}",
                "handle": handle,
                "product_id": str(product.get("id") or ""),
                "variant_id": str(variant.get("id") or ""),
                "sku": str(variant.get("sku") or "").strip(),
                "product_type": str(product.get("type") or "").strip(),
                "ean": str(variant.get("barcode") or "").strip(),
                "artist": artist,
                "title": nested_title.get_text(" ", strip=True) if nested_title else str(product.get("title") or "").strip(),
                "format": format_node.get_text(" ", strip=True) if format_node else "Vinyl",
                "price": parse_money(current_price.get_text(" ", strip=True)),
                "standard_price": parse_money(standard.get_text(" ", strip=True)) if standard else "",
                "currency": "EUR",
                "availability": availability,
                "detail_status": "listing",
            }
        )

    next_link = soup.select_one("a[href*='page='][aria-label*='Next'], a[href*='page='][aria-label*='Volgende']")
    if next_link is None:
        current_page = 1
        canonical = soup.select_one("link[rel='canonical']")
        match = re.search(r"[?&]page=(\d+)", str(canonical.get("href", "")) if canonical else "")
        if match:
            current_page = int(match.group(1))
        next_link = soup.select_one(f"a[href*='page={current_page + 1}']")
    return rows, next_link is not None, discovered_handles, skips


def scrape_listings(session: requests.Session, collection_url: str, max_pages: int | None) -> list[dict[str, str]]:
    all_rows: list[dict[str, str]] = []
    seen_accepted_handles: set[str] = set()
    seen_listing_handles: set[str] = set()
    total_skips: Counter[str] = Counter()
    pages_scraped = 0
    page = 1
    while max_pages is None or page <= max_pages:
        separator = "&" if "?" in collection_url else "?"
        response = fetch(session, f"{collection_url}{separator}page={page}")
        rows, has_next, discovered_handles, skips = parse_listing_page(response.text, collection_url)
        pages_scraped += 1
        total_skips.update(skips)
        new_listing_handles = discovered_handles - seen_listing_handles
        if not discovered_handles or not new_listing_handles:
            break
        seen_listing_handles.update(discovered_handles)
        new_rows = [row for row in rows if row["handle"] not in seen_accepted_handles]
        all_rows.extend(new_rows)
        seen_accepted_handles.update(row["handle"] for row in new_rows)
        skip_text = " ".join(f"{reason}={count}" for reason, count in sorted(skips.items())) or "none"
        print(
            f"[LISTING] page={page} cards={len(discovered_handles)} accepted={len(rows)} "
            f"new={len(new_rows)} skips={skip_text}",
            flush=True,
        )
        if not has_next:
            break
        page += 1
        time.sleep(0.5)
    total_skip_text = " ".join(
        f"{reason}={count}" for reason, count in sorted(total_skips.items())
    ) or "none"
    print(
        f"[SUMMARY] pages={pages_scraped} accepted={len(all_rows)} skips={total_skip_text}",
        flush=True,
    )
    return all_rows


def enrich_details(
    session: requests.Session,
    rows: list[dict[str, str]],
    limit: int,
    checkpoint_path: Path | None = None,
) -> None:
    pending_rows = [row for row in rows if not row.get("ean", "").strip()]
    detail_rows = pending_rows[:limit]
    for index, row in enumerate(detail_rows, start=1):
        detail_url = f"{row['product_url']}.js"
        try:
            product = fetch(session, detail_url).json()
        except RateLimitError as exc:
            print(
                f"[DETAIL] rate-limited during detail enrichment; stopping remaining detail requests: {exc}",
                flush=True,
            )
            if checkpoint_path is not None:
                write_csv(rows, checkpoint_path)
            return
        variants = product.get("variants") or []
        variant = next((item for item in variants if str(item.get("id")) == row["variant_id"]), None)
        if variant is None and len(variants) == 1:
            variant = variants[0]
        if variant:
            row["ean"] = str(variant.get("barcode") or row["ean"]).strip()
            row["sku"] = str(variant.get("sku") or row["sku"]).strip()
            row["detail_status"] = "ok" if row["ean"] else "missing_ean"
        else:
            row["detail_status"] = "variant_not_found"
        # Deliberately do not assign detail price or availability here.
        print(f"[DETAIL] {index}/{len(detail_rows)} handle={row['handle']} status={row['detail_status']}", flush=True)
        if checkpoint_path is not None:
            write_csv(rows, checkpoint_path)
        if index < len(detail_rows):
            time.sleep(0.75)


def merge_cached_details(rows: list[dict[str, str]], output: Path) -> int:
    if not output.exists():
        return 0

    with output.open(encoding="utf-8", newline="") as handle:
        cached_rows = {
            row.get("handle", ""): row
            for row in csv.DictReader(handle)
            if row.get("handle")
        }

    restored = 0
    for row in rows:
        cached = cached_rows.get(row.get("handle", ""))
        if not cached or cached.get("detail_status") != "ok":
            continue
        row["ean"] = cached.get("ean") or row["ean"]
        row["sku"] = cached.get("sku") or row["sku"]
        row["detail_status"] = "ok"
        restored += 1
    return restored


def write_csv(rows: list[dict[str, str]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape At The Movies Shop listing and product details")
    parser.add_argument("--collection-url", default=DEFAULT_COLLECTION_URL)
    parser.add_argument("--max-pages", type=int, help="Bounded pilot/crawl limit; omit to follow pagination stop conditions")
    parser.add_argument("--detail-limit", type=int, default=0, help="Enrich at most this many listing rows")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.max_pages is not None and args.max_pages < 1:
        raise SystemExit("--max-pages must be at least 1")
    if args.detail_limit < 0:
        raise SystemExit("--detail-limit cannot be negative")
    session = make_session()
    rows = scrape_listings(session, args.collection_url, args.max_pages)
    restored = merge_cached_details(rows, args.output)
    if restored:
        print(f"[RESUME] restored_detail_rows={restored}", flush=True)
    enrich_details(session, rows, min(args.detail_limit, len(rows)), checkpoint_path=args.output)
    write_csv(rows, args.output)
    print(f"[DONE] rows={len(rows)} output={args.output}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
