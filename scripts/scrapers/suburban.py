#!/usr/bin/env python3
"""Listing-first scraper for Suburban Records vinyl.

The collection page is intentionally authoritative for current price and
availability. Product pages are only used to enrich the listing with a
barcode and descriptive fields. This module never writes to the database;
the CSV it produces is consumed by ``import_suburban``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://suburban.nl"
COLLECTION_URL = f"{BASE_URL}/shop/swoof/media-format-vinyl/instock/"
SHOP_NAME = "Suburban"
SHOP_DOMAIN = "suburban.nl"

LISTING_FIELDS = (
    "source_shop",
    "product_key",
    "artist",
    "title",
    "price",
    "standard_price",
    "currency",
    "availability",
    "product_url",
    "page_found",
    "scraped_at",
    "image_url",
    "format",
)

MASTER_FIELDS = (
    "source_shop",
    "product_key",
    "artist",
    "title",
    "price",
    "standard_price",
    "currency",
    "availability",
    "product_url",
    "page_found",
    "scraped_at",
    "image_url",
    "format",
    "ean",
    "release_date",
    "detail_status",
    "enriched_at",
)

HEADERS = {
    "User-Agent": "Vinylofy Suburban scraper/1.0 (+https://vinylofy.nl)",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_product_url(value: str | None) -> str:
    if not value:
        return ""
    absolute = urljoin(BASE_URL, value)
    parsed = urlparse(absolute)
    if parsed.netloc.lower() != "suburban.nl":
        return ""
    return urlunparse(("https", "suburban.nl", parsed.path.rstrip("/") + "/", "", "", ""))


def product_key_from_url(product_url: str) -> str:
    return hashlib.sha256(product_url.encode("utf-8")).hexdigest()[:24]


def parse_price(value: str | None) -> str:
    cleaned = normalize_space(value).replace("€", "").replace("EUR", "")
    cleaned = cleaned.replace("\xa0", "").replace(" ", "")
    match = re.search(r"\d+(?:[.,]\d{1,2})?", cleaned)
    if not match:
        return ""
    number = match.group(0).replace(",", ".")
    return f"{float(number):.2f}"


def _price_amounts(container: Tag | None) -> list[tuple[str, bool]]:
    if container is None:
        return []
    amounts: list[tuple[str, bool]] = []
    for amount in container.select(".woocommerce-Price-amount, .amount"):
        if not isinstance(amount, Tag):
            continue
        value = parse_price(amount.get_text(" ", strip=True))
        if not value:
            continue
        amounts.append((value, bool(amount.find_parent(["del", "s"]))))
    if not amounts:
        return [(parse_price(container.get_text(" ", strip=True)), False)]
    return amounts


def extract_listing_prices(tile: Tag) -> tuple[str, str]:
    """Return (current_price, strike_through_standard_price).

    WooCommerce may render a sale as ``del`` + ``ins`` or as a regular amount
    next to a ``del`` amount. A strike-through is never allowed to become the
    current Vinylofy price.
    """
    price_block = tile.select_one(".product-tile__price") or tile.select_one(".price")
    if price_block is None:
        return "", ""

    standard = ""
    for tag in price_block.select("del, s"):
        standard = parse_price(tag.get_text(" ", strip=True)) or standard

    ins = price_block.select_one("ins")
    if ins is not None:
        current = parse_price(ins.get_text(" ", strip=True))
    else:
        amounts = _price_amounts(price_block)
        non_struck = [value for value, struck in amounts if value and not struck]
        current = non_struck[-1] if non_struck else ""

    if not standard:
        struck = [value for value, struck in _price_amounts(price_block) if struck]
        standard = struck[-1] if struck else ""
    return current, standard


def extract_availability(tile: Tag) -> str:
    container = tile.find_parent("li") or tile
    classes = " ".join(container.get("class", []))
    text = normalize_space(container.get_text(" ", strip=True)).casefold()
    if any(marker in classes.casefold() for marker in ("outofstock", "out-of-stock")):
        return "out_of_stock"
    if any(marker in text for marker in ("uitverkocht", "out of stock", "niet op voorraad")):
        return "out_of_stock"
    if "instock" in classes.casefold() or "op voorraad" in text:
        return "in_stock"
    return "unknown"


def extract_image_url(tile: Tag) -> str:
    image = tile.select_one("img")
    if image is None:
        return ""
    return normalize_space(
        image.get("data-src") or image.get("data-lazy-src") or image.get("src")
    )


def parse_listing_page(html: str, page: int = 1) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for tile in soup.select(".product-tile"):
        link = tile.select_one("a[href*='/product/']")
        product_url = normalize_product_url(link.get("href") if link else None)
        if not product_url or product_url in seen_urls:
            continue

        title_node = tile.select_one(".product-tile__title .title")
        artist_node = tile.select_one(".product-tile__title .artist")
        format_node = tile.select_one(".product-tile__format_price .label")
        if format_node is None:
            format_node = tile.select_one(".product-tile__format_price .product-tile__artist")
        current_price, standard_price = extract_listing_prices(tile)
        if not current_price:
            continue

        seen_urls.add(product_url)
        rows.append(
            {
                "source_shop": SHOP_NAME,
                "product_key": product_key_from_url(product_url),
                "artist": normalize_space(artist_node.get_text(" ", strip=True) if artist_node else ""),
                "title": normalize_space(title_node.get_text(" ", strip=True) if title_node else ""),
                "price": current_price,
                "standard_price": standard_price,
                "currency": "EUR",
                "availability": extract_availability(tile),
                "product_url": product_url,
                "page_found": str(page),
                "scraped_at": utc_now_iso(),
                "image_url": extract_image_url(tile),
                "format": normalize_space(format_node.get_text(" ", strip=True) if format_node else ""),
                "detail_status": "pending",
            }
        )
    return rows


def listing_page_url(page: int) -> str:
    return COLLECTION_URL if page == 1 else f"{COLLECTION_URL}?product-page={page}"


def fetch_html(session: requests.Session, url: str, timeout: int = 30) -> str:
    response = session.get(url, timeout=timeout)
    if response.status_code == 429:
        raise RuntimeError(f"Suburban rate limit reached (HTTP 429): {url}")
    if response.status_code >= 400:
        raise RuntimeError(f"Suburban HTTP {response.status_code}: {url}")
    return response.text


def scrape_listings(
    session: requests.Session,
    max_pages: int = 3,
    delay_seconds: float = 0.25,
) -> list[dict[str, str]]:
    if max_pages < 0:
        raise ValueError("max_pages must be >= 0")
    rows: list[dict[str, str]] = []
    seen_urls: set[str] = set()
    page = 1
    while max_pages == 0 or page <= max_pages:
        page_rows = parse_listing_page(fetch_html(session, listing_page_url(page)), page)
        if not page_rows:
            break
        new_rows = [row for row in page_rows if row["product_url"] not in seen_urls]
        if not new_rows:
            break
        rows.extend(new_rows)
        seen_urls.update(row["product_url"] for row in new_rows)
        page += 1
        if delay_seconds > 0 and (max_pages == 0 or page <= max_pages):
            time.sleep(delay_seconds)
    return rows


def extract_detail_page(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    ean = ""
    barcode = soup.select_one(".product-barcode__value")
    if barcode is not None:
        ean = normalize_space(barcode.get_text(" ", strip=True))
    if not ean:
        for node in soup.select("[itemprop^='gtin']"):
            candidate = normalize_space(node.get("content") or node.get_text(" ", strip=True))
            if candidate:
                ean = candidate
                break

    stock = soup.select_one(".product-info .stock, .summary .stock")
    stock_text = normalize_space(stock.get_text(" ", strip=True) if stock else "")
    release = ""
    release_match = re.search(r"(?:Release|Released)\s*:?\s*(\d{2}-\d{2}-\d{4})", soup.get_text(" ", strip=True), re.I)
    if release_match:
        release = release_match.group(1)
    format_node = soup.select_one(".product-info .release .label, .product-info .product-tile__artist.label")
    image = soup.select_one("meta[property='og:image'], meta[name='og:image']")
    return {
        "ean": ean,
        "detail_status": "ok" if ean else "missing_ean",
        "detail_availability_observed": stock_text,
        "release_date": release,
        "format": normalize_space(format_node.get_text(" ", strip=True) if format_node else ""),
        "image_url": normalize_space(image.get("content") if image else ""),
    }


def enrich_details(
    session: requests.Session,
    rows: list[dict[str, str]],
    limit: int,
    delay_seconds: float = 0.25,
) -> int:
    if limit < 0:
        raise ValueError("limit must be >= 0")
    enriched = 0
    for row in rows:
        if enriched >= limit:
            break
        if row.get("ean") and row.get("detail_status") == "ok":
            continue
        try:
            details = extract_detail_page(fetch_html(session, row["product_url"]))
        except (requests.RequestException, RuntimeError) as exc:
            row["detail_status"] = "technical_error"
            row["detail_error"] = str(exc)
        else:
            # Detail never changes listing-authoritative price or availability.
            row.update(details)
            row["enriched_at"] = utc_now_iso()
        enriched += 1
        if delay_seconds > 0 and enriched < limit:
            time.sleep(delay_seconds)
    return enriched


def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, rows: list[dict[str, str]], fields: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows({field: row.get(field, "") for field in fields} for row in rows)


def merge_existing_details(rows: list[dict[str, str]], existing: list[dict[str, str]]) -> None:
    old_by_url = {row.get("product_url", ""): row for row in existing if row.get("product_url")}
    detail_fields = ("ean", "release_date", "detail_status", "enriched_at")
    for row in rows:
        old = old_by_url.get(row.get("product_url", ""))
        if not old:
            continue
        for field in detail_fields:
            if old.get(field):
                row[field] = old[field]
        if old.get("format") and not row.get("format"):
            row["format"] = old["format"]
        if old.get("image_url") and not row.get("image_url"):
            row["image_url"] = old["image_url"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Listing-first Suburban vinyl scraper")
    parser.add_argument("--mode", choices=("listing", "detail", "both"), default="both")
    parser.add_argument("--max-pages", type=int, default=3, help="1-3 for a pilot; 0 scans until no new URLs")
    parser.add_argument("--detail-limit", type=int, default=5)
    parser.add_argument("--delay-seconds", type=float, default=0.25)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw/suburban"))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.max_pages < 0 or args.detail_limit < 0 or args.delay_seconds < 0:
        raise SystemExit("max-pages, detail-limit and delay-seconds mogen niet negatief zijn")

    output_dir = args.output_dir
    listing_path = output_dir / "suburban_listing.csv"
    master_path = output_dir / "suburban_master.csv"
    session = build_session()

    if args.mode in ("listing", "both"):
        rows = scrape_listings(session, args.max_pages, args.delay_seconds)
        if not rows:
            raise SystemExit("Suburban listing leverde geen producten op")
        merge_existing_details(rows, read_rows(master_path))
        write_rows(listing_path, rows, LISTING_FIELDS)
        write_rows(master_path, rows, MASTER_FIELDS)
        print(f"[SUBURBAN] listing pages<={args.max_pages} products={len(rows)}")
    else:
        rows = read_rows(master_path)
        if not rows:
            rows = read_rows(listing_path)
        if not rows:
            raise SystemExit(f"Geen listing/master CSV gevonden in {output_dir}")

    if args.mode in ("detail", "both"):
        count = enrich_details(session, rows, args.detail_limit, args.delay_seconds)
        write_rows(master_path, rows, MASTER_FIELDS)
        print(
            f"[SUBURBAN] detail attempted={count} "
            f"with_ean={sum(bool(row.get('ean')) for row in rows)} "
            f"pending={sum(row.get('detail_status') == 'pending' for row in rows)} "
            f"master_rows={len(rows)}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
