#!/usr/bin/env python3
"""Bounded, listing-first scraper for VIP Records vinyl.

VIP Records is a server-rendered CCV/Vertoshop shop.  The category listing is
authoritative for discovery, current price and availability.  Detail pages
are used only for GTIN/EAN and metadata enrichment.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.importers.common import strict_normalize_gtin
from scripts.scrapers._rotation import load_rotation_state, save_rotation_state, select_priority_then_round_robin


BASE_URL = "https://www.viprecords.nl"
LISTING_URL = f"{BASE_URL}/vinyl"
SHOP_NAME = "VIP Records"
SHOP_DOMAIN = "viprecords.nl"
DEFAULT_DELAY_SECONDS = 0.5
REQUEST_TIMEOUT = 30

LISTING_FIELDS = (
    "source_shop",
    "product_id",
    "product_key",
    "artist",
    "title",
    "format",
    "price",
    "standard_price",
    "is_sale",
    "availability",
    "product_url",
    "image_url",
    "page_found",
    "scraped_at",
)

MASTER_FIELDS = LISTING_FIELDS + (
    "ean",
    "detail_product_number",
    "detail_title",
    "detail_description",
    "standard_delivery_time",
    "detail_availability_observed",
    "detail_status",
    "detail_error",
    "enriched_at",
)

DETAIL_FIELDS = MASTER_FIELDS[len(LISTING_FIELDS) :]

SELECTORS = {
    "product_cards": (".cs-product",),
    "product_action": (".hook_ShowProduct[data-product-id]",),
    "title": (".cs-product__title",),
    "image": (".cs-product__img[data-src]", ".cs-product__img[src]"),
    "prices": (".cs-product__prices",),
    "current_price": (".price1",),
    "standard_price": (".from__price1", ".price__from"),
    "sale_marker": (".discount__perc", ".discount__text"),
    "cart_action": (".hook_AddProductToCart",),
    "next_page": (
        ".page__small-pagination [data-page-number]",
        ".page__pagination [data-page-number]",
    ),
}

OUT_OF_STOCK_MARKERS = (
    "uitverkocht",
    "niet beschikbaar",
    "niet leverbaar",
    "niet op voorraad",
    "niet uit voorraad",
    "out of stock",
    "sold out",
)
PREORDER_MARKERS = ("pre-order", "preorder", "voorbestelling")
FORMAT_SUFFIX_RE = re.compile(r"\s+\((?P<format>(?:\d+\s*)?LP|EP|VINYL|7\"|10\"|12\")\)\s*$", re.I)


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def normalize_url(value: str | None) -> str:
    if not value:
        return ""
    absolute = urljoin(BASE_URL, value)
    parsed = urlsplit(absolute)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {
        "www.viprecords.nl",
        "viprecords.nl",
    }:
        return ""
    path = re.sub(r"/{2,}", "/", parsed.path).rstrip("/") or "/"
    return urlunsplit(("https", "www.viprecords.nl", path, "", ""))


def product_id_from_card(card: Tag) -> str:
    for selector in SELECTORS["product_action"]:
        node = card.select_one(selector)
        if node:
            value = clean_text(node.get("data-product-id"))
            if value:
                return value
    return ""


def parse_money(value: str | None) -> str:
    text = clean_text(value).replace("€", "").replace("EUR", "")
    text = text.replace(" ", "")
    match = re.search(r"\d{1,3}(?:\.\d{3})*(?:,\d{1,2})?|\d+(?:[.,]\d{1,2})?", text)
    if not match:
        return ""
    number = match.group(0)
    if "," in number and "." in number:
        number = number.replace(".", "").replace(",", ".")
    else:
        number = number.replace(",", ".")
    try:
        return f"{float(number):.2f}"
    except ValueError:
        return ""


def _first_price(root: Tag | None, selectors: Iterable[str]) -> str:
    if root is None:
        return ""
    for selector in selectors:
        node = root.select_one(selector)
        if node:
            price = parse_money(node.get_text("", strip=True))
            if price:
                return price
    return ""


def extract_listing_prices(card: Tag) -> tuple[str, str]:
    prices = card.select_one(SELECTORS["prices"][0])
    standard = _first_price(prices, SELECTORS["standard_price"])
    current = _first_price(prices, SELECTORS["current_price"])
    if not current and prices is not None:
        # Defensive fallback for a regular-price card that omits .price1.
        copy = BeautifulSoup(str(prices), "html.parser")
        for node in copy.select(",".join(SELECTORS["standard_price"] + SELECTORS["sale_marker"])):
            node.decompose()
        current = parse_money(copy.get_text("", strip=True))
    return current, standard


def extract_availability(card: Tag) -> str:
    text = clean_text(card.get_text(" ", strip=True)).casefold()
    if any(marker in text for marker in OUT_OF_STOCK_MARKERS):
        return "out_of_stock"
    if any(marker in text for marker in PREORDER_MARKERS):
        return "preorder"
    for selector in SELECTORS["cart_action"]:
        for node in card.select(selector):
            classes = " ".join(node.get("class", [])).casefold()
            if not node.has_attr("disabled") and node.get("aria-disabled") != "true" and "disabled" not in classes:
                return "in_stock"
    return "unknown"


def parse_title(value: str) -> tuple[str, str, str]:
    raw = clean_text(value)
    format_match = FORMAT_SUFFIX_RE.search(raw)
    format_label = format_match.group("format").upper() if format_match else "Vinyl"
    without_format = clean_text(raw[: format_match.start()]) if format_match else raw
    if " - " in without_format:
        artist, title = (clean_text(part) for part in without_format.split(" - ", 1))
    else:
        artist, title = "", without_format
    return artist, title, format_label


def next_page_number(soup: BeautifulSoup, current_page: int) -> int | None:
    candidates: set[int] = set()
    for selector in SELECTORS["next_page"]:
        for node in soup.select(selector):
            try:
                candidate = int(clean_text(node.get("data-page-number")))
            except (TypeError, ValueError):
                continue
            if candidate > current_page:
                candidates.add(candidate)
    expected = current_page + 1
    return expected if expected in candidates else None


def parse_listing_page(html: str, page: int) -> tuple[list[dict[str, str]], int | None, Counter[str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []
    skips: Counter[str] = Counter()
    seen_ids: set[str] = set()
    scraped_at = now_utc_iso()

    for card in soup.select(SELECTORS["product_cards"][0]):
        product_id = product_id_from_card(card)
        if not product_id:
            skips["missing_product_id"] += 1
            continue
        if product_id in seen_ids:
            skips["duplicate_product_id_on_page"] += 1
            continue
        title_node = card.select_one(SELECTORS["title"][0])
        action_node = card.select_one(SELECTORS["product_action"][0])
        title_raw = clean_text(title_node.get_text(" ", strip=True)) if title_node else ""
        product_url = normalize_url(title_node.get("href") if title_node else action_node.get("href") if action_node else "")
        price, standard_price = extract_listing_prices(card)
        if not product_url:
            skips["missing_product_url"] += 1
            continue
        if not title_raw:
            skips["missing_title"] += 1
            continue
        if not price:
            skips["missing_listing_price"] += 1
            continue
        artist, title, format_label = parse_title(title_raw)
        image_url = ""
        for selector in SELECTORS["image"]:
            image = card.select_one(selector)
            if image:
                image_url = normalize_url(image.get("data-src") or image.get("src"))
                if image_url:
                    break
        seen_ids.add(product_id)
        rows.append(
            {
                "source_shop": SHOP_NAME,
                "product_id": product_id,
                "product_key": f"viprecords:{product_id}",
                "artist": artist,
                "title": title,
                "format": format_label,
                "price": price,
                "standard_price": standard_price,
                "is_sale": "true" if bool(standard_price or card.select_one(",".join(SELECTORS["sale_marker"]))) else "false",
                "availability": extract_availability(card),
                "product_url": product_url,
                "image_url": image_url,
                "page_found": str(page),
                "scraped_at": scraped_at,
                "ean": "",
                "detail_product_number": "",
                "detail_title": "",
                "detail_description": "",
                "standard_delivery_time": "",
                "detail_availability_observed": "",
                "detail_status": "pending",
                "detail_error": "",
                "enriched_at": "",
            }
        )
    return rows, next_page_number(soup, page), skips


def build_session() -> requests.Session:
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "VinylofyVIPRecords/1.0 (+https://vinylofy.com)",
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        }
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class RateLimitedClient:
    def __init__(self, session: requests.Session, delay_seconds: float = DEFAULT_DELAY_SECONDS):
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        self.session = session
        self.delay_seconds = delay_seconds
        self._last_request_at: float | None = None

    def get(self, url: str) -> str:
        if self._last_request_at is not None:
            remaining = self.delay_seconds - (time.monotonic() - self._last_request_at)
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_at = time.monotonic()
        response = self.session.get(url, timeout=REQUEST_TIMEOUT)
        if response.status_code == 429:
            raise RuntimeError(f"VIP Records rate limit reached (HTTP 429): {url}")
        if response.status_code >= 400:
            raise RuntimeError(f"VIP Records HTTP {response.status_code}: {url}")
        return response.text


def scrape_listings(client: RateLimitedClient, max_pages: int = 1) -> tuple[list[dict[str, str]], Counter[str], int]:
    if max_pages < 0:
        raise ValueError("max_pages must be >= 0; 0 means stop-condition bounded")
    rows: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    seen_page_sets: set[frozenset[str]] = set()
    skips: Counter[str] = Counter()
    page = 1
    pages = 0
    while max_pages == 0 or pages < max_pages:
        url = LISTING_URL if page == 1 else f"{LISTING_URL}?page={page}"
        page_rows, next_page, page_skips = parse_listing_page(client.get(url), page)
        skips.update(page_skips)
        if not page_rows:
            skips["empty_product_page"] += 1
            break
        page_ids = frozenset(row["product_id"] for row in page_rows)
        if page_ids in seen_page_sets:
            skips["repeated_product_id_set"] += 1
            break
        seen_page_sets.add(page_ids)
        pages += 1
        for row in page_rows:
            product_id = row["product_id"]
            if product_id in seen_ids:
                skips["duplicate_product_id_between_pages"] += 1
                continue
            seen_ids.add(product_id)
            rows.append(row)
        print(f"[PAGE {page}] cards={len(page_rows)} new={len(rows)} next={next_page or '-'}", flush=True)
        if next_page is None:
            skips["no_valid_next_page"] += 1
            break
        page = next_page
    return rows, skips, pages


def _jsonld_gtin_candidates(soup: BeautifulSoup) -> list[str]:
    values: list[str] = []
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            payload = json.loads(script.string or script.get_text())
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        objects = payload if isinstance(payload, list) else [payload]
        for obj in objects:
            if not isinstance(obj, dict):
                continue
            for key in ("gtin8", "gtin12", "gtin13", "gtin14", "productID", "sku"):
                if obj.get(key):
                    values.append(str(obj[key]))
    return values


def _first_valid_gtin(values: Iterable[str]) -> str:
    for value in values:
        if strict_normalize_gtin(value):
            return re.sub(r"\D", "", value)
    return ""


def parse_detail_page(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    full_text = clean_text(soup.get_text(" ", strip=True))
    candidates = _jsonld_gtin_candidates(soup)
    candidates.extend(re.findall(r"(?:Artikelnummer|product_number|productnumber|ean_number)\s*:?\s*[\"']?([0-9]{8,14})", html, re.I))
    ean = _first_valid_gtin(candidates)

    title_node = soup.select_one(".product-title, .content-page-title.product-title")
    short_description = soup.select_one(".page__product__short-description")
    delivery_match = re.search(r"Standaard levertijd\s*:?\s*([^€]+?)(?=\s+€|$)", full_text, re.I)
    detail_status = "ok" if ean else "missing_ean"
    lowered = full_text.casefold()
    if any(marker in lowered for marker in OUT_OF_STOCK_MARKERS):
        detail_availability = "out_of_stock"
    elif "bestellen" in lowered:
        detail_availability = "in_stock"
    else:
        detail_availability = "unknown"
    return {
        "ean": ean,
        "detail_product_number": ean,
        "detail_title": clean_text(title_node.get_text(" ", strip=True)) if title_node else "",
        "detail_description": clean_text(short_description.get_text(" ", strip=True)) if short_description else "",
        "standard_delivery_time": clean_text(delivery_match.group(1)) if delivery_match else "",
        "detail_availability_observed": detail_availability,
        "detail_status": detail_status,
    }


def merge_listing_rows_with_previous(
    rows: list[dict[str, str]], previous_rows: Iterable[dict[str, str]]
) -> list[dict[str, str]]:
    """Keep detail enrichment while allowing the listing to remain authoritative."""
    previous_by_id = {
        row.get("product_id", ""): row
        for row in previous_rows
        if row.get("product_id")
    }
    for row in rows:
        previous = previous_by_id.get(row.get("product_id", ""))
        if not previous:
            continue
        for field in DETAIL_FIELDS:
            if field in previous:
                row[field] = previous[field]
    return rows


def _detail_candidates(rows: Iterable[dict[str, str]]) -> list[dict[str, str]]:
    return [
        row
        for row in rows
        if not strict_normalize_gtin(row.get("ean", "")) or row.get("detail_status") != "ok"
    ]


def select_detail_batch(
    rows: list[dict[str, str]], limit: int | None, state: dict[str, object]
) -> list[dict[str, str]]:
    """Select unresolved detail records in a resumable, bounded round-robin order."""
    return select_priority_then_round_robin(
        _detail_candidates(rows),
        [],
        limit,
        state,
        "viprecords_detail_cursor",
        "viprecords_detail_refresh_cursor",
    )


def enrich_details(
    client: RateLimitedClient,
    rows: list[dict[str, str]],
    limit: int | None,
    state: dict[str, object] | None = None,
) -> int:
    if limit is not None and limit < 0:
        raise ValueError("detail limit must be >= 0")
    attempted = 0
    targets = select_detail_batch(rows, limit, state) if state is not None else rows
    for row in targets:
        if limit is not None and attempted >= limit:
            break
        if strict_normalize_gtin(row.get("ean", "")) and row.get("detail_status") == "ok":
            continue
        attempted += 1
        try:
            details = parse_detail_page(client.get(row["product_url"]))
        except (requests.RequestException, RuntimeError) as exc:
            row["detail_status"] = "technical_error"
            row["detail_error"] = str(exc)
        else:
            # Listing price, standard_price, is_sale and availability deliberately remain untouched.
            row.update(details)
            row["enriched_at"] = now_utc_iso()
        print(f"[DETAIL {attempted}] id={row.get('product_id')} status={row.get('detail_status')}", flush=True)
    return attempted


def parse_detail_limit(value: str) -> int | None:
    normalized = clean_text(value).casefold()
    if normalized in {"all", "full"}:
        return None
    try:
        limit = int(normalized)
    except ValueError as exc:
        raise ValueError("detail-limit must be a non-negative integer or 'all'") from exc
    if limit < 0:
        raise ValueError("detail-limit must be >= 0")
    return limit


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, rows: Iterable[dict[str, str]], fields: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bounded listing-first VIP Records vinyl scraper")
    parser.add_argument("--mode", choices=("listing", "detail", "both"), default="both")
    parser.add_argument("--max-pages", type=int, default=1, help="Listing page limit; 0 uses stop conditions")
    parser.add_argument("--detail-limit", default="3", help="Detail limit; use 'all' for every discovered product")
    parser.add_argument("--delay-seconds", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw/viprecords"))
    parser.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help="Persistent detail rotation state; defaults to the output directory",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.max_pages < 0:
        raise SystemExit("max-pages mag niet negatief zijn")
    try:
        detail_limit = parse_detail_limit(args.detail_limit)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    output_dir = args.output_dir
    listing_path = output_dir / "viprecords_listings.csv"
    master_path = output_dir / "viprecords_master.csv"
    state_path = args.state_file or output_dir / "viprecords_detail_rotation_state.json"
    client = RateLimitedClient(build_session(), args.delay_seconds)

    if args.mode in {"listing", "both"}:
        previous_rows = read_rows(master_path) if master_path.exists() else []
        rows, skips, pages = scrape_listings(client, args.max_pages)
        merge_listing_rows_with_previous(rows, previous_rows)
        write_rows(listing_path, rows, LISTING_FIELDS)
        write_rows(master_path, rows, MASTER_FIELDS)
        print(f"[LISTING] pages={pages} unique_products={len(rows)} skips={dict(skips)}", flush=True)
    else:
        if not master_path.exists():
            raise SystemExit(f"Master CSV not found for detail mode: {master_path}")
        rows = read_rows(master_path)

    if args.mode in {"detail", "both"}:
        rotation_state = load_rotation_state(state_path)
        attempted = enrich_details(client, rows, detail_limit, rotation_state)
        write_rows(master_path, rows, MASTER_FIELDS)
        save_rotation_state(state_path, rotation_state)
        print(f"[DETAIL] attempted={attempted} ean_hits={sum(bool(row.get('ean')) for row in rows)}", flush=True)
    print(f"[OUTPUT] listing={listing_path} master={master_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
