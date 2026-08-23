#!/usr/bin/env python3
"""Bounded listing-first scraper for Get Back Music's LP/Vinyl collection."""

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
from typing import Iterable
from urllib.parse import parse_qs, urljoin, urlsplit, urlunsplit
import sys

import requests
from bs4 import BeautifulSoup, Tag

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.importers.common import strict_normalize_gtin

BASE_URL = "https://www.getbackmusic.nl"
LISTING_URL = f"{BASE_URL}/collections/lp-vinyl"
SHOP_NAME = "Get Back Music"
DEFAULT_DELAY_SECONDS = 0.5
REQUEST_TIMEOUT = 30

LISTING_FIELDS = (
    "scraped_at", "source_shop", "product_id", "variant_id", "product_key",
    "artist", "title", "format", "price", "standard_price", "is_sale",
    "availability", "product_url", "image_url", "page_found",
)
MASTER_FIELDS = LISTING_FIELDS + (
    "ean", "detail_title", "detail_description", "release_date", "label",
    "catalogue_number", "detail_status", "detail_error", "enriched_at",
)
DETAIL_FIELDS = MASTER_FIELDS[len(LISTING_FIELDS):]

SELECTORS = {
    "product_cards": (".cardmnsy",),
    "product_form": ("product-form-component[data-product-id]",),
    "variant_id": ("input[name='id']",),
    "product_link": (".cardmnsy__cover-link", ".cardmnsy__artist", ".cardmnsy__title"),
    "artist": (".cardmnsy__artist",),
    "title": (".cardmnsy__title",),
    "image": (".cardmnsy__img",),
    "format": (".cardmnsy__format",),
    "price": (".cardmnsy__price",),
    "sale_price": (".price-item--sale", ".sale-price", ".price--sale", "[class*='sale']"),
    "compare_price": ("del", "s", ".compare-at-price", ".price--compare", "[class*='compare']"),
    "cart_action": (
        "button[type='submit']", "input[type='submit']", "button[name='add']",
        "[name='add']", ".product-form__submit", "a[href*='/cart']",
    ),
    "next_page": ("a[rel='next']", "a[href*='page=']"),
}
OUT_OF_STOCK_MARKERS = (
    "uitverkocht", "niet beschikbaar", "niet leverbaar", "niet op voorraad",
    "sold out", "out of stock",
)
FORMAT_RE = re.compile(r"\b(?:LP|VINYL|EP|7\"|10\"|12\")\b", re.I)
NON_VINYL_RE = re.compile(r"\b(?:CD|DVD|CASSETTE|DIGITAL|BLU[ -]?RAY)\b", re.I)


class RateLimitedError(RuntimeError):
    """The source requested that the scraper stop requesting pages."""


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_text(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def normalize_url(value: str | None) -> str:
    if not value:
        return ""
    absolute = urljoin(BASE_URL, value)
    parsed = urlsplit(absolute)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {"getbackmusic.nl", "www.getbackmusic.nl"}:
        return ""
    path = re.sub(r"/{2,}", "/", parsed.path).rstrip("/") or "/"
    product_match = re.search(r"(/products/[^/]+)", path, re.I)
    if not product_match:
        return ""
    return urlunsplit(("https", "www.getbackmusic.nl", product_match.group(1), "", ""))


def normalize_image_url(value: str | None) -> str:
    if not value:
        return ""
    first = clean_text(value).split(",", 1)[0].split(" ", 1)[0]
    absolute = urljoin(BASE_URL, first)
    parsed = urlsplit(absolute)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {"getbackmusic.nl", "www.getbackmusic.nl"}:
        return ""
    return urlunsplit(("https", "www.getbackmusic.nl", parsed.path, parsed.query, ""))


def parse_money(value: str | None) -> str:
    text = clean_text(value).replace("€", "").replace("EUR", "")
    text = re.sub(r"[^0-9,.]", "", text).replace(".", "").replace(",", ".")
    if not text:
        return ""
    try:
        return f"{float(text):.2f}"
    except ValueError:
        return ""


def _price_values(node: Tag | None) -> list[str]:
    if node is None:
        return []
    matches = re.findall(
        r"(?:€|EUR)?\s*(\d{1,3}(?:[.\s]\d{3})*(?:,\d{1,2})?|\d+(?:[.,]\d{1,2})?)",
        node.get_text(" ", strip=True),
        re.I,
    )
    return [parsed for item in matches if (parsed := parse_money(item))]


def _first_price(node: Tag | None, selectors: Iterable[str]) -> str:
    if node is None:
        return ""
    for selector in selectors:
        values = _price_values(node.select_one(selector))
        if values:
            return values[0]
    return ""


def extract_listing_prices(card: Tag) -> tuple[str, str]:
    price_box = card.select_one(SELECTORS["price"][0])
    if price_box is None:
        return "", ""
    standard = _first_price(price_box, SELECTORS["compare_price"])
    current = _first_price(price_box, SELECTORS["sale_price"])
    if current:
        return current, standard if standard and float(standard) > float(current) else ""
    copy = BeautifulSoup(str(price_box), "html.parser")
    for selector in SELECTORS["compare_price"]:
        for node in copy.select(selector):
            node.decompose()
    values = _price_values(copy.select_one(SELECTORS["price"][0]) or copy)
    return (values[0] if values else ""), ""


def _selected_variant_id(form: Tag | None) -> str:
    inputs = form.select(SELECTORS["variant_id"][0]) if form else []
    selected = next((item for item in inputs if item.has_attr("checked") or item.get("selected") is not None), None)
    node = selected or (inputs[0] if inputs else None)
    return clean_text(node.get("value")) if node else ""


def _product_id(card: Tag) -> str:
    form = card.select_one(SELECTORS["product_form"][0])
    return clean_text(form.get("data-product-id")) if form else ""


def _text_from_card(card: Tag, selector: str) -> str:
    node = card.select_one(selector)
    return clean_text(node.get_text(" ", strip=True)) if node else ""


def _link_from_card(card: Tag) -> str:
    link_attributes = ("href", "data-href", "data-url", "data-product-url", "data-product-link", "data-product-href")
    handle_attributes = ("data-product-handle", "data-handle")
    for selector in SELECTORS["product_link"]:
        node = card.select_one(selector)
        if node:
            for attribute in link_attributes:
                url = normalize_url(node.get(attribute))
                if url:
                    return url
            for attribute in handle_attributes:
                handle = clean_text(node.get(attribute)).strip("/")
                if handle and "/" not in handle and "?" not in handle and "#" not in handle:
                    return normalize_url(f"/products/{handle}")
    # Some Shopify themes put the URL on the card/form custom element rather
    # than on the visible anchor. This remains scoped to the current card.
    for node in card.select("[href], [data-href], [data-url], [data-product-url], [data-product-link], [data-product-href], [data-product-handle], [data-handle]"):
        for attribute in link_attributes:
            url = normalize_url(node.get(attribute))
            if url:
                return url
        for attribute in handle_attributes:
            handle = clean_text(node.get(attribute)).strip("/")
            if handle and "/" not in handle and "?" not in handle and "#" not in handle:
                return normalize_url(f"/products/{handle}")
    return ""


def _format_label(value: str) -> str:
    match = FORMAT_RE.search(value)
    return match.group(0).upper() if match else "Vinyl"


def _image_url(card: Tag) -> str:
    image = card.select_one(SELECTORS["image"][0])
    return normalize_image_url(image.get("data-src") or image.get("src") or image.get("data-srcset")) if image else ""


def extract_availability(card: Tag) -> str:
    text = clean_text(card.get_text(" ", strip=True)).casefold()
    if any(marker in text for marker in OUT_OF_STOCK_MARKERS):
        return "out_of_stock"
    form = card.select_one(SELECTORS["product_form"][0])
    if form is None:
        return "out_of_stock"
    for node in form.select(",".join(SELECTORS["cart_action"])):
        classes = " ".join(node.get("class", [])).casefold()
        label = clean_text(node.get_text(" ", strip=True)).casefold()
        disabled = node.has_attr("disabled") or node.get("aria-disabled") == "true" or "disabled" in classes
        if not disabled and not any(marker in label for marker in OUT_OF_STOCK_MARKERS):
            return "in_stock"
    return "out_of_stock"


def next_page_number(soup: BeautifulSoup, current_page: int) -> int | None:
    for selector in SELECTORS["next_page"]:
        for node in soup.select(selector):
            href = node.get("href")
            if not href:
                continue
            values = parse_qs(urlsplit(urljoin(LISTING_URL, href)).query).get("page", [])
            if values and values[0].isdigit() and int(values[0]) == current_page + 1:
                return current_page + 1
    return None


def parse_listing_page(html: str, page: int) -> tuple[list[dict[str, str]], int | None, set[str], Counter[str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []
    identities: set[str] = set()
    skips: Counter[str] = Counter()
    for card in soup.select(SELECTORS["product_cards"][0]):
        product_id = _product_id(card)
        variant_id = _selected_variant_id(card.select_one(SELECTORS["product_form"][0]))
        identity = f"{product_id}:{variant_id}" if product_id and variant_id else ""
        if not identity:
            skips["missing_product_or_variant_id"] += 1
            continue
        if identity in identities:
            skips["duplicate_product_variant_on_page"] += 1
            continue
        identities.add(identity)
        product_url = _link_from_card(card)
        artist = _text_from_card(card, SELECTORS["artist"][0])
        title = _text_from_card(card, SELECTORS["title"][0])
        format_raw = _text_from_card(card, SELECTORS["format"][0])
        if NON_VINYL_RE.search(format_raw):
            skips["non_vinyl"] += 1
            continue
        price, standard_price = extract_listing_prices(card)
        if not product_url:
            skips["missing_product_url"] += 1
            continue
        if not artist:
            skips["missing_artist"] += 1
            continue
        if not title:
            skips["missing_title"] += 1
            continue
        if not price:
            skips["missing_listing_price"] += 1
            continue
        image = card.select_one(SELECTORS["image"][0])
        price_box = card.select_one(SELECTORS["price"][0])
        sale_marker = bool(price_box and price_box.select(",".join(SELECTORS["sale_price"])))
        rows.append({
            "scraped_at": now_utc_iso(), "source_shop": "Get Back Music",
            "product_id": product_id, "variant_id": variant_id,
            "product_key": f"getbackmusic:{product_id}:{variant_id}",
            "artist": artist, "title": title,
            "format": _format_label(format_raw),
            "price": price, "standard_price": standard_price,
            "is_sale": "true" if standard_price or sale_marker else "false",
            "availability": extract_availability(card), "product_url": product_url,
            "image_url": _image_url(card), "page_found": str(page),
        })
    return rows, next_page_number(soup, page), identities, skips


class GetBackClient:
    def __init__(self, session: requests.Session, delay_seconds: float = DEFAULT_DELAY_SECONDS):
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        self.session, self.delay_seconds, self.last_request_at = session, delay_seconds, None

    def get(self, url: str) -> str:
        if self.last_request_at is not None:
            remaining = self.delay_seconds - (time.monotonic() - self.last_request_at)
            if remaining > 0:
                time.sleep(remaining)
        for attempt in range(3):
            self.last_request_at = time.monotonic()
            try:
                response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            except requests.RequestException:
                if attempt < 2:
                    time.sleep((2**attempt) + random.random() * 0.25)
                    continue
                raise
            if response.status_code == 429:
                raise RateLimitedError(f"Get Back Music rate limit reached (HTTP 429): {url}")
            if response.status_code in {500, 502, 503, 504} and attempt < 2:
                time.sleep((2**attempt) + random.random() * 0.25)
                continue
            if response.status_code >= 400:
                raise RuntimeError(f"Get Back Music HTTP {response.status_code}: {url}")
            return response.text
        raise RuntimeError(f"Get Back Music transient HTTP failure: {url}")


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "VinylofyGetBackMusic/1.0 (+https://vinylofy.com)",
        "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    })
    return session


def scrape_listings(client: GetBackClient, max_pages: int = 1) -> tuple[list[dict[str, str]], Counter[str], int]:
    if max_pages < 0:
        raise ValueError("max_pages must be >= 0")
    rows: list[dict[str, str]] = []
    seen_identities: set[str] = set()
    seen_page_sets: set[frozenset[str]] = set()
    skips: Counter[str] = Counter()
    page, pages = 1, 0
    while max_pages == 0 or pages < max_pages:
        url = LISTING_URL if page == 1 else f"{LISTING_URL}?page={page}"
        try:
            html = client.get(url)
        except RateLimitedError:
            skips["rate_limited"] += 1
            break
        except (requests.RequestException, RuntimeError):
            skips["listing_fetch_error"] += 1
            break
        page_rows, next_page, page_ids, page_skips = parse_listing_page(html, page)
        skips.update(page_skips)
        if not page_ids:
            skips["empty_product_page"] += 1
            break
        page_set = frozenset(page_ids)
        if page_set in seen_page_sets:
            skips["repeated_product_variant_set"] += 1
            break
        seen_page_sets.add(page_set)
        pages += 1
        for row in page_rows:
            if row["product_key"] in seen_identities:
                skips["duplicate_product_variant_between_pages"] += 1
            else:
                seen_identities.add(row["product_key"])
                rows.append(row)
        print(f"[PAGE {page}] cards={len(page_ids)} accepted={len(page_rows)} total={len(rows)} next={next_page or '-'}", flush=True)
        if next_page is None:
            skips["no_valid_next_page"] += 1
            break
        page = next_page
    return rows, skips, pages


def _jsonld_values(soup: BeautifulSoup) -> list[str]:
    values: list[str] = []
    for script in soup.select("script[type='application/ld+json']"):
        try:
            payload = json.loads(script.string or script.get_text())
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        for obj in (payload if isinstance(payload, list) else [payload]):
            if isinstance(obj, dict):
                values.extend(str(obj[key]) for key in ("gtin8", "gtin12", "gtin13", "gtin14", "productID", "sku") if obj.get(key))
    return values


def _valid_gtin_display(value: str) -> str:
    normalized = strict_normalize_gtin(value)
    return normalized[1:] if normalized and normalized.startswith("0") else (normalized or "")


def parse_detail_page(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    text = clean_text(soup.get_text(" ", strip=True))
    candidates = _jsonld_values(soup)
    candidates.extend(node.get("content", "") for node in soup.select("[itemprop^='gtin']") if node.get("content"))
    label_pattern = re.compile(r"(?:EAN(?:-13)?|GTIN(?:-\d{1,2})?|barcode|bar\s*code)\s*(?:[:#\-]?\s*)['\"]?([0-9][0-9 .-]{7,17})", re.I)
    candidates.extend(match.group(1) for match in label_pattern.finditer(text))
    candidates.extend(match.group(1) for match in label_pattern.finditer(html))
    ean = next((_valid_gtin_display(candidate) for candidate in candidates if _valid_gtin_display(candidate)), "")
    title_node = soup.select_one(".product__title, h1")
    desc_node = soup.select_one(".product__description, [class*='description']")
    def labelled(pattern: str) -> str:
        match = re.search(pattern + r"\s*[:\-]?\s*([^|;]+)", text, re.I)
        return clean_text(match.group(1)) if match else ""
    return {
        "ean": ean,
        "detail_title": clean_text(title_node.get_text(" ", strip=True)) if title_node else "",
        "detail_description": clean_text(desc_node.get_text(" ", strip=True)) if desc_node else "",
        "release_date": labelled(r"(?:release date|releasedatum)"),
        "label": labelled(r"label"),
        "catalogue_number": labelled(r"(?:catalogue|catalogus|cat\.? no\.?)\s*(?:number|nummer)?"),
        "detail_status": "ok" if ean else "missing_ean",
    }


def enrich_details(client: GetBackClient, rows: list[dict[str, str]], limit: int) -> int:
    if limit < 0:
        raise ValueError("detail limit must be >= 0")
    attempted = 0
    for row in [item for item in rows if not strict_normalize_gtin(item.get("ean", ""))][:limit]:
        attempted += 1
        try:
            row.update(parse_detail_page(client.get(row["product_url"])))
            row["enriched_at"] = now_utc_iso()
        except RateLimitedError as exc:
            row["detail_status"], row["detail_error"] = "technical_error", str(exc)
            print(f"[DETAIL] rate limit; stopping remaining detail requests: {exc}", flush=True)
            break
        except (requests.RequestException, RuntimeError) as exc:
            row["detail_status"], row["detail_error"] = "technical_error", str(exc)
        print(f"[DETAIL {attempted}] key={row.get('product_key')} status={row.get('detail_status')}", flush=True)
    return attempted


def merge_listing_with_previous(
    rows: list[dict[str, str]], previous_rows: Iterable[dict[str, str]]
) -> list[dict[str, str]]:
    """Refresh listing fields while retaining previously enriched detail fields."""
    previous_by_key = {
        row.get("product_key", ""): row
        for row in previous_rows
        if row.get("product_key")
    }
    for row in rows:
        previous = previous_by_key.get(row.get("product_key", ""))
        if not previous:
            continue
        for field in DETAIL_FIELDS:
            if previous.get(field):
                row[field] = previous[field]
    return rows


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
    parser = argparse.ArgumentParser(description="Bounded listing-first Get Back Music LP/Vinyl scraper")
    parser.add_argument("--mode", choices=("listing", "detail", "both"), default="both")
    parser.add_argument("--max-pages", type=int, default=1, help="Listing limit; 0 follows stop conditions")
    parser.add_argument("--detail-limit", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw/getbackmusic"))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.max_pages < 0 or args.detail_limit < 0:
        raise SystemExit("--max-pages en --detail-limit mogen niet negatief zijn")
    output_dir = args.output_dir
    listing_path, master_path = output_dir / "getbackmusic_listings.csv", output_dir / "getbackmusic_master.csv"
    client = GetBackClient(build_session(), args.delay_seconds)
    if args.mode in {"listing", "both"}:
        previous_rows = read_rows(output_dir / "getbackmusic_master.csv") if (output_dir / "getbackmusic_master.csv").exists() else []
        rows, skips, pages = scrape_listings(client, args.max_pages)
        merge_listing_with_previous(rows, previous_rows)
        write_rows(listing_path, rows, LISTING_FIELDS)
        write_rows(master_path, rows, MASTER_FIELDS)
        print(f"[LISTING] pages={pages} products={len({r['product_id'] for r in rows})} variants={len(rows)} sale={sum(r['is_sale'] == 'true' for r in rows)} skips={dict(skips)}", flush=True)
    else:
        if not master_path.exists():
            raise SystemExit(f"Master CSV not found: {master_path}")
        rows = read_rows(master_path)
    if args.mode in {"detail", "both"}:
        attempted = enrich_details(client, rows, args.detail_limit)
        write_rows(master_path, rows, MASTER_FIELDS)
        print(f"[DETAIL] attempted={attempted} variants={len(rows)} ean_hits={sum(bool(strict_normalize_gtin(r.get('ean', ''))) for r in rows)}", flush=True)
    print(f"[OUTPUT] listing={listing_path} master={master_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
