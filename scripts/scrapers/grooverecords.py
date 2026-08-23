#!/usr/bin/env python3
"""Bounded, listing-first scraper for Groove Records vinyl.

Groove Records is a classic server-rendered shop.  Category listings are the
source of truth for product discovery, current price and availability. Detail
pages are used only for EAN and metadata enrichment.

The shop publishes ``Crawl-delay: 30`` through robots.txt.  This module
therefore enforces a minimum 30-second interval between HTTP requests during
real runs and deliberately performs no concurrent requests or retry burst.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import re
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import parse_qs, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag


BASE_URL = "https://grooverecords.nl"
HOME_URL = f"{BASE_URL}/nl/"
SHOP_NAME = "Groove Records"
SHOP_DOMAIN = "grooverecords.nl"
MIN_CRAWL_DELAY_SECONDS = 30.0
DEFAULT_DELAY_SECONDS = 30.0

LISTING_FIELDS = (
    "source_shop",
    "product_key",
    "product_id",
    "category_slug",
    "category_group_id",
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

MASTER_FIELDS = LISTING_FIELDS + (
    "ean",
    "release_date",
    "label",
    "catalogue_number",
    "detail_availability_observed",
    "detail_status",
    "detail_error",
    "enriched_at",
)

# These are intentionally kept in one place so future selector changes are
# reviewable. The first selectors are the shop's documented class names;
# fallbacks cover the same HTML theme when a class is rendered on a wrapper.
SELECTORS = {
    "product_links": (
        "a[href*='aid=']",
        "a[href*='aid/']",
        "a[href*='/product/']",
        "a[href*='/article/']",
    ),
    "product_cards": (
        "article.product",
        "article.article",
        ".product",
        ".article",
        "[data-aid]",
        "[data-product-id]",
    ),
    "current_price": (
        ".articlePrice",
        "[class*='articlePrice']",
        "span.price",
        ".price",
    ),
    "standard_price": (
        ".articlePricerecommended",
        ".discountArticlePrice",
        "del",
        "s",
    ),
    "availability": (
        ".articleAvailability",
        "[class*='availability']",
        "[class*='stock']",
    ),
    "order_button": (
        "button",
        "input[type='submit']",
        "input[type='button']",
        "a",
    ),
    "next_page": ("a.listforward", "a[class~='listforward']"),
    "detail_ean": (
        ".details",
        ".productDetails",
        ".articleDetails",
        "dl",
        "table",
        "li",
        "p",
    ),
}

EXCLUDED_CATEGORY_MARKERS = (
    "accessoire",
    "accessories",
    "cadeau",
    "gift",
    "merch",
    "naald",
    "needle",
    "apparatuur",
    "turntable",
    "draaitafel",
    "headphone",
    "kabel",
    "cleaning",
    "reiniger",
    "cd",
    "dvd",
    "boek",
)
CATEGORY_RE = re.compile(r"/nl/page/([^/?#]+)/([0-9]+)/([0-9]+)(?:/)?$", re.I)


@dataclass(frozen=True)
class Category:
    slug: str
    group_id: str
    label: str

    @property
    def first_page_url(self) -> str:
        return f"{BASE_URL}/nl/page/{self.slug}/{self.group_id}/0"


class RateLimitedClient:
    """One sequential HTTP client with an enforced minimum request interval."""

    def __init__(self, session: requests.Session, delay_seconds: float = DEFAULT_DELAY_SECONDS):
        if delay_seconds < MIN_CRAWL_DELAY_SECONDS:
            raise ValueError(
                f"Groove Records requires delay_seconds >= {MIN_CRAWL_DELAY_SECONDS:g}"
            )
        self.session = session
        self.delay_seconds = delay_seconds
        self._last_request_at: float | None = None

    def get(self, url: str, timeout: int = 30) -> str:
        if self._last_request_at is not None:
            remaining = self.delay_seconds - (time.monotonic() - self._last_request_at)
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_at = time.monotonic()
        response = self.session.get(url, timeout=timeout)
        if response.status_code == 429:
            raise RuntimeError(f"Groove Records rate limit reached (HTTP 429): {url}")
        if response.status_code >= 400:
            raise RuntimeError(f"Groove Records HTTP {response.status_code}: {url}")
        return response.text


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "VinylofyGrooveRecords/1.0 (+https://vinylofy.com)",
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def normalize_space(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_url(value: str | None) -> str:
    if not value:
        return ""
    absolute = urljoin(BASE_URL, value)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {
        "grooverecords.nl",
        "www.grooverecords.nl",
    }:
        return ""
    query = parse_qs(parsed.query)
    aid = query.get("aid", [""])[0].strip()
    return urlunparse(
        (
            "https",
            SHOP_DOMAIN,
            parsed.path.rstrip("/") or "/",
            "",
            f"aid={aid}" if aid else "",
            "",
        )
    )


def product_id_from_url(url: str) -> str:
    parsed = urlparse(url)
    aid = parse_qs(parsed.query).get("aid", [""])[0].strip()
    if aid:
        return aid
    match = re.search(r"(?:aid|article|product)[/_-]?(\d+)", parsed.path, re.I)
    return match.group(1) if match else ""


def product_key_from_url(url: str) -> str:
    product_id = product_id_from_url(url)
    if product_id:
        return f"aid:{product_id}"
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:24]


def parse_money(value: str | None) -> str:
    text = normalize_space(value).replace("€", "").replace("EUR", "")
    text = text.replace("\xa0", "").replace(" ", "")
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


def _struck(node: Tag) -> bool:
    return node.name in {"del", "s"} or node.find_parent(["del", "s"]) is not None


def _first_unstruck_price(card: Tag) -> str:
    for selector in SELECTORS["current_price"]:
        for node in card.select(selector):
            if isinstance(node, Tag) and not _struck(node):
                price = parse_money(node.get_text(" ", strip=True))
                if price:
                    return price
    return ""


def extract_listing_prices(card: Tag) -> tuple[str, str]:
    standard = ""
    for selector in SELECTORS["standard_price"]:
        for node in card.select(selector):
            price = parse_money(node.get_text(" ", strip=True))
            if price:
                standard = price
                break
        if standard:
            break
    return _first_unstruck_price(card), standard


def _active_order_button(card: Tag) -> bool:
    for selector in SELECTORS["order_button"]:
        for node in card.select(selector):
            if normalize_space(node.get_text(" ", strip=True)).casefold() != "bestellen":
                continue
            classes = " ".join(node.get("class", [])).casefold()
            if node.has_attr("disabled") or node.get("aria-disabled") == "true" or "disabled" in classes:
                continue
            return True
    return False


def extract_availability(card: Tag) -> str:
    scoped = " ".join(
        normalize_space(node.get_text(" ", strip=True))
        for selector in SELECTORS["availability"]
        for node in card.select(selector)
    )
    text = normalize_space(f"{scoped} {card.get_text(' ', strip=True)}").casefold()
    if any(marker in text for marker in ("uitverkocht", "niet leverbaar", "niet op voorraad", "out of stock")):
        return "out_of_stock"
    if _active_order_button(card) or any(marker in text for marker in ("op voorraad", "in stock")):
        return "in_stock"
    return "unknown"


def _product_card(link: Tag) -> Tag:
    for parent in link.parents:
        if not isinstance(parent, Tag):
            continue
        classes = " ".join(parent.get("class", [])).casefold()
        if parent.name in {"article", "li"} or any(word in classes for word in ("product", "article", "item")):
            return parent
    return link.parent if isinstance(link.parent, Tag) else link


def _is_product_link(href: str) -> bool:
    url = normalize_url(href)
    if not url:
        return False
    parsed = urlparse(url)
    if "/nl/page/" in parsed.path:
        return False
    return bool(product_id_from_url(url)) or "/product/" in parsed.path or "/article/" in parsed.path


def _text_from(card: Tag, selectors: Iterable[str]) -> str:
    for selector in selectors:
        node = card.select_one(selector)
        if node:
            value = normalize_space(node.get_text(" ", strip=True))
            if value:
                return value
    return ""


def parse_listing_page(
    html: str,
    category: Category,
    page_index: int,
) -> tuple[list[dict[str, str]], str | None, Counter[str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []
    skips: Counter[str] = Counter()
    seen_keys: set[str] = set()
    links = [node for node in soup.select("a[href]") if isinstance(node, Tag) and _is_product_link(node.get("href", ""))]

    for link in links:
        product_url = normalize_url(link.get("href"))
        key = product_key_from_url(product_url)
        if key in seen_keys:
            continue
        card = _product_card(link)
        price, standard_price = extract_listing_prices(card)
        if not price:
            skips["missing_listing_price"] += 1
            continue
        title = _text_from(card, (".articleTitle", ".productTitle", "[class*='articleTitle']", "h2", "h3"))
        artist = _text_from(card, (".articleArtist", ".artist", "[class*='artist']"))
        if not title:
            title = normalize_space(link.get_text(" ", strip=True))
        if not title:
            skips["missing_title"] += 1
            continue
        image = card.select_one("img[data-src], img[data-lazy-src], img[src]")
        image_url = normalize_url(image.get("data-src") or image.get("data-lazy-src") or image.get("src")) if image else ""
        seen_keys.add(key)
        rows.append(
            {
                "source_shop": SHOP_NAME,
                "product_key": key,
                "product_id": product_id_from_url(product_url),
                "category_slug": category.slug,
                "category_group_id": category.group_id,
                "artist": artist,
                "title": title,
                "price": price,
                "standard_price": standard_price,
                "currency": "EUR",
                "availability": extract_availability(card),
                "product_url": product_url,
                "page_found": str(page_index),
                "scraped_at": now_utc_iso(),
                "image_url": image_url,
                "format": _text_from(card, (".articleFormat", ".format", "[class*='format']")) or "Vinyl",
                "detail_status": "pending",
            }
        )

    next_url = None
    for selector in SELECTORS["next_page"]:
        node = soup.select_one(selector)
        if node:
            candidate = normalize_url(node.get("href"))
            if candidate:
                next_url = candidate
                break
    return rows, next_url, skips


def discover_categories(html: str) -> list[Category]:
    soup = BeautifulSoup(html, "html.parser")
    categories: dict[tuple[str, str], Category] = {}
    for link in soup.select("a[href]"):
        href = normalize_url(link.get("href"))
        parsed = urlparse(href)
        match = CATEGORY_RE.search(parsed.path)
        if not match:
            continue
        slug, group_id, _page_index = match.groups()
        label = normalize_space(link.get_text(" ", strip=True)) or slug.replace("-", " ").title()
        haystack = f"{slug} {label}".casefold()
        if any(marker in haystack for marker in EXCLUDED_CATEGORY_MARKERS):
            continue
        categories[(slug, group_id)] = Category(slug, group_id, label)
    return sorted(categories.values(), key=lambda item: (item.label.casefold(), item.slug, item.group_id))


def parse_category_spec(value: str) -> Category:
    try:
        slug, group_id = value.split(":", 1)
    except ValueError as exc:
        raise ValueError("category moet SLUG:GROUP_ID zijn, bijvoorbeeld rock:30") from exc
    if not slug or not group_id.isdigit():
        raise ValueError("category moet SLUG:GROUP_ID zijn, bijvoorbeeld rock:30")
    return Category(slug, group_id, slug.replace("-", " ").title())


def scrape_listings(
    client: RateLimitedClient,
    categories: Iterable[Category],
    max_pages: int,
) -> tuple[list[dict[str, str]], Counter[str], int]:
    if not 1 <= max_pages <= 3:
        raise ValueError("max_pages must be between 1 and 3; full catalog crawls are disabled")
    rows: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    skips: Counter[str] = Counter()
    pages = 0
    for category in categories:
        next_url = category.first_page_url
        for page_number in range(max_pages):
            if not next_url:
                break
            html = client.get(next_url)
            page_rows, discovered_next, page_skips = parse_listing_page(html, category, page_number)
            pages += 1
            skips.update(page_skips)
            for row in page_rows:
                if row["product_key"] in seen_keys:
                    skips["duplicate_between_categories"] += 1
                    continue
                seen_keys.add(row["product_key"])
                rows.append(row)
            next_url = discovered_next
            if not page_rows or not discovered_next:
                break
    return rows, skips, pages


def parse_detail_page(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    text = normalize_space(soup.get_text(" ", strip=True))
    ean = ""
    for node in soup.select(",".join(SELECTORS["detail_ean"])):
        candidate_text = normalize_space(node.get_text(" ", strip=True))
        match = re.search(r"\bEAN\s*:\s*(\d{8,14})\b", candidate_text, re.I)
        if match:
            ean = match.group(1)
            break
    if not ean:
        match = re.search(r"\bEAN\s*:\s*(\d{8,14})\b", text, re.I)
        ean = match.group(1) if match else ""

    def label_value(label: str) -> str:
        match = re.search(rf"\b{re.escape(label)}\s*:\s*([^|]+?)(?=\s+\w[\w ]{{1,24}}\s*:|$)", text, re.I)
        return normalize_space(match.group(1)) if match else ""

    active_order = False
    for selector in SELECTORS["order_button"]:
        for node in soup.select(selector):
            if normalize_space(node.get_text(" ", strip=True)).casefold() != "bestellen":
                continue
            classes = " ".join(node.get("class", [])).casefold()
            if not node.has_attr("disabled") and node.get("aria-disabled") != "true" and "disabled" not in classes:
                active_order = True
    detail_availability = "in_stock" if active_order else "unknown"
    if any(marker in text.casefold() for marker in ("uitverkocht", "niet leverbaar", "niet op voorraad")):
        detail_availability = "out_of_stock"
    return {
        "ean": ean,
        "release_date": label_value("Release") or label_value("Releasedatum"),
        "label": label_value("Label"),
        "catalogue_number": label_value("Catalogusnummer") or label_value("Catalog number"),
        "detail_availability_observed": detail_availability,
        "detail_status": "ok" if ean else "missing_ean",
    }


def enrich_details(client: RateLimitedClient, rows: list[dict[str, str]], limit: int) -> int:
    if limit < 0:
        raise ValueError("detail limit must be >= 0")
    attempted = 0
    for row in rows:
        if attempted >= limit:
            break
        if row.get("ean") and row.get("detail_status") == "ok":
            continue
        attempted += 1
        try:
            details = parse_detail_page(client.get(row["product_url"]))
        except (requests.RequestException, RuntimeError) as exc:
            row["detail_status"] = "technical_error"
            row["detail_error"] = str(exc)
        else:
            # Deliberately update only detail fields. Listing price and
            # availability remain authoritative even if detail differs.
            row.update(details)
            row["enriched_at"] = now_utc_iso()
    return attempted


def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_rows(path: Path, rows: Iterable[dict[str, str]], fields: Iterable[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fields), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bounded listing-first Groove Records vinyl scraper")
    parser.add_argument("--mode", choices=("listing", "detail", "both"), default="both")
    parser.add_argument("--category", action="append", help="Category as SLUG:GROUP_ID, repeatable")
    parser.add_argument("--discover-categories", action="store_true", help="Read category links from the shop navigation")
    parser.add_argument("--max-pages", type=int, default=1, help="Bounded pilot limit: 1-3 pages per category")
    parser.add_argument("--detail-limit", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw/grooverecords"))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.detail_limit < 0:
        raise SystemExit("detail-limit mag niet negatief zijn")
    try:
        client = RateLimitedClient(build_session(), args.delay_seconds)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    if args.discover_categories:
        categories = discover_categories(client.get(HOME_URL))
        if not categories:
            raise SystemExit("Geen relevante Groove Records vinylcategorieën gevonden")
        print("[GROOVE] categories=" + ",".join(f"{item.slug}:{item.group_id}" for item in categories))
    elif args.category:
        try:
            categories = [parse_category_spec(value) for value in args.category]
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc
    else:
        categories = [Category("rock", "30", "Rock")]

    listing_path = args.output_dir / "grooverecords_listing.csv"
    master_path = args.output_dir / "grooverecords_master.csv"
    if args.mode in {"listing", "both"}:
        rows, skips, pages = scrape_listings(client, categories, args.max_pages)
        if not rows:
            raise SystemExit("Groove Records listing leverde geen producten op")
        write_rows(listing_path, rows, LISTING_FIELDS)
        write_rows(master_path, rows, MASTER_FIELDS)
        skip_text = " ".join(f"{key}={value}" for key, value in sorted(skips.items())) or "none"
        print(f"[GROOVE] listing pages={pages} products={len(rows)} skips={skip_text}")
    else:
        rows = read_rows(master_path) or read_rows(listing_path)
        if not rows:
            raise SystemExit(f"Geen listing/master CSV gevonden in {args.output_dir}")

    if args.mode in {"detail", "both"}:
        attempted = enrich_details(client, rows, args.detail_limit)
        write_rows(master_path, rows, MASTER_FIELDS)
        print(
            f"[GROOVE] detail attempted={attempted} "
            f"ean_hits={sum(bool(row.get('ean')) for row in rows)} master_rows={len(rows)}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
