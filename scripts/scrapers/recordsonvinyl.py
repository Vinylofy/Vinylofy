#!/usr/bin/env python3
"""
RecordsOnVinyl.nl scraper — CSV-native edition

Vinylofy implementation build.
State is stored in CSV files instead of SQLite.

Supported flows
1) Crawl
   - discover product URLs from the collection
   - scrape product details via Shopify JSON / HTML fallback
   - merge into a master CSV snapshot

2) Refresh prices
   - FAST + guarded implementation: parse prices from collection/listing cards
   - scope each parsed price strictly to its own <a class="grid-product__link"> card
   - update existing master rows by product handle/product_url only, never by title
   - do not create new product rows from listing pages, because listing pages do not carry reliable EANs
   - skip multi-variant handles, because one listing price cannot safely update multiple variant prices

3) Export
   - write the current master snapshot to a chosen CSV path

Why this version exists
-----------------------
A previous listing refresh was unsafe because it searched too broadly in parent DOM blocks,
which allowed unrelated numbers or adjacent product prices to overwrite the wrong product.
This version keeps the speed advantage of listing-page refreshes, but only reads prices from
price elements inside the exact product link/card being updated:

- current/sale price: .sale-price-emphasis
- original/list price: .grid-product__price--original
- product key: the /products/{handle} URL on the same a.grid-product__link

New products and EAN/barcode enrichment remain the responsibility of the crawl flow, which
uses Shopify /products/{handle}.js.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

DEFAULT_COLLECTION = "https://recordsonvinyl.nl/collections/all"
UA = "StorkStylusPricingBot/1.0"


# -----------------------------
# General helpers
# -----------------------------


def now_utc_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def normalize_url(url: str) -> str:
    p = urlparse(url)
    p = p._replace(fragment="")
    return urlunparse(p)


def canonical_product_url(url: str) -> str:
    """Strip query/fragment from product URL while preserving the product path."""
    p = urlparse(url)
    p = p._replace(query="", fragment="")
    return urlunparse(p)


def set_query_param(url: str, **params: str) -> str:
    p = urlparse(url)
    q = dict(parse_qsl(p.query, keep_blank_values=True))
    q.update({k: v for k, v in params.items() if v is not None})
    p = p._replace(query=urlencode(q))
    return urlunparse(p)


def safe_sleep(base_delay: float, jitter: float = 0.25) -> None:
    if base_delay <= 0:
        return
    time.sleep(base_delay + random.random() * jitter)


def ean13_is_valid(ean: str) -> bool:
    if not ean or not re.fullmatch(r"\d{13}", ean):
        return False
    digits = [int(c) for c in ean]
    check = digits[-1]
    total = 0
    for i, d in enumerate(digits[:-1]):
        total += 3 * d if (i + 1) % 2 == 0 else d
    calc = (10 - (total % 10)) % 10
    return calc == check


def clean_ean(candidate: Any) -> Optional[str]:
    if candidate is None:
        return None
    c = re.sub(r"\D", "", str(candidate))
    if len(c) == 13 and ean13_is_valid(c):
        return c
    return None


def cents_to_eur(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return round(float(value) / 100.0, 2)
    except Exception:
        return None


def eur_str_to_float(s: Any) -> Optional[float]:
    if s is None:
        return None
    x = str(s).strip().replace("€", "").replace("\u20ac", "")
    x = re.sub(r"[^\d,\.]", "", x)
    if not x:
        return None
    x = x.replace(",", ".")
    try:
        return round(float(x), 2)
    except Exception:
        return None


def bool_to_csv(value: Optional[bool]) -> str:
    if value is None:
        return ""
    return "True" if value else "False"


def parse_artist_album(title_raw: str) -> Tuple[Optional[str], Optional[str]]:
    if not title_raw:
        return None, None
    title = title_raw.strip()
    title = re.sub(r"\s*\((?:\d+\s*)?LPs?\)\s*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*\(LP\)\s*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*\(CD\)\s*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*\(Cassette\)\s*$", "", title, flags=re.IGNORECASE)
    for sep in [" - ", " – ", " — "]:
        if sep in title:
            artist, album = title.split(sep, 1)
            return (artist.strip() or None), (album.strip() or None)
    return None, title or None


def find_best_availability_label(text: str) -> Optional[str]:
    if not text:
        return None
    labels = [
        "Binnenkort weer op voorraad",
        "nog korte tijd leverbaar",
        "Snel weer op voorraad",
        "Preorder Now",
        "Preorder",
        "Bestelbaar",
        "Op Voorraad",
        "In stock",
        "Sold out",
        "Uitverkocht",
    ]
    low = text.lower()
    for label in labels:
        if label.lower() in low:
            return label
    return None


def availability_to_bool(label: Optional[str]) -> Optional[bool]:
    if not label:
        return None
    low = label.lower()
    if "uitverkocht" in low or "sold out" in low:
        return False
    if "op voorraad" in low or "in stock" in low or "bestelbaar" in low or "preorder" in low:
        return True
    return None


def make_session(user_agent: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
            "Accept-Language": "nl,en;q=0.8",
        }
    )
    return session


def get_with_retry(
    session: requests.Session,
    url: str,
    timeout: int = 25,
    retries: int = 4,
) -> requests.Response:
    backoff = 1.0
    for attempt in range(retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {response.status_code}")
            return response
        except Exception:
            if attempt == retries:
                raise
            time.sleep(backoff + random.random() * 0.5)
            backoff *= 2
    raise RuntimeError("unreachable")


# -----------------------------
# robots.txt caching
# -----------------------------


_ROBOTS_CACHE: Dict[Tuple[str, str], Tuple[List[str], float]] = {}
_ROBOTS_LOCK = threading.Lock()


def _fetch_robots_lines(session: requests.Session, base_url: str) -> Optional[List[str]]:
    robots_url = urljoin(base_url, "/robots.txt")
    response = session.get(robots_url, timeout=20)
    if response.status_code != 200:
        return None
    return response.text.splitlines()


def _get_cached_robots_lines(
    session: requests.Session,
    base_url: str,
    user_agent: str,
) -> Optional[List[str]]:
    key = (base_url, user_agent.lower())
    with _ROBOTS_LOCK:
        if key in _ROBOTS_CACHE:
            return _ROBOTS_CACHE[key][0]
    try:
        lines = _fetch_robots_lines(session, base_url)
    except Exception:
        lines = None
    with _ROBOTS_LOCK:
        _ROBOTS_CACHE[key] = (lines if lines is not None else [], time.time())
    return lines


def robots_allows(
    session: requests.Session,
    base_url: str,
    target_url: str,
    user_agent: str,
) -> bool:
    lines = _get_cached_robots_lines(session, base_url, user_agent)
    if lines is None or lines == []:
        return True

    active = False
    disallows: List[str] = []
    allows: List[str] = []

    for raw_line in lines:
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue
        if line.lower().startswith("user-agent:"):
            ua = line.split(":", 1)[1].strip()
            active = ua == "*" or ua.lower() in user_agent.lower()
            continue
        if not active:
            continue
        if line.lower().startswith("disallow:"):
            disallows.append(line.split(":", 1)[1].strip())
        elif line.lower().startswith("allow:"):
            allows.append(line.split(":", 1)[1].strip())

    path = urlparse(target_url).path or "/"
    for allow_path in allows:
        if allow_path and path.startswith(allow_path):
            return True
    for disallow_path in disallows:
        if disallow_path and path.startswith(disallow_path):
            return False
    return True


# -----------------------------
# Product parsing
# -----------------------------


def product_handle_from_url(url: str) -> Optional[str]:
    parsed = urlparse(url)
    match = re.search(r"/products/([^/?#]+)", parsed.path)
    return match.group(1) if match else None


def shopify_product_js_url(product_url: str) -> Optional[str]:
    parsed = urlparse(product_url)
    handle = product_handle_from_url(product_url)
    if not parsed.scheme or not parsed.netloc or not handle:
        return None
    return f"{parsed.scheme}://{parsed.netloc}/products/{handle}.js"


def scrape_product_js(
    session: requests.Session,
    product_url: str,
    ignore_robots: bool,
    user_agent: str,
) -> Optional[List[Dict[str, Any]]]:
    base = f"{urlparse(product_url).scheme}://{urlparse(product_url).netloc}"
    js_url = shopify_product_js_url(product_url)
    if not js_url:
        return None
    if not ignore_robots and not robots_allows(session, base, js_url, user_agent):
        raise RuntimeError(f"Blocked by robots.txt: {js_url}")

    response = get_with_retry(session, js_url)
    if response.status_code != 200:
        return None

    try:
        data = response.json()
    except Exception:
        return None

    title = data.get("title")
    variants = data.get("variants", [])
    if not isinstance(variants, list):
        return None

    out: List[Dict[str, Any]] = []
    for variant in variants:
        if isinstance(variant, dict):
            row = dict(variant)
            row["__title__"] = title
            out.append(row)

    if out:
        return out
    return [{"__title__": title}]


def _parse_json_ld_currency(soup: BeautifulSoup) -> str:
    currency = "EUR"
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            import json

            payload = json.loads(script.get_text(strip=True))
        except Exception:
            continue
        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if not isinstance(node, dict) or node.get("@type") != "Product":
                continue
            offers = node.get("offers")
            if isinstance(offers, dict):
                cur = offers.get("priceCurrency")
                if isinstance(cur, str) and cur.strip():
                    currency = cur.strip()
    return currency


def _extract_embedded_variants_from_html(html: str) -> List[Dict[str, Any]]:
    """Conservative fallback for old Shopify theme embedded variant arrays."""
    match = re.search(r"\[\s*\{\s*\"id\"\s*:\s*\d+", html)
    if not match:
        return []

    start = match.start()
    i = start
    depth = 0
    in_str = False
    escaped = False

    while i < len(html):
        ch = html[i]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_str = False
        else:
            if ch == '"':
                in_str = True
            elif ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    chunk = html[start : i + 1]
                    try:
                        import json

                        data = json.loads(chunk)
                    except Exception:
                        return []
                    return data if isinstance(data, list) else []
        i += 1
    return []


def scrape_product_html(
    session: requests.Session,
    product_url: str,
    ignore_robots: bool,
    user_agent: str,
) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], List[Dict[str, Any]]]:
    base = f"{urlparse(product_url).scheme}://{urlparse(product_url).netloc}"
    if not ignore_robots and not robots_allows(session, base, product_url, user_agent):
        raise RuntimeError(f"Blocked by robots.txt: {product_url}")

    response = get_with_retry(session, product_url)
    if response.status_code != 200:
        raise RuntimeError(f"HTTP {response.status_code} on {product_url}")

    html = response.text
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title_raw = h1.get_text(strip=True) if h1 else None
    artist, album = parse_artist_album(title_raw or "")
    page_text = soup.get_text(" ", strip=True)
    availability_raw = find_best_availability_label(page_text)
    currency = _parse_json_ld_currency(soup)
    variants = _extract_embedded_variants_from_html(html)
    return title_raw, artist, album, currency, availability_raw, variants


@dataclasses.dataclass
class VariantRow:
    source: str
    scraped_at: str
    product_url: str
    handle: str
    title_raw: Optional[str]
    artist: Optional[str]
    album: Optional[str]
    variant_id: int
    variant_title: Optional[str]
    sku: Optional[str]
    ean13: Optional[str]
    currency: str
    price_offer: Optional[float]
    price_list: Optional[float]
    available: Optional[bool]
    availability_raw: Optional[str]


def _variant_row_from_shopify_variant(
    product_url: str,
    handle: str,
    scraped_at: str,
    title_raw: Optional[str],
    variant: Dict[str, Any],
    currency: str = "EUR",
    availability_raw: Optional[str] = None,
) -> Optional[VariantRow]:
    try:
        variant_id = int(variant.get("id"))
    except Exception:
        return None

    artist, album = parse_artist_album(title_raw or "")
    price_offer = cents_to_eur(variant.get("price"))
    price_list = cents_to_eur(variant.get("compare_at_price"))
    if price_list is not None and price_offer is not None and price_list <= price_offer:
        price_list = None

    available_value = variant.get("available")
    available = available_value if isinstance(available_value, bool) else availability_to_bool(availability_raw)

    return VariantRow(
        source="recordsonvinyl",
        scraped_at=scraped_at,
        product_url=product_url,
        handle=handle,
        title_raw=title_raw,
        artist=artist,
        album=album,
        variant_id=variant_id,
        variant_title=variant.get("public_title") or variant.get("title") or None,
        sku=variant.get("sku") or None,
        ean13=clean_ean(variant.get("barcode") or ""),
        currency=currency or "EUR",
        price_offer=price_offer,
        price_list=price_list,
        available=available,
        availability_raw=availability_raw,
    )


def scrape_product(
    session: requests.Session,
    product_url: str,
    delay_product: float,
    ignore_robots: bool,
    user_agent: str,
) -> List[VariantRow]:
    product_url = canonical_product_url(product_url)
    handle = product_handle_from_url(product_url) or ""
    if not handle:
        raise RuntimeError(f"Could not infer handle from URL: {product_url}")

    scraped_at = now_utc_iso()
    variants_js = scrape_product_js(session, product_url, ignore_robots, user_agent)

    if variants_js is not None:
        title_raw = variants_js[0].get("__title__") if variants_js and isinstance(variants_js[0], dict) else None
        artist, album = parse_artist_album(title_raw or "")
        rows: List[VariantRow] = []

        if len(variants_js) == 1 and set(variants_js[0].keys()) == {"__title__"}:
            rows.append(
                VariantRow(
                    "recordsonvinyl",
                    scraped_at,
                    product_url,
                    handle,
                    title_raw,
                    artist,
                    album,
                    0,
                    None,
                    None,
                    None,
                    "EUR",
                    None,
                    None,
                    None,
                    None,
                )
            )
            safe_sleep(delay_product)
            return rows

        for variant in variants_js:
            if not isinstance(variant, dict):
                continue
            row = _variant_row_from_shopify_variant(product_url, handle, scraped_at, title_raw, variant, "EUR", None)
            if row is not None:
                rows.append(row)

        safe_sleep(delay_product)
        return rows

    # HTML fallback should be rare; product JSON remains the primary source.
    title_raw, artist, album, currency, availability_raw, variants = scrape_product_html(
        session,
        product_url,
        ignore_robots,
        user_agent,
    )
    rows = []
    if not variants:
        rows.append(
            VariantRow(
                "recordsonvinyl",
                scraped_at,
                product_url,
                handle,
                title_raw,
                artist,
                album,
                0,
                None,
                None,
                None,
                currency or "EUR",
                None,
                None,
                availability_to_bool(availability_raw),
                availability_raw,
            )
        )
        safe_sleep(delay_product)
        return rows

    for variant in variants:
        if not isinstance(variant, dict):
            continue
        row = _variant_row_from_shopify_variant(
            product_url,
            handle,
            scraped_at,
            title_raw,
            variant,
            currency or "EUR",
            availability_raw,
        )
        if row is not None:
            rows.append(row)

    safe_sleep(delay_product)
    return rows


# -----------------------------
# Collection scanning
# -----------------------------


def _iter_collection_pages(collection_url: str, max_pages: int) -> Iterable[Tuple[int, str]]:
    page = 1
    while True:
        if max_pages > 0 and page > max_pages:
            return
        yield page, set_query_param(collection_url, page=str(page))
        page += 1


def discover_product_urls(
    session: requests.Session,
    collection_url: str,
    max_pages: int,
    delay_listing: float,
    ignore_robots: bool,
    user_agent: str,
) -> List[str]:
    base = f"{urlparse(collection_url).scheme}://{urlparse(collection_url).netloc}"
    seen_handles: set[str] = set()
    all_urls: List[str] = []
    no_new_pages = 0

    for page_num, page_url in _iter_collection_pages(collection_url, max_pages):
        if not ignore_robots and not robots_allows(session, base, page_url, user_agent):
            raise RuntimeError(f"Blocked by robots.txt: {page_url}")

        try:
            response = get_with_retry(session, page_url)
        except Exception as exc:
            print(
                f"WARN(listing_page_fetch_failed) page={page_num} url={page_url} error={exc}. "
                f"Continuing with {len(all_urls)} discovered product URLs."
            )
            break

        if response.status_code != 200:
            print(
                f"WARN(listing_page_non_200) page={page_num} url={page_url} "
                f"status={response.status_code}. Continuing with {len(all_urls)} discovered product URLs."
            )
            break

        soup = BeautifulSoup(response.text, "html.parser")
        page_new = 0

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/products/" not in href or href.startswith(("mailto:", "tel:")):
                continue
            full_url = canonical_product_url(normalize_url(urljoin(base, href)))
            handle = product_handle_from_url(full_url)
            if not handle or handle in seen_handles:
                continue
            seen_handles.add(handle)
            all_urls.append(full_url)
            page_new += 1

        print(f"PAGE {page_num} new={page_new} total_unique={len(all_urls)} url={page_url}")
        no_new_pages = no_new_pages + 1 if page_new == 0 else 0
        safe_sleep(delay_listing)
        if no_new_pages >= 2:
            break

    return all_urls


# Listing-card price refresh helpers
# ----------------------------------
# These helpers are deliberately strict. We only parse prices from inside the exact
# <a class="grid-product__link" href=".../products/{handle}"> card, and only from the
# known price container/selectors. This prevents discount badges, product IDs, adjacent
# cards, and other page numbers from being interpreted as product prices.


def _extract_price_candidates(text: str) -> List[float]:
    if not text:
        return []
    matches = re.findall(r"(?:€\s*)?(\d{1,5}[.,]\d{2})(?:\s*€)?", text)
    out: List[float] = []
    for match in matches:
        value = eur_str_to_float(match)
        if value is not None and value not in out:
            out.append(value)
    return out


def _first_price_from_element(element: Optional[BeautifulSoup]) -> Optional[float]:
    if element is None:
        return None
    candidates = _extract_price_candidates(element.get_text(" ", strip=True))
    return candidates[0] if candidates else None


def _clean_price_box_for_regular_price(price_box: BeautifulSoup) -> BeautifulSoup:
    # BeautifulSoup tags are mutable; use a small re-parse so we do not damage the
    # original soup while removing text that is not the actual product price.
    cleaned = BeautifulSoup(str(price_box), "html.parser")
    for selector in [
        "style",
        ".visually-hidden",
        ".flair-badge-layout",
        ".flair-badge",
        ".grid-product__price--original",
        ".sale-price-emphasis",
    ]:
        for node in cleaned.select(selector):
            node.decompose()
    return cleaned


def parse_listing_prices_from_card(card: BeautifulSoup) -> Tuple[Optional[float], Optional[float]]:
    """Return (price_offer, price_list) from one exact RecordsOnVinyl product card.

    Sale example:
      .grid-product__price--original = €44.95  -> price_list
      .sale-price-emphasis           = €29.95  -> price_offer

    Regular-price example:
      .grid-product__price contains €37.95     -> price_offer
      price_list remains None
    """
    price_box = card.select_one(".grid-product__price")
    if price_box is None:
        return None, None

    sale_el = price_box.select_one(".sale-price-emphasis")
    original_el = price_box.select_one(".grid-product__price--original")

    if sale_el is not None:
        offer = _first_price_from_element(sale_el)
        list_price = _first_price_from_element(original_el)
        if offer is None:
            return None, None
        if list_price is not None and list_price <= offer:
            list_price = None
        return offer, list_price

    # No explicit sale price: parse only the cleaned price box. This avoids reading
    # hidden labels, flair badges such as "-33%", or old-price elements as the price.
    cleaned_price_box = _clean_price_box_for_regular_price(price_box)
    candidates = _extract_price_candidates(cleaned_price_box.get_text(" ", strip=True))
    if not candidates:
        return None, None
    return candidates[0], None


def discover_listing_price_entries(
    session: requests.Session,
    collection_url: str,
    max_pages: int,
    delay_listing: float,
    ignore_robots: bool,
    user_agent: str,
    limit_entries: int = 0,
) -> List[Tuple[str, Optional[float], Optional[float], Optional[str]]]:
    """Discover listing-card prices as (product_url, offer, list, title).

    A temporary 429/5xx on a later page is not fatal. The function returns the entries
    already collected so the pipeline can still export/import partial refresh results.
    """
    base = f"{urlparse(collection_url).scheme}://{urlparse(collection_url).netloc}"
    seen_handles: set[str] = set()
    entries: List[Tuple[str, Optional[float], Optional[float], Optional[str]]] = []
    no_new_pages = 0

    for page_num, page_url in _iter_collection_pages(collection_url, max_pages):
        if limit_entries and len(entries) >= limit_entries:
            break

        if not ignore_robots and not robots_allows(session, base, page_url, user_agent):
            raise RuntimeError(f"Blocked by robots.txt: {page_url}")

        try:
            response = get_with_retry(session, page_url)
        except Exception as exc:
            print(
                f"WARN(listing_page_fetch_failed) page={page_num} url={page_url} error={exc}. "
                f"Continuing with {len(entries)} listing price entries."
            )
            break

        if response.status_code != 200:
            print(
                f"WARN(listing_page_non_200) page={page_num} url={page_url} "
                f"status={response.status_code}. Continuing with {len(entries)} listing price entries."
            )
            break

        soup = BeautifulSoup(response.text, "html.parser")
        page_new = 0
        page_with_price = 0

        for card in soup.select('a.grid-product__link[href*="/products/"]'):
            href = card.get("href", "")
            if not href or href.startswith(("mailto:", "tel:")):
                continue

            full_url = canonical_product_url(normalize_url(urljoin(base, href)))
            handle = product_handle_from_url(full_url)
            if not handle or handle in seen_handles:
                continue

            seen_handles.add(handle)
            title_el = card.select_one(".grid-product__title")
            title = title_el.get_text(" ", strip=True) if title_el else None
            offer, list_price = parse_listing_prices_from_card(card)
            entries.append((full_url, offer, list_price, title))
            page_new += 1
            if offer is not None:
                page_with_price += 1

            if limit_entries and len(entries) >= limit_entries:
                break

        print(
            f"PAGE {page_num} new={page_new} with_price={page_with_price} "
            f"total_unique={len(entries)} url={page_url}"
        )
        no_new_pages = no_new_pages + 1 if page_new == 0 else 0
        safe_sleep(delay_listing)
        if no_new_pages >= 2:
            break

    return entries


# -----------------------------
# CSV state helpers
# -----------------------------


MASTER_COLUMNS = [field.name for field in dataclasses.fields(VariantRow)]


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def load_master_df(master_csv: str) -> pd.DataFrame:
    path = Path(master_csv)
    if not path.exists():
        return pd.DataFrame(columns=MASTER_COLUMNS)
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8-sig")
    for col in MASTER_COLUMNS:
        if col not in df.columns:
            df[col] = ""
    return df[MASTER_COLUMNS].copy()


def save_master_df(df: pd.DataFrame, master_csv: str) -> None:
    ensure_parent(master_csv)
    ordered = df.copy()
    for col in MASTER_COLUMNS:
        if col not in ordered.columns:
            ordered[col] = ""
    ordered = ordered[MASTER_COLUMNS]
    ordered.to_csv(master_csv, index=False)


def rows_to_df(rows: List[VariantRow]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=MASTER_COLUMNS)
    return pd.DataFrame([dataclasses.asdict(row) for row in rows], columns=MASTER_COLUMNS)


def _master_key(row: pd.Series) -> Tuple[str, str]:
    variant_id = str(row.get("variant_id", "")).strip()
    handle = str(row.get("handle", "")).strip()
    if variant_id and variant_id not in {"0", "nan", "None"}:
        return "variant_id", variant_id
    return "handle", handle


def merge_rows_into_master(master_df: pd.DataFrame, new_rows: List[VariantRow]) -> pd.DataFrame:
    if not new_rows:
        return master_df.copy()

    existing = master_df.copy()
    if existing.empty:
        existing = pd.DataFrame(columns=MASTER_COLUMNS)

    for col in MASTER_COLUMNS:
        if col not in existing.columns:
            existing[col] = ""

    # Build an index map without relying on DataFrame.set_index assignment quirks.
    key_to_pos: Dict[Tuple[str, str], int] = {}
    for pos, (_, row) in enumerate(existing.iterrows()):
        key = _master_key(row)
        if key[1]:
            key_to_pos[key] = pos

    rows_as_dicts = [dataclasses.asdict(row) for row in new_rows]

    for incoming in rows_as_dicts:
        incoming_series = pd.Series(incoming)
        key = _master_key(incoming_series)
        if not key[1]:
            continue

        if key in key_to_pos:
            pos = key_to_pos[key]
            for col in MASTER_COLUMNS:
                new_val = incoming.get(col, "")
                # These operational fields should always reflect the latest scrape.
                if col in {
                    "source",
                    "scraped_at",
                    "product_url",
                    "handle",
                    "currency",
                    "price_offer",
                    "price_list",
                    "available",
                    "availability_raw",
                }:
                    existing.iat[pos, existing.columns.get_loc(col)] = "" if new_val is None else new_val
                else:
                    if str(new_val).strip() != "":
                        existing.iat[pos, existing.columns.get_loc(col)] = new_val
        else:
            existing = pd.concat([existing, pd.DataFrame([incoming], columns=MASTER_COLUMNS)], ignore_index=True)
            key_to_pos[key] = len(existing) - 1

    return existing[MASTER_COLUMNS].copy()


def _safe_float_from_cell(value: Any) -> Optional[float]:
    if value is None or str(value).strip() == "":
        return None
    try:
        return round(float(str(value).replace(",", ".")), 2)
    except Exception:
        return None


def _price_delta_too_large(old_price: Optional[float], new_price: Optional[float], max_delta: float = 0.60) -> bool:
    """
    Guardrail only. We still trust Shopify JSON, but this logs improbable jumps.
    max_delta=0.60 allows normal retail price changes while surfacing extreme bad data.
    """
    if old_price is None or new_price is None or old_price <= 0:
        return False
    return abs(new_price - old_price) / old_price > max_delta


def _effective_handle_from_master_row(row: pd.Series) -> str:
    handle = str(row.get("handle", "")).strip()
    if handle:
        return handle
    return product_handle_from_url(str(row.get("product_url", "")).strip()) or ""


def _master_variant_identity(row: pd.Series) -> str:
    variant_id = str(row.get("variant_id", "")).strip()
    if variant_id and variant_id not in {"0", "nan", "None", ""}:
        return f"variant:{variant_id}"
    ean = str(row.get("ean13", "")).strip()
    if ean:
        return f"ean:{ean}"
    return "single_or_unknown"


def update_prices_in_master(
    master_df: pd.DataFrame,
    entries: List[Tuple[str, Optional[float], Optional[float], Optional[str]]],
) -> Tuple[pd.DataFrame, int, int]:
    """Update existing master prices from strict listing-card entries.

    Matching key is product handle extracted from the product URL. This function never
    creates new rows and never matches by title. Handles with multiple distinct variants
    are skipped because one product-card price cannot safely update multiple variant rows.
    """
    if master_df is None or master_df.empty:
        print("[WARN] No master rows available; listing refresh cannot create products without EANs.")
        return pd.DataFrame(columns=MASTER_COLUMNS), 0, len(entries)

    df = master_df.copy()
    for col in MASTER_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    handle_to_indices: Dict[str, List[int]] = {}
    for idx, row in df.iterrows():
        handle = _effective_handle_from_master_row(row)
        if handle:
            handle_to_indices.setdefault(handle, []).append(idx)

    scraped_at = now_utc_iso()
    updated_rows = 0
    skipped = 0
    missing_in_master = 0
    no_price = 0
    multi_variant = 0

    for product_url, offer, list_price, title in entries:
        handle = product_handle_from_url(product_url) or ""
        if not handle:
            skipped += 1
            continue

        if offer is None:
            no_price += 1
            skipped += 1
            continue

        indices = handle_to_indices.get(handle, [])
        if not indices:
            missing_in_master += 1
            skipped += 1
            continue

        identities = {_master_variant_identity(df.loc[idx]) for idx in indices}
        if len(identities) > 1:
            multi_variant += 1
            skipped += len(indices)
            print(
                "SKIP(multi_variant_listing_price) "
                f"handle={handle} rows={len(indices)} variants={sorted(identities)} title={title or ''}"
            )
            continue

        for idx in indices:
            old_price = _safe_float_from_cell(df.at[idx, "price_offer"])
            if _price_delta_too_large(old_price, offer):
                print(
                    "WARN(large_price_delta_from_listing) "
                    f"handle={handle} old={old_price} new={offer} url={product_url}"
                )

            df.at[idx, "scraped_at"] = scraped_at
            df.at[idx, "product_url"] = product_url
            df.at[idx, "handle"] = handle
            df.at[idx, "currency"] = "EUR"
            df.at[idx, "price_offer"] = f"{offer:.2f}"
            df.at[idx, "price_list"] = "" if list_price is None else f"{list_price:.2f}"
            updated_rows += 1

    print(
        "[INFO] Listing refresh summary: "
        f"entries={len(entries)} updated_rows={updated_rows} skipped={skipped} "
        f"missing_in_master={missing_in_master} no_price={no_price} multi_variant_handles={multi_variant}"
    )
    return df[MASTER_COLUMNS].copy(), updated_rows, skipped


def refresh_master_from_shopify_json(
    master_df: pd.DataFrame,
    refreshed_rows: List[VariantRow],
) -> Tuple[pd.DataFrame, int, int]:
    if not refreshed_rows:
        return master_df.copy(), 0, 0

    before = master_df.copy()
    merged = merge_rows_into_master(master_df, refreshed_rows)

    # Count how many incoming rows updated/inserted a usable variant.
    updated_or_inserted = 0
    skipped = 0

    old_prices_by_key: Dict[Tuple[str, str], Optional[float]] = {}
    if not before.empty:
        for _, row in before.iterrows():
            key = _master_key(row)
            if key[1]:
                old_prices_by_key[key] = _safe_float_from_cell(row.get("price_offer"))

    for row in refreshed_rows:
        row_dict = dataclasses.asdict(row)
        key = _master_key(pd.Series(row_dict))
        if not key[1]:
            skipped += 1
            continue
        old_price = old_prices_by_key.get(key)
        if _price_delta_too_large(old_price, row.price_offer):
            print(
                "WARN(large_price_delta_from_shopify_json) "
                f"handle={row.handle} variant_id={row.variant_id} old={old_price} new={row.price_offer}"
            )
        updated_or_inserted += 1

    return merged, updated_or_inserted, skipped


def export_latest(master_csv: str, out_csv: str) -> int:
    df = load_master_df(master_csv)
    ensure_parent(out_csv)
    df.to_csv(out_csv, index=False)
    return len(df)


# -----------------------------
# Thread-local sessions
# -----------------------------


_thread_local = threading.local()


def get_thread_session(user_agent: str) -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = make_session(user_agent)
        _thread_local.session = session
    return session


def scrape_task(args_tuple: Tuple[str, float, bool, str]) -> Tuple[str, List[VariantRow]]:
    url, delay_product, ignore_robots, user_agent = args_tuple
    session = get_thread_session(user_agent)
    rows = scrape_product(
        session,
        url,
        delay_product=delay_product,
        ignore_robots=ignore_robots,
        user_agent=user_agent,
    )
    return url, rows


def scrape_many_products(
    product_urls: List[str],
    delay_product: float,
    ignore_robots: bool,
    user_agent: str,
    workers: int = 1,
) -> Tuple[List[VariantRow], int]:
    scraped_rows: List[VariantRow] = []
    processed = 0

    if workers and workers > 1 and len(product_urls) > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(scrape_task, (url, delay_product, ignore_robots, user_agent)): url
                for url in product_urls
            }
            for future in as_completed(futures):
                url = futures[future]
                try:
                    _, rows = future.result()
                    scraped_rows.extend(rows)
                    processed += 1
                    print(f"PROGRESS {processed}/{len(product_urls)} url={url} variants={len(rows)}")
                except Exception as exc:
                    processed += 1
                    print(f"PROGRESS {processed}/{len(product_urls)} url={url} WARN(fetch_failed) {exc}")
        return scraped_rows, processed

    session = make_session(user_agent)
    for url in product_urls:
        try:
            rows = scrape_product(
                session,
                url,
                delay_product=delay_product,
                ignore_robots=ignore_robots,
                user_agent=user_agent,
            )
            scraped_rows.extend(rows)
            processed += 1
            print(f"PROGRESS {processed}/{len(product_urls)} url={url} variants={len(rows)}")
        except Exception as exc:
            processed += 1
            print(f"PROGRESS {processed}/{len(product_urls)} url={url} WARN(fetch_failed) {exc}")

    return scraped_rows, processed




def unique_product_urls_from_master(master_df: pd.DataFrame) -> List[str]:
    """Return stable, de-duplicated product URLs already present in the master CSV.

    Refresh should update known products. It should not first crawl the listing pages,
    because listing pages can rate-limit/503 and were also the source of the old wrong
    price parsing issue. New products are the job of the crawl workflow.
    """
    if master_df is None or master_df.empty or "product_url" not in master_df.columns:
        return []

    seen_handles: set[str] = set()
    urls: List[str] = []
    for raw_url in master_df["product_url"].fillna("").astype(str).tolist():
        url = canonical_product_url(normalize_url(raw_url.strip()))
        if not url or "/products/" not in url:
            continue
        handle = product_handle_from_url(url)
        key = handle or url
        if key in seen_handles:
            continue
        seen_handles.add(key)
        urls.append(url)
    return urls


# -----------------------------
# Commands
# -----------------------------


def cmd_crawl(args: argparse.Namespace) -> None:
    session = make_session(args.user_agent)
    master_csv = args.master_csv
    master_df = load_master_df(master_csv)

    product_urls = discover_product_urls(
        session,
        args.collection_url,
        args.max_pages,
        args.delay_listing,
        args.ignore_robots,
        args.user_agent,
    )

    existing_handles = set(master_df["handle"].astype(str).tolist()) if not master_df.empty else set()
    to_scrape: List[str] = []
    for url in product_urls:
        handle = product_handle_from_url(url)
        if not handle:
            continue
        if not args.rescrape and handle in existing_handles:
            continue
        to_scrape.append(url)

    if args.limit_products and args.limit_products > 0:
        to_scrape = to_scrape[: args.limit_products]

    print(f"[INFO] Discovered URLs: {len(product_urls)}; planned to scrape: {len(to_scrape)}")
    scraped_rows, processed = scrape_many_products(
        to_scrape,
        delay_product=args.delay_product,
        ignore_robots=args.ignore_robots,
        user_agent=args.user_agent,
        workers=max(1, getattr(args, "workers", 1)),
    )

    merged = merge_rows_into_master(master_df, scraped_rows)
    save_master_df(merged, master_csv)
    if getattr(args, "out", None):
        export_latest(master_csv, args.out)

    print(
        f"[OK] Done. processed_products={processed} "
        f"variant_rows={len(scraped_rows)} master_rows={len(merged)} out={args.out or '-'}"
    )


def cmd_refresh_prices_fast(args: argparse.Namespace) -> None:
    """
    Fast guarded listing-price refresh.

    The command name is preserved so recordsonvinyl_automation.py and the GitHub Actions
    workflow do not need to change. Internally this scans collection/listing pages and
    reads prices only from the exact product card linked to each /products/{handle} URL.

    It updates existing master rows by handle only. It does not create new products,
    because listing pages do not contain reliable EAN/barcode data.
    """
    session = make_session(args.user_agent)
    master_csv = args.master_csv
    master_df = load_master_df(master_csv)

    limit_entries = args.limit_products if args.limit_products and args.limit_products > 0 else 0
    entries = discover_listing_price_entries(
        session,
        args.collection_url,
        args.max_pages,
        args.delay_listing,
        args.ignore_robots,
        args.user_agent,
        limit_entries=limit_entries,
    )

    print(
        f"[INFO] Listing refresh entries discovered: {len(entries)} "
        f"limit_entries={limit_entries or 'none'}"
    )

    updated_df, updated_rows, skipped = update_prices_in_master(master_df, entries)
    save_master_df(updated_df, master_csv)
    if getattr(args, "out", None):
        export_latest(master_csv, args.out)

    print(
        f"[OK] Done. listing_entries={len(entries)} updated_master_rows={updated_rows} "
        f"skipped={skipped} out={args.out or '-'}"
    )


def cmd_export(args: argparse.Namespace) -> None:
    n = export_latest(args.master_csv, args.out)
    print(f"[OK] Exported {n} rows -> {args.out}")


# -----------------------------
# Interactive launcher (no-args)
# -----------------------------


def _ask_int(prompt: str, default: int) -> int:
    while True:
        value = input(f"{prompt} [{default}]: ").strip()
        if not value:
            return default
        try:
            return int(value)
        except ValueError:
            print(" -> geef een geheel getal op")


def _ask_path(prompt: str, default: str) -> str:
    value = input(f"{prompt} [{default}]: ").strip()
    return value or default


def default_outfile(tag: str) -> str:
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return os.path.join("output", f"recordsonvinyl_{tag}_{ts}.csv")


def interactive_main() -> argparse.Namespace:
    print("=== RecordsOnVinyl scraper ===")
    print("1) Crawl (discover + scrape product pages)")
    print("2) Refresh prices FAST (listing cards, link-bound)")
    print("3) Export snapshot (no scraping)")
    choice = input("Kies 1/2/3 [1]: ").strip() or "1"
    if choice not in {"1", "2", "3"}:
        choice = "1"

    base = argparse.Namespace(
        master_csv="data/raw/recordsonvinyl/recordsonvinyl_master.csv",
        delay_listing=0.15,
        delay_product=0.60,
        ignore_robots=False,
        user_agent=UA,
        out=None,
        collection_url=DEFAULT_COLLECTION,
        max_pages=0,
        limit_products=0,
        rescrape=False,
        workers=1,
    )

    if choice == "1":
        base.max_pages = _ask_int("Max pages (0 = tot uitgeput)", 0)
        base.limit_products = _ask_int("Max records (0 = geen limiet)", 5000)
        base.out = default_outfile("crawl")
        base.cmd = "crawl"
        base.func = cmd_crawl
        return base

    if choice == "2":
        base.max_pages = _ask_int("Max pages (0 = tot uitgeput)", 0)
        base.limit_products = _ask_int("Max records (0 = geen limiet)", 0)
        base.out = default_outfile("prices_listing")
        base.cmd = "refresh-prices"
        base.func = cmd_refresh_prices_fast
        print(f"[INFO] Refresh prices FAST will scan listing cards: {base.collection_url}")
        return base

    base.out = _ask_path("Output CSV", default_outfile("export"))
    base.cmd = "export"
    base.func = cmd_export
    return base


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RecordsOnVinyl.nl scraper (CSV-native)")
    parser.add_argument("--master-csv", default="data/raw/recordsonvinyl/recordsonvinyl_master.csv", help="Master CSV path")
    parser.add_argument("--delay-listing", type=float, default=0.15)
    parser.add_argument("--delay-product", type=float, default=0.60)
    parser.add_argument("--ignore-robots", action="store_true")
    parser.add_argument("--user-agent", default=UA)
    parser.add_argument("--out", default=None)
    parser.add_argument("--workers", type=int, default=1)

    sub = parser.add_subparsers(dest="cmd", required=True)

    crawl = sub.add_parser("crawl")
    crawl.add_argument("--collection-url", default=DEFAULT_COLLECTION)
    crawl.add_argument("--max-pages", type=int, default=0)
    crawl.add_argument("--limit-products", type=int, default=0)
    crawl.add_argument("--rescrape", action="store_true")
    crawl.set_defaults(func=cmd_crawl)

    refresh = sub.add_parser("refresh-prices")
    refresh.add_argument("--collection-url", default=DEFAULT_COLLECTION)
    refresh.add_argument("--max-pages", type=int, default=0)
    refresh.add_argument("--limit-products", type=int, default=0)
    refresh.set_defaults(func=cmd_refresh_prices_fast)

    export = sub.add_parser("export")
    export.add_argument("--out", required=True)
    export.set_defaults(func=cmd_export)

    return parser


def main() -> None:
    import sys

    if len(sys.argv) == 1:
        args = interactive_main()
        args.func(args)
        return

    parser = build_arg_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
