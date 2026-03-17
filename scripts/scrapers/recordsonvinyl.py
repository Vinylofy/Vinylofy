#!/usr/bin/env python3
"""
RecordsOnVinyl.nl scraper — CSV-native edition

State is stored in CSV files instead of SQLite.

Supported flows
1) Crawl
   - discover product URLs from the collection
   - scrape product details via Shopify JSON / HTML fallback
   - merge into a master CSV snapshot
2) Refresh prices FAST
   - scan listing pages only
   - update price/availability for handles that have exactly one known variant in master CSV
3) Export
   - write the current master snapshot to a chosen CSV path
"""

from __future__ import annotations

import argparse
import csv
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
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

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
    s = 0
    for i, d in enumerate(digits[:-1]):
        s += 3 * d if (i + 1) % 2 == 0 else d
    calc = (10 - (s % 10)) % 10
    return calc == check


def clean_ean(candidate: Any) -> Optional[str]:
    if candidate is None:
        return None
    c = re.sub(r"\D", "", str(candidate))
    if len(c) == 13 and ean13_is_valid(c):
        return c
    return None


def cents_to_eur(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value) / 100.0, 2)
    except Exception:
        return None


def eur_str_to_float(s: str) -> Optional[float]:
    if not s:
        return None
    x = s.strip().replace("€", "").replace("\u20ac", "")
    x = re.sub(r"[^\d,\.]", "", x)
    x = x.replace(",", ".")
    try:
        return round(float(x), 2)
    except Exception:
        return None


def parse_artist_album(title_raw: str) -> Tuple[Optional[str], Optional[str]]:
    if not title_raw:
        return None, None
    t = title_raw.strip()
    t = re.sub(r"\s*\((?:\d+\s*)?LPs?\)\s*$", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*\(LP\)\s*$", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*\(CD\)\s*$", "", t, flags=re.IGNORECASE)
    t = re.sub(r"\s*\(Cassette\)\s*$", "", t, flags=re.IGNORECASE)
    for sep in [" - ", " – ", " — "]:
        if sep in t:
            a, b = t.split(sep, 1)
            return (a.strip() or None), (b.strip() or None)
    return None, (t or None)


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
    for lab in labels:
        if lab.lower() in low:
            return lab
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
    s = requests.Session()
    s.headers.update({"User-Agent": user_agent, "Accept-Language": "nl,en;q=0.8"})
    return s


def get_with_retry(session: requests.Session, url: str, timeout: int = 25, retries: int = 4) -> requests.Response:
    backoff = 1.0
    for attempt in range(retries + 1):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(f"HTTP {r.status_code}")
            return r
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
    r = session.get(robots_url, timeout=20)
    if r.status_code != 200:
        return None
    return r.text.splitlines()


def _get_cached_robots_lines(session: requests.Session, base_url: str, user_agent: str) -> Optional[List[str]]:
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


def robots_allows(session: requests.Session, base_url: str, target_url: str, user_agent: str) -> bool:
    lines = _get_cached_robots_lines(session, base_url, user_agent)
    if lines is None or lines == []:
        return True
    ua = None
    disallows: List[str] = []
    allows: List[str] = []
    for line in lines:
        line = line.split("#", 1)[0].strip()
        if not line:
            continue
        if line.lower().startswith("user-agent:"):
            ua = line.split(":", 1)[1].strip()
        elif ua and (ua == "*" or ua.lower() in user_agent.lower()):
            if line.lower().startswith("disallow:"):
                disallows.append(line.split(":", 1)[1].strip())
            elif line.lower().startswith("allow:"):
                allows.append(line.split(":", 1)[1].strip())
    path = urlparse(target_url).path or "/"
    for a in allows:
        if a and path.startswith(a):
            return True
    for d in disallows:
        if d and path.startswith(d):
            return False
    return True


# -----------------------------
# Product parsing
# -----------------------------
def product_handle_from_url(url: str) -> Optional[str]:
    p = urlparse(url)
    m = re.search(r"/products/([^/?#]+)", p.path)
    return m.group(1) if m else None


def scrape_product_js(session: requests.Session, product_url: str, ignore_robots: bool, user_agent: str) -> Optional[List[Dict[str, Any]]]:
    base = f"{urlparse(product_url).scheme}://{urlparse(product_url).netloc}"
    handle = product_handle_from_url(product_url)
    if not handle:
        return None
    js_url = f"{base}/products/{handle}.js"
    if not ignore_robots and not robots_allows(session, base, js_url, user_agent):
        raise RuntimeError(f"Blocked by robots.txt: {js_url}")
    r = get_with_retry(session, js_url)
    if r.status_code != 200:
        return None
    try:
        data = r.json()
    except Exception:
        return None
    title = data.get("title")
    variants = data.get("variants", [])
    if not isinstance(variants, list):
        return None
    out: List[Dict[str, Any]] = []
    for v in variants:
        if isinstance(v, dict):
            out.append(v)
    if out:
        out[0]["__title__"] = title
    else:
        out = [{"__title__": title}]
    return out


def scrape_product_html(session: requests.Session, product_url: str, ignore_robots: bool, user_agent: str) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], List[Dict[str, Any]]]:
    base = f"{urlparse(product_url).scheme}://{urlparse(product_url).netloc}"
    if not ignore_robots and not robots_allows(session, base, product_url, user_agent):
        raise RuntimeError(f"Blocked by robots.txt: {product_url}")
    r = get_with_retry(session, product_url)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} on {product_url}")
    html = r.text
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    title_raw = h1.get_text(strip=True) if h1 else None
    artist, album = parse_artist_album(title_raw or "")
    page_text = soup.get_text(" ", strip=True)
    availability_raw = find_best_availability_label(page_text)
    currency = "EUR"
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            j = __import__('json').loads(script.get_text(strip=True))
        except Exception:
            continue
        nodes = j if isinstance(j, list) else [j]
        for node in nodes:
            if isinstance(node, dict) and node.get("@type") == "Product":
                offers = node.get("offers")
                if isinstance(offers, dict):
                    cur = offers.get("priceCurrency")
                    if isinstance(cur, str) and cur.strip():
                        currency = cur.strip()
    m = re.search(r"\[\s*\{\s*\"id\"\s*:\s*\d+", html)
    variants: List[Dict[str, Any]] = []
    if m:
        start = m.start()
        i = start
        depth = 0
        in_str = False
        esc = False
        while i < len(html):
            ch = html[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
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
                            data = __import__('json').loads(chunk)
                            if isinstance(data, list):
                                variants = data
                        except Exception:
                            variants = []
                        break
            i += 1
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


def scrape_product(session: requests.Session, product_url: str, delay_product: float, ignore_robots: bool, user_agent: str) -> List[VariantRow]:
    handle = product_handle_from_url(product_url) or ""
    if not handle:
        raise RuntimeError(f"Could not infer handle from URL: {product_url}")
    scraped_at = now_utc_iso()
    variants_js = scrape_product_js(session, product_url, ignore_robots, user_agent)
    if variants_js is not None:
        title_raw = variants_js[0].get("__title__") if variants_js and isinstance(variants_js[0], dict) else None
        artist, album = parse_artist_album(title_raw or "")
        rows: List[VariantRow] = []
        if len(variants_js) == 1 and list(variants_js[0].keys()) == ["__title__"]:
            rows.append(VariantRow("recordsonvinyl", scraped_at, product_url, handle, title_raw, artist, album, 0, None, None, None, "EUR", None, None, None, None))
            safe_sleep(delay_product)
            return rows
        for v in variants_js:
            if not isinstance(v, dict):
                continue
            try:
                variant_id = int(v.get("id"))
            except Exception:
                continue
            ean13 = clean_ean(v.get("barcode") or "")
            sku = v.get("sku") or None
            variant_title = v.get("public_title") or v.get("title") or None
            price_offer = cents_to_eur(v.get("price"))
            price_list = cents_to_eur(v.get("compare_at_price"))
            if price_list is not None and price_offer is not None and price_list <= price_offer:
                price_list = None
            available_val = v.get("available")
            available = available_val if isinstance(available_val, bool) else None
            rows.append(VariantRow("recordsonvinyl", scraped_at, product_url, handle, title_raw, artist, album, variant_id, variant_title, sku, ean13, "EUR", price_offer, price_list, available, None))
        safe_sleep(delay_product)
        return rows
    title_raw, artist, album, currency, availability_raw, variants = scrape_product_html(session, product_url, ignore_robots, user_agent)
    rows: List[VariantRow] = []
    if not variants:
        rows.append(VariantRow("recordsonvinyl", scraped_at, product_url, handle, title_raw, artist, album, 0, None, None, None, currency or "EUR", None, None, availability_to_bool(availability_raw), availability_raw))
        safe_sleep(delay_product)
        return rows
    for v in variants:
        if not isinstance(v, dict):
            continue
        try:
            variant_id = int(v.get("id"))
        except Exception:
            continue
        ean13 = clean_ean(v.get("barcode") or "")
        sku = v.get("sku") or None
        variant_title = v.get("title") or v.get("public_title") or None
        price_offer = cents_to_eur(v.get("price"))
        price_list = cents_to_eur(v.get("compare_at_price"))
        if price_list is not None and price_offer is not None and price_list <= price_offer:
            price_list = None
        available_val = v.get("available")
        available = available_val if isinstance(available_val, bool) else availability_to_bool(availability_raw)
        rows.append(VariantRow("recordsonvinyl", scraped_at, product_url, handle, title_raw, artist, album, variant_id, variant_title, sku, ean13, currency or "EUR", price_offer, price_list, available, availability_raw))
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


def discover_product_urls(session: requests.Session, collection_url: str, max_pages: int, delay_listing: float, ignore_robots: bool, user_agent: str) -> List[str]:
    base = f"{urlparse(collection_url).scheme}://{urlparse(collection_url).netloc}"
    seen: set[str] = set()
    all_urls: List[str] = []
    no_new_pages = 0
    for page_num, page_url in _iter_collection_pages(collection_url, max_pages):
        if not ignore_robots and not robots_allows(session, base, page_url, user_agent):
            raise RuntimeError(f"Blocked by robots.txt: {page_url}")
        r = get_with_retry(session, page_url)
        if r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        links: List[str] = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/products/" not in href or href.startswith(("mailto:", "tel:")):
                continue
            links.append(normalize_url(urljoin(base, href)))
        new_count = 0
        for u in links:
            if u not in seen:
                seen.add(u)
                all_urls.append(u)
                new_count += 1
        print(f"PAGE {page_num} new={new_count} total_unique={len(all_urls)} url={page_url}")
        no_new_pages = (no_new_pages + 1) if new_count == 0 else 0
        safe_sleep(delay_listing)
        if no_new_pages >= 2:
            break
    return all_urls


def _extract_price_candidates(text: str) -> List[float]:
    if not text:
        return []
    matches = re.findall(r"(?:€\s*)?(\d{1,5}[.,]\d{2})(?:\s*€)?", text)
    out: List[float] = []
    for m in matches:
        v = eur_str_to_float(m)
        if v is not None and v not in out:
            out.append(v)
    return out


def parse_listing_prices_from_card(card: BeautifulSoup) -> Tuple[Optional[float], Optional[float]]:
    text = card.get_text(" ", strip=True)
    if re.search(r"€\s*\d+[.,]\d{2}\s*[-–—]\s*€\s*\d+[.,]\d{2}", text):
        return None, None
    prices = _extract_price_candidates(text)
    if not prices:
        return None, None
    if len(prices) == 1:
        return prices[0], None
    if len(prices) == 2:
        lo, hi = min(prices), max(prices)
        return (lo, hi) if hi > lo else (lo, None)
    return None, None


def discover_listing_price_entries(session: requests.Session, collection_url: str, max_pages: int, delay_listing: float, ignore_robots: bool, user_agent: str) -> List[Tuple[str, Optional[float], Optional[float], Optional[str]]]:
    base = f"{urlparse(collection_url).scheme}://{urlparse(collection_url).netloc}"
    seen: set[str] = set()
    out: List[Tuple[str, Optional[float], Optional[float], Optional[str]]] = []
    no_new_pages = 0
    for page_num, page_url in _iter_collection_pages(collection_url, max_pages):
        if not ignore_robots and not robots_allows(session, base, page_url, user_agent):
            raise RuntimeError(f"Blocked by robots.txt: {page_url}")
        r = get_with_retry(session, page_url)
        if r.status_code != 200:
            break
        soup = BeautifulSoup(r.text, "html.parser")
        anchors = [a for a in soup.find_all("a", href=True) if "/products/" in a["href"]]
        page_new = 0
        for a in anchors:
            href = a["href"]
            if href.startswith(("mailto:", "tel:")):
                continue
            full = normalize_url(urljoin(base, href))
            if full in seen:
                continue
            card = a.find_parent(["li", "div", "article", "section"]) or a
            offer, list_ = parse_listing_prices_from_card(card)
            if offer is None and list_ is None:
                continue
            seen.add(full)
            avail = find_best_availability_label(card.get_text(" ", strip=True))
            out.append((full, offer, list_, avail))
            page_new += 1
        print(f"PAGE {page_num} listing_price_items={page_new} total_unique={len(out)} url={page_url}")
        no_new_pages = (no_new_pages + 1) if page_new == 0 else 0
        safe_sleep(delay_listing)
        if no_new_pages >= 2:
            break
    return out


# -----------------------------
# CSV state helpers
# -----------------------------
MASTER_COLUMNS = [f.name for f in dataclasses.fields(VariantRow)]


def ensure_parent(path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


def load_master_df(master_csv: str) -> pd.DataFrame:
    path = Path(master_csv)
    if not path.exists():
        return pd.DataFrame(columns=MASTER_COLUMNS)
    df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding='utf-8-sig')
    for col in MASTER_COLUMNS:
        if col not in df.columns:
            df[col] = ''
    return df[MASTER_COLUMNS].copy()


def save_master_df(df: pd.DataFrame, master_csv: str) -> None:
    ensure_parent(master_csv)
    ordered = df.copy()
    for col in MASTER_COLUMNS:
        if col not in ordered.columns:
            ordered[col] = ''
    ordered = ordered[MASTER_COLUMNS]
    ordered.to_csv(master_csv, index=False)


def rows_to_df(rows: List[VariantRow]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=MASTER_COLUMNS)
    return pd.DataFrame([dataclasses.asdict(r) for r in rows], columns=MASTER_COLUMNS)


def merge_rows_into_master(master_df: pd.DataFrame, new_rows: List[VariantRow]) -> pd.DataFrame:
    if not new_rows:
        return master_df.copy()
    existing = master_df.copy()
    if existing.empty:
        existing = pd.DataFrame(columns=MASTER_COLUMNS)
    incoming = rows_to_df(new_rows)
    key_cols = ['variant_id']
    existing_idx = existing.set_index(key_cols, drop=False)
    for _, row in incoming.iterrows():
        key = row['variant_id']
        if key in existing_idx.index:
            for col in MASTER_COLUMNS:
                new_val = row[col]
                if col in {'source', 'scraped_at', 'product_url', 'handle', 'currency', 'price_offer', 'price_list', 'available', 'availability_raw'}:
                    existing_idx.at[key, col] = new_val
                else:
                    if str(new_val).strip() != '':
                        existing_idx.at[key, col] = new_val
        else:
            existing_idx.loc[key] = row
    merged = existing_idx.reset_index(drop=True)
    return merged[MASTER_COLUMNS].copy()


def update_prices_in_master(master_df: pd.DataFrame, entries: List[Tuple[str, Optional[float], Optional[float], Optional[str]]]) -> Tuple[pd.DataFrame, int, int]:
    if master_df.empty:
        return master_df.copy(), 0, len(entries)
    df = master_df.copy()
    counts = df.groupby('handle')['variant_id'].count().to_dict()
    updated = 0
    skipped = 0
    for url, offer, list_, avail_raw in entries:
        handle = product_handle_from_url(url)
        if not handle:
            skipped += 1
            print(f"SKIP(no_handle) url={url}")
            continue
        if counts.get(handle, 0) != 1:
            skipped += 1
            print(f"SKIP(multi_or_missing_variant:{counts.get(handle,0)}) url={url}")
            continue
        mask = df['handle'] == handle
        checked_at = now_utc_iso()
        df.loc[mask, 'product_url'] = url
        df.loc[mask, 'scraped_at'] = checked_at
        df.loc[mask, 'price_offer'] = '' if offer is None else f"{offer:.2f}"
        df.loc[mask, 'price_list'] = '' if list_ is None else f"{list_:.2f}"
        avail_bool = availability_to_bool(avail_raw)
        if avail_bool is None:
            df.loc[mask, 'available'] = ''
        else:
            df.loc[mask, 'available'] = 'True' if avail_bool else 'False'
        df.loc[mask, 'availability_raw'] = '' if avail_raw is None else avail_raw
        updated += int(mask.sum())
        print(f"UPDATED handle={handle} offer={offer} list={list_}")
    return df, updated, skipped


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
    s = getattr(_thread_local, 'session', None)
    if s is None:
        s = make_session(user_agent)
        _thread_local.session = s
    return s


def scrape_task(args_tuple: Tuple[str, float, bool, str]) -> Tuple[str, List[VariantRow]]:
    url, delay_product, ignore_robots, user_agent = args_tuple
    session = get_thread_session(user_agent)
    rows = scrape_product(session, url, delay_product=delay_product, ignore_robots=ignore_robots, user_agent=user_agent)
    return url, rows


# -----------------------------
# Commands
# -----------------------------
def cmd_crawl(args: argparse.Namespace) -> None:
    session = make_session(args.user_agent)
    master_csv = args.master_csv
    master_df = load_master_df(master_csv)
    product_urls = discover_product_urls(session, args.collection_url, args.max_pages, args.delay_listing, args.ignore_robots, args.user_agent)
    existing_handles = set(master_df['handle'].astype(str).tolist()) if not master_df.empty else set()
    to_scrape: List[str] = []
    for u in product_urls:
        handle = product_handle_from_url(u)
        if not handle:
            continue
        if (not args.rescrape) and (handle in existing_handles):
            continue
        to_scrape.append(u)
    if args.limit_products and args.limit_products > 0:
        to_scrape = to_scrape[: args.limit_products]
    print(f"[INFO] Discovered URLs: {len(product_urls)}; planned to scrape: {len(to_scrape)}")
    scraped_rows: List[VariantRow] = []
    processed = 0
    if args.workers and args.workers > 1 and len(to_scrape) > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {ex.submit(scrape_task, (u, args.delay_product, args.ignore_robots, args.user_agent)): u for u in to_scrape}
            for fut in as_completed(futures):
                url = futures[fut]
                try:
                    _, rows = fut.result()
                    scraped_rows.extend(rows)
                    processed += 1
                    print(f"PROGRESS {processed}/{len(to_scrape)} url={url} variants={len(rows)}")
                except Exception as e:
                    processed += 1
                    print(f"PROGRESS {processed}/{len(to_scrape)} url={url} WARN(fetch_failed) {e}")
    else:
        for u in to_scrape:
            rows = scrape_product(session, u, delay_product=args.delay_product, ignore_robots=args.ignore_robots, user_agent=args.user_agent)
            scraped_rows.extend(rows)
            processed += 1
            print(f"PROGRESS {processed}/{len(to_scrape)} url={u} variants={len(rows)}")
    merged = merge_rows_into_master(master_df, scraped_rows)
    save_master_df(merged, master_csv)
    if getattr(args, 'out', None):
        export_latest(master_csv, args.out)
    print(f"[OK] Done. processed_products={processed} variant_rows={len(scraped_rows)} master_rows={len(merged)} out={args.out or '-'}")


def cmd_refresh_prices_fast(args: argparse.Namespace) -> None:
    session = make_session(args.user_agent)
    master_csv = args.master_csv
    master_df = load_master_df(master_csv)
    entries = discover_listing_price_entries(session, args.collection_url, args.max_pages, args.delay_listing, args.ignore_robots, args.user_agent)
    if args.limit_products and args.limit_products > 0:
        entries = entries[: args.limit_products]
    print(f"[INFO] Listing price entries: {len(entries)} (safe updates only: single-variant handles)")
    updated_df, updated_variants, skipped = update_prices_in_master(master_df, entries)
    save_master_df(updated_df, master_csv)
    if getattr(args, 'out', None):
        export_latest(master_csv, args.out)
    print(f"[OK] Done. processed_listing_items={len(entries)} updated_variants={updated_variants} skipped={skipped} out={args.out or '-'}")


def cmd_export(args: argparse.Namespace) -> None:
    n = export_latest(args.master_csv, args.out)
    print(f"[OK] Exported {n} rows -> {args.out}")


# -----------------------------
# Interactive launcher (no-args)
# -----------------------------
def _ask_int(prompt: str, default: int) -> int:
    while True:
        s = input(f"{prompt} [{default}]: ").strip()
        if not s:
            return default
        try:
            return int(s)
        except ValueError:
            print("  -> geef een geheel getal op")


def _ask_path(prompt: str, default: str) -> str:
    s = input(f"{prompt} [{default}]: ").strip()
    return s or default


def default_outfile(tag: str) -> str:
    ts = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    return os.path.join("output", f"recordsonvinyl_{tag}_{ts}.csv")


def interactive_main() -> argparse.Namespace:
    print("=== RecordsOnVinyl scraper ===")
    print("1) Crawl (discover + scrape product pages)")
    print("2) Refresh prices FAST (listing pages only)")
    print("3) Export snapshot (no scraping)")
    choice = input("Kies 1/2/3 [1]: ").strip() or "1"
    if choice not in {"1", "2", "3"}:
        choice = "1"
    base = argparse.Namespace(
        master_csv="recordsonvinyl_products_master.csv",
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
        base.max_pages = 0
        base.limit_products = 0
        base.out = default_outfile("prices_fast")
        base.cmd = "refresh-prices"
        base.func = cmd_refresh_prices_fast
        print(f"[INFO] Refresh prices FAST will scan: {base.collection_url} (all pages)")
        return base
    base.out = _ask_path("Output CSV", default_outfile("export"))
    base.cmd = "export"
    base.func = cmd_export
    return base


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="RecordsOnVinyl.nl scraper (CSV-native)")
    p.add_argument("--master-csv", default="recordsonvinyl_products_master.csv", help="Master CSV path")
    p.add_argument("--delay-listing", type=float, default=0.15)
    p.add_argument("--delay-product", type=float, default=0.60)
    p.add_argument("--ignore-robots", action="store_true")
    p.add_argument("--user-agent", default=UA)
    p.add_argument("--out", default=None)
    p.add_argument("--workers", type=int, default=1)
    sub = p.add_subparsers(dest='cmd', required=True)
    c = sub.add_parser('crawl')
    c.add_argument('--collection-url', default=DEFAULT_COLLECTION)
    c.add_argument('--max-pages', type=int, default=0)
    c.add_argument('--limit-products', type=int, default=0)
    c.add_argument('--rescrape', action='store_true')
    c.set_defaults(func=cmd_crawl)
    rp = sub.add_parser('refresh-prices')
    rp.add_argument('--collection-url', default=DEFAULT_COLLECTION)
    rp.add_argument('--max-pages', type=int, default=0)
    rp.add_argument('--limit-products', type=int, default=0)
    rp.set_defaults(func=cmd_refresh_prices_fast)
    e = sub.add_parser('export')
    e.add_argument('--out', required=True)
    e.set_defaults(func=cmd_export)
    return p


def main() -> None:
    import sys
    if len(sys.argv) == 1:
        args = interactive_main()
        args.func(args)
        return
    parser = build_arg_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
