#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://3345.nl"
REQUEST_TIMEOUT = 30
LISTING_SLEEP_SECONDS = 0.15
SAVE_EVERY_REFRESHED_PRODUCTS = 25
DEFAULT_DETAIL_WORKERS = 8
THREAD_LOCAL = threading.local()

DEFAULT_LINKS_FILE = "3345_product_links.txt"
DEFAULT_CSV_FILE = "3345_products.csv"
DEFAULT_STATE_FILE = "3345_detail_rotation_state.json"

DISCOVERY_SOURCES: dict[str, str] = {
    "browse-all-music": BASE_URL + "/collections/browse-all-music?page={page}",
    "all": BASE_URL + "/collections/all?page={page}",
}

DETAIL_FIELDS = ("ean", "release_date", "genre", "style", "price", "availability")
NON_MUSIC_TOKENS = {
    "giftcard",
    "gift card",
    "tote",
    "bag",
    "slipmat",
    "brush",
    "cleaner",
    "cleaning",
    "inner sleeve",
    "outer sleeve",
    "sleeve",
    "plastic bag",
    "cap",
    "hoodie",
    "shirt",
    "poster",
    "book",
    "magazine",
    "voucher",
}

PRICE_EXCLUSION_CONTEXTS = (
    "free shipping",
    "orders over",
    "order over",
    "shipping over",
    "verzending",
    "gratis verzending",
    "bestellingen boven",
    "free delivery",
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9,nl;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def get_thread_session() -> requests.Session:
    session = getattr(THREAD_LOCAL, "session", None)
    if session is None:
        session = build_session()
        THREAD_LOCAL.session = session
    return session


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


# ---------------------------------------------------------------------------
# Generic text helpers
# ---------------------------------------------------------------------------
def clean_text(value: str | None) -> str:
    if value is None:
        return ""
    value = str(value).replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_price(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    value = value.replace("EUR", "€").replace("EURO", "€")
    m = re.search(r"€\s*([\d\.,]+)", value)
    if m:
        return f"€{clean_text(m.group(1))}"
    if re.fullmatch(r"[\d\.,]+", value):
        return f"€{value}"
    return value.replace("€ ", "€")


def first_match(pattern: str, text: str, flags: int = 0) -> str:
    match = re.search(pattern, text, flags)
    return clean_text(match.group(1)) if match else ""


def extract_between_labels(text: str, label_variants: Iterable[str], next_labels: Iterable[str]) -> str:
    for label in label_variants:
        label_re = re.escape(label)
        next_re = "|".join(re.escape(x) for x in next_labels)
        pattern = rf"{label_re}\s*(.*?)\s*(?=(?:{next_re})|$)"
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return clean_text(match.group(1))
    return ""


def canonicalize_product_url(url: str) -> str:
    if not url:
        return ""
    joined = urljoin(BASE_URL, url)
    parts = urlsplit(joined)
    path = parts.path or ""
    if path.startswith("/nl/products/"):
        path = path.replace("/nl/products/", "/products/", 1)
    path = re.sub(r"/+", "/", path)
    return urlunsplit(("https", "3345.nl", path.rstrip("/"), "", ""))


def parse_artist_title_from_string(value: str) -> tuple[str, str]:
    value = clean_text(value)
    if not value:
        return "", ""
    if " - " in value:
        artist, title = value.split(" - ", 1)
        return clean_text(artist), clean_text(title)
    return "", value


def looks_like_format_text(value: str) -> bool:
    value = clean_text(value).lower()
    return bool(
        value
        and (
            "lp" in value
            or "vinyl" in value
            or '7"' in value
            or '10"' in value
            or '12"' in value
            or value in {"ep", "cd"}
        )
    )


def strip_format_suffix_from_title(value: str) -> tuple[str, str]:
    value = clean_text(value)
    if not value:
        return "", ""

    match = re.search(r"\(([^\)]+)\)\s*$", value)
    if match:
        candidate = clean_text(match.group(1))
        if looks_like_format_text(candidate):
            title = clean_text(value[: match.start()])
            return title, candidate
    return value, ""


def _price_match_has_excluded_context(text: str, start: int, end: int) -> bool:
    context = clean_text(text[max(0, start - 80) : min(len(text), end + 80)]).lower()
    return any(token in context for token in PRICE_EXCLUSION_CONTEXTS)


def extract_price_from_text(text: str) -> str:
    text = clean_text(text)
    if not text:
        return ""

    matches = list(re.finditer(r"€\s*([\d\.,]+)", text, flags=re.IGNORECASE))
    if not matches:
        return ""

    for match in reversed(matches):
        if _price_match_has_excluded_context(text, match.start(), match.end()):
            continue
        return normalize_price(match.group(1))
    return ""


def looks_like_non_music_row(url: str, row: dict[str, str] | None = None) -> bool:
    row = row or {}
    haystack = " ".join(
        [
            clean_text(url),
            clean_text(row.get("artist")),
            clean_text(row.get("title")),
            clean_text(row.get("format")),
            clean_text(row.get("source_collection")),
        ]
    ).lower()
    return any(token in haystack for token in NON_MUSIC_TOKENS)


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------
def get_fieldnames() -> list[str]:
    return [
        "artist",
        "title",
        "ean",
        "release_date",
        "genre",
        "style",
        "format",
        "price",
        "url",
        "availability",
        "detail_status",
        "is_secondhand",
        "source_collection",
    ]


def empty_row(url: str) -> dict[str, str]:
    row = {field: "" for field in get_fieldnames()}
    row["url"] = canonicalize_product_url(url)
    row["detail_status"] = "listing_only"
    row["availability"] = "in_stock"
    row["is_secondhand"] = "false"
    return row


def read_links_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines = [canonicalize_product_url(line) for line in path.read_text(encoding="utf-8").splitlines()]
    return [line for line in lines if line]


def append_links(path: Path, links: Iterable[str]) -> list[str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = set(read_links_file(path))
    new_links = []
    for link in links:
        normalized = canonicalize_product_url(link)
        if not normalized or normalized in existing:
            continue
        existing.add(normalized)
        new_links.append(normalized)

    if not new_links:
        return []

    with path.open("a", encoding="utf-8") as f:
        for link in new_links:
            f.write(link + "\n")
    return new_links


def load_csv_rows_by_url(csv_path: Path, fieldnames: Sequence[str]) -> dict[str, dict[str, str]]:
    rows_by_url: dict[str, dict[str, str]] = {}
    if not csv_path.exists():
        return rows_by_url

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = canonicalize_product_url(clean_text(row.get("url")))
            if not url:
                continue
            normalized = {field: clean_text(row.get(field)) for field in fieldnames}
            normalized["url"] = url
            if not normalized.get("detail_status"):
                normalized["detail_status"] = "listing_only"
            if not normalized.get("availability"):
                normalized["availability"] = "in_stock"
            if not normalized.get("is_secondhand"):
                normalized["is_secondhand"] = "false"
            rows_by_url[url] = normalized
    return rows_by_url


def write_all_rows_to_csv(csv_path: Path, rows_by_url: dict[str, dict], fieldnames: Sequence[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for url in sorted(rows_by_url.keys()):
            row = {field: clean_text(rows_by_url[url].get(field)) for field in fieldnames}
            row["url"] = url
            writer.writerow(row)


def merge_row(existing: dict[str, str] | None, incoming: dict[str, str]) -> dict[str, str]:
    merged = empty_row(incoming.get("url") or (existing or {}).get("url") or "")
    if existing:
        merged.update({k: clean_text(v) for k, v in existing.items()})

    for key, value in incoming.items():
        value = clean_text(value)
        if not value:
            continue
        merged[key] = value

    merged["url"] = canonicalize_product_url(merged.get("url"))
    return merged


def row_is_missing_details(row: dict[str, str] | None) -> bool:
    if not row:
        return True
    required_fields = set(DETAIL_FIELDS) | {"price", "availability"}
    return any(not clean_text(row.get(field)) for field in required_fields)


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------
def load_state(path: Path) -> dict:
    if not path.exists():
        return {
            "discovery": {"next_page_by_source": {}},
            "detail": {"refresh_cursor": 0, "backfill_cursor": 0},
            "meta": {"updated_at": utc_now_iso()},
        }
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("state must be an object")
    except Exception:
        return {
            "discovery": {"next_page_by_source": {}},
            "detail": {"refresh_cursor": 0, "backfill_cursor": 0},
            "meta": {"updated_at": utc_now_iso()},
        }
    data.setdefault("discovery", {}).setdefault("next_page_by_source", {})
    data.setdefault("detail", {}).setdefault("refresh_cursor", 0)
    data.setdefault("detail", {}).setdefault("backfill_cursor", 0)
    data.setdefault("meta", {})
    return data


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state.setdefault("meta", {})["updated_at"] = utc_now_iso()
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def rotate_slice(items: Sequence[str], limit: int, cursor: int) -> tuple[list[str], int]:
    items = [clean_text(x) for x in items if clean_text(x)]
    if not items or limit <= 0:
        return [], cursor
    if cursor < 0:
        cursor = 0
    cursor = cursor % len(items)
    if limit >= len(items):
        return list(items), 0
    result = list(items[cursor : cursor + limit])
    if len(result) < limit:
        result.extend(items[: limit - len(result)])
    next_cursor = (cursor + limit) % len(items)
    return result, next_cursor


# ---------------------------------------------------------------------------
# Listing pages
# ---------------------------------------------------------------------------
def card_candidates_for_anchor(anchor: Tag) -> list[Tag]:
    candidates: list[Tag] = []
    node = anchor
    for _ in range(8):
        parent = getattr(node, "parent", None)
        if not isinstance(parent, Tag):
            break
        candidates.append(parent)
        node = parent
    return candidates


def _candidate_product_links(candidate: Tag) -> list[str]:
    links: list[str] = []
    for a in candidate.select("a[href*='/products/']"):
        href = canonicalize_product_url(clean_text(a.get("href")))
        if href:
            links.append(href)
    return links


def _candidate_price_nodes(candidate: Tag) -> list[Tag]:
    selectors = (
        ".price-item--sale",
        ".price__sale .price-item",
        ".price-item--regular",
        ".price__regular .price-item",
        ".price .price-item",
        ".card-information .price",
    )
    nodes: list[Tag] = []
    seen_ids: set[int] = set()
    for selector in selectors:
        for node in candidate.select(selector):
            if not isinstance(node, Tag):
                continue
            ident = id(node)
            if ident in seen_ids:
                continue
            text = clean_text(node.get_text(" ", strip=True))
            if not extract_price_from_text(text):
                continue
            seen_ids.add(ident)
            nodes.append(node)
    return nodes


def pick_card_for_anchor(anchor: Tag) -> Tag:
    target_url = canonicalize_product_url(clean_text(anchor.get("href")))
    title_text = clean_text(anchor.get_text(" ", strip=True))
    best = anchor
    best_score: tuple[int, int, int, int] | None = None

    for candidate in card_candidates_for_anchor(anchor):
        text = clean_text(candidate.get_text(" ", strip=True))
        if not text or len(text) > 900:
            continue

        product_links = _candidate_product_links(candidate)
        unique_links = sorted(set(product_links))
        if not unique_links:
            continue

        same_target_count = sum(1 for link in product_links if link == target_url)
        foreign_links = [link for link in unique_links if link != target_url]
        price_nodes = _candidate_price_nodes(candidate)
        if not price_nodes:
            continue

        score = 0
        # Strongly prefer the smallest ancestor that still isolates exactly this product.
        if target_url and unique_links == [target_url]:
            score += 12
        elif target_url and same_target_count >= 1 and not foreign_links:
            score += 10
        elif target_url and same_target_count >= 1:
            score += 2
        else:
            continue

        if title_text and title_text in text:
            score += 2

        if candidate.select_one(".card__information") or candidate.select_one(".card-information"):
            score += 2

        if len(price_nodes) == 1:
            score += 3
        elif len(price_nodes) <= 2:
            score += 1
        else:
            score -= 3

        # Smaller blocks are better once they contain a valid price and only this product.
        score_tuple = (score, -len(unique_links), -len(text), same_target_count)
        if best_score is None or score_tuple > best_score:
            best_score = score_tuple
            best = candidate

    return best


def extract_listing_price(card: Tag) -> str:
    price_nodes = _candidate_price_nodes(card)
    if price_nodes:
        # Prefer sale, then regular, then any other price node, but only inside this card.
        for preferred in (".price-item--sale", ".price-item--regular", ".price__sale .price-item", ".price__regular .price-item"):
            for node in card.select(preferred):
                if not isinstance(node, Tag):
                    continue
                text = clean_text(node.get_text(" ", strip=True))
                price = extract_price_from_text(text)
                if price:
                    return price

        for node in price_nodes:
            text = clean_text(node.get_text(" ", strip=True))
            price = extract_price_from_text(text)
            if price:
                return price

    return ""


def extract_listing_vendor(card: Tag) -> str:
    selectors = [
        ".card__vendor",
        ".product-card__vendor",
        ".vendor",
        ".card-information__text .caption-with-letter-spacing",
    ]
    for selector in selectors:
        node = card.select_one(selector)
        if node:
            text = clean_text(node.get_text(" ", strip=True))
            if text and not re.search(r"€\s*[\d\.,]+", text):
                return text
    return ""


def extract_listing_availability(card: Tag) -> str:
    text = clean_text(card.get_text(" ", strip=True)).lower()
    if any(token in text for token in ("pre order", "pre-order", "preorder")):
        return "preorder"
    if any(token in text for token in ("sold out", "uitverkocht", "out of stock")):
        return "out_of_stock"
    return "in_stock"


def extract_listing_row_from_anchor(anchor: Tag, source_name: str = "") -> dict[str, str] | None:
    href = clean_text(anchor.get("href"))
    url = canonicalize_product_url(href)
    if not url or "/products/" not in url:
        return None

    full_title = clean_text(anchor.get_text(" ", strip=True))
    if not full_title:
        return None

    card = pick_card_for_anchor(anchor)
    vendor = extract_listing_vendor(card)
    # 3345 listing pages can leak site-wide prices such as the free-shipping threshold.
    # Detail pages are authoritative for product price, so keep listing discovery price empty.
    price = ""
    availability = extract_listing_availability(card)

    artist, title = parse_artist_title_from_string(full_title)
    title_no_format, format_from_title = strip_format_suffix_from_title(title)
    if title_no_format:
        title = title_no_format
    if not artist and vendor:
        artist = vendor

    row = empty_row(url)
    row.update(
        {
            "artist": artist,
            "title": title or full_title,
            "format": format_from_title,
            "price": price,
            "url": url,
            "availability": availability,
            "source_collection": source_name,
            "is_secondhand": "false",
        }
    )
    return row


def extract_listing_rows(html: str, source_name: str = "") -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    selectors = [
        "a.full-unstyled-link.notranslate[href*='/products/']",
        "a.full-unstyled-link[href*='/products/']",
        "a[href*='/products/']",
    ]
    anchors: list[Tag] = []
    for selector in selectors:
        anchors = [a for a in soup.select(selector) if isinstance(a, Tag)]
        if anchors:
            break

    for anchor in anchors:
        row = extract_listing_row_from_anchor(anchor, source_name=source_name)
        if not row:
            continue
        url = row["url"]
        if url in seen or looks_like_non_music_row(url, row):
            continue
        seen.add(url)
        rows.append(row)
    return rows


def scrape_listing_pages(
    session: requests.Session,
    links_file: Path,
    state_file: Path,
    source_names: Sequence[str] | None = None,
    max_pages_per_source: int = 15,
    csv_path: Path | None = None,
) -> list[str]:
    source_names = list(source_names or ["browse-all-music", "all"])
    fieldnames = get_fieldnames()
    rows_by_url = load_csv_rows_by_url(csv_path, fieldnames) if csv_path else {}
    state = load_state(state_file)
    page_state: dict[str, int] = state.setdefault("discovery", {}).setdefault("next_page_by_source", {})

    total_new_links = 0
    collected_new_links: list[str] = []

    for source_name in source_names:
        template = DISCOVERY_SOURCES.get(source_name)
        if not template:
            print(f"[DISCOVERY] onbekende source overgeslagen: {source_name}")
            continue
        start_page = int(page_state.get(source_name, 1) or 1)
        source_new = 0
        source_seen = 0
        page = start_page

        for _ in range(max(1, int(max_pages_per_source or 1))):
            url = template.format(page=page)
            try:
                html = fetch_html(session, url)
                page_rows = extract_listing_rows(html, source_name=source_name)
            except Exception as exc:
                print(f"[DISCOVERY {source_name} p{page}] FOUT bij ophalen: {exc}")
                break

            if not page_rows:
                print(f"[DISCOVERY {source_name} p{page}] geen productlinks; reset naar pagina 1")
                page = 1
                break

            links = [row["url"] for row in page_rows]
            new_links = append_links(links_file, links)
            source_new += len(new_links)
            total_new_links += len(new_links)
            collected_new_links.extend(new_links)

            missing_price = 0
            for row in page_rows:
                source_seen += 1
                if csv_path:
                    current = rows_by_url.get(row["url"])
                    merged = merge_row(current, row)
                    if not merged.get("price"):
                        missing_price += 1
                    rows_by_url[row["url"]] = merged
            if csv_path and page_rows:
                write_all_rows_to_csv(csv_path, rows_by_url, fieldnames)

            print(
                f"[DISCOVERY {source_name} p{page}] producten={len(page_rows)} | nieuw={len(new_links)} | "
                f"zonder_prijs={missing_price} | source_totaal_nieuw={source_new}"
            )

            page += 1
            time.sleep(LISTING_SLEEP_SECONDS)

        page_state[source_name] = max(1, page)
        print(f"[DISCOVERY {source_name}] gezien={source_seen} | nieuw={source_new} | volgende_pagina={page_state[source_name]}")

    save_state(state_file, state)
    return collected_new_links


# ---------------------------------------------------------------------------
# Detail pages
# ---------------------------------------------------------------------------
def extract_json_ld_product(soup: BeautifulSoup) -> dict:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for item in candidates:
            if not isinstance(item, dict):
                continue
            if item.get("@type") == "Product":
                return item
    return {}


def extract_artist_and_title(soup: BeautifulSoup, page_text: str) -> tuple[str, str]:
    artist = ""
    title = ""
    h1 = soup.find("h1")
    if h1:
        full_h1 = clean_text(h1.get_text(" ", strip=True))
        artist_link = h1.find("a")
        if artist_link:
            artist = clean_text(artist_link.get_text(" ", strip=True))
        if artist and full_h1.lower().startswith(artist.lower()):
            remainder = full_h1[len(artist):].strip().lstrip("-–—: ").strip()
            title = remainder
        elif " - " in full_h1:
            left, right = full_h1.split(" - ", 1)
            artist = artist or clean_text(left)
            title = clean_text(right)
        else:
            title = full_h1
    if not artist:
        h1_text = first_match(r"#\s*(.+?)\n", page_text)
        if " - " in h1_text:
            artist, title = [clean_text(x) for x in h1_text.split(" - ", 1)]
    return artist, title


def extract_detail_availability(soup: BeautifulSoup, page_text: str) -> str:
    price_root = select_price_root(soup)
    scope = price_root.find_parent(class_="product__price-quantity") if price_root else None
    if not isinstance(scope, Tag):
        scope = soup.select_one(".product__price-quantity") or soup.select_one(".product-form") or soup.select_one("main")
    scope_text = clean_text(scope.get_text(" ", strip=True)).lower() if isinstance(scope, Tag) else clean_text(page_text).lower()

    if any(token in scope_text for token in ("sold out", "uitverkocht", "out of stock")):
        return "out_of_stock"
    if any(token in scope_text for token in ("pre order", "pre-order", "preorder", "coming soon")):
        return "preorder"

    cta_texts: list[str] = []
    has_enabled_cart_form = False
    for node in soup.select("form[action$='/cart/add'], form[action*='/cart/add']"):
        text = clean_text(node.get_text(" ", strip=True) or node.get("value"))
        if text:
            cta_texts.append(text.lower())
        disabled_descendant = node.select_one("button[disabled], input[disabled], button[aria-disabled='true'], input[aria-disabled='true']")
        if disabled_descendant is None:
            has_enabled_cart_form = True

    for node in soup.select("button[name='add'], button.product-form__submit, button[type='submit'], input[type='submit']"):
        text = clean_text(node.get_text(" ", strip=True) or node.get("value"))
        if text:
            cta_texts.append(text.lower())

    if any(("sold out" in t) or ("out of stock" in t) or ("uitverkocht" in t) for t in cta_texts):
        return "out_of_stock"
    if any(token in t for t in cta_texts for token in ("pre order", "pre-order", "preorder", "coming soon")):
        return "preorder"
    if any("add to cart" in t or "toevoegen aan winkelwagen" in t or t == "toevoegen" for t in cta_texts):
        return "in_stock"
    if has_enabled_cart_form:
        return "in_stock"

    return "in_stock"

def _extract_first_price(node: Tag | None) -> str:
    if not isinstance(node, Tag):
        return ""
    text = clean_text(node.get_text(" ", strip=True))
    match = re.search(r"€\s*([\d\.,]+)", text)
    if not match:
        return ""
    return normalize_price(match.group(1))


def select_price_root(soup: BeautifulSoup) -> Tag | None:
    candidates = [node for node in soup.select("div[id^='price-template']") if isinstance(node, Tag)]
    if not candidates:
        return None

    def score(node: Tag) -> tuple[int, int]:
        node_id = clean_text(node.get("id"))
        score = 0
        if "__main" in node_id:
            score += 4
        if isinstance(node.find_parent(class_="product__price-quantity"), Tag):
            score += 3
        if node.select_one(".price-item--sale"):
            score += 2
        if node.select_one(".price-item--regular"):
            score += 1
        text_len = len(clean_text(node.get_text(" ", strip=True)))
        return (score, -abs(text_len - 80))

    return max(candidates, key=score)


def extract_price(soup: BeautifulSoup, page_text: str) -> str:
    price_root = select_price_root(soup)

    def _scan_text_for_price(text: str) -> str:
        text = clean_text(text)
        if not text:
            return ""
        for pattern in (
            r"Default\s+Title(?:\s*-\s*(?:sold\s*out|out\s+of\s+stock|uitverkocht))?\s*-\s*€\s*([\d\.,]+)",
            r"Sale\s+price\s*€\s*([\d\.,]+)",
            r"Regular\s+price\s*€\s*([\d\.,]+)",
            r"Aanbiedingsprijs\s*€\s*([\d\.,]+)",
            r"Normale\s+prijs\s*€\s*([\d\.,]+)",
            r"(?:sold\s*out|out\s+of\s+stock|uitverkocht)\s*-\s*€\s*([\d\.,]+)",
        ):
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return normalize_price(match.group(1))
        generic_matches = list(re.finditer(r"€\s*([\d\.,]+)", text, flags=re.IGNORECASE))
        for match in reversed(generic_matches):
            if _price_match_has_excluded_context(text, match.start(), match.end()):
                continue
            return normalize_price(match.group(1))
        return ""

    if isinstance(price_root, Tag):
        for selector in (
            ".price-item--sale",
            ".price__sale .price-item",
            ".price-item--regular",
            ".price__regular .price-item",
        ):
            for node in price_root.select(selector):
                price = _extract_first_price(node)
                if price:
                    return price

        price = _scan_text_for_price(price_root.get_text(" ", strip=True))
        if price:
            return price

        parent = price_root.find_parent(class_="product__price-quantity")
        if isinstance(parent, Tag):
            price = _scan_text_for_price(parent.get_text(" ", strip=True))
            if price:
                return price

    price = _scan_text_for_price(page_text)
    if price:
        return price

    return ""

def extract_detail_fields(html: str, url: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)
    page_text = re.sub(r"\n+", "\n", page_text)

    product_json = extract_json_ld_product(soup)
    artist, title = extract_artist_and_title(soup, page_text)

    ean = first_match(r"Barcode:\s*([0-9A-Za-z\-]+)", page_text)
    if not ean:
        ean = first_match(r"gtin13\s*[:=]\s*\"?([0-9]{8,14})\"?", html, flags=re.IGNORECASE)
    if not ean:
        ean = clean_text(product_json.get("gtin13") or product_json.get("gtin"))

    release_date = extract_between_labels(
        page_text,
        label_variants=("Release Date", "Release"),
        next_labels=(
            "Genre:",
            "Style:",
            "Format:",
            "Product variants",
            "Quantity:",
            "Regular price",
            "Normale prijs",
            "Sale price",
            "Aanbiedingsprijs",
        ),
    )
    genre = extract_between_labels(
        page_text,
        label_variants=("Genre:",),
        next_labels=(
            "Style:",
            "Format:",
            "Product variants",
            "Quantity:",
            "Regular price",
            "Normale prijs",
            "Sale price",
            "Aanbiedingsprijs",
        ),
    )
    style = extract_between_labels(
        page_text,
        label_variants=("Style:",),
        next_labels=(
            "Format:",
            "Product variants",
            "Quantity:",
            "Regular price",
            "Normale prijs",
            "Sale price",
            "Aanbiedingsprijs",
        ),
    )
    format_value = extract_between_labels(
        page_text,
        label_variants=("Format:",),
        next_labels=(
            "Product variants",
            "Quantity:",
            "Regular price",
            "Normale prijs",
            "Sale price",
            "Aanbiedingsprijs",
            "Unit price",
            "Pre Order",
            "Out Of Stock",
            "Tracklist",
            "Related Products:",
        ),
    )

    if not artist:
        brand = product_json.get("brand")
        if isinstance(brand, dict):
            artist = clean_text(brand.get("name"))
        elif isinstance(brand, str):
            artist = clean_text(brand)
    if not title:
        name = clean_text(product_json.get("name"))
        if name and artist and name.lower().startswith(artist.lower()):
            title = clean_text(name[len(artist):].lstrip("-–—: "))
        else:
            title = name
    if not format_value:
        name = clean_text(product_json.get("name"))
        m = re.search(r"\(([^\)]+)\)\s*$", name)
        if m:
            format_value = clean_text(m.group(1))
    price = extract_price(soup, page_text)
    if not price:
        offers = product_json.get("offers")
        if isinstance(offers, dict) and offers.get("price"):
            price = normalize_price(str(offers["price"]).replace(".", ","))
        elif isinstance(offers, list):
            for offer in offers:
                if isinstance(offer, dict) and offer.get("price"):
                    price = normalize_price(str(offer["price"]).replace(".", ","))
                    break

    availability = extract_detail_availability(soup, page_text)
    detail_status = "ok" if ean else "missing_ean"

    row = {
        "artist": artist,
        "title": title,
        "ean": ean,
        "release_date": release_date,
        "genre": genre,
        "style": style,
        "format": format_value,
        "price": price,
        "url": canonicalize_product_url(url),
        "availability": availability,
        "detail_status": detail_status,
        "is_secondhand": "false",
    }
    return row


def fetch_detail_row(url: str) -> dict[str, str]:
    session = get_thread_session()
    html = fetch_html(session, url)
    return extract_detail_fields(html, url)


def scrape_product_details(
    session: requests.Session | None,
    links: Sequence[str],
    csv_path: Path,
    update_existing: bool = True,
    workers: int = DEFAULT_DETAIL_WORKERS,
    state_file: Path | None = None,
    rows_by_url: dict[str, dict[str, str]] | None = None,
    status_prefix: str = "DETAIL",
) -> int:
    _ = session  # compatibility with current automation wrapper
    if not links:
        print(f"[{status_prefix}] Geen links om te scrapen.")
        return 0

    fieldnames = get_fieldnames()
    if rows_by_url is None:
        rows_by_url = load_csv_rows_by_url(csv_path, fieldnames)

    unique_urls = []
    seen = set()
    for url in links:
        normalized = canonicalize_product_url(url)
        if normalized and normalized not in seen and not looks_like_non_music_row(normalized, rows_by_url.get(normalized)):
            seen.add(normalized)
            unique_urls.append(normalized)

    total = len(unique_urls)
    written = 0
    completed = 0
    max_workers = max(1, int(workers or 1))

    def _apply_row(url: str, row: dict[str, str]) -> None:
        current = rows_by_url.get(url) if update_existing else None
        incoming = dict(row)

        incoming_status = clean_text(incoming.get("detail_status"))
        incoming_price = clean_text(incoming.get("price"))
        current_price = clean_text((current or {}).get("price"))

        # 3345 detail pages are authoritative for price.
        # Listing/discovery rows should never overwrite an existing detail price,
        # and a detail row should be allowed to correct a stale listing price.
        if incoming_status == "listing_only":
            incoming["price"] = ""
        elif not incoming_price and current_price:
            incoming["price"] = current_price

        rows_by_url[url] = merge_row(current, incoming)

    if max_workers == 1:
        for idx, url in enumerate(unique_urls, start=1):
            try:
                row = fetch_detail_row(url)
                _apply_row(url, row)
                written += 1
                completed += 1
                merged = rows_by_url[url]
                print(
                    f"[{status_prefix} {idx}/{total}] verwerkt | ean={merged.get('ean') or '-'} | "
                    f"price={merged.get('price') or '-'} | avail={merged.get('availability') or '-'} | "
                    f"status={merged.get('detail_status') or '-'}"
                )
                if completed % SAVE_EVERY_REFRESHED_PRODUCTS == 0:
                    write_all_rows_to_csv(csv_path, rows_by_url, fieldnames)
                    print(f"[{status_prefix}] tussentijds opgeslagen na {completed} producten")
            except Exception as exc:
                completed += 1
                print(f"[{status_prefix} {idx}/{total}] FOUT bij {url}: {exc}")
        write_all_rows_to_csv(csv_path, rows_by_url, fieldnames)
        return written

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(fetch_detail_row, url): url for url in unique_urls}
        for idx, future in enumerate(as_completed(future_to_url), start=1):
            url = future_to_url[future]
            try:
                row = future.result()
                _apply_row(url, row)
                written += 1
                merged = rows_by_url[url]
                print(
                    f"[{status_prefix} {idx}/{total}] verwerkt | ean={merged.get('ean') or '-'} | "
                    f"price={merged.get('price') or '-'} | avail={merged.get('availability') or '-'} | "
                    f"status={merged.get('detail_status') or '-'}"
                )
            except Exception as exc:
                print(f"[{status_prefix} {idx}/{total}] FOUT bij {url}: {exc}")
            finally:
                completed += 1
                if completed % SAVE_EVERY_REFRESHED_PRODUCTS == 0:
                    write_all_rows_to_csv(csv_path, rows_by_url, fieldnames)
                    print(f"[{status_prefix}] tussentijds opgeslagen na {completed} producten")

    write_all_rows_to_csv(csv_path, rows_by_url, fieldnames)
    if state_file:
        state = load_state(state_file)
        save_state(state_file, state)
    return written


# ---------------------------------------------------------------------------
# Target selection
# ---------------------------------------------------------------------------
def pick_detail_targets_from_listing(
    rows_by_url: dict[str, dict[str, str]],
    listing_urls_seen: Sequence[str],
    new_links: Sequence[str],
    limit_details: int,
    state_file: Path,
) -> list[str]:
    limit = max(0, int(limit_details or 0))
    if limit == 0:
        return []
    state = load_state(state_file)
    cursor = int(state.setdefault("detail", {}).get("refresh_cursor", 0) or 0)

    priority: list[str] = []
    fallback: list[str] = []
    seen = set()
    new_links_set = {canonicalize_product_url(url) for url in new_links if canonicalize_product_url(url)}

    for url in list(new_links) + list(listing_urls_seen):
        nurl = canonicalize_product_url(url)
        if not nurl or nurl in seen:
            continue
        row = rows_by_url.get(nurl)
        if looks_like_non_music_row(nurl, row):
            continue
        seen.add(nurl)
        if nurl in new_links_set or row_is_missing_details(row):
            priority.append(nurl)
        else:
            fallback.append(nurl)

    selected_priority = priority[:limit]
    remaining = max(0, limit - len(selected_priority))
    selected_fallback, next_cursor = rotate_slice(fallback, remaining, cursor)
    selected = selected_priority + [url for url in selected_fallback if url not in set(selected_priority)]

    state["detail"]["refresh_cursor"] = next_cursor
    save_state(state_file, state)
    print(
        f"[SELECT refresh-known] priority={len(priority)} | fallback={len(fallback)} | gekozen={len(selected)} | cursor={cursor}->{next_cursor}"
    )
    return selected


def select_backfill_targets(
    links_file: Path,
    csv_file: Path,
    limit_details: int,
    state_file: Path,
) -> list[str]:
    links = read_links_file(links_file)
    rows_by_url = load_csv_rows_by_url(csv_file, get_fieldnames())
    candidates: list[str] = []
    seen = set()
    for url in links:
        row = rows_by_url.get(url)
        if not row or looks_like_non_music_row(url, row):
            continue
        if row.get("ean") and not row_is_missing_details(row):
            continue
        if url in seen:
            continue
        seen.add(url)
        candidates.append(url)

    state = load_state(state_file)
    cursor = int(state.setdefault("detail", {}).get("backfill_cursor", 0) or 0)
    selected, next_cursor = rotate_slice(candidates, max(0, int(limit_details or 0)), cursor)
    state["detail"]["backfill_cursor"] = next_cursor
    save_state(state_file, state)
    print(
        f"[SELECT backfill] kandidaten={len(candidates)} | gekozen={len(selected)} | cursor={cursor}->{next_cursor}"
    )
    return selected


def select_links_for_detail_refresh(
    links_file: Path,
    csv_file: Path,
    limit_details: int,
    state_file: Path,
    strategy: str = "refresh-known",
) -> list[str]:
    rows_by_url = load_csv_rows_by_url(csv_file, get_fieldnames())
    links = read_links_file(links_file)
    if strategy == "backfill":
        return select_backfill_targets(links_file, csv_file, limit_details, state_file)
    listing_like = [url for url in links if rows_by_url.get(url)]
    return pick_detail_targets_from_listing(rows_by_url, listing_like, [], limit_details, state_file)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="3345.nl listing-first scraper")
    parser.add_argument(
        "--mode",
        choices=["new", "refresh", "both", "menu", "links", "details", "refresh-details", "discovery"],
        default="menu",
    )
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--max-pages", type=int, default=15)
    parser.add_argument("--links-file", default=DEFAULT_LINKS_FILE)
    parser.add_argument("--csv-file", default=DEFAULT_CSV_FILE)
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--detail-workers", type=int, default=DEFAULT_DETAIL_WORKERS)
    parser.add_argument("--limit-details", type=int, default=250)
    parser.add_argument("--source", action="append", dest="sources", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    mode = args.mode
    if mode == "links":
        mode = "new"
    elif mode == "details":
        mode = "refresh-details"
    elif mode == "discovery":
        mode = "new"

    links_file = Path(args.links_file)
    csv_file = Path(args.csv_file)
    state_file = Path(args.state_file)
    session = build_session()

    if mode in {"new", "refresh", "both"}:
        new_links = scrape_listing_pages(
            session=session,
            links_file=links_file,
            state_file=state_file,
            source_names=args.sources or ["browse-all-music", "all"],
            max_pages_per_source=max(1, args.max_pages),
            csv_path=csv_file,
        )
        rows_by_url = load_csv_rows_by_url(csv_file, get_fieldnames())
        listing_urls_seen = list(rows_by_url.keys())
        print(f"[LINKS] klaar. Nieuw opgeslagen links: {len(new_links)}")
        if mode == "refresh":
            print("[REFRESH] klaar. Prijzen/listing-data zijn via collectiepagina's bijgewerkt.")
        else:
            targets = pick_detail_targets_from_listing(
                rows_by_url=rows_by_url,
                listing_urls_seen=listing_urls_seen,
                new_links=new_links,
                limit_details=args.limit_details,
                state_file=state_file,
            )
            written = scrape_product_details(
                session=session,
                links=targets,
                csv_path=csv_file,
                update_existing=True,
                workers=args.detail_workers,
                state_file=state_file,
                rows_by_url=rows_by_url,
            )
            print(f"[DETAIL] klaar. Bijgewerkte productregels: {written}")
    elif mode == "refresh-details":
        rows_by_url = load_csv_rows_by_url(csv_file, get_fieldnames())
        targets = read_links_file(links_file)
        written = scrape_product_details(
            session=session,
            links=targets,
            csv_path=csv_file,
            update_existing=True,
            workers=args.detail_workers,
            state_file=state_file,
            rows_by_url=rows_by_url,
            status_prefix="REFRESH-DETAILS",
        )
        print(f"[REFRESH-DETAILS] klaar. Bijgewerkte productregels: {written}")
    else:
        print("Menu mode is not implemented in this repo build; use CLI arguments.")
        return 2

    print("Klaar.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
