#!/usr/bin/env python3
"""
3345.nl scraper tuned for three separate automation flows:
1. discovery       -> build and rotate the frontier of product URLs
2. refresh-known   -> refresh URLs that already produced a valid EAN
3. backfill        -> try new / unmatched URLs with strict retry rules

This keeps the daily detail budget focused on URLs that can actually improve
Vinylofy output, while still expanding coverage over time.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://3345.nl"
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_REQUESTS = 0.35
SAVE_EVERY_REFRESHED_PRODUCTS = 25
DEFAULT_REFRESH_WORKERS = 10
DEFAULT_STATE_FILE = "3345_detail_rotation_state.json"
DEFAULT_LINKS_FILE = "3345_product_links.txt"
DEFAULT_CSV_FILE = "3345_products.csv"

DISCOVERY_SOURCES: dict[str, str] = {
    "browse-all-music": BASE_URL + "/collections/browse-all-music?page={page}",
    "all": BASE_URL + "/collections/all?page={page}",
}

FIELDNAMES = [
    "artist",
    "title",
    "ean",
    "release_date",
    "genre",
    "style",
    "format",
    "price",
    "availability",
    "url",
    "source_collection",
    "detail_status",
    "is_secondhand",
]

NON_MUSIC_TOKENS = {
    "giftcard",
    "gift-card",
    "gift card",
    "tote",
    "slipmat",
    "brush",
    "cleaner",
    "cleaning",
    "stylus",
    "cartridge",
    "needle",
    "hoodie",
    "t-shirt",
    "tshirt",
    "shirt",
    "cap",
    "mug",
    "poster",
    "turntable",
    "headphones",
    "adapter",
    "merch",
    "loyalty-program",
    "loyalty program",
    "shop-visit",
    "sell-your-vinyl",
}

MUSIC_FORMAT_TOKENS = {
    "lp",
    "2lp",
    "3lp",
    "4lp",
    "5lp",
    '7"',
    '10"',
    '12"',
    "vinyl",
    "ep",
    "single",
    "cd",
    "cassette",
    "blu-ray",
    "dvd",
}


# ---------------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_price(value: str | None) -> str:
    value = clean_text(value)
    if not value:
        return ""

    value = value.replace("EUR", "").replace("€", "")
    value = clean_text(value).replace(" ", "")

    if "," in value and "." in value:
        value = value.replace(".", "").replace(",", ".")
    elif "," in value:
        value = value.replace(",", ".")

    try:
        numeric = float(value)
    except Exception:
        return ""

    return f"€{numeric:.2f}".replace(".", ",")


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


def normalize_availability(value: str | None) -> str:
    raw = clean_text(value).lower().replace("-", "_").replace(" ", "_")
    if raw in {"out_of_stock", "sold_out", "items_no_longer_available"}:
        return "out_of_stock"
    if raw in {"pre_order", "preorder", "coming_soon"}:
        return "preorder"
    if raw in {"in_stock", "available"}:
        return "in_stock"
    return "in_stock"


def looks_like_music_format(value: str | None) -> bool:
    haystack = clean_text(value).lower()
    return any(token in haystack for token in MUSIC_FORMAT_TOKENS)


def looks_like_secondhand(url: str, row: dict[str, str] | None = None) -> bool:
    row = row or {}
    haystack = " ".join(
        [
            clean_text(url),
            clean_text(row.get("artist")),
            clean_text(row.get("title")),
            clean_text(row.get("genre")),
            clean_text(row.get("source_collection")),
        ]
    ).lower()
    return "used" in haystack or "second hand" in haystack or "second-hand" in haystack


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
    has_non_music_token = any(token in haystack for token in NON_MUSIC_TOKENS)
    looks_music_like = bool(clean_text(row.get("genre"))) or looks_like_music_format(row.get("format"))
    return has_non_music_token and not looks_music_like


def bool_to_str(value: bool) -> str:
    return "true" if value else "false"


def str_to_bool(value: str | None) -> bool:
    return clean_text(value).lower() in {"1", "true", "yes", "y"}


# ---------------------------------------------------------------------------
# State helpers
# ---------------------------------------------------------------------------
def load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def ensure_state_sections(state: dict) -> None:
    state.setdefault("links", {})
    state.setdefault("cursors", {})
    state.setdefault("runs", {})


def link_meta(state: dict, url: str) -> dict:
    ensure_state_sections(state)
    links = state["links"]
    value = links.get(url)
    if not isinstance(value, dict):
        value = {}
        links[url] = value
    return value


def increment_run_counter(state: dict, key: str) -> int:
    ensure_state_sections(state)
    runs = state["runs"]
    runs[key] = int(runs.get(key, 0) or 0) + 1
    return runs[key]


def iso_or_blank(value: str | None) -> str:
    return clean_text(value)


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


def fetch_html(session: requests.Session, url: str) -> str:
    response = session.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.text


# ---------------------------------------------------------------------------
# Listing pages / frontier building
# ---------------------------------------------------------------------------
def extract_product_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")

    found: list[str] = []
    seen: set[str] = set()
    selectors = [
        "a.full-unstyled-link.notranslate[href^='/products/']",
        "a.full-unstyled-link[href^='/products/']",
        "a[href^='/products/']",
    ]

    for selector in selectors:
        for anchor in soup.select(selector):
            href = clean_text(anchor.get("href"))
            if not href:
                continue

            full_url = urljoin(BASE_URL, href)
            if "/products/" not in full_url:
                continue
            if full_url in seen:
                continue

            seen.add(full_url)
            found.append(full_url)

        if found:
            break

    return found


def read_links_file(path: Path) -> list[str]:
    if not path.exists():
        return []
    lines = [clean_text(line) for line in path.read_text(encoding="utf-8").splitlines()]
    return [line for line in lines if line]


def write_links_file(path: Path, links: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(links) + ("\n" if links else ""), encoding="utf-8")


def append_links(path: Path, links: Iterable[str]) -> list[str]:
    existing = read_links_file(path)
    seen = set(existing)
    new_links: list[str] = []
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        existing.append(link)
        new_links.append(link)
    if new_links:
        write_links_file(path, existing)
    return new_links


def build_collection_url(source_name: str, page: int) -> str:
    template = DISCOVERY_SOURCES[source_name]
    return template.format(page=max(1, page))


def scrape_listing_pages(
    session: requests.Session,
    links_file: Path,
    state_file: Path,
    source_names: Sequence[str] | None = None,
    max_pages_per_source: int = 15,
) -> list[str]:
    state = load_state(state_file)
    ensure_state_sections(state)
    run_number = increment_run_counter(state, "discovery")
    source_names = list(source_names or DISCOVERY_SOURCES.keys())

    discovered_new_links: list[str] = []
    total_pages = 0

    for source_name in source_names:
        if source_name not in DISCOVERY_SOURCES:
            print(f"[DISCOVERY] onbekende source overgeslagen: {source_name}")
            continue

        cursor_key = f"discovery::{source_name}"
        cursor = int(state["cursors"].get(cursor_key, 1) or 1)
        start_page = max(1, cursor)
        pages_visited = 0
        seen_signatures: set[tuple[str, ...]] = set()
        source_new_links = 0

        print(
            f"[DISCOVERY] source={source_name} | run={run_number} | start_page={start_page} | pages={max_pages_per_source}"
        )

        for offset in range(max_pages_per_source):
            page = start_page + offset
            url = build_collection_url(source_name, page)
            total_pages += 1
            pages_visited += 1

            try:
                html = fetch_html(session, url)
                links = extract_product_links(html)
            except Exception as exc:
                print(f"[DISCOVERY {source_name} p{page}] FOUT: {exc}")
                if offset == 0:
                    state["cursors"][cursor_key] = 1
                break

            if not links:
                print(f"[DISCOVERY {source_name} p{page}] gevonden=0 | stop")
                state["cursors"][cursor_key] = 1
                break

            signature = tuple(links)
            if signature in seen_signatures:
                print(f"[DISCOVERY {source_name} p{page}] herhaalde productset | reset naar pagina 1")
                state["cursors"][cursor_key] = 1
                break
            seen_signatures.add(signature)

            new_links = append_links(links_file, links)
            source_new_links += len(new_links)
            discovered_new_links.extend(new_links)

            now = utc_now_iso()
            for link in links:
                meta = link_meta(state, link)
                meta.setdefault("first_seen_at", now)
                meta["last_seen_in_listing_at"] = now
                meta["discovery_source"] = source_name
                meta.setdefault("blank_ean_attempts", 0)
            print(
                f"[DISCOVERY {source_name} p{page}] gevonden={len(links)} | nieuw={len(new_links)} | source_totaal={source_new_links}"
            )
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        else:
            state["cursors"][cursor_key] = start_page + max_pages_per_source

        if pages_visited and cursor_key not in state["cursors"]:
            state["cursors"][cursor_key] = start_page + pages_visited

    save_state(state_file, state)
    print(
        f"[DISCOVERY] sources={','.join(source_names)} | pages={total_pages} | nieuwe_links={len(discovered_new_links)} | state={state_file}"
    )
    return discovered_new_links


# ---------------------------------------------------------------------------
# Detail extraction
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
        expanded: list[dict] = []
        for item in candidates:
            if isinstance(item, dict) and isinstance(item.get("@graph"), list):
                expanded.extend(x for x in item["@graph"] if isinstance(x, dict))
            elif isinstance(item, dict):
                expanded.append(item)

        for item in expanded:
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
                remainder = full_h1[len(artist):].strip()
                remainder = remainder.lstrip("-–—: ").strip()
                title = remainder
        elif " - " in full_h1:
            left, right = full_h1.split(" - ", 1)
            artist = clean_text(left)
            title = clean_text(right)
        else:
            title = full_h1

    if not artist:
        h1_text = first_match(r"#\s*(.+?)\n", page_text)
        if " - " in h1_text:
            artist, title = [clean_text(x) for x in h1_text.split(" - ", 1)]

    return artist, title


def extract_price(soup: BeautifulSoup) -> str:
    selectors = [
        "main .price__sale .price-item",
        "main .price__regular .price-item",
        "main .price .price-item",
        "main [data-product-block] .price-item",
        "main .product__info-container .price-item",
    ]
    bad_tokens = {
        "unit price",
        "vanaf",
        "from",
        "compare at",
        "bespaar",
        "save",
        "free shipping",
        "shipping",
    }

    numeric_candidates: list[tuple[float, str]] = []
    seen: set[str] = set()

    for selector in selectors:
        for node in soup.select(selector):
            txt = clean_text(node.get_text(" ", strip=True))
            if not txt or "€" not in txt:
                continue
            lower = txt.lower()
            if any(token in lower for token in bad_tokens):
                continue
            if any(cls in (node.get("class") or []) for cls in ["visually-hidden", "sr-only"]):
                continue

            match = re.search(r"€\s*([\d\.,]+)", txt)
            if not match:
                continue

            normalized = normalize_price(match.group(1))
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            try:
                numeric_value = float(normalized.replace("€", "").replace(",", "."))
            except Exception:
                continue
            numeric_candidates.append((numeric_value, normalized))

    if numeric_candidates:
        numeric_candidates.sort(key=lambda x: x[0])
        return numeric_candidates[0][1]

    return ""


def extract_availability(soup: BeautifulSoup, page_text: str, product_json: dict) -> str:
    lower_text = page_text.lower()

    if "out of stock" in lower_text or "sold out" in lower_text or "items no longer available" in lower_text:
        return "out_of_stock"
    if "pre order" in lower_text or "pre-order" in lower_text or "coming soon" in lower_text:
        return "preorder"

    offers = product_json.get("offers")
    if isinstance(offers, dict):
        availability = normalize_availability(offers.get("availability"))
        if availability != "in_stock":
            return availability

    add_to_cart = soup.find(string=re.compile(r"add to cart", re.IGNORECASE))
    if add_to_cart:
        return "in_stock"

    return "in_stock"


def derive_detail_status(*, ean: str, secondhand: bool, non_music: bool) -> str:
    if non_music:
        return "non_music"
    if secondhand and not ean:
        return "secondhand_without_ean"
    if not ean:
        return "missing_ean"
    return "ok"


def extract_detail_fields(html: str, url: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text("\n", strip=True)
    page_text = re.sub(r"\n+", "\n", page_text)

    product_json = extract_json_ld_product(soup)
    artist, title = extract_artist_and_title(soup, page_text)

    ean = first_match(r"Barcode:\s*([0-9A-Za-z\-]+)", page_text)
    if not ean:
        ean = first_match(r"gtin13\s*[:=]\s*\"?([0-9]{8,14})\"?", html, flags=re.IGNORECASE)

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
            "Coming Soon",
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

    if not ean:
        ean = clean_text(product_json.get("gtin13") or product_json.get("gtin"))

    if not format_value:
        name = clean_text(product_json.get("name"))
        match = re.search(r"\(([^\)]+)\)\s*$", name)
        if match:
            format_value = clean_text(match.group(1))

    price = extract_price(soup)
    if not price:
        offers = product_json.get("offers")
        if isinstance(offers, dict):
            offer_price = offers.get("price")
            if offer_price:
                price = normalize_price(str(offer_price))

    availability = extract_availability(soup, page_text, product_json)

    secondhand = (
        "** note: this is a second-hand product!" in page_text.lower()
        or clean_text(title).lower().startswith("used")
        or clean_text(artist).lower().startswith("used")
    )

    row = {
        "artist": artist,
        "title": title,
        "ean": ean,
        "release_date": release_date,
        "genre": genre,
        "style": style,
        "format": format_value,
        "price": price,
        "availability": availability,
        "url": url,
        "source_collection": "",
        "is_secondhand": bool_to_str(secondhand),
        "detail_status": "",
    }
    row["detail_status"] = derive_detail_status(
        ean=row["ean"],
        secondhand=secondhand,
        non_music=looks_like_non_music_row(url, row),
    )
    return row


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------
def read_existing_csv_rows(csv_path: Path) -> dict[str, dict[str, str]]:
    rows_by_url: dict[str, dict[str, str]] = {}
    if not csv_path.exists():
        return rows_by_url

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            url = clean_text((row or {}).get("url"))
            if not url:
                continue
            normalized = {field: clean_text((row or {}).get(field, "")) for field in FIELDNAMES}
            rows_by_url[url] = normalized

    return rows_by_url


def write_all_rows_to_csv(csv_path: Path, rows_by_url: dict[str, dict[str, str]]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        for url in sorted(rows_by_url.keys()):
            writer.writerow({field: rows_by_url[url].get(field, "") for field in FIELDNAMES})


def append_row_to_csv(csv_path: Path, row: dict[str, str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = csv_path.exists()
    with csv_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in FIELDNAMES})


# ---------------------------------------------------------------------------
# Selection strategies
# ---------------------------------------------------------------------------
def _rotation_select(items: Sequence[str], limit: int, state: dict, rotation_key: str) -> list[str]:
    if not items or limit <= 0:
        return []
    ensure_state_sections(state)
    start_index = int(state["cursors"].get(rotation_key, 0) or 0) % len(items)
    ordered = list(items[start_index:]) + list(items[:start_index])
    selected = ordered[:limit]
    state["cursors"][rotation_key] = (start_index + len(selected)) % len(items)
    return selected


def _select_refresh_known(
    *,
    links_file: Path,
    csv_file: Path,
    limit_details: int,
    state_file: Path,
) -> list[str]:
    all_links = set(read_links_file(links_file))
    rows_by_url = read_existing_csv_rows(csv_file)
    state = load_state(state_file)
    ensure_state_sections(state)

    known_ean_links = [
        url
        for url, row in rows_by_url.items()
        if url in all_links and clean_text(row.get("ean"))
    ]
    selected = _rotation_select(known_ean_links, limit_details, state, "refresh_known_cursor")
    now = utc_now_iso()
    for url in selected:
        meta = link_meta(state, url)
        meta["last_selected_for"] = "refresh-known"
        meta["last_selected_at"] = now
    save_state(state_file, state)
    print(
        f"[SELECT refresh-known] met_ean={len(known_ean_links)} | geselecteerd={len(selected)} | limit={limit_details}"
    )
    return selected


def _backfill_retry_allowed(meta: dict, row: dict[str, str]) -> bool:
    retry_after = clean_text(meta.get("retry_after"))
    if retry_after and retry_after > utc_now_iso():
        return False

    attempts = int(meta.get("blank_ean_attempts", 0) or 0)
    if looks_like_non_music_row(meta.get("url") or "", row):
        return attempts < 1
    if looks_like_secondhand(meta.get("url") or "", row):
        return False
    return attempts < 3


def _set_next_retry(meta: dict, row: dict[str, str]) -> None:
    attempts = int(meta.get("blank_ean_attempts", 0) or 0)
    now_epoch = int(time.time())
    if looks_like_non_music_row(meta.get("url") or "", row):
        delay_days = 120
    elif looks_like_secondhand(meta.get("url") or "", row):
        delay_days = 60
    elif attempts <= 1:
        delay_days = 7
    elif attempts == 2:
        delay_days = 30
    else:
        delay_days = 90
    retry_epoch = now_epoch + (delay_days * 86400)
    meta["retry_after"] = datetime.fromtimestamp(retry_epoch, tz=timezone.utc).replace(microsecond=0).isoformat()


def _select_backfill(
    *,
    links_file: Path,
    csv_file: Path,
    limit_details: int,
    state_file: Path,
) -> list[str]:
    all_links = read_links_file(links_file)
    rows_by_url = read_existing_csv_rows(csv_file)
    state = load_state(state_file)
    ensure_state_sections(state)

    never_scraped = [url for url in all_links if url not in rows_by_url]

    retryable_blank_ean: list[str] = []
    for url, row in rows_by_url.items():
        if url not in all_links:
            continue
        if clean_text(row.get("ean")):
            continue
        meta = link_meta(state, url)
        meta["url"] = url
        if _backfill_retry_allowed(meta, row):
            retryable_blank_ean.append(url)

    selected: list[str] = []
    new_budget = min(limit_details, len(never_scraped))
    if new_budget:
        selected.extend(never_scraped[:new_budget])

    remaining = limit_details - len(selected)
    if remaining > 0 and retryable_blank_ean:
        selected.extend(_rotation_select(retryable_blank_ean, remaining, state, "backfill_retry_cursor"))

    now = utc_now_iso()
    for url in selected:
        meta = link_meta(state, url)
        meta["url"] = url
        meta["last_selected_for"] = "backfill"
        meta["last_selected_at"] = now
    save_state(state_file, state)
    print(
        f"[SELECT backfill] nooit_gescraped={len(never_scraped)} | retry_blank={len(retryable_blank_ean)} | geselecteerd={len(selected)} | limit={limit_details}"
    )
    return selected


def _select_mixed(
    *,
    links_file: Path,
    csv_file: Path,
    limit_details: int,
    state_file: Path,
) -> list[str]:
    refresh_budget = max(1, int(limit_details * 0.8))
    backfill_budget = max(0, limit_details - refresh_budget)
    selected = _select_refresh_known(
        links_file=links_file,
        csv_file=csv_file,
        limit_details=refresh_budget,
        state_file=state_file,
    )
    if backfill_budget > 0:
        selected.extend(
            _select_backfill(
                links_file=links_file,
                csv_file=csv_file,
                limit_details=backfill_budget,
                state_file=state_file,
            )
        )
    deduped: list[str] = []
    seen: set[str] = set()
    for url in selected:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)
    return deduped[:limit_details]


def select_links_for_detail_refresh(
    *,
    links_file: Path,
    csv_file: Path,
    limit_details: int | None = None,
    state_file: Path | None = None,
    strategy: str = "mixed",
) -> list[str]:
    all_links = read_links_file(links_file)
    if not all_links:
        return []

    limit = int(limit_details or 0)
    if limit <= 0:
        limit = len(all_links)

    state_path = state_file or (csv_file.parent / DEFAULT_STATE_FILE)
    if strategy == "refresh-known":
        return _select_refresh_known(
            links_file=links_file,
            csv_file=csv_file,
            limit_details=limit,
            state_file=state_path,
        )
    if strategy == "backfill":
        return _select_backfill(
            links_file=links_file,
            csv_file=csv_file,
            limit_details=limit,
            state_file=state_path,
        )
    return _select_mixed(
        links_file=links_file,
        csv_file=csv_file,
        limit_details=limit,
        state_file=state_path,
    )


# ---------------------------------------------------------------------------
# Detail scraping and state updates
# ---------------------------------------------------------------------------
def _apply_state_after_row(state: dict, url: str, row: dict[str, str]) -> None:
    meta = link_meta(state, url)
    now = utc_now_iso()
    meta["url"] = url
    meta["last_detail_scraped_at"] = now
    meta["last_price"] = clean_text(row.get("price"))
    meta["last_availability"] = clean_text(row.get("availability"))
    meta["last_detail_status"] = clean_text(row.get("detail_status"))
    meta["last_ean"] = clean_text(row.get("ean"))
    meta["is_secondhand"] = str_to_bool(row.get("is_secondhand"))
    if clean_text(row.get("ean")):
        meta["blank_ean_attempts"] = 0
        meta["retry_after"] = ""
        meta["known_ean"] = True
    else:
        meta["blank_ean_attempts"] = int(meta.get("blank_ean_attempts", 0) or 0) + 1
        meta["known_ean"] = False
        _set_next_retry(meta, row)


def scrape_product_details(
    session: requests.Session,
    links: Sequence[str],
    csv_path: Path,
    update_existing: bool,
    workers: int = 1,
    state_file: Path | None = None,
) -> int:
    if not links:
        print("[DETAILS] Geen links om te verwerken.")
        return 0

    state_path = state_file or (csv_path.parent / DEFAULT_STATE_FILE)
    state = load_state(state_path)
    ensure_state_sections(state)

    written = 0
    total = len(links)
    rows_by_url = read_existing_csv_rows(csv_path) if update_existing else read_existing_csv_rows(csv_path)
    worker_count = max(1, int(workers or 1))
    print(f"[DETAILS] workers={worker_count} | update_existing={update_existing}")

    def _fetch_one(url: str) -> tuple[str, dict[str, str]]:
        local_session = build_session() if worker_count > 1 else session
        try:
            html = fetch_html(local_session, url)
            row = extract_detail_fields(html, url)
            existing = rows_by_url.get(url, {})
            row["source_collection"] = clean_text(existing.get("source_collection"))
            if not row["source_collection"]:
                meta = link_meta(state, url)
                row["source_collection"] = clean_text(meta.get("discovery_source"))
            if worker_count <= 1:
                time.sleep(SLEEP_BETWEEN_REQUESTS)
            return url, row
        finally:
            if worker_count > 1:
                local_session.close()

    if worker_count == 1:
        iterable = enumerate(links, start=1)
        for idx, url in iterable:
            try:
                _, row = _fetch_one(url)
                rows_by_url[url] = {field: clean_text(row.get(field, "")) for field in FIELDNAMES}
                _apply_state_after_row(state, url, row)
                written += 1
                print(
                    f"[PRODUCT {idx}/{total}] verwerkt | ean={row['ean'] or '-'} | price={row['price'] or '-'} | avail={row['availability'] or '-'} | status={row['detail_status'] or '-'}"
                )
                if written % SAVE_EVERY_REFRESHED_PRODUCTS == 0:
                    write_all_rows_to_csv(csv_path, rows_by_url)
                    save_state(state_path, state)
                    print(f"[DETAILS] tussentijds opgeslagen na {written} producten")
            except Exception as exc:
                print(f"[PRODUCT {idx}/{total}] FOUT bij {url}: {exc}")
        write_all_rows_to_csv(csv_path, rows_by_url)
        save_state(state_path, state)
        return written

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_meta = {executor.submit(_fetch_one, url): (idx, url) for idx, url in enumerate(links, start=1)}
        for completed, future in enumerate(as_completed(future_to_meta), start=1):
            original_idx, url = future_to_meta[future]
            try:
                _, row = future.result()
                rows_by_url[url] = {field: clean_text(row.get(field, "")) for field in FIELDNAMES}
                _apply_state_after_row(state, url, row)
                written += 1
                print(
                    f"[PRODUCT {completed}/{total}] verwerkt | src_idx={original_idx} | ean={row['ean'] or '-'} | price={row['price'] or '-'} | avail={row['availability'] or '-'} | status={row['detail_status'] or '-'}"
                )
                if written % SAVE_EVERY_REFRESHED_PRODUCTS == 0:
                    write_all_rows_to_csv(csv_path, rows_by_url)
                    save_state(state_path, state)
                    print(f"[DETAILS] tussentijds opgeslagen na {written} producten")
            except Exception as exc:
                print(f"[PRODUCT {completed}/{total}] FOUT bij {url}: {exc}")

    write_all_rows_to_csv(csv_path, rows_by_url)
    save_state(state_path, state)
    return written


# ---------------------------------------------------------------------------
# CLI / legacy compatibility helpers
# ---------------------------------------------------------------------------
def run_new_default(*, links_file: Path, csv_file: Path, start_page: int = 1, max_pages: int | None = None) -> dict[str, int | str]:
    session = build_session()
    state_file = csv_file.parent / DEFAULT_STATE_FILE
    pages = max_pages if max_pages is not None else 15
    new_links = scrape_listing_pages(
        session=session,
        links_file=links_file,
        state_file=state_file,
        source_names=["browse-all-music", "all"],
        max_pages_per_source=pages,
    )
    written = scrape_product_details(
        session=session,
        links=new_links,
        csv_path=csv_file,
        update_existing=False,
        workers=1,
        state_file=state_file,
    )
    return {"new_links": len(new_links), "written": written, "mode": "new"}


def run_refresh_default(
    *,
    links_file: Path,
    csv_file: Path,
    limit_details: int | None = None,
    state_file: Path | None = None,
) -> dict[str, int | str]:
    session = build_session()
    chosen_state = state_file or (csv_file.parent / DEFAULT_STATE_FILE)
    links = select_links_for_detail_refresh(
        links_file=links_file,
        csv_file=csv_file,
        limit_details=limit_details,
        state_file=chosen_state,
        strategy="refresh-known",
    )
    written = scrape_product_details(
        session=session,
        links=links,
        csv_path=csv_file,
        update_existing=True,
        workers=DEFAULT_REFRESH_WORKERS,
        state_file=chosen_state,
    )
    return {"links": len(links), "written": written, "mode": "refresh"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="3345.nl scraper")
    parser.add_argument(
        "--mode",
        choices=[
            "new",
            "refresh",
            "both",
            "links",
            "details",
            "discovery",
            "refresh-known",
            "backfill",
        ],
        default="both",
    )
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--max-pages", type=int, default=15)
    parser.add_argument("--limit-details", type=int, default=250)
    parser.add_argument("--workers", type=int, default=DEFAULT_REFRESH_WORKERS)
    parser.add_argument("--links-file", default=DEFAULT_LINKS_FILE)
    parser.add_argument("--csv-file", default=DEFAULT_CSV_FILE)
    parser.add_argument("--state-file", default=None)
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=None,
        help="Discovery source(s), repeatable. Defaults to browse-all-music and all.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    mode = args.mode
    if mode == "links":
        mode = "discovery"
    elif mode == "details":
        mode = "refresh-known"
    elif mode == "new":
        mode = "discovery"
    elif mode == "refresh":
        mode = "refresh-known"

    links_file = Path(args.links_file)
    csv_file = Path(args.csv_file)
    state_file = Path(args.state_file) if args.state_file else (csv_file.parent / DEFAULT_STATE_FILE)

    print("=" * 72)
    print("3345.nl scraper gestart")
    print(f"mode       : {mode}")
    print(f"links_file : {links_file}")
    print(f"csv_file   : {csv_file}")
    print(f"state_file : {state_file}")
    print("=" * 72)

    session = build_session()

    if mode == "discovery":
        scrape_listing_pages(
            session=session,
            links_file=links_file,
            state_file=state_file,
            source_names=args.sources or ["browse-all-music", "all"],
            max_pages_per_source=max(1, args.max_pages),
        )
        print("Klaar.")
        return 0

    if mode == "refresh-known":
        links = select_links_for_detail_refresh(
            links_file=links_file,
            csv_file=csv_file,
            limit_details=max(1, args.limit_details),
            state_file=state_file,
            strategy="refresh-known",
        )
        written = scrape_product_details(
            session=session,
            links=links,
            csv_path=csv_file,
            update_existing=True,
            workers=max(1, args.workers),
            state_file=state_file,
        )
        print(f"[DETAILS] klaar. Verwerkte regels: {written}")
        print("Klaar.")
        return 0

    if mode == "backfill":
        links = select_links_for_detail_refresh(
            links_file=links_file,
            csv_file=csv_file,
            limit_details=max(1, args.limit_details),
            state_file=state_file,
            strategy="backfill",
        )
        written = scrape_product_details(
            session=session,
            links=links,
            csv_path=csv_file,
            update_existing=True,
            workers=max(1, args.workers),
            state_file=state_file,
        )
        print(f"[DETAILS] klaar. Verwerkte regels: {written}")
        print("Klaar.")
        return 0

    # Legacy both = run discovery first, then 80/20 mixed detail refresh.
    scrape_listing_pages(
        session=session,
        links_file=links_file,
        state_file=state_file,
        source_names=args.sources or ["browse-all-music", "all"],
        max_pages_per_source=max(1, args.max_pages),
    )
    links = select_links_for_detail_refresh(
        links_file=links_file,
        csv_file=csv_file,
        limit_details=max(1, args.limit_details),
        state_file=state_file,
        strategy="mixed",
    )
    written = scrape_product_details(
        session=session,
        links=links,
        csv_path=csv_file,
        update_existing=True,
        workers=max(1, args.workers),
        state_file=state_file,
    )
    print(f"[DETAILS] klaar. Verwerkte regels: {written}")
    print("Klaar.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
