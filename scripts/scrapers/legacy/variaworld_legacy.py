#!/usr/bin/env python3
"""
Stand-alone Variaworld scraper.

Menu:
1. Nieuwe items en prijzen
2. EAN verrijking
3. Beide
4. UIT

Fase 1
- Loopt listing-pagina's af
- Slaat basisproductdata op in CSV

Fase 2
- Leest product CSV
- Opent detailpagina's
- Verrijkt EAN in dezelfde CSV
"""

from __future__ import annotations

import csv
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Config
# =========================

BASE_URL = "https://www.variaworld.nl"
LISTING_URL_TEMPLATES = [
    "https://www.variaworld.nl/vinyl/lp-nieuw/~alles~m_ge%3D%5Bm%3Bj%5D%26m_so%3D2%26at%3D0036049700%26m_mt%3D128%26m_su%3D1%26startpagina%3D{page}",
    "https://www.variaworld.nl/vinyl/lp-nieuw/~alles~m_ge%3D%5Bm%3Bj%5D%26m_so%3D2%26m_su%3D1%26startpagina%3D{page}",
    "https://www.variaworld.nl/vinyl/lp-nieuw/~alles~m_ge%3D%5Bm%3Bj%5D%26m_so%3D2%26m_sr%3Dart%26m_gr%3Dnieuw%26aantalperpagina%3D100%26m_su%3D1%26startpagina%3D{page}",
    "https://www.variaworld.nl/vinyl/lp-nieuw/m_ge=%5Bm%3Bj%5D&m_so=2&m_su=1&startpagina={page}",
    "https://www.variaworld.nl/vinyl/lp-nieuw/m_ge=[m;j]&m_so=2&m_sr=art&m_gr=nieuw&aantalperpagina=100&m_su=1&startpagina={page}",
    "https://www.variaworld.nl/vinyl/lp-nieuw/m_ge=j&m_so=2&m_su=1&startpagina={page}",
    "https://www.variaworld.nl/vinyl/lp-nieuw/~vinyl~m_ge%3Dj%26m_so%3D2%26m_su%3D1%26at%3D9048776400%26startpagina%3D{page}",
]

REQUEST_TIMEOUT = 30
REQUEST_DELAY_SECONDS = 0.2
MAX_RETRIES = 3
BACKOFF_FACTOR = 0.7
STOP_AFTER_CONSECUTIVE_EMPTY_PAGES = 1
LISTING_SAVE_EVERY_PAGES = 5
EAN_SAVE_EVERY_RECORDS = 100
DETAIL_BACKFILL_DELAY_SECONDS = 0.1

OUTPUT_DIR = Path("output")
PRODUCTS_CSV = OUTPUT_DIR / "variaworld_products.csv"
ERRORS_CSV = OUTPUT_DIR / "variaworld_errors.csv"
EAN_ROTATION_STATE_FILE = OUTPUT_DIR / "variaworld_ean_rotation_state.json"

CSV_FIELDS = [
    "source",
    "product_id",
    "artist",
    "title",
    "carrier_raw",
    "carrier",
    "price_raw",
    "price",
    "currency",
    "product_url",
    "listing_page",
    "ean",
    "listing_status",
    "ean_status",
    "created_at",
    "updated_at",
    "last_seen_at",
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)

EAN_RE = re.compile(r"EAN\s*:?\s*([0-9]{8,14})", re.IGNORECASE)
EURO_RE = re.compile(r"€\s*([0-9][0-9\.,]*)")
NON_DIGIT_RE = re.compile(r"[^0-9,\.-]+")
WHITESPACE_RE = re.compile(r"\s+")
DETAIL_HREF_RE = re.compile(r"detail\.php", re.IGNORECASE)
GENERIC_LINE_RE = re.compile(
    r"^(bestel|bestellen|meer info|lees meer|wishlist|winkelwagen|toevoegen|voorzijde|achterzijde|"
    r"nieuw binnen|pre-order|stocksale|platenbeurs|aanmelden|menu)$",
    re.IGNORECASE,
)
GEOBLOCK_RE = re.compile(
    r"(site is not reachable from|not reachable from .*united states of america|please mail to info@variaworld\.nl|not reachable from \$country_visitor)",
    re.IGNORECASE,
)


@dataclass
class ScrapeError:
    phase: str
    url: str
    error: str
    created_at: str


# =========================
# Helpers
# =========================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_output_dir() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        read=MAX_RETRIES,
        connect=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session


def fetch_html(session: requests.Session, url: str, *, params: Optional[dict] = None) -> str:
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    response.encoding = response.encoding or "utf-8"
    html = response.text

    if GEOBLOCK_RE.search(html or ""):
        raise RuntimeError(
            "Geo-blocked by Variaworld for this runner IP/country; use a self-hosted NL/EU runner"
        )

    return html


def clean_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return WHITESPACE_RE.sub(" ", str(value).replace("\xa0", " ")).strip()


def normalize_carrier(carrier_raw: str) -> str:
    value = clean_text(carrier_raw).lower()
    if not value:
        return ""
    if "lp" in value or "vinyl" in value:
        return "LP"
    if "cd" in value:
        return "CD"
    if "box" in value:
        return "BOX"
    if "12 inch" in value or '12"' in value:
        return "12 INCH"
    if "7 inch" in value or '7"' in value:
        return "7 INCH"
    return clean_text(carrier_raw).upper()


def normalize_price_text(price_raw: str) -> str:
    price_raw = clean_text(price_raw)
    price_raw = price_raw.replace("€", "")
    return clean_text(price_raw)


def parse_price(price_raw: str) -> str:
    cleaned = NON_DIGIT_RE.sub("", normalize_price_text(price_raw))
    if not cleaned:
        return ""
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return f"{float(cleaned):.2f}"
    except ValueError:
        return ""


def extract_product_id(product_url: str) -> str:
    parsed = urlparse(product_url)
    query = parse_qs(parsed.query)
    values = query.get("at")
    return values[-1] if values else ""


def product_key(row: Dict[str, str]) -> str:
    return row.get("product_id") or row.get("product_url", "")


def load_rows(csv_path: Path) -> List[Dict[str, str]]:
    if not csv_path.exists():
        return []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            normalized = {field: row.get(field, "") for field in CSV_FIELDS}
            rows.append(normalized)
    return rows


def write_rows(csv_path: Path, rows: Iterable[Dict[str, str]]) -> None:
    ensure_output_dir()
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})


def append_errors(errors: List[ScrapeError]) -> None:
    if not errors:
        return
    ensure_output_dir()
    file_exists = ERRORS_CSV.exists()
    with ERRORS_CSV.open("a", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["phase", "url", "error", "created_at"])
        if not file_exists:
            writer.writeheader()
        for item in errors:
            writer.writerow(
                {
                    "phase": item.phase,
                    "url": item.url,
                    "error": item.error,
                    "created_at": item.created_at,
                }
            )


def upsert_row(row_map: Dict[str, Dict[str, str]], new_row: Dict[str, str]) -> None:
    key = product_key(new_row)
    if not key:
        return

    now = utc_now_iso()
    existing = row_map.get(key)
    if existing is None:
        new_row.setdefault("created_at", now)
        new_row["updated_at"] = now
        new_row["last_seen_at"] = now
        row_map[key] = {field: new_row.get(field, "") for field in CSV_FIELDS}
        return

    merged = existing.copy()
    for field in CSV_FIELDS:
        incoming = new_row.get(field, "")
        if incoming not in (None, ""):
            merged[field] = incoming
    merged["created_at"] = existing.get("created_at") or now
    merged["updated_at"] = now
    merged["last_seen_at"] = now
    row_map[key] = merged


def row_sort_key(row: Dict[str, str]) -> Tuple[str, str, str]:
    return (
        clean_text(row.get("artist", "")).lower(),
        clean_text(row.get("title", "")).lower(),
        clean_text(row.get("product_id", "")).lower(),
    )


def first_non_empty(values: Iterable[str]) -> str:
    for value in values:
        value = clean_text(value)
        if value:
            return value
    return ""


# =========================
# Rotation helpers
# =========================

def load_rotation_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_rotation_state(path: Path, state: dict) -> None:
    ensure_output_dir()
    path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


def select_round_robin_batch(rows: List[Dict[str, str]], limit: int, state: dict, key: str) -> List[Dict[str, str]]:
    if limit <= 0 or not rows:
        return rows

    total = len(rows)
    if total <= limit:
        state[key] = 0
        return rows

    offset = int(state.get(key, 0) or 0) % total
    selected = rows[offset : offset + limit]
    if len(selected) < limit:
        selected += rows[: limit - len(selected)]

    state[key] = (offset + limit) % total
    return selected


# =========================
# Parsing helpers
# =========================

def normalize_product_url(href: str) -> str:
    href = clean_text(unescape(href))
    href = href.replace("&amp;", "&")
    return urljoin(BASE_URL, href)


def extract_text_lines(node: Tag) -> List[str]:
    lines: List[str] = []
    seen = set()
    for text in node.stripped_strings:
        value = clean_text(text)
        if not value:
            continue
        if value in seen:
            continue
        seen.add(value)
        lines.append(value)
    return lines


def looks_like_carrier(value: str) -> bool:
    text = clean_text(value).lower()
    return any(token in text for token in ["lp", "vinyl", "cd", "box", "12 inch", "7 inch"])


def looks_like_price(value: str) -> bool:
    return bool(EURO_RE.search(clean_text(value)))


def extract_price_text_from_text(text: str) -> str:
    match = EURO_RE.search(text)
    return match.group(1) if match else ""


def is_noise_line(value: str) -> bool:
    text = clean_text(value)
    if not text:
        return True
    if GENERIC_LINE_RE.match(text):
        return True
    if looks_like_price(text):
        return True
    if text.lower().startswith("ean"):
        return True
    return False


def pick_artist_title(lines: List[str]) -> Tuple[str, str]:
    filtered = [line for line in lines if not is_noise_line(line)]
    if not filtered:
        return "", ""

    artist = ""
    title = ""

    if len(filtered) >= 2:
        artist = filtered[0]
        title = filtered[1]
    elif len(filtered) == 1:
        if " - " in filtered[0]:
            left, right = filtered[0].split(" - ", 1)
            artist, title = clean_text(left), clean_text(right)
        else:
            title = filtered[0]

    return artist, title


def best_container(anchor: Tag) -> Tag:
    best = anchor
    best_score = -1
    node: Optional[Tag] = anchor

    for _ in range(5):
        if node is None or not isinstance(node, Tag):
            break
        text = clean_text(node.get_text(" ", strip=True))
        lines = extract_text_lines(node)
        score = len(lines)
        if EURO_RE.search(text):
            score += 4
        if "detail.php" in text.lower():
            score -= 1
        if score > best_score:
            best = node
            best_score = score
        node = node.parent if isinstance(node.parent, Tag) else None

    return best


def row_from_anchor(anchor: Tag, page_number: int) -> Dict[str, str]:
    href = clean_text(anchor.get("href"))
    product_url = normalize_product_url(href)
    container = best_container(anchor)

    artist = first_non_empty(
        [
            anchor.select_one("div.koptekst").get_text(" ", strip=True) if anchor.select_one("div.koptekst") else "",
            container.select_one("div.koptekst").get_text(" ", strip=True) if container.select_one("div.koptekst") else "",
        ]
    )

    text_blocks = [
        clean_text(div.get_text(" ", strip=True))
        for div in container.select("div.tekst")
        if clean_text(div.get_text(" ", strip=True))
    ]
    title = text_blocks[0] if len(text_blocks) >= 1 else ""
    carrier_raw = text_blocks[1] if len(text_blocks) >= 2 else ""

    lines = extract_text_lines(container)
    if not artist or not title:
        line_artist, line_title = pick_artist_title(lines)
        artist = artist or line_artist
        title = title or line_title

    if not carrier_raw:
        carrier_raw = first_non_empty([line for line in lines if looks_like_carrier(line)])

    price_raw = ""
    price_node = container.select_one("div.overzicht_van_prijs span.div_kleur_prijs_1")
    if price_node is None:
        for selector in [
            "span.div_kleur_prijs_1",
            "[class*='prijs']",
            "[class*='price']",
        ]:
            candidate = container.select_one(selector)
            if candidate and EURO_RE.search(candidate.get_text(" ", strip=True)):
                price_node = candidate
                break
    if price_node is not None:
        price_raw = extract_price_text_from_text(price_node.get_text(" ", strip=True))
    if not price_raw:
        price_raw = extract_price_text_from_text(container.get_text(" ", strip=True))

    price = parse_price(price_raw)
    carrier = normalize_carrier(carrier_raw)

    return {
        "source": "variaworld",
        "product_id": extract_product_id(product_url),
        "artist": artist,
        "title": title,
        "carrier_raw": carrier_raw,
        "carrier": carrier,
        "price_raw": normalize_price_text(price_raw),
        "price": price,
        "currency": "EUR" if price_raw else "",
        "product_url": product_url,
        "listing_page": str(page_number),
        "ean": "",
        "listing_status": "ok",
        "ean_status": "",
        "created_at": "",
        "updated_at": "",
        "last_seen_at": "",
    }


def parse_listing_page(html: str, page_number: int) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    rows_by_url: Dict[str, Dict[str, str]] = {}

    for anchor in soup.find_all("a", href=True):
        href = clean_text(anchor.get("href"))
        if not href or not DETAIL_HREF_RE.search(href):
            continue

        row = row_from_anchor(anchor, page_number)
        product_url = row.get("product_url", "")
        if not product_url:
            continue

        existing = rows_by_url.get(product_url)
        if existing is None:
            rows_by_url[product_url] = row
            continue

        score_existing = sum(1 for key in ["artist", "title", "carrier_raw", "price"] if clean_text(existing.get(key, "")))
        score_new = sum(1 for key in ["artist", "title", "carrier_raw", "price"] if clean_text(row.get(key, "")))
        if score_new > score_existing:
            rows_by_url[product_url] = row

    return list(rows_by_url.values())


def detail_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return clean_text(soup.get_text(" ", strip=True))


def extract_json_ld_snapshot(soup: BeautifulSoup) -> Dict[str, str]:
    name = ""
    price_raw = ""

    for script in soup.select("script[type='application/ld+json']"):
        raw = clean_text(script.string or script.get_text(" ", strip=True))
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue

        candidates = payload if isinstance(payload, list) else [payload]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            name = name or clean_text(obj.get("name"))
            offers = obj.get("offers")
            if isinstance(offers, dict):
                price_raw = price_raw or clean_text(str(offers.get("price", "")))
            if name and price_raw:
                return {"name": name, "price_raw": price_raw}

    return {"name": name, "price_raw": price_raw}


def split_name_to_artist_title(name: str) -> Tuple[str, str]:
    value = clean_text(name)
    if " - " in value:
        left, right = value.split(" - ", 1)
        return clean_text(left), clean_text(right)
    return "", value


def extract_detail_snapshot(html: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    text = detail_text(html)
    json_ld = extract_json_ld_snapshot(soup)

    artist = ""
    title = ""
    carrier_raw = ""
    price_raw = ""

    artist_match = re.search(r"Artiest\s*:?\s*(.*?)\s+Titel\s*:?", text, re.IGNORECASE)
    if artist_match:
        artist = clean_text(artist_match.group(1))

    title_match = re.search(
        r"Titel\s*:?\s*(.*?)\s+(?:Produktspecificatie|Drager\s*:|formaat\s*:|EAN\s*:|Levertijd|Alle items bevinden)",
        text,
        re.IGNORECASE,
    )
    if title_match:
        title = clean_text(title_match.group(1))

    carrier_match = re.search(
        r"(?:Drager|formaat)\s*:?\s*(.*?)\s+(?:Genre|EAN\s*:|Levertijd|released|Nummers|Alle items bevinden)",
        text,
        re.IGNORECASE,
    )
    if carrier_match:
        carrier_raw = clean_text(carrier_match.group(1))

    if not price_raw:
        for selector in [
            "[class*='prijs']",
            "[class*='price']",
            "meta[itemprop='price']",
        ]:
            node = soup.select_one(selector)
            if node is None:
                continue
            value = clean_text(node.get("content") if node.has_attr("content") else node.get_text(" ", strip=True))
            if value:
                price_raw = value
                break

    if not price_raw:
        price_raw = extract_price_text_from_text(text)

    if (not artist or not title) and json_ld.get("name"):
        json_artist, json_title = split_name_to_artist_title(json_ld["name"])
        artist = artist or json_artist
        title = title or json_title

    if not price_raw and json_ld.get("price_raw"):
        price_raw = json_ld["price_raw"]

    return {
        "artist": artist,
        "title": title,
        "carrier_raw": carrier_raw,
        "carrier": normalize_carrier(carrier_raw),
        "price_raw": normalize_price_text(price_raw),
        "price": parse_price(price_raw),
        "ean": extract_ean_from_html(html),
    }


def needs_detail_backfill(row: Dict[str, str]) -> bool:
    return not clean_text(row.get("artist")) or not clean_text(row.get("title")) or not clean_text(row.get("price"))


def enrich_listing_row_from_detail(session: requests.Session, row: Dict[str, str]) -> Dict[str, str]:
    product_url = row.get("product_url", "")
    if not product_url:
        return row

    html = fetch_html(session, product_url)
    snapshot = extract_detail_snapshot(html)

    for field in ["artist", "title", "carrier_raw", "carrier", "price_raw", "price", "ean"]:
        incoming = clean_text(snapshot.get(field, ""))
        if incoming and not clean_text(row.get(field, "")):
            row[field] = incoming

    if clean_text(row.get("price_raw")) and not clean_text(row.get("currency")):
        row["currency"] = "EUR"

    return row


def fetch_listing_page_with_fallback(
    session: requests.Session,
    page_number: int,
    selected_template: Optional[str],
) -> Tuple[str, str, List[Dict[str, str]], List[ScrapeError]]:
    errors: List[ScrapeError] = []

    if selected_template:
        templates = [selected_template]
    else:
        templates = LISTING_URL_TEMPLATES

    best_template = ""
    best_url = ""
    best_items: List[Dict[str, str]] = []

    for template in templates:
        url = template.format(page=page_number)
        try:
            html = fetch_html(session, url)
            items = parse_listing_page(html, page_number)
        except Exception as exc:  # noqa: BLE001
            errors.append(ScrapeError("listing", url, f"{type(exc).__name__}: {exc}", utc_now_iso()))
            continue

        print(f"[LISTING CANDIDATE] page={page_number} | items={len(items)} | url={url}")

        if len(items) > len(best_items):
            best_template = template
            best_url = url
            best_items = items

        if items and selected_template is None:
            break

    return best_template, best_url, best_items, errors


# =========================
# Detail / EAN parsing
# =========================

def extract_ean_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for node in soup.select("div.detail_artikeltekst"):
        text = clean_text(node.get_text(" ", strip=True))
        match = EAN_RE.search(text)
        if match:
            return match.group(1)

    full_text = clean_text(soup.get_text(" ", strip=True))
    match = EAN_RE.search(full_text)
    return match.group(1) if match else ""


# =========================
# Phases
# =========================

def run_listing_phase() -> None:
    print("\n[FASE 1] Nieuwe items en prijzen")
    ensure_output_dir()

    session = build_session()
    existing_rows = load_rows(PRODUCTS_CSV)
    row_map: Dict[str, Dict[str, str]] = {product_key(row): row for row in existing_rows if product_key(row)}
    errors: List[ScrapeError] = []

    page = 1
    selected_template: Optional[str] = None
    consecutive_empty_pages = 0
    total_seen = 0
    total_upserted = 0
    pages_with_items = 0

    while True:
        template, page_url, items, page_errors = fetch_listing_page_with_fallback(session, page, selected_template)
        errors.extend(page_errors)

        if template:
            selected_template = template

        item_count = len(items)
        if item_count == 0:
            consecutive_empty_pages += 1
            print(f"[PAGINA {page}] geen items gevonden | url={page_url or 'geen bruikbare listing-url'}")

            if page == 1:
                append_errors(errors)
                if any("geo-blocked" in (err.error or "").lower() or "not reachable" in (err.error or "").lower() for err in errors):
                    raise RuntimeError("Variaworld blokkeert deze runner op IP/geolocatie; zet deze workflow op self-hosted NL/EU")
                raise RuntimeError("Variaworld listing gaf op pagina 1 geen producten terug")

            if consecutive_empty_pages >= STOP_AFTER_CONSECUTIVE_EMPTY_PAGES:
                print("[LISTING] stop: eind van resultaatset bereikt")
                break

            page += 1
            time.sleep(REQUEST_DELAY_SECONDS)
            continue

        pages_with_items += 1
        consecutive_empty_pages = 0

        backfilled = 0
        for item in items:
            if needs_detail_backfill(item):
                try:
                    enrich_listing_row_from_detail(session, item)
                    backfilled += 1
                    time.sleep(DETAIL_BACKFILL_DELAY_SECONDS)
                except Exception as exc:  # noqa: BLE001
                    message = f"{type(exc).__name__}: {exc}"
                    errors.append(ScrapeError("listing-detail-backfill", item.get("product_url", ""), message, utc_now_iso()))
                    item["listing_status"] = "partial"

        total_seen += item_count

        before_count = len(row_map)
        for item in items:
            upsert_row(row_map, item)
        after_count = len(row_map)

        page_new = max(after_count - before_count, 0)
        total_upserted += item_count

        should_save = (page % LISTING_SAVE_EVERY_PAGES == 0)
        if should_save:
            write_rows(PRODUCTS_CSV, sorted(row_map.values(), key=row_sort_key))
            save_status = f"opgeslagen={PRODUCTS_CSV}"
        else:
            save_status = "nog_niet_opgeslagen"

        print(
            f"[PAGINA {page}] items={item_count} | nieuw={page_new} | "
            f"backfilled={backfilled} | totaal_uniek={len(row_map)} | {save_status}"
        )

        page += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    if pages_with_items == 0 or len(row_map) == 0:
        append_errors(errors)
        raise RuntimeError("Variaworld listing leverde geen bruikbare records op")

    write_rows(PRODUCTS_CSV, sorted(row_map.values(), key=row_sort_key))
    append_errors(errors)
    print(
        f"[LISTING] klaar. Gezien={total_seen} | unieke records={len(row_map)} | "
        f"verwerkt={total_upserted} | eindsave={PRODUCTS_CSV} | template={selected_template}"
    )


def run_ean_phase(limit_ean: int | None = None) -> None:
    print("\n[FASE 2] EAN verrijking")
    ensure_output_dir()

    rows = load_rows(PRODUCTS_CSV)
    if not rows:
        print(f"[DETAIL] Geen inputbestand gevonden: {PRODUCTS_CSV}")
        return

    targets = [row for row in rows if not clean_text(row.get("ean", ""))]
    total_candidates = len(targets)

    if limit_ean is not None and limit_ean > 0:
        rotation_state = load_rotation_state(EAN_ROTATION_STATE_FILE)
        targets = select_round_robin_batch(targets, limit_ean, rotation_state, "ean_targets")
        save_rotation_state(EAN_ROTATION_STATE_FILE, rotation_state)
        print(
            f"[ROTATIE] ean_candidates={total_candidates} | geselecteerd={len(targets)} | "
            f"state={EAN_ROTATION_STATE_FILE}"
        )

    print(f"[DETAIL] totaal records={len(rows)} | zonder EAN={len(targets)} | limit_ean={limit_ean}")

    if not targets:
        print("[DETAIL] Alles is al verrijkt met een EAN.")
        return

    row_map: Dict[str, Dict[str, str]] = {product_key(row): row for row in rows if product_key(row)}
    session = build_session()
    errors: List[ScrapeError] = []

    processed = 0
    found = 0
    not_found = 0

    for index, row in enumerate(targets, start=1):
        product_url = row.get("product_url", "")
        key = product_key(row)
        if not key or not product_url:
            continue

        try:
            html = fetch_html(session, product_url)
            ean = extract_ean_from_html(html)

            if ean:
                row_map[key]["ean"] = ean
                row_map[key]["ean_status"] = "found"
                found += 1
                print(f"[DETAIL {index}/{len(targets)}] EAN gevonden | {ean} | {product_url}")
            else:
                row_map[key]["ean_status"] = "not_found"
                not_found += 1
                print(f"[DETAIL {index}/{len(targets)}] Geen EAN | {product_url}")

            row_map[key]["updated_at"] = utc_now_iso()

        except Exception as exc:  # noqa: BLE001
            message = f"{type(exc).__name__}: {exc}"
            row_map[key]["ean_status"] = "error"
            row_map[key]["updated_at"] = utc_now_iso()
            errors.append(ScrapeError("detail", product_url, message, utc_now_iso()))
            print(f"[DETAIL {index}/{len(targets)}] FOUT | {message} | {product_url}")

        processed += 1
        if processed % EAN_SAVE_EVERY_RECORDS == 0:
            write_rows(PRODUCTS_CSV, sorted(row_map.values(), key=row_sort_key))
            print(f"[DETAIL] tussensave na {processed} records | {PRODUCTS_CSV}")

        time.sleep(REQUEST_DELAY_SECONDS)

    write_rows(PRODUCTS_CSV, sorted(row_map.values(), key=row_sort_key))
    append_errors(errors)
    print(
        f"[DETAIL] klaar. Verwerkt={processed} | found={found} | not_found={not_found} | "
        f"errors={len(errors)} | eindsave={PRODUCTS_CSV}"
    )


# =========================
# Menu
# =========================

def print_menu() -> None:
    print("\n=== VARIAWORLD SCRAPER ===")
    print("1. Nieuwe items en prijzen")
    print("2. EAN verrijking")
    print("3. Beide")
    print("4. UIT")


def main() -> None:
    ensure_output_dir()

    while True:
        print_menu()
        choice = input("Maak een keuze [1-4]: ").strip()

        if choice == "1":
            run_listing_phase()
        elif choice == "2":
            raw_limit = input("Max aantal detailpagina's? Enter = alles: ").strip()
            limit_ean = int(raw_limit) if raw_limit.isdigit() and int(raw_limit) > 0 else None
            run_ean_phase(limit_ean=limit_ean)
        elif choice == "3":
            run_listing_phase()
            run_ean_phase()
        elif choice == "4":
            print("Afsluiten.")
            break
        else:
            print("Ongeldige keuze. Kies 1, 2, 3 of 4.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nAfgebroken door gebruiker.")
        sys.exit(130)
