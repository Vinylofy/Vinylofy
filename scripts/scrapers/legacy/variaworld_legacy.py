#!/usr/bin/env python3
"""Stand-alone Variaworld scraper.

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

Opzet is modulair gehouden zodat dezelfde functies later eenvoudig in een
bestaand dashboard of scraper-hub kunnen worden opgenomen.
"""

from __future__ import annotations

import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# =========================
# Config
# =========================
BASE_URL = "https://www.variaworld.nl"
LISTING_URL_TEMPLATE = (
    "https://www.variaworld.nl/vinyl/lp-nieuw/"
    "m_ge=[j;m]&m_so=2&m_sr=art&m_gr=nieuw&aantalperpagina=100&m_su=1&startpagina={page}"
)

REQUEST_TIMEOUT = 30
REQUEST_DELAY_SECONDS = 0.2
MAX_RETRIES = 3
BACKOFF_FACTOR = 0.7
STOP_AFTER_CONSECUTIVE_EMPTY_PAGES = 1
LISTING_SAVE_EVERY_PAGES = 5
EAN_SAVE_EVERY_RECORDS = 25

OUTPUT_DIR = Path("output")
PRODUCTS_CSV = OUTPUT_DIR / "variaworld_products.csv"
ERRORS_CSV = OUTPUT_DIR / "variaworld_errors.csv"

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

EAN_RE = re.compile(r"EAN\s*:\s*([0-9]{8,14})", re.IGNORECASE)
NON_DIGIT_RE = re.compile(r"[^0-9,\.-]+")
WHITESPACE_RE = re.compile(r"\s+")


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
    return response.text


def clean_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return WHITESPACE_RE.sub(" ", value.replace("\xa0", " ")).strip()


def normalize_carrier(carrier_raw: str) -> str:
    value = clean_text(carrier_raw).lower()
    if not value:
        return ""
    if "lp" in value:
        return "LP"
    if "cd" in value:
        return "CD"
    if "box" in value:
        return "BOX"
    if "12 inch" in value or "12\"" in value:
        return "12 INCH"
    return clean_text(carrier_raw).upper()


def normalize_price_text(price_raw: str) -> str:
    price_raw = clean_text(price_raw)
    price_raw = price_raw.replace("€", "")
    return clean_text(price_raw)


def parse_price(price_raw: str) -> str:
    cleaned = NON_DIGIT_RE.sub("", normalize_price_text(price_raw))
    cleaned = cleaned.replace(".", "").replace(",", ".")
    if not cleaned:
        return ""
    try:
        return f"{float(cleaned):.2f}"
    except ValueError:
        return ""


def extract_product_id(product_url: str) -> str:
    query = parse_qs(urlparse(product_url).query)
    values = query.get("at")
    return values[0] if values else ""


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


# =========================
# Listing parsing
# =========================
def parse_listing_page(html: str, page_number: int) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict[str, str]] = []

    anchors = soup.select("a.overzichtbox_2[href]")
    if not anchors:
        anchors = [
            a
            for a in soup.select("a[href]")
            if "detail.php" in (a.get("href") or "") and a.select_one("div.koptekst")
        ]

    for anchor in anchors:
        href = clean_text(anchor.get("href"))
        if not href:
            continue

        product_url = urljoin(BASE_URL, href)
        artist = clean_text(anchor.select_one("div.koptekst").get_text(" ", strip=True) if anchor.select_one("div.koptekst") else "")

        text_blocks = [
            clean_text(div.get_text(" ", strip=True))
            for div in anchor.select("div.tekst")
            if clean_text(div.get_text(" ", strip=True))
        ]
        title = text_blocks[0] if len(text_blocks) >= 1 else ""
        carrier_raw = text_blocks[1] if len(text_blocks) >= 2 else ""
        carrier = normalize_carrier(carrier_raw)

        price_node = anchor.select_one("div.overzicht_van_prijs span.div_kleur_prijs_1")
        if price_node is None:
            for span in anchor.select("span.div_kleur_prijs_1"):
                if "€" in span.get_text(" ", strip=True):
                    price_node = span
                    break
        price_raw = clean_text(price_node.get_text(" ", strip=True) if price_node else "")
        price = parse_price(price_raw)

        row = {
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
        items.append(row)

    return items


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
    rows = load_rows(PRODUCTS_CSV)
    row_map: Dict[str, Dict[str, str]] = {product_key(row): row for row in rows if product_key(row)}
    errors: List[ScrapeError] = []

    page = 1
    consecutive_empty_pages = 0
    total_seen = 0
    total_upserted = 0

    while True:
        page_url = LISTING_URL_TEMPLATE.format(page=page)

        try:
            html = fetch_html(session, page_url)
            items = parse_listing_page(html, page)
        except Exception as exc:  # noqa: BLE001
            message = f"{type(exc).__name__}: {exc}"
            print(f"[PAGINA {page}] FOUT bij ophalen/parsen: {message} | url={page_url}")
            errors.append(ScrapeError("listing", page_url, message, utc_now_iso()))
            break

        item_count = len(items)
        if item_count == 0:
            consecutive_empty_pages += 1
            print(f"[PAGINA {page}] geen items gevonden | url={page_url}")
            if consecutive_empty_pages >= STOP_AFTER_CONSECUTIVE_EMPTY_PAGES:
                print("[LISTING] stop: eind van resultaatset bereikt")
                break
            page += 1
            time.sleep(REQUEST_DELAY_SECONDS)
            continue

        consecutive_empty_pages = 0
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
            f"[PAGINA {page}] items={item_count} | nieuw={page_new} | totaal_uniek={len(row_map)} | {save_status}"
        )

        page += 1
        time.sleep(REQUEST_DELAY_SECONDS)

    write_rows(PRODUCTS_CSV, sorted(row_map.values(), key=row_sort_key))
    append_errors(errors)
    print(
        f"[LISTING] klaar. Gezien={total_seen} | unieke records={len(row_map)} | verwerkt={total_upserted} | eindsave={PRODUCTS_CSV}"
    )


def run_ean_phase(limit_ean: int | None = None) -> None:
    print("\n[FASE 2] EAN verrijking")
    ensure_output_dir()

    rows = load_rows(PRODUCTS_CSV)
    if not rows:
        print(f"[DETAIL] Geen inputbestand gevonden: {PRODUCTS_CSV}")
        return

    targets = [row for row in rows if not clean_text(row.get("ean", ""))]
    if limit_ean is not None and limit_ean > 0:
        targets = targets[:limit_ean]
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
        f"[DETAIL] klaar. Verwerkt={processed} | found={found} | not_found={not_found} | errors={len(errors)} | eindsave={PRODUCTS_CSV}"
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
