#!/usr/bin/env python3
"""
3345.nl scraper

Functies:
1. Nieuwe producten zoeken
   - Scrape collectiepagina's
   - Voeg nieuwe productlinks toe aan een tekstbestand
   - Haal detaildata op voor alleen de nieuw gevonden producten

2. Prijzen/details verversen
   - Gebruik bestaande links uit het linksbestand
   - Haal detaildata opnieuw op
   - Werk bestaande CSV-regels bij op URL (geen duplicaten)

3. Beide
   - Eerst nieuwe producten zoeken
   - Daarna alle bestaande producten/prijzen verversen

Gebruik:
    python 3345_scraper.py
    python 3345_scraper.py --mode new
    python 3345_scraper.py --mode refresh
    python 3345_scraper.py --mode both

Benodigd:
    pip install requests beautifulsoup4
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Sequence
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from _rotation import load_rotation_state, save_rotation_state, select_priority_then_round_robin

BASE_URL = "https://3345.nl"
COLLECTION_URL_PAGE_1 = "https://3345.nl/collections/all"
COLLECTION_URL_PAGED = "https://3345.nl/collections/all?page={page}"

DEFAULT_LINKS_FILE = "3345_product_links.txt"
DEFAULT_CSV_FILE = "3345_products.csv"
DEFAULT_STATE_FILE = "3345_detail_rotation_state.json"
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_REQUESTS = 0.5
SAVE_EVERY_REFRESHED_PRODUCTS = 25
DEFAULT_REFRESH_WORKERS = 10
FIELDNAMES = [
    "artist",
    "title",
    "ean",
    "release_date",
    "genre",
    "style",
    "format",
    "price",
    "url",
]


# ---------------------------
# HTTP helpers
# ---------------------------
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


# ---------------------------
# Generic text helpers
# ---------------------------
def clean_text(value: str | None) -> str:
    if not value:
        return ""
    value = value.replace("\xa0", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def normalize_price(value: str) -> str:
    value = clean_text(value)
    if not value:
        return ""
    if not value.startswith("€"):
        value = f"€{value}"
    return value.replace("€ ", "€")


def first_match(pattern: str, text: str, flags: int = 0) -> str:
    match = re.search(pattern, text, flags)
    return clean_text(match.group(1)) if match else ""


def extract_between_labels(text: str, label_variants: Iterable[str], next_labels: Iterable[str]) -> str:
    """Zoek de waarde na een label tot aan het eerstvolgende volgende label."""
    for label in label_variants:
        label_re = re.escape(label)
        next_re = "|".join(re.escape(x) for x in next_labels)
        pattern = rf"{label_re}\s*(.*?)\s*(?=(?:{next_re})|$)"
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return clean_text(match.group(1))
    return ""


# ---------------------------
# Listing pages
# ---------------------------
def build_collection_url(page: int) -> str:
    if page <= 1:
        return COLLECTION_URL_PAGE_1
    return COLLECTION_URL_PAGED.format(page=page)


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
        for a in soup.select(selector):
            href = clean_text(a.get("href"))
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


def append_links(path: Path, links: Iterable[str]) -> list[str]:
    existing = set(read_links_file(path))
    new_links = [link for link in links if link not in existing]
    if not new_links:
        return []

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for link in new_links:
            f.write(link + "\n")
    return new_links


def scrape_listing_pages(
    session: requests.Session,
    links_file: Path,
    start_page: int = 1,
    max_pages: int | None = None,
) -> list[str]:
    total_new_links = 0
    collected_new_links: list[str] = []
    page = start_page
    seen_signatures: set[tuple[str, ...]] = set()

    while True:
        if max_pages is not None and page > start_page + max_pages - 1:
            break

        url = build_collection_url(page)
        try:
            html = fetch_html(session, url)
            links = extract_product_links(html)
        except Exception as exc:
            print(f"[PAGINA {page}] FOUT bij ophalen: {exc}")
            break

        if not links:
            print(f"[PAGINA {page}] gevonden=0 | nieuw opgeslagen=0 | totaal nieuw={total_new_links}")
            print(f"[STOP] Pagina {page} bevat geen productlinks meer.")
            break

        signature = tuple(links)
        if signature in seen_signatures:
            print(
                f"[PAGINA {page}] gevonden={len(links)} | nieuw opgeslagen=0 | totaal nieuw={total_new_links}"
            )
            print(f"[STOP] Pagina {page} herhaalt een eerdere productset. Waarschijnlijk einde pagination.")
            break
        seen_signatures.add(signature)

        new_links = append_links(links_file, links)
        added = len(new_links)
        total_new_links += added
        collected_new_links.extend(new_links)

        print(
            f"[PAGINA {page}] gevonden={len(links)} | nieuw opgeslagen={added} | totaal nieuw={total_new_links}"
        )

        # Belangrijk: niet stoppen op 0 nieuwe links.
        # We lopen alle pagina's af, want oudere pagina's kunnen al bekend zijn terwijl latere pagina's wel nieuw bevatten.
        page += 1
        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return collected_new_links


# ---------------------------
# Product details
# ---------------------------
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
            remainder = full_h1[len(artist):].strip()
            remainder = remainder.lstrip("-–—: ").strip()
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


def extract_price(soup: BeautifulSoup, page_text: str) -> str:
    selectors = [
        ".price-item--sale",
        ".price-item--regular",
        ".price__sale .price-item",
        ".price__regular .price-item",
        ".price .price-item",
    ]
    for selector in selectors:
        for node in soup.select(selector):
            txt = clean_text(node.get_text(" ", strip=True))
            if "€" in txt:
                m = re.search(r"€\s*([\d\.,]+)", txt)
                if m:
                    return normalize_price(m.group(0))

    patterns = [
        r"Sale price\s*€\s*([\d\.,]+)",
        r"Regular price\s*€\s*([\d\.,]+)",
        r"Aanbiedingsprijs\s*€\s*([\d\.,]+)",
        r"Normale prijs\s*€\s*([\d\.,]+)",
        r"Default Title\s*(?:-|–)?\s*(?:Sold out|Uitverkocht)?\s*(?:-|–)?\s*€\s*([\d\.,]+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, page_text, flags=re.IGNORECASE)
        if m:
            return normalize_price(m.group(1))

    m = re.search(r"€\s*([\d\.,]+)", page_text)
    if m:
        return normalize_price(m.group(1))
    return ""


def extract_detail_fields(html: str, url: str) -> dict:
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
        m = re.search(r"\(([^\)]+)\)\s*$", name)
        if m:
            format_value = clean_text(m.group(1))

    price = extract_price(soup, page_text)
    if not price:
        offers = product_json.get("offers")
        if isinstance(offers, dict) and offers.get("price"):
            price = normalize_price(str(offers["price"]).replace(".", ","))

    return {
        "artist": artist,
        "title": title,
        "ean": ean,
        "release_date": release_date,
        "genre": genre,
        "style": style,
        "format": format_value,
        "price": price,
        "url": url,
    }


def read_existing_csv_rows(csv_path: Path) -> dict[str, dict[str, str]]:
    rows_by_url: dict[str, dict[str, str]] = {}
    if not csv_path.exists():
        return rows_by_url

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = clean_text((row or {}).get("url"))
            if not url:
                continue
            normalized = {field: clean_text((row or {}).get(field, "")) for field in FIELDNAMES}
            rows_by_url[url] = normalized
    return rows_by_url


def select_links_for_detail_refresh(
    *,
    links_file: Path,
    csv_file: Path,
    limit_details: int | None = None,
    state_file: Path | None = None,
) -> list[str]:
    all_links = read_links_file(links_file)
    if not all_links:
        return []

    if limit_details is None or limit_details <= 0:
        return all_links

    rows_by_url = read_existing_csv_rows(csv_file)
    missing_ean_links = [url for url in all_links if not clean_text(rows_by_url.get(url, {}).get("ean", ""))]
    known_ean_links = [url for url in all_links if clean_text(rows_by_url.get(url, {}).get("ean", ""))]

    rotation_state_path = state_file or (csv_file.parent / DEFAULT_STATE_FILE)
    rotation_state = load_rotation_state(rotation_state_path)
    selected_links = select_priority_then_round_robin(
        missing_ean_links,
        known_ean_links,
        limit_details,
        rotation_state,
        "missing_ean_links",
        "known_ean_links",
    )
    save_rotation_state(rotation_state_path, rotation_state)

    print(
        f"[ROTATIE] totaal_links={len(all_links)} | zonder_ean={len(missing_ean_links)} | "
        f"met_ean={len(known_ean_links)} | geselecteerd={len(selected_links)} | state={rotation_state_path}"
    )
    return selected_links


def write_all_rows_to_csv(csv_path: Path, rows_by_url: dict[str, dict[str, str]]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for url in sorted(rows_by_url.keys()):
            writer.writerow({field: rows_by_url[url].get(field, "") for field in FIELDNAMES})


def append_row_to_csv(csv_path: Path, row: dict) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    file_exists = csv_path.exists()
    with csv_path.open("a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in FIELDNAMES})


def scrape_product_details(
    session: requests.Session,
    links: Sequence[str],
    csv_path: Path,
    update_existing: bool,
    workers: int = 1,
) -> int:
    if not links:
        print("[DETAILS] Geen links om te verwerken.")
        return 0

    written = 0
    total = len(links)

    if update_existing:
        rows_by_url = read_existing_csv_rows(csv_path)
        worker_count = max(1, int(workers or 1))
        print(f"[DETAILS] refresh workers={worker_count}")

        def _fetch_one(url: str) -> tuple[str, dict[str, str]]:
            local_session = build_session() if worker_count > 1 else session
            try:
                html = fetch_html(local_session, url)
                row = extract_detail_fields(html, url)
                if worker_count <= 1:
                    time.sleep(SLEEP_BETWEEN_REQUESTS)
                return url, row
            finally:
                if worker_count > 1:
                    local_session.close()

        if worker_count == 1:
            for idx, url in enumerate(links, start=1):
                try:
                    _, row = _fetch_one(url)
                    rows_by_url[url] = row
                    written += 1

                    print(
                        f"[PRODUCT {idx}/{total}] ververst | "
                        f"artist={row['artist'] or '-'} | "
                        f"title={row['title'] or '-'} | "
                        f"price={row['price'] or '-'}"
                    )

                    if written % SAVE_EVERY_REFRESHED_PRODUCTS == 0:
                        write_all_rows_to_csv(csv_path, rows_by_url)
                        print(f"[DETAILS] tussentijds opgeslagen na {written} producten")
                except Exception as exc:
                    print(f"[PRODUCT {idx}/{total}] FOUT bij {url}: {exc}")

            write_all_rows_to_csv(csv_path, rows_by_url)
            return written

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_to_meta = {executor.submit(_fetch_one, url): (idx, url) for idx, url in enumerate(links, start=1)}
            for completed, future in enumerate(as_completed(future_to_meta), start=1):
                original_idx, url = future_to_meta[future]
                try:
                    _, row = future.result()
                    rows_by_url[url] = row
                    written += 1
                    print(
                        f"[PRODUCT {completed}/{total}] ververst | "
                        f"artist={row['artist'] or '-'} | "
                        f"title={row['title'] or '-'} | "
                        f"price={row['price'] or '-'} | "
                        f"src_idx={original_idx}"
                    )
                    if written % SAVE_EVERY_REFRESHED_PRODUCTS == 0:
                        write_all_rows_to_csv(csv_path, rows_by_url)
                        print(f"[DETAILS] tussentijds opgeslagen na {written} producten")
                except Exception as exc:
                    print(f"[PRODUCT {completed}/{total}] FOUT bij {url}: {exc}")

        write_all_rows_to_csv(csv_path, rows_by_url)
        return written

    existing_urls = set(read_existing_csv_rows(csv_path).keys())
    for idx, url in enumerate(links, start=1):
        if url in existing_urls:
            print(f"[PRODUCT {idx}/{total}] SKIP reeds in CSV: {url}")
            continue

        try:
            html = fetch_html(session, url)
            row = extract_detail_fields(html, url)
            append_row_to_csv(csv_path, row)
            existing_urls.add(url)
            written += 1

            print(
                f"[PRODUCT {idx}/{total}] opgeslagen | "
                f"artist={row['artist'] or '-'} | "
                f"title={row['title'] or '-'} | "
                f"price={row['price'] or '-'}"
            )
        except Exception as exc:
            print(f"[PRODUCT {idx}/{total}] FOUT bij {url}: {exc}")

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return written


# ---------------------------
# Input helpers
# ---------------------------
def ask_menu_choice() -> str:
    print("\nWat wil je doen?")
    print("1. Nieuwe producten zoeken")
    print("2. Prijzen/details verversen")
    print("3. Beide")

    while True:
        choice = clean_text(input("Kies 1, 2 of 3: "))
        if choice == "1":
            return "new"
        if choice == "2":
            return "refresh"
        if choice == "3":
            return "both"
        print("Ongeldige keuze. Kies 1, 2 of 3.")


def ask_int(prompt: str, default: int | None = None) -> int | None:
    raw = clean_text(input(prompt))
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        print("Ongeldige invoer, standaardwaarde wordt gebruikt.")
        return default


# ---------------------------
# Default dashboard flows
# ---------------------------
def run_new_default(*, links_file: Path, csv_file: Path, start_page: int = 1, max_pages: int | None = None) -> dict[str, int | str]:
    session = build_session()
    print("=" * 72)
    print("3345.nl scraper gestart")
    print("mode       : new")
    print(f"start_page : {start_page}")
    print(f"max_pages  : {max_pages if max_pages is not None else 'tot einde'}")
    print(f"links_file : {links_file}")
    print(f"csv_file   : {csv_file}")
    print("=" * 72)

    new_links = scrape_listing_pages(
        session=session,
        links_file=links_file,
        start_page=start_page,
        max_pages=max_pages,
    )
    print(f"[LINKS] klaar. Nieuw opgeslagen links: {len(new_links)}")
    written = scrape_product_details(
        session=session,
        links=new_links,
        csv_path=csv_file,
        update_existing=False,
    )
    print(f"[DETAILS] klaar. Nieuw geschreven productregels: {written}")
    print("Klaar.")
    return {"new_links": len(new_links), "written": written, "mode": "new"}


def run_refresh_default(*, links_file: Path, csv_file: Path, limit_details: int | None = None, state_file: Path | None = None) -> dict[str, int | str]:
    session = build_session()
    print("=" * 72)
    print("3345.nl scraper gestart")
    print("mode       : refresh")
    print("start_page : 1")
    print("max_pages  : n.v.t.")
    print(f"links_file : {links_file}")
    print(f"csv_file   : {csv_file}")
    print("=" * 72)

    all_links = read_links_file(links_file)
    written = scrape_product_details(
        session=session,
        links=all_links,
        csv_path=csv_file,
        update_existing=True,
        workers=DEFAULT_REFRESH_WORKERS,
    )
    print(f"[DETAILS] klaar. Ververste productregels: {written}")
    print("Klaar.")
    return {"links": len(all_links), "written": written, "mode": "refresh"}


# ---------------------------
# CLI
# ---------------------------
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="3345.nl vinyl scraper")
    parser.add_argument(
        "--mode",
        choices=["new", "refresh", "both", "links", "details"],
        default=None,
        help="new = nieuwe producten, refresh = prijzen/details verversen, both = beide",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=None,
        help="Startpagina voor collectie-scrape. standaard: 1",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Maximaal aantal collectiepagina's. standaard: doorgaan tot einde",
    )
    parser.add_argument(
        "--links-file",
        default=DEFAULT_LINKS_FILE,
        help=f"Bestand om productlinks in op te slaan. standaard: {DEFAULT_LINKS_FILE}",
    )
    parser.add_argument(
        "--csv-file",
        default=DEFAULT_CSV_FILE,
        help=f"CSV-bestand voor productdetails. standaard: {DEFAULT_CSV_FILE}",
    )
    parser.add_argument(
        "--limit-details",
        type=int,
        default=None,
        help="Maximaal aantal detailpagina's in refresh/both. Leeg = alles.",
    )
    parser.add_argument(
        "--state-file",
        default=None,
        help="Optioneel rotatie-statebestand voor detailrefresh.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    mode = args.mode
    if mode == "links":
        mode = "new"
    elif mode == "details":
        mode = "refresh"

    start_page = args.start_page if args.start_page is not None else 1
    max_pages = args.max_pages

    if mode is None:
        mode = ask_menu_choice()
        if mode in {"new", "both"}:
            start_page = ask_int("Vanaf welke pagina starten? [1]: ", default=1) or 1
            max_pages = ask_int("Max aantal pagina's? [leeg = alle]: ", default=None)

    links_file = Path(args.links_file)
    csv_file = Path(args.csv_file)
    state_file = Path(args.state_file) if args.state_file else (csv_file.parent / DEFAULT_STATE_FILE)
    session = build_session()

    print("=" * 72)
    print("3345.nl scraper gestart")
    print(f"mode       : {mode}")
    print(f"start_page : {start_page}")
    print(f"max_pages  : {max_pages if max_pages is not None else 'tot einde'}")
    print(f"links_file : {links_file}")
    print(f"csv_file   : {csv_file}")
    print("=" * 72)

    new_links: list[str] = []

    if mode in {"new", "both"}:
        new_links = scrape_listing_pages(
            session=session,
            links_file=links_file,
            start_page=start_page,
            max_pages=max_pages,
        )
        print(f"[LINKS] klaar. Nieuw opgeslagen links: {len(new_links)}")

    if mode == "new":
        written = scrape_product_details(
            session=session,
            links=new_links,
            csv_path=csv_file,
            update_existing=False,
        )
        print(f"[DETAILS] klaar. Nieuw geschreven productregels: {written}")

    elif mode == "refresh":
        all_links = select_links_for_detail_refresh(
            links_file=links_file,
            csv_file=csv_file,
            limit_details=args.limit_details,
            state_file=state_file,
        )
        written = scrape_product_details(
            session=session,
            links=all_links,
            csv_path=csv_file,
            update_existing=True,
            workers=DEFAULT_REFRESH_WORKERS,
        )
        print(f"[DETAILS] klaar. Ververste productregels: {written}")

    elif mode == "both":
        all_links = select_links_for_detail_refresh(
            links_file=links_file,
            csv_file=csv_file,
            limit_details=args.limit_details,
            state_file=state_file,
        )
        written = scrape_product_details(
            session=session,
            links=all_links,
            csv_path=csv_file,
            update_existing=True,
            workers=DEFAULT_REFRESH_WORKERS,
        )
        print(f"[DETAILS] klaar. Ververste productregels: {written}")

    print("Klaar.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
