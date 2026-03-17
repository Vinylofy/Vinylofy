from __future__ import annotations

import csv
import os
import re
import sys
import time
import uuid
import threading
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DOMAIN = "https://bobsvinyl.nl"
BASE_COLLECTION_URL = "https://bobsvinyl.nl/collections/nieuwe-lps"
COLLECTION_NAME = "nieuwe-lps"
DEFAULT_DELAY_SECONDS = 0.20
DEFAULT_WORKERS = 5
TIMEOUT = 30
DETAIL_FETCH_RETRIES = 3
DETAIL_RETRY_SLEEP_SECONDS = 1.2
STEP1_FILE = "bobsvinyl_step1.csv"
STEP2_FILE = "bobsvinyl_step2_enriched.csv"
DETAIL_ISSUES_FILE = "bobsvinyl_missing_eans.csv"

RESOLVED_DETAIL_STATUSES = {"ok", "2e_hands_bevestigd"}

STEP1_COLUMNS = [
    "url",
    "url_listing",
    "product_handle",
    "artist",
    "title",
    "drager",
    "prijs",
    "bron_collectie",
    "bron_listing_urls",
]

STEP2_COLUMNS = STEP1_COLUMNS + [
    "ean",
    "mogelijk_2e_hands",
    "detail_status",
    "detail_opmerking",
    "detail_checked_at",
]

DETAIL_ISSUE_COLUMNS = [
    "url",
    "artist",
    "title",
    "drager",
    "prijs",
    "ean",
    "mogelijk_2e_hands",
    "detail_status",
    "detail_opmerking",
    "detail_checked_at",
]

_thread_local = threading.local()


# ---------- Utilities ----------


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/145.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session



def get_thread_session(force_new: bool = False) -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if force_new and session is not None:
        try:
            session.close()
        except Exception:
            pass
        session = None

    if session is None:
        session = make_session()
        _thread_local.session = session
    return session



def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()



def nl_price(value: str) -> str:
    value = normalize_text(value)
    value = value.replace("€", "").replace("EUR", "").strip()
    value = value.replace(" ", "")
    return value.replace(".", ",")



def unique_pipe_join(existing: str, new_value: str) -> str:
    items = [x for x in (existing or "").split(" | ") if x]
    if new_value and new_value not in items:
        items.append(new_value)
    return " | ".join(items)



def canonical_product_url(url: str) -> str:
    absolute = urljoin(BASE_DOMAIN, url)
    parts = urlsplit(absolute)
    path = parts.path.rstrip("/")
    if not path:
        path = "/"
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))



def collection_page_url(page: int) -> str:
    if page <= 1:
        return BASE_COLLECTION_URL
    return f"{BASE_COLLECTION_URL}?page={page}"



def product_handle_from_url(url: str) -> str:
    path = urlsplit(url).path.rstrip("/")
    if not path:
        return ""
    return path.split("/")[-1]



def now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")



def load_csv_as_dict(path: str, columns: List[str]) -> Dict[str, Dict[str, str]]:
    rows: Dict[str, Dict[str, str]] = OrderedDict()
    if not os.path.exists(path):
        return rows

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            raw_url = normalize_text(raw.get("url", ""))
            if not raw_url:
                continue
            url = canonical_product_url(raw_url)
            row = {col: normalize_text(raw.get(col, "")) for col in columns}
            row["url"] = url
            if not row.get("product_handle"):
                row["product_handle"] = product_handle_from_url(url)
            if not row.get("bron_collectie"):
                row["bron_collectie"] = COLLECTION_NAME
            rows[url] = row
    return rows



def write_csv(path: str, rows_by_url: Dict[str, Dict[str, str]], columns: List[str]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination.with_name(f"{destination.name}.{uuid.uuid4().hex}.tmp")

    with open(temp_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for _, row in sorted(
            rows_by_url.items(),
            key=lambda item: (
                normalize_text(item[1].get("artist", "")).lower(),
                normalize_text(item[1].get("title", "")).lower(),
                item[0],
            ),
        ):
            safe_row = {col: normalize_text(row.get(col, "")) for col in columns}
            writer.writerow(safe_row)

    last_error: Exception | None = None
    for attempt in range(12):
        try:
            os.replace(temp_path, destination)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.5 + (attempt * 0.15))
        except Exception:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass
            raise

    backup_path = destination.with_name(f"{destination.stem}_write_failed_{int(time.time())}{destination.suffix}")
    try:
        os.replace(temp_path, backup_path)
    except Exception:
        pass

    raise PermissionError(
        f"Kon {destination.name} niet overschrijven. Sluit Excel/preview/OneDrive lock op dit bestand en probeer opnieuw. "
        f"Eventuele noodkopie: {backup_path.name}"
    ) from last_error



def write_detail_issues(rows_by_url: Dict[str, Dict[str, str]]) -> None:
    issue_rows: Dict[str, Dict[str, str]] = OrderedDict()
    for url, row in rows_by_url.items():
        ean = normalize_text(row.get("ean", ""))
        status = normalize_text(row.get("detail_status", ""))
        if ean:
            continue
        if status in RESOLVED_DETAIL_STATUSES:
            continue
        if not status:
            continue

        issue_rows[url] = {col: normalize_text(row.get(col, "")) for col in DETAIL_ISSUE_COLUMNS}
        issue_rows[url]["url"] = url

    write_csv(DETAIL_ISSUES_FILE, issue_rows, DETAIL_ISSUE_COLUMNS)



def fetch_soup(session: requests.Session, url: str) -> BeautifulSoup:
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")



def extract_price(card) -> str:
    sale = card.select_one(".price-item--sale.price-item--last")
    regular = card.select_one(".price-item--regular")
    node = sale or regular
    return nl_price(node.get_text(" ", strip=True) if node else "")



def split_artist_title_drager(raw_title: str) -> Tuple[str, str, str]:
    text = normalize_text(raw_title)
    if not text:
        return "", "", ""

    drager = ""
    match = re.search(r"\(([^()]*)\)\s*$", text)
    if match:
        candidate = normalize_text(match.group(1))
        if candidate and len(candidate) <= 30:
            drager = candidate
            text = normalize_text(text[:match.start()])

    artist = ""
    title = text
    if " - " in text:
        artist, title = text.split(" - ", 1)
    elif " – " in text:
        artist, title = text.split(" – ", 1)

    return normalize_text(artist), normalize_text(title), normalize_text(drager)



def parse_listing_card(card, listing_url: str) -> Dict[str, str] | None:
    link = card.select_one('a.full-unstyled-link[href*="/products/"]') or card.select_one('a[href*="/products/"]')
    if link is None:
        return None

    href = normalize_text(link.get("href", ""))
    if not href or "/products/" not in href:
        return None

    raw_title = normalize_text(link.get_text(" ", strip=True))
    artist, title, drager = split_artist_title_drager(raw_title)
    if not title and raw_title:
        title = raw_title

    listing_absolute = urljoin(BASE_DOMAIN, href)
    canonical = canonical_product_url(listing_absolute)
    prijs = extract_price(card)

    return {
        "url": canonical,
        "url_listing": listing_absolute,
        "product_handle": product_handle_from_url(canonical),
        "artist": artist,
        "title": title,
        "drager": drager,
        "prijs": prijs,
        "bron_collectie": COLLECTION_NAME,
        "bron_listing_urls": listing_url,
    }



def merge_row(existing: Dict[str, str] | None, incoming: Dict[str, str], columns: List[str]) -> Dict[str, str]:
    if existing is None:
        merged = {col: "" for col in columns}
        merged.update(incoming)
        return merged

    merged = dict(existing)
    for key, value in incoming.items():
        if key == "bron_listing_urls":
            merged[key] = unique_pipe_join(merged.get(key, ""), value)
        elif value:
            merged[key] = value

    for col in columns:
        merged.setdefault(col, "")
    return merged



def print_threadsafe(lock: threading.Lock, message: str) -> None:
    with lock:
        print(message, flush=True)


# ---------- Listing scraping ----------


def scrape_all_listings(
    output_path: str,
    columns: List[str],
    workers: int = DEFAULT_WORKERS,
    load_from_path: str | None = None,
    delay_seconds: float = DEFAULT_DELAY_SECONDS,
    stop_on_no_new_urls: bool = False,
) -> Dict[str, Dict[str, str]]:
    del workers  # step 1 is bewust lineair over collectiepagina's; workers blijven relevant voor detailverrijking.

    load_path = load_from_path or output_path
    rows_by_url = load_csv_as_dict(load_path, columns)

    if rows_by_url:
        print(f"[INFO] Bestaand bestand geladen: {load_path} ({len(rows_by_url)} records)")
    else:
        print(f"[INFO] Start met leeg bestand: {output_path}")

    session = make_session()
    total_pages = 0
    total_new = 0
    page = 1
    seen_urls = set(rows_by_url.keys())

    while True:
        current_url = collection_page_url(page)
        try:
            soup = fetch_soup(session, current_url)
        except Exception as exc:
            print(f"[FOUT] collectie | pagina {page} kon niet worden geladen: {exc}")
            break

        cards = soup.select("div.card-wrapper.product-card-wrapper")
        parsed_rows: List[Dict[str, str]] = []
        page_urls: List[str] = []

        for card in cards:
            row = parse_listing_card(card, current_url)
            if row is None:
                continue
            parsed_rows.append(row)
            page_urls.append(row["url"])

        if not parsed_rows:
            print(f"[STOP] collectie {COLLECTION_NAME} | pagina {page} bevat geen producten meer.")
            break

        unique_page_urls = set(page_urls)
        new_on_page = [url for url in unique_page_urls if url not in seen_urls]

        new_for_file = 0
        updated_for_file = 0
        for row in parsed_rows:
            row_url = row["url"]
            existing = rows_by_url.get(row_url)
            if existing is None:
                new_for_file += 1
            else:
                updated_for_file += 1
            rows_by_url[row_url] = merge_row(existing, row, columns)

        try:
            write_csv(output_path, rows_by_url, columns)
        except PermissionError as exc:
            print(f"[FOUT] Wegschrijven mislukt na pagina {page}: {exc}")
            raise

        seen_urls.update(unique_page_urls)
        total_pages += 1
        total_new += new_for_file

        print(
            (
                f"[OK] collectie {COLLECTION_NAME} | pagina {page} | producten: {len(parsed_rows)} | "
                f"unieke op pagina: {len(unique_page_urls)} | nieuw in bestand: {new_for_file} | "
                f"geüpdatet: {updated_for_file} | totaal bestand: {len(rows_by_url)}"
            ),
            flush=True,
        )

        if stop_on_no_new_urls and not new_on_page and page > 1:
            print(f"[STOP] collectie {COLLECTION_NAME} | pagina {page} bracht geen nieuwe URL's meer.")
            break

        page += 1
        time.sleep(delay_seconds)

    print(f"[KLAAR] {output_path} opgeslagen met {len(rows_by_url)} records uit {total_pages} pagina's.")
    return rows_by_url


# ---------- Detail enrichment ----------


def collect_product_text(soup: BeautifulSoup) -> str:
    product_title = normalize_text(
        soup.select_one(".product__title h1").get_text(" ", strip=True) if soup.select_one(".product__title h1") else ""
    )
    info_container = normalize_text(
        soup.select_one(".product__info-container").get_text(" ", strip=True)
        if soup.select_one(".product__info-container")
        else ""
    )
    description_container = normalize_text(
        soup.select_one(".product__description").get_text(" ", strip=True)
        if soup.select_one(".product__description")
        else ""
    )
    return " ".join(x for x in [product_title, info_container, description_container] if x)



def validate_product_page(soup: BeautifulSoup) -> Tuple[bool, str]:
    title = normalize_text(
        soup.select_one(".product__title h1").get_text(" ", strip=True) if soup.select_one(".product__title h1") else ""
    )
    has_info_container = soup.select_one(".product__info-container") is not None
    has_description = soup.select_one(".product__description") is not None
    has_price = soup.select_one(".price.price--large, .price__container") is not None
    has_add_to_cart = soup.select_one("form[action*='/cart/add']") is not None

    if not title:
        return False, "geen producttitel gevonden"
    if not has_info_container:
        return False, "geen product-info-container gevonden"
    if not (has_description or has_price or has_add_to_cart):
        return False, "productpagina lijkt incompleet"
    return True, "ok"



def extract_ean_from_soup(soup: BeautifulSoup, product_text: str) -> str:
    candidate_texts: List[str] = []
    if product_text:
        candidate_texts.append(product_text)

    description = soup.select_one(".product__description")
    if description is not None:
        candidate_texts.append(str(description))
        candidate_texts.append(description.get_text(" ", strip=True))

    info_container = soup.select_one(".product__info-container")
    if info_container is not None:
        candidate_texts.append(str(info_container))
        candidate_texts.append(info_container.get_text(" ", strip=True))

    for script in soup.select("script"):
        script_text = script.string or script.get_text(" ", strip=True)
        if not script_text:
            continue
        if any(token in script_text.lower() for token in ["gtin", "barcode", "ean"]):
            candidate_texts.append(script_text)

    patterns = [
        r"\bEAN\b\s*[:#-]?\s*([0-9]{8,14})",
        r"\bbarcode\b\s*[:#-]?\s*([0-9]{8,14})",
        r"\bgtin(?:8|12|13|14)?\b\s*[:=]\s*['\"]?([0-9]{8,14})",
        r"['\"]gtin(?:8|12|13|14)?['\"]\s*:\s*['\"]([0-9]{8,14})",
        r"['\"]barcode['\"]\s*:\s*['\"]([0-9]{8,14})",
    ]

    for text in candidate_texts:
        normalized = normalize_text(text)
        if not normalized:
            continue
        for pattern in patterns:
            match = re.search(pattern, normalized, flags=re.IGNORECASE)
            if match:
                return match.group(1)
    return ""



def detect_second_hand(soup: BeautifulSoup, product_text: str) -> str:
    lowered = product_text.lower()
    second_hand_patterns = [
        r"\b2e\s*hands?\b",
        r"\btweedehands\b",
        r"\bsecond\s*hand\b",
        r"\bused\b",
        r"\bpre[-\s]?owned\b",
    ]
    if any(re.search(pattern, lowered, flags=re.IGNORECASE) for pattern in second_hand_patterns):
        return "JA"

    # Tweede controle: product-specifieke HTML-blokken, niet de hele pagina.
    html_blocks = []
    for selector in [".product__title", ".product__info-container", ".product__description"]:
        node = soup.select_one(selector)
        if node is not None:
            html_blocks.append(normalize_text(node.get_text(" ", strip=True)).lower())
    html_text = " ".join(html_blocks)
    if any(re.search(pattern, html_text, flags=re.IGNORECASE) for pattern in second_hand_patterns):
        return "JA"

    return "NEE"



def parse_detail_result(soup: BeautifulSoup) -> Tuple[bool, str, str, str]:
    valid_page, validation_note = validate_product_page(soup)
    if not valid_page:
        return False, "", "", validation_note

    product_text = collect_product_text(soup)
    ean = extract_ean_from_soup(soup, product_text)
    second_hand = detect_second_hand(soup, product_text)
    return True, ean, second_hand, validation_note



def enrich_one(row: Dict[str, str]) -> Dict[str, str]:
    url = row.get("url", "")
    result = dict(row)
    last_status = ""
    last_note = ""

    for attempt in range(1, DETAIL_FETCH_RETRIES + 1):
        session = get_thread_session(force_new=(attempt > 1))
        try:
            soup = fetch_soup(session, url)
            valid_page, ean, second_hand, validation_note = parse_detail_result(soup)

            result["detail_checked_at"] = now_iso()
            result["mogelijk_2e_hands"] = second_hand or "NEE"

            if not valid_page:
                last_status = "retry_ongeldige_pagina"
                last_note = f"poging {attempt}/{DETAIL_FETCH_RETRIES}: {validation_note}"
            elif ean:
                result["ean"] = ean
                result["detail_status"] = "ok"
                result["detail_opmerking"] = ""
                result["_status"] = "ok"
                return result
            elif second_hand == "JA":
                result["ean"] = ""
                result["detail_status"] = "2e_hands_bevestigd"
                result["detail_opmerking"] = "2e hands-vermelding gevonden; geen EAN zichtbaar"
                result["_status"] = "2e hands bevestigd"
                return result
            else:
                last_status = "geen_ean_na_retries"
                last_note = f"poging {attempt}/{DETAIL_FETCH_RETRIES}: valide productpagina, maar geen EAN gevonden"

        except Exception as exc:
            result["detail_checked_at"] = now_iso()
            last_status = "fout_bij_detailfetch"
            last_note = f"poging {attempt}/{DETAIL_FETCH_RETRIES}: {exc}"

        if attempt < DETAIL_FETCH_RETRIES:
            time.sleep(DETAIL_RETRY_SLEEP_SECONDS * attempt)

    result["detail_status"] = last_status or "geen_ean_na_retries"
    result["detail_opmerking"] = last_note
    result["mogelijk_2e_hands"] = result.get("mogelijk_2e_hands", "") or "NEE"
    result["ean"] = normalize_text(result.get("ean", row.get("ean", "")))
    result["_status"] = result["detail_status"]
    return result



def needs_enrichment(row: Dict[str, str]) -> bool:
    ean = normalize_text(row.get("ean", ""))
    detail_status = normalize_text(row.get("detail_status", ""))
    if ean:
        return False
    if detail_status in RESOLVED_DETAIL_STATUSES:
        return False
    return True



def enrich_all(
    input_path: str,
    output_path: str,
    workers: int = DEFAULT_WORKERS,
) -> Dict[str, Dict[str, str]]:
    rows_by_url = load_csv_as_dict(input_path, STEP2_COLUMNS)
    if not rows_by_url:
        print(f"[INFO] Geen records gevonden in {input_path}. Eerst listings scrapen.")
        return rows_by_url

    todo_rows = {url: row for url, row in rows_by_url.items() if needs_enrichment(row)}
    skipped = len(rows_by_url) - len(todo_rows)

    print(
        f"[INFO] Verrijking gestart vanuit {input_path} ({len(rows_by_url)} records | "
        f"te verrijken: {len(todo_rows)} | overgeslagen: {skipped})"
    )

    if not todo_rows:
        if output_path != input_path or not os.path.exists(output_path):
            write_csv(output_path, rows_by_url, STEP2_COLUMNS)
        write_detail_issues(rows_by_url)
        print(f"[KLAAR] Geen detailverrijking nodig; alle records hebben al een resolved detailstatus.")
        return rows_by_url

    rows_lock = threading.Lock()
    print_lock = threading.Lock()
    total = len(todo_rows)
    processed = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(enrich_one, row): url for url, row in todo_rows.items()}

        for future in as_completed(futures):
            url = futures[future]
            enriched_row = future.result()
            processed += 1

            with rows_lock:
                rows_by_url[url] = merge_row(rows_by_url.get(url), enriched_row, STEP2_COLUMNS)
                try:
                    write_csv(output_path, rows_by_url, STEP2_COLUMNS)
                    write_detail_issues(rows_by_url)
                except PermissionError as exc:
                    print_threadsafe(print_lock, f"[FOUT] Wegschrijven mislukt tijdens verrijking: {exc}")
                    raise

            artist = enriched_row.get("artist", "")
            title = enriched_row.get("title", "")
            ean = enriched_row.get("ean", "")
            second_hand = enriched_row.get("mogelijk_2e_hands", "")
            status = enriched_row.get("detail_status", enriched_row.get("_status", ""))
            note = enriched_row.get("detail_opmerking", "")
            print_threadsafe(
                print_lock,
                (
                    f"[OK] enrich {processed}/{total} | {artist} - {title} | "
                    f"EAN: {ean or '-'} | mogelijk 2e hands: {second_hand or '-'} | "
                    f"status: {status or '-'}{(' | ' + note) if note else ''}"
                ),
            )

    print(
        f"[KLAAR] {output_path} opgeslagen met {len(rows_by_url)} records; "
        f"{total} detailpagina's verwerkt. Missersbestand: {DETAIL_ISSUES_FILE}"
    )
    return rows_by_url


# ---------- Sync helpers ----------


def sync_step1_to_existing_step2(step1_rows: Dict[str, Dict[str, str]]) -> None:
    if not os.path.exists(STEP2_FILE):
        return

    step2_rows = load_csv_as_dict(STEP2_FILE, STEP2_COLUMNS)
    if not step2_rows:
        return

    changed = 0
    for url, step1_row in step1_rows.items():
        if url not in step2_rows:
            continue
        merged = merge_row(step2_rows[url], step1_row, STEP2_COLUMNS)
        if merged != step2_rows[url]:
            step2_rows[url] = merged
            changed += 1

    if changed:
        write_csv(STEP2_FILE, step2_rows, STEP2_COLUMNS)
        write_detail_issues(step2_rows)
        print(f"[SYNC] {STEP2_FILE} bijgewerkt met {changed} prijs/listing-updates.")


# ---------- Menu actions ----------


def run_step1(workers: int = DEFAULT_WORKERS) -> None:
    source_path = STEP1_FILE if os.path.exists(STEP1_FILE) else (STEP2_FILE if os.path.exists(STEP2_FILE) else STEP1_FILE)
    rows = scrape_all_listings(
        output_path=STEP1_FILE,
        columns=STEP1_COLUMNS,
        workers=workers,
        load_from_path=source_path,
        stop_on_no_new_urls=False,
    )
    sync_step1_to_existing_step2(rows)



def run_step2(workers: int = DEFAULT_WORKERS) -> None:
    source_path = STEP2_FILE if os.path.exists(STEP2_FILE) else STEP1_FILE
    enrich_all(input_path=source_path, output_path=STEP2_FILE, workers=workers)



def run_both(workers: int = DEFAULT_WORKERS) -> None:
    run_step1(workers=workers)
    run_step2(workers=workers)


# ---------- CLI ----------


def print_menu() -> None:
    print()
    print("Bob's Vinyl scraper | Nieuwe LP's")
    print("=" * 50)
    print("1. Alleen prijzen updaten via collectiepagina's")
    print("2. Alleen verrijken (records zonder EAN of zonder resolved detailstatus)")
    print("3. Beide")
    print("Q. Afsluiten")
    print()



def ask_workers() -> int:
    raw = input(f"Aantal bots [standaard {DEFAULT_WORKERS}]: ").strip()
    if not raw:
        return DEFAULT_WORKERS
    try:
        value = int(raw)
        if value < 1:
            raise ValueError
        return value
    except ValueError:
        print(f"Ongeldige invoer. Standaard {DEFAULT_WORKERS} wordt gebruikt.")
        return DEFAULT_WORKERS



def main() -> int:
    try:
        while True:
            print_menu()
            choice = input("Maak een keuze [1/2/3/Q]: ").strip().lower()

            if choice == "1":
                workers = ask_workers()
                run_step1(workers=workers)
                return 0
            if choice == "2":
                workers = ask_workers()
                run_step2(workers=workers)
                return 0
            if choice == "3" or choice == "":
                workers = ask_workers()
                run_both(workers=workers)
                return 0
            if choice == "q":
                print("Afgebroken.")
                return 0

            print("Ongeldige keuze. Kies 1, 2, 3 of Q.")
    except KeyboardInterrupt:
        print("\nAfgebroken door gebruiker.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
