#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stand-alone scraper for https://www.platenzaak.nl/collections/vinyl

Phases
1. Scan listing pages for new titles and price updates
2. Enrich product pages with EAN and detail fields
3. Run both phases in sequence

Outputs
- platenzaak_listing.csv
- platenzaak_enriched.csv
- platenzaak_master.csv
- platenzaak_changes.csv
- platenzaak_errors.log

Dependencies:
    pip install requests beautifulsoup4
"""

from __future__ import annotations

import csv
import hashlib
import os
import re
import sys
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.platenzaak.nl"
COLLECTION_URL = f"{BASE_URL}/collections/vinyl"
SHOP_NAME = "platenzaak.nl"

LISTING_CSV = "platenzaak_listing.csv"
ENRICHED_CSV = "platenzaak_enriched.csv"
MASTER_CSV = "platenzaak_master.csv"
CHANGES_CSV = "platenzaak_changes.csv"
ERROR_LOG = "platenzaak_errors.log"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

LISTING_FIELDS = [
    "source_shop",
    "product_key",
    "artist",
    "title",
    "price",
    "currency",
    "availability",
    "product_url",
    "page_found",
    "scraped_at",
]

ENRICH_FIELDS = [
    "product_url",
    "ean",
    "release_date",
    "product_type",
    "contents",
    "edition",
    "vinyl_details",
    "exclusive",
    "reissue",
    "boxset",
    "coloured_vinyl",
    "enriched_at",
]

CHANGE_FIELDS = [
    "change_type",
    "product_url",
    "artist",
    "title",
    "old_price",
    "new_price",
    "detected_at",
]

DETAIL_LABEL_MAP = {
    "Releasedatum": "release_date",
    "Producttype": "product_type",
    "Inhoud": "contents",
    "Editie": "edition",
    "Vinyl details": "vinyl_details",
    "Exclusive": "exclusive",
    "Reissue": "reissue",
    "Boxset": "boxset",
    "Coloured Vinyl": "coloured_vinyl",
    "EAN": "ean",
}


@dataclass
class ScrapeStats:
    pages_processed: int = 0
    products_seen: int = 0
    new_products: int = 0
    price_updates: int = 0


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def log_error(message: str) -> None:
    timestamp = utc_now_iso()
    line = f"[{timestamp}] {message}\n"
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(line)


def fetch_soup(session: requests.Session, url: str, timeout: int = 30) -> BeautifulSoup:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def to_absolute_url(href: str) -> str:
    return urljoin(BASE_URL, href)


def product_key_from_url(url: str) -> str:
    clean = normalize_product_url(url)
    return hashlib.md5(clean.encode("utf-8")).hexdigest()


def normalize_product_url(url: str) -> str:
    if not url:
        return ""
    parsed = urlparse(url)
    path = parsed.path.rstrip("/")
    return f"{BASE_URL}{path}"


def safe_float_from_price_text(text: str) -> str:
    if not text:
        return ""
    cleaned = text.replace("€", "").replace("\xa0", " ")
    cleaned = re.sub(r"\s+", "", cleaned)
    cleaned = cleaned.replace(",", ".")
    match = re.search(r"\d+(?:\.\d{1,2})?", cleaned)
    if not match:
        return ""
    value = float(match.group(0))
    return f"{value:.2f}"


def read_csv_as_dict(path: str, key_field: str) -> Dict[str, dict]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        out: Dict[str, dict] = {}
        for row in reader:
            key = (row.get(key_field) or "").strip()
            if key:
                out[key] = row
        return out


def write_csv(path: str, rows: Iterable[dict], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def append_changes(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def discover_last_page(soup: BeautifulSoup) -> int:
    max_page = 1
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/collections/vinyl" not in href:
            continue
        parsed = urlparse(urljoin(BASE_URL, href))
        qs = parse_qs(parsed.query)
        page_value = qs.get("page", [None])[0]
        if page_value and str(page_value).isdigit():
            max_page = max(max_page, int(page_value))
    return max_page


def extract_availability(block: BeautifulSoup) -> str:
    text = normalize_space(block.get_text(" ", strip=True))
    if "Uitverkocht" in text:
        return "Uitverkocht"
    if "Pre-Order" in text or "Pre-order" in text:
        return "Pre-Order"
    if "In winkelmandje" in text:
        return "In winkelmandje"
    return ""


def extract_price(block: BeautifulSoup) -> str:
    wishlist = block.select_one("[data-wlh-price]")
    if wishlist and wishlist.get("data-wlh-price"):
        value = wishlist.get("data-wlh-price", "").strip()
        try:
            return f"{float(value):.2f}"
        except ValueError:
            pass

    amount = block.select_one(".price .amount")
    if amount:
        return safe_float_from_price_text(amount.get_text(" ", strip=True))

    price_text = normalize_space(block.get_text(" ", strip=True))
    return safe_float_from_price_text(price_text)


def extract_artist_and_title(title_link: BeautifulSoup) -> Tuple[str, str]:
    strings = [normalize_space(s) for s in title_link.stripped_strings if normalize_space(s)]
    artist = ""
    title = ""

    artist_el = title_link.select_one("span")
    if artist_el:
        artist = normalize_space(artist_el.get_text(" ", strip=True))

    if strings:
        if artist:
            if normalize_space(strings[0]).casefold() == artist.casefold():
                title = normalize_space(" ".join(strings[1:]))
            else:
                title = normalize_space(" ".join([s for s in strings if s.casefold() != artist.casefold()]))
        elif len(strings) >= 2:
            artist = strings[0]
            title = normalize_space(" ".join(strings[1:]))
        else:
            title = strings[0]

    combined = normalize_space(title_link.get_text(" ", strip=True))
    if not title and combined:
        title = combined

    return artist, title


def parse_listing_block(block: BeautifulSoup, page_number: int) -> Optional[dict]:
    title_link = block.select_one(".product-block__title-price a.title")
    if not title_link or not title_link.get("href"):
        return None

    product_url = normalize_product_url(to_absolute_url(title_link["href"]))
    artist, title = extract_artist_and_title(title_link)
    price = extract_price(block)
    availability = extract_availability(block)

    return {
        "source_shop": SHOP_NAME,
        "product_key": product_key_from_url(product_url),
        "artist": artist,
        "title": title,
        "price": price,
        "currency": "EUR",
        "availability": availability,
        "product_url": product_url,
        "page_found": str(page_number),
        "scraped_at": utc_now_iso(),
    }


def parse_listing_page(soup: BeautifulSoup, page_number: int) -> List[dict]:
    items: List[dict] = []
    seen_urls = set()

    for block in soup.select("div.product-block__inner"):
        row = parse_listing_block(block, page_number)
        if not row:
            continue
        url = row["product_url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)
        items.append(row)

    return items


def scan_listing(max_pages: Optional[int] = None, delay_seconds: float = 0.25) -> ScrapeStats:
    session = build_session()
    existing = read_csv_as_dict(LISTING_CSV, "product_url")
    current: Dict[str, dict] = {}
    stats = ScrapeStats()
    changes_buffer: List[dict] = []

    print(f"[SCAN] start: {COLLECTION_URL}")
    first_soup = fetch_soup(session, COLLECTION_URL)
    discovered_last_page = discover_last_page(first_soup)
    last_page = discovered_last_page if max_pages is None else min(discovered_last_page, max_pages)
    print(f"[SCAN] gedetecteerde laatste pagina: {discovered_last_page} | te scrapen: {last_page}")

    for page in range(1, last_page + 1):
        url = COLLECTION_URL if page == 1 else f"{COLLECTION_URL}?page={page}"
        try:
            soup = first_soup if page == 1 else fetch_soup(session, url)
            page_rows = parse_listing_page(soup, page)
        except Exception as exc:
            log_error(f"SCAN page={page} url={url} error={exc}")
            print(f"[SCAN] pagina {page}/{last_page} FOUT: {exc}")
            continue

        page_new = 0
        page_updates = 0
        for row in page_rows:
            product_url = row["product_url"]
            current[product_url] = row
            stats.products_seen += 1

            old = existing.get(product_url)
            if old is None:
                page_new += 1
                stats.new_products += 1
                changes_buffer.append({
                    "change_type": "new_product",
                    "product_url": product_url,
                    "artist": row.get("artist", ""),
                    "title": row.get("title", ""),
                    "old_price": "",
                    "new_price": row.get("price", ""),
                    "detected_at": utc_now_iso(),
                })
            else:
                old_price = (old.get("price") or "").strip()
                new_price = (row.get("price") or "").strip()
                if old_price and new_price and old_price != new_price:
                    page_updates += 1
                    stats.price_updates += 1
                    changes_buffer.append({
                        "change_type": "price_update",
                        "product_url": product_url,
                        "artist": row.get("artist", old.get("artist", "")),
                        "title": row.get("title", old.get("title", "")),
                        "old_price": old_price,
                        "new_price": new_price,
                        "detected_at": utc_now_iso(),
                    })

        write_csv(LISTING_CSV, current.values(), LISTING_FIELDS)
        if changes_buffer:
            append_changes(CHANGES_CSV, changes_buffer, CHANGE_FIELDS)
            changes_buffer = []

        stats.pages_processed += 1
        print(
            f"[SCAN] pagina {page}/{last_page} | producten={len(page_rows)} | "
            f"nieuw={page_new} | prijsupdates={page_updates} | totaal_uniek={len(current)}"
        )
        if page < last_page:
            time.sleep(delay_seconds)

    build_master_file()
    print(
        f"[SCAN] klaar | pagina's={stats.pages_processed} | totaal_uniek={len(current)} | "
        f"nieuw={stats.new_products} | prijsupdates={stats.price_updates}"
    )
    return stats


def extract_detail_table(soup: BeautifulSoup) -> Dict[str, str]:
    details = {field: "" for field in ENRICH_FIELDS if field not in {"product_url", "enriched_at"}}

    for row in soup.select("div.product-detail-accordion table tr"):
        cells = row.select("td")
        if len(cells) < 2:
            continue
        label = normalize_space(cells[0].get_text(" ", strip=True))
        value = normalize_space(cells[1].get_text(" ", strip=True))
        mapped = DETAIL_LABEL_MAP.get(label)
        if mapped:
            details[mapped] = value

    if not details.get("ean"):
        body_text = normalize_space(soup.get_text(" ", strip=True))
        match = re.search(r"\bEAN\s*([0-9]{8,14})\b", body_text, flags=re.IGNORECASE)
        if match:
            details["ean"] = match.group(1)

    return details


def enrich_product(session: requests.Session, product_url: str) -> dict:
    soup = fetch_soup(session, product_url)
    details = extract_detail_table(soup)
    row = {field: "" for field in ENRICH_FIELDS}
    row["product_url"] = product_url
    row.update(details)
    row["enriched_at"] = utc_now_iso()
    return row


def determine_enrichment_targets(force_all: bool = False, limit_enrich: int | None = None) -> List[str]:
    listing = read_csv_as_dict(LISTING_CSV, "product_url")
    enriched = read_csv_as_dict(ENRICHED_CSV, "product_url")

    if not listing:
        raise FileNotFoundError(
            f"Geen {LISTING_CSV} gevonden. Draai eerst listing scan (optie 1 of 3)."
        )

    targets: List[str] = []
    for product_url in listing.keys():
        if force_all:
            targets.append(product_url)
            continue
        e = enriched.get(product_url)
        if e is None:
            targets.append(product_url)
            continue
        if not (e.get("ean") or "").strip():
            targets.append(product_url)
            continue
    if limit_enrich is not None and limit_enrich > 0:
        targets = targets[:limit_enrich]
    return targets


def run_enrichment(force_all: bool = False, delay_seconds: float = 0.25, limit_enrich: int | None = None) -> int:
    session = build_session()
    listing = read_csv_as_dict(LISTING_CSV, "product_url")
    enriched = read_csv_as_dict(ENRICHED_CSV, "product_url")
    targets = determine_enrichment_targets(force_all=force_all, limit_enrich=limit_enrich)

    total = len(targets)
    print(f"[ENRICH] te verrijken urls: {total} | force_all={force_all} | limit_enrich={limit_enrich}")
    if total == 0:
        build_master_file()
        print("[ENRICH] niets te doen.")
        return 0

    success = 0
    for idx, product_url in enumerate(targets, start=1):
        try:
            row = enrich_product(session, product_url)
            enriched[product_url] = row
            success += 1
            ean = row.get("ean", "") or "-"
            print(f"[ENRICH] {idx}/{total} OK | EAN={ean} | {product_url}")
        except Exception as exc:
            log_error(f"ENRICH idx={idx} url={product_url} error={exc}")
            title = listing.get(product_url, {}).get("title", "")
            print(f"[ENRICH] {idx}/{total} FOUT | {title or product_url} | {exc}")
        finally:
            write_csv(ENRICHED_CSV, enriched.values(), ENRICH_FIELDS)
            build_master_file()
            if idx < total:
                time.sleep(delay_seconds)

    print(f"[ENRICH] klaar | success={success}/{total}")
    return success


def build_master_file() -> None:
    listing = read_csv_as_dict(LISTING_CSV, "product_url")
    enriched = read_csv_as_dict(ENRICHED_CSV, "product_url")
    if not listing and not enriched:
        return

    rows: List[dict] = []
    all_urls = list(dict.fromkeys(list(listing.keys()) + list(enriched.keys())))
    for url in all_urls:
        merged = {}
        merged.update(listing.get(url, {}))
        merged.update(enriched.get(url, {}))
        merged["product_url"] = url
        if "product_key" not in merged or not merged.get("product_key"):
            merged["product_key"] = product_key_from_url(url)
        if "source_shop" not in merged or not merged.get("source_shop"):
            merged["source_shop"] = SHOP_NAME
        rows.append(merged)

    master_fields = []
    for field in LISTING_FIELDS + ENRICH_FIELDS:
        if field not in master_fields:
            master_fields.append(field)

    write_csv(MASTER_CSV, rows, master_fields)


def ask_menu_choice() -> str:
    print("\nPLATENZAAK SCRAPER")
    print("1. Scan nieuwe titels + prijsupdates")
    print("2. Enrichment (EAN + detailvelden)")
    print("3. Beide")
    print("0. Stop")
    return input("Kies een optie [1/2/3/0]: ").strip()


def ask_max_pages() -> Optional[int]:
    raw = input("Max aantal pagina's? Enter = alles: ").strip()
    if not raw:
        return None
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    print("Ongeldige invoer, ik pak alles.")
    return None


def ask_force_all_enrichment() -> bool:
    raw = input("Alles opnieuw verrijken? [j/N]: ").strip().lower()
    return raw in {"j", "ja", "y", "yes"}


def ask_limit_enrich() -> Optional[int]:
    raw = input("Max aantal detailpagina's? Enter = alles: ").strip()
    if not raw:
        return None
    if raw.isdigit() and int(raw) > 0:
        return int(raw)
    print("Ongeldige invoer, ik pak alles.")
    return None


def main() -> int:
    try:
        choice = ask_menu_choice()
        if choice == "0":
            print("Gestopt.")
            return 0
        if choice == "1":
            max_pages = ask_max_pages()
            scan_listing(max_pages=max_pages)
            return 0
        if choice == "2":
            force_all = ask_force_all_enrichment()
            run_enrichment(force_all=force_all)
            return 0
        if choice == "3":
            max_pages = ask_max_pages()
            scan_listing(max_pages=max_pages)
            run_enrichment(force_all=False)
            return 0

        print("Onbekende keuze.")
        return 1
    except KeyboardInterrupt:
        print("\nAfgebroken door gebruiker.")
        return 130
    except Exception as exc:
        log_error(f"FATAL error={exc}")
        print(f"FOUT: {exc}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
