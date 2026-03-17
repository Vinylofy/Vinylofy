from __future__ import annotations

import csv
import os
import re
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DOMAIN = "https://www.sounds-venlo.nl"
TIMEOUT = 30
DEFAULT_DELAY_SECONDS = 0.20
DETAIL_DELAY_SECONDS = 0.05
DETAIL_WORKERS = 10
STEP1_WRITE_EVERY_PAGES = 50
STEP2_WRITE_EVERY_RECORDS = 50
STEP1_FILE = "sounds_venlo_step1.csv"
STEP2_FILE = "sounds_venlo_step2_enriched.csv"

SEED_URLS = [
    "https://www.sounds-venlo.nl/pop-2/?filter-type=2213183",
    "https://www.sounds-venlo.nl/blues/?filter-type=2213183",
    "https://www.sounds-venlo.nl/electronic/?filter-type=2213183",
    "https://www.sounds-venlo.nl/experimenteel-avant-garde/?filter-type=2213183",
    "https://www.sounds-venlo.nl/rock-2/?filter-type=2213183",
    "https://www.sounds-venlo.nl/hiphop-2/?filter-type=2213183",
    "https://www.sounds-venlo.nl/jazz-2/?filter-type=2213183",
    "https://www.sounds-venlo.nl/klassiek/?filter-type=2213183",
    "https://www.sounds-venlo.nl/original-sound-tracks/?filter-type=2213183",
    "https://www.sounds-venlo.nl/prog-page/?filter-type=2213183",
    "https://www.sounds-venlo.nl/psyche-stoner/?filter-type=2213183",
    "https://www.sounds-venlo.nl/punk-garage-new-wave/?filter-type=2213183",
    "https://www.sounds-venlo.nl/reggae-2/?filter-type=2213183",
    "https://www.sounds-venlo.nl/roots-americana-folk/?filter-type=2213183",
    "https://www.sounds-venlo.nl/soul/?filter-type=2213183",
    "https://www.sounds-venlo.nl/world-3/?filter-type=2213183",
    "https://www.sounds-venlo.nl/re%C3%AFssues-box-sets/?filter-type=2213183",
]

STEP1_COLUMNS = [
    "url",
    "artist",
    "title",
    "drager",
    "prijs",
    "op_voorraad",
    "bron_categorieen",
    "bron_listing_urls",
]

STEP2_COLUMNS = STEP1_COLUMNS + [
    "ean",
    "genre",
    "release",
    "maatschappij",
]

thread_local = threading.local()


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=DETAIL_WORKERS + 2, pool_maxsize=DETAIL_WORKERS + 2)
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


def get_thread_session() -> requests.Session:
    session = getattr(thread_local, "session", None)
    if session is None:
        session = make_session()
        thread_local.session = session
    return session


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def nl_price(value: str) -> str:
    value = normalize_text(value)
    value = value.replace("€", "").strip()
    value = value.replace(" ", "")
    return value.replace(".", ",")


def normalize_artist_name(value: str) -> str:
    value = normalize_text(value)
    if "," not in value:
        return value

    parts = [normalize_text(x) for x in value.split(",") if normalize_text(x)]
    if len(parts) >= 2:
        return " ".join(parts[1:] + [parts[0]])
    return value


def category_name_from_seed(seed_url: str) -> str:
    path = urlsplit(seed_url).path.strip("/")
    slug = path.split("/")[0] if path else "onbekend"
    return slug.replace("-", " ")


def build_page_url(seed_url: str, page: int) -> str:
    parts = urlsplit(seed_url)
    path = parts.path
    if not path.endswith("/"):
        path += "/"
    if page <= 1:
        new_path = path
    else:
        new_path = f"{path}p{page}/"
    return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))


def unique_pipe_join(existing: str, new_value: str) -> str:
    items = [x for x in (existing or "").split(" | ") if x]
    if new_value and new_value not in items:
        items.append(new_value)
    return " | ".join(items)


def load_csv_as_dict(path: str, columns: List[str]) -> Dict[str, Dict[str, str]]:
    rows: Dict[str, Dict[str, str]] = OrderedDict()
    if not os.path.exists(path):
        return rows

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            url = normalize_text(raw.get("url", ""))
            if not url:
                continue
            row = {col: normalize_text(raw.get(col, "")) for col in columns}
            rows[url] = row
    return rows


def write_csv(path: str, rows_by_url: Dict[str, Dict[str, str]], columns: List[str]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for _, row in sorted(
            rows_by_url.items(),
            key=lambda item: (item[1].get("artist", ""), item[1].get("title", ""), item[0]),
        ):
            safe_row = {col: normalize_text(row.get(col, "")) for col in columns}
            writer.writerow(safe_row)


def fetch_soup(session: requests.Session, url: str) -> BeautifulSoup:
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def merge_row(existing: Dict[str, str] | None, incoming: Dict[str, str], columns: List[str]) -> Dict[str, str]:
    if existing is None:
        merged = {col: "" for col in columns}
        merged.update(incoming)
        return merged

    merged = dict(existing)
    for key, value in incoming.items():
        if key == "bron_categorieen":
            merged[key] = unique_pipe_join(merged.get(key, ""), value)
        elif key == "bron_listing_urls":
            merged[key] = unique_pipe_join(merged.get(key, ""), value)
        elif value:
            merged[key] = value

    for col in columns:
        merged.setdefault(col, "")
    return merged


def parse_listing_card(card, category_name: str, listing_url: str) -> Dict[str, str] | None:
    artist_node = card.select_one("span.font-bold")
    title_node = card.select_one("a.full-click")
    meta_node = card.select_one("p.text-xs") or card.select_one("p.text-gray-700")

    if artist_node is None or title_node is None or meta_node is None:
        return None

    artist_raw = normalize_text(artist_node.get_text(" ", strip=True))
    artist = normalize_artist_name(artist_raw)
    title = normalize_text(title_node.get_text(" ", strip=True))
    href = normalize_text(title_node.get("href", ""))
    url = urljoin(BASE_DOMAIN, href)

    meta_text = normalize_text(meta_node.get_text(" ", strip=True))
    drager_match = re.search(r"^(2-LP|3-LP|4-LP|5-LP|LP|12\"|7\")\b", meta_text, flags=re.IGNORECASE)
    drager = normalize_text(drager_match.group(1) if drager_match else "")

    price_match = re.search(r"€\s*([0-9]+(?:[\.,][0-9]{2})?)", meta_text)
    prijs = ""
    if price_match:
        prijs = price_match.group(1).replace(".", ",")

    voorraad_text = ""
    voorraad_span = meta_node.select_one("span.block")
    if voorraad_span:
        voorraad_text = normalize_text(voorraad_span.get_text(" ", strip=True))
    else:
        voorraad_text = meta_text
    op_voorraad = "JA" if "Op voorraad" in voorraad_text else "NEE"

    if not url or not title:
        return None

    return {
        "url": url,
        "artist": artist,
        "title": title,
        "drager": drager,
        "prijs": prijs,
        "op_voorraad": op_voorraad,
        "bron_categorieen": category_name,
        "bron_listing_urls": listing_url,
    }


def parse_listing_page(session: requests.Session, url: str, category_name: str) -> List[Dict[str, str]]:
    soup = fetch_soup(session, url)
    cards = soup.select("div.relative.group")
    rows: List[Dict[str, str]] = []
    for card in cards:
        row = parse_listing_card(card, category_name, url)
        if row is not None and row.get("url"):
            rows.append(row)
    return rows


def scrape_step1() -> Dict[str, Dict[str, str]]:
    session = make_session()
    rows_by_url = load_csv_as_dict(STEP1_FILE, STEP1_COLUMNS)

    if rows_by_url:
        print(f"[INFO] Bestaand bestand geladen: {STEP1_FILE} ({len(rows_by_url)} records)")
    else:
        print(f"[INFO] Start met leeg bestand: {STEP1_FILE}")

    total_pages_done = 0
    pages_since_write = 0

    for seed_url in SEED_URLS:
        category_name = category_name_from_seed(seed_url)
        seen_in_category = set()
        page = 1

        while True:
            page_url = build_page_url(seed_url, page)
            try:
                page_rows = parse_listing_page(session, page_url, category_name)
            except Exception as exc:
                print(f"[FOUT] {category_name} | pagina {page} kon niet worden geladen: {exc}")
                break

            if not page_rows:
                print(f"[STOP] {category_name} | pagina {page} bevat geen artikelen meer.")
                break

            page_urls = [row["url"] for row in page_rows]
            new_in_category = [u for u in page_urls if u not in seen_in_category]
            if not new_in_category:
                print(f"[STOP] {category_name} | pagina {page} bevat geen nieuwe URL's meer.")
                break

            new_in_file = 0
            updated_in_file = 0
            for row in page_rows:
                existing = rows_by_url.get(row["url"])
                if existing is None:
                    new_in_file += 1
                else:
                    updated_in_file += 1
                rows_by_url[row["url"]] = merge_row(existing, row, STEP1_COLUMNS)

            seen_in_category.update(new_in_category)
            total_pages_done += 1
            pages_since_write += 1

            if pages_since_write >= STEP1_WRITE_EVERY_PAGES:
                write_csv(STEP1_FILE, rows_by_url, STEP1_COLUMNS)
                print(f"[SAVE] tussentijds opgeslagen na {pages_since_write} pagina's | totaal bestand: {len(rows_by_url)}")
                pages_since_write = 0

            print(
                f"[OK] {category_name} | pagina {page} | artikelen: {len(page_rows)} | "
                f"nieuw in categorie: {len(new_in_category)} | nieuw in bestand: {new_in_file} | "
                f"geüpdatet: {updated_in_file} | totaal bestand: {len(rows_by_url)}"
            )

            page += 1
            time.sleep(DEFAULT_DELAY_SECONDS)

    write_csv(STEP1_FILE, rows_by_url, STEP1_COLUMNS)
    print(f"[KLAAR] {STEP1_FILE} opgeslagen met {len(rows_by_url)} records uit {total_pages_done} pagina's.")
    return rows_by_url


def parse_detail_page(session: requests.Session, url: str) -> Dict[str, str]:
    soup = fetch_soup(session, url)

    details = {
        "ean": "",
        "genre": "",
        "release": "",
        "maatschappij": "",
    }

    title_h1 = soup.select_one("h1")
    if title_h1 is None:
        return details

    wrapper = title_h1.find_parent("div")
    while wrapper is not None:
        text = normalize_text(wrapper.get_text(" ", strip=True))
        if "Release" in text and ("LP" in text or '12"' in text or '7"' in text):
            break
        wrapper = wrapper.find_parent("div")

    search_root = wrapper if wrapper is not None else soup

    first_meta = search_root.select_one("div.flex.divide-x.divide-black.divide-solid.text-tiny.md\\:text-base.mb-4")
    second_meta = search_root.select_one("div.flex.divide-x.divide-black.divide-solid.text-tiny.md\\:text-base.mb-6")

    if first_meta is not None:
        parts = [normalize_text(x.get_text(" ", strip=True)) for x in first_meta.find_all(["a", "p"], recursive=False)]
        parts = [p for p in parts if p]
        if len(parts) >= 1:
            details["genre"] = parts[0]
        if len(parts) >= 2:
            details["maatschappij"] = parts[1]
        for part in parts:
            if re.fullmatch(r"\d{8,14}", part):
                details["ean"] = part
                break

    if second_meta is not None:
        parts = [normalize_text(x.get_text(" ", strip=True)) for x in second_meta.find_all("p", recursive=False)]
        for part in parts:
            if part.lower().startswith("release "):
                details["release"] = normalize_text(part[8:])
                break

    if not details["ean"] or not details["genre"] or not details["maatschappij"] or not details["release"]:
        all_text = []
        for node in search_root.find_all(["a", "p"]):
            text = normalize_text(node.get_text(" ", strip=True))
            if text:
                all_text.append(text)
        if not details["ean"]:
            for part in all_text:
                if re.fullmatch(r"\d{8,14}", part):
                    details["ean"] = part
                    break
        if not details["release"]:
            for part in all_text:
                if part.lower().startswith("release "):
                    details["release"] = normalize_text(part[8:])
                    break

    return details


def needs_detail_enrichment(row: Dict[str, str]) -> bool:
    return not all(normalize_text(row.get(field, "")) for field in ("ean", "genre", "release", "maatschappij"))


def parse_detail_worker(url: str) -> tuple[str, Dict[str, str]]:
    session = get_thread_session()
    details = parse_detail_page(session, url)
    if DETAIL_DELAY_SECONDS > 0:
        time.sleep(DETAIL_DELAY_SECONDS)
    return url, details


def scrape_step2() -> Dict[str, Dict[str, str]]:
    source_path = STEP2_FILE if os.path.exists(STEP2_FILE) else STEP1_FILE
    rows_by_url = load_csv_as_dict(source_path, STEP2_COLUMNS)

    if not rows_by_url:
        print("[INFO] Geen bronbestand gevonden. Draai eerst stap 1 of beide.")
        return {}

    print(f"[INFO] Bronbestand geladen: {source_path} ({len(rows_by_url)} records)")

    urls_to_process = [url for url, row in rows_by_url.items() if needs_detail_enrichment(row)]
    total = len(urls_to_process)

    if total == 0:
        write_csv(STEP2_FILE, rows_by_url, STEP2_COLUMNS)
        print(f"[INFO] Alle records zijn al verrijkt. Bestand opnieuw opgeslagen: {STEP2_FILE}")
        return rows_by_url

    print(f"[INFO] Detailverrijking gestart voor {total} records | workers={DETAIL_WORKERS} | batch-save={STEP2_WRITE_EVERY_RECORDS}")

    updated = 0
    failed = 0
    completed_since_write = 0

    with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as executor:
        future_to_url = {executor.submit(parse_detail_worker, url): url for url in urls_to_process}

        for index, future in enumerate(as_completed(future_to_url), start=1):
            url = future_to_url[future]
            try:
                result_url, details = future.result()
                rows_by_url[result_url] = merge_row(rows_by_url.get(result_url), details, STEP2_COLUMNS)
                updated += 1
                completed_since_write += 1
                print(
                    f"[DETAIL] {index}/{total} | bijgewerkt | "
                    f"EAN: {rows_by_url[result_url].get('ean', '') or '-'} | {result_url}"
                )
            except Exception as exc:
                failed += 1
                completed_since_write += 1
                print(f"[FOUT] detail {index}/{total} kon niet worden geladen: {url} | {exc}")

            if completed_since_write >= STEP2_WRITE_EVERY_RECORDS:
                write_csv(STEP2_FILE, rows_by_url, STEP2_COLUMNS)
                print(f"[SAVE] tussentijds opgeslagen na {completed_since_write} detailrecords | totaal bestand: {len(rows_by_url)}")
                completed_since_write = 0

    write_csv(STEP2_FILE, rows_by_url, STEP2_COLUMNS)
    print(
        f"[KLAAR] {STEP2_FILE} opgeslagen met {len(rows_by_url)} records. "
        f"Bijgewerkt: {updated} | Fouten: {failed}"
    )
    return rows_by_url


def run_both() -> None:
    step1_rows = scrape_step1()
    if not step1_rows:
        return

    seed_for_step2: Dict[str, Dict[str, str]] = OrderedDict()
    for url, row in step1_rows.items():
        seed_for_step2[url] = {col: row.get(col, "") for col in STEP2_COLUMNS}
    write_csv(STEP2_FILE, seed_for_step2, STEP2_COLUMNS)

    scrape_step2()


def print_menu() -> None:
    print()
    print("Sounds Venlo scraper")
    print("=" * 50)
    print("1. Stap 1 - scrape artiest, titel, drager, prijs, voorraad en URL")
    print("2. Stap 2 - vul EAN, genre, release en maatschappij aan vanaf detailpagina's")
    print("3. Beide - alles in één run")
    print("Q. Afsluiten")
    print()


def main() -> int:
    try:
        while True:
            print_menu()
            choice = input("Maak een keuze [1/2/3/Q]: ").strip().lower()

            if choice == "1":
                scrape_step1()
                return 0
            if choice == "2":
                scrape_step2()
                return 0
            if choice == "3" or choice == "":
                run_both()
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
