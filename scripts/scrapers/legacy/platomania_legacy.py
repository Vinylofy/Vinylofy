from __future__ import annotations

import csv
import os
import re
import sys
import time
from collections import OrderedDict
from typing import Dict, List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DOMAIN = "https://www.platomania.nl"
DEFAULT_DELAY_SECONDS = 0.35
STEP1_FILE = "platomania_step1.csv"
STEP2_FILE = "platomania_step2_enriched.csv"
TIMEOUT = 30

SEED_URLS = [
    "https://www.platomania.nl/nieuw-vinyl-poprock",
    "https://www.platomania.nl/nieuw-vinyl-metalpunk",
    "https://www.platomania.nl/nieuw-vinyl-nederlandstalig",
    "https://www.platomania.nl/nieuw-vinyl-symfoprog",
    "https://www.platomania.nl/roots-vinyl",
    "https://www.platomania.nl/world-vinyl",
    "https://www.platomania.nl/nieuw-vinyl-hiphopsoul",
    "https://www.platomania.nl/nieuw-vinyl-reggaedub",
    "https://www.platomania.nl/nieuw-vinyl-jazz",
    "https://www.platomania.nl/nieuw-vinyl-electronic-albums",
    "https://www.platomania.nl/etalage/list/id/3715",
    "https://www.platomania.nl/nieuw-vinyl-klassiek",
    "https://www.platomania.nl/vinyl-reissues",
]

STEP1_COLUMNS = [
    "url",
    "artist",
    "title",
    "type",
    "drager",
    "prijs",
    "op_voorraad",
    "bron_categorieen",
    "bron_listing_urls",
]

STEP2_COLUMNS = STEP1_COLUMNS + [
    "label",
    "releasedatum",
    "herkomst",
    "item_nr",
    "ean",
]


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
    )
    adapter = HTTPAdapter(max_retries=retry)
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


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def nl_price(value: str) -> str:
    value = normalize_text(value).replace("€", "").strip()
    value = value.replace(" ", "")
    return value.replace(".", ",")


def page_url(seed_url: str, page: int) -> str:
    if page <= 1:
        return seed_url
    separator = "&" if "?" in seed_url else "?"
    return f"{seed_url}{separator}page={page}"


def category_name_from_url(url: str) -> str:
    slug = url.rstrip("/").split("/")[-1]
    slug = slug.replace("-", " ")
    return slug


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
        for _, row in sorted(rows_by_url.items(), key=lambda item: (item[1].get("artist", ""), item[1].get("title", ""), item[0])):
            safe_row = {col: normalize_text(row.get(col, "")) for col in columns}
            writer.writerow(safe_row)


def fetch_soup(session: requests.Session, url: str) -> BeautifulSoup:
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def get_direct_type_text(content) -> str:
    for child in content.children:
        if getattr(child, "name", None) == "p":
            text = normalize_text(child.get_text(" ", strip=True))
            if text:
                return text
    return ""


def parse_details_from_content(content) -> Dict[str, str]:
    details = {
        "label": "",
        "releasedatum": "",
        "herkomst": "",
        "item_nr": "",
        "ean": "",
    }

    for node in content.select("div.article-details__text"):
        classes = node.get("class", [])
        if "article-details__delivery-text" in classes:
            continue

        text = normalize_text(node.get_text(" ", strip=True))
        if not text or text.startswith("Status:"):
            continue

        if text.startswith("Label:"):
            details["label"] = normalize_text(text.split(":", 1)[1])
        elif text.startswith("Releasedatum:"):
            details["releasedatum"] = normalize_text(text.split(":", 1)[1])
        elif text.startswith("Herkomst:"):
            details["herkomst"] = normalize_text(text.split(":", 1)[1])
        elif text.startswith("Item-nr:"):
            details["item_nr"] = normalize_text(text.split(":", 1)[1])
        elif text.startswith("EAN:"):
            details["ean"] = normalize_text(text.split(":", 1)[1])

    return details


def parse_article(article, category_name: str, listing_url: str, include_extra: bool) -> Dict[str, str] | None:
    content = article.select_one("div.article__content")
    if content is None:
        return None

    artist = normalize_text((content.select_one("h1.product-card__artist") or {}).get_text(" ", strip=True) if content.select_one("h1.product-card__artist") else "")
    title = normalize_text((content.select_one("h2.product-card__title") or {}).get_text(" ", strip=True) if content.select_one("h2.product-card__title") else "")

    article_link = content.select_one('a[href^="/article/"]') or article.select_one('a[href^="/article/"]')
    href = article_link.get("href", "") if article_link else ""
    absolute_url = urljoin(BASE_DOMAIN, href)

    medium = normalize_text(article.select_one("div.article__medium").get_text(" ", strip=True) if article.select_one("div.article__medium") else "")
    price = nl_price(article.select_one("div.article__price").get_text(" ", strip=True) if article.select_one("div.article__price") else "")
    type_text = get_direct_type_text(content)

    delivery = normalize_text(article.select_one("div.article__delivery-time").get_text(" ", strip=True) if article.select_one("div.article__delivery-time") else "")
    in_stock = "JA" if "Op voorraad" in delivery else "NEE"

    if not absolute_url or not title:
        return None

    row = {
        "url": absolute_url,
        "artist": artist,
        "title": title,
        "type": type_text,
        "drager": medium,
        "prijs": price,
        "op_voorraad": in_stock,
        "bron_categorieen": category_name,
        "bron_listing_urls": listing_url,
    }

    if include_extra:
        row.update(parse_details_from_content(content))

    return row


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


# ---------- Scraping ----------

def scrape_seed(
    session: requests.Session,
    seed_url: str,
    rows_by_url: Dict[str, Dict[str, str]],
    columns: List[str],
    include_extra: bool,
    output_path: str,
) -> Tuple[int, int]:
    category_name = category_name_from_url(seed_url)
    seen_in_category = set()
    total_pages = 0
    total_new_for_output = 0

    page = 1
    while True:
        current_url = page_url(seed_url, page)
        try:
            soup = fetch_soup(session, current_url)
        except Exception as exc:
            print(f"[FOUT] {category_name} | pagina {page} kon niet worden geladen: {exc}")
            break

        articles = soup.select("article.article")
        parsed_rows: List[Dict[str, str]] = []
        page_urls: List[str] = []

        for article in articles:
            row = parse_article(article, category_name, current_url, include_extra)
            if row is None:
                continue
            parsed_rows.append(row)
            page_urls.append(row["url"])

        if not parsed_rows:
            print(f"[STOP] {category_name} | pagina {page} bevat geen artikelen meer.")
            break

        new_in_category = [url for url in page_urls if url not in seen_in_category]
        if not new_in_category:
            print(f"[STOP] {category_name} | pagina {page} bevat geen nieuwe URL's meer.")
            break

        new_for_output = 0
        updated_for_output = 0
        for row in parsed_rows:
            row_url = row["url"]
            existing = rows_by_url.get(row_url)
            if existing is None:
                new_for_output += 1
            else:
                updated_for_output += 1
            rows_by_url[row_url] = merge_row(existing, row, columns)

        seen_in_category.update(new_in_category)
        total_pages += 1
        total_new_for_output += new_for_output

        write_csv(output_path, rows_by_url, columns)
        print(
            f"[OK] {category_name} | pagina {page} | artikelen: {len(parsed_rows)} | "
            f"nieuw in categorie: {len(new_in_category)} | nieuw in bestand: {new_for_output} | "
            f"geüpdatet: {updated_for_output} | totaal bestand: {len(rows_by_url)}"
        )

        page += 1
        time.sleep(DEFAULT_DELAY_SECONDS)

    return total_pages, total_new_for_output


def scrape_all(
    mode: str,
    output_path: str,
    columns: List[str],
    load_from_path: str | None = None,
) -> Dict[str, Dict[str, str]]:
    include_extra = mode in {"step2", "both"}
    load_path = load_from_path or output_path
    rows_by_url = load_csv_as_dict(load_path, columns)

    if rows_by_url:
        print(f"[INFO] Bestaand bestand geladen: {load_path} ({len(rows_by_url)} records)")
    else:
        print(f"[INFO] Start met leeg bestand: {output_path}")

    session = make_session()
    grand_total_pages = 0

    for seed_url in SEED_URLS:
        pages_done, _ = scrape_seed(
            session=session,
            seed_url=seed_url,
            rows_by_url=rows_by_url,
            columns=columns,
            include_extra=include_extra,
            output_path=output_path,
        )
        grand_total_pages += pages_done

    write_csv(output_path, rows_by_url, columns)
    print(f"[KLAAR] {output_path} opgeslagen met {len(rows_by_url)} records uit {grand_total_pages} pagina's.")
    return rows_by_url


# ---------- Menu actions ----------

def run_step1() -> None:
    scrape_all(mode="step1", output_path=STEP1_FILE, columns=STEP1_COLUMNS)


def run_step2() -> None:
    source_path = STEP1_FILE if os.path.exists(STEP1_FILE) else STEP2_FILE
    scrape_all(mode="step2", output_path=STEP2_FILE, columns=STEP2_COLUMNS, load_from_path=source_path)


def run_both() -> None:
    enriched_rows = scrape_all(mode="both", output_path=STEP2_FILE, columns=STEP2_COLUMNS)

    step1_rows: Dict[str, Dict[str, str]] = OrderedDict()
    for url, row in enriched_rows.items():
        step1_rows[url] = {col: row.get(col, "") for col in STEP1_COLUMNS}
    write_csv(STEP1_FILE, step1_rows, STEP1_COLUMNS)
    print(f"[KLAAR] {STEP1_FILE} opgeslagen met {len(step1_rows)} records.")


# ---------- CLI ----------

def print_menu() -> None:
    print()
    print("Platomania scraper")
    print("=" * 50)
    print("1. Stap 1 - scrape artiest, titel, type, drager, prijs, voorraad en URL")
    print("2. Stap 2 - vul label, releasedatum, herkomst, item-nr en EAN aan vanaf landingspagina's")
    print("3. Beide - alles in één run")
    print("Q. Afsluiten")
    print()


def main() -> int:
    try:
        while True:
            print_menu()
            choice = input("Maak een keuze [1/2/3/Q]: ").strip().lower()

            if choice == "1":
                run_step1()
                return 0
            if choice == "2":
                run_step2()
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
