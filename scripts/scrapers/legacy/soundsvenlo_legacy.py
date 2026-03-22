
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import time
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup
from playwright.sync_api import Browser, BrowserContext, Page, Playwright, sync_playwright

BASE_DOMAIN = "https://www.sounds-venlo.nl"
DEFAULT_OUTPUT_DIR = "data/raw/soundsvenlo"
STEP1_FILE = "sounds_venlo_step1.csv"
STEP2_FILE = "sounds_venlo_step2_enriched.csv"

# The site now serves vinyl pages behind anti-bot rules that reject simple HTTP requests.
# We intentionally crawl through a browser and keep the seed list minimal: /vinyl/ exposes
# the full vinyl assortment, while the product detail pages provide the importer fields we need.
SEED_URLS = [
    f"{BASE_DOMAIN}/vinyl/",
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

TIMEOUT_MS = 45_000
DEFAULT_DELAY_SECONDS = 0.35
DETAIL_DELAY_SECONDS = 0.20
STEP1_WRITE_EVERY_PAGES = 25
STEP2_WRITE_EVERY_RECORDS = 25

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)


def normalize_text(value: str | None) -> str:
    if value is None:
        return ""
    value = value.replace("\xa0", " ")
    return re.sub(r"\s+", " ", value).strip()


def nl_price(value: str) -> str:
    value = normalize_text(value).replace("€", "").replace(" ", "")
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
    path = parts.path if parts.path.endswith("/") else parts.path + "/"
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


def load_csv_as_dict(path: Path, columns: List[str]) -> Dict[str, Dict[str, str]]:
    rows: Dict[str, Dict[str, str]] = OrderedDict()
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for raw in reader:
            url = normalize_text(raw.get("url", ""))
            if not url:
                continue
            row = {col: normalize_text(raw.get(col, "")) for col in columns}
            rows[url] = row
    return rows


def write_csv(path: Path, rows_by_url: Dict[str, Dict[str, str]], columns: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for _, row in sorted(
            rows_by_url.items(),
            key=lambda item: (
                item[1].get("artist", ""),
                item[1].get("title", ""),
                item[0],
            ),
        ):
            safe_row = {col: normalize_text(row.get(col, "")) for col in columns}
            writer.writerow(safe_row)


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


def infer_stock_label(text: str) -> str:
    lower = text.lower()
    if "op voorraad" in lower:
        return "JA"
    if "levertijd" in lower or "uitverkocht" in lower or "niet op voorraad" in lower:
        return "NEE"
    return ""


def parse_listing_card(card, category_name: str, listing_url: str) -> Dict[str, str] | None:
    artist_node = card.select_one("span.font-bold")
    title_node = card.select_one("a.full-click")
    meta_node = card.select_one("p.text-xs") or card.select_one("p.text-gray-700")

    raw_text = normalize_text(card.get_text(" ", strip=True))
    if title_node is None:
        return None

    artist = normalize_artist_name(artist_node.get_text(" ", strip=True) if artist_node else "")
    title = normalize_text(title_node.get_text(" ", strip=True))
    href = normalize_text(title_node.get("href", ""))
    url = urljoin(BASE_DOMAIN, href)

    meta_text = normalize_text(meta_node.get_text(" ", strip=True) if meta_node else raw_text)
    drager_match = re.search(r"\b(2-LP|3-LP|4-LP|5-LP|LP|12\"|7\")\b", meta_text, flags=re.IGNORECASE)
    price_match = re.search(r"€\s*([0-9]+(?:[.,][0-9]{2})?)", raw_text)
    stock_label = infer_stock_label(raw_text)

    if not title or not url:
        return None

    if not artist:
        lines = [normalize_text(x) for x in raw_text.split("\n") if normalize_text(x)]
        if lines:
            artist = normalize_artist_name(lines[0])

    return {
        "url": url,
        "artist": artist,
        "title": title,
        "drager": normalize_text(drager_match.group(1) if drager_match else ""),
        "prijs": nl_price(price_match.group(1)) if price_match else "",
        "op_voorraad": stock_label,
        "bron_categorieen": category_name,
        "bron_listing_urls": listing_url,
    }


def parse_listing_page_html(html: str, listing_url: str, category_name: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("div.relative.group")
    rows: List[Dict[str, str]] = []
    for card in cards:
        row = parse_listing_card(card, category_name, listing_url)
        if row is not None and row.get("url"):
            rows.append(row)
    return rows


def parse_detail_page_html(html: str, url: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = normalize_text(soup.get_text(" ", strip=True))

    details = {
        "artist": "",
        "title": "",
        "drager": "",
        "prijs": "",
        "op_voorraad": "",
        "ean": "",
        "genre": "",
        "release": "",
        "maatschappij": "",
    }

    # Core product info area
    title_h1 = soup.select_one("h1")
    if title_h1 is not None:
        details["title"] = normalize_text(title_h1.get_text(" ", strip=True))

    # Prefer visible structured block near the title, but also keep regex fallbacks on the rendered page text.
    if not details["artist"]:
        artist_candidates = []
        for node in soup.select("main a, main p, main span"):
            text = normalize_text(node.get_text(" ", strip=True))
            if not text:
                continue
            if text == details["title"]:
                continue
            if re.fullmatch(r"\d{8,14}", text):
                continue
            if text.upper() == text and len(text) > 4:
                continue
            artist_candidates.append(text)
        if artist_candidates:
            details["artist"] = normalize_artist_name(artist_candidates[0])

    drager_match = re.search(r"\b(2-LP|3-LP|4-LP|5-LP|LP|12\"|7\")\b", page_text, flags=re.IGNORECASE)
    if drager_match:
        details["drager"] = normalize_text(drager_match.group(1))

    price_match = re.search(r"€\s*([0-9]+(?:[.,][0-9]{2})?)", page_text)
    if price_match:
        details["prijs"] = nl_price(price_match.group(1))

    stock_match = re.search(r"(Op voorraad:[^€#]{0,80}|Levertijd:[^€#]{0,80}|Uitverkocht)", page_text, flags=re.IGNORECASE)
    if stock_match:
        details["op_voorraad"] = infer_stock_label(stock_match.group(1))

    # Use the visible info block that is currently present on product pages.
    if not details["genre"] or not details["maatschappij"] or not details["ean"] or not details["release"]:
        product_anchor = title_h1.find_parent("main") if title_h1 else soup
        all_text = [normalize_text(x.get_text(" ", strip=True)) for x in product_anchor.find_all(["a", "p", "span"], recursive=True)]
        all_text = [x for x in all_text if x]

        if not details["ean"]:
            for part in all_text:
                if re.fullmatch(r"\d{8,14}", part):
                    details["ean"] = part
                    break

        if not details["release"]:
            m = re.search(r"Release\s+(\d{2}-\d{2}-\d{4})", page_text, flags=re.IGNORECASE)
            if m:
                details["release"] = m.group(1)

        if not details["genre"]:
            # On current product pages the sequence is typically:
            # breadcrumb -> artist -> title -> genre -> label -> format -> ean -> release
            title_index = -1
            if details["title"] in all_text:
                title_index = all_text.index(details["title"])
            if title_index >= 0:
                window = all_text[title_index + 1 : title_index + 8]
                for idx, item in enumerate(window):
                    if not item or item == details["artist"] or item == details["title"]:
                        continue
                    if item.startswith("Release "):
                        continue
                    if re.fullmatch(r"\d{8,14}", item):
                        continue
                    if re.fullmatch(r"(2-LP|3-LP|4-LP|5-LP|LP|12\"|7\")", item, flags=re.IGNORECASE):
                        continue
                    details["genre"] = item
                    if idx + 1 < len(window):
                        next_item = window[idx + 1]
                        if (
                            next_item
                            and not re.fullmatch(r"\d{8,14}", next_item)
                            and not next_item.startswith("Release ")
                            and not re.fullmatch(r"(2-LP|3-LP|4-LP|5-LP|LP|12\"|7\")", next_item, flags=re.IGNORECASE)
                        ):
                            details["maatschappij"] = next_item
                    break

        if not details["maatschappij"]:
            # Label often appears right before format or right after genre.
            m = re.search(
                r"#\s*[^\n]+?\s+([A-Za-zÀ-ÿ/&][A-Za-zÀ-ÿ0-9 '&/.\-]+)\s+(2-LP|3-LP|4-LP|5-LP|LP|12\"|7\")\s+\d{8,14}",
                page_text,
                flags=re.IGNORECASE,
            )
            if m:
                details["maatschappij"] = normalize_text(m.group(1))

    return details


def needs_detail_enrichment(row: Dict[str, str]) -> bool:
    needed = ("ean", "genre", "release", "maatschappij")
    return not all(normalize_text(row.get(field, "")) for field in needed)


class BrowserFetcher:
    def __init__(self, headless: bool = True) -> None:
        self.headless = headless
        self._playwright: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None

    def __enter__(self) -> "BrowserFetcher":
        self._playwright = sync_playwright().start()
        self.browser = self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
            ],
        )
        self.context = self.browser.new_context(
            user_agent=USER_AGENT,
            locale="nl-NL",
            timezone_id="Europe/Amsterdam",
            viewport={"width": 1440, "height": 2200},
            extra_http_headers={
                "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
                "Upgrade-Insecure-Requests": "1",
                "DNT": "1",
            },
        )
        self.context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
            Object.defineProperty(navigator, 'language', { get: () => 'nl-NL' });
            Object.defineProperty(navigator, 'languages', { get: () => ['nl-NL', 'nl', 'en-US', 'en'] });
            Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
            """
        )
        self.page = self.context.new_page()
        self.page.set_default_navigation_timeout(TIMEOUT_MS)
        self.page.set_default_timeout(TIMEOUT_MS)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            if self.page is not None:
                self.page.close()
        finally:
            try:
                if self.context is not None:
                    self.context.close()
            finally:
                try:
                    if self.browser is not None:
                        self.browser.close()
                finally:
                    if self._playwright is not None:
                        self._playwright.stop()

    def fetch_html(self, url: str) -> str:
        assert self.page is not None

        response = self.page.goto(url, wait_until="domcontentloaded")
        self.page.wait_for_timeout(900)
        self._dismiss_overlays()

        status = response.status if response is not None else 0
        html = self.page.content()
        if status >= 400:
            raise RuntimeError(f"{status} response for {url}")

        if self.page.locator("div.relative.group").count() == 0 and "toegang geweigerd" in html.lower():
            raise RuntimeError(f"Access denied for {url}")

        return html

    def _dismiss_overlays(self) -> None:
        assert self.page is not None
        selectors = [
            "button:has-text('Accepteren')",
            "button:has-text('Accept')",
            "button:has-text('Alles accepteren')",
            "button:has-text('Sluiten')",
        ]
        for selector in selectors:
            try:
                locator = self.page.locator(selector).first
                if locator.count() > 0 and locator.is_visible():
                    locator.click(timeout=2_000)
                    self.page.wait_for_timeout(250)
            except Exception:
                pass


def scrape_step1(output_dir: Path, headless: bool = True) -> Dict[str, Dict[str, str]]:
    step1_path = output_dir / STEP1_FILE
    rows_by_url = load_csv_as_dict(step1_path, STEP1_COLUMNS)
    if rows_by_url:
        print(f"[INFO] Bestaand bestand geladen: {step1_path} ({len(rows_by_url)} records)")
    else:
        print(f"[INFO] Start met leeg bestand: {step1_path}")

    total_pages_done = 0
    pages_since_write = 0

    with BrowserFetcher(headless=headless) as fetcher:
        for seed_url in SEED_URLS:
            category_name = category_name_from_seed(seed_url)
            seen_in_category: set[str] = set()
            page_no = 1

            while True:
                page_url = build_page_url(seed_url, page_no)
                try:
                    html = fetcher.fetch_html(page_url)
                    page_rows = parse_listing_page_html(html, page_url, category_name)
                except Exception as exc:
                    print(f"[FOUT] {category_name} | pagina {page_no} kon niet worden geladen: {exc}")
                    break

                if not page_rows:
                    print(f"[STOP] {category_name} | pagina {page_no} bevat geen artikelen meer.")
                    break

                page_urls = [row["url"] for row in page_rows]
                new_in_category = [u for u in page_urls if u not in seen_in_category]
                if not new_in_category:
                    print(f"[STOP] {category_name} | pagina {page_no} bevat geen nieuwe URL's meer.")
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
                    write_csv(step1_path, rows_by_url, STEP1_COLUMNS)
                    print(
                        f"[SAVE] tussentijds opgeslagen na {pages_since_write} pagina's | "
                        f"totaal bestand: {len(rows_by_url)}"
                    )
                    pages_since_write = 0

                print(
                    f"[OK] {category_name} | pagina {page_no} | artikelen: {len(page_rows)} | "
                    f"nieuw in categorie: {len(new_in_category)} | nieuw in bestand: {new_in_file} | "
                    f"geüpdatet: {updated_in_file} | totaal bestand: {len(rows_by_url)}"
                )

                page_no += 1
                time.sleep(DEFAULT_DELAY_SECONDS)

    write_csv(step1_path, rows_by_url, STEP1_COLUMNS)
    print(f"[KLAAR] {step1_path.name} opgeslagen met {len(rows_by_url)} records uit {total_pages_done} pagina's.")
    return rows_by_url


def scrape_step2(output_dir: Path, headless: bool = True, limit_detail: int | None = None) -> Dict[str, Dict[str, str]]:
    step1_path = output_dir / STEP1_FILE
    step2_path = output_dir / STEP2_FILE

    source_path = step2_path if step2_path.exists() else step1_path
    rows_by_url = load_csv_as_dict(source_path, STEP2_COLUMNS)

    if not rows_by_url:
        print("[INFO] Geen bronbestand gevonden. Draai eerst stap 1 of both.")
        write_csv(step2_path, rows_by_url, STEP2_COLUMNS)
        return rows_by_url

    print(f"[INFO] Bronbestand geladen: {source_path} ({len(rows_by_url)} records)")

    urls_to_process = [url for url, row in rows_by_url.items() if needs_detail_enrichment(row)]
    if limit_detail is not None:
        urls_to_process = urls_to_process[:limit_detail]

    total = len(urls_to_process)
    if total == 0:
        write_csv(step2_path, rows_by_url, STEP2_COLUMNS)
        print(f"[INFO] Alle records zijn al verrijkt. {step2_path.name} is opnieuw opgeslagen.")
        return rows_by_url

    processed_since_write = 0
    done = 0

    with BrowserFetcher(headless=headless) as fetcher:
        for url in urls_to_process:
            try:
                html = fetcher.fetch_html(url)
                details = parse_detail_page_html(html, url)
            except Exception as exc:
                print(f"[FOUT] detail kon niet worden geladen: {url} | {exc}")
                details = {
                    "artist": "",
                    "title": "",
                    "drager": "",
                    "prijs": "",
                    "op_voorraad": "",
                    "ean": "",
                    "genre": "",
                    "release": "",
                    "maatschappij": "",
                }

            existing = rows_by_url.get(url, {col: "" for col in STEP2_COLUMNS})
            merged = dict(existing)

            for field in ("artist", "title", "drager", "prijs", "op_voorraad", "ean", "genre", "release", "maatschappij"):
                value = normalize_text(details.get(field, ""))
                if value:
                    merged[field] = value

            for col in STEP2_COLUMNS:
                merged.setdefault(col, "")
            rows_by_url[url] = merged

            done += 1
            processed_since_write += 1

            print(
                f"[DETAIL] {done}/{total} | {normalize_text(merged.get('artist'))} - "
                f"{normalize_text(merged.get('title'))} | ean={normalize_text(merged.get('ean'))} | "
                f"prijs={normalize_text(merged.get('prijs'))}"
            )

            if processed_since_write >= STEP2_WRITE_EVERY_RECORDS:
                write_csv(step2_path, rows_by_url, STEP2_COLUMNS)
                print(
                    f"[SAVE] step2 tussentijds opgeslagen na {processed_since_write} detailrecords | "
                    f"totaal bestand: {len(rows_by_url)}"
                )
                processed_since_write = 0

            time.sleep(DETAIL_DELAY_SECONDS)

    write_csv(step2_path, rows_by_url, STEP2_COLUMNS)
    print(f"[KLAAR] {step2_path.name} opgeslagen met {len(rows_by_url)} records.")
    return rows_by_url


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sounds Venlo scraper via Playwright")
    parser.add_argument("--mode", choices=["step1", "step2", "both"], default="both")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit-detail", type=int, default=None)
    parser.add_argument("--headful", action="store_true", help="Run browser headed instead of headless")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    headless = not args.headful

    if args.mode in {"step1", "both"}:
        scrape_step1(output_dir=output_dir, headless=headless)

    if args.mode in {"step2", "both"}:
        scrape_step2(output_dir=output_dir, headless=headless, limit_detail=args.limit_detail)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
