#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import random
import re
import time
from collections import OrderedDict
from pathlib import Path
from typing import Dict, Iterable, List
from urllib.parse import urljoin, urlsplit, urlunsplit

from bs4 import BeautifulSoup
from playwright.sync_api import (
    Browser,
    BrowserContext,
    Error as PlaywrightError,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

BASE_DOMAIN = "https://www.sounds-venlo.nl"
DEFAULT_OUTPUT_DIR = "data/raw/soundsvenlo"
STEP1_FILE = "sounds_venlo_step1.csv"
STEP2_FILE = "sounds_venlo_step2_enriched.csv"

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
DEFAULT_DELAY_SECONDS = 0.45
DETAIL_DELAY_SECONDS = 0.30
STEP1_WRITE_EVERY_PAGES = 25
STEP2_WRITE_EVERY_RECORDS = 25
FETCH_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3.0
MAX_EMPTY_PAGES = 2

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

WARMUP_URLS = [
    f"{BASE_DOMAIN}/",
    f"{BASE_DOMAIN}/vinyl/",
]

COOKIE_SELECTORS = [
    "button:has-text('Accepteren')",
    "button:has-text('Alles accepteren')",
    "button:has-text('Accept')",
    "button:has-text('I agree')",
    "button:has-text('Akkoord')",
    "#onetrust-accept-btn-handler",
    "button[aria-label='Accept']",
]

LISTING_CARD_SELECTORS = [
    "div.relative.group",
    "main div.group",
    "a.full-click",
]

BLOCK_PATTERNS = [
    "403 forbidden",
    "access denied",
    "request unsuccessful",
    "toegang geweigerd",
    "forbidden",
    "not allowed",
    "blocked",
    "captcha",
]


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
            rows[url] = {col: normalize_text(raw.get(col, "")) for col in columns}
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
            writer.writerow({col: normalize_text(row.get(col, "")) for col in columns})


def merge_row(existing: Dict[str, str] | None, incoming: Dict[str, str], columns: List[str]) -> Dict[str, str]:
    if existing is None:
        base = {col: "" for col in columns}
        base.update(incoming)
        return base

    merged = dict(existing)
    for key, value in incoming.items():
        if key in {"bron_categorieen", "bron_listing_urls"}:
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


def contains_block_text(text: str) -> bool:
    lower = normalize_text(text).lower()
    return any(pattern in lower for pattern in BLOCK_PATTERNS)


def parse_listing_card(card, category_name: str, listing_url: str) -> Dict[str, str] | None:
    artist_node = card.select_one("span.font-bold")
    title_node = card.select_one("a.full-click") or card.select_one("a[href*='/vinyl/']")
    meta_node = card.select_one("p.text-xs") or card.select_one("p.text-gray-700")
    raw_text = normalize_text(card.get_text(" ", strip=True))

    if title_node is None:
        return None

    artist = normalize_artist_name(artist_node.get_text(" ", strip=True) if artist_node else "")
    title = normalize_text(title_node.get_text(" ", strip=True))
    href = normalize_text(title_node.get("href", ""))
    url = urljoin(BASE_DOMAIN, href)

    if not title or not url:
        return None

    meta_text = normalize_text(meta_node.get_text(" ", strip=True) if meta_node else raw_text)
    drager_match = re.search(r"\b(2-LP|3-LP|4-LP|5-LP|LP|12\"|7\")\b", meta_text, flags=re.IGNORECASE)
    price_match = re.search(r"€\s*([0-9]+(?:[.,][0-9]{2})?)", raw_text)
    stock_label = infer_stock_label(raw_text)

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

    cards = []
    for selector in LISTING_CARD_SELECTORS:
        cards = soup.select(selector)
        if cards:
            break

    rows: List[Dict[str, str]] = []
    seen_urls: set[str] = set()
    for card in cards:
        row = parse_listing_card(card, category_name, listing_url)
        if row is None:
            continue
        url = row.get("url", "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        rows.append(row)
    return rows


def parse_detail_page_html(html: str, url: str) -> Dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = normalize_text(soup.get_text(" ", strip=True))
    title_h1 = soup.select_one("h1")

    details = {
        "artist": "",
        "title": normalize_text(title_h1.get_text(" ", strip=True) if title_h1 else ""),
        "drager": "",
        "prijs": "",
        "op_voorraad": "",
        "ean": "",
        "genre": "",
        "release": "",
        "maatschappij": "",
    }

    if not details["artist"]:
        artist_candidates: List[str] = []
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

    stock_match = re.search(r"(Op voorraad:[^€#]{0,120}|Levertijd:[^€#]{0,120}|Uitverkocht)", page_text, flags=re.IGNORECASE)
    if stock_match:
        details["op_voorraad"] = infer_stock_label(stock_match.group(1))

    text_chunks = [normalize_text(x.get_text(" ", strip=True)) for x in soup.find_all(["a", "p", "span", "li", "div"]) ]
    text_chunks = [x for x in text_chunks if x]

    if not details["ean"]:
        for chunk in text_chunks:
            if re.fullmatch(r"\d{8,14}", chunk):
                details["ean"] = chunk
                break

    if not details["release"]:
        m = re.search(r"Release\s+(\d{2}-\d{2}-\d{4})", page_text, flags=re.IGNORECASE)
        if m:
            details["release"] = m.group(1)
        else:
            m = re.search(r"(\d{2}-\d{2}-\d{4})", page_text)
            if m:
                details["release"] = m.group(1)

    if details["title"] in text_chunks:
        title_index = text_chunks.index(details["title"])
        window = text_chunks[title_index + 1 : title_index + 10]
        filtered = []
        for item in window:
            if not item:
                continue
            if item == details["artist"] or item == details["title"]:
                continue
            if item.startswith("Release "):
                continue
            if re.fullmatch(r"\d{8,14}", item):
                continue
            if re.fullmatch(r"(2-LP|3-LP|4-LP|5-LP|LP|12\"|7\")", item, flags=re.IGNORECASE):
                continue
            filtered.append(item)
        if filtered:
            details["genre"] = filtered[0]
        if len(filtered) >= 2:
            details["maatschappij"] = filtered[1]

    if not details["genre"]:
        m = re.search(r"Genre\s*:?\s*([A-Za-z0-9&/'\- ]{2,80})", page_text, flags=re.IGNORECASE)
        if m:
            details["genre"] = normalize_text(m.group(1))

    if not details["maatschappij"]:
        m = re.search(r"Maatschappij\s*:?\s*([A-Za-z0-9&/'\- ]{2,80})", page_text, flags=re.IGNORECASE)
        if m:
            details["maatschappij"] = normalize_text(m.group(1))
        else:
            m = re.search(r"Label\s*:?\s*([A-Za-z0-9&/'\- ]{2,80})", page_text, flags=re.IGNORECASE)
            if m:
                details["maatschappij"] = normalize_text(m.group(1))

    details["url"] = url
    return details


class BrowserFetcher:
    def __init__(self, headless: bool = True) -> None:
        self.headless = headless
        self.user_agent = random.choice(USER_AGENTS)
        self.playwright: Playwright | None = None
        self.browser: Browser | None = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None

    def __enter__(self) -> "BrowserFetcher":
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--disable-popup-blocking",
                "--disable-renderer-backgrounding",
                "--disable-background-timer-throttling",
                "--disable-backgrounding-occluded-windows",
                "--window-size=1440,2000",
                "--no-sandbox",
            ],
        )
        self.context = self.browser.new_context(
            user_agent=self.user_agent,
            viewport={"width": 1440, "height": 2000},
            locale="nl-NL",
            timezone_id="Europe/Amsterdam",
            java_script_enabled=True,
            ignore_https_errors=True,
        )
        self.context.set_extra_http_headers(
            {
                "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
                "Upgrade-Insecure-Requests": "1",
            }
        )
        self.context.add_init_script(
            """
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
            Object.defineProperty(navigator, 'language', {get: () => 'nl-NL'});
            Object.defineProperty(navigator, 'languages', {get: () => ['nl-NL', 'nl', 'en-US', 'en']});
            Object.defineProperty(navigator, 'hardwareConcurrency', {get: () => 8});
            Object.defineProperty(navigator, 'deviceMemory', {get: () => 8});
            window.chrome = { runtime: {} };
            """
        )
        self.page = self.context.new_page()
        self.page.set_default_navigation_timeout(TIMEOUT_MS)
        self.page.set_default_timeout(TIMEOUT_MS)
        self._warm_up_session()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        for obj in (self.page, self.context, self.browser, self.playwright):
            try:
                if obj is not None:
                    obj.close() if hasattr(obj, "close") else obj.stop()
            except Exception:
                pass
        self.page = None
        self.context = None
        self.browser = None
        self.playwright = None

    def _dismiss_overlays(self) -> None:
        if self.page is None:
            return
        for selector in COOKIE_SELECTORS:
            try:
                locator = self.page.locator(selector).first
                if locator.is_visible(timeout=1200):
                    locator.click(timeout=1500)
                    self.page.wait_for_timeout(500)
            except Exception:
                continue

    def _warm_up_session(self) -> None:
        if self.page is None:
            return
        for warmup_url in WARMUP_URLS:
            try:
                self.page.goto(warmup_url, wait_until="domcontentloaded")
                self.page.wait_for_timeout(1500)
                self._dismiss_overlays()
                self.page.mouse.move(200, 300)
                self.page.mouse.wheel(0, 900)
                self.page.wait_for_timeout(500)
            except Exception:
                continue

    def _new_page(self) -> Page:
        assert self.context is not None
        page = self.context.new_page()
        page.set_default_navigation_timeout(TIMEOUT_MS)
        page.set_default_timeout(TIMEOUT_MS)
        return page

    def _looks_like_listing(self, html: str) -> bool:
        lower = html.lower()
        if contains_block_text(lower):
            return False
        return (
            "full-click" in lower
            or "font-bold" in lower
            or "/vinyl/" in lower
            or "relative group" in lower
        )

    def fetch_html(self, url: str, expect_listing: bool = False) -> str:
        last_error: Exception | None = None

        for attempt in range(1, FETCH_RETRIES + 1):
            page = self._new_page()
            try:
                response = page.goto(url, wait_until="domcontentloaded")
                page.wait_for_timeout(1500 + attempt * 500)
                self.page = page
                self._dismiss_overlays()
                page.mouse.move(250, 420)
                page.mouse.wheel(0, 1200)
                page.wait_for_timeout(800)
                html = page.content()
                status = response.status if response is not None else 0

                if status == 403 or contains_block_text(html):
                    raise RuntimeError(f"403/block response for {url}")
                if status >= 400:
                    raise RuntimeError(f"HTTP {status} for {url}")
                if expect_listing and not self._looks_like_listing(html):
                    raise RuntimeError(f"Listing markup not detected for {url}")
                return html
            except (PlaywrightTimeoutError, PlaywrightError, RuntimeError) as exc:
                last_error = exc
                try:
                    page.close()
                except Exception:
                    pass
                self.page = self._new_page()
                self._warm_up_session()
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)
            except Exception as exc:
                last_error = exc
                try:
                    page.close()
                except Exception:
                    pass
                self.page = self._new_page()
                self._warm_up_session()
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

        raise RuntimeError(str(last_error) if last_error else f"Failed to fetch {url}")


def scrape_step1(output_dir: str = DEFAULT_OUTPUT_DIR, headless: bool = True) -> Path:
    output_path = Path(output_dir) / STEP1_FILE
    rows_by_url = load_csv_as_dict(output_path, STEP1_COLUMNS)

    with BrowserFetcher(headless=headless) as fetcher:
        total_pages = 0
        for seed_url in SEED_URLS:
            category_name = category_name_from_seed(seed_url)
            empty_pages = 0
            known_urls_on_seed: set[str] = set()

            for page_num in range(1, 999):
                listing_url = build_page_url(seed_url, page_num)
                print(f"[soundsvenlo] step1 page {page_num}: {listing_url}")
                html = fetcher.fetch_html(listing_url, expect_listing=True)
                page_rows = parse_listing_page_html(html, listing_url, category_name)

                if not page_rows:
                    empty_pages += 1
                    if empty_pages >= MAX_EMPTY_PAGES:
                        print(f"[soundsvenlo] stop after {empty_pages} lege pagina's vanaf {listing_url}")
                        break
                    time.sleep(DEFAULT_DELAY_SECONDS)
                    continue

                empty_pages = 0
                new_on_page = 0
                duplicate_page = 0
                for row in page_rows:
                    url = row["url"]
                    if url in known_urls_on_seed:
                        duplicate_page += 1
                    else:
                        known_urls_on_seed.add(url)
                    before = rows_by_url.get(url)
                    rows_by_url[url] = merge_row(before, row, STEP1_COLUMNS)
                    if before is None:
                        new_on_page += 1

                total_pages += 1
                if total_pages % STEP1_WRITE_EVERY_PAGES == 0:
                    write_csv(output_path, rows_by_url, STEP1_COLUMNS)

                print(
                    f"[soundsvenlo] page {page_num}: rows={len(page_rows)} new={new_on_page} "
                    f"known_on_seed={len(known_urls_on_seed)} duplicates_seen={duplicate_page} total={len(rows_by_url)}"
                )
                time.sleep(DEFAULT_DELAY_SECONDS + random.uniform(0.1, 0.4))

    write_csv(output_path, rows_by_url, STEP1_COLUMNS)
    print(f"[soundsvenlo] step1 klaar: {len(rows_by_url)} records -> {output_path}")
    return output_path


def iter_step2_targets(step1_rows: Dict[str, Dict[str, str]], step2_rows: Dict[str, Dict[str, str]]) -> Iterable[Dict[str, str]]:
    for url, row in step1_rows.items():
        existing = step2_rows.get(url)
        if existing and normalize_text(existing.get("ean", "")):
            continue
        yield row


def scrape_step2(
    output_dir: str = DEFAULT_OUTPUT_DIR,
    limit_detail: int | None = None,
    headless: bool = True,
) -> Path:
    step1_path = Path(output_dir) / STEP1_FILE
    step2_path = Path(output_dir) / STEP2_FILE

    step1_rows = load_csv_as_dict(step1_path, STEP1_COLUMNS)
    if not step1_rows:
        raise SystemExit(f"Geen step1-bestand gevonden of leeg: {step1_path}")

    step2_rows = load_csv_as_dict(step2_path, STEP2_COLUMNS)
    targets = list(iter_step2_targets(step1_rows, step2_rows))
    if limit_detail is not None:
        targets = targets[:limit_detail]

    with BrowserFetcher(headless=headless) as fetcher:
        processed = 0
        for row in targets:
            url = row["url"]
            print(f"[soundsvenlo] step2 detail {processed + 1}/{len(targets)}: {url}")
            html = fetcher.fetch_html(url, expect_listing=False)
            detail = parse_detail_page_html(html, url)
            merged = {col: "" for col in STEP2_COLUMNS}
            for col in STEP1_COLUMNS:
                merged[col] = normalize_text(row.get(col, ""))
            for col in STEP2_COLUMNS:
                if detail.get(col):
                    merged[col] = normalize_text(detail.get(col, ""))
            step2_rows[url] = merge_row(step2_rows.get(url), merged, STEP2_COLUMNS)

            processed += 1
            if processed % STEP2_WRITE_EVERY_RECORDS == 0:
                write_csv(step2_path, step2_rows, STEP2_COLUMNS)
            time.sleep(DETAIL_DELAY_SECONDS + random.uniform(0.1, 0.35))

    write_csv(step2_path, step2_rows, STEP2_COLUMNS)
    print(f"[soundsvenlo] step2 klaar: {len(step2_rows)} records -> {step2_path}")
    return step2_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sounds Venlo scraper (legacy engine)")
    parser.add_argument("--mode", choices=["step1", "step2", "both"], default="both")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--limit-detail", type=int, default=None)
    parser.add_argument("--headful", action="store_true", help="Run browser headed instead of headless")
    return parser


def main(argv: List[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    headless = not args.headful

    if args.mode in {"step1", "both"}:
        scrape_step1(output_dir=args.output_dir, headless=headless)
    if args.mode in {"step2", "both"}:
        scrape_step2(output_dir=args.output_dir, limit_detail=args.limit_detail, headless=headless)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
