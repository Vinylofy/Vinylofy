#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

SHOP_ID = "soundshaarlem"
BASE_URL = "https://www.soundshaarlem.nl"
LISTING_PATH = "/nl/pagina/lp"
DEFAULT_OUTPUT_DIR = "data/raw/soundshaarlem"
DEFAULT_OUTPUT_FILE = "soundshaarlem_products.csv"
DEFAULT_STATE_FILE = "soundshaarlem_state.json"
DEFAULT_TIMEOUT = 30
DEFAULT_DELAY_SECONDS = 0.5
DEFAULT_LIMIT = 192
DEFAULT_SORT = "release"

CSV_COLUMNS = [
    "shop_id",
    "source_type",
    "listing_url",
    "detail_url",
    "release_id_raw",
    "ean_raw",
    "ean_normalized",
    "ean_source",
    "artist_slug_raw",
    "title_slug_raw",
    "display_name_raw",
    "title_raw",
    "artist_raw",
    "format_label_raw",
    "price_current",
    "price_old",
    "currency",
    "availability",
    "availability_text",
    "release_date_raw",
    "import_text",
    "scraped_at",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}

PRICE_RE = re.compile(r"€\s*([0-9][0-9\.,]*)")
DETAIL_URL_RE = re.compile(
    r"/(?:nl|en)/release/(?P<release_id>\d+)/(?P<artist_slug>[^/]+)/(?P<title_slug>[^/]+)/(?:[^/]+/)?(?P<format_slug>[^/]+)/(?P<ean_tail>\d+)/?$",
    re.IGNORECASE,
)
FORMAT_LINE_RE = re.compile(r"^\d+\s*-\s*.+$", re.IGNORECASE)
PAGE_STATS_RE = re.compile(
    r"Pagina\s+(?P<page>\d+)\s+van\s+totaal\s+(?P<total_pages>\d+)\.\s+Totaal resultaten:\s+(?P<total_results>[0-9\.,]+)",
    re.IGNORECASE,
)


@dataclass
class PageStats:
    page: int | None = None
    total_pages: int | None = None
    total_results: int | None = None


class ScraperError(RuntimeError):
    pass



def log(message: str) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{now}] [{SHOP_ID}] {message}")



def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p



def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()



def clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()



def split_clean_lines(value: str) -> list[str]:
    return [clean_text(line) for line in (value or "").splitlines() if clean_text(line)]



def parse_eur_to_decimal(text: str) -> str | None:
    raw = clean_text(text)
    if not raw:
        return None
    raw = raw.replace("€", "").replace(" ", "")
    if "," in raw and "." in raw:
        # English pages can already use dot decimals; Dutch pages often use comma.
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        raw = raw.replace(".", "").replace(",", ".")
    try:
        return f"{float(raw):.2f}"
    except ValueError:
        return None



def extract_prices(text: str) -> list[str]:
    values: list[str] = []
    for match in PRICE_RE.finditer(text or ""):
        amount = parse_eur_to_decimal(match.group(1))
        if amount:
            values.append(amount)
    return values



def choose_prices(prices: list[str]) -> tuple[str | None, str | None]:
    if not prices:
        return None, None
    if len(prices) == 1:
        return prices[0], None
    if prices[0] == prices[-1]:
        return prices[0], None
    return prices[-1], prices[0]



def is_valid_gtin13(candidate: str) -> bool:
    if not candidate or not candidate.isdigit() or len(candidate) != 13:
        return False
    digits = [int(ch) for ch in candidate]
    checksum = digits[-1]
    body = digits[:-1]
    total = 0
    for idx, digit in enumerate(body):
        total += digit if idx % 2 == 0 else digit * 3
    expected = (10 - (total % 10)) % 10
    return checksum == expected



def normalize_ean(candidate: str | None) -> str | None:
    if not candidate:
        return None
    digits = re.sub(r"\D", "", candidate)
    if len(digits) == 12:
        digits = f"0{digits}"
    if is_valid_gtin13(digits):
        return digits
    return None



def parse_detail_url(detail_url: str) -> dict[str, str]:
    parsed = urlparse(detail_url)
    match = DETAIL_URL_RE.search(parsed.path)
    if not match:
        return {
            "release_id_raw": "",
            "artist_slug_raw": "",
            "title_slug_raw": "",
            "ean_raw": "",
            "ean_normalized": "",
            "ean_source": "",
        }
    ean_tail = match.group("ean_tail")
    ean_normalized = normalize_ean(ean_tail)
    return {
        "release_id_raw": match.group("release_id"),
        "artist_slug_raw": match.group("artist_slug"),
        "title_slug_raw": match.group("title_slug"),
        "ean_raw": ean_tail,
        "ean_normalized": ean_normalized or "",
        "ean_source": "url" if ean_normalized else "",
    }



def find_card_container(anchor: Tag) -> Tag | None:
    for parent in anchor.parents:
        if not isinstance(parent, Tag):
            continue
        text = clean_text(parent.get_text("\n", strip=True))
        if "€" in text and len(text) <= 900:
            return parent
        if parent.name in {"body", "html"}:
            break
    if isinstance(anchor.parent, Tag):
        return anchor.parent
    return None



def parse_format_from_lines(lines: list[str]) -> str:
    for line in lines:
        if FORMAT_LINE_RE.match(line):
            return line.replace(" -", "-").replace("- ", "-")
    return ""



def parse_page_stats(page_text: str) -> PageStats:
    match = PAGE_STATS_RE.search(page_text)
    if not match:
        return PageStats()
    total_results_raw = match.group("total_results").replace(".", "").replace(",", "")
    total_results = int(total_results_raw) if total_results_raw.isdigit() else None
    return PageStats(
        page=int(match.group("page")),
        total_pages=int(match.group("total_pages")),
        total_results=total_results,
    )



def default_row(scraped_at: str) -> dict[str, str]:
    return {column: "" for column in CSV_COLUMNS} | {
        "shop_id": SHOP_ID,
        "currency": "EUR",
        "availability": "unknown",
        "scraped_at": scraped_at,
    }


class SoundsHaarlemScraper:
    def __init__(self, timeout: int = DEFAULT_TIMEOUT, delay_seconds: float = DEFAULT_DELAY_SECONDS) -> None:
        self.timeout = timeout
        self.delay_seconds = delay_seconds
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def get(self, url: str) -> str:
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)
        return response.text

    def build_listing_url(self, page: int, limit: int = DEFAULT_LIMIT, sort: str = DEFAULT_SORT) -> str:
        return f"{BASE_URL}{LISTING_PATH}?limit={limit}&page={page}&sort={sort}"

    def parse_listing_page(self, html: str, listing_url: str, scraped_at: str) -> tuple[list[dict[str, str]], PageStats]:
        soup = BeautifulSoup(html, "html.parser")
        page_text = soup.get_text("\n", strip=True)
        stats = parse_page_stats(page_text)
        rows_by_url: dict[str, dict[str, str]] = {}

        for anchor in soup.find_all("a", href=True):
            href = clean_text(anchor.get("href", ""))
            if "/release/" not in href:
                continue

            detail_url = urljoin(BASE_URL, href.split("#", 1)[0])
            parsed_bits = parse_detail_url(detail_url)
            row = rows_by_url.get(detail_url)
            if row is None:
                row = default_row(scraped_at)
                row.update(parsed_bits)
                row.update(
                    {
                        "source_type": "listing",
                        "listing_url": listing_url,
                        "detail_url": detail_url,
                    }
                )
                rows_by_url[detail_url] = row

            anchor_text = clean_text(anchor.get_text(" ", strip=True))
            if anchor_text and len(anchor_text) > len(row["display_name_raw"]):
                row["display_name_raw"] = anchor_text

            card = find_card_container(anchor)
            card_text = clean_text(card.get_text("\n", strip=True)) if card else ""
            if not card_text:
                continue
            lines = split_clean_lines(card_text)
            format_label = parse_format_from_lines(lines)
            if format_label and not row["format_label_raw"]:
                row["format_label_raw"] = format_label

            current, old = choose_prices(extract_prices(card_text))
            if current:
                row["price_current"] = current
                row["availability"] = row["availability"] or "in_stock"
            if old:
                row["price_old"] = old

        rows = list(rows_by_url.values())
        rows.sort(key=lambda item: (item["detail_url"], item["display_name_raw"]))
        return rows, stats

    def parse_detail_page(self, html: str, detail_url: str, scraped_at: str) -> dict[str, str]:
        soup = BeautifulSoup(html, "html.parser")
        row = default_row(scraped_at)
        row.update(parse_detail_url(detail_url))
        row.update(
            {
                "source_type": "detail",
                "detail_url": detail_url,
            }
        )

        h1 = soup.find("h1")
        if h1:
            row["title_raw"] = clean_text(h1.get_text(" ", strip=True))

        page_text = soup.get_text("\n", strip=True)
        lines = split_clean_lines(page_text)

        artist_line = next(
            (
                line
                for line in lines
                if line.lower().startswith("door ") or line.lower().startswith("by ")
            ),
            "",
        )
        if artist_line:
            row["artist_raw"] = clean_text(artist_line.split(" ", 1)[1])

        format_label = parse_format_from_lines(lines)
        if format_label:
            row["format_label_raw"] = format_label

        prices = extract_prices(page_text)
        current, old = choose_prices(prices)
        if current:
            row["price_current"] = current
        if old:
            row["price_old"] = old

        availability_text = ""
        for line in lines:
            lower = line.lower()
            if any(token in lower for token in ("voorraad", "leverkans", "levertijd", "in stock", "delivery")):
                availability_text = line
                break
        if availability_text:
            row["availability_text"] = availability_text
            lower = availability_text.lower()
            if "op voorraad" in lower or "in stock" in lower:
                row["availability"] = "in_stock"
            elif "niet op voorraad" in lower or "uitverkocht" in lower or "out of stock" in lower:
                row["availability"] = "out_of_stock"

        label_map = {
            "releasedatum": "release_date_raw",
            "release date": "release_date_raw",
            "import": "import_text",
            "barcode": "ean_raw",
        }
        for idx, line in enumerate(lines):
            key = line.lower()
            if key not in label_map:
                continue
            for next_idx in range(idx + 1, min(idx + 4, len(lines))):
                value = lines[next_idx]
                if value.lower() in label_map:
                    break
                row[label_map[key]] = value
                break

        detail_ean = normalize_ean(row["ean_raw"])
        if detail_ean:
            row["ean_normalized"] = detail_ean
            row["ean_source"] = "detail_barcode"
        elif row["ean_normalized"]:
            row["ean_source"] = row["ean_source"] or "url"

        if not row["display_name_raw"]:
            pieces = [part for part in [row["artist_raw"], row["title_raw"]] if part]
            row["display_name_raw"] = " ".join(pieces)

        return row



def load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows: list[dict[str, str]] = []
        for raw_row in reader:
            row = {column: clean_text(raw_row.get(column, "")) for column in CSV_COLUMNS}
            rows.append(row)
        return rows



def write_csv_rows(path: Path, rows: list[dict[str, str]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in CSV_COLUMNS})



def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")



def merge_rows(existing_rows: list[dict[str, str]], new_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    for row in existing_rows + new_rows:
        key = row.get("detail_url", "") or f"missing::{len(merged)}"
        current = merged.get(key, {column: "" for column in CSV_COLUMNS})
        for column in CSV_COLUMNS:
            value = row.get(column, "")
            if value != "":
                current[column] = value
        merged[key] = current
    result = list(merged.values())
    result.sort(key=lambda item: (item.get("detail_url", ""), item.get("display_name_raw", "")))
    return result



def run_discover(
    scraper: SoundsHaarlemScraper,
    output_csv: Path,
    state_json: Path,
    max_pages: int,
    start_page: int,
    limit: int,
    sort: str,
    merge_existing: bool,
) -> int:
    scraped_at = now_iso()
    rows: list[dict[str, str]] = []
    total_pages_seen: int | None = None
    total_results_seen: int | None = None

    for page in range(start_page, max_pages + 1):
        listing_url = scraper.build_listing_url(page=page, limit=limit, sort=sort)
        log(f"DISCOVERY p{page}: {listing_url}")
        html = scraper.get(listing_url)
        page_rows, stats = scraper.parse_listing_page(html, listing_url, scraped_at)
        if stats.total_pages is not None:
            total_pages_seen = stats.total_pages
        if stats.total_results is not None:
            total_results_seen = stats.total_results
        if not page_rows:
            log(f"DISCOVERY p{page}: geen producten gevonden, stop")
            break
        rows.extend(page_rows)
        log(f"DISCOVERY p{page}: producten={len(page_rows)}")
        if total_pages_seen is not None and page >= total_pages_seen:
            break

    unique_rows = merge_rows([], rows)
    existing_rows = load_csv_rows(output_csv) if merge_existing else []
    final_rows = merge_rows(existing_rows, unique_rows)
    write_csv_rows(output_csv, final_rows)

    missing_ean = sum(1 for row in final_rows if not row.get("ean_normalized"))
    payload = {
        "shop_id": SHOP_ID,
        "mode": "discover",
        "updated_at": scraped_at,
        "pages_requested": max_pages,
        "start_page": start_page,
        "pages_seen": total_pages_seen,
        "total_results_seen": total_results_seen,
        "rows_written": len(final_rows),
        "missing_ean": missing_ean,
        "output_csv": str(output_csv),
    }
    write_json(state_json, payload)
    log(f"DISCOVERY klaar: rows={len(final_rows)} | missing_ean={missing_ean}")
    return 0



def needs_detail(row: dict[str, str]) -> bool:
    if not row.get("detail_url"):
        return False
    if not normalize_ean(row.get("ean_normalized") or row.get("ean_raw")):
        return True
    if not row.get("price_current"):
        return True
    return False



def run_detail_fallback(
    scraper: SoundsHaarlemScraper,
    input_csv: Path,
    output_csv: Path,
    state_json: Path,
    limit_details: int,
    force: bool,
) -> int:
    rows = load_csv_rows(input_csv)
    if not rows:
        raise ScraperError(f"Geen inputbestand of lege input: {input_csv}")

    queue = [row for row in rows if force or needs_detail(row)]
    if limit_details > 0:
        queue = queue[:limit_details]

    log(f"DETAIL fallback queue={len(queue)}")
    scraped_at = now_iso()
    updates: list[dict[str, str]] = []

    for index, base_row in enumerate(queue, start=1):
        detail_url = base_row.get("detail_url", "")
        if not detail_url:
            continue
        log(f"DETAIL ({index}/{len(queue)}): {detail_url}")
        html = scraper.get(detail_url)
        detail_row = scraper.parse_detail_page(html, detail_url, scraped_at)
        detail_row["listing_url"] = base_row.get("listing_url", "")
        for column in CSV_COLUMNS:
            if not detail_row.get(column) and base_row.get(column):
                detail_row[column] = base_row[column]
        if detail_row.get("price_current") and detail_row.get("availability") in {"", "unknown"}:
            detail_row["availability"] = "in_stock"
        if detail_row.get("ean_source") == "url" and detail_row.get("ean_normalized"):
            detail_row["ean_source"] = base_row.get("ean_source", "url") or "url"
        detail_row["source_type"] = "detail"
        updates.append(detail_row)

    final_rows = merge_rows(rows, updates)
    write_csv_rows(output_csv, final_rows)

    payload = {
        "shop_id": SHOP_ID,
        "mode": "detail-fallback",
        "updated_at": scraped_at,
        "input_csv": str(input_csv),
        "output_csv": str(output_csv),
        "detail_queue": len(queue),
        "rows_written": len(final_rows),
        "rows_updated": len(updates),
        "missing_ean": sum(1 for row in final_rows if not row.get("ean_normalized")),
        "unknown_availability": sum(1 for row in final_rows if row.get("availability") in {"", "unknown"}),
    }
    write_json(state_json, payload)
    log(
        "DETAIL fallback klaar: "
        f"rows_updated={len(updates)} | rows_total={len(final_rows)} | "
        f"missing_ean={payload['missing_ean']}"
    )
    return 0



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Listing-first scraper voor Sounds Haarlem LP-catalogus")
    parser.add_argument(
        "--mode",
        choices=["discover", "refresh-known", "detail-fallback", "both"],
        default="discover",
        help="discover = listing crawl, refresh-known = listing crawl met merge, detail-fallback = details voor misses, both = refresh-known + detail-fallback",
    )
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--output-file", default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--input-file", default="")
    parser.add_argument("--max-pages", type=int, default=13)
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--sort", default=DEFAULT_SORT)
    parser.add_argument("--limit-details", type=int, default=25)
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT)
    parser.add_argument("--delay-seconds", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--force-detail", action="store_true")
    return parser



def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    output_dir = ensure_dir(args.output_dir)
    output_csv = output_dir / args.output_file
    state_json = output_dir / args.state_file
    input_csv = Path(args.input_file) if args.input_file else output_csv

    scraper = SoundsHaarlemScraper(timeout=args.timeout, delay_seconds=args.delay_seconds)

    try:
        if args.mode == "discover":
            return run_discover(
                scraper=scraper,
                output_csv=output_csv,
                state_json=state_json,
                max_pages=args.max_pages,
                start_page=args.start_page,
                limit=args.limit,
                sort=args.sort,
                merge_existing=False,
            )
        if args.mode == "refresh-known":
            return run_discover(
                scraper=scraper,
                output_csv=output_csv,
                state_json=state_json,
                max_pages=args.max_pages,
                start_page=args.start_page,
                limit=args.limit,
                sort=args.sort,
                merge_existing=True,
            )
        if args.mode == "detail-fallback":
            return run_detail_fallback(
                scraper=scraper,
                input_csv=input_csv,
                output_csv=output_csv,
                state_json=state_json,
                limit_details=args.limit_details,
                force=args.force_detail,
            )
        if args.mode == "both":
            code = run_discover(
                scraper=scraper,
                output_csv=output_csv,
                state_json=state_json,
                max_pages=args.max_pages,
                start_page=args.start_page,
                limit=args.limit,
                sort=args.sort,
                merge_existing=True,
            )
            if code != 0:
                return code
            return run_detail_fallback(
                scraper=scraper,
                input_csv=output_csv,
                output_csv=output_csv,
                state_json=state_json,
                limit_details=args.limit_details,
                force=args.force_detail,
            )
        raise ScraperError(f"Onbekende mode: {args.mode}")
    except requests.HTTPError as exc:
        log(f"HTTP fout: {exc}")
        return 1
    except requests.RequestException as exc:
        log(f"Netwerkfout: {exc}")
        return 1
    except ScraperError as exc:
        log(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
