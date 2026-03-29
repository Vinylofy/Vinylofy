#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode, urljoin, urlparse, parse_qsl

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None

CURRENT = Path(__file__).resolve().parent
if str(CURRENT) not in sys.path:
    sys.path.insert(0, str(CURRENT))

from legacy.bobsvinyl_legacy import (  # noqa: E402
    STEP2_COLUMNS as LEGACY_STEP2_COLUMNS,
    canonical_product_url,
    fetch_soup,
    load_csv_as_dict,
    merge_row,
    nl_price,
    normalize_text,
    now_iso,
    product_handle_from_url,
    split_artist_title_drager,
    write_csv,
)

DEFAULT_OUTPUT_DIR = "data/raw/bobsvinyl"
OUTPUT_FILE = "bobsvinyl_step2_enriched.csv"
SUMMARY_FILE = "output/bobsvinyl_refresh_known_summary.json"
REFRESH_COLUMNS = list(LEGACY_STEP2_COLUMNS) + ["availability"]
PRICE_PATTERN = re.compile(r"€\s*([0-9]+(?:[\.,][0-9]{2})?)")
BASE_COLLECTION_URL = "https://bobsvinyl.nl/en/collections/all"
LOCK = threading.Lock()


@dataclass
class SeedRow:
    row: dict[str, str]
    last_seen_at: datetime | None


@dataclass
class ListingRow:
    url: str
    title: str
    price: str
    availability: str
    source_page: str



def utc_now() -> datetime:
    return datetime.now(UTC)



def parse_dt(value: str | None) -> datetime | None:
    value = normalize_text(value)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)



def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)



def with_page_param(base_url: str, page: int) -> str:
    parts = urlparse(base_url)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query["page"] = str(page)
    return parts._replace(query=urlencode(query)).geturl()



def load_seed_rows_from_csv(paths: Iterable[Path]) -> dict[str, SeedRow]:
    rows: dict[str, SeedRow] = OrderedDict()
    for path in paths:
        if not path.exists():
            continue
        loaded = load_csv_as_dict(str(path), REFRESH_COLUMNS)
        for url, row in loaded.items():
            canonical = canonical_product_url(url)
            row = {column: normalize_text(row.get(column, "")) for column in REFRESH_COLUMNS}
            row["url"] = canonical
            row.setdefault("product_handle", product_handle_from_url(canonical))
            rows[canonical] = SeedRow(row=row, last_seen_at=parse_dt(row.get("detail_checked_at")))
    return rows



def load_seed_rows_from_db() -> dict[str, SeedRow]:
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        return OrderedDict()
    if psycopg is None:
        raise RuntimeError("psycopg is niet geïnstalleerd maar DATABASE_URL is wel gezet")

    sql = """
    with ranked as (
      select distinct on (p.product_url)
        p.product_url,
        p.price,
        p.currency,
        p.availability,
        p.last_seen_at,
        pr.ean,
        pr.artist,
        pr.title,
        pr.format_label,
        s.domain,
        s.name
      from public.prices p
      join public.products pr on pr.id = p.product_id
      join public.shops s on s.id = p.shop_id
      where lower(coalesce(s.domain, '')) = 'bobsvinyl.nl'
        and p.product_url is not null
      order by p.product_url, p.last_seen_at desc nulls last, p.updated_at desc nulls last, p.created_at desc nulls last
    )
    select
      product_url,
      price,
      currency,
      availability,
      last_seen_at,
      ean,
      artist,
      title,
      format_label
    from ranked
    order by last_seen_at asc nulls first, product_url asc
    """

    rows: dict[str, SeedRow] = OrderedDict()
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            for product_url, price, currency, availability, last_seen_at, ean, artist, title, format_label in cur.fetchall():
                url = canonical_product_url(str(product_url))
                row = {column: "" for column in REFRESH_COLUMNS}
                row.update(
                    {
                        "url": url,
                        "url_listing": url,
                        "product_handle": product_handle_from_url(url),
                        "artist": normalize_text(artist),
                        "title": normalize_text(title),
                        "drager": normalize_text(format_label),
                        "prijs": "" if price is None else nl_price(str(price)),
                        "bron_collectie": "known-listing-refresh",
                        "bron_listing_urls": "",
                        "ean": normalize_text(ean),
                        "mogelijk_2e_hands": "NEE",
                        "detail_status": "ok" if normalize_text(ean) else "",
                        "detail_opmerking": "",
                        "detail_checked_at": "",
                        "availability": normalize_text(availability) or "unknown",
                    }
                )
                rows[url] = SeedRow(row=row, last_seen_at=parse_dt(last_seen_at.isoformat() if last_seen_at else None))
    return rows



def combine_seed_rows(csv_rows: dict[str, SeedRow], db_rows: dict[str, SeedRow]) -> dict[str, SeedRow]:
    combined: dict[str, SeedRow] = OrderedDict()
    for source in (csv_rows, db_rows):
        for url, seed in source.items():
            if url not in combined:
                combined[url] = seed
                continue
            merged = merge_row(combined[url].row, seed.row, REFRESH_COLUMNS)
            last_seen = combined[url].last_seen_at
            if seed.last_seen_at and (last_seen is None or seed.last_seen_at > last_seen):
                last_seen = seed.last_seen_at
            combined[url] = SeedRow(row=merged, last_seen_at=last_seen)
    return combined



def select_target_rows(seed_rows: dict[str, SeedRow], stale_hours: float, limit_urls: int | None) -> OrderedDict[str, SeedRow]:
    now = utc_now()
    threshold = None if stale_hours <= 0 else now - timedelta(hours=stale_hours)
    selected: list[tuple[str, SeedRow]] = []
    for url, seed in seed_rows.items():
        last_seen = seed.last_seen_at
        if threshold is None or last_seen is None or last_seen < threshold:
            selected.append((url, seed))
    selected.sort(key=lambda item: (item[1].last_seen_at or datetime(1970, 1, 1, tzinfo=UTC), item[0]))
    if limit_urls is not None and limit_urls > 0:
        selected = selected[:limit_urls]
    return OrderedDict(selected)



def normalize_page_title(value: str) -> str:
    value = normalize_text(value)
    value = re.sub(r"\s+", " ", value)
    return value



def extract_price(text: str) -> str:
    matches = PRICE_PATTERN.findall(text)
    if not matches:
        return ""
    return f"€{matches[-1].replace('.', ',')}"



def classify_availability(text: str) -> str:
    lowered = normalize_text(text).lower()
    if "pre-order" in lowered or "preorder" in lowered:
        return "preorder"
    if "sold out" in lowered or "uitverkocht" in lowered:
        return "out_of_stock"
    if "add to cart" in lowered or "toevoegen aan winkelwagen" in lowered:
        return "in_stock"
    return "unknown"



def iter_candidate_containers(soup):
    selectors = [
        "li.grid__item",
        ".grid__item",
        ".card-wrapper",
        ".card",
        ".product-card-wrapper",
        ".product-grid-item",
    ]
    seen = set()
    for selector in selectors:
        for node in soup.select(selector):
            identifier = id(node)
            if identifier in seen:
                continue
            seen.add(identifier)
            yield node



def extract_listing_rows_from_page(soup, page_url: str) -> list[ListingRow]:
    rows: OrderedDict[str, ListingRow] = OrderedDict()

    def add_candidate(url: str, title: str, raw_text: str) -> None:
        canonical = canonical_product_url(url)
        if not canonical:
            return
        if "/products/" not in canonical:
            return
        price = extract_price(raw_text)
        if not price:
            return
        rows[canonical] = ListingRow(
            url=canonical,
            title=normalize_page_title(title),
            price=price,
            availability=classify_availability(raw_text),
            source_page=page_url,
        )

    for container in iter_candidate_containers(soup):
        anchor = container.select_one('a[href*="/products/"]')
        if not anchor:
            continue
        href = normalize_text(anchor.get("href"))
        if not href:
            continue
        url = urljoin(page_url, href)
        title = anchor.get_text(" ", strip=True)
        container_text = container.get_text(" ", strip=True)
        add_candidate(url, title, container_text)

    if rows:
        return list(rows.values())

    for anchor in soup.select('a[href*="/products/"]'):
        href = normalize_text(anchor.get("href"))
        if not href:
            continue
        url = urljoin(page_url, href)
        parent = anchor.parent if anchor.parent is not None else anchor
        raw_text = parent.get_text(" ", strip=True)
        title = anchor.get_text(" ", strip=True)
        add_candidate(url, title, raw_text)

    return list(rows.values())



def detect_total_pages(soup) -> int | None:
    candidates: list[int] = []
    for anchor in soup.select('a[href*="page="]'):
        href = normalize_text(anchor.get("href"))
        text = normalize_text(anchor.get_text(" ", strip=True))
        if text.isdigit():
            try:
                candidates.append(int(text))
            except ValueError:
                pass
        if href:
            parsed = urlparse(urljoin(BASE_COLLECTION_URL, href))
            query = dict(parse_qsl(parsed.query, keep_blank_values=True))
            page_value = normalize_text(query.get("page"))
            if page_value.isdigit():
                candidates.append(int(page_value))
    return max(candidates) if candidates else None



def split_title_listing(title: str, fallback_artist: str, fallback_title: str, fallback_drager: str) -> tuple[str, str, str]:
    artist, album, drager = split_artist_title_drager(title)
    artist = artist or fallback_artist
    album = album or fallback_title
    drager = drager or fallback_drager
    return normalize_text(artist), normalize_text(album), normalize_text(drager)



def refresh_rows_via_collection(
    targets: OrderedDict[str, SeedRow],
    workers: int,
    max_pages: int | None,
    delay_seconds: float,
) -> tuple[list[dict[str, str]], dict[str, object]]:
    known_urls = set(targets.keys())
    refreshed: OrderedDict[str, dict[str, str]] = OrderedDict()
    pages_crawled = 0
    matched_total = 0
    total_pages_detected: int | None = None
    empty_pages_in_row = 0
    page = 1
    max_pages_effective = max_pages if (max_pages is not None and max_pages > 0) else None

    while True:
        if max_pages_effective is not None and page > max_pages_effective:
            break
        if len(refreshed) >= len(known_urls):
            break

        page_url = with_page_param(BASE_COLLECTION_URL, page)
        soup = fetch_soup(page_url)
        pages_crawled += 1

        if page == 1:
            total_pages_detected = detect_total_pages(soup)
            if max_pages_effective is None and total_pages_detected:
                max_pages_effective = total_pages_detected

        listings = extract_listing_rows_from_page(soup, page_url)
        if not listings:
            empty_pages_in_row += 1
            print(f"[REFRESH all p{page}] geen listings gevonden")
            if empty_pages_in_row >= 2:
                break
            page += 1
            if delay_seconds > 0:
                time.sleep(delay_seconds)
            continue

        empty_pages_in_row = 0
        page_matches = 0
        for listing in listings:
            if listing.url not in known_urls:
                continue
            if listing.url in refreshed:
                continue
            seed = targets[listing.url]
            base_row = {column: normalize_text(seed.row.get(column, "")) for column in REFRESH_COLUMNS}
            artist, album, drager = split_title_listing(
                listing.title,
                fallback_artist=base_row.get("artist", ""),
                fallback_title=base_row.get("title", ""),
                fallback_drager=base_row.get("drager", ""),
            )
            row = dict(base_row)
            row.update(
                {
                    "url": listing.url,
                    "url_listing": listing.url,
                    "product_handle": product_handle_from_url(listing.url),
                    "artist": artist,
                    "title": album,
                    "drager": drager,
                    "prijs": listing.price,
                    "bron_collectie": "collections-all-refresh",
                    "bron_listing_urls": listing.source_page,
                    "detail_checked_at": now_iso(),
                    "detail_opmerking": "listing refresh via /collections/all",
                    "availability": listing.availability,
                }
            )
            if not row.get("detail_status") and row.get("ean"):
                row["detail_status"] = "ok"
            refreshed[listing.url] = row
            page_matches += 1
            matched_total += 1

        print(
            f"[REFRESH all p{page}] listings={len(listings)} | matched_known={page_matches} | refreshed_total={len(refreshed)}/{len(known_urls)}"
        )

        if max_pages_effective is not None and page >= max_pages_effective:
            break
        page += 1
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    summary = {
        "collection_url": BASE_COLLECTION_URL,
        "pages_crawled": pages_crawled,
        "total_pages_detected": total_pages_detected,
        "selected_known_urls": len(known_urls),
        "matched_known_urls": len(refreshed),
        "unmatched_known_urls": max(0, len(known_urls) - len(refreshed)),
        "max_pages": 0 if max_pages is None else max_pages,
        "delay_seconds": delay_seconds,
    }
    return list(refreshed.values()), summary



def run_refresh_known(
    output_dir: str = DEFAULT_OUTPUT_DIR,
    workers: int = 1,
    stale_hours: float = 20.0,
    limit_urls: int | None = None,
    max_pages: int | None = None,
    delay_seconds: float = 0.25,
) -> int:
    output_dir_path = Path(output_dir)
    ensure_parent(output_dir_path / OUTPUT_FILE)
    ensure_parent(Path(SUMMARY_FILE))

    csv_seed_paths = [
        output_dir_path / "bobsvinyl_step2_enriched.csv",
        output_dir_path / "bobsvinyl_step1.csv",
        output_dir_path / "bobsvinyl_products.csv",
    ]

    csv_rows = load_seed_rows_from_csv(csv_seed_paths)
    db_rows = load_seed_rows_from_db()
    combined = combine_seed_rows(csv_rows, db_rows)
    targets = select_target_rows(combined, stale_hours=stale_hours, limit_urls=limit_urls)

    if not targets:
        summary = {
            "status": "no_targets",
            "selected_known_urls": 0,
            "stale_hours": stale_hours,
            "limit_urls": limit_urls,
            "max_pages": max_pages,
            "generated_at": now_iso(),
        }
        Path(SUMMARY_FILE).write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(summary, indent=2, ensure_ascii=False))
        return 0

    refreshed_rows, crawl_summary = refresh_rows_via_collection(
        targets=targets,
        workers=workers,
        max_pages=max_pages,
        delay_seconds=delay_seconds,
    )

    output_path = output_dir_path / OUTPUT_FILE
    write_csv(str(output_path), refreshed_rows, REFRESH_COLUMNS)

    summary = {
        "status": "ok",
        "generated_at": now_iso(),
        "workers": workers,
        "stale_hours": stale_hours,
        "limit_urls": limit_urls,
        "max_pages": max_pages,
        "output_file": str(output_path),
        **crawl_summary,
    }
    Path(SUMMARY_FILE).write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(run_refresh_known())
