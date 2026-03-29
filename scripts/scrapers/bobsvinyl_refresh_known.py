#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

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
    collect_product_text,
    detect_second_hand,
    extract_ean_from_soup,
    fetch_soup,
    get_thread_session,
    load_csv_as_dict,
    merge_row,
    nl_price,
    normalize_text,
    now_iso,
    product_handle_from_url,
    split_artist_title_drager,
    validate_product_page,
    write_csv,
)

DEFAULT_OUTPUT_DIR = "data/raw/bobsvinyl"
OUTPUT_FILE = "bobsvinyl_step2_enriched.csv"
SUMMARY_FILE = "output/bobsvinyl_refresh_known_summary.json"
REFRESH_COLUMNS = list(LEGACY_STEP2_COLUMNS) + ["availability"]
PRICE_PATTERN = re.compile(r"€\s*([0-9]+(?:[\.,][0-9]{2})?)")
LOCK = threading.Lock()


@dataclass
class SeedRow:
    row: dict[str, str]
    last_seen_at: datetime | None



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
                        "bron_collectie": "known-url-refresh",
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



def select_stale_rows(seed_rows: dict[str, SeedRow], stale_hours: float, limit_urls: int | None) -> OrderedDict[str, SeedRow]:
    now = utc_now()
    threshold = None if stale_hours <= 0 else now - timedelta(hours=stale_hours)
    stale: list[tuple[str, SeedRow]] = []
    fresh: list[tuple[str, SeedRow]] = []

    for url, seed in seed_rows.items():
        last_seen = seed.last_seen_at
        if threshold is None or last_seen is None or last_seen < threshold:
            stale.append((url, seed))
        else:
            fresh.append((url, seed))

    stale.sort(key=lambda item: (item[1].last_seen_at or datetime(1970, 1, 1, tzinfo=UTC), item[0]))
    if limit_urls is not None and limit_urls > 0:
        stale = stale[:limit_urls]

    selected = OrderedDict(stale)
    for url, seed in fresh:
        selected.setdefault(url, seed)
    return selected



def extract_price_from_detail_page(soup) -> str:
    candidate_texts: list[str] = []
    selectors = [
        ".price__container",
        ".price.price--large",
        ".product__info-container",
    ]
    for selector in selectors:
        for node in soup.select(selector):
            candidate_texts.append(normalize_text(node.get_text(" ", strip=True)))

    candidate_texts.append(collect_product_text(soup))

    for text in candidate_texts:
        if not text:
            continue
        matches = PRICE_PATTERN.findall(text)
        if matches:
            return nl_price(matches[0])
    return ""



def detect_availability(soup) -> str:
    page_text = normalize_text(soup.get_text(" ", strip=True)).lower()
    if "sold out" in page_text or "uitverkocht" in page_text:
        return "out_of_stock"

    button_texts = [normalize_text(node.get_text(" ", strip=True)).lower() for node in soup.select("form[action*='/cart/add'] button")]
    if any("sold out" in text or "uitverkocht" in text for text in button_texts):
        return "out_of_stock"
    if any("add to cart" in text or "in winkelwagen" in text for text in button_texts):
        return "in_stock"
    return "unknown"



def refresh_one(seed: SeedRow) -> dict[str, str]:
    base_row = {column: normalize_text(seed.row.get(column, "")) for column in REFRESH_COLUMNS}
    url = canonical_product_url(base_row.get("url", ""))
    base_row["url"] = url
    base_row["url_listing"] = base_row.get("url_listing") or url
    base_row["product_handle"] = base_row.get("product_handle") or product_handle_from_url(url)
    base_row["bron_collectie"] = base_row.get("bron_collectie") or "known-url-refresh"

    result = dict(base_row)
    result["detail_checked_at"] = now_iso()
    result.setdefault("mogelijk_2e_hands", "NEE")

    try:
        soup = fetch_soup(get_thread_session(), url)
    except Exception as exc:
        result["detail_status"] = "fout_bij_refresh_fetch"
        result["detail_opmerking"] = str(exc)
        result["_refresh_result"] = "fetch_error"
        return result

    valid_page, validation_note = validate_product_page(soup)
    if not valid_page:
        result["detail_status"] = "refresh_ongeldige_pagina"
        result["detail_opmerking"] = validation_note
        result["_refresh_result"] = "invalid_page"
        return result

    title_node = soup.select_one(".product__title h1")
    raw_title = normalize_text(title_node.get_text(" ", strip=True) if title_node else "")
    artist, title, drager = split_artist_title_drager(raw_title)
    if artist:
        result["artist"] = artist
    if title:
        result["title"] = title
    elif raw_title and not result.get("title"):
        result["title"] = raw_title
    if drager:
        result["drager"] = drager

    product_text = collect_product_text(soup)
    fetched_ean = extract_ean_from_soup(soup, product_text)
    existing_ean = normalize_text(base_row.get("ean"))
    final_ean = fetched_ean or existing_ean
    result["ean"] = final_ean

    second_hand = detect_second_hand(soup, product_text) or "NEE"
    result["mogelijk_2e_hands"] = second_hand
    result["availability"] = detect_availability(soup)

    refreshed_price = extract_price_from_detail_page(soup)
    if refreshed_price:
        result["prijs"] = refreshed_price

    if second_hand == "JA":
        result["detail_status"] = "2e_hands_bevestigd"
        result["detail_opmerking"] = "2e hands-vermelding gevonden tijdens known-url refresh"
        result["_refresh_result"] = "second_hand"
        return result

    if not result.get("prijs"):
        result["detail_status"] = "refresh_zonder_prijs"
        result["detail_opmerking"] = "Valide productpagina, maar geen prijs gevonden"
        result["_refresh_result"] = "missing_price"
        return result

    if final_ean:
        result["detail_status"] = "ok"
        if fetched_ean:
            result["detail_opmerking"] = ""
            result["_refresh_result"] = "ok_fetched_ean"
        else:
            result["detail_opmerking"] = "EAN behouden uit bestaand record tijdens known-url refresh"
            result["_refresh_result"] = "ok_preserved_ean"
        return result

    result["detail_status"] = "refresh_geen_ean"
    result["detail_opmerking"] = "Valide productpagina, prijs gevonden, maar geen EAN beschikbaar"
    result["_refresh_result"] = "missing_ean"
    return result



def build_summary(rows_by_url: dict[str, dict[str, str]], selected_count: int, total_seed_rows: int, started_at: datetime) -> dict[str, object]:
    counters = {
        "total_seed_rows": total_seed_rows,
        "selected_for_refresh": selected_count,
        "ok_fetched_ean": 0,
        "ok_preserved_ean": 0,
        "second_hand": 0,
        "missing_price": 0,
        "missing_ean": 0,
        "invalid_page": 0,
        "fetch_error": 0,
        "out_of_stock": 0,
        "in_stock": 0,
        "unknown_availability": 0,
    }

    for row in rows_by_url.values():
        refresh_result = normalize_text(row.get("_refresh_result"))
        if refresh_result in counters:
            counters[refresh_result] += 1
        availability = normalize_text(row.get("availability"))
        if availability == "out_of_stock":
            counters["out_of_stock"] += 1
        elif availability == "in_stock":
            counters["in_stock"] += 1
        elif availability:
            counters["unknown_availability"] += 1

    return {
        "started_at": started_at.isoformat(),
        "finished_at": utc_now().isoformat(),
        **counters,
    }



def write_summary(path: Path, summary: dict[str, object]) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")



def run_refresh_known(
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    workers: int = 6,
    stale_hours: float = 20.0,
    limit_urls: int | None = None,
) -> int:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / OUTPUT_FILE
    summary_path = Path(SUMMARY_FILE)

    csv_seed_rows = load_seed_rows_from_csv([output_dir / OUTPUT_FILE, output_dir / "bobsvinyl_step1.csv"])
    db_seed_rows = load_seed_rows_from_db()
    seed_rows = combine_seed_rows(csv_seed_rows, db_seed_rows)

    if not seed_rows:
        print("[INFO] Geen Bob known URLs gevonden in DB of lokale CSV-bestanden.", flush=True)
        write_summary(summary_path, {
            "started_at": utc_now().isoformat(),
            "finished_at": utc_now().isoformat(),
            "total_seed_rows": 0,
            "selected_for_refresh": 0,
        })
        return 0

    selected = select_stale_rows(seed_rows, stale_hours=stale_hours, limit_urls=limit_urls)
    selected_count = sum(1 for _, seed in selected.items() if (stale_hours <= 0) or (seed.last_seen_at is None) or (seed.last_seen_at < utc_now() - timedelta(hours=stale_hours)))

    print(
        f"[INFO] Bob refresh-known gestart | seed_rows={len(seed_rows)} | stale_hours={stale_hours} | "
        f"te_verversen={selected_count} | workers={workers} | limit_urls={limit_urls}",
        flush=True,
    )

    rows_by_url: dict[str, dict[str, str]] = OrderedDict()
    for url, seed in selected.items():
        row = {column: normalize_text(seed.row.get(column, "")) for column in REFRESH_COLUMNS}
        row["url"] = canonical_product_url(url)
        row.setdefault("product_handle", product_handle_from_url(url))
        row.setdefault("availability", normalize_text(row.get("availability")) or "unknown")
        rows_by_url[url] = row

    stale_urls = []
    threshold = None if stale_hours <= 0 else utc_now() - timedelta(hours=stale_hours)
    for url, seed in selected.items():
        if threshold is None or seed.last_seen_at is None or seed.last_seen_at < threshold:
            stale_urls.append(url)

    started_at = utc_now()

    if stale_urls:
        processed = 0
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {executor.submit(refresh_one, selected[url]): url for url in stale_urls}
            for future in as_completed(futures):
                url = futures[future]
                processed += 1
                try:
                    refreshed = future.result()
                except Exception as exc:  # pragma: no cover
                    refreshed = dict(rows_by_url[url])
                    refreshed["detail_checked_at"] = now_iso()
                    refreshed["detail_status"] = "fout_bij_refresh_worker"
                    refreshed["detail_opmerking"] = str(exc)
                    refreshed["_refresh_result"] = "fetch_error"
                with LOCK:
                    rows_by_url[url] = merge_row(rows_by_url.get(url), refreshed, REFRESH_COLUMNS + ["_refresh_result"])
                    if processed % 50 == 0 or processed == len(stale_urls):
                        csv_rows = OrderedDict((k, {col: normalize_text(v.get(col, "")) for col in REFRESH_COLUMNS}) for k, v in rows_by_url.items())
                        write_csv(str(output_path), csv_rows, REFRESH_COLUMNS)
                print(
                    f"[REFRESH] {processed}/{len(stale_urls)} | {refreshed.get('artist', '')} - {refreshed.get('title', '')} | "
                    f"prijs={refreshed.get('prijs', '-') or '-'} | ean={refreshed.get('ean', '-') or '-'} | "
                    f"status={refreshed.get('detail_status', '-') or '-'} | availability={refreshed.get('availability', '-') or '-'}",
                    flush=True,
                )
                time.sleep(0.05)

    csv_rows = OrderedDict((k, {col: normalize_text(v.get(col, "")) for col in REFRESH_COLUMNS}) for k, v in rows_by_url.items())
    write_csv(str(output_path), csv_rows, REFRESH_COLUMNS)
    summary = build_summary(rows_by_url, selected_count=selected_count, total_seed_rows=len(seed_rows), started_at=started_at)
    write_summary(summary_path, summary)
    print(f"[KLAAR] {output_path} opgeslagen met {len(rows_by_url)} Bob known URLs.", flush=True)
    print(f"[SUMMARY] {summary_path}", flush=True)
    return 0



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bob's Vinyl known-url refresher")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--stale-hours", type=float, default=20.0)
    parser.add_argument("--limit-urls", type=int, default=None)
    return parser



def main() -> int:
    args = build_parser().parse_args()
    return run_refresh_known(
        output_dir=args.output_dir,
        workers=max(1, args.workers),
        stale_hours=args.stale_hours,
        limit_urls=args.limit_urls,
    )


if __name__ == "__main__":
    raise SystemExit(main())
