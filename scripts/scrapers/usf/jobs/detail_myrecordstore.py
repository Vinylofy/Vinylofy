from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scripts.scrapers.usf.core.db import db_connection, load_env
from scripts.scrapers.usf.core.link_registry import insert_raw_shop_scrape

SHOP_ID = "myrecordstore"
BASE_URL = "https://myrecordstore.nl"
HEADERS = {
    "User-Agent": "Mozilla/5.0 Vinylofy MyRecordStore detail enrichment",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}
EAN_RE = re.compile(r"\b(?:EAN|GTIN|Barcode|UPC)\b\D{0,160}([0-9][0-9\s\-]{7,20}[0-9])", re.I)
BARE_EAN_RE = re.compile(r"(?<!\d)([0-9]{8}|[0-9]{12,14})(?!\d)")

def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()

def normalize_ean(value: str | None) -> str | None:
    digits = re.sub(r"\D+", "", value or "")
    return digits if len(digits) in {8, 12, 13, 14} else None

def extract_jsonld_ean(soup: BeautifulSoup) -> str | None:
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        stack = data if isinstance(data, list) else [data]
        while stack:
            item = stack.pop(0)
            if isinstance(item, list):
                stack.extend(item)
            elif isinstance(item, dict):
                for key in ("gtin13", "gtin14", "gtin12", "gtin", "sku", "mpn", "productID"):
                    ean = normalize_ean(str(item.get(key) or ""))
                    if ean:
                        return ean
                stack.extend(v for v in item.values() if isinstance(v, (dict, list)))
    return None

def extract_ean(soup: BeautifulSoup, text: str) -> str | None:
    ean = extract_jsonld_ean(soup)
    if ean:
        return ean
    for meta in soup.select("meta[content]"):
        ean = normalize_ean(str(meta.get("content") or ""))
        if ean:
            name = clean(meta.get("name") or meta.get("property")).lower()
            if any(k in name for k in ("ean", "gtin", "barcode", "upc", "sku")):
                return ean
    for match in EAN_RE.finditer(text):
        ean = normalize_ean(match.group(1))
        if ean:
            return ean
    low = text.lower()
    for marker in ("ean", "gtin", "barcode", "specificaties", "productinformatie"):
        idx = low.find(marker)
        if idx >= 0:
            window = text[idx:idx + 2000]
            for candidate in BARE_EAN_RE.findall(window):
                ean = normalize_ean(candidate)
                if ean:
                    return ean
    return None

def extract_image_url(soup: BeautifulSoup, fallback: str | None) -> str | None:
    for sel in ('meta[property="og:image"][content]', 'meta[name="twitter:image"][content]'):
        node = soup.select_one(sel)
        if node and node.get("content"):
            return urljoin(BASE_URL, str(node.get("content")))
    img = soup.select_one("main img[src], article img[src], img[src]")
    if img and img.get("src"):
        return urljoin(BASE_URL, str(img.get("src")))
    return fallback

def get_candidates(conn, *, limit: int, rescrape_days: int):
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select id, source_url, source_product_id, payload, last_detail_scraped_at
            from public.shop_product_links
            where shop_id = %s
              and status = 'active'
              and coalesce(payload->>'ean_enrichment_status', '') <> 'found'
              and (
                last_detail_scraped_at is null
                or last_detail_scraped_at < now() - (%s * interval '1 day')
              )
            order by last_detail_scraped_at nulls first, last_seen_at desc
            limit %s
            """,
            (SHOP_ID, rescrape_days, limit),
        )
        return [dict(row) for row in cur.fetchall()]

def update_link(conn, *, link_id: str, payload: dict[str, Any], ean: str | None, detail_payload: dict[str, Any], rescrape_days: int, write: bool):
    now = datetime.now(timezone.utc)
    miss_count = int(payload.get("ean_content_miss_count") or 0)
    enrichment = {
        "ean_last_attempt_at": now.isoformat(),
        "detail_seen_at": now.isoformat(),
        "detail_payload": detail_payload,
    }
    if ean:
        enrichment.update({"ean_enrichment_status": "found", "ean": ean, "ean_raw": ean, "ean_next_attempt_at": None})
    else:
        enrichment.update({
            "ean_enrichment_status": "missing",
            "ean": None,
            "ean_raw": None,
            "ean_content_miss_count": miss_count + 1,
            "ean_next_attempt_at": (now + timedelta(days=rescrape_days)).isoformat(),
            "ean_revisit_after_days": rescrape_days,
        })
    if not write:
        print("[MYRECORDSTORE-DETAIL] dry_run_link_enrichment", {"link_id": link_id, "ean": ean}, flush=True)
        return
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.shop_product_links
            set payload = coalesce(payload, '{}'::jsonb) || %s,
                last_detail_scraped_at = now()
            where id = %s
            """,
            (Jsonb(enrichment), link_id),
        )

def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich My Record Store details for EAN/image/metadata.")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--sleep", type=float, default=1.0)
    parser.add_argument("--rescrape-days", type=int, default=14)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    load_env()
    session = requests.Session()
    session.headers.update(HEADERS)

    stats = {"candidates": 0, "processed": 0, "found_ean": 0, "missing_ean": 0, "http_429": 0, "errors": 0}
    with db_connection() as conn:
        rows = get_candidates(conn, limit=args.limit, rescrape_days=args.rescrape_days)
        stats["candidates"] = len(rows)

        for idx, row in enumerate(rows, start=1):
            payload = row.get("payload") or {}
            if not isinstance(payload, dict):
                payload = {}
            url = row["source_url"]
            print("[MYRECORDSTORE-DETAIL] fetch", {"index": idx, "url": url}, flush=True)
            try:
                response = session.get(url, timeout=45)
                if response.status_code == 429:
                    stats["http_429"] += 1
                    print("[MYRECORDSTORE-DETAIL-WARN] HTTP 429; stop batch", {"url": url}, flush=True)
                    break
                if response.status_code >= 400:
                    stats["errors"] += 1
                    print("[MYRECORDSTORE-DETAIL-WARN] HTTP error", {"url": url, "status": response.status_code}, flush=True)
                    continue

                soup = BeautifulSoup(response.text, "html.parser")
                text = clean(soup.get_text(" ", strip=True))
                ean = extract_ean(soup, text)
                image_url = extract_image_url(soup, payload.get("image_url"))
                title = clean(soup.select_one("h1").get_text(" ", strip=True)) if soup.select_one("h1") else payload.get("title")

                detail_payload = {
                    "title": title,
                    "ean": ean,
                    "image_url": image_url,
                    "price": payload.get("price"),
                    "price_source": "listing",
                    "availability": payload.get("availability"),
                    "listing_payload": payload,
                }

                insert_raw_shop_scrape(
                    run_id=None,
                    shop_id=SHOP_ID,
                    source_url=url,
                    source_product_id=row.get("source_product_id"),
                    title_raw=title,
                    ean_raw=ean,
                    price_raw=payload.get("price"),
                    availability_raw=payload.get("availability"),
                    image_url_raw=image_url,
                    payload=detail_payload,
                )

                update_link(conn, link_id=str(row["id"]), payload=payload, ean=ean, detail_payload=detail_payload, rescrape_days=args.rescrape_days, write=args.write)
                stats["processed"] += 1
                stats["found_ean" if ean else "missing_ean"] += 1
                print("[MYRECORDSTORE-DETAIL] parsed", {"url": url, "ean_found": bool(ean), "price_source": "listing"}, flush=True)
                time.sleep(args.sleep)
            except Exception as exc:
                stats["errors"] += 1
                print("[MYRECORDSTORE-DETAIL-WARN] failed", {"url": url, "error": str(exc)}, flush=True)

    print("[MYRECORDSTORE-DETAIL-SUMMARY]", stats, flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
