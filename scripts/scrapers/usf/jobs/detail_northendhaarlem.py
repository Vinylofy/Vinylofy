from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, Tag
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scripts.scrapers.usf.core.db import db_connection, load_env
from scripts.scrapers.usf.core.link_registry import insert_raw_shop_scrape

SHOP_ID = "northendhaarlem"
SHOP_DOMAIN = "northendhaarlem.nl"
BASE_URL = "https://www.northendhaarlem.nl"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.7,en;q=0.6",
}

BARCODE_RE = re.compile(r"\b(?:EAN|Barcode|UPC)\b\D{0,120}([0-9][0-9\s\-]{7,20}[0-9])", flags=re.I)
BARE_EAN_RE = re.compile(r"(?<!\d)([0-9]{8}|[0-9]{12,14})(?!\d)")
PRICE_RE = re.compile(r"€\s*([0-9]{1,5})\s*[,.]\s*(?:\^\{?)?([0-9]{2})(?:\})?", flags=re.I)
FORMAT_RE = re.compile(
    r"\b(?:LP|2LP|3LP|4LP|5LP|7INCH|7\s?INCH|10\s?INCH|12\s?INCH|SINGLE|VINYL|BOX\s?SET)\b",
    flags=re.I,
)


def clean(text: str | None) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def decimal_price(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value).replace(",", ".")).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def normalize_ean(value: str | None) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D+", "", value)
    if len(digits) in {8, 12, 13, 14}:
        return digits
    return None


def normalize_availability(text: str | None) -> str | None:
    value = clean(text).lower()
    if not value:
        return None
    if "uitverkocht" in value or "niet op voorraad" in value:
        return "out_of_stock"
    if "op voorraad" in value or re.search(r"\bvoorraad\s+[0-9]+\s+stuk", value):
        return "in_stock"
    if (
        "op bestelling" in value
        or "levertijd" in value
        or "pre-order" in value
        or "preorder" in value
        or "wordt verwacht" in value
        or "awaiting repress" in value
        or "awaiting re-press" in value
    ):
        return "preorder"
    return "unknown"


def infer_format(title: str | None, text: str | None = None) -> str | None:
    haystack = " ".join(part for part in [title or "", text or ""] if part)
    match = FORMAT_RE.search(haystack)
    return clean(match.group(0)).upper().replace(" ", "") if match else None


def extract_prices(text: str) -> tuple[str | None, str | None, bool]:
    prices: list[str] = []
    for euros, cents in PRICE_RE.findall(text or ""):
        price = f"{int(euros)}.{cents}"
        if price not in prices:
            prices.append(price)
    if not prices:
        return None, None, False
    current_price = prices[-1]
    original_price = prices[0] if len(prices) >= 2 and prices[0] != current_price else None
    return current_price, original_price, bool(original_price)


def main_product_text(soup: BeautifulSoup) -> str:
    text = clean(soup.get_text(" ", strip=True))
    for marker in ("Klanten kochten ook", "Gerelateerde producten", "Meer", "Menu"):
        idx = text.lower().find(marker.lower())
        if idx > 0:
            return text[:idx]
    return text


def extract_ean(text: str) -> str | None:
    for match in BARCODE_RE.finditer(text or ""):
        ean = normalize_ean(match.group(1))
        if ean:
            return ean
    # Conservative fallback: only consider bare EANs in a likely specification area.
    lowered = text.lower()
    for marker in ("specificatie", "omschrijving", "barcode", "ean", "upc"):
        idx = lowered.find(marker)
        if idx >= 0:
            window = text[idx : idx + 1500]
            for candidate in BARE_EAN_RE.findall(window):
                ean = normalize_ean(candidate)
                if ean:
                    return ean
    return None


def extract_image_url(soup: BeautifulSoup) -> str | None:
    meta = soup.select_one('meta[property="og:image"][content], meta[name="twitter:image"][content]')
    if meta and meta.get("content"):
        return urljoin(BASE_URL, str(meta.get("content")))
    image = soup.select_one("main img[src], article img[src], img[src*='cdn.myonlinestore'], img[src]")
    if isinstance(image, Tag) and image.get("src"):
        return urljoin(BASE_URL, str(image.get("src")))
    return None


def extract_title(soup: BeautifulSoup, fallback: str | None) -> str | None:
    heading = soup.select_one("h1")
    if heading:
        title = clean(heading.get_text(" ", strip=True))
        if title:
            return title
    return clean(fallback) or None


def get_detail_candidates(conn, *, limit: int, rescrape_days: int) -> list[dict[str, Any]]:
    sql = """
        select
            id,
            shop_id,
            source_url,
            source_product_id,
            payload,
            last_detail_scraped_at
        from shop_product_links
        where shop_id = %s
          and status = 'active'
          and coalesce(payload->>'ean_enrichment_status', '') <> 'found'
          and (
                last_detail_scraped_at is null
                or last_detail_scraped_at < now() - (%s * interval '1 day')
              )
        order by last_detail_scraped_at nulls first, last_seen_at desc
        limit %s
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (SHOP_ID, rescrape_days, limit))
        return list(cur.fetchall())


def update_link_enrichment(
    conn,
    *,
    link_id: int,
    existing_payload: dict[str, Any],
    source_url: str,
    ean: str | None,
    detail_payload: dict[str, Any],
    rescrape_days: int,
    write: bool,
) -> None:
    attempted_at = utc_now()
    miss_count = as_int(existing_payload.get("ean_content_miss_count"), 0)
    enrichment: dict[str, Any] = {
        "ean_last_attempt_at": attempted_at.isoformat(),
        "detail_seen_at": attempted_at.isoformat(),
        "detail_source_url": source_url,
        "detail_payload": detail_payload,
    }
    if ean:
        enrichment.update(
            {
                "ean_enrichment_status": "found",
                "ean": ean,
                "ean_raw": ean,
                "ean_next_attempt_at": None,
                "ean_content_miss_count": miss_count,
            }
        )
    else:
        enrichment.update(
            {
                "ean_enrichment_status": "missing",
                "ean": None,
                "ean_raw": None,
                "ean_content_miss_count": miss_count + 1,
                "ean_next_attempt_at": (attempted_at + timedelta(days=rescrape_days)).isoformat(),
                "ean_revisit_after_days": rescrape_days,
            }
        )

    if not write:
        print("[NORTHEND-DETAIL] dry_run_link_enrichment", {"link_id": link_id, "payload": enrichment}, flush=True)
        return

    with conn.cursor() as cur:
        cur.execute(
            """
            update shop_product_links
            set payload = coalesce(payload, '{}'::jsonb) || %s,
                last_detail_scraped_at = now()
            where id = %s
            """,
            (Jsonb(enrichment), link_id),
        )


def fetch_html(url: str, timeout: int = 45, attempts: int = 3) -> tuple[int, str]:
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                url,
                headers=REQUEST_HEADERS,
                timeout=(10, timeout),
            )
            return response.status_code, response.text
        except requests.exceptions.RequestException as exc:
            last_error = exc
            if attempt >= attempts:
                print(
                    "[NORTHEND-DETAIL-FETCH-ERROR]",
                    {
                        "url": url,
                        "attempt": attempt,
                        "attempts": attempts,
                        "error": repr(exc),
                    },
                    flush=True,
                )
                return 599, ""

            sleep_seconds = attempt * 5
            print(
                "[NORTHEND-DETAIL-FETCH-RETRY]",
                {
                    "url": url,
                    "attempt": attempt,
                    "attempts": attempts,
                    "sleep_seconds": sleep_seconds,
                    "error": repr(exc),
                },
                flush=True,
            )
            time.sleep(sleep_seconds)

    print(
        "[NORTHEND-DETAIL-FETCH-ERROR]",
        {"url": url, "error": repr(last_error)},
        flush=True,
    )
    return 599, ""


def parse_detail_page(html: str, *, listing_payload: dict[str, Any]) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    text = main_product_text(soup)
    title_raw = extract_title(soup, listing_payload.get("title_raw"))
    ean = extract_ean(text)
    image_url = extract_image_url(soup) or listing_payload.get("image_url")
    availability = normalize_availability(text) or listing_payload.get("availability")
    detail_price, detail_original_price, detail_is_sale = extract_prices(text)
    listing_price = listing_payload.get("price")
    format_raw = infer_format(title_raw, text) or listing_payload.get("format") or listing_payload.get("carrier")

    return {
        "ean": ean,
        "title_raw": title_raw,
        "artist_raw": listing_payload.get("artist_raw"),
        "format": format_raw,
        "carrier": format_raw,
        "image_url": image_url,
        "availability": availability,
        "price": listing_price,
        "price_source": "listing",
        "detail_price_seen": detail_price,
        "detail_original_price_seen": detail_original_price,
        "detail_price_is_sale_seen": detail_is_sale,
        "detail_text": text[:2500],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Enrich North End Haarlem USF links with detail EAN metadata.")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--delay-seconds", type=float, default=0.75)
    parser.add_argument("--rescrape-days", type=int, default=14)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    load_env()
    with db_connection() as conn:
        candidates = get_detail_candidates(conn, limit=args.limit, rescrape_days=args.rescrape_days)
        print(
            "[NORTHEND-DETAIL] candidates",
            {"count": len(candidates), "limit": args.limit, "rescrape_days": args.rescrape_days, "write": args.write},
            flush=True,
        )

        for index, row in enumerate(candidates, start=1):
            payload = dict(row.get("payload") or {})
            source_url = row["source_url"]
            print("[NORTHEND-DETAIL] fetch", {"index": index, "url": source_url}, flush=True)
            status_code, html = fetch_html(source_url)
            if status_code >= 400:
                print("[NORTHEND-DETAIL-SKIP] http_error", {"url": source_url, "status_code": status_code}, flush=True)
                update_link_enrichment(
                    conn,
                    link_id=row["id"],
                    existing_payload=payload,
                    source_url=source_url,
                    ean=None,
                    detail_payload={"http_status": status_code},
                    rescrape_days=args.rescrape_days,
                    write=args.write,
                )
                if args.write:
                    conn.commit()
                continue

            detail = parse_detail_page(html, listing_payload=payload)
            ean = detail.get("ean")
            price = detail.get("price")
            availability = detail.get("availability") or payload.get("availability")
            image_url = detail.get("image_url") or payload.get("image_url")
            title_raw = detail.get("title_raw") or payload.get("title_raw")

            raw_payload = {
                **payload,
                "detail_source_url": source_url,
                "detail_seen_at": utc_now().isoformat(),
                "ean_enrichment_status": "found" if ean else "missing",
                "price_source": "listing",
                "detail_price_seen": detail.get("detail_price_seen"),
                "detail_original_price_seen": detail.get("detail_original_price_seen"),
                "detail_price_is_sale_seen": detail.get("detail_price_is_sale_seen"),
                "detail_text": detail.get("detail_text"),
            }

            if args.write:
                insert_raw_shop_scrape(
                    run_id=None,
                    shop_id=SHOP_ID,
                    source_url=source_url,
                    source_product_id=row.get("source_product_id"),
                    title_raw=title_raw,
                    ean_raw=ean,
                    price_raw=price,
                    availability_raw=availability,
                    image_url_raw=image_url,
                    payload={
                        **raw_payload,
                        "artist_raw": detail.get("artist_raw") or payload.get("artist_raw"),
                        "currency": "EUR",
                    },
                )

            update_link_enrichment(
                conn,
                link_id=row["id"],
                existing_payload=payload,
                source_url=source_url,
                ean=ean,
                detail_payload={k: v for k, v in raw_payload.items() if k not in {"detail_text", "listing_text"}},
                rescrape_days=args.rescrape_days,
                write=args.write,
            )

            print(
                "[NORTHEND-DETAIL] parsed",
                {"url": source_url, "ean_found": bool(ean), "price_source": "listing", "price": str(price) if price else None},
                flush=True,
            )
            if args.write:
                conn.commit()
            if args.delay_seconds > 0:
                time.sleep(args.delay_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
