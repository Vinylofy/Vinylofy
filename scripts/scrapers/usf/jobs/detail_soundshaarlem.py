from __future__ import annotations

import argparse
import re
import time
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import insert_raw_shop_scrape, mark_detail_scraped

SHOP_ID = "soundshaarlem"
BASE_URL = "https://soundshaarlem.nl"

PRICE_RE = re.compile(r"€\s*([0-9]+(?:[.,][0-9]{1,2})?)")
BARCODE_RE = re.compile(r"\bBarcode\s+([0-9]{8,14})\b", flags=re.I)


def clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
        }
    )
    return session


def normalize_url(url: str) -> str:
    parsed = urlparse(urljoin(BASE_URL, url))
    return urlunparse(("https", "soundshaarlem.nl", parsed.path.rstrip("/"), "", "", ""))


def extract_price(text: str) -> str | None:
    text = clean(text)

    sale_matches = re.findall(r"Sale price\s*€\s*([0-9]+(?:[.,][0-9]{1,2})?)", text, flags=re.I)
    if sale_matches:
        return sale_matches[-1].replace(",", ".")

    regular_matches = re.findall(r"Regular price\s*€\s*([0-9]+(?:[.,][0-9]{1,2})?)", text, flags=re.I)
    if regular_matches:
        return regular_matches[0].replace(",", ".")

    matches = PRICE_RE.findall(text)
    if matches:
        return matches[-1].replace(",", ".")

    return None


def extract_availability(text: str) -> str:
    lower = clean(text).lower()
    if "in-stock" in lower or "in stock" in lower:
        return "in_stock"
    if "pre-order" in lower or "pre order" in lower or "preorder" in lower:
        return "preorder"
    if "back-order" in lower or "back order" in lower or "backorder" in lower:
        return "preorder"
    if "sold out" in lower or "uitverkocht" in lower:
        return "out_of_stock"
    return "unknown"


def extract_detail_field(text: str, label: str) -> str | None:
    # Product details are rendered as text blocks such as:
    # Artist Various Title ... Format LP Release date ... Barcode 019...
    labels = [
        "Artist",
        "Title",
        "Format",
        "Extra",
        "Genre",
        "Release date",
        "Barcode",
        "Origin",
    ]

    alternatives = "|".join(re.escape(item) for item in labels if item != label)
    pattern = rf"\b{re.escape(label)}\s+(.+?)(?=\s+(?:{alternatives})\s+|$)"
    match = re.search(pattern, text, flags=re.I)
    if not match:
        return None

    return clean(match.group(1))


def extract_image_url(soup: BeautifulSoup) -> str | None:
    for selector in (
        'meta[property="og:image"]',
        'meta[name="og:image"]',
        'meta[property="twitter:image"]',
        'meta[name="twitter:image"]',
    ):
        tag = soup.select_one(selector)
        if tag and clean(tag.get("content")):
            return urljoin(BASE_URL, clean(tag.get("content")))

    img = soup.select_one('img[src*="cdn.shopify.com"], img[src]')
    if img and clean(img.get("src")):
        return urljoin(BASE_URL, clean(img.get("src")))

    return None


def parse_product_page(html: str, source_url: str, listing_payload: dict[str, Any]) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    text = clean(soup.get_text(" ", strip=True))

    title_tag = soup.select_one("h1")
    page_title = clean(title_tag.get_text(" ", strip=True)) if title_tag else clean(listing_payload.get("title"))

    barcode_match = BARCODE_RE.search(text)
    barcode = barcode_match.group(1) if barcode_match else None

    artist = extract_detail_field(text, "Artist")
    product_title = extract_detail_field(text, "Title")
    format_label = extract_detail_field(text, "Format")
    release_date = extract_detail_field(text, "Release date")
    origin = extract_detail_field(text, "Origin")
    genre = extract_detail_field(text, "Genre")
    extra = extract_detail_field(text, "Extra")

    price = extract_price(text) or clean(listing_payload.get("price") or listing_payload.get("price_current")) or None
    availability = extract_availability(text)
    if availability == "unknown":
        availability = clean(listing_payload.get("availability")) or "unknown"

    return {
        "source_url": source_url,
        "artist_raw": artist,
        "title_raw": product_title or page_title,
        "display_name_raw": page_title,
        "format_label_raw": format_label or clean(listing_payload.get("format") or listing_payload.get("format_label_raw")),
        "release_date_raw": release_date,
        "origin_raw": origin,
        "genre_raw": genre,
        "extra_raw": extra,
        "ean_raw": barcode,
        "ean_normalized": barcode,
        "ean_source": "detail_barcode_text" if barcode else None,
        "price_current": price,
        "currency": "EUR" if price else None,
        "availability": availability,
        "availability_text": availability,
        "image_url": extract_image_url(soup),
    }


def get_detail_candidates(limit: int, rescrape_days: int) -> list[dict[str, Any]]:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, shop_id, source_url, source_product_id, payload, last_detail_scraped_at
                from public.shop_product_links
                where shop_id = %s
                  and status = 'active'
                  and (
                    last_detail_scraped_at is null
                    or last_detail_scraped_at < now() - (%s * interval '1 day')
                  )
                order by last_detail_scraped_at asc nulls first, first_seen_at asc
                limit %s
                """,
                (SHOP_ID, rescrape_days, limit),
            )
            rows = cur.fetchall()

    return [
        {
            "id": str(row[0]),
            "shop_id": row[1],
            "source_url": row[2],
            "source_product_id": row[3],
            "payload": row[4] or {},
            "last_detail_scraped_at": row[5],
        }
        for row in rows
    ]


def title_raw_from_detail(row: dict[str, Any], payload: dict[str, Any]) -> str | None:
    artist = clean(row.get("artist_raw"))
    title = clean(row.get("title_raw") or row.get("display_name_raw") or payload.get("title"))
    if artist and title:
        return f"{artist} - {title}"
    return title or artist or None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Sounds Haarlem Shopify detail pages into raw_shop_scrapes.")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--rescrape-days", type=int, default=14)
    parser.add_argument("--delay-seconds", type=float, default=0.5)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")
    if args.rescrape_days < 1:
        raise SystemExit("[ERROR] --rescrape-days moet minimaal 1 zijn.")
    if args.delay_seconds < 0:
        raise SystemExit("[ERROR] --delay-seconds mag niet negatief zijn.")

    links = get_detail_candidates(limit=args.limit, rescrape_days=args.rescrape_days)
    session = make_session()

    print(
        "[DETAIL] queued",
        {
            "shop": SHOP_ID,
            "queued": len(links),
            "limit": args.limit,
            "rescrape_days": args.rescrape_days,
            "write": bool(args.write),
        },
        flush=True,
    )

    if not links:
        print("[DETAIL] niets te doen.", flush=True)
        return 0

    for idx, link in enumerate(links, start=1):
        source_url = normalize_url(clean(link.get("source_url")))
        payload = dict(link.get("payload") or {})

        print("[DETAIL] fetch", {"idx": idx, "total": len(links), "url": source_url}, flush=True)

        try:
            response = session.get(source_url, timeout=30)

            if response.status_code == 404:
                print("[DETAIL][WARN] 404", {"url": source_url}, flush=True)
                continue

            if response.status_code == 429:
                print("[DETAIL][WARN] HTTP 429, stopping safely.", {"url": source_url}, flush=True)
                break

            response.raise_for_status()
            detail_row = parse_product_page(response.text, source_url, payload)

        except Exception as exc:
            print("[DETAIL][WARN] failed", {"url": source_url, "error": str(exc)}, flush=True)
            continue

        ean_raw = clean(detail_row.get("ean_normalized") or detail_row.get("ean_raw")) or None
        price_raw = clean(detail_row.get("price_current") or payload.get("price") or payload.get("price_current")) or None
        availability_raw = clean(detail_row.get("availability") or payload.get("availability")) or "unknown"
        image_url_raw = clean(detail_row.get("image_url")) or None

        print(
            "[DETAIL] sample",
            {
                "url": source_url,
                "ean": ean_raw,
                "price": price_raw,
                "availability": availability_raw,
                "title": title_raw_from_detail(detail_row, payload),
            },
            flush=True,
        )

        if not args.write:
            if idx < len(links) and args.delay_seconds > 0:
                time.sleep(args.delay_seconds)
            continue

        raw_id = insert_raw_shop_scrape(
            run_id=None,
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=clean(link.get("source_product_id")) or None,
            title_raw=title_raw_from_detail(detail_row, payload),
            ean_raw=ean_raw,
            price_raw=price_raw,
            availability_raw=availability_raw,
            image_url_raw=image_url_raw,
            payload={
                "source": "detail_soundshaarlem_shopify",
                "detail_fields": detail_row,
                "listing_payload": payload,
                "policy": "detail enriches barcode/EAN; listing remains preferred for fast price refresh",
            },
        )
        mark_detail_scraped(str(link["id"]))

        print("[DETAIL] stored", {"raw_id": raw_id, "url": source_url, "ean": ean_raw}, flush=True)

        if idx < len(links) and args.delay_seconds > 0:
            time.sleep(args.delay_seconds)

    if not args.write:
        print("[DETAIL] dry-run complete; geen databasewrites.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
