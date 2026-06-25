#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import time

import psycopg

import requests

from scripts.scrapers.usf.core.link_registry import (
    get_links_for_detail_scrape,
    insert_raw_shop_scrape,
    mark_detail_scraped,
)


SHOP_ID = "sounds"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch Sounds detail pages into raw_shop_scrapes")
    p.add_argument("--limit", type=int, default=5)
    p.add_argument("--sleep", type=float, default=1.5)
    return p




def get_listing_price(source_url: str) -> str | None:
    """
    Listing-first policy:
    prijs komt uit shop_product_links.payload->>'price', niet uit de detailpagina.
    """
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        return None

    try:
        with psycopg.connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select payload->>'price'
                    from shop_product_links
                    where shop_id = %s
                      and source_url = %s
                    limit 1
                    """,
                    (SHOP_ID, source_url),
                )
                row = cur.fetchone()

        if not row or row[0] in (None, ""):
            return None

        return str(row[0]).replace(",", ".")
    except Exception as exc:
        print(f"[DETAIL][WARN] listing price lookup failed url={source_url} error={exc}", flush=True)
        return None


def extract_title(html: str) -> str | None:
    m = re.search(r"<title>(.*?)</title>", html, flags=re.I | re.S)
    if not m:
        return None
    return re.sub(r"\s+", " ", m.group(1)).strip()



def extract_ean(html: str) -> str | None:
    patterns = [
        r'"barcode"\s*:\s*"(\d{8,14})"',
        r'"gtin(?:8|12|13|14)?"\s*:\s*"(\d{8,14})"',
        r'\b(?:EAN|GTIN|Barcode|Streepjescode)\D{0,80}(\d{8,14})\b',
    ]

    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            digits = re.sub(r"\D", "", match.group(1))
            if 8 <= len(digits) <= 14:
                return digits

    return None


def extract_price(html: str) -> str | None:
    money_patterns = [
        r'property=["\']product:price:amount["\'][^>]*content=["\']([0-9]+(?:[.,][0-9]{1,2})?)["\']',
        r'content=["\']([0-9]+(?:[.,][0-9]{1,2})?)["\'][^>]*property=["\']product:price:amount["\']',
        r'"price"\s*:\s*"([0-9]+(?:[.,][0-9]{1,2})?)"',
        r'€\s*([0-9]+(?:[.,][0-9]{1,2})?)',
    ]

    for pattern in money_patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1).replace(",", ".")

    # Shopify JSON gebruikt vaak centen: "price":2499 = 24.99
    cent_patterns = [
        r'"price"\s*:\s*(\d{3,7})',
        r'"price_min"\s*:\s*(\d{3,7})',
    ]

    for pattern in cent_patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            cents = int(match.group(1))
            if cents > 100:
                return f"{cents / 100:.2f}"

    return None


def extract_availability(html: str) -> str | None:
    """Extract availability from Shopify product HTML.

    Shopify pages can contain hidden/stale generic text such as "Sold out"
    even when the active variant JSON says available=true. Therefore explicit
    Shopify variant availability and visible purchase signals must win over
    generic negative text.
    """
    lower = html.lower()

    # Explicit Shopify variant/product JSON wins over generic page text.
    if re.search(r'"available"\s*:\s*true\b', html, flags=re.IGNORECASE):
        return "in_stock"

    if re.search(r'"available"\s*:\s*false\b', html, flags=re.IGNORECASE):
        return "out_of_stock"

    # Positive purchase signals win over hidden generic "sold out" snippets.
    if (
        "instock" in lower
        or "add to cart" in lower
        or "in winkelwagen" in lower
        or "in winkelmandje" in lower
        or "binnen 48 uur" in lower
    ):
        return "in_stock"

    if "outofstock" in lower or "sold out" in lower or "uitverkocht" in lower:
        return "out_of_stock"

    return None


def extract_image_url(html: str) -> str | None:
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
        r'"featured_image"\s*:\s*"([^"]+)"',
        r'"image"\s*:\s*"([^"]+)"',
    ]

    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            image_url = match.group(1).replace("\\/", "/")
            if image_url.startswith("//"):
                image_url = "https:" + image_url
            return image_url

    return None

def main() -> int:
    args = build_parser().parse_args()
    links = get_links_for_detail_scrape(SHOP_ID, limit=args.limit)

    print(f"[DETAIL] queued={len(links)}", flush=True)

    session = requests.Session()

    for idx, link in enumerate(links, start=1):
        url = link["source_url"]
        print(f"[DETAIL] {idx}/{len(links)} {url}", flush=True)

        response = session.get(url, timeout=30)

        if response.status_code == 429:
            print("[WARN] HTTP 429, stopping safely.", flush=True)
            break

        if response.status_code in (404, 410):
            print(f"[DETAIL][SKIP] dead link status={response.status_code} url={url}")
            mark_detail_scraped(link["id"])
            continue

        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            print(f"[DETAIL][WARN] HTTP error url={url} error={exc}")
            continue

        html = response.text
        title = extract_title(html)
        ean_raw = extract_ean(html)
        price_raw = get_listing_price(url)  # listing-first policy: current price comes from shop_product_links.payload
        availability_raw = extract_availability(html)
        image_url_raw = extract_image_url(html)

        raw_id = insert_raw_shop_scrape(
            run_id=None,
            shop_id=SHOP_ID,
            source_url=url,
            source_product_id=link["source_product_id"],
            title_raw=title,
            ean_raw=ean_raw,
            price_raw=price_raw,
            availability_raw=availability_raw,
            image_url_raw=image_url_raw,
            payload={
                "html_length": len(html),
                "status_code": response.status_code,
                "source": "detail_sounds",
                "detail_price_policy": "no_current_price_from_detail_page",
            },
        )

        mark_detail_scraped(link["id"])
        print(f"[DETAIL] stored raw_id={raw_id}", flush=True)

        time.sleep(args.sleep)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
