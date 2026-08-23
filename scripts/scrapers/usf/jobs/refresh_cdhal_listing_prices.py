#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import re
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import (
    parse_qsl,
    urlencode,
    urljoin,
    urlsplit,
    urlunsplit,
)

import requests
from bs4 import BeautifulSoup, Tag
from psycopg.rows import dict_row

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.delist_missing_links import (
    mark_missing_links_out_of_stock,
)
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import (
    ListingOffer,
    sync_listing_offers,
)
from scripts.scrapers.usf.core.models import DiscoveredLink


SHOP_ID = "cdhal"
SHOP_NAME = "CD Hal Ruinen"
SHOP_DOMAIN = "cdhal.nl"
SHOP_COUNTRY = "NL"
CURRENCY = "EUR"

BASE_URL = "https://www.cdhal.nl"
LISTING_BASE_URL = f"{BASE_URL}/vinyl"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

TRACKING_PARAMETERS = {
    "fbclid",
    "gclid",
    "msclkid",
    "ref",
    "source",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}

SYSTEM_PREFIXES = (
    "/account",
    "/catalog/",
    "/catalogsearch",
    "/checkout",
    "/customer",
    "/media",
    "/privacy",
    "/pub",
    "/search",
    "/static",
    "/wishlist",
)


def clean(value: object) -> str:
    return re.sub(
        r"\s+",
        " ",
        str(value or "").replace("\xa0", " "),
    ).strip()


def normalize_price(value: object) -> str | None:
    text = clean(value)

    if not text:
        return None

    text = (
        text.replace("EUR", "")
        .replace("eur", "")
        .replace("€", "")
        .strip()
    )

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        text = text.replace(",", ".")

    match = re.search(r"(?<!\d)(\d+(?:\.\d{1,2})?)(?!\d)", text)

    if not match:
        return None

    try:
        amount = Decimal(match.group(1)).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None

    if amount <= 0:
        return None

    return f"{amount:.2f}"


def canonical_product_url(value: object) -> str:
    raw = clean(value)

    if not raw:
        return ""

    parsed = urlsplit(urljoin(BASE_URL, raw))

    if parsed.scheme not in {"http", "https"}:
        return ""

    if parsed.netloc.lower() not in {"cdhal.nl", "www.cdhal.nl"}:
        return ""

    path = re.sub(r"/+", "/", parsed.path or "/").rstrip("/")

    if not path or path == "/":
        return ""

    lowered = path.lower()

    if lowered == "/vinyl":
        return ""

    if any(
        lowered == prefix.rstrip("/")
        or lowered.startswith(prefix)
        for prefix in SYSTEM_PREFIXES
    ):
        return ""

    query = [
        (key, item)
        for key, item in parse_qsl(
            parsed.query,
            keep_blank_values=True,
        )
        if key.lower() not in TRACKING_PARAMETERS
        and key.lower()
        not in {
            "p",
            "product_list_dir",
            "product_list_limit",
            "product_list_order",
        }
    ]

    return urlunsplit(
        (
            "https",
            "www.cdhal.nl",
            path,
            urlencode(query),
            "",
        )
    )


def listing_url(page: int) -> str:
    if page <= 1:
        return LISTING_BASE_URL

    return f"{LISTING_BASE_URL}?{urlencode({'p': page})}"


def product_cards(soup: BeautifulSoup) -> list[Tag]:
    cards: list[Tag] = []

    for card in soup.select("li.product-item"):
        if not isinstance(card, Tag):
            continue

        link = card.select_one(
            "a.product-item-link[href], "
            ".product-item-name a[href]"
        )

        if not isinstance(link, Tag):
            continue

        if not canonical_product_url(link.get("href")):
            continue

        cards.append(card)

    return cards


def extract_url(card: Tag) -> str:
    link = card.select_one(
        "a.product-item-link[href], "
        ".product-item-name a[href]"
    )

    if not isinstance(link, Tag):
        return ""

    return canonical_product_url(link.get("href"))


def extract_title(card: Tag) -> str | None:
    link = card.select_one(
        "a.product-item-link, "
        ".product-item-name a"
    )

    if not isinstance(link, Tag):
        return None

    value = clean(
        link.get("title")
        or link.get("aria-label")
        or link.get_text(" ", strip=True)
    )

    return value or None


def split_artist_title(
    title_raw: str,
) -> tuple[str | None, str]:
    title = clean(title_raw)

    for separator in (" - ", " – ", " — "):
        if separator not in title:
            continue

        artist, album = title.split(separator, 1)

        if clean(artist) and clean(album):
            return clean(artist), clean(album)

    return None, title


def extract_product_id(
    card: Tag,
    source_url: str,
) -> str | None:
    nodes = (
        card,
        card.select_one("[data-product-id]"),
        card.select_one("[data-price-box]"),
    )

    for node in nodes:
        if not isinstance(node, Tag):
            continue

        for attribute in (
            "data-product-id",
            "data-product",
            "data-price-box",
            "id",
        ):
            match = re.search(
                r"(\d{2,})",
                clean(node.get(attribute)),
            )

            if match:
                return match.group(1)

    slug = urlsplit(source_url).path.rstrip("/").split("/")[-1]
    return slug[:240] if slug else None


def attribute_price(
    card: Tag,
    price_type: str,
) -> str | None:
    values: list[Decimal] = []

    selector = (
        f"[data-price-type='{price_type}']"
        "[data-price-amount]"
    )

    for node in card.select(selector):
        if not isinstance(node, Tag):
            continue

        value = normalize_price(
            node.get("data-price-amount")
        )

        if value:
            values.append(Decimal(value))

    if not values:
        return None

    return f"{min(values):.2f}"


def fallback_visible_price(
    card: Tag,
    selectors: tuple[str, ...],
) -> str | None:
    for selector in selectors:
        for node in card.select(selector):
            if not isinstance(node, Tag):
                continue

            candidates = (
                node.get("data-price-amount"),
                node.get("content"),
                node.get_text(" ", strip=True),
            )

            for candidate in candidates:
                value = normalize_price(candidate)

                if value:
                    return value

    return None


def extract_prices(
    card: Tag,
) -> tuple[str | None, str | None, bool, bool]:
    current_price = attribute_price(
        card,
        "finalPrice",
    )
    old_price = attribute_price(
        card,
        "oldPrice",
    )

    if current_price is None:
        current_price = fallback_visible_price(
            card,
            (
                ".special-price .price",
                ".price-final_price .price",
                ".price-box .price",
            ),
        )

    if old_price is None:
        old_price = fallback_visible_price(
            card,
            (
                ".old-price [data-price-amount]",
                ".old-price .price",
            ),
        )

    sale = False
    invalid_sale_pair = False

    if current_price and old_price:
        sale = Decimal(current_price) < Decimal(old_price)
        invalid_sale_pair = not sale

    return (
        current_price,
        old_price,
        sale,
        invalid_sale_pair,
    )


def extract_availability_raw(card: Tag) -> str:
    text = clean(card.get_text(" ", strip=True))

    patterns = (
        r"Niet op voorraad\s*:\s*Levertijd\s*\d+\s*-\s*\d+\s*Werkdagen",
        r"Direct leverbaar",
        r"Binnenkort leverbaar",
        r"Tijdelijk niet leverbaar",
    )

    for pattern in patterns:
        match = re.search(
            pattern,
            text,
            flags=re.IGNORECASE,
        )

        if match:
            return clean(match.group(0))

    return ""


def normalize_availability(
    value: str,
) -> tuple[str, str, bool]:
    text = clean(value).lower()

    if "tijdelijk niet leverbaar" in text:
        return (
            "out_of_stock",
            "temporary_unavailable",
            False,
        )

    if "binnenkort leverbaar" in text:
        return (
            "preorder",
            "coming_soon",
            True,
        )

    if (
        "niet op voorraad" in text
        and (
            "levertijd" in text
            or "werkdag" in text
        )
    ):
        return (
            "unknown",
            "backorder_with_lead_time",
            False,
        )

    if "direct leverbaar" in text:
        return (
            "in_stock",
            "direct",
            True,
        )

    return (
        "unknown",
        "unclassified",
        False,
    )


def parse_listing_page(
    html: str,
    *,
    page: int,
    source_listing_url: str,
    seen_at: datetime,
    debug: bool = False,
) -> tuple[
    list[DiscoveredLink],
    list[ListingOffer],
    dict[str, Any],
]:
    soup = BeautifulSoup(html, "html.parser")
    cards = product_cards(soup)

    links: dict[str, DiscoveredLink] = {}
    offers: dict[str, ListingOffer] = {}

    status_counts: Counter[str] = Counter()

    parsed_prices = 0
    sales = 0
    missing_prices = 0
    zero_prices = 0
    invalid_sale_pairs = 0

    for position, card in enumerate(cards, start=1):
        source_url = extract_url(card)
        title_raw = extract_title(card)

        if not source_url or not title_raw:
            continue

        artist, title = split_artist_title(title_raw)

        (
            current_price,
            old_price,
            sale,
            invalid_sale_pair,
        ) = extract_prices(card)

        raw_amounts = [
            clean(node.get("data-price-amount"))
            for node in card.select("[data-price-amount]")
            if isinstance(node, Tag)
        ]

        if current_price:
            parsed_prices += 1
        elif any(
            value in {"0", "0.0", "0.00"}
            for value in raw_amounts
        ):
            zero_prices += 1
        else:
            missing_prices += 1

        if sale:
            sales += 1

        if invalid_sale_pair:
            invalid_sale_pairs += 1

        availability_raw = extract_availability_raw(card)

        (
            availability,
            availability_kind,
            publishable_availability,
        ) = normalize_availability(availability_raw)

        status_counts[availability] += 1

        payload: dict[str, Any] = {
            "source": "cdhal_vinyl_listing",
            "discovery_url": source_listing_url,
            "page": page,
            "listing_position": position,
            "artist": artist,
            "title": title,
            "title_raw": title_raw,
            "price": current_price,
            "old_price": old_price,
            "sale": sale,
            "currency": CURRENCY,
            "price_source": "listing",
            "availability": availability,
            "source_availability": availability,
            "availability_kind": availability_kind,
            "availability_raw": availability_raw,
            "is_secondhand": False,
            "publish_eligible": bool(
                current_price
                and publishable_availability
                and not invalid_sale_pair
            ),
            "listing_seen_at": seen_at.isoformat(),
            "image_url": None,
            "image_policy": "no_shop_image_capture",
        }

        product_id = extract_product_id(
            card,
            source_url,
        )

        links[source_url] = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=product_id,
            payload=payload,
        )

        if current_price and not invalid_sale_pair:
            offers[source_url] = ListingOffer(
                shop_name=SHOP_NAME,
                shop_domain=SHOP_DOMAIN,
                shop_country=SHOP_COUNTRY,
                source_url=source_url,
                price=current_price,
                availability=availability,
                currency=CURRENCY,
                seen_at=seen_at,
                raw=payload,
            )

        if debug and position <= 40:
            print(
                "[CDHAL-LISTING-DEBUG]",
                {
                    "page": page,
                    "position": position,
                    "url": source_url,
                    "title": title_raw,
                    "price": current_price,
                    "old_price": old_price,
                    "sale": sale,
                    "availability": availability,
                    "availability_raw": availability_raw,
                },
                flush=True,
            )

    diagnostics = {
        "page": page,
        "cards": len(cards),
        "valid_links": len(links),
        "prices": parsed_prices,
        "sales": sales,
        "missing_prices": missing_prices,
        "zero_prices": zero_prices,
        "invalid_sale_pairs": invalid_sale_pairs,
        "availability": dict(status_counts),
    }

    return (
        list(links.values()),
        list(offers.values()),
        diagnostics,
    )


def fetch_html_with_playwright(
    url: str,
    *,
    referer: str | None = None,
    timeout_ms: int = 45_000,
) -> str:
    """Fetch CD Hal listing HTML through a normal Chromium context.

    CD Hal currently returns HTTP 403 to the Python requests transport from
    GitHub-hosted runners, while the public page remains reachable through a
    browser-like client. This is a transport fallback only: no CAPTCHA or
    authentication bypass is attempted.
    """
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            try:
                page = browser.new_page(
                    user_agent=HEADERS["User-Agent"],
                    locale="nl-NL",
                    extra_http_headers={
                        "Accept-Language": HEADERS["Accept-Language"],
                        **({"Referer": referer} if referer else {}),
                    },
                )
                response = page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=timeout_ms,
                )
                status = response.status if response else None
                html = page.content()
                if status is not None and status >= 400:
                    raise RuntimeError(
                        f"Playwright fetch failed status={status} url={url}"
                    )
                return html
            finally:
                browser.close()
    except Exception as exc:
        raise RuntimeError(
            f"CD Hal browser fallback failed for {url}: {exc}"
        ) from exc


def fetch_listing_html(
    session: requests.Session,
    url: str,
    *,
    referer: str | None = None,
) -> tuple[str, int, str]:
    """Use requests first and Chromium only for 403/transport failures."""
    try:
        response = session.get(
            url,
            timeout=45,
            headers={"Referer": referer} if referer else None,
        )
        if response.status_code != 403:
            response.raise_for_status()
            return response.text, response.status_code, "requests"

        print(
            "[CDHAL-LISTING-WARN] requests returned 403; using Playwright",
            {"url": url},
            flush=True,
        )
    except requests.RequestException as exc:
        print(
            "[CDHAL-LISTING-WARN] requests failed; using Playwright",
            {"url": url, "error": str(exc)},
            flush=True,
        )

    return fetch_html_with_playwright(url, referer=referer), 200, "playwright"


def load_registry_offers() -> list[ListingOffer]:
    with db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                select source_url, payload
                from public.shop_product_links
                where shop_id = %s
                  and source_url is not null
                  and payload->>'price' is not null
                  and trim(payload->>'price') <> ''
                order by source_url
                """,
                (SHOP_ID,),
            )

            rows = [
                dict(row)
                for row in cursor.fetchall()
            ]

    offers: list[ListingOffer] = []

    for row in rows:
        payload = row.get("payload")

        if not isinstance(payload, dict):
            continue

        source_url = canonical_product_url(
            row.get("source_url")
        )
        price = normalize_price(
            payload.get("price")
        )

        if not source_url or price is None:
            continue

        availability = clean(
            payload.get("availability")
        ).lower()

        if availability not in {
            "in_stock",
            "out_of_stock",
            "preorder",
            "unknown",
        }:
            availability = "unknown"

        offers.append(
            ListingOffer(
                shop_name=SHOP_NAME,
                shop_domain=SHOP_DOMAIN,
                shop_country=SHOP_COUNTRY,
                source_url=source_url,
                price=price,
                availability=availability,
                currency=CURRENCY,
                raw=payload,
            )
        )

    return offers


def synchronize(
    offers: list[ListingOffer],
    *,
    write: bool,
    label: str,
) -> None:
    if not offers:
        print(
            "[CDHAL-LISTING-SYNC]",
            {
                "label": label,
                "offers": 0,
                "write": write,
            },
            flush=True,
        )
        return

    with db_connection() as conn:
        stats = sync_listing_offers(
            conn,
            offers,
            write=write,
        )

    print(
        "[CDHAL-LISTING-SYNC]",
        {
            "label": label,
            **vars(stats),
            "write": write,
        },
        flush=True,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Listing-first USF-refresh voor CD Hal Ruinen."
        )
    )

    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="0 betekent volledige crawl tot natuurlijke stop.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.50,
    )
    parser.add_argument(
        "--max-page-failures",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--technical-page-limit",
        type=int,
        default=500,
    )
    parser.add_argument(
        "--debug",
        action="store_true",
    )
    parser.add_argument(
        "--write",
        action="store_true",
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.start_page < 1:
        raise SystemExit(
            "[ERROR] --start-page moet minimaal 1 zijn."
        )

    if args.max_pages < 0:
        raise SystemExit(
            "[ERROR] --max-pages mag niet negatief zijn."
        )

    if args.sleep < 0:
        raise SystemExit(
            "[ERROR] --sleep mag niet negatief zijn."
        )

    if args.max_page_failures < 1:
        raise SystemExit(
            "[ERROR] --max-page-failures moet minimaal 1 zijn."
        )

    session = requests.Session()
    session.headers.update(HEADERS)

    run_started_at = datetime.now(timezone.utc)

    all_links: dict[str, DiscoveredLink] = {}
    all_offers: dict[str, ListingOffer] = {}

    signatures: set[str] = set()

    current_page = args.start_page
    pages_fetched = 0
    failures = 0

    scan_completed_safely = False
    stop_reason = "not_started"

    while (
        args.max_pages == 0
        or pages_fetched < args.max_pages
    ):
        if current_page > args.technical_page_limit:
            stop_reason = "technical_page_limit"
            break

        current_url = listing_url(current_page)

        print(
            "[CDHAL-LISTING]",
            {
                "page": current_page,
                "url": current_url,
                "write": args.write,
            },
            flush=True,
        )

        try:
            html, http_status, transport = fetch_listing_html(
                session,
                current_url,
                referer=(
                    listing_url(current_page - 1)
                    if current_page > args.start_page
                    else None
                ),
            )
        except (requests.RequestException, RuntimeError) as exc:
            failures += 1

            print(
                "[CDHAL-LISTING-WARN]",
                {
                    "page": current_page,
                    "error": str(exc),
                    "failures": failures,
                },
                flush=True,
            )

            if failures >= args.max_page_failures:
                stop_reason = "http_failures"
                break

            time.sleep(args.sleep)
            continue

        failures = 0

        response_soup = BeautifulSoup(html, "html.parser")

        product_structure_present = bool(
            response_soup.select_one(
                "li.product-item"
            )
            and response_soup.select_one(
                "a.product-item-link[href]"
            )
            and response_soup.select_one(
                "[data-price-amount]"
            )
        )

        lowered = html.lower()

        hard_block_markers = [
            marker
            for marker in (
                "access denied",
                "cloudflare ray id",
                "verify you are human",
                "robot or human",
            )
            if marker in lowered
        ]

        if hard_block_markers and not product_structure_present:
            stop_reason = "blocked_or_captcha"

            print(
                "[CDHAL-LISTING-WARN]",
                {
                    "page": current_page,
                    "markers": hard_block_markers,
                    "product_structure_present": False,
                },
                flush=True,
            )
            break

        links, offers, diagnostics = parse_listing_page(
            html,
            page=current_page,
            source_listing_url=current_url,
            seen_at=run_started_at,
            debug=args.debug,
        )

        urls = sorted(
            link.source_url
            for link in links
        )

        signature = hashlib.sha256(
            "\n".join(urls).encode("utf-8")
        ).hexdigest()

        new_link_count = sum(
            source_url not in all_links
            for source_url in urls
        )

        print(
            "[CDHAL-LISTING-PAGE]",
            {
                **diagnostics,
                "http_status": http_status,
                "transport": transport,
                "new_links": new_link_count,
                "total_before": len(all_links),
            },
            flush=True,
        )

        if not urls:
            scan_completed_safely = True
            stop_reason = "no_valid_product_links"
            break

        if signature in signatures:
            scan_completed_safely = True
            stop_reason = "repeated_page"
            break

        if new_link_count == 0:
            scan_completed_safely = True
            stop_reason = "zero_new_links"
            break

        signatures.add(signature)

        for link in links:
            all_links[link.source_url] = link

        for offer in offers:
            all_offers[offer.source_url] = offer

        pages_fetched += 1
        current_page += 1

        if (
            args.max_pages
            and pages_fetched >= args.max_pages
        ):
            stop_reason = "configured_test_limit"
            scan_completed_safely = False
            break

        time.sleep(args.sleep)

    print(
        "[CDHAL-LISTING-SUMMARY]",
        {
            "pages_fetched": pages_fetched,
            "links": len(all_links),
            "price_offers": len(all_offers),
            "scan_completed_safely": scan_completed_safely,
            "stop_reason": stop_reason,
            "write": args.write,
        },
        flush=True,
    )

    if not all_links:
        raise SystemExit(
            "[ERROR] Geen geldige CDHAL-productlinks gevonden."
        )

    if not all_offers:
        raise SystemExit(
            "[ERROR] Geen geldige positieve listingprijzen gevonden."
        )

    if not args.write:
        print(
            "[CDHAL-LISTING] dry-run compleet; "
            "geen registry-, prijs- of delistingwrites.",
            flush=True,
        )
        return 0

    result = upsert_discovered_links(
        list(all_links.values())
    )

    print(
        "[CDHAL-LISTING-REGISTRY]",
        {
            "inserted": result.inserted,
            "updated": result.updated,
            "total": result.total,
        },
        flush=True,
    )

    synchronize(
        list(all_offers.values()),
        write=True,
        label="current_listing",
    )

    full_production_scan = (
        args.start_page == 1
        and args.max_pages == 0
        and scan_completed_safely
        and stop_reason
        in {
            "no_valid_product_links",
            "repeated_page",
            "zero_new_links",
        }
    )

    if full_production_scan:
        delist_result = mark_missing_links_out_of_stock(
            shop_id=SHOP_ID,
            seen_source_urls=all_links.keys(),
            run_started_at=run_started_at,
            write=True,
        )

        print(
            "[CDHAL-LISTING-MISSING]",
            delist_result,
            flush=True,
        )

        synchronize(
            load_registry_offers(),
            write=True,
            label="registry_after_missing_delist",
        )
    else:
        print(
            "[CDHAL-LISTING] missing-link-delisting overgeslagen; "
            "crawl was begrensd of niet aantoonbaar volledig.",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
