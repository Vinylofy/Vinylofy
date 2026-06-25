#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.fast_listing_price_sync import bulk_update_prices_from_link_registry
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import ListingOffer, sync_listing_offers
from scripts.scrapers.usf.core.models import DiscoveredLink


SHOP_ID = "soundsvenlo"
SHOP_NAME = "Sounds Venlo"
SHOP_DOMAIN = "sounds-venlo.nl"
SHOP_COUNTRY = "NL"
BASE_URL = "https://www.sounds-venlo.nl"

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

DEFAULT_HEADERS = {
    "User-Agent": DEFAULT_USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

PRODUCT_PATH_EXCLUDES = {
    "",
    "vinyl",
    "cd",
    "pop",
    "blues",
    "electronic",
    "experimenteel",
    "heavy",
    "hiphop",
    "jazz",
    "klassiek",
    "original-sound-tracks",
    "prog-page",
    "psyche-stoner",
    "punk",
    "reggae",
    "roots",
    "soul",
    "world",
    "aanbiedingen",
    "service",
    "nieuwsbrieven",
    "sounds-venlo",
    "record-store-day",
    "live-on-stage",
    "winkelwagen",
    "account",
    "zoeken",
    "search",
    "contact",
    "privacy",
    "algemene-voorwaarden",
}


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def normalize_product_url(href: str) -> str:
    url = urljoin(BASE_URL, href.split("#", 1)[0].split("?", 1)[0])
    return url.rstrip("/") + "/"


def normalize_price(value: str | None) -> str | None:
    text = clean(value)
    if not text:
        return None

    text = text.replace("€", "").replace("EUR", "").strip()
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")

    match = re.search(r"([0-9]+(?:\.[0-9]{1,2})?)", text)
    if not match:
        return None

    euros = match.group(1)
    if "." not in euros:
        return f"{euros}.00"

    whole, cents = euros.split(".", 1)
    return f"{whole}.{cents[:2].ljust(2, '0')}"


def extract_price(text: str) -> str | None:
    """Return current visible listing price. Sale/current price wins if labelled."""
    text = clean(text)
    if not text:
        return None

    sale_patterns = [
        r"(?:aanbiedingsprijs|actieprijs|sale\s*price|special\s*price|nu\s*voor|voor)\s*:?\s*(?:€|EUR)?\s*([0-9]+(?:[.,][0-9]{1,2})?)",
        r"(?:nu|now)\s*:?\s*(?:€|EUR)?\s*([0-9]+(?:[.,][0-9]{1,2})?)",
    ]
    for pattern in sale_patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return normalize_price(match.group(1))

    amounts = re.findall(
        r"(?:€|EUR)\s*([0-9]+(?:[.,][0-9]{1,2})?)",
        text,
        flags=re.IGNORECASE,
    )
    if not amounts:
        amounts = re.findall(r"\b([0-9]+[.,][0-9]{2})\b", text)

    if amounts:
        return normalize_price(amounts[-1])

    return None


def extract_availability(text: str) -> str:
    lower = clean(text).lower()

    if any(marker in lower for marker in ("pre-order", "preorder", "verwacht")):
        return "preorder"

    if any(
        marker in lower
        for marker in (
            "tijdelijk niet leverbaar",
            "(tijdelijk) niet leverbaar",
            "niet leverbaar",
            "uitverkocht",
            "sold out",
            "out of stock",
        )
    ):
        return "out_of_stock"

    if any(
        marker in lower
        for marker in (
            "op voorraad",
            "in winkelwagen",
            "wordt toegevoegd",
            "1-2 werkdagen",
            "leverbaar",
        )
    ):
        return "in_stock"

    return "unknown"


def likely_product_url(url: str) -> bool:
    parsed = urlparse(url)

    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return False
    if parsed.netloc and parsed.netloc != SHOP_DOMAIN:
        return False

    path = parsed.path.strip("/")
    lower_path = path.lower()

    if not path:
        return False
    if "/" in path:
        return False
    if lower_path in PRODUCT_PATH_EXCLUDES:
        return False
    if lower_path.startswith(("p", "page", "cart", "checkout", "account")) and lower_path[1:].isdigit():
        return False
    if lower_path.startswith(("mailto:", "tel:", "javascript:")):
        return False

    return True


def source_product_id_from_url(url: str) -> str | None:
    slug = urlparse(url).path.strip("/").split("/")[-1]
    if not slug:
        return None
    return slug[:240]


def listing_url_for_page(page: int) -> str:
    if page <= 1:
        return f"{BASE_URL}/vinyl/"
    return f"{BASE_URL}/vinyl/p{page}/"


def has_group_class(tag: Tag) -> bool:
    classes = tag.get("class", [])
    return isinstance(classes, list) and "group" in classes


def fetch_html_with_playwright(url: str, *, referer: str | None = None, timeout_ms: int = 45000) -> str:
    """Fetch listing HTML through Chromium.

    Sounds Venlo returns 403 for requests/curl from hosted environments,
    but allows a real browser context. Keep this isolated to transport.
    """
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            page = browser.new_page(
                user_agent=DEFAULT_USER_AGENT,
                locale="nl-NL",
                extra_http_headers={
                    "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
                    **({"Referer": referer} if referer else {}),
                },
            )
            response = page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            status = response.status if response else None
            html = page.content()

            if status and status >= 400:
                raise RuntimeError(f"Playwright fetch failed status={status} url={url}")

            return html
        finally:
            browser.close()


def fetch_listing_html(session: requests.Session, url: str, *, referer: str | None = None) -> str:
    """Fast requests first; Playwright fallback on 403 or request failure."""
    try:
        response = session.get(
            url,
            timeout=30,
            headers={"Referer": referer} if referer else None,
        )

        if response.status_code != 403:
            response.raise_for_status()
            return response.text

        print(
            "[LISTING-REFRESH][WARN] requests returned 403; using Playwright",
            {"url": url},
            flush=True,
        )
    except requests.RequestException as exc:
        print(
            "[LISTING-REFRESH][WARN] requests fetch failed; using Playwright",
            {"url": url, "error": str(exc)},
            flush=True,
        )

    return fetch_html_with_playwright(url, referer=referer)


def parse_listing_page(
    html: str,
    *,
    page: int,
    listing_url: str,
    seen_at: datetime,
    debug: bool,
) -> tuple[list[DiscoveredLink], list[ListingOffer]]:
    """Parse Sounds Venlo product cards via a.full-click.

    Observed card structure:
      span                = artist
      a.full-click[href]  = product URL + title
      p                   = format + price + availability
    """
    soup = BeautifulSoup(html, "html.parser")

    links_by_url: dict[str, DiscoveredLink] = {}
    offers_by_url: dict[str, ListingOffer] = {}
    position = 0

    anchors = soup.select("a.full-click[href]")

    if debug:
        print("[LISTING-DEBUG] full_click_anchors", {"count": len(anchors)}, flush=True)

    for anchor in anchors:
        href = clean(anchor.get("href"))
        if not href:
            continue
        if href.lower().startswith(("mailto:", "tel:", "javascript:", "#")):
            continue

        source_url = normalize_product_url(href)
        if not likely_product_url(source_url):
            continue

        title = clean(anchor.get_text(" ", strip=True))
        if not title or title.lower() in {"image", "afbeelding", "meer info"}:
            continue

        card = anchor.find_parent(lambda tag: isinstance(tag, Tag) and tag.name == "div" and has_group_class(tag))
        if card is None:
            title_block = anchor.find_parent("div")
            card = title_block.find_parent("div") if isinstance(title_block, Tag) else None

        if card is None or not isinstance(card, Tag):
            continue

        card_text = clean(card.get_text(" ", strip=True))
        price = extract_price(card_text)
        if not price:
            continue

        availability = extract_availability(card_text)
        if availability == "unknown":
            continue

        artist: str | None = None
        title_parent = anchor.parent if isinstance(anchor.parent, Tag) else None
        if isinstance(title_parent, Tag):
            artist_span = title_parent.find("span")
            if isinstance(artist_span, Tag):
                artist = clean(artist_span.get_text(" ", strip=True))

        if not artist:
            artist_span = card.find("span")
            if isinstance(artist_span, Tag):
                artist = clean(artist_span.get_text(" ", strip=True))

        format_label: str | None = None
        format_match = re.search(
            r"\b((?:\d+\s*[-]?\s*)?(?:LP|CD|DVD|Blu-ray|7\s*(?:inch|\")|10\s*(?:inch|\")|12\s*(?:inch|\")))\b",
            card_text,
            flags=re.IGNORECASE,
        )
        if format_match:
            format_label = clean(format_match.group(1)).upper().replace(" ", "")

        position += 1
        source_product_id = source_product_id_from_url(source_url)

        payload = {
            "discovery_source": "soundsvenlo_vinyl_listing",
            "discovery_url": listing_url,
            "page": page,
            "listing_position": position,
            "artist": artist,
            "title": title,
            "format": format_label,
            "price": price,
            "price_source": "listing",
            "availability": availability,
            "listing_seen_at": seen_at.isoformat(),
        }

        links_by_url[source_url] = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=source_product_id,
            payload=payload,
        )
        offers_by_url[source_url] = ListingOffer(
            shop_name=SHOP_NAME,
            shop_domain=SHOP_DOMAIN,
            shop_country=SHOP_COUNTRY,
            source_url=source_url,
            price=price,
            availability=availability,
            currency="EUR",
            seen_at=seen_at,
            raw=payload,
        )

        if debug and position <= 20:
            print(
                "[LISTING-DEBUG]",
                {
                    "page": page,
                    "position": position,
                    "url": source_url,
                    "artist": artist,
                    "title": title,
                    "format": format_label,
                    "price": price,
                    "availability": availability,
                },
                flush=True,
            )

    return list(links_by_url.values()), list(offers_by_url.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Refresh Sounds Venlo listing prices into shop_product_links/prices."
    )
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="Aantal listingpagina's; 0 = doorlopen tot leeg/dubbel.",
    )
    parser.add_argument("--sleep", type=float, default=0.35)
    parser.add_argument("--max-page-failures", type=int, default=3)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--fast-price-sync", action="store_true")
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.start_page < 1:
        raise SystemExit("[ERROR] --start-page moet minimaal 1 zijn.")
    if args.max_pages < 0:
        raise SystemExit("[ERROR] --max-pages mag niet negatief zijn.")
    if args.sleep < 0:
        raise SystemExit("[ERROR] --sleep mag niet negatief zijn.")
    if args.max_page_failures < 1:
        raise SystemExit("[ERROR] --max-page-failures moet minimaal 1 zijn.")

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    seen_at = datetime.now(timezone.utc)
    all_links: list[DiscoveredLink] = []
    all_offers: list[ListingOffer] = []
    seen_page_signatures: set[tuple[str, ...]] = set()

    page = args.start_page
    pages_done = 0
    consecutive_failures = 0

    while args.max_pages == 0 or pages_done < args.max_pages:
        listing_url = listing_url_for_page(page)
        print(
            "[LISTING-REFRESH] page",
            {"shop": SHOP_ID, "page": page, "url": listing_url},
            flush=True,
        )

        referer = f"{BASE_URL}/vinyl/" if page <= 1 else listing_url_for_page(page - 1)

        try:
            html = fetch_listing_html(session, listing_url, referer=referer)
        except Exception as exc:
            consecutive_failures += 1
            print(
                "[LISTING-REFRESH][WARN] fetch failed",
                {
                    "page": page,
                    "url": listing_url,
                    "error": str(exc),
                    "consecutive_failures": consecutive_failures,
                },
                flush=True,
            )
            if consecutive_failures >= args.max_page_failures:
                break
            page += 1
            pages_done += 1
            time.sleep(args.sleep)
            continue

        consecutive_failures = 0

        links, offers = parse_listing_page(
            html,
            page=page,
            listing_url=listing_url,
            seen_at=seen_at,
            debug=args.debug,
        )

        page_signature = tuple(link.source_url for link in links)
        if page_signature and page_signature in seen_page_signatures:
            print(
                "[LISTING-REFRESH][WARN] duplicate listing page detected; stopping.",
                {
                    "page": page,
                    "links": len(links),
                    "first_link": page_signature[0],
                    "last_link": page_signature[-1],
                },
                flush=True,
            )
            break

        if page_signature:
            seen_page_signatures.add(page_signature)

        print(
            "[LISTING-REFRESH-PAGE]",
            {
                "page": page,
                "links": len(links),
                "offers_with_price": len(offers),
                "write": args.write,
            },
            flush=True,
        )

        if not links:
            print(
                "[LISTING-REFRESH] no priced product links; stopping.",
                {"page": page},
                flush=True,
            )
            break

        all_links.extend(links)
        all_offers.extend(offers)

        pages_done += 1
        page += 1

        if args.max_pages == 0 or pages_done < args.max_pages:
            time.sleep(args.sleep)

    print(
        "[LISTING-REFRESH]",
        {
            "shop": SHOP_ID,
            "links": len(all_links),
            "offers_with_price": len(all_offers),
            "write": args.write,
        },
        flush=True,
    )

    for offer in all_offers[:5]:
        print(
            "[LISTING-SAMPLE]",
            {
                "source_url": offer.source_url,
                "price": str(offer.price),
                "availability": offer.availability,
            },
            flush=True,
        )

    if not all_links:
        raise SystemExit("[ERROR] Sounds Venlo listing refresh leverde geen links op.")

    if args.write and not all_offers:
        raise SystemExit("[ERROR] Sounds Venlo listing refresh vond geen prijzen; schrijf niets weg.")

    if not args.write:
        print("[LISTING-REFRESH] dry-run complete; geen databasewrites.", flush=True)
        return 0

    result = upsert_discovered_links(all_links)
    print(
        "[LISTING-REFRESH] registry",
        {"inserted": result.inserted, "updated": result.updated, "total": result.total},
        flush=True,
    )

    with db_connection() as conn:
        if args.fast_price_sync:
            stats = bulk_update_prices_from_link_registry(
                conn,
                shop_registry_id=SHOP_ID,
                shop_domain=SHOP_DOMAIN,
                write=True,
                currency="EUR",
            )
            print("[LISTING-REFRESH] fast_price_sync", vars(stats), flush=True)
        else:
            stats = sync_listing_offers(conn, all_offers, write=True)
            print("[LISTING-REFRESH] price_sync", vars(stats), flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
