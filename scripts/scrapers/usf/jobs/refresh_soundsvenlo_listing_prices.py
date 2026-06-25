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
from scripts.scrapers.usf.core.fast_listing_price_sync import (
    bulk_update_prices_from_link_registry,
)
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import (
    ListingOffer,
    sync_listing_offers,
)
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
    """Return the current visible sale/listing price.

    If sale wording is present, the sale/current price wins over any old
    crossed-out price. If the page only exposes multiple amounts without
    semantic labels, the last visible amount wins; this matches the existing
    Sounds Delft listing-first guardrail.
    """
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

    # Only real Sounds Venlo HTTP(S) product pages.
    # Prevent footer/contact links like mailto:, tel:, javascript: and anchors.
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return False
    if parsed.netloc and parsed.netloc != SHOP_DOMAIN:
        return False

    path = parsed.path.strip("/")
    lower_path = path.lower()

    if not path:
        return False

    if lower_path.startswith((
        "vinyl",
        "cd",
        "dvd",
        "blu-ray",
        "merchandise",
        "accessoires",
        "contact",
        "klantenservice",
        "winkelwagen",
        "account",
        "zoeken",
        "search",
        "privacy",
        "algemene-voorwaarden",
        "retour",
    )):
        return False

    if any(marker in lower_path for marker in (
        "mailto:",
        "tel:",
        "javascript:",
        "?",
        "#",
    )):
        return False

    # Sounds Venlo product pages are usually single slug pages ending in slash,
    # often with an EAN or internal product id in the slug.
    # Category/listing pages contain deeper paths such as /world-3/p8/.
    if "/" in path:
        return False

    # Avoid obvious non-product static/site pages.
    if "." in lower_path and not lower_path.endswith(".html"):
        return False

    return True
def source_product_id_from_url(url: str) -> str | None:
    slug = urlparse(url).path.strip("/").split("/")[-1]
    if not slug:
        return None
    return slug[:240]


def product_container_for(anchor: Tag) -> Tag | None:
    node: Tag | None = anchor

    # Walk up from the product anchor, but never allow page-level wrappers
    # such as body/footer/header/nav to become a product card.
    for _ in range(24):
        if node is None or not isinstance(node, Tag):
            break

        if node.name in {"body", "html", "footer", "header", "nav"}:
            break

        text = clean(node.get_text(" ", strip=True))
        lower = text.lower()

        has_price = extract_price(text) is not None
        has_stock_signal = any(
            marker in lower
            for marker in (
                "op voorraad",
                "niet leverbaar",
                "uitverkocht",
                "pre-order",
                "preorder",
                "verwacht",
                "1-2 werkdagen",
            )
        )

        if has_price and has_stock_signal:
            return node

        node = node.parent if isinstance(node.parent, Tag) else None

    return None
def first_product_title_anchor(container: Tag, source_url: str) -> Tag | None:
    for candidate in container.select("a[href]"):
        href = clean(candidate.get("href"))
        if not href:
            continue
        if normalize_product_url(href) != source_url:
            continue
        text = clean(candidate.get_text(" ", strip=True))
        if not text:
            continue
        if text.lower() in {"image", "afbeelding", "meer info"}:
            continue
        return candidate
    return None


def extract_artist_title_format(
    container: Tag,
    source_url: str,
) -> tuple[str | None, str | None, str | None]:
    title_anchor = first_product_title_anchor(container, source_url)
    title = clean(title_anchor.get_text(" ", strip=True)) if title_anchor else None

    lines = [
        clean(line)
        for line in container.get_text("\n", strip=True).splitlines()
        if clean(line)
    ]

    artist: str | None = None
    if title and title in lines:
        title_index = lines.index(title)
        before_title = [
            line
            for line in lines[:title_index]
            if line.lower() not in {"image", "afbeelding"}
            and "placeholder" not in line.lower()
        ]
        if before_title:
            artist = before_title[-1]

    text = clean(container.get_text(" ", strip=True))
    format_label: str | None = None
    match = re.search(
        r"\b((?:\d+\s*[-]?\s*)?(?:LP|CD|DVD|Blu-ray|7\s*(?:inch|\")|10\s*(?:inch|\")|12\s*(?:inch|\")))\b\s*€",
        text,
        flags=re.IGNORECASE,
    )
    if match:
        format_label = clean(match.group(1)).replace(" ", "").upper()
        format_label = format_label.replace("-", "-")

    return artist, title, format_label


def parse_listing_page(
    html: str,
    *,
    page: int,
    listing_url: str,
    seen_at: datetime,
    debug: bool,
) -> tuple[list[DiscoveredLink], list[ListingOffer]]:
    """Parse Sounds Venlo listing from rendered text order.

    The rendered listing follows this pattern:
      artist -> product title link -> format/price/availability

    DOM card detection is unreliable on this site because title/image/price
    blocks are not always contained in a compact product-card ancestor.
    """
    soup = BeautifulSoup(html, "html.parser")

    text_nodes: list[dict[str, object]] = []

    for node in soup.find_all(string=True):
        value = clean(node)
        if not value:
            continue

        parent = node.parent if isinstance(node.parent, Tag) else None
        anchor = parent.find_parent("a") if parent else None
        if parent and parent.name == "a":
            anchor = parent

        anchor_url = None
        if isinstance(anchor, Tag):
            href = clean(anchor.get("href"))
            if href and not href.lower().startswith(("mailto:", "tel:", "javascript:", "#")):
                normalized = normalize_product_url(href)
                if likely_product_url(normalized):
                    anchor_url = normalized

        text_nodes.append({"text": value, "anchor_url": anchor_url})

    links_by_url: dict[str, DiscoveredLink] = {}
    offers_by_url: dict[str, ListingOffer] = {}
    position = 0

    ignored_previous = {
        "image",
        "afbeelding",
        "filters",
        "sluiten",
        "vinyl",
        "alle resultaten voor \"\"",
        "onderstaande releases zijn uit voorraad leverbaar",
    }

    for idx, item in enumerate(text_nodes):
        source_url = item.get("anchor_url")
        if not isinstance(source_url, str) or not source_url:
            continue

        title = clean(item.get("text"))
        if not title:
            continue

        lower_title = title.lower()
        if lower_title in {"image", "afbeelding", "meer info"}:
            continue
        if "placeholder" in lower_title:
            continue

        # Product info is normally immediately after the linked title.
        forward_text = clean(" ".join(
            str(part["text"])
            for part in text_nodes[idx: idx + 10]
            if part.get("text")
        ))

        price = extract_price(forward_text)
        if not price:
            continue

        availability = extract_availability(forward_text)
        if availability == "unknown":
            # Prevent accidental menu/footer matches.
            continue

        format_label: str | None = None
        match = re.search(
            r"\b((?:\\d+\\s*[-]?\\s*)?(?:LP|CD|DVD|Blu-ray|7\\s*(?:inch|\")|10\\s*(?:inch|\")|12\\s*(?:inch|\")))\\b\\s*(?:€|EUR)",
            forward_text,
            flags=re.IGNORECASE,
        )
        if match:
            format_label = clean(match.group(1)).replace(" ", "").upper()

        artist: str | None = None
        for prev in reversed(text_nodes[max(0, idx - 8): idx]):
            candidate = clean(prev.get("text"))
            if not candidate:
                continue
            lower = candidate.lower()
            if lower in ignored_previous:
                continue
            if lower.startswith(("€", "lp €", "cd €")):
                continue
            if "op voorraad" in lower or "niet leverbaar" in lower:
                continue
            if len(candidate) > 120:
                continue
            artist = candidate
            break

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
def fetch_html_with_playwright(url: str, *, referer: str | None = None, timeout_ms: int = 45000) -> str:
    """Fetch listing HTML through Chromium.

    Sounds Venlo returns 403 for requests/curl from hosted environments,
    but allows a real browser context. This keeps only the transport layer
    browser-based; the USF flow remains listing-first.
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
    """Fetch listing HTML, falling back to Playwright on 403."""
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


def listing_url_for_page(page: int) -> str:
    if page <= 1:
        return f"{BASE_URL}/vinyl/"
    return f"{BASE_URL}/vinyl/p{page}/"


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

        try:
            referer = f"{BASE_URL}/vinyl/" if page <= 1 else listing_url_for_page(page - 1)
            response = session.get(
                listing_url,
                timeout=30,
                headers={"Referer": referer},
            )
        except requests.RequestException as exc:
            consecutive_failures += 1
            print(
                "[LISTING-REFRESH][WARN] request failed",
                {
                    "page": page,
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

        if response.status_code == 429:
            print("[LISTING-REFRESH][WARN] HTTP 429, stopping safely.", flush=True)
            break

        if response.status_code in (404, 410):
            print(
                "[LISTING-REFRESH] end reached",
                {"page": page, "status": response.status_code},
                flush=True,
            )
            break

        if response.status_code >= 500:
            consecutive_failures += 1
            print(
                "[LISTING-REFRESH][WARN] server error",
                {
                    "page": page,
                    "status": response.status_code,
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

        if response.status_code == 403:
            print("[LISTING-REFRESH][WARN] requests returned 403; using Playwright", {"url": listing_url}, flush=True)
            html = fetch_html_with_playwright(listing_url, referer=referer)
        else:
            response.raise_for_status()
            html = response.text
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
