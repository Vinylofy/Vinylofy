#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from scripts.importers.common import normalize_ean, normalize_text, parse_price
from scripts.scrapers.usf.core.link_registry import (
    get_links_for_detail_scrape,
    insert_raw_shop_scrape,
    mark_detail_scraped,
)

SHOP_ID = "imusic"
PRICE_LABELS = {"prijs", "price", "preis"}
NORMAL_PRICE_MARKERS = {
    "normale prijs",
    "normal price",
    "normaler preis",
    "prix normal",
}
STOP_AVAILABILITY_MARKERS = {
    "onze klanten zeggen",
    "our customers say",
    "unsere kunden sagen",
    "voeg toe aan",
    "add to your",
    "zu deiner",
}
OUT_OF_STOCK_MARKERS = {
    "uitverkocht",
    "niet leverbaar",
    "sold out",
    "out of stock",
    "not available",
    "unavailable",
}
PREORDER_MARKERS = {
    "pre-order",
    "preorder",
    "voorbestelling",
}
IN_STOCK_MARKERS = {
    "koop",
    "buy",
    "kopen",
    "kaufen",
    "op voorraad",
    "weinig op voorraad",
    "besteld",
    "ordered",
    "warehouse",
    "magazijn",
    "lager",
}


@dataclass(frozen=True)
class ParsedOffer:
    title: str | None
    artist: str | None
    title_raw: str | None
    ean: str | None
    price_raw: str | None
    availability: str
    availability_text: str | None
    image_url: str | None
    format_label: str | None
    canonical_url: str
    final_url: str
    http_status: int


def text_lines(soup: BeautifulSoup) -> list[str]:
    return [
        normalize_text(line)
        for line in soup.get_text("\n").splitlines()
        if normalize_text(line)
    ]


def meta_content(soup: BeautifulSoup, *keys: str) -> str | None:
    for key in keys:
        node = soup.find("meta", attrs={"property": key}) or soup.find(
            "meta", attrs={"name": key}
        )
        if node and node.get("content"):
            return normalize_text(node.get("content"))
    return None


def extract_canonical_url(soup: BeautifulSoup, fallback_url: str) -> str:
    node = soup.find("link", attrs={"rel": lambda value: value and "canonical" in str(value).lower()})
    href = normalize_text(node.get("href")) if node else ""
    return href or fallback_url


def extract_image_url(soup: BeautifulSoup, base_url: str) -> str | None:
    image = meta_content(soup, "og:image", "twitter:image")
    if image:
        return urljoin(base_url, image)

    node = soup.find("img")
    if node and node.get("src"):
        return urljoin(base_url, normalize_text(node.get("src")))

    return None


def extract_ean(soup: BeautifulSoup, lines: list[str]) -> str | None:
    text = soup.get_text(" ", strip=True)
    match = re.search(r"EAN/UPC\s+([0-9][0-9\-\s]{7,24})", text, flags=re.I)
    if match:
        return normalize_ean(match.group(1))

    for index, line in enumerate(lines):
        if "EAN/UPC" not in line.upper():
            continue

        inline = re.search(r"([0-9][0-9\-\s]{7,24})", line)
        if inline:
            return normalize_ean(inline.group(1))

        for candidate in lines[index + 1 : index + 4]:
            found = re.search(r"([0-9][0-9\-\s]{7,24})", candidate)
            if found:
                return normalize_ean(found.group(1))

    return None


def extract_price_token(value: str) -> str | None:
    match = re.search(r"€\s*([0-9]+(?:[.,][0-9]{2}))", value)
    if match:
        return match.group(1)

    match = re.search(r"\b([0-9]+(?:[.,][0-9]{2}))\b", value)
    if match:
        return match.group(1)

    return None


def extract_main_price(lines: list[str]) -> tuple[str | None, int | None]:
    for index, line in enumerate(lines):
        if line.lower() not in PRICE_LABELS:
            continue

        for candidate in lines[index + 1 : index + 14]:
            low = candidate.lower()
            if any(marker in low for marker in NORMAL_PRICE_MARKERS):
                continue

            token = extract_price_token(candidate)
            if token and parse_price(token) is not None:
                return token, index

    for index, line in enumerate(lines):
        low = line.lower()
        if not any(label in low for label in PRICE_LABELS):
            continue
        if any(marker in low for marker in NORMAL_PRICE_MARKERS):
            continue

        token = extract_price_token(line)
        if token and parse_price(token) is not None:
            return token, index

    return None, None


def extract_availability(lines: list[str], price_index: int | None) -> tuple[str, str | None]:
    if price_index is None:
        return "unknown", None

    window: list[str] = []
    for line in lines[price_index + 1 : price_index + 12]:
        low = line.lower()
        if any(marker in low for marker in STOP_AVAILABILITY_MARKERS):
            break
        window.append(line)

    raw = " | ".join(window) if window else None
    low = (raw or "").lower()

    if any(marker in low for marker in PREORDER_MARKERS):
        return "preorder", raw

    if any(marker in low for marker in OUT_OF_STOCK_MARKERS):
        return "out_of_stock", raw

    if any(marker in low for marker in IN_STOCK_MARKERS):
        return "in_stock", raw

    return "unknown", raw


def split_og_title(value: str | None) -> tuple[str | None, str | None]:
    if not value:
        return None, None

    text = normalize_text(value)
    text = re.sub(r"\s*-\s*iMusic\s*$", "", text, flags=re.I)

    if " · " in text:
        left, right = text.split(" · ", 1)
        title = re.sub(r"\s*\([^)]*\)\s*$", "", right).strip()
        return normalize_text(left) or None, normalize_text(title) or None

    return None, text or None


def extract_title_artist(soup: BeautifulSoup, lines: list[str]) -> tuple[str | None, str | None]:
    og_title = meta_content(soup, "og:title", "twitter:title")
    artist, title = split_og_title(og_title)

    if title:
        return title, artist

    if soup.title and soup.title.string:
        artist, title = split_og_title(soup.title.string)
        if title:
            return title, artist

    heading_lines = [
        line
        for line in lines
        if line not in {"#", "##", "* * *"} and len(line) <= 120
    ]
    fallback_title = heading_lines[0] if heading_lines else None
    return fallback_title, None


def extract_format(lines: list[str], title: str | None) -> str | None:
    combined = " ".join(lines[:80]).upper()
    title_text = normalize_text(title).upper()

    for candidate in ("4LP", "3LP", "2LP", "LP", "12\"", "10\"", "7\"", "CD"):
        if candidate in title_text or candidate in combined:
            if candidate == '12"':
                return "12 inch"
            if candidate == '10"':
                return "10 inch"
            if candidate == '7"':
                return "7 inch"
            return candidate

    if "VINYL" in combined:
        return "LP"

    return None


def parse_offer(html: str, *, final_url: str, status_code: int) -> ParsedOffer:
    soup = BeautifulSoup(html, "html.parser")
    lines = text_lines(soup)

    canonical_url = extract_canonical_url(soup, final_url)
    image_url = extract_image_url(soup, canonical_url)
    ean = extract_ean(soup, lines)
    price_raw, price_index = extract_main_price(lines)
    availability, availability_text = extract_availability(lines, price_index)
    title, artist = extract_title_artist(soup, lines)
    format_label = extract_format(lines, title)

    title_raw = None
    if artist and title:
        title_raw = f"{artist} - {title}"
    elif title:
        title_raw = title

    return ParsedOffer(
        title=title,
        artist=artist,
        title_raw=title_raw,
        ean=ean,
        price_raw=price_raw,
        availability=availability,
        availability_text=availability_text,
        image_url=image_url,
        format_label=format_label,
        canonical_url=canonical_url,
        final_url=final_url,
        http_status=status_code,
    )


def expected_ean_from_link(link: dict[str, Any]) -> str | None:
    payload = link.get("payload") or {}
    return (
        normalize_ean(link.get("source_product_id"))
        or normalize_ean(payload.get("ean"))
        or normalize_ean(link.get("source_url"))
    )


def registry_payload_from_link(link: dict[str, Any]) -> dict[str, Any]:
    payload = link.get("payload") or {}
    return payload if isinstance(payload, dict) else {}


def listing_price_from_payload(payload: dict[str, Any]) -> str | None:
    value = normalize_text(
        payload.get("listing_price_raw")
        or payload.get("listing_price_hint")
        or payload.get("price_raw")
    )
    if value and parse_price(value) is not None:
        return value
    return None


def listing_availability_from_payload(payload: dict[str, Any]) -> tuple[str | None, str | None]:
    raw = normalize_text(
        payload.get("listing_availability_raw")
        or payload.get("listing_availability_hint")
        or payload.get("availability_raw")
    )
    if not raw:
        return None, None

    low = raw.lower()

    if any(marker in low for marker in PREORDER_MARKERS):
        return "preorder", raw

    if any(marker in low for marker in OUT_OF_STOCK_MARKERS):
        return "out_of_stock", raw

    if any(marker in low for marker in IN_STOCK_MARKERS):
        return "in_stock", raw

    return None, raw


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; Vinylofy-iMusic-EANLookup/1.0; "
                "+https://vinylofy.nl)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.6",
        }
    )
    return session


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fetch iMusic detailpagina's vanuit bestaande EAN-links."
    )
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--sleep", type=float, default=0.25)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")
    if args.timeout <= 0:
        raise SystemExit("[ERROR] --timeout moet positief zijn.")
    if args.sleep < 0:
        raise SystemExit("[ERROR] --sleep mag niet negatief zijn.")

    links = get_links_for_detail_scrape(SHOP_ID, limit=args.limit)
    session = build_session()

    stats = {
        "queued": len(links),
        "fetched": 0,
        "offers": 0,
        "skipped": 0,
        "errors": 0,
        "write": args.write,
    }

    for link in links:
        expected_ean = expected_ean_from_link(link)
        source_url = link["source_url"]

        try:
            response = session.get(
                source_url,
                timeout=args.timeout,
                allow_redirects=True,
            )
            stats["fetched"] += 1
        except requests.RequestException as exc:
            stats["errors"] += 1
            print(
                "[IMUSIC-DETAIL-ERROR]",
                {
                    "source_url": source_url,
                    "expected_ean": expected_ean,
                    "error": str(exc),
                },
                flush=True,
            )
            continue

        final_url = response.url
        status_code = response.status_code

        if status_code >= 400:
            stats["skipped"] += 1
            print(
                "[IMUSIC-DETAIL-SKIP]",
                {
                    "source_url": source_url,
                    "final_url": final_url,
                    "expected_ean": expected_ean,
                    "status_code": status_code,
                    "reason": "http_status",
                },
                flush=True,
            )
            if args.write:
                mark_detail_scraped(link["id"])
            time.sleep(args.sleep)
            continue

        parsed = parse_offer(
            response.text,
            final_url=final_url,
            status_code=status_code,
        )

        if expected_ean and parsed.ean != expected_ean:
            stats["skipped"] += 1
            print(
                "[IMUSIC-DETAIL-SKIP]",
                {
                    "source_url": source_url,
                    "final_url": final_url,
                    "expected_ean": expected_ean,
                    "found_ean": parsed.ean,
                    "reason": "ean_mismatch_or_missing",
                },
                flush=True,
            )
            if args.write:
                mark_detail_scraped(link["id"])
            time.sleep(args.sleep)
            continue

        registry_payload = registry_payload_from_link(link)
        listing_price_raw = listing_price_from_payload(registry_payload)
        listing_availability, listing_availability_text = listing_availability_from_payload(registry_payload)

        # Listingprijs is leidend als die uit een iMusic listing/verzamelpagina komt
        # en parsebaar is. Detailprijs blijft fallback voor EAN-only lookups.
        final_price_raw = listing_price_raw or parsed.price_raw
        final_price_source = "genre_listing" if listing_price_raw else "detail_page"

        final_availability = listing_availability or parsed.availability
        final_availability_text = listing_availability_text or parsed.availability_text
        final_availability_source = "genre_listing" if listing_availability else "detail_page"

        if not final_price_raw:
            stats["skipped"] += 1
            print(
                "[IMUSIC-DETAIL-SKIP]",
                {
                    "source_url": source_url,
                    "final_url": final_url,
                    "ean": parsed.ean,
                    "reason": "missing_price",
                    "listing_price_raw": listing_price_raw,
                    "detail_price_raw": parsed.price_raw,
                    "availability": final_availability,
                    "availability_text": final_availability_text,
                },
                flush=True,
            )
            if args.write:
                mark_detail_scraped(link["id"])
            time.sleep(args.sleep)
            continue

        raw_payload = {
            "source": "imusic_ean_detail",
            "shop_product_link_id": link["id"],
            "registry_source_url": source_url,
            "final_url": final_url,
            "canonical_url": parsed.canonical_url,
            "http_status": parsed.http_status,
            "expected_ean": expected_ean,
            "found_ean": parsed.ean,
            "artist": parsed.artist,
            "title": parsed.title,
            "format": parsed.format_label,
            "price_source": final_price_source,
            "listing_price_raw": listing_price_raw,
            "detail_price_raw": parsed.price_raw,
            "availability_source": final_availability_source,
            "availability_text": final_availability_text,
            "detail_availability": parsed.availability,
            "detail_availability_text": parsed.availability_text,
            "registry_payload": registry_payload,
        }

        raw_id = None
        if args.write:
            raw_id = insert_raw_shop_scrape(
                run_id=None,
                shop_id=SHOP_ID,
                source_url=parsed.canonical_url or final_url,
                source_product_id=parsed.ean or expected_ean,
                title_raw=parsed.title_raw,
                ean_raw=parsed.ean,
                price_raw=final_price_raw,
                availability_raw=final_availability,
                image_url_raw=parsed.image_url,
                payload=raw_payload,
            )
            mark_detail_scraped(link["id"])

        stats["offers"] += 1
        print(
            "[IMUSIC-DETAIL-OFFER]",
            {
                "raw_id": raw_id,
                "source_url": source_url,
                "product_url": parsed.canonical_url or final_url,
                "ean": parsed.ean,
                "price_raw": final_price_raw,
                "price_source": final_price_source,
                "listing_price_raw": listing_price_raw,
                "detail_price_raw": parsed.price_raw,
                "availability": final_availability,
                "availability_source": final_availability_source,
                "availability_text": final_availability_text,
                "title_raw": parsed.title_raw,
                "image_url": parsed.image_url,
                "write": args.write,
            },
            flush=True,
        )

        time.sleep(args.sleep)

    print("[IMUSIC-DETAIL] done", stats, flush=True)
    if not args.write:
        print("[IMUSIC-DETAIL] dry-run complete; geen databasewrites.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
