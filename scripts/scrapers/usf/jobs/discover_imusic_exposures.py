#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

from scripts.importers.common import normalize_ean, normalize_text, parse_price
from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "imusic"
BASE_URL = "https://imusic.nl"
DEFAULT_OFFSET_STEP = 100
DEFAULT_DELAY_SECONDS = 4.0
DEFAULT_TIMEOUT_SECONDS = 20.0

PRODUCT_HREF_RE = re.compile(r"/music/([0-9][0-9\-\s]{7,20})(?:/|$|\?|#)")
EURO_PRICE_RE = re.compile(r"€\s*([0-9]+(?:[.,][0-9]{2}))")

AVAILABILITY_MARKERS = (
    "op voorraad",
    "weinig op voorraad",
    "besteld in een afgelegen magazijn",
    "verwachte levering",
    "koop",
    "pre-order",
    "preorder",
    "voorbestelling",
    "uitverkocht",
    "niet leverbaar",
)

EXPOSURE_DEFAULTS: dict[str, list[tuple[str, str, str, str]]] = {
    "daily": [
        ("16705", "vinylaanbiedingen", "Vinylaanbiedingen", "daily"),
        ("3146", "nieuwe-lp-s-en-aankomende-vinyl-releases", "Nieuwe LP's en aankomende vinyl releases", "daily"),
        ("11003", "vinyl-meest-populair", "Vinyl meest populair", "daily"),
    ],
    "weekly": [
        ("21670", "gelimiteerde-gekleurde-vinyl-soundtracks", "Gelimiteerde gekleurde vinyl soundtracks", "weekly"),
        ("11855", "blue-note-tone-poet-series-blue-note-80", "Blue Note Tone Poet / Blue Note 80", "weekly"),
    ],
    "rsd": [
        ("24202", "record-store-day-2026-rsd-2026-", "Record Store Day 2026 / RSD 2026", "once"),
    ],
}


@dataclass(frozen=True)
class ExposureSpec:
    exposure_id: str
    slug: str
    name: str
    frequency: str


def clean(value: object) -> str:
    return normalize_text(value) or ""


def default_exposures(exposure_set: str) -> list[ExposureSpec]:
    if exposure_set == "all":
        raw = EXPOSURE_DEFAULTS["daily"] + EXPOSURE_DEFAULTS["weekly"] + EXPOSURE_DEFAULTS["rsd"]
    elif exposure_set in EXPOSURE_DEFAULTS:
        raw = EXPOSURE_DEFAULTS[exposure_set]
    else:
        raise SystemExit("[ERROR] --exposure-set moet daily, weekly, rsd, all of custom zijn.")

    return [
        ExposureSpec(exposure_id=exposure_id, slug=slug, name=name, frequency=frequency)
        for exposure_id, slug, name, frequency in raw
    ]


def parse_custom_exposures(value: str | None) -> list[ExposureSpec]:
    if not value:
        return []

    specs: list[ExposureSpec] = []
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue

        pieces = part.split(":")
        if len(pieces) < 2:
            raise SystemExit(
                "[ERROR] Custom exposure moet minimaal id:slug zijn, "
                "bv. 16705:vinylaanbiedingen:daily"
            )

        exposure_id = pieces[0].strip()
        slug = pieces[1].strip()
        frequency = pieces[2].strip() if len(pieces) >= 3 and pieces[2].strip() else "custom"
        name = pieces[3].strip() if len(pieces) >= 4 and pieces[3].strip() else slug

        if not exposure_id.isdigit():
            raise SystemExit(f"[ERROR] Ongeldige exposure id: {exposure_id}")
        if not slug:
            raise SystemExit(f"[ERROR] Lege slug voor exposure id: {exposure_id}")

        specs.append(
            ExposureSpec(
                exposure_id=exposure_id,
                slug=slug,
                name=name,
                frequency=frequency,
            )
        )

    return specs


def build_exposure_url(exposure: ExposureSpec, *, offset: int) -> str:
    url = f"{BASE_URL}/exposure/{exposure.exposure_id}/{exposure.slug}"
    if offset > 0:
        return f"{url}?offset={offset}#tbl"
    return url


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; Vinylofy-iMusic-ExposureDiscovery/1.0; "
                "+https://vinylofy.nl)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.6",
        }
    )
    return session


def ean_from_href(href: object) -> str | None:
    href_text = clean(href)
    if not href_text:
        return None

    match = PRODUCT_HREF_RE.search(href_text)
    if not match:
        return None

    return normalize_ean(match.group(1))


def context_text_until_next_product(anchor: Tag, current_ean: str) -> str:
    parts: list[str] = []

    anchor_text = clean(anchor.get_text(" ", strip=True))
    if anchor_text:
        parts.append(anchor_text)

    for node in anchor.next_elements:
        if node is anchor:
            continue

        if isinstance(node, Tag):
            if node.name == "a" and node.has_attr("href"):
                next_ean = ean_from_href(node.get("href"))
                if next_ean and next_ean != current_ean:
                    break

            if node.name in {"script", "style", "noscript", "svg"}:
                continue

        if isinstance(node, NavigableString):
            value = clean(str(node))
            if value:
                parts.append(value)

        joined = " | ".join(parts)
        low = joined.lower()

        # Bij sale-items niet stoppen op "Normale prijs"; wacht op koop/preorder.
        if "€" in joined and (
            "koop" in low
            or "pre-order" in low
            or "preorder" in low
            or "voorbestelling" in low
        ):
            if len(joined) > 80:
                break

        if len(joined) > 2200:
            break

    return " | ".join(parts)


def extract_price_raw(text: str) -> str | None:
    matches = EURO_PRICE_RE.findall(text)
    valid: list[str] = []
    for match in matches:
        if parse_price(match) is not None:
            valid.append(match)

    if not valid:
        return None

    # Laatste prijs wint: iMusic zet bij aanbiedingen vaak eerst "Normale prijs".
    return valid[-1]


def extract_availability_hint(text: str) -> str | None:
    low = text.lower()
    if not any(marker in low for marker in AVAILABILITY_MARKERS):
        return None

    parts = [clean(part) for part in re.split(r"\s*\|\s*|\s{2,}", text)]
    snippets = [
        part
        for part in parts
        if part and any(marker in part.lower() for marker in AVAILABILITY_MARKERS)
    ]

    if snippets:
        return " | ".join(snippets[:4])

    found = [marker for marker in AVAILABILITY_MARKERS if marker in low]
    return " | ".join(found[:4]) if found else None


def extract_title_hint(anchor: Tag, context_text: str) -> str | None:
    link_text = clean(anchor.get_text(" ", strip=True))
    if link_text and len(link_text) > 3:
        return link_text[:300]

    title_attr = clean(anchor.get("title"))
    if title_attr:
        return title_attr[:300]

    aria = clean(anchor.get("aria-label"))
    if aria:
        return aria[:300]

    if context_text:
        return context_text[:300]

    return None


def parse_exposure_links(
    html: str,
    *,
    listing_url: str,
    exposure: ExposureSpec,
    offset: int,
) -> list[DiscoveredLink]:
    soup = BeautifulSoup(html, "html.parser")
    links_by_ean: dict[str, DiscoveredLink] = {}

    for position, anchor in enumerate(soup.find_all("a", href=True), start=1):
        href = clean(anchor.get("href"))
        if not href:
            continue

        ean = ean_from_href(href)
        if not ean:
            continue

        listing_product_url = urljoin(BASE_URL, href)
        context_text = context_text_until_next_product(anchor, ean)
        price_raw = extract_price_raw(context_text)
        availability_hint = extract_availability_hint(context_text)
        title_hint = extract_title_hint(anchor, context_text)

        payload: dict[str, Any] = {
            "source": "imusic_exposure_listing",
            "detail_priority": "high",
            "exposure_id": exposure.exposure_id,
            "exposure_slug": exposure.slug,
            "exposure_name": exposure.name,
            "exposure_frequency": exposure.frequency,
            "offset": offset,
            "listing_url": listing_url,
            "listing_product_url": listing_product_url,
            "listing_position": position,
            "ean": ean,
        }

        if title_hint:
            payload["listing_title_hint"] = title_hint
        if price_raw:
            payload["listing_price_raw"] = price_raw
            payload["listing_currency"] = "EUR"
            payload["listing_price_source"] = "exposure_listing"
        if availability_hint:
            payload["listing_availability_hint"] = availability_hint
        if context_text:
            payload["listing_context_sample"] = context_text[:1000]

        queue_url = f"{BASE_URL}/music/{ean}"

        links_by_ean[ean] = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=queue_url,
            source_product_id=ean,
            payload=payload,
        )

    return list(links_by_ean.values())


def requeue_detail_for_links(links: list[DiscoveredLink]) -> int:
    source_urls = [link.source_url for link in links if link.source_url]
    if not source_urls:
        return 0

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.shop_product_links
                set last_detail_scraped_at = null
                where shop_id = %s
                  and source_url = any(%s)
                """,
                (SHOP_ID, source_urls),
            )
            return int(cur.rowcount or 0)


def discover_exposure_links(
    *,
    exposures: list[ExposureSpec],
    start_page_per_exposure: int,
    max_pages_per_exposure: int,
    offset_step: int,
    delay_seconds: float,
    timeout_seconds: float,
) -> list[DiscoveredLink]:
    session = build_session()
    all_links_by_ean: dict[str, DiscoveredLink] = {}

    for exposure in exposures:
        pages_processed = 0
        offset = (start_page_per_exposure - 1) * offset_step
        seen_offsets: set[int] = set()

        while pages_processed < max_pages_per_exposure:
            if offset in seen_offsets:
                break

            seen_offsets.add(offset)
            listing_url = build_exposure_url(exposure, offset=offset)

            try:
                response = session.get(
                    listing_url,
                    timeout=timeout_seconds,
                    allow_redirects=True,
                )
            except requests.RequestException as exc:
                print(
                    "[IMUSIC-EXPOSURE-ERROR]",
                    {
                        "exposure_id": exposure.exposure_id,
                        "exposure_name": exposure.name,
                        "offset": offset,
                        "url": listing_url,
                        "error": str(exc),
                    },
                    flush=True,
                )
                break

            if response.status_code >= 400:
                print(
                    "[IMUSIC-EXPOSURE-HTTP-SKIP]",
                    {
                        "exposure_id": exposure.exposure_id,
                        "exposure_name": exposure.name,
                        "offset": offset,
                        "url": listing_url,
                        "status_code": response.status_code,
                    },
                    flush=True,
                )
                break

            links = parse_exposure_links(
                response.text,
                listing_url=response.url,
                exposure=exposure,
                offset=offset,
            )

            new_count = 0
            for link in links:
                ean = link.source_product_id or ""
                if ean not in all_links_by_ean:
                    new_count += 1
                all_links_by_ean[ean] = link

            print(
                "[IMUSIC-EXPOSURE-PAGE]",
                {
                    "exposure_id": exposure.exposure_id,
                    "exposure_name": exposure.name,
                    "frequency": exposure.frequency,
                    "offset": offset,
                    "status_code": response.status_code,
                    "page_links": len(links),
                    "new_links_added": new_count,
                    "unique_total": len(all_links_by_ean),
                    "url": response.url,
                },
                flush=True,
            )

            pages_processed += 1

            if not links:
                print(
                    "[IMUSIC-EXPOSURE-DISCOVER] stop empty page",
                    {
                        "exposure_id": exposure.exposure_id,
                        "exposure_name": exposure.name,
                        "offset": offset,
                    },
                    flush=True,
                )
                break

            offset += offset_step

            if delay_seconds > 0:
                time.sleep(delay_seconds)

    return list(all_links_by_ean.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Discover iMusic exposure/verzamelpagina links met hoge detailprioriteit. "
            "Schrijft shop_product_links; detail_imusic valideert EAN/UPC."
        )
    )
    parser.add_argument(
        "--exposure-set",
        choices=["daily", "weekly", "rsd", "all", "custom"],
        default="daily",
    )
    parser.add_argument(
        "--exposures",
        default=None,
        help="Alleen voor custom. Format: id:slug:frequency:name,id:slug:frequency:name",
    )
    parser.add_argument("--start-page-per-exposure", type=int, default=1)
    parser.add_argument("--max-pages-per-exposure", type=int, default=1)
    parser.add_argument("--offset-step", type=int, default=DEFAULT_OFFSET_STEP)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--requeue-detail", action="store_true")
    parser.add_argument("--write", action="store_true")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.start_page_per_exposure < 1:
        raise SystemExit("[ERROR] --start-page-per-exposure moet minimaal 1 zijn.")
    if args.max_pages_per_exposure < 1:
        raise SystemExit("[ERROR] --max-pages-per-exposure moet minimaal 1 zijn.")
    if args.offset_step < 1:
        raise SystemExit("[ERROR] --offset-step moet minimaal 1 zijn.")
    if args.delay < 0:
        raise SystemExit("[ERROR] --delay mag niet negatief zijn.")
    if args.timeout <= 0:
        raise SystemExit("[ERROR] --timeout moet positief zijn.")


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)

    if args.exposure_set == "custom":
        exposures = parse_custom_exposures(args.exposures)
        if not exposures:
            raise SystemExit("[ERROR] --exposure-set custom vereist --exposures.")
    else:
        exposures = default_exposures(args.exposure_set)

    print(
        "[IMUSIC-EXPOSURE-DISCOVER] start",
        {
            "exposure_set": args.exposure_set,
            "exposures": [
                {
                    "id": exposure.exposure_id,
                    "slug": exposure.slug,
                    "name": exposure.name,
                    "frequency": exposure.frequency,
                }
                for exposure in exposures
            ],
            "start_page_per_exposure": args.start_page_per_exposure,
            "max_pages_per_exposure": args.max_pages_per_exposure,
            "offset_step": args.offset_step,
            "delay": args.delay,
            "requeue_detail": args.requeue_detail,
            "write": args.write,
        },
        flush=True,
    )

    links = discover_exposure_links(
        exposures=exposures,
        start_page_per_exposure=args.start_page_per_exposure,
        max_pages_per_exposure=args.max_pages_per_exposure,
        offset_step=args.offset_step,
        delay_seconds=args.delay,
        timeout_seconds=args.timeout,
    )

    print(
        "[IMUSIC-EXPOSURE-DISCOVER] collected",
        {
            "unique_links": len(links),
            "write": args.write,
            "requeue_detail": args.requeue_detail,
        },
        flush=True,
    )

    for link in links[:20]:
        payload = link.payload or {}
        print(
            "[IMUSIC-EXPOSURE-SAMPLE]",
            {
                "source_url": link.source_url,
                "source_product_id": link.source_product_id,
                "exposure_id": payload.get("exposure_id"),
                "exposure_name": payload.get("exposure_name"),
                "listing_price_raw": payload.get("listing_price_raw"),
                "listing_availability_hint": payload.get("listing_availability_hint"),
                "listing_product_url": payload.get("listing_product_url"),
            },
            flush=True,
        )

    if not args.write:
        print("[IMUSIC-EXPOSURE-DISCOVER] dry-run complete; geen databasewrites.", flush=True)
        return 0

    result = upsert_discovered_links(links)
    requeued = requeue_detail_for_links(links) if args.requeue_detail else 0

    print(
        "[IMUSIC-EXPOSURE-DISCOVER] registry",
        {
            **vars(result),
            "requeued_detail": requeued,
        },
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
