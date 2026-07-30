#!/usr/bin/env python3
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import re
import threading
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, Tag
from psycopg.rows import dict_row

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import (
    upsert_discovered_links,
)
from scripts.scrapers.usf.core.listing_price_sync import (
    ListingOffer,
    sync_listing_offers,
)
from scripts.scrapers.usf.core.models import DiscoveredLink


SHOP_ID = "shop3345"
SHOP_NAME = "3345"
SHOP_DOMAIN = "3345.nl"
SHOP_COUNTRY = "NL"

STOCK_PAYLOAD_KEYS = (
    "availability",
    "stock_previous_availability",
    "stock_scraped_availability",
    "stock_authority",
    "stock_updated_at",
)

BASE_URL = "https://3345.nl"

PRICE_AUTHORITY = "boost_filtered_listing_api"

BOOST_API_URL = (
    "https://services.mybcapps.com/"
    "bc-sf-filter/filter"
)
BOOST_SHOP = "3345-test.myshopify.com"
BOOST_COLLECTION_ID = "640093454680"
BOOST_LIMIT = 70

MIN_EXPECTED_PRODUCTS = 14_000
MAX_EXPECTED_PRODUCTS = 18_000

DEFAULT_CTA_WORKERS = 24
DEFAULT_CTA_TIMEOUT_SECONDS = 30

CTA_RETRY_WORKERS = 8
CTA_RETRY_COOLDOWN_SECONDS = 5


CTA_SECTION_QUERY = (
    "sections=variant-buttons,variant-badge"
)

_CTA_THREAD_LOCAL = threading.local()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/149.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
    "Referer": (
        "https://3345.nl/collections/all"
        "?format=_Format_LP&stock=true"
    ),
    "Origin": "https://3345.nl",
}

SECONDHAND_VENDOR_MARKERS = {
    "3345 second hand",
    "3345 secondhand",
}

SECONDHAND_EXACT_TAGS = {
    "used base",
    "used_pos",
}


def clean(value: object) -> str:
    return re.sub(
        r"\s+",
        " ",
        str(value or "").replace("\xa0", " "),
    ).strip()


def normalize_product_url(value: object) -> str:
    raw = clean(value)

    if not raw:
        return ""

    parsed = urlparse(raw)

    if parsed.scheme and parsed.netloc:
        if parsed.netloc.lower() not in {
            "3345.nl",
            "www.3345.nl",
        }:
            return ""

        path = parsed.path
    else:
        path = raw

    path = path.rstrip("/")

    if path.startswith("/nl/products/"):
        path = path[3:]

    if not path.startswith("/products/"):
        return ""

    return f"{BASE_URL}{path}"


def source_product_id_from_url(
    source_url: str,
) -> str | None:
    slug = (
        urlparse(source_url)
        .path.rstrip("/")
        .split("/")[-1]
    )

    return slug[:240] if slug else None


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
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")

    match = re.search(
        r"(\d+(?:\.\d{1,2})?)",
        text,
    )

    if not match:
        return None

    amount = match.group(1)

    if "." not in amount:
        return f"{amount}.00"

    whole, cents = amount.split(".", 1)

    return f"{whole}.{cents[:2].ljust(2, '0')}"


def tags_for(
    product: dict[str, Any],
) -> list[str]:
    raw = product.get("tags")

    if isinstance(raw, list):
        return [
            clean(value)
            for value in raw
            if clean(value)
        ]

    if isinstance(raw, str):
        return [
            clean(value)
            for value in raw.split(",")
            if clean(value)
        ]

    return []


def product_is_lp(
    product: dict[str, Any],
) -> bool:
    tags = {
        tag.lower()
        for tag in tags_for(product)
    }

    product_type = clean(
        product.get("product_type")
    ).lower()

    return (
        "_format_lp" in tags
        or product_type == "lp"
    )


def available_variants(
    product: dict[str, Any],
) -> list[dict[str, Any]]:
    variants = product.get("variants")

    if not isinstance(variants, list):
        return []

    return [
        variant
        for variant in variants
        if isinstance(variant, dict)
        and variant.get("available") is True
    ]


def product_is_available(
    product: dict[str, Any],
) -> bool:
    return (
        product.get("available") is True
        and bool(available_variants(product))
    )


def lowest_available_price(
    product: dict[str, Any],
) -> str | None:
    prices: list[str] = []

    for variant in available_variants(product):
        price = normalize_price(
            variant.get("price")
        )

        if price is not None:
            prices.append(price)

    if not prices:
        return None

    return min(
        prices,
        key=lambda value: int(
            value.replace(".", "")
        ),
    )


def detect_secondhand(
    product: dict[str, Any],
) -> bool:
    """Gebruik uitsluitend harde 3345-tweedehandssignalen.

    Geen algemene zoekactie naar woorden als 'used' of
    'second hand' in een albumtitel. Daardoor blijven titels
    zoals 'I Used To Think I Could Fly' en artiestennamen zoals
    'The Second Hand Orchestra' terecht nieuw.
    """
    handle = clean(
        product.get("handle")
    ).lower()

    title = clean(
        product.get("title")
    ).lower()

    vendor = clean(
        product.get("vendor")
    ).lower()

    tags = {
        tag.lower()
        for tag in tags_for(product)
    }

    if handle.startswith("used-"):
        return True

    if re.match(
        r"^used\s*[-–—:]",
        title,
        flags=re.IGNORECASE,
    ):
        return True

    if vendor in SECONDHAND_VENDOR_MARKERS:
        return True

    if tags.intersection(
        SECONDHAND_EXACT_TAGS
    ):
        return True

    if any(
        re.fullmatch(
            r"[a-z]_class_used",
            tag,
            flags=re.IGNORECASE,
        )
        for tag in tags
    ):
        return True

    return False


def boost_params(
    *,
    page: int,
) -> list[tuple[str, str]]:
    return [
        ("shop", BOOST_SHOP),
        (
            "collection_scope",
            BOOST_COLLECTION_ID,
        ),
        ("page", str(page)),
        ("limit", str(BOOST_LIMIT)),
        ("product_available", "true"),
        ("tag", "_Format_LP"),
        ("build_filter_tree", "false"),
        ("event_type", "page"),
    ]


def fetch_boost_page(
    session: requests.Session,
    *,
    page: int,
    max_attempts: int,
) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(
        1,
        max_attempts + 1,
    ):
        try:
            response = session.get(
                BOOST_API_URL,
                params=boost_params(page=page),
                timeout=90,
            )

            if response.status_code in {
                429,
                502,
                503,
                504,
            }:
                wait_seconds = attempt * 2

                print(
                    "[3345-BOOST-RETRY]",
                    {
                        "page": page,
                        "attempt": attempt,
                        "status": (
                            response.status_code
                        ),
                        "wait_seconds": (
                            wait_seconds
                        ),
                    },
                    flush=True,
                )

                time.sleep(wait_seconds)
                continue

            response.raise_for_status()

            payload = response.json()

            if not isinstance(payload, dict):
                raise RuntimeError(
                    "Boost-response is geen "
                    "JSON-object."
                )

            products = payload.get("products")

            if not isinstance(products, list):
                raise RuntimeError(
                    "Boost-response bevat geen "
                    "products-lijst."
                )

            return payload

        except (
            requests.RequestException,
            ValueError,
            TypeError,
            RuntimeError,
        ) as exc:
            last_error = exc

            if attempt < max_attempts:
                time.sleep(attempt * 2)

    raise RuntimeError(
        f"Boost-pagina {page} mislukte na "
        f"{max_attempts} pogingen: {last_error}"
    )



def product_to_records(
    product: dict[str, Any],
    *,
    page: int,
    position: int,
    seen_at: datetime,
) -> tuple[DiscoveredLink, ListingOffer]:
    product_id = clean(product.get("id"))
    handle = clean(product.get("handle"))

    if not product_id:
        raise RuntimeError(
            f"Product op pagina {page} heeft geen id."
        )

    if not handle:
        raise RuntimeError(
            f"Product {product_id} heeft geen handle."
        )

    if not product_is_lp(product):
        raise RuntimeError(
            f"Boost gaf niet-LP-product {handle}."
        )

    if not product_is_available(product):
        raise RuntimeError(
            f"Boost gaf onbeschikbaar product {handle}."
        )

    price = lowest_available_price(product)

    if price is None:
        raise RuntimeError(
            f"Product {handle} heeft geen "
            "beschikbare variantprijs."
        )

    source_url = normalize_product_url(
        f"/products/{handle}"
    )

    if not source_url:
        raise RuntimeError(
            f"Product {handle} gaf geen geldige URL."
        )

    secondhand = detect_secondhand(product)



    payload: dict[str, Any] = {
        "source": (
            "shop3345_boost_filtered_lp_price"
        ),
        "price_authority": PRICE_AUTHORITY,
        "discovery_url": (
            "https://3345.nl/collections/all"
            "?format=_Format_LP&stock=true"
        ),
        "boost_api_url": BOOST_API_URL,
        "boost_collection_id": (
            BOOST_COLLECTION_ID
        ),
        "boost_product_id": product_id,
        "page": page,
        "listing_position": position,
        "artist": clean(
            product.get("vendor")
        ) or None,
        "title": clean(
            product.get("title")
        ) or None,
        "format": "LP",
        "price": price,
        "currency": "EUR",
        "price_source": "listing",
        "listing_price_transport": (
            "boost_bc_sf_filter"
        ),
        "is_secondhand": secondhand,
        "listing_seen_at": seen_at.isoformat(),
        "tags": tags_for(product),
    }

    link = DiscoveredLink(
        shop_id=SHOP_ID,
        source_url=source_url,
        source_product_id=(
            source_product_id_from_url(
                source_url
            )
        ),
        payload=payload,
    )

    offer = ListingOffer(
        shop_name=SHOP_NAME,
        shop_domain=SHOP_DOMAIN,
        shop_country=SHOP_COUNTRY,
        source_url=source_url,
        price=price,
        availability=None,
        currency="EUR",
        seen_at=seen_at,
        raw=payload,
    )

    return link, offer


def sync_offers(
    offers: list[ListingOffer],
    *,
    write: bool,
    label: str,
) -> None:
    if not offers:
        print(
            "[3345-LISTING]",
            {
                "sync": label,
                "offers": 0,
            },
            flush=True,
        )
        return

    with db_connection() as conn:
        stats = sync_listing_offers(
            conn,
            offers,
            write=write,
                    preserve_availability=True,
        )

    print(
        "[3345-LISTING]",
        {
            "sync": label,
            **vars(stats),
            "write": write,
        },
        flush=True,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "USF listing-, prijs- en voorraadflow "
            "voor 3345 via de gefilterde Boost-feed."
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
        help=(
            "0 = volledige Boost-feed. "
            "Een begrensde run delist nooit."
        ),
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0.10,
    )

    parser.add_argument(
        "--max-page-failures",
        type=int,
        default=3,
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
            "[ERROR] --max-page-failures moet "
            "minimaal 1 zijn."
        )



    session = requests.Session()
    session.headers.update(HEADERS)

    run_started_at = datetime.now(
        timezone.utc
    )

    first_payload = fetch_boost_page(
        session,
        page=args.start_page,
        max_attempts=args.max_page_failures,
    )

    total_product = int(
        first_payload.get("total_product") or 0
    )

    if not (
        MIN_EXPECTED_PRODUCTS
        <= total_product
        <= MAX_EXPECTED_PRODUCTS
    ):
        raise SystemExit(
            "[ERROR] Onverwacht totaal aantal "
            f"Boost-producten: {total_product}. "
            f"Veilige bandbreedte is "
            f"{MIN_EXPECTED_PRODUCTS}–"
            f"{MAX_EXPECTED_PRODUCTS}."
        )

    expected_pages = math.ceil(
        total_product / BOOST_LIMIT
    )

    if args.start_page > expected_pages:
        raise SystemExit(
            f"[ERROR] Startpagina {args.start_page} "
            f"ligt na pagina {expected_pages}."
        )

    if args.max_pages:
        final_page = min(
            expected_pages,
            (
                args.start_page
                + args.max_pages
                - 1
            ),
        )
    else:
        final_page = expected_pages

    all_links: dict[
        str,
        DiscoveredLink,
    ] = {}

    all_offers: dict[
        str,
        ListingOffer,
    ] = {}

    product_ids: set[str] = set()
    handles: set[str] = set()
    page_signatures: set[
        tuple[str, ...]
    ] = set()

    secondhand_count = 0
    pages_fetched = 0

    for page in range(
        args.start_page,
        final_page + 1,
    ):
        started = time.monotonic()

        if page == args.start_page:
            payload = first_payload
        else:
            payload = fetch_boost_page(
                session,
                page=page,
                max_attempts=(
                    args.max_page_failures
                ),
            )

        page_total = int(
            payload.get("total_product") or 0
        )

        if page_total != total_product:
            raise SystemExit(
                "[ERROR] total_product wijzigde "
                f"op pagina {page}: "
                f"{page_total} versus "
                f"{total_product}."
            )

        products = payload.get("products")

        if not isinstance(products, list):
            raise SystemExit(
                f"[ERROR] Pagina {page} bevat "
                "geen products-lijst."
            )

        expected_size = (
            BOOST_LIMIT
            if page < expected_pages
            else (
                total_product
                - BOOST_LIMIT
                * (expected_pages - 1)
            )
        )

        if len(products) != expected_size:
            raise SystemExit(
                f"[ERROR] Pagina {page} bevat "
                f"{len(products)} producten; "
                f"verwacht {expected_size}."
            )

        page_ids = tuple(
            clean(product.get("id"))
            for product in products
            if isinstance(product, dict)
        )

        if (
            len(page_ids) != len(products)
            or not all(page_ids)
        ):
            raise SystemExit(
                f"[ERROR] Pagina {page} bevat "
                "producten zonder id."
            )

        if page_ids in page_signatures:
            raise SystemExit(
                f"[ERROR] Pagina {page} "
                "herhaalt een eerdere pagina."
            )

        page_signatures.add(page_ids)

        page_secondhand = 0

        for position, product in enumerate(
            products,
            start=1,
        ):
            if not isinstance(product, dict):
                raise SystemExit(
                    f"[ERROR] Pagina {page} "
                    "bevat een niet-objectproduct."
                )

            product_id = clean(
                product.get("id")
            )

            handle = clean(
                product.get("handle")
            )

            if product_id in product_ids:
                raise SystemExit(
                    f"[ERROR] Dubbel product-id: "
                    f"{product_id}."
                )

            if handle in handles:
                raise SystemExit(
                    f"[ERROR] Dubbele handle: "
                    f"{handle}."
                )

            product_ids.add(product_id)
            handles.add(handle)

            link, offer = product_to_records(
                product,
                page=page,
                position=position,
                seen_at=run_started_at,
            )

            if bool(
                link.payload.get(
                    "is_secondhand"
                )
            ):
                page_secondhand += 1
                secondhand_count += 1

            all_links[
                link.source_url
            ] = link

            all_offers[
                offer.source_url
            ] = offer

            if (
                args.debug
                and pages_fetched == 0
                and position <= 10
            ):
                print(
                    "[3345-BOOST-DEBUG]",
                    {
                        "page": page,
                        "position": position,
                        "url": link.source_url,
                        "artist": (
                            link.payload.get(
                                "artist"
                            )
                        ),
                        "title": (
                            link.payload.get(
                                "title"
                            )
                        ),
                        "price": (
                            link.payload.get(
                                "price"
                            )
                        ),
                        "is_secondhand": (
                            link.payload.get(
                                "is_secondhand"
                            )
                        ),
                    },
                    flush=True,
                )

        pages_fetched += 1

        print(
            "[3345-BOOST-PAGE]",
            {
                "page": page,
                "expected_pages": (
                    expected_pages
                ),
                "products": len(products),
                "total_unique": (
                    len(all_links)
                ),
                "secondhand": (
                    page_secondhand
                ),
                "elapsed_seconds": round(
                    time.monotonic()
                    - started,
                    2,
                ),
            },
            flush=True,
        )

        if args.sleep:
            time.sleep(args.sleep)











    if not args.write:
        print(
            "[3345-BOOST] Dry-run compleet; "
            "geen registry-, prijs- of "
            "delistingwrites.",
            flush=True,
        )
        return 0

    registry_result = upsert_discovered_links(
        list(all_links.values()),
                          preserve_payload_keys=STOCK_PAYLOAD_KEYS,
    )

    print(
        "[3345-LISTING-REGISTRY]",
        {
            "inserted": (
                registry_result.inserted
            ),
            "updated": (
                registry_result.updated
            ),
            "total": registry_result.total,
        },
        flush=True,
    )

    sync_offers(
        list(all_offers.values()),
        write=True,
        label="current_boost_listing",
    )


    return 0


if __name__ == "__main__":
    raise SystemExit(main())
