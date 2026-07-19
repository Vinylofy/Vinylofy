#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import json
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import requests
from psycopg.rows import dict_row

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.delist_missing_links import mark_missing_links_out_of_stock
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.listing_price_sync import ListingOffer, sync_listing_offers
from scripts.scrapers.usf.core.models import DiscoveredLink
from scripts.scrapers.usf.jobs.everythingjazz_product_type import (
    canonical_vinyl_format,
    is_everythingjazz_vinyl_type,
)

SHOP_ID = "everythingjazz"
SHOP_NAME = "Everything Jazz EU"
SHOP_DOMAIN = "eustore.everythingjazz.com"
SHOP_COUNTRY = "NL"
BASE_URL = "https://eustore.everythingjazz.com"
COLLECTION_HANDLE = "alle-produkte"
COLLECTION_JSON_URL = f"{BASE_URL}/collections/{COLLECTION_HANDLE}/products.json"
SAFE_DEFAULT_PAGE_SIZE = 50
SAFE_MAX_PAGE_SIZE = 100
DEFAULT_MAX_PRODUCTS = 2500
DEFAULT_MAX_RUNTIME_SECONDS = 900
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; Vinylofy-USF/1.0; +https://vinylofy.com)"
)
PREORDER_PATTERN = re.compile(r"\bpre[\s-]?order\b", flags=re.IGNORECASE)


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def normalize_product_url(handle: object) -> str:
    value = clean(handle).strip("/")
    if not value or "/" in value:
        return ""
    return f"{BASE_URL}/products/{value}"


def source_product_id_from_url(source_url: str) -> str | None:
    handle = urlparse(source_url).path.rstrip("/").split("/")[-1]
    return handle[:240] if handle else None


def normalize_money(value: object) -> str | None:
    text = clean(value)
    if not text:
        return None
    text = text.replace("€", "").replace("EUR", "").strip()
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    match = re.fullmatch(r"\d+(?:\.\d{1,2})?", text)
    if not match:
        return None
    try:
        amount = Decimal(text).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None
    if amount < 0:
        return None
    return f"{amount:.2f}"


def cents_key(value: str) -> int:
    return int((Decimal(value) * 100).quantize(Decimal("1")))


def product_tags(product: dict[str, Any]) -> list[str]:
    raw = product.get("tags")
    if isinstance(raw, list):
        return [clean(item) for item in raw if clean(item)]
    if isinstance(raw, str):
        return [clean(item) for item in raw.split(",") if clean(item)]
    return []


def is_preorder(product: dict[str, Any]) -> bool:
    haystack = " ".join(
        [clean(product.get("title")), *product_tags(product)]
    )
    return bool(PREORDER_PATTERN.search(haystack))


def select_variant_price(
    product: dict[str, Any],
) -> tuple[str | None, str | None, int, int]:
    variants = product.get("variants")
    if not isinstance(variants, list):
        return None, None, 0, 0

    priced: list[tuple[str, str | None, bool]] = []
    for variant in variants:
        if not isinstance(variant, dict):
            continue
        price = normalize_money(variant.get("price"))
        if price is None:
            continue
        compare_at = normalize_money(variant.get("compare_at_price"))
        if compare_at is not None and cents_key(compare_at) <= cents_key(price):
            compare_at = None
        priced.append((price, compare_at, variant.get("available") is True))

    available = [item for item in priced if item[2]]
    candidates = available or priced
    if not candidates:
        return None, None, len(available), len(variants)

    selected = min(candidates, key=lambda item: cents_key(item[0]))
    return selected[0], selected[1], len(available), len(variants)


def parse_product(
    product: dict[str, Any],
    *,
    page: int,
    seen_at: datetime,
) -> tuple[DiscoveredLink | None, ListingOffer | None, str | None]:
    product_type = clean(product.get("product_type"))
    if not is_everythingjazz_vinyl_type(product_type):
        return None, None, "unsupported_product_type"

    handle = clean(product.get("handle"))
    source_url = normalize_product_url(handle)
    if not source_url:
        return None, None, "missing_handle"

    title = clean(product.get("title"))
    vendor = clean(product.get("vendor"))
    price, compare_at, available_variant_count, variant_count = select_variant_price(
        product
    )
    orderable = available_variant_count > 0
    availability = (
        "preorder" if orderable and is_preorder(product) else
        "in_stock" if orderable else
        "out_of_stock"
    )

    payload: dict[str, Any] = {
        "source": "everythingjazz_collection_products_json",
        "collection_handle": COLLECTION_HANDLE,
        "collection_page": page,
        "shopify_product_id": product.get("id"),
        "handle": handle,
        "artist": vendor or None,
        "title": title or None,
        "product_type": product_type,
        "format": canonical_vinyl_format(product_type),
        "tags": product_tags(product),
        "price": price,
        "compare_at_price": compare_at,
        "is_sale": compare_at is not None,
        "currency": "EUR",
        "availability": availability,
        "orderable": orderable,
        "available_variant_count": available_variant_count,
        "variant_count": variant_count,
        "price_source": "collection_products_json",
        "publish_eligible": bool(orderable and price is not None),
        "listing_seen_at": seen_at.isoformat(),
    }

    link = DiscoveredLink(
        shop_id=SHOP_ID,
        source_url=source_url,
        source_product_id=source_product_id_from_url(source_url),
        payload=payload,
    )
    offer = None
    if price is not None:
        offer = ListingOffer(
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
    return link, offer, None


def listing_url_for_page(page: int, limit: int) -> str:
    return f"{COLLECTION_JSON_URL}?{urlencode({'limit': limit, 'page': page})}"


def compact_product(product: dict[str, Any]) -> dict[str, Any]:
    """Bewaar uitsluitend velden die de listingparser daadwerkelijk gebruikt."""
    variants: list[dict[str, Any]] = []
    raw_variants = product.get("variants")
    if isinstance(raw_variants, list):
        for variant in raw_variants:
            if not isinstance(variant, dict):
                continue
            variants.append(
                {
                    "price": variant.get("price"),
                    "compare_at_price": variant.get("compare_at_price"),
                    "available": variant.get("available") is True,
                }
            )

    return {
        "id": product.get("id"),
        "handle": product.get("handle"),
        "title": product.get("title"),
        "vendor": product.get("vendor"),
        "product_type": product.get("product_type"),
        "tags": product.get("tags"),
        "variants": variants,
    }


def fetch_page(
    session: requests.Session,
    *,
    page: int,
    limit: int,
    timeout: int,
) -> list[dict[str, Any]]:
    url = listing_url_for_page(page, limit)
    with session.get(url, timeout=timeout) as response:
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"collection endpoint gaf geen geldige JSON: {url}"
            ) from exc

    raw_products = payload.get("products") if isinstance(payload, dict) else None
    if not isinstance(raw_products, list):
        raise RuntimeError(f"collection endpoint mist products-lijst: {url}")

    products = [
        compact_product(item)
        for item in raw_products
        if isinstance(item, dict)
    ]
    del raw_products
    del payload
    gc.collect()
    return products


def registry_active_count() -> int:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select count(*)
                from public.shop_product_links
                where shop_id = %s and status = 'active'
                """,
                (SHOP_ID,),
            )
            return int(cur.fetchone()[0])


def load_registry_offers() -> list[ListingOffer]:
    offers: list[ListingOffer] = []
    with db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
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
            rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        payload = row.get("payload")
        if not isinstance(payload, dict):
            continue
        source_url = clean(row.get("source_url"))
        price = normalize_money(payload.get("price"))
        if not source_url or price is None:
            continue
        availability = clean(payload.get("availability")).lower() or "unknown"
        offers.append(
            ListingOffer(
                shop_name=SHOP_NAME,
                shop_domain=SHOP_DOMAIN,
                shop_country=SHOP_COUNTRY,
                source_url=source_url,
                price=price,
                availability=availability,
                currency="EUR",
                raw=payload,
            )
        )
    return offers


def sync_offers(offers: list[ListingOffer], *, write: bool, label: str) -> None:
    if not offers:
        print("[EVERYTHINGJAZZ-LISTING]", {"sync": label, "offers": 0})
        return
    with db_connection() as conn:
        stats = sync_listing_offers(conn, offers, write=write)
    print(
        "[EVERYTHINGJAZZ-LISTING]",
        {"sync": label, **vars(stats), "write": write},
        flush=True,
    )


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="USF listing-, prijs- en voorraadrefresh voor Everything Jazz EU."
    )
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="0 = doorlopen tot een lege pagina; begrensde runs delisten nooit.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=SAFE_DEFAULT_PAGE_SIZE,
        help=(
            "Shopify-producten per request; hard begrensd op "
            f"{SAFE_MAX_PAGE_SIZE} om Codespaces en Actions te beschermen."
        ),
    )
    parser.add_argument("--sleep", type=float, default=0.25)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--max-page-failures", type=int, default=3)
    parser.add_argument(
        "--max-products",
        type=int,
        default=DEFAULT_MAX_PRODUCTS,
        help="Harde guard op het totale aantal ruwe producten per run.",
    )
    parser.add_argument(
        "--max-runtime-seconds",
        type=int,
        default=DEFAULT_MAX_RUNTIME_SECONDS,
        help="Harde runtimeguard; bij overschrijding worden geen writes gedaan.",
    )
    parser.add_argument(
        "--debug-products-per-page",
        type=int,
        default=3,
        help="Maximaal aantal productregels per pagina bij --debug.",
    )
    parser.add_argument(
        "--expected-min-links",
        type=int,
        default=1000,
        help="Alleen op volledige scans; guard tegen een ingestorte catalogus.",
    )
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument(
        "--output",
        default="output/usf-everythingjazz/listing-summary.json",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.start_page < 1:
        raise SystemExit("[ERROR] --start-page moet minimaal 1 zijn.")
    if args.max_pages < 0:
        raise SystemExit("[ERROR] --max-pages mag niet negatief zijn.")
    if not 1 <= args.page_size <= SAFE_MAX_PAGE_SIZE:
        raise SystemExit(
            "[ERROR] --page-size moet tussen 1 en "
            f"{SAFE_MAX_PAGE_SIZE} liggen."
        )
    if args.sleep < 0 or args.timeout < 1 or args.max_page_failures < 1:
        raise SystemExit("[ERROR] ongeldige timing- of failure-instelling.")
    if args.max_products < 1 or args.max_runtime_seconds < 1:
        raise SystemExit("[ERROR] scan- en runtimeguards moeten positief zijn.")
    if not 0 <= args.debug_products_per_page <= 20:
        raise SystemExit(
            "[ERROR] --debug-products-per-page moet tussen 0 en 20 liggen."
        )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json",
            "Accept-Language": "en-GB,en;q=0.9,nl;q=0.8",
            "Cache-Control": "no-cache",
        }
    )

    started_at = datetime.now(timezone.utc)
    links_by_url: dict[str, DiscoveredLink] = {}
    offers_by_url: dict[str, ListingOffer] = {}
    signatures: set[tuple[str, ...]] = set()
    excluded_by_reason: dict[str, int] = {}
    pages_fetched = 0
    page = args.start_page
    consecutive_failures = 0
    scan_completed_safely = False
    raw_products_seen = 0
    allowed_products_seen = 0
    missing_price = 0
    stop_reason = "not_started"

    while args.max_pages == 0 or pages_fetched < args.max_pages:
        elapsed_seconds = (
            datetime.now(timezone.utc) - started_at
        ).total_seconds()
        if elapsed_seconds >= args.max_runtime_seconds:
            stop_reason = "max_runtime_guard"
            print(
                "[EVERYTHINGJAZZ-LISTING][GUARD]",
                {
                    "reason": stop_reason,
                    "elapsed_seconds": round(elapsed_seconds, 2),
                    "max_runtime_seconds": args.max_runtime_seconds,
                },
                flush=True,
            )
            break
        url = listing_url_for_page(page, args.page_size)
        print(
            "[EVERYTHINGJAZZ-LISTING]",
            {"page": page, "url": url, "write": args.write},
            flush=True,
        )
        try:
            products = fetch_page(
                session,
                page=page,
                limit=args.page_size,
                timeout=args.timeout,
            )
        except (requests.RequestException, RuntimeError) as exc:
            consecutive_failures += 1
            print(
                "[EVERYTHINGJAZZ-LISTING][WARN]",
                {
                    "page": page,
                    "error": str(exc),
                    "consecutive_failures": consecutive_failures,
                },
                flush=True,
            )
            if consecutive_failures >= args.max_page_failures:
                stop_reason = "max_page_failures"
                break
            time.sleep(args.sleep)
            continue

        consecutive_failures = 0
        if not products:
            scan_completed_safely = True
            stop_reason = "empty_page"
            print(
                "[EVERYTHINGJAZZ-LISTING] lege JSON-pagina; volledige scan klaar",
                {"page": page},
                flush=True,
            )
            break

        signature = tuple(
            sorted(clean(item.get("handle")) for item in products if clean(item.get("handle")))
        )
        if not signature:
            raise SystemExit(
                "[ERROR] JSON-pagina bevat producten maar geen enkele bruikbare handle."
            )
        if signature in signatures:
            scan_completed_safely = False
            stop_reason = "repeated_page_guard"
            print(
                "[EVERYTHINGJAZZ-LISTING][GUARD] herhaalde JSON-pagina; scan gestopt",
                {"page": page},
                flush=True,
            )
            break
        signatures.add(signature)

        raw_page_count = len(products)
        if raw_products_seen + raw_page_count > args.max_products:
            stop_reason = "max_products_guard"
            print(
                "[EVERYTHINGJAZZ-LISTING][GUARD]",
                {
                    "reason": stop_reason,
                    "raw_products_seen": raw_products_seen,
                    "next_page_products": raw_page_count,
                    "max_products": args.max_products,
                },
                flush=True,
            )
            del products
            gc.collect()
            break
        final_partial_page = raw_page_count < args.page_size

        page_links = 0
        page_offers = 0
        for product in products:
            raw_products_seen += 1
            link, offer, exclusion = parse_product(
                product,
                page=page,
                seen_at=started_at,
            )
            if exclusion:
                excluded_by_reason[exclusion] = excluded_by_reason.get(exclusion, 0) + 1
                continue
            if link is None:
                continue
            allowed_products_seen += 1
            page_links += 1
            links_by_url[link.source_url] = link
            if offer is None:
                missing_price += 1
            else:
                page_offers += 1
                offers_by_url[offer.source_url] = offer
            if (
                args.debug
                and page_links <= args.debug_products_per_page
            ):
                print(
                    "[EVERYTHINGJAZZ-LISTING-DEBUG]",
                    link.payload,
                    flush=True,
                )

        pages_fetched += 1
        print(
            "[EVERYTHINGJAZZ-LISTING-PAGE]",
            {
                "page": page,
                "raw_products": raw_page_count,
                "vinyl_links": page_links,
                "price_offers": page_offers,
                "total_vinyl_links": len(links_by_url),
            },
            flush=True,
        )
        del products
        gc.collect()

        if final_partial_page:
            scan_completed_safely = True
            stop_reason = "partial_last_page"
            break

        page += 1
        if args.max_pages and pages_fetched >= args.max_pages:
            scan_completed_safely = False
            stop_reason = "max_pages_limit"
            break
        time.sleep(args.sleep)

    session.close()
    gc.collect()
    full_scan = (
        args.start_page == 1
        and scan_completed_safely
        and stop_reason in {"empty_page", "partial_last_page"}
    )
    price_coverage = (
        len(offers_by_url) / len(links_by_url) if links_by_url else 0.0
    )
    summary: dict[str, Any] = {
        "shop_id": SHOP_ID,
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "pages_fetched": pages_fetched,
        "page_size": args.page_size,
        "max_products": args.max_products,
        "max_runtime_seconds": args.max_runtime_seconds,
        "stop_reason": stop_reason,
        "elapsed_seconds": round(
            (datetime.now(timezone.utc) - started_at).total_seconds(), 2
        ),
        "raw_products_seen": raw_products_seen,
        "allowed_products_seen": allowed_products_seen,
        "links": len(links_by_url),
        "offers": len(offers_by_url),
        "missing_price": missing_price,
        "price_coverage": round(price_coverage, 4),
        "excluded_by_reason": excluded_by_reason,
        "scan_completed_safely": scan_completed_safely,
        "full_scan": full_scan,
        "write": args.write,
    }
    write_summary(Path(args.output), summary)
    print("[EVERYTHINGJAZZ-LISTING-SUMMARY]", summary, flush=True)

    if not links_by_url:
        raise SystemExit("[ERROR] geen Vinyl/Vinyl-Box-links gevonden.")
    if not offers_by_url:
        raise SystemExit("[ERROR] links gevonden, maar geen enkele listingprijs.")
    if price_coverage < 0.90:
        raise SystemExit(
            f"[ERROR] prijsdekking te laag: {price_coverage:.1%}; geen writes uitgevoerd."
        )
    if full_scan and len(links_by_url) < args.expected_min_links:
        raise SystemExit(
            "[ERROR] volledige scan bleef onder de minimumguard: "
            f"{len(links_by_url)} < {args.expected_min_links}."
        )

    if not args.write:
        print(
            "[EVERYTHINGJAZZ-LISTING] dry-run compleet; geen databasewrites.",
            flush=True,
        )
        return 0

    previous_active = registry_active_count()
    registry_result = upsert_discovered_links(list(links_by_url.values()))
    print(
        "[EVERYTHINGJAZZ-LISTING-REGISTRY]",
        {
            "previous_active": previous_active,
            "inserted": registry_result.inserted,
            "updated": registry_result.updated,
            "total": registry_result.total,
        },
        flush=True,
    )
    sync_offers(list(offers_by_url.values()), write=True, label="current_listing")

    decline_guard_ok = not (
        previous_active >= 100 and len(links_by_url) < int(previous_active * 0.60)
    )
    if full_scan and decline_guard_ok:
        delist_result = mark_missing_links_out_of_stock(
            shop_id=SHOP_ID,
            seen_source_urls=links_by_url.keys(),
            run_started_at=started_at,
            write=True,
        )
        print("[EVERYTHINGJAZZ-LISTING-MISSING]", delist_result, flush=True)
        sync_offers(
            load_registry_offers(),
            write=True,
            label="registry_after_missing_delist",
        )
    else:
        reason = (
            "catalog_decline_guard"
            if full_scan and not decline_guard_ok
            else "scan_not_proven_complete"
        )
        print(
            "[EVERYTHINGJAZZ-LISTING] missing-link-delisting overgeslagen",
            {"reason": reason, "previous_active": previous_active},
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
