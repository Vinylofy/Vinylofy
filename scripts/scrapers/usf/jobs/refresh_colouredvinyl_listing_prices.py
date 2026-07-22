#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

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


SHOP_ID = "colouredvinyl"
SHOP_NAME = "Coloured Vinyl"
SHOP_DOMAIN = "colouredvinyl.nl"
SHOP_COUNTRY = "NL"

BASE_URL = "https://www.colouredvinyl.nl"
CATALOG_URL = f"{BASE_URL}/vinyl/"

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

CARD_SELECTORS = (
    "ul.products > li.product",
    "li.product",
    ".products .product",
    "article.product",
)


def clean(value: object) -> str:
    return re.sub(
        r"\s+",
        " ",
        str(value or "").replace("\xa0", " "),
    ).strip()


def listing_url_for_page(page: int) -> str:
    if page <= 1:
        return CATALOG_URL
    return f"{CATALOG_URL}page/{page}/"


def normalize_product_url(value: object) -> str:
    raw = clean(value)
    if not raw:
        return ""

    parsed = urlparse(urljoin(BASE_URL, raw))

    if parsed.netloc.lower() not in {
        "colouredvinyl.nl",
        "www.colouredvinyl.nl",
    }:
        return ""

    parts = [part for part in parsed.path.split("/") if part]

    # Productpagina:
    # /vinyl/{artiest}/{product}/
    #
    # Categorieën als /vinyl/soundtracks/ hebben maar twee segmenten
    # en worden hierdoor uitgesloten.
    if len(parts) < 3 or parts[0].lower() != "vinyl":
        return ""

    if parts[1].lower() == "page":
        return ""

    return f"{BASE_URL}/{'/'.join(parts)}/"


def source_product_id_from_url(source_url: str) -> str | None:
    parts = [
        part
        for part in urlparse(source_url).path.split("/")
        if part
    ]

    if len(parts) < 3:
        return None

    # Gebruik artiest + productslug om gelijke albumslugs
    # van verschillende artiesten te onderscheiden.
    value = "/".join(parts[1:])
    return value[:240] if value else None


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

    match = re.search(
        r"(\d{1,3}(?:[.\s]\d{3})*(?:,\d{1,2})"
        r"|\d+(?:[.,]\d{1,2})?)",
        text,
    )

    if not match:
        return None

    amount = match.group(1).replace(" ", "")

    if "," in amount:
        amount = amount.replace(".", "").replace(",", ".")
    elif amount.count(".") > 1:
        amount = amount.replace(".", "")

    if "." not in amount:
        return f"{amount}.00"

    whole, cents = amount.split(".", 1)
    return f"{whole}.{cents[:2].ljust(2, '0')}"


def node_price(node: Tag | None) -> str | None:
    if not isinstance(node, Tag):
        return None

    for candidate in node.select(
        ".woocommerce-Price-amount, bdi"
    ):
        price = normalize_price(
            candidate.get_text(" ", strip=True)
        )
        if price:
            return price

    return normalize_price(
        node.get_text(" ", strip=True)
    )


def extract_prices(
    card: Tag,
) -> tuple[str | None, str | None]:
    price_container = card.select_one(".price")

    if not isinstance(price_container, Tag):
        return None, None

    # WooCommerce zet bij aanbiedingen:
    # del = oorspronkelijke prijs
    # ins = actuele aanbiedingsprijs
    sale_node = price_container.select_one("ins")
    old_node = price_container.select_one("del")

    if isinstance(sale_node, Tag):
        return (
            node_price(sale_node),
            node_price(old_node),
        )

    return node_price(price_container), None


def extract_title(
    card: Tag,
    anchor: Tag,
) -> str | None:
    for selector in (
        "h2.woocommerce-loop-product__title",
        ".woocommerce-loop-product__title",
        ".product-title",
        "h2",
        "h3",
    ):
        node = card.select_one(selector)

        if isinstance(node, Tag):
            value = clean(
                node.get_text(" ", strip=True)
            )
            if value:
                return value

    for value in (
        anchor.get("aria-label"),
        anchor.get("title"),
        anchor.get_text(" ", strip=True),
    ):
        cleaned = clean(value)
        if cleaned:
            return cleaned

    return None


def extract_artist(card: Tag) -> str | None:
    for selector in (
        ".product-artist",
        ".artist",
        ".woocommerce-loop-product__artist",
        "[data-artist]",
    ):
        node = card.select_one(selector)

        if not isinstance(node, Tag):
            continue

        value = clean(
            node.get("data-artist")
            or node.get_text(" ", strip=True)
        )

        if value:
            return value

    return None


def extract_woocommerce_product_id(
    card: Tag,
) -> str | None:
    candidates: list[str] = []

    for class_name in card.get("class", []):
        match = re.fullmatch(
            r"post-(\\d+)",
            str(class_name),
        )

        if match:
            candidates.append(
                match.group(1)
            )

    for node in card.select(
        "[data-product_id]"
    ):
        value = clean(
            node.get("data-product_id")
        )

        if value.isdigit():
            candidates.append(value)

    unique = list(dict.fromkeys(candidates))
    return unique[0] if len(unique) == 1 else None


def extract_listing_sku(
    card: Tag,
) -> str | None:
    values = [
        clean(node.get("data-product_sku"))
        for node in card.select(
            "[data-product_sku]"
        )
        if clean(
            node.get("data-product_sku")
        )
    ]

    unique = list(dict.fromkeys(values))
    return unique[0] if len(unique) == 1 else None


def extract_listing_image_alt(
    card: Tag,
) -> str | None:
    for image in card.select("img"):
        value = clean(image.get("alt"))

        if value:
            return value

    return None


def detect_availability(
    card: Tag,
) -> tuple[str, str]:
    text = clean(
        card.get_text(" ", strip=True)
    )
    lowered = text.lower()

    if any(
        marker in lowered
        for marker in (
            "uitverkocht",
            "out of stock",
            "sold out",
        )
    ):
        return (
            "out_of_stock",
            "explicit_out_of_stock",
        )

    if any(
        marker in lowered
        for marker in (
            "pre-order",
            "preorder",
            "pre order",
        )
    ):
        return (
            "preorder",
            "explicit_preorder",
        )

    # Op deze catalogus worden uitverkochte producten
    # expliciet gemarkeerd. Een zichtbare kaart zonder dat
    # kenmerk wordt daarom als bestelbaar behandeld.
    return (
        "in_stock",
        "visible_without_out_of_stock_marker",
    )


def find_cards(
    soup: BeautifulSoup,
) -> list[Tag]:
    best_cards: list[Tag] = []
    best_urls: set[str] = set()

    for selector in CARD_SELECTORS:
        cards = [
            node
            for node in soup.select(selector)
            if isinstance(node, Tag)
        ]

        urls: set[str] = set()
        selected_cards: list[Tag] = []

        for card in cards:
            card_urls = {
                normalize_product_url(
                    anchor.get("href")
                )
                for anchor in card.select("a[href]")
                if isinstance(anchor, Tag)
            }
            card_urls.discard("")

            if not card_urls:
                continue

            urls.update(card_urls)
            selected_cards.append(card)

        if len(urls) > len(best_urls):
            best_urls = urls
            best_cards = selected_cards

    return best_cards


def extract_declared_last_page(
    soup: BeautifulSoup,
) -> int | None:
    numbers: set[int] = set()

    selectors = (
        ".woocommerce-pagination .page-numbers, "
        "nav.woocommerce-pagination a, "
        "nav.woocommerce-pagination span"
    )

    for node in soup.select(selectors):
        if not isinstance(node, Tag):
            continue

        text_match = re.fullmatch(
            r"\s*(\d+)\s*",
            node.get_text(" ", strip=True),
        )

        if text_match:
            numbers.add(
                int(text_match.group(1))
            )

        href = clean(node.get("href"))
        href_match = re.search(
            r"/page/(\d+)/?",
            href,
        )

        if href_match:
            numbers.add(
                int(href_match.group(1))
            )

    return max(numbers) if numbers else None


def parse_listing_page(
    html: str,
    *,
    page: int,
    listing_url: str,
    seen_at: datetime,
    debug: bool,
) -> tuple[
    list[DiscoveredLink],
    list[ListingOffer],
    dict[str, Any],
]:
    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    cards = find_cards(soup)
    declared_last_page = (
        extract_declared_last_page(soup)
    )

    links_by_url: dict[
        str,
        DiscoveredLink,
    ] = {}

    offers_by_url: dict[
        str,
        ListingOffer,
    ] = {}

    missing_price_urls: list[str] = []
    skipped_cards: list[dict[str, Any]] = []
    duplicate_cards: list[dict[str, Any]] = []
    availability_counts: dict[str, int] = {}
    sale_count = 0

    for position, card in enumerate(
        cards,
        start=1,
    ):
        all_anchors = [
            anchor
            for anchor in card.select("a[href]")
            if isinstance(anchor, Tag)
        ]

        normalized_by_anchor = [
            (
                anchor,
                normalize_product_url(
                    anchor.get("href")
                ),
            )
            for anchor in all_anchors
        ]

        normalized_urls = sorted(
            {
                normalized
                for _, normalized in normalized_by_anchor
                if normalized
            }
        )

        raw_hrefs = [
            clean(anchor.get("href"))
            for anchor in all_anchors
            if clean(anchor.get("href"))
        ]

        if len(normalized_urls) != 1:
            skip_record = {
                "page": page,
                "position": position,
                "reason": (
                    "no_product_url"
                    if not normalized_urls
                    else "multiple_product_urls"
                ),
                "normalized_urls": normalized_urls,
                "raw_hrefs": raw_hrefs[:20],
                "card_classes": (
                    card.get("class")
                    or []
                ),
                "card_text": clean(
                    card.get_text(
                        " ",
                        strip=True,
                    )
                )[:800],
            }

            skipped_cards.append(
                skip_record
            )

            if debug:
                print(
                    "[COLOUREDVINYL-LISTING-SKIP]",
                    json.dumps(
                        skip_record,
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )

            continue

        source_url = normalized_urls[0]

        anchor = next(
            anchor
            for anchor, normalized
            in normalized_by_anchor
            if normalized == source_url
        )

        card_title = extract_title(
            card,
            anchor,
        )
        card_artist = extract_artist(card)

        (
            card_price,
            card_original_price,
        ) = extract_prices(card)

        (
            card_availability,
            card_availability_evidence,
        ) = detect_availability(card)

        card_product_id = (
            extract_woocommerce_product_id(
                card
            )
        )
        card_sku = extract_listing_sku(
            card
        )
        card_image_alt = (
            extract_listing_image_alt(
                card
            )
        )

        availability_counts[
            card_availability
        ] = (
            availability_counts.get(
                card_availability,
                0,
            )
            + 1
        )

        if card_original_price is not None:
            sale_count += 1

        if source_url in links_by_url:
            existing_link = (
                links_by_url[source_url]
            )
            existing_payload = dict(
                existing_link.payload
                if isinstance(
                    existing_link.payload,
                    dict,
                )
                else {}
            )

            existing_variants = (
                existing_payload.get(
                    "url_collision_variants"
                )
            )

            if not isinstance(
                existing_variants,
                list,
            ):
                existing_variants = [{
                    "page": (
                        existing_payload.get(
                            "page"
                        )
                    ),
                    "listing_position": (
                        existing_payload.get(
                            "listing_position"
                        )
                    ),
                    "woocommerce_product_id": (
                        existing_payload.get(
                            "woocommerce_product_id"
                        )
                    ),
                    "sku": (
                        existing_payload.get(
                            "source_sku"
                        )
                    ),
                    "artist": (
                        existing_payload.get(
                            "artist"
                        )
                    ),
                    "title": (
                        existing_payload.get(
                            "title"
                        )
                    ),
                    "price": (
                        existing_payload.get(
                            "price"
                        )
                    ),
                    "original_price": (
                        existing_payload.get(
                            "original_price"
                        )
                    ),
                    "availability": (
                        existing_payload.get(
                            "availability"
                        )
                    ),
                    "image_alt": (
                        existing_payload.get(
                            "image_alt"
                        )
                    ),
                }]

            new_variant = {
                "page": page,
                "listing_position": position,
                "woocommerce_product_id": (
                    card_product_id
                ),
                "sku": card_sku,
                "artist": card_artist,
                "title": card_title,
                "price": card_price,
                "original_price": (
                    card_original_price
                ),
                "availability": (
                    card_availability
                ),
                "image_alt": card_image_alt,
            }

            variant_keys = {
                json.dumps(
                    variant,
                    ensure_ascii=False,
                    sort_keys=True,
                )
                for variant in existing_variants
                if isinstance(variant, dict)
            }

            new_variant_key = json.dumps(
                new_variant,
                ensure_ascii=False,
                sort_keys=True,
            )

            if new_variant_key not in variant_keys:
                existing_variants.append(
                    new_variant
                )

            collision_pages = sorted({
                int(variant["page"])
                for variant in existing_variants
                if (
                    isinstance(variant, dict)
                    and isinstance(
                        variant.get("page"),
                        int,
                    )
                )
            })

            collision_payload: dict[
                str,
                Any,
            ] = {
                "source": (
                    "colouredvinyl_vinyl_"
                    "listing_html"
                ),
                "discovery_url": listing_url,
                "pages": collision_pages,
                "artist": (
                    existing_payload.get(
                        "artist"
                    )
                    or card_artist
                ),
                "title": (
                    existing_payload.get(
                        "title"
                    )
                    or card_title
                ),
                "price": None,
                "original_price": None,
                "currency": "EUR",
                "price_source": "listing",
                "availability": "unknown",
                "source_availability": (
                    "ambiguous"
                ),
                "availability_evidence": (
                    "shared_url_multiple_"
                    "woocommerce_products"
                ),
                "publish_eligible": False,
                "url_collision": True,
                "url_collision_count": len(
                    existing_variants
                ),
                "url_collision_variants": (
                    existing_variants
                ),
                "quarantine_reason": (
                    "shared_product_url_"
                    "multiple_variants"
                ),
                "listing_seen_at": (
                    seen_at.isoformat()
                ),
            }

            links_by_url[
                source_url
            ] = DiscoveredLink(
                shop_id=SHOP_ID,
                source_url=source_url,
                source_product_id=None,
                payload=collision_payload,
            )

            # Een gedeelde URL kan niet veilig
            # aan één prijs, SKU of EAN worden
            # gekoppeld. Verwijder daarom ook
            # de eerder opgebouwde offer.
            offers_by_url.pop(
                source_url,
                None,
            )

            collision_record = {
                "page": page,
                "position": position,
                "reason": (
                    "shared_product_url_"
                    "multiple_variants"
                ),
                "source_url": source_url,
                "variant_count": len(
                    existing_variants
                ),
                "variants": (
                    existing_variants
                ),
                "raw_hrefs": raw_hrefs[:20],
                "card_classes": (
                    card.get("class")
                    or []
                ),
            }

            duplicate_cards.append(
                collision_record
            )

            if debug:
                print(
                    "[COLOUREDVINYL-LISTING-"
                    "COLLISION]",
                    json.dumps(
                        collision_record,
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )

            continue

        title = card_title
        artist = card_artist
        price = card_price
        original_price = (
            card_original_price
        )
        availability = (
            card_availability
        )
        availability_evidence = (
            card_availability_evidence
        )

        payload: dict[str, Any] = {
            "source": (
                "colouredvinyl_vinyl_listing_html"
            ),
            "discovery_url": listing_url,
            "page": page,
            "listing_position": position,
            "artist": artist,
            "title": title,
            "woocommerce_product_id": (
                card_product_id
            ),
            "source_sku": card_sku,
            "image_alt": card_image_alt,
            "url_collision": False,
            "price": price,
            "original_price": original_price,
            "currency": "EUR",
            "price_source": "listing",
            "availability": availability,
            "source_availability": availability,
            "availability_evidence": (
                availability_evidence
            ),
            "publish_eligible": (
                availability == "in_stock"
                and price is not None
            ),
            "listing_seen_at": (
                seen_at.isoformat()
            ),
        }

        link = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=(
                card_product_id
                or source_product_id_from_url(
                    source_url
                )
            ),
            payload=payload,
        )

        links_by_url[source_url] = link

        # Ook expliciet uitverkochte kaarten gaan met
        # hun prijs naar de listing-price-sync, zodat een
        # bestaande aanbieding werkelijk out_of_stock wordt.
        if price is not None:
            offers_by_url[
                source_url
            ] = ListingOffer(
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

        elif availability == "in_stock":
            missing_price_urls.append(
                source_url
            )

        if (
            debug
            and len(links_by_url) <= 30
        ):
            print(
                "[COLOUREDVINYL-LISTING-SAMPLE]",
                json.dumps(
                    {
                        "page": page,
                        "position": position,
                        "url": source_url,
                        "artist": artist,
                        "title": title,
                        "price": price,
                        "original_price": (
                            original_price
                        ),
                        "availability": (
                            availability
                        ),
                        "availability_evidence": (
                            availability_evidence
                        ),
                        "card_classes": (
                            card.get("class")
                            or []
                        ),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ),
                flush=True,
            )

    diagnostics = {
        "raw_cards": len(cards),
        "links": len(links_by_url),
        "offers_with_price": (
            len(offers_by_url)
        ),
        "missing_in_stock_prices": (
            len(missing_price_urls)
        ),
        "missing_in_stock_price_sample": (
            missing_price_urls[:10]
        ),
        "availability": (
            availability_counts
        ),
        "sale_count": sale_count,
        "skipped_cards": len(
            skipped_cards
        ),
        "skipped_card_sample": (
            skipped_cards[:10]
        ),
        "duplicate_cards": len(
            duplicate_cards
        ),
        "duplicate_card_sample": (
            duplicate_cards[:10]
        ),
        "declared_last_page": (
            declared_last_page
        ),
    }

    return (
        list(links_by_url.values()),
        list(offers_by_url.values()),
        diagnostics,
    )


def load_registry_offers(
    *,
    seen_at: datetime,
) -> list[ListingOffer]:
    with db_connection() as conn:
        with conn.cursor(
            row_factory=dict_row
        ) as cur:
            cur.execute(
                """
                select
                  source_url,
                  payload
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
                for row in cur.fetchall()
            ]

    offers: list[ListingOffer] = []

    for row in rows:
        payload = row.get("payload")

        if not isinstance(
            payload,
            dict,
        ):
            continue

        source_url = normalize_product_url(
            row.get("source_url")
        )
        price = normalize_price(
            payload.get("price")
        )
        availability = (
            clean(
                payload.get("availability")
            ).lower()
            or "unknown"
        )

        if (
            not source_url
            or price is None
        ):
            continue

        if availability not in {
            "in_stock",
            "out_of_stock",
            "preorder",
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
                currency="EUR",
                seen_at=seen_at,
                raw=payload,
            )
        )

    return offers


def sync_offers(
    offers: list[ListingOffer],
    *,
    write: bool,
    label: str,
) -> None:
    if not offers:
        print(
            "[COLOUREDVINYL-LISTING-SYNC]",
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
        "[COLOUREDVINYL-LISTING-SYNC]",
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
            "USF listing-, prijs-, voorraad- en "
            "veilige delistingflow voor "
            "Coloured Vinyl."
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
        default=2,
        help=(
            "Veilige standaard: 2 pagina's. "
            "Gebruik 0 voor een volledige scan. "
            "Een begrensde scan delist nooit "
            "ontbrekende links."
        ),
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0.5,
    )

    parser.add_argument(
        "--max-page-failures",
        type=int,
        default=2,
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
            "[ERROR] --start-page moet "
            "minimaal 1 zijn."
        )

    if args.max_pages < 0:
        raise SystemExit(
            "[ERROR] --max-pages mag niet "
            "negatief zijn."
        )

    if args.sleep < 0:
        raise SystemExit(
            "[ERROR] --sleep mag niet "
            "negatief zijn."
        )

    if args.max_page_failures < 1:
        raise SystemExit(
            "[ERROR] --max-page-failures "
            "moet minimaal 1 zijn."
        )

    session = requests.Session()
    session.headers.update(HEADERS)

    run_started_at = datetime.now(
        timezone.utc
    )

    all_links: dict[
        str,
        DiscoveredLink,
    ] = {}

    all_offers: dict[
        str,
        ListingOffer,
    ] = {}

    signatures: set[
        tuple[str, ...]
    ] = set()

    page_diagnostics: list[
        dict[str, Any]
    ] = []

    page = args.start_page
    pages_fetched = 0
    consecutive_failures = 0
    declared_last_page: int | None = None
    scan_completed_safely = False
    stop_reason = "not_started"

    while (
        args.max_pages == 0
        or pages_fetched < args.max_pages
    ):
        listing_url = listing_url_for_page(
            page
        )

        print(
            "[COLOUREDVINYL-LISTING]",
            {
                "page": page,
                "url": listing_url,
                "write": args.write,
            },
            flush=True,
        )

        try:
            response = session.get(
                listing_url,
                timeout=45,
                allow_redirects=True,
            )

        except requests.RequestException as exc:
            consecutive_failures += 1

            print(
                "[COLOUREDVINYL-LISTING-WARN]",
                {
                    "page": page,
                    "error": str(exc),
                    "consecutive_failures": (
                        consecutive_failures
                    ),
                },
                flush=True,
            )

            if (
                consecutive_failures
                >= args.max_page_failures
            ):
                stop_reason = (
                    "request_failures"
                )
                break

            time.sleep(args.sleep)
            continue

        if response.status_code in {
            404,
            410,
        }:
            if (
                args.max_pages == 0
                and pages_fetched > 0
            ):
                scan_completed_safely = True
                stop_reason = (
                    f"http_{response.status_code}"
                    "_after_catalog"
                )
            else:
                stop_reason = (
                    f"http_{response.status_code}"
                )

            break

        try:
            response.raise_for_status()

        except requests.RequestException as exc:
            consecutive_failures += 1

            print(
                "[COLOUREDVINYL-LISTING-WARN]",
                {
                    "page": page,
                    "status": (
                        response.status_code
                    ),
                    "error": str(exc),
                    "consecutive_failures": (
                        consecutive_failures
                    ),
                },
                flush=True,
            )

            if (
                consecutive_failures
                >= args.max_page_failures
            ):
                stop_reason = "http_failures"
                break

            time.sleep(args.sleep)
            continue

        consecutive_failures = 0

        if args.debug:
            debug_dir = Path(
                "output/usf-colouredvinyl"
            )
            debug_dir.mkdir(
                parents=True,
                exist_ok=True,
            )

            html_path = (
                debug_dir
                / f"listing-page-{page}.html"
            )

            html_path.write_text(
                response.text,
                encoding="utf-8",
            )

            print(
                "[COLOUREDVINYL-LISTING-HTML]",
                {
                    "path": str(html_path),
                    "bytes": (
                        len(response.content)
                    ),
                    "final_url": response.url,
                },
                flush=True,
            )

        (
            links,
            offers,
            diagnostics,
        ) = parse_listing_page(
            response.text,
            page=page,
            listing_url=listing_url,
            seen_at=run_started_at,
            debug=args.debug,
        )

        diagnostics.update(
            {
                "page": page,
                "requested_url": (
                    listing_url
                ),
                "final_url": response.url,
                "status": (
                    response.status_code
                ),
            }
        )

        page_diagnostics.append(
            diagnostics
        )

        page_last = diagnostics.get(
            "declared_last_page"
        )

        if isinstance(page_last, int):
            declared_last_page = max(
                declared_last_page or 0,
                page_last,
            )

        signature = tuple(
            sorted(
                link.source_url
                for link in links
            )
        )

        if not signature:
            if (
                args.max_pages == 0
                and pages_fetched > 0
            ):
                scan_completed_safely = True
                stop_reason = (
                    "empty_page_after_catalog"
                )
            else:
                stop_reason = "empty_page"

            break

        if signature in signatures:
            if (
                args.max_pages == 0
                and pages_fetched > 0
            ):
                scan_completed_safely = True
                stop_reason = (
                    "repeated_product_set"
                )
            else:
                stop_reason = (
                    "repeated_product_set_"
                    "in_limited_run"
                )

            break

        signatures.add(signature)

        for link in links:
            all_links[
                link.source_url
            ] = link

        for offer in offers:
            all_offers[
                offer.source_url
            ] = offer

        pages_fetched += 1

        print(
            "[COLOUREDVINYL-LISTING-PAGE]",
            diagnostics,
            flush=True,
        )

        if (
            args.max_pages == 0
            and declared_last_page
            and page >= declared_last_page
        ):
            scan_completed_safely = True
            stop_reason = (
                "reached_declared_last_page"
            )
            break

        if (
            args.max_pages
            and pages_fetched
            >= args.max_pages
        ):
            stop_reason = (
                "artificial_page_limit"
            )
            scan_completed_safely = False
            break

        page += 1
        time.sleep(args.sleep)

    missing_in_stock_price_urls = sorted(
        link.source_url
        for link in all_links.values()
        if (
            isinstance(link.payload, dict)
            and link.payload.get(
                "availability"
            ) == "in_stock"
            and not link.payload.get("price")
        )
    )

    summary = {
        "pages_fetched": pages_fetched,
        "raw_cards": sum(
            int(item.get("raw_cards", 0))
            for item in page_diagnostics
        ),
        "links": len(all_links),
        "offers_with_price": (
            len(all_offers)
        ),
        "skipped_cards": sum(
            int(item.get("skipped_cards", 0))
            for item in page_diagnostics
        ),
        "url_collision_events": sum(
            int(item.get("duplicate_cards", 0))
            for item in page_diagnostics
        ),
        "sale_cards": sum(
            int(item.get("sale_count", 0))
            for item in page_diagnostics
        ),
        "declared_last_page": (
            declared_last_page
        ),
        "scan_completed_safely": (
            scan_completed_safely
        ),
        "stop_reason": stop_reason,
        "missing_in_stock_prices": (
            len(
                missing_in_stock_price_urls
            )
        ),
        "write": args.write,
    }

    print(
        "[COLOUREDVINYL-LISTING-SUMMARY]",
        summary,
        flush=True,
    )

    output_dir = Path(
        "output/usf-colouredvinyl"
    )
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    diagnostics_path = (
        output_dir
        / "listing-diagnostics.json"
    )

    diagnostics_path.write_text(
        json.dumps(
            {
                "summary": summary,
                "pages": page_diagnostics,
            },
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(
        "[COLOUREDVINYL-LISTING-DIAGNOSTICS]",
        {
            "path": str(
                diagnostics_path
            )
        },
        flush=True,
    )

    if not all_links:
        raise SystemExit(
            "[ERROR] Coloured Vinyl "
            "listingcrawl leverde geen "
            "productlinks op."
        )

    if missing_in_stock_price_urls:
        raise SystemExit(
            "[ERROR] De listingcrawl bevat "
            "zichtbare, niet-uitverkochte "
            "producten zonder prijs. "
            "Er worden geen databasewrites "
            "uitgevoerd. Voorbeelden: "
            f"{missing_in_stock_price_urls[:10]}"
        )

    if not all_offers:
        raise SystemExit(
            "[ERROR] De listingcrawl vond "
            "productlinks maar geen enkele "
            "prijs. Er worden geen "
            "databasewrites uitgevoerd."
        )

    if not args.write:
        print(
            "[COLOUREDVINYL-LISTING] "
            "dry-run compleet; geen registry-, "
            "prijs- of delistingwrites.",
            flush=True,
        )
        return 0

    registry_result = (
        upsert_discovered_links(
            list(all_links.values())
        )
    )

    print(
        "[COLOUREDVINYL-LISTING-REGISTRY]",
        {
            "inserted": (
                registry_result.inserted
            ),
            "updated": (
                registry_result.updated
            ),
            "total": (
                registry_result.total
            ),
        },
        flush=True,
    )

    sync_offers(
        list(all_offers.values()),
        write=True,
        label="current_listing",
    )

    if scan_completed_safely:
        delist_result = (
            mark_missing_links_out_of_stock(
                shop_id=SHOP_ID,
                seen_source_urls=(
                    all_links.keys()
                ),
                run_started_at=(
                    run_started_at
                ),
                write=True,
            )
        )

        print(
            "[COLOUREDVINYL-LISTING-MISSING]",
            delist_result,
            flush=True,
        )

        # De missing-link-helper wijzigt de
        # registry. Synchroniseer daarna opnieuw,
        # zodat bestaande publieke price-records
        # eveneens out_of_stock worden.
        registry_offers = (
            load_registry_offers(
                seen_at=run_started_at
            )
        )

        sync_offers(
            registry_offers,
            write=True,
            label=(
                "registry_after_missing_delist"
            ),
        )

    else:
        print(
            "[COLOUREDVINYL-LISTING] "
            "missing-link-delisting overgeslagen; "
            "de scan was niet aantoonbaar "
            "volledig.",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
