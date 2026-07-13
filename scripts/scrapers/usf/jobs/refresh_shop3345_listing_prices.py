#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from psycopg.rows import dict_row

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.delist_missing_links import (
    mark_missing_links_out_of_stock,
)
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

BASE_URL = "https://3345.nl"
COLLECTION_URL = "https://3345.nl/nl/collections/all"

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
    "li.grid__item",
    ".product-grid-item",
    ".product-card-wrapper",
    ".card-wrapper",
    ".product-card",
    ".product-item",
    "article",
)

PRICE_SELECTORS = (
    # Actuele 3345-collectie:
    # <div data-product-bottom> ... <div>€24,99</div>
    "[data-product-bottom]",
    ".card__product-bottom",
    ".price__sale .price-item--sale",
    ".price-item--sale",
    ".price__current",
    ".product-item__price",
    ".product-card__price",
    ".price-item--regular",
    ".price",
    "[data-product-price]",
    "[data-price]",
)

SECONDHAND_ARTIST_MARKERS = (
    "3345 second hand",
    "3345 secondhand",
)

SECONDHAND_GENERAL_PATTERNS = (
    r"\bused\b",
    r"\bsecond[\s-]?hand\b",
)


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

    url = urljoin(BASE_URL, raw)
    parsed = urlparse(url)

    if parsed.netloc.lower() not in {"3345.nl", "www.3345.nl"}:
        return ""

    path = parsed.path.rstrip("/")

    # De collectie kan afwisselend /nl/products/... en /products/...
    # teruggeven. Intern gebruiken we één canonieke URL.
    if path.startswith("/nl/products/"):
        path = path[3:]

    if not path.startswith("/products/"):
        return ""

    return f"https://3345.nl{path}"


def source_product_id_from_url(source_url: str) -> str | None:
    slug = urlparse(source_url).path.rstrip("/").split("/")[-1]
    return slug[:240] if slug else None


def listing_url_for_page(page: int) -> str:
    """Gebruik de werkende Shopify-collectiepaginering.

    De eerder opgegeven 3345-filterparameters geven momenteel HTTP 404.
    Daarom halen we de gewone collectie op en dwingen we formaat en
    publiceerbaarheid lokaal per productkaart af.
    """
    query = urlencode(
        {
            "page": max(1, page),
        }
    )
    return f"{COLLECTION_URL}?{query}"


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

    match = re.search(r"(\d+(?:\.\d{1,2})?)", text)

    if not match:
        return None

    amount = match.group(1)

    if "." not in amount:
        return f"{amount}.00"

    whole, cents = amount.split(".", 1)
    return f"{whole}.{cents[:2].ljust(2, '0')}"


def find_product_card(anchor: Tag) -> Tag | None:
    """Vind de volledige 3345-productkaart.

    Titel, vendor, formaat, prijs en quick-add kunnen in verschillende
    zusterblokken binnen dezelfde kaart staan. Daarom stoppen we niet bij
    de eerste ancestor met alleen Add to cart. We geven voorrang aan de
    kleinste unieke productcontainer die [data-product-bottom] bevat.
    """
    unique_candidates: list[Tag] = []

    for depth, parent in enumerate(anchor.parents):
        if depth > 14:
            break

        if not isinstance(parent, Tag):
            continue

        product_urls = {
            normalize_product_url(node.get("href"))
            for node in parent.select("a[href*='/products/']")
            if isinstance(node, Tag)
        }
        product_urls.discard("")

        if len(product_urls) != 1:
            continue

        unique_candidates.append(parent)

        # Dit is de concrete commerciële onderkant uit de actuele
        # 3345-collectiebron: formaat en prijs staan hierin.
        if parent.select_one("[data-product-bottom]"):
            return parent

    # Tweede voorkeur: een volledige card-wrapper met prijs én CTA.
    for parent in unique_candidates:
        parent_text = clean(
            parent.get_text(" ", strip=True)
        ).lower()

        has_price = bool(
            re.search(
                r"(?:€|eur)\s*\d+(?:[.,]\d{1,2})?",
                parent_text,
                flags=re.IGNORECASE,
            )
        )

        has_availability = any(
            marker in parent_text
            for marker in (
                "add to cart",
                "add to chart",
                "toevoegen aan winkelwagen",
                "in winkelwagen",
                "sold out",
                "out of stock",
                "coming soon",
                "pre-order",
                "pre order",
                "preorder",
            )
        )

        if has_price and has_availability:
            return parent

    # Alleen als noodfallback de grootste nog unieke productcontainer.
    if unique_candidates:
        return unique_candidates[-1]

    return None

def node_is_hidden_or_disabled(node: Tag) -> bool:
    classes = {
        clean(value).lower()
        for value in (node.get("class") or [])
    }

    return any(
        (
            node.has_attr("disabled"),
            clean(node.get("aria-disabled")).lower() == "true",
            clean(node.get("aria-hidden")).lower() == "true",
            "disabled" in classes,
            "hidden" in classes,
        )
    )


def has_active_add_to_cart(card: Tag) -> bool:
    card_text = clean(
        card.get_text(" ", strip=True)
    ).lower()

    # Op 3345 kan de quick-addtekst buiten het feitelijke button-element
    # staan, maar wel binnen dezelfde productkaart.
    if any(
        marker in card_text
        for marker in (
            "add to cart",
            "add to chart",
            "toevoegen aan winkelwagen",
            "in winkelwagen",
        )
    ):
        if not any(
            marker in card_text
            for marker in (
                "sold out",
                "out of stock",
                "uitverkocht",
                "niet op voorraad",
                "coming soon",
            )
        ):
            return True

    selectors = (
        "form[action*='/cart/add'] button",
        "button[name='add']",
        "button",
        "input[type='submit']",
        "a.button",
    )

    for selector in selectors:
        for node in card.select(selector):
            if not isinstance(node, Tag):
                continue

            if node_is_hidden_or_disabled(node):
                continue

            node_text = clean(
                node.get_text(" ", strip=True)
                or node.get("value")
                or node.get("aria-label")
            ).lower()

            if any(
                marker in node_text
                for marker in (
                    "add to cart",
                    "add to chart",
                    "toevoegen aan winkelwagen",
                    "in winkelwagen",
                )
            ):
                return True

    return False


def detect_source_availability(card: Tag) -> str:
    text = clean(card.get_text(" ", strip=True)).lower()

    if any(
        marker in text
        for marker in (
            "coming soon",
            "pre-order",
            "pre order",
            "preorder",
            "binnenkort verwacht",
        )
    ):
        return "preorder"

    if any(
        marker in text
        for marker in (
            "sold out",
            "out of stock",
            "uitverkocht",
            "niet op voorraad",
        )
    ):
        return "out_of_stock"

    if has_active_add_to_cart(card):
        return "in_stock"

    # Alleen een actieve Add to cart bewijst bestelbaarheid.
    return "out_of_stock"


def detect_secondhand(
    *,
    artist: str | None,
    title: str | None,
    card_text: str,
    source_url: str,
) -> bool:
    artist_text = clean(artist).lower()

    if any(
        marker in artist_text
        for marker in SECONDHAND_ARTIST_MARKERS
    ):
        return True

    combined = " ".join(
        (
            clean(artist),
            clean(title),
            clean(card_text),
            clean(source_url),
        )
    ).lower()

    return any(
        re.search(pattern, combined, flags=re.IGNORECASE)
        for pattern in SECONDHAND_GENERAL_PATTERNS
    )


def extract_artist(card: Tag) -> str | None:
    selectors = (
        ".card-vendor-wrapper .vendor-link",
        "a.vendor-link",
        "[data-vendor]",
        ".product-card__vendor",
        ".card-information .caption-with-letter-spacing",
        ".vendor",
        ".product-vendor",
    )

    for selector in selectors:
        node = card.select_one(selector)

        if not isinstance(node, Tag):
            continue

        value = clean(
            node.get("data-vendor")
            or node.get_text(" ", strip=True)
        )

        if value:
            return value

    return None


def extract_title(anchor: Tag, card: Tag) -> str | None:
    candidates = (
        anchor.get("aria-label"),
        anchor.get("title"),
        anchor.get_text(" ", strip=True),
    )

    for candidate in candidates:
        value = clean(candidate)

        if value:
            return value

    for selector in (
        ".card__heading",
        ".product-card__title",
        ".product-item__title",
        "h3",
        "h2",
    ):
        node = card.select_one(selector)

        if isinstance(node, Tag):
            value = clean(node.get_text(" ", strip=True))

            if value:
                return value

    return None


def extract_listing_format(card: Tag) -> str | None:
    """Lees het zichtbare fysieke formaat uit de productkaart.

    We accepteren LP en meervoudige LP-sets zoals 2XLP en 3XLP.
    CD, cassette, 7-inch, 10-inch, 12-inch, merchandise en accessoires
    worden niet als LP gepubliceerd.
    """
    texts: list[str] = []

    selectors = (
        "[data-format]",
        ".product-format",
        ".card__format",
        ".product-card__format",
        ".product-item__format",
        ".caption-with-letter-spacing",
        ".card-information",
    )

    for selector in selectors:
        for node in card.select(selector):
            if not isinstance(node, Tag):
                continue

            value = clean(
                node.get("data-format")
                or node.get_text(" ", strip=True)
            )

            if value:
                texts.append(value)

    # De volledige kaarttekst is alleen een laatste lokale bron.
    texts.append(clean(card.get_text(" ", strip=True)))

    for value in texts:
        match = re.search(
            r"(?<![A-Z0-9])(?:(\d+)\s*[X×]\s*)?LP(?![A-Z0-9])",
            value.upper(),
        )

        if not match:
            continue

        quantity = match.group(1)

        if quantity:
            return f"{quantity}XLP"

        return "LP"

    return None


def extract_price(card: Tag) -> str | None:
    """Lees uitsluitend de productprijs uit de unieke 3345-kaart."""

    # Primaire actuele bron:
    #
    # <div class="card__product-bottom ..." data-product-bottom>
    #   <div>7"</div>
    #   <div>€24,99</div>
    # </div>
    product_bottom = card.select_one(
        "[data-product-bottom]"
    )

    if isinstance(product_bottom, Tag):
        bottom_text = clean(
            product_bottom.get_text(" ", strip=True)
        )

        match = re.search(
            r"(?:€|EUR)\s*(\d+(?:[.,]\d{1,2})?)",
            bottom_text,
            flags=re.IGNORECASE,
        )

        if match:
            return normalize_price(match.group(1))

    # Ondersteun ook dezelfde class wanneer het data-attribuut wijzigt.
    product_bottom = card.select_one(
        ".card__product-bottom"
    )

    if isinstance(product_bottom, Tag):
        bottom_text = clean(
            product_bottom.get_text(" ", strip=True)
        )

        match = re.search(
            r"(?:€|EUR)\s*(\d+(?:[.,]\d{1,2})?)",
            bottom_text,
            flags=re.IGNORECASE,
        )

        if match:
            return normalize_price(match.group(1))

    # Bestaande Shopify-prijsstructuren als gecontroleerde fallback.
    for selector in PRICE_SELECTORS:
        for node in card.select(selector):
            if not isinstance(node, Tag):
                continue

            if node_is_hidden_or_disabled(node):
                continue

            values = (
                node.get("data-product-price"),
                node.get("data-price"),
                node.get("content"),
                node.get_text(" ", strip=True),
            )

            for value in values:
                value_text = clean(value)

                match = re.search(
                    r"(?:€|EUR)\s*(\d+(?:[.,]\d{1,2})?)",
                    value_text,
                    flags=re.IGNORECASE,
                )

                if match:
                    return normalize_price(
                        match.group(1)
                    )

                # Alleen expliciete prijsattributen mogen zonder €-teken.
                if (
                    node.get("data-product-price") is not None
                    or node.get("data-price") is not None
                    or node.get("content") is not None
                ):
                    price = normalize_price(value)

                    if price:
                        return price

    # Laatste fallback blijft lokaal binnen de bewezen unieke productkaart.
    card_text = clean(
        card.get_text(" ", strip=True)
    )

    matches = list(
        re.finditer(
            r"(?:€|EUR)\s*(\d+(?:[.,]\d{1,2})?)",
            card_text,
            flags=re.IGNORECASE,
        )
    )

    for match in matches:
        context = clean(
            card_text[
                max(0, match.start() - 80):
                min(len(card_text), match.end() + 80)
            ]
        ).lower()

        # Nooit de algemene verzenddrempel als productprijs gebruiken.
        if any(
            marker in context
            for marker in (
                "free shipping",
                "gratis verzending",
                "orders over",
                "bestellingen vanaf",
                "shipping",
                "verzend",
            )
        ):
            continue

        return normalize_price(match.group(1))

    return None

def public_availability(
    *,
    source_availability: str,
    secondhand: bool,
    add_to_cart: bool,
) -> str:
    if secondhand:
        return "out_of_stock"

    if source_availability == "in_stock" and add_to_cart:
        return "in_stock"

    return "out_of_stock"


def parse_listing_page(
    html: str,
    *,
    page: int,
    listing_url: str,
    seen_at: datetime,
    debug: bool,
) -> tuple[list[DiscoveredLink], list[ListingOffer]]:
    soup = BeautifulSoup(html, "html.parser")

    links_by_url: dict[str, DiscoveredLink] = {}
    offers_by_url: dict[str, ListingOffer] = {}

    position = 0
    examined_urls: set[str] = set()

    for anchor in soup.select("a[href*='/products/']"):
        if not isinstance(anchor, Tag):
            continue

        source_url = normalize_product_url(anchor.get("href"))

        if not source_url or source_url in examined_urls:
            continue

        # Ook niet-LP-producten direct markeren als onderzocht, zodat meerdere
        # afbeelding-/titelanchors niet tot dubbele skipregels leiden.
        examined_urls.add(source_url)

        card = find_product_card(anchor)

        if card is None:
            continue

        title = extract_title(anchor, card)

        if not title:
            continue

        artist = extract_artist(card)

        # Fallback voor themes waar vendor niet via een eigen class staat.
        if not artist and " - " in title:
            possible_artist, possible_title = title.split(" - ", 1)

            if clean(possible_artist) and clean(possible_title):
                artist = clean(possible_artist)

        card_text = clean(card.get_text(" ", strip=True))
        listing_format = extract_listing_format(card)

        # De normale collectie bevat ook cd's, singles, merchandise,
        # accessoires en cadeaubonnen. Alleen LP-formaten gaan verder.
        if listing_format is None:
            if debug:
                print(
                    "[3345-LISTING-SKIP]",
                    {
                        "page": page,
                        "url": source_url,
                        "title": title,
                        "reason": "not_lp",
                    },
                    flush=True,
                )
            continue

        add_to_cart = has_active_add_to_cart(card)
        source_availability = detect_source_availability(card)
        secondhand = detect_secondhand(
            artist=artist,
            title=title,
            card_text=card_text,
            source_url=source_url,
        )
        availability = public_availability(
            source_availability=source_availability,
            secondhand=secondhand,
            add_to_cart=add_to_cart,
        )
        price = extract_price(card)

        position += 1

        payload: dict[str, Any] = {
            "source": "shop3345_all_collection_local_lp_filter",
            "discovery_url": listing_url,
            "page": page,
            "listing_position": position,
            "artist": artist,
            "title": title,
            "format": listing_format,
            "price": price,
            "currency": "EUR",
            "price_source": "listing",
            "source_availability": source_availability,
            "availability": availability,
            "listing_cta_add_to_cart": add_to_cart,
            "is_secondhand": secondhand,
            "publish_eligible": (
                availability == "in_stock"
                and not secondhand
                and add_to_cart
                and price is not None
            ),
            "listing_seen_at": seen_at.isoformat(),
        }

        link = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=source_product_id_from_url(source_url),
            payload=payload,
        )
        links_by_url[source_url] = link

        # Alle prijsdragende records gaan naar de sync, dus ook sold out,
        # coming soon en tweedehands. Daardoor worden bestaande publieke
        # aanbiedingen daadwerkelijk out_of_stock.
        if price is not None:
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

        if debug and position <= 30:
            print(
                "[3345-LISTING-DEBUG]",
                {
                    "page": page,
                    "position": position,
                    "url": source_url,
                    "artist": artist,
                    "title": title,
                    "format": listing_format,
                    "price": price,
                    "source_availability": source_availability,
                    "availability": availability,
                    "add_to_cart": add_to_cart,
                    "is_secondhand": secondhand,
                },
                flush=True,
            )

    return list(links_by_url.values()), list(offers_by_url.values())


def load_registry_offers() -> list[ListingOffer]:
    """Lees registryregels terug na missing-link-delisting.

    Hierdoor wordt een registryregel die tijdens deze run op out_of_stock is
    gezet ook naar een reeds bestaande publieke price-row gesynchroniseerd.
    """
    with db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
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
            rows = [dict(row) for row in cur.fetchall()]

    offers: list[ListingOffer] = []

    for row in rows:
        payload = row.get("payload")

        if not isinstance(payload, dict):
            continue

        source_url = normalize_product_url(row.get("source_url"))
        price = normalize_price(payload.get("price"))

        if not source_url or price is None:
            continue

        secondhand = bool(payload.get("is_secondhand"))
        source_availability = clean(
            payload.get("source_availability")
            or payload.get("availability")
        ).lower()
        add_to_cart = bool(payload.get("listing_cta_add_to_cart"))

        availability = public_availability(
            source_availability=source_availability,
            secondhand=secondhand,
            add_to_cart=add_to_cart,
        )

        # mark_missing_links_out_of_stock zet payload.availability expliciet op
        # out_of_stock. Dat heeft altijd voorrang.
        if clean(payload.get("availability")).lower() == "out_of_stock":
            availability = "out_of_stock"

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


def sync_offers(
    offers: list[ListingOffer],
    *,
    write: bool,
    label: str,
) -> None:
    if not offers:
        print(
            "[3345-LISTING]",
            {"sync": label, "offers": 0},
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
            "USF listing-, prijs- en voorraadflow voor 3345."
        )
    )
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help=(
            "0 = doorlopen tot een lege of herhaalde pagina. "
            "Een begrensde run delist nooit ontbrekende links."
        ),
    )
    parser.add_argument("--sleep", type=float, default=0.35)
    parser.add_argument("--max-page-failures", type=int, default=3)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--write", action="store_true")
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
    signatures: set[tuple[str, ...]] = set()

    page = args.start_page
    pages_fetched = 0
    consecutive_failures = 0
    scan_completed_safely = False

    while args.max_pages == 0 or pages_fetched < args.max_pages:
        listing_url = listing_url_for_page(page)

        print(
            "[3345-LISTING]",
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
            )
            response.raise_for_status()

            if args.debug and page == args.start_page:
                debug_dir = Path("output/usf-shop3345")
                debug_dir.mkdir(parents=True, exist_ok=True)

                html_path = debug_dir / "listing-page-1.html"
                html_path.write_text(
                    response.text,
                    encoding="utf-8",
                )

                print(
                    "[3345-LISTING-HTML]",
                    {
                        "path": str(html_path),
                        "bytes": len(response.content),
                        "contains_euro": "€" in response.text,
                        "contains_product_bottom": (
                            "data-product-bottom" in response.text
                        ),
                        "contains_2499": (
                            "24,99" in response.text
                            or "24.99" in response.text
                        ),
                    },
                    flush=True,
                )

        except requests.RequestException as exc:
            consecutive_failures += 1

            print(
                "[3345-LISTING][WARN]",
                {
                    "page": page,
                    "error": str(exc),
                    "consecutive_failures": consecutive_failures,
                },
                flush=True,
            )

            if consecutive_failures >= args.max_page_failures:
                break

            time.sleep(args.sleep)
            continue

        consecutive_failures = 0

        links, offers = parse_listing_page(
            response.text,
            page=page,
            listing_url=listing_url,
            seen_at=run_started_at,
            debug=args.debug,
        )

        signature = tuple(
            sorted(link.source_url for link in links)
        )

        if not signature:
            print(
                "[3345-LISTING] lege pagina; volledige scan klaar",
                {"page": page},
                flush=True,
            )
            scan_completed_safely = True
            break

        if signature in signatures:
            print(
                "[3345-LISTING] herhaalde pagina; volledige scan klaar",
                {"page": page},
                flush=True,
            )
            scan_completed_safely = True
            break

        signatures.add(signature)

        for link in links:
            all_links[link.source_url] = link

        for offer in offers:
            all_offers[offer.source_url] = offer

        pages_fetched += 1

        print(
            "[3345-LISTING-PAGE]",
            {
                "page": page,
                "links": len(links),
                "price_offers": len(offers),
                "total_links": len(all_links),
            },
            flush=True,
        )

        page += 1

        if args.max_pages and pages_fetched >= args.max_pages:
            # Een kunstmatige limiet bewijst nooit cataloguscompleetheid.
            scan_completed_safely = False
            break

        time.sleep(args.sleep)

    print(
        "[3345-LISTING-SUMMARY]",
        {
            "pages_fetched": pages_fetched,
            "links": len(all_links),
            "price_offers": len(all_offers),
            "scan_completed_safely": scan_completed_safely,
            "write": args.write,
        },
        flush=True,
    )

    if all_links and not all_offers:
        raise SystemExit(
            "[ERROR] De 3345-scan vond productlinks maar geen enkele "
            "listingprijs. De ontvangen collectie-HTML bevat vermoedelijk "
            "niet de normale prijsdragende productkaarten."
        )

    if not all_links:
        raise SystemExit(
            "[ERROR] De 3345-scan leverde geen productlinks op."
        )

    if not args.write:
        print(
            "[3345-LISTING] dry-run compleet; "
            "geen registry-, prijs- of delistingwrites.",
            flush=True,
        )
        return 0

    registry_result = upsert_discovered_links(
        list(all_links.values())
    )

    print(
        "[3345-LISTING-REGISTRY]",
        {
            "inserted": registry_result.inserted,
            "updated": registry_result.updated,
            "total": registry_result.total,
        },
        flush=True,
    )

    # Synchroniseer alle kaarten met een prijs, inclusief expliciete
    # out-of-stockkaarten en tweedehands.
    sync_offers(
        list(all_offers.values()),
        write=True,
        label="current_listing",
    )

    if scan_completed_safely:
        delist_result = mark_missing_links_out_of_stock(
            shop_id=SHOP_ID,
            seen_source_urls=all_links.keys(),
            run_started_at=run_started_at,
            write=True,
        )

        print(
            "[3345-LISTING-MISSING]",
            delist_result,
            flush=True,
        )

        # Belangrijk: de generieke missing-link-helper wijzigt de registry.
        # Daarna worden alle registryprijzen nogmaals gesynchroniseerd, zodat
        # bestaande public.prices-records eveneens out_of_stock worden.
        registry_offers = load_registry_offers()

        sync_offers(
            registry_offers,
            write=True,
            label="registry_after_missing_delist",
        )
    else:
        print(
            "[3345-LISTING] missing-link-delisting overgeslagen; "
            "de scan was niet aantoonbaar volledig.",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
