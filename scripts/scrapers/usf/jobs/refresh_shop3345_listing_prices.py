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
PRICE_AUTHORITY = "listing_html_only"

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
        html_price = extract_price(card)
        price = html_price
        price_transport = (
            "html"
            if html_price is not None
            else None
        )

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
            "listing_price_transport": price_transport,
            "html_listing_price": html_price,
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
                    "html_price": html_price,
                    "price_transport": price_transport,
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


def _3345_market(html: str) -> dict[str, str | None]:
    """Lees alleen expliciet actieve Shopify-marketwaarden.

    Generieke JSON-velden met valuta zijn onbruikbaar, omdat de
    storefront ook lijsten met beschikbare valuta bevat.
    """
    values: dict[str, str | None] = {
        "country": None,
        "currency": None,
    }

    patterns = {
        "country": (
            r"(?:window\.)?Shopify\.country\s*=\s*"
            r"['\"]([A-Z]{2})",
        ),
        "currency": (
            r"(?:window\.)?Shopify\.currency\.active\s*=\s*"
            r"['\"]([A-Z]{3})",
        ),
    }

    for key, candidates in patterns.items():
        for pattern in candidates:
            match = re.search(pattern, html)

            if match:
                values[key] = match.group(1)
                break

    soup = BeautifulSoup(html, "html.parser")
    html_node = soup.find("html")

    values["html_lang"] = (
        clean(html_node.get("lang"))
        if isinstance(html_node, Tag)
        else None
    )

    return values


def _3345_metrics(html: str) -> dict[str, object]:
    soup = BeautifulSoup(html, "html.parser")
    product_urls: list[str] = []
    seen_urls: set[str] = set()
    cards: dict[int, tuple[str, Tag]] = {}

    for anchor in soup.select('a[href*="/products/"]'):
        href = clean(anchor.get("href"))
        if not href:
            continue
        url = normalize_product_url(href)
        if url not in seen_urls:
            seen_urls.add(url)
            product_urls.append(url)
        card = find_product_card(anchor)
        if card is not None:
            cards.setdefault(id(card), (url, card))

    add_to_cart_cards = 0
    price_blocks = 0
    linked_prices: list[dict[str, str]] = []
    for url, card in cards.values():
        if has_active_add_to_cart(card):
            add_to_cart_cards += 1
        amount = extract_price(card)
        if amount is not None:
            price_blocks += 1
            if len(linked_prices) < 5:
                linked_prices.append({"url": url, "price": str(amount)})

    lower = html.lower()
    return {
        "html_bytes": len(html.encode("utf-8")),
        "product_cards": len(cards),
        "add_to_cart_cards": add_to_cart_cards,
        "price_blocks": price_blocks,
        "contains_euro": "€" in html or "&euro;" in lower or "eur" in lower,
        "first_product_urls": product_urls[:5],
        "first_linked_prices": linked_prices,
    }


def _3345_log_response(response: requests.Response, *, requested_url: str, purpose: str, page: int | None, attempt: int) -> dict[str, object]:
    redirects = [{"status": item.status_code, "url": item.url, "location": clean(item.headers.get("location"))} for item in response.history]
    headers = {name: clean(response.headers.get(name)) for name in ("content-type", "content-language", "cache-control", "cf-cache-status", "server", "vary", "x-request-id", "x-shopid", "x-shopify-stage", "x-sorting-hat-podid") if response.headers.get(name)}
    metrics = _3345_metrics(response.text)
    print("[3345-REQUEST]", {"purpose": purpose, "page": page, "attempt": attempt, "requested_url": requested_url}, flush=True)
    print("[3345-REDIRECT]", {"purpose": purpose, "page": page, "attempt": attempt, "history": redirects, "final_url": response.url, "final_query": urlparse(response.url).query}, flush=True)
    print("[3345-RESPONSE]", {"purpose": purpose, "page": page, "attempt": attempt, "status": response.status_code, "headers": headers, "response_cookie_names": sorted(response.cookies.keys()), "html_bytes": metrics["html_bytes"]}, flush=True)
    print("[3345-MARKET]", {"purpose": purpose, "page": page, "attempt": attempt, **_3345_market(response.text)}, flush=True)
    print("[3345-PRICE-MARKERS]", {"purpose": purpose, "page": page, "attempt": attempt, **metrics}, flush=True)
    return metrics


def _3345_save_html(response: requests.Response, name: str, *, debug: bool) -> None:
    if not debug:
        return
    directory = Path("output/usf-shop3345")
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / name
    target.write_text(response.text, encoding="utf-8")
    print("[3345-LISTING-HTML]", {"path": str(target), "bytes": len(response.content), "final_url": response.url}, flush=True)


def build_3345_session(*, debug: bool) -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)

    print(
        "[3345-SESSION]",
        {
            "user_agent": session.headers.get("User-Agent"),
            "accept": session.headers.get("Accept"),
            "accept_language": session.headers.get(
                "Accept-Language"
            ),
            "header_names": sorted(session.headers.keys()),
            "cookie_names_before": sorted(
                session.cookies.keys()
            ),
        },
        flush=True,
    )

    home_url = f"{BASE_URL}/"

    home_response = session.get(
        home_url,
        timeout=45,
        allow_redirects=True,
    )
    home_response.raise_for_status()

    _3345_log_response(
        home_response,
        requested_url=home_url,
        purpose="home_bootstrap",
        page=None,
        attempt=1,
    )
    _3345_save_html(
        home_response,
        "home-bootstrap.html",
        debug=debug,
    )

    localization_url = f"{BASE_URL}/localization"

    payload = {
        "_method": "PUT",
        "country_code": "NL",
        "language_code": "nl",
        "return_to": "/nl",
    }

    print(
        "[3345-LOCALIZATION]",
        {
            "url": localization_url,
            "method": "POST",
            "payload_fields": sorted(payload),
            "target_country": "NL",
            "target_language": "nl",
            "return_to": "/nl",
            "cookie_names_before": sorted(
                session.cookies.keys()
            ),
        },
        flush=True,
    )

    localized_response = session.post(
        localization_url,
        data=payload,
        headers={
            "Origin": BASE_URL,
            "Referer": home_response.url,
        },
        timeout=45,
        allow_redirects=True,
    )
    localized_response.raise_for_status()

    _3345_log_response(
        localized_response,
        requested_url=localization_url,
        purpose="localization_post",
        page=None,
        attempt=1,
    )
    _3345_save_html(
        localized_response,
        "localized-bootstrap.html",
        debug=debug,
    )

    market = _3345_market(localized_response.text)

    country = clean(
        market.get("country")
    ).upper()

    html_lang = clean(
        market.get("html_lang")
    ).lower()

    content_language = clean(
        localized_response.headers.get("content-language")
    ).lower()

    final_path = urlparse(
        localized_response.url
    ).path

    cart_currency_values = sorted({
        clean(cookie.value).upper()
        for cookie in session.cookies
        if (
            cookie.name == "cart_currency"
            and clean(cookie.value)
        )
    })

    localization_values = sorted({
        clean(cookie.value).upper()
        for cookie in session.cookies
        if (
            cookie.name == "localization"
            and clean(cookie.value)
        )
    })

    active_currency = (
        cart_currency_values[0]
        if len(cart_currency_values) == 1
        else None
    )

    print(
        "[3345-LOCALIZATION-RESULT]",
        {
            "final_url": localized_response.url,
            "final_path": final_path,
            "country": country or None,
            "currency": active_currency,
            "currency_source": "cart_currency_cookie",
            "cart_currency_values": cart_currency_values,
            "localization_values": localization_values,
            "content_language": content_language or None,
            "html_lang": html_lang or None,
            "cookie_names_after": sorted(
                session.cookies.keys()
            ),
        },
        flush=True,
    )

    if country != "NL":
        raise SystemExit(
            "[ERROR] Shopify-localization activeerde niet "
            f"de Nederlandse market: {market!r}"
        )

    if cart_currency_values != ["EUR"]:
        raise SystemExit(
            "[ERROR] Actieve cart_currency-cookie is niet "
            f"eenduidig EUR: {cart_currency_values!r}"
        )

    if not html_lang.startswith("nl"):
        raise SystemExit(
            "[ERROR] Shopify-localization activeerde niet "
            f"de Nederlandse HTML-taal: {market!r}"
        )

    if not content_language.startswith("nl"):
        raise SystemExit(
            "[ERROR] Shopify-localization gaf geen "
            f"Nederlandse Content-Language: "
            f"{content_language!r}"
        )

    if not final_path.startswith("/nl"):
        raise SystemExit(
            "[ERROR] Shopify-localization eindigde niet "
            f"op de Nederlandse route: "
            f"{localized_response.url!r}"
        )

    return session


def fetch_3345_listing(session: requests.Session, listing_url: str, *, page: int, debug: bool) -> requests.Response:
    response: requests.Response | None = None
    referer = f"{BASE_URL}/"
    for attempt in (1, 2):
        response = session.get(listing_url, headers={"Referer": referer}, timeout=45, allow_redirects=True)
        response.raise_for_status()
        metrics = _3345_log_response(response, requested_url=listing_url, purpose="listing", page=page, attempt=attempt)
        print("[3345-RESPONSE]", {"purpose": "listing_session", "page": page, "attempt": attempt, "session_cookie_names": sorted(session.cookies.keys())}, flush=True)
        _3345_save_html(response, f"listing-page-{page}-attempt-{attempt}.html", debug=debug)
        if int(metrics["product_cards"]) > 0 and int(metrics["price_blocks"]) > 0 and bool(metrics["contains_euro"]):
            break
        referer = response.url
        time.sleep(0.75)
    if response is None:
        raise RuntimeError("3345 listingrequest leverde geen response op")
    _3345_save_html(response, f"listing-page-{page}.html", debug=debug)
    return response

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

    session = build_3345_session(debug=args.debug)

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
            response = fetch_3345_listing(session, listing_url, page=page, debug=args.debug)

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

    transport_counts: dict[str, int] = {}
    non_html_price_urls: list[str] = []
    arctic_am: list[dict[str, object]] = []
    samples: list[tuple[str, object, str]] = []
    for offer in all_offers.values():
        raw = offer.raw if isinstance(offer.raw, dict) else {}
        mode = clean(raw.get("listing_price_transport")) or "missing"
        transport_counts[mode] = transport_counts.get(mode, 0) + 1
        if mode != "html":
            non_html_price_urls.append(offer.source_url)
        if len(samples) < 5:
            samples.append((offer.source_url, offer.price, mode))
        artist_key = clean(raw.get("artist")).lower()
        title_key = clean(raw.get("title")).lower()
        if artist_key == "arctic monkeys" and title_key in {"am", "arctic monkeys - am"}:
            arctic_am.append({"url": offer.source_url, "price": offer.price, "html_price": raw.get("html_listing_price"), "price_transport": mode})
    print("[3345-LISTING-SOURCE]", {"authority": PRICE_AUTHORITY, "transport_counts": transport_counts, "non_html_price_urls": non_html_price_urls[:10], "first_five_prices": samples}, flush=True)
    print("[3345-ARCTIC-AM]", {"status": "found" if arctic_am else "not_seen_in_scanned_pages", "matches": arctic_am}, flush=True)
    if non_html_price_urls:
        raise SystemExit("[ERROR] Een geprijsd 3345-offer gebruikt geen listing-HTML.")
    missing_publishable_html_prices = sorted(
        link.source_url
        for link in all_links.values()
        if isinstance(link.payload, dict)
        and link.payload.get("availability") == "in_stock"
        and bool(
            link.payload.get(
                "listing_cta_add_to_cart"
            )
        )
        and not bool(
            link.payload.get(
                "is_secondhand"
            )
        )
        and link.payload.get("price") is None
    )

    print(
        "[3345-HTML-PRICE-COVERAGE]",
        {
            "authority": PRICE_AUTHORITY,
            "missing_publishable_prices": len(
                missing_publishable_html_prices
            ),
            "sample": (
                missing_publishable_html_prices[:10]
            ),
        },
        flush=True,
    )

    if missing_publishable_html_prices:
        raise SystemExit(
            "[ERROR] De 3345-run bevat bestelbare LP's "
            "zonder zichtbare listingprijs. "
            "Er worden geen databasewrites uitgevoerd."
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
