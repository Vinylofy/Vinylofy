from __future__ import annotations

import html
import json
import re
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

SHOP_ID = "musiconvinyl"
SHOP_NAME = "Music On Vinyl"
SHOP_DOMAIN = "musiconvinyl.com"
SHOP_COUNTRY = "NL"
CURRENCY = "EUR"
BASE_URL = "https://www.musiconvinyl.com"

DEFAULT_COLLECTIONS = (
    "all-products",
    "new-releases",
    "pre-order",
    "back-in-stock",
)

EAN_RE = re.compile(r"^\d{12,13}$")


@dataclass(frozen=True)
class MusicOnVinylProduct:
    source_url: str
    source_product_id: str
    title: str | None
    artist: str | None
    ean: str | None
    price: str | None
    regular_price: str | None
    sale_price: str | None
    price_source: str | None
    availability: str
    image_url: str | None
    payload: dict[str, Any]


def clean(value: Any) -> str | None:
    if value is None:
        return None
    text = html.unescape(str(value)).strip()
    text = re.sub(r"\s+", " ", text)
    return text or None


def cents_to_eur(value: Any) -> str | None:
    if value is None:
        return None
    try:
        cents = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return str((cents / Decimal("100")).quantize(Decimal("0.01")))


def parse_price_text(value: Any) -> str | None:
    text = clean(value)
    if not text:
        return None
    text = text.replace("\xa0", " ")
    match = re.search(r"(\d{1,5})(?:[.,](\d{2}))", text)
    if not match:
        return None
    euros, cents = match.groups()
    return f"{euros}.{cents}"


def normalize_ean_candidate(value: Any) -> str | None:
    text = clean(value)
    if not text:
        return None
    digits = re.sub(r"\D+", "", text)
    if EAN_RE.match(digits):
        return digits
    return None


def pick_variant(product: dict[str, Any]) -> dict[str, Any]:
    variants = product.get("variants") or []
    if not isinstance(variants, list) or not variants:
        return {}
    available = [v for v in variants if isinstance(v, dict) and v.get("available")]
    return available[0] if available else variants[0]


def infer_availability(product: dict[str, Any], variant: dict[str, Any], *, collection: str) -> str:
    haystack = " ".join(
        filter(
            None,
            [
                clean(collection),
                clean(product.get("title")),
                clean(product.get("body_html")),
                " ".join(str(t) for t in product.get("tags") or []),
                clean(variant.get("title")),
            ],
        )
    ).lower()

    if "pre-order" in haystack or "preorder" in haystack or collection == "pre-order":
        return "preorder"
    if variant and variant.get("available") is False:
        return "out_of_stock"
    if product.get("available") is False:
        return "out_of_stock"
    if variant.get("available") is True or product.get("available") is True:
        return "in_stock"
    return "unknown"


def choose_price(variant: dict[str, Any]) -> tuple[str | None, str | None, str | None, str | None]:
    current = cents_to_eur(variant.get("price"))
    compare_at = cents_to_eur(variant.get("compare_at_price"))

    if current and compare_at:
        try:
            if Decimal(current) < Decimal(compare_at):
                return current, compare_at, current, "shopify_variant_sale_price"
        except InvalidOperation:
            pass
        return current, compare_at, None, "shopify_variant_price"

    if current:
        return current, None, None, "shopify_variant_price"

    return None, None, None, None


def product_url(handle: str | None) -> str:
    return urljoin(BASE_URL, f"/products/{handle}") if handle else BASE_URL


def image_url(product: dict[str, Any]) -> str | None:
    images = product.get("images") or []
    if isinstance(images, list) and images:
        first = images[0]
        if isinstance(first, dict):
            return clean(first.get("src"))
        return clean(first)
    image = product.get("image")
    if isinstance(image, dict):
        return clean(image.get("src"))
    return clean(image)


def product_to_record(product: dict[str, Any], *, collection: str) -> MusicOnVinylProduct:
    variant = pick_variant(product)
    handle = clean(product.get("handle"))
    source_url = product_url(handle)
    title = clean(product.get("title"))
    artist = clean(product.get("vendor"))
    ean = normalize_ean_candidate(variant.get("barcode"))
    price, regular_price, sale_price, price_source = choose_price(variant)
    availability = infer_availability(product, variant, collection=collection)
    variant_id = clean(variant.get("id"))
    source_product_id = ean or variant_id or handle or clean(product.get("id")) or source_url

    payload = {
        "source": "musiconvinyl_collection_json",
        "collection": collection,
        "shopify_product_id": clean(product.get("id")),
        "shopify_handle": handle,
        "shopify_variant_id": variant_id,
        "variant_title": clean(variant.get("title")),
        "variant_available": variant.get("available"),
        "product_available": product.get("available"),
        "barcode": clean(variant.get("barcode")),
        "ean": ean,
        "artist": artist,
        "title": title,
        "regular_price": regular_price,
        "sale_price": sale_price,
        "chosen_price": price,
        "price_source": price_source,
        "availability": availability,
        "image_url": image_url(product),
        "tags": product.get("tags") or [],
        "product_type": clean(product.get("product_type")),
    }

    return MusicOnVinylProduct(
        source_url=source_url,
        source_product_id=source_product_id,
        title=title,
        artist=artist,
        ean=ean,
        price=price,
        regular_price=regular_price,
        sale_price=sale_price,
        price_source=price_source,
        availability=availability,
        image_url=image_url(product),
        payload=payload,
    )


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 VinylofyBot/1.0 (+https://vinylofy.com)",
            "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        }
    )
    return session


def fetch_collection_json(
    session: requests.Session,
    *,
    collection: str,
    page: int,
    limit: int,
    timeout: float,
) -> list[dict[str, Any]]:
    url = f"{BASE_URL}/collections/{collection}/products.json"
    response = session.get(url, params={"limit": limit, "page": page}, timeout=timeout)
    response.raise_for_status()
    data = response.json()
    products = data.get("products") or []
    if not isinstance(products, list):
        return []
    return [p for p in products if isinstance(p, dict)]


def discover_products(
    *,
    collections: tuple[str, ...] = DEFAULT_COLLECTIONS,
    max_pages: int = 1,
    limit: int = 250,
    timeout: float = 20.0,
    sleep: float = 0.25,
) -> list[MusicOnVinylProduct]:
    if max_pages < 1:
        raise ValueError("max_pages moet minimaal 1 zijn")
    session = make_session()
    by_url: dict[str, MusicOnVinylProduct] = {}

    for collection in collections:
        for page in range(1, max_pages + 1):
            products = fetch_collection_json(
                session,
                collection=collection,
                page=page,
                limit=limit,
                timeout=timeout,
            )
            print(
                "[MUSICONVINYL-DISCOVERY-PAGE]",
                {"collection": collection, "page": page, "products": len(products)},
                flush=True,
            )
            if not products:
                break
            for product in products:
                record = product_to_record(product, collection=collection)
                by_url[record.source_url] = record
                print(
                    "[MUSICONVINYL-LISTING] parsed",
                    {
                        "url": record.source_url,
                        "title": record.title,
                        "artist": record.artist,
                        "regular_price": record.regular_price,
                        "sale_price": record.sale_price,
                        "chosen_price": record.price,
                        "price_source": record.price_source,
                        "availability": record.availability,
                        "ean": record.ean,
                        "source_product_id": record.source_product_id,
                    },
                    flush=True,
                )
            if sleep:
                time.sleep(sleep)

    return list(by_url.values())


def fetch_product_js(source_url: str, *, timeout: float = 20.0) -> dict[str, Any] | None:
    handle = source_url.rstrip("/").split("/")[-1]
    if not handle:
        return None
    url = f"{BASE_URL}/products/{handle}.js"
    response = make_session().get(url, timeout=timeout)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    data = response.json()
    return data if isinstance(data, dict) else None
