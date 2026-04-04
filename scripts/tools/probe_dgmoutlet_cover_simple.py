#!/usr/bin/env python3
from __future__ import annotations

from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

LISTING_URL = "https://www.dgmoutlet.nl/muziek-films-games/muziek/lp/?order=name-asc&p=1"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}

IMAGE_KEYS = ["src", "data-src", "srcset", "data-srcset", "data-original"]


def first_non_empty(tag, keys):
    for key in keys:
        value = tag.get(key)
        if value:
            return key, value.strip()
    return None, None


def main() -> None:
    session = requests.Session()
    session.headers.update(HEADERS)

    listing_resp = session.get(LISTING_URL, timeout=30)
    listing_resp.raise_for_status()
    listing_soup = BeautifulSoup(listing_resp.text, "html.parser")

    cards = listing_soup.select("div.card.product-box.box-standard")

    print("=== LISTING ===")
    print(f"listing_url: {LISTING_URL}")
    print(f"status_code: {listing_resp.status_code}")
    print(f"card_count: {len(cards)}")

    if not cards:
        print("Geen cards gevonden.")
        return

    first_card = cards[0]

    name_link = first_card.select_one("a.product-name")
    image_link = first_card.select_one("a.product-image-link")

    product_href = ""
    if name_link and name_link.get("href"):
        product_href = name_link.get("href")
    elif image_link and image_link.get("href"):
        product_href = image_link.get("href")

    product_url = urljoin(LISTING_URL, product_href) if product_href else ""

    print("\n=== EERSTE CARD ===")
    print(f"title: {name_link.get_text(' ', strip=True) if name_link else ''}")
    print(f"product_url: {product_url}")

    print("\n=== LISTING IMAGE ATTRS ===")
    found_listing_image = False
    for tag in first_card.find_all(["img", "source"]):
        key, value = first_non_empty(tag, IMAGE_KEYS)
        if key and value:
            print(f"{tag.name}.{key}: {value}")
            found_listing_image = True

    if not found_listing_image:
        print("geen bruikbare image-attributen gevonden op de eerste listing card")

    if not product_url:
        print("\nGeen product_url gevonden, productpagina probe wordt overgeslagen.")
        return

    product_resp = session.get(product_url, timeout=30)
    product_resp.raise_for_status()
    product_soup = BeautifulSoup(product_resp.text, "html.parser")

    print("\n=== PRODUCTPAGINA ===")
    print(f"product_status_code: {product_resp.status_code}")
    print(f"product_url: {product_url}")

    og_image = product_soup.select_one('meta[property="og:image"]')
    twitter_image = product_soup.select_one('meta[name="twitter:image"]')

    print("\n=== META IMAGE FIELDS ===")
    print(f"og:image: {og_image.get('content', '').strip() if og_image else ''}")
    print(f"twitter:image: {twitter_image.get('content', '').strip() if twitter_image else ''}")

    print("\n=== JSON-LD IMAGE ===")
    json_ld_found = False
    for script in product_soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.get_text(strip=True)
        if '"image"' in raw:
            print(raw[:1000])
            json_ld_found = True
            break
    if not json_ld_found:
        print("geen json-ld image gevonden")

    print("\n=== EERSTE IMG/SOURCE OP PRODUCTPAGINA ===")
    found_product_image = False
    for tag in product_soup.find_all(["img", "source"]):
        key, value = first_non_empty(tag, IMAGE_KEYS)
        if key and value:
            print(f"{tag.name}.{key}: {value}")
            found_product_image = True
            break

    if not found_product_image:
        print("geen bruikbare img/source gevonden op productpagina")


if __name__ == "__main__":
    main()