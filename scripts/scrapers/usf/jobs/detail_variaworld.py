#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from scripts.scrapers.usf.core.link_registry import (
    get_links_for_detail_scrape,
    insert_raw_shop_scrape,
    mark_detail_scraped,
)

SHOP_ID = "variaworld"
BASE_URL = "https://www.variaworld.nl"

HEADERS = {
    "User-Agent": "Mozilla/5.0 Vinylofy Variaworld detail enrichment",
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_ean(value: object) -> str | None:
    digits = re.sub(r"\D", "", str(value or ""))
    if 8 <= len(digits) <= 14:
        return digits
    return None


def payload_dict(value: object) -> dict:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}
    return {}


def get_listing_price(link: dict) -> str | None:
    payload = payload_dict(link.get("payload"))
    value = payload.get("price")
    if value in (None, ""):
        return None
    return str(value).replace(",", ".").strip() or None


def extract_title(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    if soup.title:
        title = clean(soup.title.get_text(" ", strip=True))
        title = re.sub(r"\s*\|\s*Variaworld\.nl\s*$", "", title, flags=re.I)
        return title or None

    h1 = soup.find("h1")
    if h1:
        return clean(h1.get_text(" ", strip=True)) or None

    return None


def extract_ean(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    for block in soup.select(".detail_tekstblok, .detail_tekst_container, .detail_container, #content"):
        text = clean(block.get_text(" ", strip=True))
        match = re.search(r"\bEAN\s*:\s*(\d{8,14})\b", text, flags=re.I)
        if match:
            return normalize_ean(match.group(1))

    patterns = [
        r"\bEAN\s*:\s*(\d{8,14})\b",
        r"\b(?:EAN|GTIN|Barcode|Streepjescode|UPC)\D{0,80}(\d{8,14})\b",
        r"<title>.*?\b(\d{8,14})\b.*?</title>",
    ]

    for pattern in patterns:
        match = re.search(pattern, html, flags=re.I | re.S)
        if match:
            ean = normalize_ean(match.group(1))
            if ean:
                return ean

    return None


def extract_image_url(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        '.detail_foto_groot_container img[name="imagegroot"][src]',
        ".detail_foto_groot_container img[src]",
        "a#Zoomer[href]",
        ".header_img img[src]",
    ]

    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue

        src = node.get("src") or node.get("href")
        if src and "/customized/img/" not in src:
            return urljoin(BASE_URL, src)

    match = re.search(r'/(fotogroot/[^"\']+_voorzijde\.jpg)', html, flags=re.I)
    if match:
        return urljoin(BASE_URL, "/" + match.group(1))

    return None


def extract_availability(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    text = clean(soup.select_one("#content").get_text(" ", strip=True)) if soup.select_one("#content") else clean(soup.get_text(" ", strip=True))
    lower = text.lower()

    if any(token in lower for token in ("uitverkocht", "niet leverbaar", "sold out", "out of stock")):
        return "out_of_stock"

    if "leverbaar vanaf" in lower or "pre-order" in lower or "preorder" in lower:
        return "preorder"

    if "levertijd" in lower or "toevoegen aan winkelwagen" in lower:
        return "in_stock"

    return None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch Variaworld detail pages into raw_shop_scrapes.")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--sleep", type=float, default=1.5)
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")
    if args.sleep < 0:
        raise SystemExit("[ERROR] --sleep mag niet negatief zijn.")

    links = get_links_for_detail_scrape(SHOP_ID, limit=args.limit)
    print(f"[DETAIL] queued={len(links)}", flush=True)

    session = requests.Session()
    session.headers.update(HEADERS)

    for idx, link in enumerate(links, start=1):
        url = link["source_url"]
        print(f"[DETAIL] {idx}/{len(links)} {url}", flush=True)

        try:
            response = session.get(url, timeout=30)
        except requests.RequestException as exc:
            print(f"[DETAIL][WARN] request failed url={url} error={exc}", flush=True)
            continue

        if response.status_code == 429:
            print("[DETAIL][WARN] HTTP 429, stopping safely.", flush=True)
            break

        if response.status_code in (404, 410):
            print(f"[DETAIL][SKIP] dead link status={response.status_code} url={url}", flush=True)
            mark_detail_scraped(link["id"])
            continue

        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            print(f"[DETAIL][WARN] HTTP error url={url} error={exc}", flush=True)
            continue

        response.encoding = response.apparent_encoding or response.encoding or "utf-8"
        html = response.text

        title = extract_title(html)
        ean_raw = extract_ean(html)
        price_raw = get_listing_price(link)  # listing-first: nooit detailprijs als actuele prijs gebruiken
        availability_raw = extract_availability(html)
        image_url_raw = extract_image_url(html)

        raw_id = insert_raw_shop_scrape(
            run_id=None,
            shop_id=SHOP_ID,
            source_url=url,
            source_product_id=link.get("source_product_id"),
            title_raw=title,
            ean_raw=ean_raw,
            price_raw=price_raw,
            availability_raw=availability_raw,
            image_url_raw=image_url_raw,
            payload={
                "html_length": len(html),
                "status_code": response.status_code,
                "source": "detail_variaworld",
                "detail_price_policy": "no_current_price_from_detail_page",
                "listing_payload": payload_dict(link.get("payload")),
            },
        )

        mark_detail_scraped(link["id"])
        print("[DETAIL] stored", {
            "raw_id": raw_id,
            "ean": ean_raw,
            "price_from_listing": price_raw,
            "availability": availability_raw,
            "image": image_url_raw,
        }, flush=True)

        time.sleep(args.sleep)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
