from __future__ import annotations

import argparse
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "soundsvenlo"
SHOP_NAME = "Sounds Venlo"
SHOP_DOMAIN = "sounds-venlo.nl"
SHOP_COUNTRY = "NL"
BASE_URL = "https://www.sounds-venlo.nl"

SEED_URLS = [
    "https://www.sounds-venlo.nl/vinyl/",
    "https://www.sounds-venlo.nl/nieuwe-vinyl-releases/",
    "https://www.sounds-venlo.nl/verwachte-vinyl-releases/",
    "https://www.sounds-venlo.nl/lp-aanbiedingen/",
    "https://www.sounds-venlo.nl/record-store-day/",
]

VINYL_FORMAT_RE = re.compile(
    r'\b(?:[2-9]-)?LP\b|(?:^|\s)(?:7|10|12)"(?:\s|$)|\b(?:7|10|12)-?INCH\b|\bEP\b',
    flags=re.I,
)
NON_VINYL_FORMAT_RE = re.compile(r"\b(?:CD|DVD|BLU[\s-]?RAY|B\+C|CD3|CDS)\b", flags=re.I)
PRICE_RE = re.compile(r"€\s*([0-9]+(?:[.,][0-9]{1,2})?)")


def clean(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": BASE_URL + "/",
        }
    )

    try:
        session.get(BASE_URL + "/", timeout=30)
    except requests.RequestException:
        pass

    return session


def normalize_product_url(href: str) -> str:
    return urljoin(BASE_URL, href.split("#", 1)[0].split("?", 1)[0]).rstrip("/") + "/"


def is_product_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc != "www.sounds-venlo.nl":
        return False
    path = parsed.path.strip("/")
    return bool(re.match(r"^\d{4,}-.+", path))


def source_product_id_from_url(url: str) -> str | None:
    path = urlparse(url).path.strip("/")
    match = re.match(r"^(\d{4,})", path)
    return match.group(1) if match else None


def page_url(seed_url: str, page: int) -> str:
    base = seed_url.split("#", 1)[0].split("?", 1)[0].rstrip("/")
    if page <= 1:
        return base + "/"
    return f"{base}/p{page}/"


def normalize_price(value: Any) -> str | None:
    text = clean(value)
    if not text:
        return None

    match = PRICE_RE.search(text)
    if match:
        return match.group(1).replace(",", ".")

    text = text.replace("€", "").replace("EUR", "").replace("\xa0", " ").strip()
    text = text.replace(",", ".")
    if re.match(r"^[0-9]+(?:\.[0-9]{1,2})?$", text):
        return text
    return None


def extract_price(text: str) -> str | None:
    return normalize_price(text)


def extract_availability(text: str) -> str:
    lower = text.lower()

    if "uitverkocht" in lower or "sold out" in lower or "niet leverbaar" in lower:
        return "out_of_stock"
    if "verwacht" in lower or "pre-order" in lower or "preorder" in lower:
        return "preorder"
    if "op voorraad" in lower:
        return "in_stock"
    return "unknown"


def extract_format(text: str) -> str | None:
    match = VINYL_FORMAT_RE.search(text)
    if match:
        return clean(match.group(0)).upper().replace(" ", "")
    return None


def looks_like_vinyl(text: str) -> bool:
    has_vinyl = bool(VINYL_FORMAT_RE.search(text))
    has_non_vinyl = bool(NON_VINYL_FORMAT_RE.search(text))
    if has_vinyl:
        return True
    if has_non_vinyl:
        return False
    return False


def likely_product_container(anchor):
    node = anchor
    best = anchor.parent

    for _ in range(10):
        if node is None:
            break
        text = clean(node.get_text(" ", strip=True)) if hasattr(node, "get_text") else ""
        if "€" in text and len(text) < 900:
            best = node
        node = getattr(node, "parent", None)

    return best


def extract_artist(container_text: str, title: str) -> str | None:
    if not container_text or not title:
        return None

    idx = container_text.lower().find(title.lower())
    if idx <= 0:
        return None

    prefix = container_text[:idx]
    prefix = re.sub(r"^(alle resultaten voor\s+\"?\"?\s*)", "", prefix, flags=re.I)
    prefix = re.sub(r"\b(?:image|placeholder for)\b.*$", "", prefix, flags=re.I)
    prefix = clean(prefix)

    parts = [clean(part) for part in re.split(r"\s{2,}|\n|\r", prefix) if clean(part)]
    candidate = parts[-1] if parts else prefix
    candidate = clean(candidate)

    if not candidate:
        return None
    if len(candidate) > 120:
        return None
    if "€" in candidate:
        return None

    return candidate


def parse_listing_page(
    html: str,
    *,
    seed_url: str,
    listing_url: str,
    page: int,
    seen_at: datetime,
) -> list[DiscoveredLink]:
    soup = BeautifulSoup(html, "html.parser")

    links_by_url: dict[str, DiscoveredLink] = {}

    for position, anchor in enumerate(soup.select("a[href]"), start=1):
        href = clean(anchor.get("href"))
        if not href:
            continue

        source_url = normalize_product_url(href)
        if not is_product_url(source_url):
            continue

        title = clean(anchor.get_text(" ", strip=True))
        if not title:
            continue

        container = likely_product_container(anchor)
        container_text = clean(container.get_text(" ", strip=True)) if container else title

        price = extract_price(container_text)
        if not price:
            continue

        if not looks_like_vinyl(container_text):
            continue

        artist = extract_artist(container_text, title)
        fmt = extract_format(container_text)
        availability = extract_availability(container_text)

        source_product_id = source_product_id_from_url(source_url)

        payload = {
            "discovery_source": "soundsvenlo_listing",
            "seed_url": seed_url,
            "listing_url": listing_url,
            "page": page,
            "listing_position": position,
            "artist": artist,
            "title": title,
            "format": fmt,
            "price": price,
            "prijs": price,
            "price_source": "listing",
            "availability": availability,
            "listing_text": container_text[:800],
            "listing_seen_at": seen_at.isoformat(),
        }

        links_by_url[source_url] = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=source_product_id,
            payload=payload,
        )

    return list(links_by_url.values())


def discover_links(
    *,
    seed_limit: int = 0,
    max_pages_per_seed: int = 0,
    delay_seconds: float = 0.35,
    max_page_failures: int = 3,
) -> list[DiscoveredLink]:
    session = make_session()
    seed_urls = list(SEED_URLS[:seed_limit] if seed_limit and seed_limit > 0 else SEED_URLS)
    seen_at = datetime.now(timezone.utc)

    all_links_by_url: dict[str, DiscoveredLink] = {}
    total_pages = 0

    print(
        "[DISCOVER] config",
        {
            "shop": SHOP_ID,
            "seeds": len(seed_urls),
            "seed_limit": seed_limit,
            "max_pages_per_seed": max_pages_per_seed,
            "delay_seconds": delay_seconds,
            "max_page_failures": max_page_failures,
        },
        flush=True,
    )

    for seed_index, seed_url in enumerate(seed_urls, start=1):
        seen_page_signatures: set[tuple[str, ...]] = set()
        consecutive_failures = 0
        page = 1

        while True:
            if max_pages_per_seed and max_pages_per_seed > 0 and page > max_pages_per_seed:
                break

            listing_url = page_url(seed_url, page)
            print(
                "[DISCOVER] page",
                {
                    "seed_index": seed_index,
                    "seed_url": seed_url,
                    "page": page,
                    "url": listing_url,
                },
                flush=True,
            )

            try:
                response = session.get(listing_url, timeout=30)
            except requests.RequestException as exc:
                consecutive_failures += 1
                print(
                    "[DISCOVER][WARN] request_failed",
                    {
                        "url": listing_url,
                        "error": str(exc),
                        "consecutive_failures": consecutive_failures,
                    },
                    flush=True,
                )
                if consecutive_failures >= max_page_failures:
                    break
                page += 1
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
                continue

            if response.status_code == 403:
                print(
                    "[DISCOVER][WARN] HTTP 403 forbidden; Sounds Venlo blocks this runtime/request.",
                    {"url": listing_url, "status_code": response.status_code},
                    flush=True,
                )
                break

            if response.status_code == 429:
                print("[DISCOVER][WARN] HTTP 429, stopping safely.", {"url": listing_url}, flush=True)
                break

            if response.status_code >= 500:
                consecutive_failures += 1
                print(
                    "[DISCOVER][WARN] server_error",
                    {
                        "url": listing_url,
                        "status_code": response.status_code,
                        "consecutive_failures": consecutive_failures,
                    },
                    flush=True,
                )
                if consecutive_failures >= max_page_failures:
                    break
                page += 1
                if delay_seconds > 0:
                    time.sleep(delay_seconds)
                continue

            if response.status_code in (404, 410):
                print("[DISCOVER] stop_not_found", {"url": listing_url, "status_code": response.status_code}, flush=True)
                break

            response.raise_for_status()
            consecutive_failures = 0

            links = parse_listing_page(
                response.text,
                seed_url=seed_url,
                listing_url=listing_url,
                page=page,
                seen_at=seen_at,
            )

            page_signature = tuple(link.source_url for link in links)

            if not links:
                print("[DISCOVER] stop_empty", {"seed_url": seed_url, "page": page}, flush=True)
                break

            if page_signature and page_signature in seen_page_signatures:
                print(
                    "[DISCOVER] stop_duplicate_page",
                    {
                        "seed_url": seed_url,
                        "page": page,
                        "links": len(links),
                        "first_link": page_signature[0],
                        "last_link": page_signature[-1],
                    },
                    flush=True,
                )
                break

            if page_signature:
                seen_page_signatures.add(page_signature)

            new_urls = 0
            for link in links:
                if link.source_url not in all_links_by_url:
                    new_urls += 1
                all_links_by_url[link.source_url] = link

            total_pages += 1
            print(
                "[DISCOVER] page_result",
                {
                    "seed_url": seed_url,
                    "page": page,
                    "links": len(links),
                    "new_urls": new_urls,
                    "catalog_unique_total": len(all_links_by_url),
                },
                flush=True,
            )

            page += 1
            if delay_seconds > 0:
                time.sleep(delay_seconds)

    print(
        "[DISCOVER] summary",
        {"shop": SHOP_ID, "links": len(all_links_by_url), "pages": total_pages},
        flush=True,
    )

    return list(all_links_by_url.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover Sounds Venlo vinyl listing links for USF.")
    parser.add_argument("--seed-limit", type=int, default=0, help="Aantal seedcategorieën; 0 = alle seeds.")
    parser.add_argument("--max-pages-per-seed", type=int, default=1, help="Aantal pagina's per seed; 0 = tot leeg/dubbel.")
    parser.add_argument("--delay-seconds", type=float, default=0.35)
    parser.add_argument("--max-page-failures", type=int, default=3)
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--write", action="store_true", help="Schrijf links naar shop_product_links.")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.seed_limit < 0:
        raise SystemExit("[ERROR] --seed-limit mag niet negatief zijn.")
    if args.max_pages_per_seed < 0:
        raise SystemExit("[ERROR] --max-pages-per-seed mag niet negatief zijn.")
    if args.delay_seconds < 0:
        raise SystemExit("[ERROR] --delay-seconds mag niet negatief zijn.")
    if args.max_page_failures < 1:
        raise SystemExit("[ERROR] --max-page-failures moet minimaal 1 zijn.")
    if args.sample_size < 0:
        raise SystemExit("[ERROR] --sample-size mag niet negatief zijn.")

    links = discover_links(
        seed_limit=args.seed_limit,
        max_pages_per_seed=args.max_pages_per_seed,
        delay_seconds=args.delay_seconds,
        max_page_failures=args.max_page_failures,
    )

    for link in links[: args.sample_size]:
        print(
            "[DISCOVER] sample",
            {
                "source_url": link.source_url,
                "source_product_id": link.source_product_id,
                "artist": link.payload.get("artist"),
                "title": link.payload.get("title"),
                "format": link.payload.get("format"),
                "price": link.payload.get("price"),
                "availability": link.payload.get("availability"),
            },
            flush=True,
        )

    if not links:
        raise SystemExit("[ERROR] Sounds Venlo discovery leverde geen vinyl-links op.")

    if not args.write:
        print("[DISCOVER] dry-run complete; geen databasewrites.", flush=True)
        return 0

    result = upsert_discovered_links(links)
    print(
        "[DISCOVER] registered",
        {"inserted": result.inserted, "updated": result.updated, "total": result.total},
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
