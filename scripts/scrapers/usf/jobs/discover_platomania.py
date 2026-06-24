from __future__ import annotations

import argparse
import re
import time
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

from scripts.scrapers.legacy.platomania_legacy import (
    SEED_URLS,
    category_name_from_url,
    fetch_soup,
    make_session,
    page_url,
    parse_article,
)
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "platomania"
SHOP_NAME = "Platomania"
SHOP_DOMAIN = "platomania.nl"
SHOP_COUNTRY = "NL"


def clean(value: Any) -> str:
    return str(value or "").strip()


def source_product_id_from_url(url: str) -> str | None:
    path = urlparse(url).path
    match = re.search(r"/article/([^/]+)", path)
    if match:
        return match.group(1)
    return path.rstrip("/").split("/")[-1] or None


def normalize_price(value: Any) -> str | None:
    text = clean(value)
    if not text:
        return None
    text = text.replace("€", "").replace("EUR", "").replace(" ", "").strip()
    text = text.replace(".", ",")
    if re.match(r"^[0-9]+(?:,[0-9]{1,2})?$", text):
        return text
    return None


def normalize_availability(row: dict[str, Any]) -> str:
    stock = clean(row.get("op_voorraad")).upper()
    if stock in {"JA", "YES", "TRUE", "1"}:
        return "in_stock"

    release_text = clean(row.get("releasedatum"))
    try:
        release_date = datetime.strptime(release_text, "%d-%m-%Y").date()
        if release_date > date.today():
            return "preorder"
    except ValueError:
        pass

    return "unknown"


def is_vinyl_row(row: dict[str, Any]) -> bool:
    haystack = " ".join(
        [
            clean(row.get("drager")),
            clean(row.get("type")),
            clean(row.get("title")),
        ]
    ).upper()

    vinyl_markers = (
        "LP",
        "VINYL",
        "7 INCH",
        "7INCH",
        "10 INCH",
        "10INCH",
        "12 INCH",
        "12INCH",
    )
    non_vinyl_markers = ("CD", "DVD", "BLURAY", "BLU-RAY", "CASSETTE")

    if any(marker in haystack for marker in vinyl_markers):
        return True
    if any(marker in haystack for marker in non_vinyl_markers):
        return False
    return False


def build_payload(row: dict[str, Any], *, seed_url: str, listing_url: str, page: int) -> dict[str, Any]:
    price = normalize_price(row.get("prijs"))

    payload: dict[str, Any] = {
        "discovery_source": "platomania_listing",
        "seed_url": seed_url,
        "listing_url": listing_url,
        "page": page,
        "availability": normalize_availability(row),
        "price_source": "listing",
    }

    if price:
        payload["price"] = price
        payload["prijs"] = price

    for key in (
        "artist",
        "title",
        "type",
        "drager",
        "op_voorraad",
        "bron_categorieen",
        "bron_listing_urls",
        "label",
        "releasedatum",
        "herkomst",
        "item_nr",
        "ean",
    ):
        value = clean(row.get(key))
        if value:
            payload[key] = value

    return payload


def discover_links(
    *,
    seed_limit: int = 0,
    max_pages_per_seed: int = 0,
    delay_seconds: float = 0.35,
) -> list[DiscoveredLink]:
    session = make_session()
    seed_urls = list(SEED_URLS[:seed_limit] if seed_limit and seed_limit > 0 else SEED_URLS)

    links_by_url: dict[str, DiscoveredLink] = {}
    total_pages = 0

    print(
        "[DISCOVER] config",
        {
            "shop": SHOP_ID,
            "seeds": len(seed_urls),
            "seed_limit": seed_limit,
            "max_pages_per_seed": max_pages_per_seed,
            "delay_seconds": delay_seconds,
        },
        flush=True,
    )

    for seed_index, seed_url in enumerate(seed_urls, start=1):
        category_name = category_name_from_url(seed_url)
        seen_in_seed: set[str] = set()
        page = 1

        while True:
            if max_pages_per_seed and max_pages_per_seed > 0 and page > max_pages_per_seed:
                break

            listing_url = page_url(seed_url, page)
            print(
                "[DISCOVER] page",
                {
                    "seed_index": seed_index,
                    "category": category_name,
                    "page": page,
                    "url": listing_url,
                },
                flush=True,
            )

            try:
                soup = fetch_soup(session, listing_url)
            except Exception as exc:
                print(
                    "[DISCOVER][WARN] fetch_failed",
                    {
                        "category": category_name,
                        "page": page,
                        "url": listing_url,
                        "error": str(exc),
                    },
                    flush=True,
                )
                break

            parsed_rows: list[dict[str, str]] = []

            for article in soup.select("article.article"):
                row = parse_article(
                    article,
                    category_name=category_name,
                    listing_url=listing_url,
                    include_extra=True,
                )
                if not row:
                    continue
                if not is_vinyl_row(row):
                    continue
                parsed_rows.append(row)

            page_urls = [clean(row.get("url")) for row in parsed_rows if clean(row.get("url"))]
            new_urls_in_seed = [url for url in page_urls if url not in seen_in_seed]

            if not parsed_rows:
                print("[DISCOVER] stop_empty", {"category": category_name, "page": page}, flush=True)
                break

            if not new_urls_in_seed:
                print(
                    "[DISCOVER] stop_duplicate_page",
                    {"category": category_name, "page": page, "rows": len(parsed_rows)},
                    flush=True,
                )
                break

            for row in parsed_rows:
                source_url = clean(row.get("url"))
                if not source_url:
                    continue

                seen_in_seed.add(source_url)
                payload = build_payload(row, seed_url=seed_url, listing_url=listing_url, page=page)

                links_by_url[source_url] = DiscoveredLink(
                    shop_id=SHOP_ID,
                    source_url=source_url,
                    source_product_id=source_product_id_from_url(source_url),
                    payload=payload,
                )

            total_pages += 1
            print(
                "[DISCOVER] page_result",
                {
                    "category": category_name,
                    "page": page,
                    "vinyl_rows": len(parsed_rows),
                    "new_urls_in_seed": len(new_urls_in_seed),
                    "catalog_unique_total": len(links_by_url),
                },
                flush=True,
            )

            page += 1
            if delay_seconds > 0:
                time.sleep(delay_seconds)

    print(
        "[DISCOVER] summary",
        {"shop": SHOP_ID, "links": len(links_by_url), "pages": total_pages},
        flush=True,
    )

    return list(links_by_url.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Discover Platomania vinyl links for USF.")
    parser.add_argument("--seed-limit", type=int, default=0, help="Aantal seedcategorieën; 0 = alle seeds.")
    parser.add_argument("--max-pages-per-seed", type=int, default=1, help="Aantal pagina's per seed; 0 = tot leeg/dubbel.")
    parser.add_argument("--delay-seconds", type=float, default=0.35)
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
    if args.sample_size < 0:
        raise SystemExit("[ERROR] --sample-size mag niet negatief zijn.")

    links = discover_links(
        seed_limit=args.seed_limit,
        max_pages_per_seed=args.max_pages_per_seed,
        delay_seconds=args.delay_seconds,
    )

    for link in links[: args.sample_size]:
        print(
            "[DISCOVER] sample",
            {
                "source_url": link.source_url,
                "source_product_id": link.source_product_id,
                "artist": link.payload.get("artist"),
                "title": link.payload.get("title"),
                "ean": link.payload.get("ean"),
                "price": link.payload.get("price"),
                "availability": link.payload.get("availability"),
                "drager": link.payload.get("drager"),
            },
            flush=True,
        )

    if not links:
        raise SystemExit("[ERROR] Platomania discovery leverde geen vinyl-links op.")

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
