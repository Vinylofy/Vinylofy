#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode

from scripts.scrapers.legacy.platenzaak_legacy import (
    COLLECTION_URL,
    build_session,
    fetch_soup,
    parse_listing_page,
    product_key_from_url,
)
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "platenzaak"
SORT_BY = "created-descending"


def clean(value: Any) -> str:
    return str(value or "").strip()


def build_page_url(page: int) -> str:
    query = {
        "filter.v.price.gte": "",
        "filter.v.price.lte": "",
        "sort_by": SORT_BY,
    }

    if page > 1:
        query["page"] = str(page)

    return f"{COLLECTION_URL}?{urlencode(query)}"


def build_payload(
    row: dict[str, Any],
    *,
    page: int,
    seen_at: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "discovery_source": "platenzaak_latest_vinyl_listing",
        "detail_priority": "high",
        "platenzaak_latest_priority": True,
        "platenzaak_latest_page": page,
        "platenzaak_latest_seen_at": seen_at,
        "platenzaak_latest_sort": SORT_BY,
    }

    for field in (
        "source_shop",
        "product_key",
        "artist",
        "title",
        "price",
        "currency",
        "availability",
        "page_found",
        "scraped_at",
    ):
        value = clean(row.get(field))
        if value:
            payload[field] = value

    return payload


def discover_latest_links(
    *,
    max_pages: int,
    delay_seconds: float,
) -> list[DiscoveredLink]:
    session = build_session()
    seen_at = datetime.now(timezone.utc).isoformat()

    links: list[DiscoveredLink] = []
    seen_urls: set[str] = set()

    for page in range(1, max_pages + 1):
        url = build_page_url(page)

        print(
            f"[PLATENZAAK-LATEST] page={page}/{max_pages} url={url}",
            flush=True,
        )

        soup = fetch_soup(session, url)
        rows = parse_listing_page(soup, page)

        print(
            f"[PLATENZAAK-LATEST] page={page} rows={len(rows)}",
            flush=True,
        )

        if not rows:
            print(
                "[PLATENZAAK-LATEST] geen resultaten; veilig stoppen.",
                flush=True,
            )
            break

        for row in rows:
            source_url = clean(row.get("product_url"))

            if not source_url or source_url in seen_urls:
                continue

            seen_urls.add(source_url)

            source_product_id = (
                clean(row.get("product_key"))
                or product_key_from_url(source_url)
            )

            links.append(
                DiscoveredLink(
                    shop_id=SHOP_ID,
                    source_url=source_url,
                    source_product_id=source_product_id,
                    payload=build_payload(
                        row,
                        page=page,
                        seen_at=seen_at,
                    ),
                )
            )

        if page < max_pages and delay_seconds > 0:
            time.sleep(delay_seconds)

    return links


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Registreer de nieuwste Platenzaak-vinylpagina's als "
            "prioriteit voor de detailscrape."
        )
    )
    parser.add_argument("--max-pages", type=int, default=5)
    parser.add_argument("--delay-seconds", type=float, default=0.25)
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.max_pages < 1:
        raise SystemExit("[ERROR] --max-pages moet minimaal 1 zijn.")

    if args.delay_seconds < 0:
        raise SystemExit("[ERROR] --delay-seconds mag niet negatief zijn.")

    if args.sample_size < 0:
        raise SystemExit("[ERROR] --sample-size mag niet negatief zijn.")

    links = discover_latest_links(
        max_pages=args.max_pages,
        delay_seconds=args.delay_seconds,
    )

    print(
        "[PLATENZAAK-LATEST] summary "
        + json.dumps(
            {
                "shop": SHOP_ID,
                "sort_by": SORT_BY,
                "links": len(links),
                "max_pages": args.max_pages,
                "write": bool(args.write),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )

    for link in links[: args.sample_size]:
        print(
            "[PLATENZAAK-LATEST] sample "
            + json.dumps(
                {
                    "source_url": link.source_url,
                    "source_product_id": link.source_product_id,
                    "artist": link.payload.get("artist"),
                    "title": link.payload.get("title"),
                    "page": link.payload.get(
                        "platenzaak_latest_page"
                    ),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )

    if not links:
        raise SystemExit(
            "[ERROR] Nieuwste Platenzaak-pagina's leverden geen links op."
        )

    if not args.write:
        print(
            "[PLATENZAAK-LATEST] dry-run; geen databasewrites.",
            flush=True,
        )
        return 0

    result = upsert_discovered_links(links)

    print(
        f"[PLATENZAAK-LATEST] registered "
        f"inserted={result.inserted} "
        f"updated={result.updated} "
        f"total={result.total}",
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
