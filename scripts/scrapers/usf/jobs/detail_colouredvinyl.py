#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup, Tag
from psycopg.rows import dict_row

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import (
    insert_raw_shop_scrape,
    mark_detail_scraped,
)


SHOP_ID = "colouredvinyl"
BASE_URL = "https://www.colouredvinyl.nl"

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36 "
    "Vinylofy/1.0"
)


CONNECT_TIMEOUT_SECONDS = 12
READ_TIMEOUT_SECONDS = 60

# Een URL wordt maximaal twee keer geprobeerd:
# de eerste request plus één connect/statusretry.
REQUEST_RETRIES = 1

# Bij een bredere runner- of siteblokkade stoppen
# we na drie volledig mislukte product-URL's.
MAX_CONSECUTIVE_TRANSPORT_FAILURES = 3


def configure_transport(
    session: requests.Session,
) -> None:
    retries = Retry(
        total=REQUEST_RETRIES,
        connect=REQUEST_RETRIES,
        read=REQUEST_RETRIES,
        status=REQUEST_RETRIES,
        redirect=2,
        backoff_factor=3.0,
        status_forcelist=(
            429,
            500,
            502,
            503,
            504,
        ),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=1,
        pool_maxsize=1,
    )

    session.mount(
        "https://",
        adapter,
    )


def clean(value: object) -> str:
    return re.sub(
        r"\s+",
        " ",
        str(value or "").replace("\xa0", " "),
    ).strip()


def normalize_product_url(value: object) -> str:
    text = clean(value)

    if not text:
        return ""

    text = text.split("#", 1)[0]
    text = text.split("?", 1)[0]
    text = text.rstrip("/")

    return text + "/"


def normalize_ean(value: object) -> str | None:
    digits = re.sub(r"\D", "", clean(value))

    if len(digits) == 11:
        digits = "0" + digits

    if len(digits) in {8, 12, 13, 14}:
        return digits

    return None


def normalize_price(value: object) -> str | None:
    text = clean(value)

    if not text:
        return None

    text = (
        text.replace("€", "")
        .replace("EUR", "")
        .strip()
    )

    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")

    match = re.search(
        r"([0-9]+(?:\.[0-9]{1,2})?)",
        text,
    )

    if not match:
        return None

    amount = match.group(1)

    if "." not in amount:
        return amount + ".00"

    whole, cents = amount.split(".", 1)

    return f"{whole}.{cents[:2].ljust(2, '0')}"


def get_listing_payload(
    row: dict[str, Any],
) -> dict[str, Any]:
    payload = row.get("payload")

    return payload if isinstance(payload, dict) else {}


def fetch_detail_queue(
    *,
    limit: int,
    retry_days: int,
) -> list[dict[str, Any]]:
    with db_connection() as connection:
        with connection.cursor(
            row_factory=dict_row,
        ) as cursor:
            cursor.execute(
                """
                with links as (
                    select
                        l.id,
                        l.shop_id,
                        l.source_url,
                        trim(
                            trailing '/'
                            from split_part(
                                l.source_url,
                                '?',
                                1
                            )
                        ) as source_url_norm,
                        l.source_product_id,
                        l.payload,
                        l.first_seen_at,
                        l.last_seen_at,
                        l.last_detail_scraped_at
                    from public.shop_product_links l
                    where l.shop_id = %s
                      and l.status = 'active'
                      and l.source_url is not null
                      and coalesce(
                            l.payload->>'availability',
                            'unknown'
                          ) = 'in_stock'
                      and coalesce(
                            l.payload->>'publish_eligible',
                            'true'
                          ) <> 'false'
                      and coalesce(
                            l.payload->>'url_collision',
                            'false'
                          ) <> 'true'
                ),
                latest_raw as (
                    select distinct on (
                        r.shop_id,
                        trim(
                            trailing '/'
                            from split_part(
                                r.source_url,
                                '?',
                                1
                            )
                        )
                    )
                        r.shop_id,
                        trim(
                            trailing '/'
                            from split_part(
                                r.source_url,
                                '?',
                                1
                            )
                        ) as source_url_norm,
                        r.id as raw_scrape_id,
                        r.ean_raw,
                        r.payload as raw_payload,
                        r.scraped_at
                    from public.raw_shop_scrapes r
                    where r.shop_id = %s
                    order by
                        r.shop_id,
                        trim(
                            trailing '/'
                            from split_part(
                                r.source_url,
                                '?',
                                1
                            )
                        ),
                        r.scraped_at desc nulls last,
                        r.id desc
                ),
                attempts as (
                    select
                        r.shop_id,
                        trim(
                            trailing '/'
                            from split_part(
                                r.source_url,
                                '?',
                                1
                            )
                        ) as source_url_norm,
                        count(*)::int as attempts,
                        count(*) filter (
                            where r.ean_raw is null
                        )::int as missing_ean_attempts
                    from public.raw_shop_scrapes r
                    where r.shop_id = %s
                    group by
                        r.shop_id,
                        trim(
                            trailing '/'
                            from split_part(
                                r.source_url,
                                '?',
                                1
                            )
                        )
                )
                select
                    l.id,
                    l.shop_id,
                    l.source_url,
                    l.source_product_id,
                    l.payload,
                    l.last_detail_scraped_at,
                    lr.raw_scrape_id,
                    lr.ean_raw as latest_ean_raw,
                    lr.raw_payload,
                    coalesce(a.attempts, 0) as attempts,
                    coalesce(
                        a.missing_ean_attempts,
                        0
                    ) as missing_ean_attempts
                from links l
                left join latest_raw lr
                  on lr.shop_id = l.shop_id
                 and lr.source_url_norm =
                     l.source_url_norm
                left join attempts a
                  on a.shop_id = l.shop_id
                 and a.source_url_norm =
                     l.source_url_norm
                where (
                    lr.raw_scrape_id is null
                    or (
                        lr.ean_raw is null
                        and coalesce(
                            a.missing_ean_attempts,
                            0
                        ) < 3
                    )
                )
                  and (
                    l.last_detail_scraped_at is null
                    or l.last_detail_scraped_at
                       < now()
                         - (
                             %s
                             * interval '1 day'
                           )
                    or lr.raw_scrape_id is null
                  )
                order by
                    case
                        when lr.raw_scrape_id is null
                        then 0
                        else 1
                    end,
                    l.first_seen_at asc nulls last,
                    l.id asc
                limit %s
                """,
                (
                    SHOP_ID,
                    SHOP_ID,
                    SHOP_ID,
                    retry_days,
                    limit,
                ),
            )

            return [
                dict(row)
                for row in cursor.fetchall()
            ]


def extract_product_meta(
    soup: BeautifulSoup,
    html: str,
) -> dict[str, str | None]:
    product_meta = soup.select_one(
        ".product_meta"
    )

    meta_text = clean(
        product_meta.get_text(
            " ",
            strip=True,
        )
        if product_meta
        else ""
    )

    searchable = meta_text or clean(html)

    ean = None

    ean_patterns = (
        r"\bEAN\s*:\s*([0-9]{8,14})\b",
        r'"gtin(?:8|12|13|14)?"\s*:\s*'
        r'"([0-9]{8,14})"',
        r"\bGTIN\s*:\s*([0-9]{8,14})\b",
    )

    for pattern in ean_patterns:
        match = re.search(
            pattern,
            html if "gtin" in pattern else searchable,
            flags=re.IGNORECASE,
        )

        if not match:
            continue

        ean = normalize_ean(match.group(1))

        if ean:
            break

    sku_match = re.search(
        r"\bSKU\s*:\s*([A-Za-z0-9._-]+)",
        searchable,
        flags=re.IGNORECASE,
    )

    artist_match = re.search(
        r"(?:^|\s)Artiest\s*:\s*(.+?)\s*$",
        meta_text,
        flags=re.IGNORECASE,
    )

    return {
        "ean": ean,
        "sku": (
            clean(sku_match.group(1))
            if sku_match
            else None
        ),
        "artist": (
            clean(artist_match.group(1))
            if artist_match
            else None
        ),
        "meta_text": meta_text or None,
    }


def extract_attributes(
    soup: BeautifulSoup,
) -> dict[str, str]:
    attributes: dict[str, str] = {}

    rows = soup.select(
        "table.woocommerce-product-attributes tr, "
        "tr.woocommerce-product-attributes-item"
    )

    for row in rows:
        if not isinstance(row, Tag):
            continue

        label_node = row.select_one(
            "th, "
            ".woocommerce-product-attributes-"
            "item__label"
        )

        value_node = row.select_one(
            "td, "
            ".woocommerce-product-attributes-"
            "item__value"
        )

        label = clean(
            label_node.get_text(
                " ",
                strip=True,
            )
            if label_node
            else ""
        ).rstrip(":")

        value = clean(
            value_node.get_text(
                " ",
                strip=True,
            )
            if value_node
            else ""
        )

        if not label or not value:
            continue

        attributes[label.lower()] = value

    return attributes


def extract_title(
    soup: BeautifulSoup,
) -> str | None:
    node = soup.select_one(
        "h1.product_title, "
        "h1.entry-title, "
        "h1"
    )

    title = clean(
        node.get_text(
            " ",
            strip=True,
        )
        if node
        else ""
    )

    return title or None


def extract_artist_fallback(
    soup: BeautifulSoup,
) -> str | None:
    breadcrumb = soup.select_one(
        ".woocommerce-breadcrumb"
    )

    if breadcrumb:
        text = clean(
            breadcrumb.get_text(
                " ",
                strip=True,
            )
        )

        match = re.search(
            r"\bShop\s+(.+?)\s*$",
            text,
            flags=re.IGNORECASE,
        )

        if match:
            artist = clean(match.group(1))

            if artist:
                return artist

    return None


def detail_availability_evidence(
    soup: BeautifulSoup,
) -> str:
    summary = soup.select_one(
        ".summary, "
        ".product-summary"
    )

    text = clean(
        summary.get_text(
            " ",
            strip=True,
        )
        if summary
        else ""
    ).lower()

    if "uitverkocht" in text:
        return "detail_out_of_stock"

    if (
        "toevoegen aan winkelwagen" in text
        or "op voorraad" in text
    ):
        return "detail_purchasable"

    return "detail_unknown"


def parse_detail(
    *,
    html: str,
    source_url: str,
    listing_payload: dict[str, Any],
) -> dict[str, Any]:
    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    product_meta = extract_product_meta(
        soup,
        html,
    )

    attributes = extract_attributes(soup)

    title = extract_title(soup)

    artist = (
        product_meta.get("artist")
        or clean(listing_payload.get("artist"))
        or extract_artist_fallback(soup)
    )

    format_label = (
        attributes.get("inhoud")
        or clean(
            listing_payload.get("format")
        )
        or None
    )

    source_product_id = (
        product_meta.get("sku")
        or clean(
            listing_payload.get(
                "woocommerce_product_id"
            )
        )
        or clean(
            listing_payload.get(
                "source_sku"
            )
        )
        or urlparse(source_url)
        .path.strip("/")
        .split("/")[-1]
    )

    title_raw = None

    if artist and title:
        title_raw = f"{artist} | {title}"
    elif title:
        title_raw = title

    return {
        "ean": product_meta.get("ean"),
        "sku": product_meta.get("sku"),
        "artist": artist or None,
        "title": title,
        "title_raw": title_raw,
        "source_product_id": (
            source_product_id or None
        ),
        "format": format_label,
        "colour": attributes.get("kleur"),
        "genre": attributes.get("genre"),
        "label": attributes.get(
            "platenlabel"
        ),
        "release_date": attributes.get(
            "releasedatum"
        ),
        "size": attributes.get("size"),
        "origin": attributes.get(
            "herkomst"
        ),
        "detail_availability_evidence": (
            detail_availability_evidence(
                soup
            )
        ),
        "product_meta_text": (
            product_meta.get("meta_text")
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Enrich Coloured Vinyl registrylinks "
            "met EAN en detailmetadata. "
            "Listingprijs en listingvoorraad "
            "blijven altijd leidend."
        )
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=25,
    )

    parser.add_argument(
        "--sleep",
        type=float,
        default=0.50,
    )

    parser.add_argument(
        "--retry-days",
        type=int,
        default=30,
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

    if args.limit < 1:
        raise SystemExit(
            "[ERROR] --limit moet minimaal 1 zijn."
        )

    if args.sleep < 0:
        raise SystemExit(
            "[ERROR] --sleep mag niet negatief zijn."
        )

    if args.retry_days < 1:
        raise SystemExit(
            "[ERROR] --retry-days moet minimaal 1 zijn."
        )

    queue = fetch_detail_queue(
        limit=args.limit,
        retry_days=args.retry_days,
    )

    print(
        "[COLOUREDVINYL-DETAIL-QUEUE]",
        {
            "queued": len(queue),
            "limit": args.limit,
            "retry_days": args.retry_days,
            "write": args.write,
        },
        flush=True,
    )

    session = requests.Session()

    session.headers.update({
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": (
            "text/html,application/xhtml+xml,"
            "application/xml;q=0.9,*/*;q=0.8"
        ),
        "Accept-Language": (
            "nl-NL,nl;q=0.9,en;q=0.8"
        ),
        "Referer": f"{BASE_URL}/vinyl/",
        "Connection": "close",
    })

    configure_transport(session)

    diagnostics: dict[str, Any] = {
        "summary": {},
        "items": [],
    }

    stats = {
        "queued": len(queue),
        "fetched": 0,
        "parsed": 0,
        "ean_found": 0,
        "artist_found": 0,
        "title_found": 0,
        "stored": 0,
        "missing_ean": 0,
        "request_failed": 0,
        "http_failed": 0,
        "dead_links": 0,
        "listing_policy_blocks": 0,
        "transport_aborted": False,
        "transport_abort_reason": None,
        "max_consecutive_transport_failures_seen": 0,
    }

    consecutive_transport_failures = 0

    for index, row in enumerate(
        queue,
        start=1,
    ):
        source_url = normalize_product_url(
            row.get("source_url")
        )

        listing_payload = get_listing_payload(
            row
        )

        listing_price = normalize_price(
            listing_payload.get("price")
        )

        listing_availability = clean(
            listing_payload.get(
                "availability"
            )
        ).lower()

        publish_eligible = (
            str(
                listing_payload.get(
                    "publish_eligible",
                    True,
                )
            ).lower()
            not in {"false", "0", "no"}
        )

        url_collision = (
            str(
                listing_payload.get(
                    "url_collision",
                    False,
                )
            ).lower()
            in {"true", "1", "yes"}
        )

        if (
            listing_availability != "in_stock"
            or not publish_eligible
            or url_collision
            or not listing_price
        ):
            stats[
                "listing_policy_blocks"
            ] += 1

            print(
                "[COLOUREDVINYL-DETAIL-BLOCK]",
                {
                    "index": index,
                    "url": source_url,
                    "availability": (
                        listing_availability
                    ),
                    "publish_eligible": (
                        publish_eligible
                    ),
                    "url_collision": (
                        url_collision
                    ),
                    "listing_price": (
                        listing_price
                    ),
                },
                flush=True,
            )

            continue

        print(
            "[COLOUREDVINYL-DETAIL]",
            {
                "index": index,
                "total": len(queue),
                "url": source_url,
                "write": args.write,
            },
            flush=True,
        )

        try:
            response = session.get(
                source_url,
                timeout=(
                    CONNECT_TIMEOUT_SECONDS,
                    READ_TIMEOUT_SECONDS,
                ),
                allow_redirects=True,
            )
        except requests.RequestException as exc:
            stats["request_failed"] += 1

            consecutive_transport_failures += 1

            stats[
                "max_consecutive_transport_failures_seen"
            ] = max(
                int(
                    stats[
                        "max_consecutive_transport_failures_seen"
                    ]
                ),
                consecutive_transport_failures,
            )

            print(
                "[COLOUREDVINYL-DETAIL-WARN]",
                {
                    "url": source_url,
                    "error_type": (
                        type(exc).__name__
                    ),
                    "error": str(exc),
                    "consecutive_transport_failures": (
                        consecutive_transport_failures
                    ),
                    "request_retries": (
                        REQUEST_RETRIES
                    ),
                },
                flush=True,
            )

            if (
                consecutive_transport_failures
                >= MAX_CONSECUTIVE_TRANSPORT_FAILURES
            ):
                stats["transport_aborted"] = True
                stats["transport_abort_reason"] = (
                    "max_consecutive_transport_failures"
                )

                print(
                    "[COLOUREDVINYL-DETAIL-ABORT]",
                    {
                        "reason": (
                            "max_consecutive_transport_failures"
                        ),
                        "threshold": (
                            MAX_CONSECUTIVE_TRANSPORT_FAILURES
                        ),
                        "processed_index": index,
                        "remaining_queue": (
                            len(queue) - index
                        ),
                        "action": (
                            "stop_without_marking_or_writing"
                        ),
                    },
                    flush=True,
                )

                break

            failure_backoff = min(
                30.0,
                5.0
                * consecutive_transport_failures,
            )

            print(
                "[COLOUREDVINYL-DETAIL-BACKOFF]",
                {
                    "seconds": failure_backoff,
                    "next_action": (
                        "try_next_registry_link"
                    ),
                },
                flush=True,
            )

            time.sleep(failure_backoff)
            continue

        consecutive_transport_failures = 0
        stats["fetched"] += 1

        if response.status_code == 429:
            stats["transport_aborted"] = True
            stats["transport_abort_reason"] = (
                "http_429"
            )

            print(
                "[COLOUREDVINYL-DETAIL-WARN]",
                {
                    "url": source_url,
                    "status": 429,
                    "action": (
                        "stop_without_marking_or_writing"
                    ),
                },
                flush=True,
            )
            break

        if response.status_code in {404, 410}:
            stats["dead_links"] += 1

            if args.write:
                mark_detail_scraped(
                    str(row["id"])
                )

            print(
                "[COLOUREDVINYL-DETAIL-DEAD]",
                {
                    "url": source_url,
                    "status": (
                        response.status_code
                    ),
                    "write": args.write,
                },
                flush=True,
            )

            continue

        if response.status_code != 200:
            stats["http_failed"] += 1

            print(
                "[COLOUREDVINYL-DETAIL-WARN]",
                {
                    "url": source_url,
                    "status": (
                        response.status_code
                    ),
                },
                flush=True,
            )

            continue

        parsed = parse_detail(
            html=response.text,
            source_url=source_url,
            listing_payload=listing_payload,
        )

        stats["parsed"] += 1
        stats["ean_found"] += int(
            bool(parsed.get("ean"))
        )
        stats["artist_found"] += int(
            bool(parsed.get("artist"))
        )
        stats["title_found"] += int(
            bool(parsed.get("title"))
        )
        stats["missing_ean"] += int(
            not parsed.get("ean")
        )

        item = {
            "index": index,
            "source_url": source_url,
            "status": response.status_code,
            "html_bytes": len(
                response.content
            ),
            "ean": parsed.get("ean"),
            "sku": parsed.get("sku"),
            "artist": parsed.get(
                "artist"
            ),
            "title": parsed.get("title"),
            "format": parsed.get(
                "format"
            ),
            "colour": parsed.get(
                "colour"
            ),
            "genre": parsed.get("genre"),
            "label": parsed.get("label"),
            "release_date": parsed.get(
                "release_date"
            ),
            "listing_price": (
                listing_price
            ),
            "listing_availability": (
                listing_availability
            ),
            "detail_availability_evidence": (
                parsed.get(
                    "detail_availability_evidence"
                )
            ),
        }

        diagnostics["items"].append(item)

        print(
            "[COLOUREDVINYL-DETAIL-SAMPLE]",
            json.dumps(
                item,
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )

        if args.write:
            raw_id = insert_raw_shop_scrape(
                run_id=None,
                shop_id=SHOP_ID,
                source_url=source_url,
                source_product_id=(
                    parsed.get(
                        "source_product_id"
                    )
                ),
                title_raw=parsed.get(
                    "title_raw"
                ),
                ean_raw=parsed.get("ean"),
                price_raw=listing_price,
                availability_raw=(
                    listing_availability
                ),
                image_url_raw=None,
                payload={
                    "source": (
                        "detail_colouredvinyl"
                    ),
                    "scraped_at": (
                        datetime.now(
                            timezone.utc
                        ).isoformat()
                    ),
                    "status_code": (
                        response.status_code
                    ),
                    "html_length": len(
                        response.text
                    ),
                    "detail_price_policy": (
                        "listing_price_and_"
                        "availability_are_"
                        "authoritative"
                    ),
                    "image_policy": (
                        "no_shop_image_stored"
                    ),
                    "listing_payload": (
                        listing_payload
                    ),
                    "sku": parsed.get("sku"),
                    "artist": parsed.get(
                        "artist"
                    ),
                    "title": parsed.get(
                        "title"
                    ),
                    "format": parsed.get(
                        "format"
                    ),
                    "colour": parsed.get(
                        "colour"
                    ),
                    "genre": parsed.get(
                        "genre"
                    ),
                    "label": parsed.get(
                        "label"
                    ),
                    "release_date": (
                        parsed.get(
                            "release_date"
                        )
                    ),
                    "size": parsed.get(
                        "size"
                    ),
                    "origin": parsed.get(
                        "origin"
                    ),
                    "detail_availability_"
                    "evidence": parsed.get(
                        "detail_availability_"
                        "evidence"
                    ),
                },
            )

            mark_detail_scraped(
                str(row["id"])
            )

            stats["stored"] += 1

            print(
                "[COLOUREDVINYL-DETAIL-WRITE]",
                {
                    "raw_id": raw_id,
                    "url": source_url,
                    "ean": parsed.get(
                        "ean"
                    ),
                    "price_from_listing": (
                        listing_price
                    ),
                    "image_stored": False,
                },
                flush=True,
            )

        if args.sleep:
            time.sleep(args.sleep)

    diagnostics["summary"] = {
        **stats,
        "write": args.write,
        "price_authority": "listing",
        "availability_authority": (
            "listing"
        ),
        "image_stored": False,
    }

    output_dir = Path(
        "output/usf-colouredvinyl"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    diagnostics_path = (
        output_dir
        / "detail-diagnostics.json"
    )

    diagnostics_path.write_text(
        json.dumps(
            diagnostics,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(
        "[COLOUREDVINYL-DETAIL-SUMMARY]",
        diagnostics["summary"],
        flush=True,
    )

    print(
        "[COLOUREDVINYL-DETAIL-DIAGNOSTICS]",
        {
            "path": str(
                diagnostics_path
            )
        },
        flush=True,
    )

    if not args.write:
        print(
            "[COLOUREDVINYL-DETAIL] "
            "dry-run compleet; "
            "geen databasewrites.",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
