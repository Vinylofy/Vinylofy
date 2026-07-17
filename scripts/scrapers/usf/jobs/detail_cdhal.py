#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup, Tag
from psycopg.rows import dict_row

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import (
    insert_raw_shop_scrape,
    mark_detail_scraped,
)


SHOP_ID = "cdhal"
BASE_URL = "https://www.cdhal.nl"

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

SUPPORTED_EAN_LENGTHS = {8, 12, 13, 14}


def clean(value: object) -> str:
    return re.sub(
        r"\s+",
        " ",
        str(value or "").replace("\xa0", " "),
    ).strip()


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
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    else:
        text = text.replace(",", ".")

    match = re.search(
        r"(?<!\d)(\d+(?:\.\d{1,2})?)(?!\d)",
        text,
    )

    if not match:
        return None

    amount = match.group(1)

    if "." not in amount:
        return f"{amount}.00"

    whole, cents = amount.split(".", 1)
    return f"{whole}.{cents[:2].ljust(2, '0')}"


def normalize_availability(value: object) -> str:
    text = clean(value).lower()
    text = text.replace("-", "_").replace(" ", "_")

    aliases = {
        "in_stock": "in_stock",
        "instock": "in_stock",
        "direct_leverbaar": "in_stock",
        "out_of_stock": "out_of_stock",
        "outofstock": "out_of_stock",
        "tijdelijk_niet_leverbaar": "out_of_stock",
        "preorder": "preorder",
        "pre_order": "preorder",
        "binnenkort_leverbaar": "preorder",
        "unknown": "unknown",
    }

    return aliases.get(text, "unknown")


def normalize_ean(value: object) -> str | None:
    if value is None:
        return None

    text = clean(value)

    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]

    digits = re.sub(r"\D", "", text)

    if len(digits) == 11:
        digits = "0" + digits

    if len(digits) in SUPPORTED_EAN_LENGTHS:
        return digits

    return None


def normalize_product_url(value: object) -> str:
    text = clean(value)

    if not text:
        return ""

    parsed = urlsplit(text)

    if parsed.scheme not in {"http", "https"}:
        return ""

    if parsed.netloc.lower() not in {
        "cdhal.nl",
        "www.cdhal.nl",
    }:
        return ""

    path = re.sub(
        r"/+",
        "/",
        parsed.path or "/",
    ).rstrip("/")

    return urlunsplit(
        (
            "https",
            "www.cdhal.nl",
            path,
            "",
            "",
        )
    )



def request_detail_page(
    session: requests.Session,
    source_url: str,
    *,
    max_attempts: int = 4,
) -> requests.Response:
    """
    Haal een CDHAL-detailpagina op met begrensde retry bij HTTP 429.

    Retry-After wordt gerespecteerd, maar begrensd om een Actions-run
    niet onbeperkt te laten wachten.
    """
    last_response: requests.Response | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = session.get(
                source_url,
                timeout=45,
            )
        except requests.RequestException:
            if attempt >= max_attempts:
                raise

            delay = min(3 * attempt, 15)

            print(
                "[DETAIL-RETRY]",
                {
                    "url": source_url,
                    "attempt": attempt,
                    "reason": "request_exception",
                    "sleep_seconds": delay,
                },
                flush=True,
            )

            time.sleep(delay)
            continue

        last_response = response

        if response.status_code != 429:
            return response

        retry_after_raw = clean(
            response.headers.get("Retry-After")
        )

        try:
            retry_after = int(retry_after_raw)
        except (TypeError, ValueError):
            retry_after = 5 * attempt

        delay = max(
            3,
            min(retry_after, 30),
        )

        print(
            "[DETAIL-RETRY]",
            {
                "url": source_url,
                "attempt": attempt,
                "status_code": 429,
                "retry_after_raw": (
                    retry_after_raw or None
                ),
                "sleep_seconds": delay,
            },
            flush=True,
        )

        if attempt < max_attempts:
            time.sleep(delay)

    if last_response is None:
        raise RuntimeError(
            "CDHAL-detailrequest leverde geen response."
        )

    return last_response


def fetch_detail_queue(
    *,
    limit: int,
    retry_days: int,
) -> list[dict[str, Any]]:
    """
    Verrijk alleen actieve CDHAL-links die volgens de listing publiceerbaar
    zijn en nog geen geldige EAN-snapshot hebben.

    Mislukte no-EAN-pogingen worden maximaal drie keer opnieuw geprobeerd.
    """
    with db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                with link_rows as (
                    select
                        l.id,
                        l.shop_id,
                        l.source_url,
                        trim(
                            trailing '/'
                            from split_part(l.source_url, '?', 1)
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
                            (l.payload->>'publish_eligible')::boolean,
                            false
                          ) = true
                      and coalesce(
                            l.payload->>'availability',
                            'unknown'
                          ) in ('in_stock', 'preorder')
                ),
                latest_raw as (
                    select distinct on (
                        r.shop_id,
                        trim(
                            trailing '/'
                            from split_part(r.source_url, '?', 1)
                        )
                    )
                        r.shop_id,
                        trim(
                            trailing '/'
                            from split_part(r.source_url, '?', 1)
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
                            from split_part(r.source_url, '?', 1)
                        ),
                        r.scraped_at desc nulls last,
                        r.id desc
                ),
                raw_attempts as (
                    select
                        r.shop_id,
                        trim(
                            trailing '/'
                            from split_part(r.source_url, '?', 1)
                        ) as source_url_norm,
                        count(*) as attempts,
                        count(*) filter (
                            where r.ean_raw is null
                        ) as missing_ean_attempts
                    from public.raw_shop_scrapes r
                    where r.shop_id = %s
                    group by
                        r.shop_id,
                        trim(
                            trailing '/'
                            from split_part(r.source_url, '?', 1)
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
                    coalesce(ra.attempts, 0) as detail_attempts,
                    coalesce(
                        ra.missing_ean_attempts,
                        0
                    ) as missing_ean_attempts
                from link_rows l
                left join latest_raw lr
                    on lr.shop_id = l.shop_id
                   and lr.source_url_norm = l.source_url_norm
                left join raw_attempts ra
                    on ra.shop_id = l.shop_id
                   and ra.source_url_norm = l.source_url_norm
                where (
                    lr.raw_scrape_id is null
                    or (
                        lr.ean_raw is null
                        and coalesce(
                            ra.missing_ean_attempts,
                            0
                        ) < 3
                    )
                )
                  and (
                    l.last_detail_scraped_at is null
                    or l.last_detail_scraped_at
                        < now() - (%s * interval '1 day')
                    or lr.raw_scrape_id is null
                )
                order by
                    case
                        when lr.raw_scrape_id is null then 0
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


def listing_payload(
    row: dict[str, Any],
) -> dict[str, Any]:
    payload = row.get("payload")
    return payload if isinstance(payload, dict) else {}


def listing_price(
    row: dict[str, Any],
) -> str | None:
    return normalize_price(
        listing_payload(row).get("price")
    )


def listing_availability(
    row: dict[str, Any],
) -> str:
    return normalize_availability(
        listing_payload(row).get("availability")
    )


def extract_ean_from_soup(
    soup: BeautifulSoup,
) -> str | None:
    """
    CDHAL Magento-contract:
    td.col.data[data-th="EAN"]

    Het datalabel wordt case-insensitief behandeld.
    """
    for cell in soup.select("td.col.data[data-th]"):
        if not isinstance(cell, Tag):
            continue

        label = clean(
            cell.get("data-th")
        ).casefold()

        if label != "ean":
            continue

        value = normalize_ean(
            cell.get_text(
                " ",
                strip=True,
            )
        )

        if value:
            return value

    # Magento kan dezelfde productspecificatie in een mobiel/alternatief
    # attributenblok renderen.
    for row in soup.select("tr"):
        if not isinstance(row, Tag):
            continue

        label_cell = row.select_one(
            "th, td[data-th='Label'], .col.label"
        )
        value_cell = row.select_one(
            "td.col.data, td[data-th]"
        )

        if not isinstance(label_cell, Tag):
            continue

        if not isinstance(value_cell, Tag):
            continue

        if clean(
            label_cell.get_text(
                " ",
                strip=True,
            )
        ).casefold() != "ean":
            continue

        value = normalize_ean(
            value_cell.get_text(
                " ",
                strip=True,
            )
        )

        if value:
            return value

    return None


def extract_ean(
    html: str,
) -> str | None:
    return extract_ean_from_soup(
        BeautifulSoup(
            html,
            "html.parser",
        )
    )


def extract_title_raw(
    html: str,
    payload: dict[str, Any],
) -> str | None:
    soup = BeautifulSoup(
        html,
        "html.parser",
    )

    heading = soup.select_one(
        "h1.page-title span.base, "
        "h1.page-title, "
        "h1"
    )

    if isinstance(heading, Tag):
        title = clean(
            heading.get_text(
                " ",
                strip=True,
            )
        )

        if title:
            return title

    artist = clean(
        payload.get("artist")
    )
    title = clean(
        payload.get("title")
    )

    if artist and title:
        return f"{artist} - {title}"

    return title or None


def parse_detail_html(
    html: str,
    *,
    source_url: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    listing = payload or {}

    return {
        "source_url": normalize_product_url(
            source_url
        ),
        "title_raw": extract_title_raw(
            html,
            listing,
        ),
        "ean_raw": extract_ean(html),
        "price_raw": normalize_price(
            listing.get("price")
        ),
        "availability_raw": normalize_availability(
            listing.get("availability")
        ),
        "image_url_raw": None,
        "payload": {
            "source": "detail_cdhal",
            "detail_price_policy": (
                "listing_price_and_availability_are_authoritative"
            ),
            "listing_payload": listing,
            "image_policy": "no_shop_image_capture",
        },
    }


def fetch_live_sample(
    source_url: str,
) -> dict[str, Any]:
    normalized_url = normalize_product_url(
        source_url
    )

    if not normalized_url:
        raise ValueError(
            "Ongeldige CDHAL sample-URL."
        )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept-Language": (
                "nl-NL,nl;q=0.9,en;q=0.8"
            ),
        }
    )

    response = request_detail_page(
        session,
        normalized_url,
    )
    response.raise_for_status()

    parsed = parse_detail_html(
        response.text,
        source_url=normalized_url,
        payload={},
    )

    return {
        **parsed,
        "status_code": response.status_code,
        "html_length": len(response.text),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "CDHAL-detailpagina’s verrijken naar raw_shop_scrapes. "
            "Listingprijs en listingstatus blijven leidend."
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
        "--sample-url",
        type=str,
        default="",
        help=(
            "Fetch één detailpagina zonder databasequery of write."
        ),
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

    if args.sample_url:
        sample = fetch_live_sample(
            args.sample_url
        )

        print(
            "[DETAIL-SAMPLE]",
            {
                "source_url": sample["source_url"],
                "status_code": sample["status_code"],
                "html_length": sample["html_length"],
                "title_raw": sample["title_raw"],
                "ean_raw": sample["ean_raw"],
                "price_raw": sample["price_raw"],
                "availability_raw": sample[
                    "availability_raw"
                ],
                "image_url_raw": sample[
                    "image_url_raw"
                ],
            },
            flush=True,
        )

        if args.write:
            raise SystemExit(
                "[ERROR] --sample-url ondersteunt nooit writes."
            )

        return 0

    rows = fetch_detail_queue(
        limit=args.limit,
        retry_days=args.retry_days,
    )

    print(
        "[DETAIL]",
        {
            "shop": SHOP_ID,
            "queued": len(rows),
            "write": args.write,
            "retry_days": args.retry_days,
        },
        flush=True,
    )

    if not rows:
        return 0

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept-Language": (
                "nl-NL,nl;q=0.9,en;q=0.8"
            ),
        }
    )

    stored = 0
    skipped = 0

    for index, row in enumerate(
        rows,
        start=1,
    ):
        source_url = normalize_product_url(
            row["source_url"]
        )
        payload = listing_payload(row)

        print(
            "[DETAIL]",
            {
                "index": index,
                "total": len(rows),
                "url": source_url,
                "attempts": row.get(
                    "detail_attempts"
                ),
            },
            flush=True,
        )

        if not args.write:
            print(
                "[DETAIL-QUEUE-SAMPLE]",
                {
                    "source_url": source_url,
                    "listing_price": listing_price(
                        row
                    ),
                    "listing_availability": (
                        listing_availability(row)
                    ),
                    "artist": payload.get(
                        "artist"
                    ),
                    "title": payload.get(
                        "title"
                    ),
                },
                flush=True,
            )

            skipped += 1
            continue

        try:
            response = request_detail_page(
                session,
                source_url,
            )
        except requests.RequestException as exc:
            skipped += 1

            print(
                "[DETAIL-WARN]",
                {
                    "url": source_url,
                    "error": str(exc),
                },
                flush=True,
            )
            continue

        if response.status_code == 429:
            skipped += 1

            print(
                "[DETAIL-SKIP]",
                {
                    "url": source_url,
                    "status_code": 429,
                    "reason": "rate_limited_after_retries",
                },
                flush=True,
            )
            continue

        if response.status_code in {
            404,
            410,
        }:
            mark_detail_scraped(
                row["id"]
            )
            skipped += 1

            print(
                "[DETAIL-SKIP]",
                {
                    "url": source_url,
                    "status_code": response.status_code,
                    "reason": "dead_link",
                },
                flush=True,
            )
            continue

        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            skipped += 1

            print(
                "[DETAIL-WARN]",
                {
                    "url": source_url,
                    "error": str(exc),
                },
                flush=True,
            )
            continue

        parsed = parse_detail_html(
            response.text,
            source_url=source_url,
            payload=payload,
        )

        price_raw = listing_price(row)
        availability_raw = listing_availability(
            row
        )

        if (
            price_raw is None
            or availability_raw
            not in {
                "in_stock",
                "preorder",
            }
        ):
            mark_detail_scraped(
                row["id"]
            )
            skipped += 1

            print(
                "[DETAIL-SKIP]",
                {
                    "url": source_url,
                    "price": price_raw,
                    "availability": availability_raw,
                    "reason": (
                        "listing_no_longer_publishable"
                    ),
                },
                flush=True,
            )
            continue

        source_product_id = (
            clean(
                row.get(
                    "source_product_id"
                )
            )
            or source_url.rstrip(
                "/"
            ).split("/")[-1]
        )

        raw_id = insert_raw_shop_scrape(
            run_id=None,
            shop_id=SHOP_ID,
            source_url=source_url,
            source_product_id=source_product_id,
            title_raw=parsed[
                "title_raw"
            ],
            ean_raw=parsed[
                "ean_raw"
            ],
            price_raw=price_raw,
            availability_raw=availability_raw,
            image_url_raw=None,
            payload={
                **parsed["payload"],
                "scraped_at": datetime.now(
                    timezone.utc
                ).isoformat(),
                "html_length": len(
                    response.text
                ),
                "status_code": (
                    response.status_code
                ),
            },
        )

        mark_detail_scraped(
            row["id"]
        )

        stored += 1

        print(
            "[DETAIL-STORED]",
            {
                "raw_id": raw_id,
                "url": source_url,
                "ean": parsed["ean_raw"],
                "price_from_listing": price_raw,
                "availability_from_listing": (
                    availability_raw
                ),
                "image_url_raw": None,
            },
            flush=True,
        )

        time.sleep(args.sleep)

    print(
        "[DETAIL-DONE]",
        {
            "stored": stored,
            "skipped": skipped,
            "write": args.write,
        },
        flush=True,
    )

    if not args.write:
        print(
            "[DETAIL] dry-run complete; "
            "geen databasewrites.",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
