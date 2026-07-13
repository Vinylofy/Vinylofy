from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from typing import Any

import psycopg
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from psycopg.types.json import Jsonb

from scripts.scrapers.legacy.platenzaak_legacy import (
    build_session,
    extract_detail_table,
)
from scripts.scrapers.usf.core.large_catalog_policy import (
    content_miss_retry,
    technical_failure_retry,
)
from scripts.scrapers.usf.core.link_registry import insert_raw_shop_scrape

SHOP_ID = "platenzaak"
REQUEST_TIMEOUT_SECONDS = 30
CLAIM_HOURS = 2


@dataclass(frozen=True)
class EligibleLink:
    id: str
    source_url: str
    source_product_id: str | None
    payload: dict[str, Any]
    ean_status: str
    content_miss_count: int
    technical_failure_count: int


@dataclass(frozen=True)
class DetailOutcome:
    kind: str
    http_status: int | None
    ean: str | None = None
    details: dict[str, str] | None = None
    image_url: str | None = None
    error: str | None = None
    note: str | None = None


def clean(value: Any) -> str:
    return str(value or "").strip()


def load_database_url() -> str:
    try:
        load_dotenv(".env.local")
        load_dotenv()
    except Exception:
        pass

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise SystemExit("[ERROR] DATABASE_URL ontbreekt.")
    return database_url


def build_listing_title(payload: dict[str, Any]) -> str | None:
    artist = clean(payload.get("artist"))
    title = clean(payload.get("title"))

    if artist and title:
        return f"{artist} - {title}"
    return title or artist or None


def get_eligible_links(limit: int) -> list[EligibleLink]:
    database_url = load_database_url()

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id,
                    source_url,
                    source_product_id,
                    payload,
                    coalesce(
                        ean_enrichment_status,
                        'pending'
                    ) as ean_status,
                    coalesce(
                        ean_content_miss_count,
                        0
                    ) as content_miss_count,
                    coalesce(
                        ean_technical_failure_count,
                        0
                    ) as technical_failure_count
                from public.shop_product_links
                where shop_id = %s
                  and status = 'active'
                  and coalesce(
                      ean_enrichment_status,
                      'pending'
                  ) in (
                      'pending',
                      'not_found',
                      'technical_error'
                  )
                  and (
                      ean_next_attempt_at is null
                      or ean_next_attempt_at <= now()
                  )
                order by
                    case
                        when
                            payload->>'detail_priority' = 'high'
                            and nullif(
                                payload->>'platenzaak_latest_seen_at',
                                ''
                            )::timestamptz
                                >= now() - interval '14 days'
                        then 0
                        else 1
                    end,
                    case coalesce(
                        ean_enrichment_status,
                        'pending'
                    )
                        when 'pending' then 1
                        when 'technical_error' then 2
                        when 'not_found' then 3
                        else 9
                    end,
                    case
                        when
                            payload->>'detail_priority' = 'high'
                            and nullif(
                                payload->>'platenzaak_latest_seen_at',
                                ''
                            )::timestamptz
                                >= now() - interval '14 days'
                        then nullif(
                            payload->>'platenzaak_latest_page',
                            ''
                        )::integer
                        else null
                    end asc nulls last,
                    ean_next_attempt_at asc nulls first,
                    first_seen_at asc
                limit %s
                """,
                (SHOP_ID, limit),
            )
            rows = cur.fetchall()

    return [
        EligibleLink(
            id=str(row[0]),
            source_url=str(row[1]),
            source_product_id=row[2],
            payload=dict(row[3] or {}),
            ean_status=str(row[4]),
            content_miss_count=int(row[5] or 0),
            technical_failure_count=int(row[6] or 0),
        )
        for row in rows
    ]

def claim_link(link_id: str) -> EligibleLink | None:
    database_url = load_database_url()

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.shop_product_links
                set
                    ean_next_attempt_at = now() + (%s * interval '1 hour'),
                    ean_last_result = 'processing'
                where id = %s
                  and shop_id = %s
                  and status = 'active'
                  and coalesce(ean_enrichment_status, 'pending') in (
                      'pending',
                      'not_found',
                      'technical_error'
                  )
                  and (
                      ean_next_attempt_at is null
                      or ean_next_attempt_at <= now()
                  )
                returning
                    id,
                    source_url,
                    source_product_id,
                    payload,
                    coalesce(ean_enrichment_status, 'pending') as ean_status,
                    coalesce(ean_content_miss_count, 0) as content_miss_count,
                    coalesce(ean_technical_failure_count, 0) as technical_failure_count
                """,
                (CLAIM_HOURS, link_id, SHOP_ID),
            )
            row = cur.fetchone()
            conn.commit()

    if not row:
        return None

    return EligibleLink(
        id=str(row[0]),
        source_url=str(row[1]),
        source_product_id=row[2],
        payload=dict(row[3] or {}),
        ean_status=str(row[4]),
        content_miss_count=int(row[5] or 0),
        technical_failure_count=int(row[6] or 0),
    )


def extract_og_image(soup: BeautifulSoup) -> str | None:
    for selector in [
        'meta[property="og:image"]',
        'meta[name="og:image"]',
        'meta[property="twitter:image"]',
        'meta[name="twitter:image"]',
    ]:
        tag = soup.select_one(selector)
        if tag and clean(tag.get("content")):
            return clean(tag.get("content"))
    return None


def looks_second_hand(soup: BeautifulSoup, details: dict[str, str]) -> bool:
    haystack = " ".join(
        [
            soup.get_text(" ", strip=True).lower(),
            " ".join(clean(v).lower() for v in details.values()),
        ]
    )
    markers = [
        "tweedehands",
        "2e hands",
        "second-hand",
        "second hand",
        "used vinyl",
    ]
    return any(marker in haystack for marker in markers)


def fetch_detail(session: requests.Session, link: EligibleLink) -> DetailOutcome:
    try:
        response = session.get(link.source_url, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        return DetailOutcome(
            kind="technical_error",
            http_status=None,
            error=str(exc),
        )

    status_code = int(response.status_code)

    if status_code in (404, 410):
        return DetailOutcome(
            kind="dead_link",
            http_status=status_code,
            note=f"HTTP {status_code}",
        )

    if status_code == 429 or status_code >= 500:
        return DetailOutcome(
            kind="technical_error",
            http_status=status_code,
            error=f"HTTP {status_code}",
        )

    try:
        response.raise_for_status()
    except requests.RequestException as exc:
        return DetailOutcome(
            kind="technical_error",
            http_status=status_code,
            error=str(exc),
        )

    soup = BeautifulSoup(response.text, "html.parser")
    details = extract_detail_table(soup)
    ean = clean(details.get("ean")) or None
    image_url = extract_og_image(soup)

    if ean:
        return DetailOutcome(
            kind="found",
            http_status=status_code,
            ean=ean,
            details=details,
            image_url=image_url,
        )

    if looks_second_hand(soup, details):
        return DetailOutcome(
            kind="second_hand",
            http_status=status_code,
            details=details,
            image_url=image_url,
            note="second_hand_without_ean",
        )

    return DetailOutcome(
        kind="content_miss",
        http_status=status_code,
        details=details,
        image_url=image_url,
        note="valid_detail_page_without_ean",
    )


def insert_raw_snapshot(link: EligibleLink, outcome: DetailOutcome) -> str:
    listing_payload = dict(link.payload)
    details = dict(outcome.details or {})

    raw_payload = {
        "detail_source": "platenzaak_detail_page",
        "outcome": outcome.kind,
        "http_status": outcome.http_status,
        "note": outcome.note,
        "error": outcome.error,
        "detail_fields": details,
        "listing_payload": listing_payload,
    }

    return insert_raw_shop_scrape(
        run_id=None,
        shop_id=SHOP_ID,
        source_url=link.source_url,
        source_product_id=link.source_product_id,
        title_raw=build_listing_title(listing_payload),
        ean_raw=outcome.ean or clean(details.get("ean")) or None,
        price_raw=None,  # listing-first policy: current price comes from listing refresh only
        availability_raw=clean(listing_payload.get("availability")) or None,
        image_url_raw=outcome.image_url,
        payload=raw_payload,
    )


def update_link_after_outcome(link: EligibleLink, outcome: DetailOutcome) -> str | None:
    database_url = load_database_url()
    raw_id: str | None = None

    if outcome.kind in {"found", "content_miss", "second_hand"}:
        raw_id = insert_raw_snapshot(link, outcome)

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            if outcome.kind == "found":
                cur.execute(
                    """
                    update public.shop_product_links
                    set
                        last_detail_scraped_at = now(),
                        ean_enrichment_status = 'found',
                        ean_last_attempt_at = now(),
                        ean_next_attempt_at = null,
                        ean_last_result = 'found',
                        ean_last_error = null,
                        ean_last_http_status = %s
                    where id = %s
                    """,
                    (outcome.http_status, link.id),
                )

            elif outcome.kind == "second_hand":
                cur.execute(
                    """
                    update public.shop_product_links
                    set
                        last_detail_scraped_at = now(),
                        ean_enrichment_status = 'second_hand_no_ean',
                        ean_last_attempt_at = now(),
                        ean_next_attempt_at = null,
                        ean_last_result = 'second_hand_no_ean',
                        ean_last_error = null,
                        ean_last_http_status = %s
                    where id = %s
                    """,
                    (outcome.http_status, link.id),
                )

            elif outcome.kind == "content_miss":
                new_count = link.content_miss_count + 1
                decision = content_miss_retry(new_count)
                cur.execute(
                    """
                    update public.shop_product_links
                    set
                        last_detail_scraped_at = now(),
                        ean_enrichment_status = %s,
                        ean_content_miss_count = %s,
                        ean_last_attempt_at = now(),
                        ean_next_attempt_at = now() + (%s * interval '1 hour'),
                        ean_last_result = %s,
                        ean_last_error = null,
                        ean_last_http_status = %s
                    where id = %s
                    """,
                    (
                        decision.status,
                        new_count,
                        decision.delay_hours,
                        decision.reason,
                        outcome.http_status,
                        link.id,
                    ),
                )

            elif outcome.kind == "technical_error":
                new_count = link.technical_failure_count + 1
                decision = technical_failure_retry(new_count)
                cur.execute(
                    """
                    update public.shop_product_links
                    set
                        ean_enrichment_status = %s,
                        ean_technical_failure_count = %s,
                        ean_last_attempt_at = now(),
                        ean_next_attempt_at = now() + (%s * interval '1 hour'),
                        ean_last_result = %s,
                        ean_last_error = %s,
                        ean_last_http_status = %s
                    where id = %s
                    """,
                    (
                        decision.status,
                        new_count,
                        decision.delay_hours,
                        decision.reason,
                        outcome.error,
                        outcome.http_status,
                        link.id,
                    ),
                )

            elif outcome.kind == "dead_link":
                cur.execute(
                    """
                    update public.shop_product_links
                    set
                        status = 'inactive',
                        ean_enrichment_status = 'not_applicable',
                        ean_last_attempt_at = now(),
                        ean_next_attempt_at = null,
                        ean_last_result = 'dead_link',
                        ean_last_error = null,
                        ean_last_http_status = %s
                    where id = %s
                    """,
                    (outcome.http_status, link.id),
                )

            else:
                raise RuntimeError(f"Onbekende outcome kind: {outcome.kind}")

        conn.commit()

    return raw_id


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch eligible Platenzaak detailpagina's voor EAN-verrijking "
            "met large-catalog retrybeleid."
        )
    )
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--delay-seconds", type=float, default=0.5)
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Schrijf raw snapshots en retrystatussen. Zonder --write is dit dry-run.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")
    if args.delay_seconds < 0:
        raise SystemExit("[ERROR] --delay-seconds mag niet negatief zijn.")
    if args.sample_size < 0:
        raise SystemExit("[ERROR] --sample-size mag niet negatief zijn.")

    links = get_eligible_links(args.limit)

    print(
        "[DETAIL] eligible "
        + json.dumps(
            {
                "shop": SHOP_ID,
                "eligible": len(links),
                "limit": args.limit,
                "write": bool(args.write),
            },
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )

    if not links:
        print("[DETAIL] niets te doen.", flush=True)
        return 0

    session = build_session()

    for idx, initial_link in enumerate(links, start=1):
        if args.write:
            link = claim_link(initial_link.id)
            if link is None:
                print(
                    "[DETAIL] skip "
                    + json.dumps(
                        {
                            "idx": idx,
                            "source_url": initial_link.source_url,
                            "reason": "niet_meer_eligible",
                        },
                        ensure_ascii=False,
                        sort_keys=True,
                    ),
                    flush=True,
                )
                continue
        else:
            link = initial_link

        outcome = fetch_detail(session, link)
        raw_id = None

        if args.write:
            raw_id = update_link_after_outcome(link, outcome)

        print(
            "[DETAIL] result "
            + json.dumps(
                {
                    "idx": idx,
                    "source_url": link.source_url,
                    "previous_status": link.ean_status,
                    "content_misses": link.content_miss_count,
                    "technical_failures": link.technical_failure_count,
                    "outcome": outcome.kind,
                    "ean": outcome.ean,
                    "http_status": outcome.http_status,
                    "raw_id": raw_id,
                    "write": bool(args.write),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )

        if idx < len(links) and args.delay_seconds > 0:
            time.sleep(args.delay_seconds)

    if not args.write:
        print("[DETAIL] dry-run complete; geen databasewrites.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
