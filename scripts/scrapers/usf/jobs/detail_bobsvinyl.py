#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from typing import Any

import requests
from bs4 import BeautifulSoup
from psycopg.types.json import Jsonb

from scripts.scrapers.bobsvinyl import (
    make_session,
    parse_detail_result,
)
from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.large_catalog_policy import (
    content_miss_retry,
    technical_failure_retry,
)


SHOP_ID = "bobsvinyl"
DEFAULT_LIMIT = 5
DEFAULT_SLEEP_SECONDS = 0.50
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
    second_hand: str | None = None
    note: str | None = None
    error: str | None = None


def clean(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def build_listing_title(payload: dict[str, Any]) -> str | None:
    artist = clean(payload.get("artist"))
    title = clean(payload.get("title"))

    if artist and title:
        return f"{artist} - {title}"

    return title or artist


def get_eligible_links(limit: int) -> list[EligibleLink]:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                    id,
                    source_url,
                    source_product_id,
                    payload,
                    ean_enrichment_status,
                    ean_content_miss_count,
                    ean_technical_failure_count
                from public.shop_product_links
                where shop_id = %s
                  and status = 'active'
                  and ean_enrichment_status in (
                      'pending',
                      'not_found',
                      'technical_error'
                  )
                  and (
                      ean_next_attempt_at is null
                      or ean_next_attempt_at <= now()
                  )
                order by
                    case ean_enrichment_status
                        when 'pending' then 1
                        when 'technical_error' then 2
                        when 'not_found' then 3
                        else 4
                    end,
                    ean_next_attempt_at asc nulls first,
                    first_seen_at asc,
                    id asc
                limit %s
                """,
                (SHOP_ID, limit),
            )
            rows = cur.fetchall()

    return [
        EligibleLink(
            id=str(row[0]),
            source_url=str(row[1]),
            source_product_id=clean(row[2]),
            payload=dict(row[3] or {}),
            ean_status=str(row[4]),
            content_miss_count=int(row[5] or 0),
            technical_failure_count=int(row[6] or 0),
        )
        for row in rows
    ]


def claim_link(link_id: str) -> EligibleLink | None:
    """
    Tijdelijke claim. Bij een procescrash wordt de link na CLAIM_HOURS
    automatisch opnieuw eligible.
    """
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.shop_product_links
                set
                    ean_next_attempt_at =
                        now() + (%s * interval '1 hour'),
                    ean_last_result = 'processing'
                where id = %s
                  and shop_id = %s
                  and status = 'active'
                  and ean_enrichment_status in (
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
                    ean_enrichment_status,
                    ean_content_miss_count,
                    ean_technical_failure_count
                """,
                (CLAIM_HOURS, link_id, SHOP_ID),
            )
            row = cur.fetchone()

    if row is None:
        return None

    return EligibleLink(
        id=str(row[0]),
        source_url=str(row[1]),
        source_product_id=clean(row[2]),
        payload=dict(row[3] or {}),
        ean_status=str(row[4]),
        content_miss_count=int(row[5] or 0),
        technical_failure_count=int(row[6] or 0),
    )


def fetch_detail(
    session: requests.Session,
    link: EligibleLink,
) -> DetailOutcome:
    try:
        response = session.get(
            link.source_url,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as exc:
        return DetailOutcome(
            kind="technical_error",
            http_status=None,
            error=f"{type(exc).__name__}: {exc}",
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
            error=f"{type(exc).__name__}: {exc}",
        )

    soup = BeautifulSoup(response.text, "html.parser")
    valid_page, ean, second_hand, validation_note = parse_detail_result(soup)

    if not valid_page:
        return DetailOutcome(
            kind="technical_error",
            http_status=status_code,
            note=validation_note,
            error=f"ongeldige_productpagina: {validation_note}",
        )

    if clean(ean):
        return DetailOutcome(
            kind="found",
            http_status=status_code,
            ean=clean(ean),
            second_hand=second_hand,
            note=validation_note,
        )

    if second_hand == "JA":
        return DetailOutcome(
            kind="second_hand",
            http_status=status_code,
            second_hand=second_hand,
            note="Tweedehandsproduct zonder zichtbare EAN",
        )

    return DetailOutcome(
        kind="content_miss",
        http_status=status_code,
        second_hand=second_hand,
        note="Geldige productpagina, maar geen EAN gevonden",
    )


def insert_raw_snapshot(
    cur: Any,
    *,
    link: EligibleLink,
    outcome: DetailOutcome,
) -> str:
    listing_payload = dict(link.payload)

    raw_payload = {
        "source": "detail_bobsvinyl",
        "shop_product_link_id": link.id,
        "detail_result": outcome.kind,
        "http_status": outcome.http_status,
        "second_hand": outcome.second_hand,
        "detail_note": outcome.note,
        "listing_payload": listing_payload,
    }

    cur.execute(
        """
        insert into public.raw_shop_scrapes (
            run_id,
            shop_id,
            source_url,
            source_product_id,
            title_raw,
            ean_raw,
            price_raw,
            availability_raw,
            image_url_raw,
            payload
        )
        values (
            null,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        returning id
        """,
        (
            SHOP_ID,
            link.source_url,
            link.source_product_id,
            build_listing_title(listing_payload),
            outcome.ean,
            None,  # listing-first policy: detail jobs do not write current prices
            None,
            clean(listing_payload.get("image_url")),
            Jsonb(raw_payload),
        ),
    )

    return str(cur.fetchone()[0])


def store_outcome(
    *,
    link: EligibleLink,
    outcome: DetailOutcome,
) -> str | None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            raw_id: str | None = None

            if outcome.kind in {"found", "content_miss", "second_hand"}:
                raw_id = insert_raw_snapshot(
                    cur,
                    link=link,
                    outcome=outcome,
                )

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
                        ean_next_attempt_at =
                            now() + (%s * interval '1 hour'),
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
                        ean_next_attempt_at =
                            now() + (%s * interval '1 hour'),
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
                        outcome.error or outcome.note,
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
                        last_detail_scraped_at = now(),
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
                raise RuntimeError(
                    f"Onbekend detailresultaat: {outcome.kind}"
                )

            if cur.rowcount != 1:
                raise RuntimeError(
                    f"Linkstatus kon niet worden bijgewerkt: {link.id}"
                )

    return raw_id


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verrijk eligible Bob's Vinyl registrylinks via detailpagina's "
            "met het generieke large-catalog EAN-retrybeleid."
        )
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Maximaal aantal eligible links; standaard 5.",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP_SECONDS,
        help="Pauze tussen detailrequests.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help=(
            "Schrijf raw snapshots en retrystatussen. "
            "Zonder deze vlag is dit een dry-run."
        ),
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    if args.sleep < 0:
        raise SystemExit("[ERROR] --sleep mag niet negatief zijn.")

    links = get_eligible_links(args.limit)

    print(
        "[DETAIL]",
        {
            "shop": SHOP_ID,
            "eligible": len(links),
            "limit": args.limit,
            "write": args.write,
        },
        flush=True,
    )

    session = make_session()
    processed = 0

    for index, initial_link in enumerate(links, start=1):
        if args.write:
            link = claim_link(initial_link.id)
            if link is None:
                print(
                    "[DETAIL][SKIP]",
                    {
                        "link_id": initial_link.id,
                        "reason": "niet_meer_eligible",
                    },
                    flush=True,
                )
                continue
        else:
            link = initial_link

        outcome = fetch_detail(session, link)

        print(
            "[DETAIL-RESULT]",
            {
                "index": index,
                "link_id": link.id,
                "source_url": link.source_url,
                "previous_status": link.ean_status,
                "content_misses": link.content_miss_count,
                "technical_failures": link.technical_failure_count,
                "result": outcome.kind,
                "ean": outcome.ean,
                "second_hand": outcome.second_hand,
                "http_status": outcome.http_status,
                "note": outcome.note,
                "error": outcome.error,
            },
            flush=True,
        )

        if args.write:
            raw_id = store_outcome(
                link=link,
                outcome=outcome,
            )
            print(
                "[DETAIL-WRITE]",
                {
                    "link_id": link.id,
                    "raw_id": raw_id,
                    "result": outcome.kind,
                },
                flush=True,
            )

        processed += 1

        if index < len(links):
            time.sleep(args.sleep)

    print(
        "[DETAIL] complete",
        {
            "processed": processed,
            "write": args.write,
        },
        flush=True,
    )

    if not args.write:
        print(
            "[DETAIL] dry-run complete; geen databasewrites.",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
