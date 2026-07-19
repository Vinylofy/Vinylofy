#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scripts.scrapers.usf.core.db import db_connection

SHOP_ID = "everythingjazz"


def json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return value


def fetch_candidates(limit: int) -> list[dict[str, Any]]:
    sql = """
        with latest_raw as (
            select distinct on (r.shop_id, r.source_url)
                r.*
            from public.raw_shop_scrapes r
            where r.shop_id = %s
            order by r.shop_id, r.source_url, r.scraped_at desc nulls last, r.id desc
        )
        select
            r.id as raw_scrape_id,
            r.shop_id,
            r.source_url,
            r.source_product_id,
            r.title_raw,
            r.ean_raw,
            r.price_raw,
            r.availability_raw,
            r.image_url_raw,
            r.scraped_at,
            r.parse_status,
            r.payload
        from latest_raw r
        left join public.staged_offers s on s.raw_scrape_id = r.id
        left join public.quarantine_offers q
            on q.shop_id = r.shop_id
           and q.source_url = r.source_url
           and q.resolved_at is null
        where s.id is null
          and q.id is null
          and (
              r.ean_raw is null
              or r.price_raw is null
              or coalesce(r.payload->>'detail_issue', '') <> ''
          )
        order by r.scraped_at asc nulls last, r.id asc
        limit %s
    """
    with db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, (SHOP_ID, limit))
            return [dict(row) for row in cur.fetchall()]


def classify(row: dict[str, Any]) -> tuple[str, str]:
    payload = row.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    issue = str(payload.get("detail_issue") or "").strip()
    mapping = {
        "unsupported_product_type": (
            "non_vinyl_product",
            "product.js bevestigt geen producttype Vinyl of Vinyl-Box.",
        ),
        "ambiguous_variant_ean": (
            "ambiguous_variant_ean",
            "Meerdere bestelbare varianten hebben verschillende publieke barcodes; één URL kan niet veilig aan één EAN worden gekoppeld.",
        ),
        "missing_variants": (
            "missing_variants",
            "product.js bevat geen bruikbare variants-lijst.",
        ),
        "missing_ean": (
            "missing_ean",
            "Geen geldige publieke barcode aangetroffen in de relevante varianten.",
        ),
        "detail_request_error": (
            "detail_request_error",
            "De product.js-detailbron kon niet betrouwbaar worden opgehaald of verwerkt.",
        ),
    }
    if issue in mapping:
        return mapping[issue]
    if not row.get("ean_raw") and not row.get("price_raw"):
        return (
            "missing_ean_and_listing_price",
            "De nieuwste detailsnapshot bevat geen geldige EAN en de registry bevat geen listingprijs.",
        )
    if not row.get("ean_raw"):
        return (
            "missing_ean",
            "De nieuwste detailsnapshot bevat geen geldige publieke EAN.",
        )
    if not row.get("price_raw"):
        return (
            "missing_listing_price",
            "De detailsnapshot bevat een EAN, maar de gekoppelde listing-snapshot bevat geen prijs.",
        )
    return (
        "not_stageable",
        "De nieuwste raw snapshot is niet gestaged; controleer de stagevalidatie.",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Quarantine Everything Jazz raw failures.")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    rows = fetch_candidates(args.limit)
    print(
        "[EVERYTHINGJAZZ-QUARANTINE]",
        {"candidates": len(rows), "write": args.write},
        flush=True,
    )
    inserted = 0
    if not args.write:
        for row in rows:
            issue_type, issue_detail = classify(row)
            print(
                "[EVERYTHINGJAZZ-QUARANTINE-PREVIEW]",
                {
                    "raw_scrape_id": str(row["raw_scrape_id"]),
                    "url": row.get("source_url"),
                    "issue_type": issue_type,
                    "issue_detail": issue_detail,
                },
                flush=True,
            )
        return 0

    with db_connection() as conn:
        with conn.cursor() as cur:
            for row in rows:
                issue_type, issue_detail = classify(row)
                payload = json_safe(
                    {
                        "raw_scrape_id": str(row["raw_scrape_id"]),
                        "source_product_id": row.get("source_product_id"),
                        "title_raw": row.get("title_raw"),
                        "ean_raw": row.get("ean_raw"),
                        "price_raw": row.get("price_raw"),
                        "availability_raw": row.get("availability_raw"),
                        "scraped_at": row.get("scraped_at"),
                        "parse_status": row.get("parse_status"),
                        "raw_payload": row.get("payload") or {},
                    }
                )
                cur.execute(
                    """
                    insert into public.quarantine_offers (
                        staged_offer_id,
                        shop_id,
                        source_url,
                        ean_normalized,
                        ean_match_key,
                        issue_type,
                        issue_detail,
                        payload
                    )
                    values (null, %s, %s, null, null, %s, %s, %s)
                    returning id
                    """,
                    (
                        SHOP_ID,
                        row["source_url"],
                        issue_type,
                        issue_detail,
                        Jsonb(payload),
                    ),
                )
                quarantine_id = str(cur.fetchone()[0])
                inserted += 1
                print(
                    "[EVERYTHINGJAZZ-QUARANTINE-ITEM]",
                    {
                        "quarantine_id": quarantine_id,
                        "url": row.get("source_url"),
                        "issue_type": issue_type,
                    },
                    flush=True,
                )
    print(
        "[EVERYTHINGJAZZ-QUARANTINE]",
        {"inserted": inserted, "write": True},
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
