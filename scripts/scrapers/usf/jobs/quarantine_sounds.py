from __future__ import annotations

import argparse
import html
import os
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


SHOP_ID = "sounds"


def load_env() -> None:
    for env_file in (".env.local", ".env"):
        path = Path(env_file)
        if not path.exists():
            continue

        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            if key and key not in os.environ:
                os.environ[key] = value


def is_non_catalog_product(row: dict[str, Any]) -> bool:
    combined = " ".join(
        str(row.get(field) or "")
        for field in ("source_product_id", "title_raw", "source_url")
    )
    combined = html.unescape(combined).lower()

    markers = (
        "cadeaukaart",
        "gift card",
        "giftcard",
        "voucher",
    )
    return any(marker in combined for marker in markers)


def classify_issue(row: dict[str, Any]) -> tuple[str, str]:
    if is_non_catalog_product(row):
        return (
            "non_catalog_product",
            "Product is geen publieke vinylrelease en heeft daarom geen EAN nodig.",
        )

    missing_ean = not row.get("ean_raw")
    missing_price = not row.get("price_raw")

    if missing_ean and missing_price:
        return (
            "missing_ean_and_price",
            "Nieuwste detail-snapshot bevat geen geldige EAN en geen prijs.",
        )

    if missing_ean:
        return (
            "missing_ean",
            "Nieuwste detail-snapshot bevat een prijs, maar geen geldige EAN.",
        )

    if missing_price:
        return (
            "missing_price",
            "Nieuwste detail-snapshot bevat een EAN, maar geen geldige prijs.",
        )

    return (
        "not_stageable",
        "Record is niet gestaged, hoewel EAN en prijs aanwezig lijken.",
    )


def fetch_candidates(
    conn: psycopg.Connection,
    limit: int,
) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            with ranked_raw as (
                select
                    r.*,
                    row_number() over (
                        partition by r.shop_id, r.source_url
                        order by r.scraped_at desc nulls last, r.id desc
                    ) as rn
                from public.raw_shop_scrapes r
                where r.shop_id = %s
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
            from ranked_raw r
            left join public.staged_offers s
              on s.raw_scrape_id = r.id
            where r.rn = 1
              and s.id is null
              and (
                    r.ean_raw is null
                 or r.price_raw is null
              )
            order by r.scraped_at desc nulls last
            limit %s
            """,
            (SHOP_ID, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def build_payload(row: dict[str, Any]) -> dict[str, Any]:
    scraped_at = row.get("scraped_at")

    return {
        "raw_scrape_id": str(row["raw_scrape_id"]),
        "source_product_id": row.get("source_product_id"),
        "title_raw": row.get("title_raw"),
        "ean_raw": row.get("ean_raw"),
        "price_raw": row.get("price_raw"),
        "availability_raw": row.get("availability_raw"),
        "image_url_raw": row.get("image_url_raw"),
        "scraped_at": scraped_at.isoformat() if scraped_at else None,
        "parse_status": row.get("parse_status"),
        "raw_payload": row.get("payload") or {},
    }


def upsert_quarantine(
    conn: psycopg.Connection,
    row: dict[str, Any],
    issue_type: str,
    issue_detail: str,
) -> str:
    payload = build_payload(row)

    with conn.cursor() as cur:
        cur.execute(
            """
            select id
            from public.quarantine_offers
            where shop_id = %s
              and source_url = %s
              and resolved_at is null
            order by created_at desc
            limit 1
            """,
            (SHOP_ID, row["source_url"]),
        )
        existing = cur.fetchone()

        if existing:
            quarantine_id = str(existing[0])

            cur.execute(
                """
                update public.quarantine_offers
                set
                    ean_normalized = null,
                    ean_match_key = null,
                    issue_type = %s,
                    issue_detail = %s,
                    payload = %s
                where id = %s
                """,
                (
                    issue_type,
                    issue_detail,
                    Jsonb(payload),
                    quarantine_id,
                ),
            )
            return "updated"

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
            values (
                null,
                %s,
                %s,
                null,
                null,
                %s,
                %s,
                %s
            )
            """,
            (
                SHOP_ID,
                row["source_url"],
                issue_type,
                issue_detail,
                Jsonb(payload),
            ),
        )
        return "inserted"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Schrijf records naar quarantine_offers. Zonder --write is dit een dry-run.",
    )
    args = parser.parse_args()

    load_env()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("[ERROR] DATABASE_URL ontbreekt.")

    dry_run = not args.write

    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        rows = fetch_candidates(conn, args.limit)

        print(
            f"[QUARANTINE] shop={SHOP_ID} "
            f"queued={len(rows)} dry_run={dry_run}"
        )

        inserted = 0
        updated = 0

        for index, row in enumerate(rows, start=1):
            issue_type, issue_detail = classify_issue(row)

            print(
                f"[QUARANTINE] {index}/{len(rows)} "
                f"issue={issue_type} "
                f"product={row.get('source_product_id')} "
                f"url={row.get('source_url')}"
            )

            if dry_run:
                continue

            result = upsert_quarantine(
                conn,
                row,
                issue_type,
                issue_detail,
            )
            conn.commit()

            if result == "inserted":
                inserted += 1
            else:
                updated += 1

        print(
            f"[QUARANTINE] done "
            f"inserted={inserted} updated={updated}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
