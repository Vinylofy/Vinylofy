from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scripts.scrapers.usf.core.db import db_connection


@dataclass(frozen=True)
class QuarantineItem:
    staged_offer_id: str
    source_url: str
    issue_type: str
    issue_detail: str
    quarantine_id: str | None = None


@dataclass(frozen=True)
class QuarantineResult:
    candidates: int
    inserted: int
    items: tuple[QuarantineItem, ...]


def json_safe(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return str(value)

    if isinstance(value, dict):
        return {
            str(key): json_safe(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]

    return value


def classify_promote_error(stage_reason: str | None) -> tuple[str, str]:
    reason = str(stage_reason or "").strip()

    if "missing artist after inference" in reason.lower():
        return (
            "missing_artist",
            "EAN en prijs zijn aanwezig, maar er is geen betrouwbare artiestbron.",
        )

    return (
        "promotion_error",
        reason or "Promotion is mislukt zonder aanvullende foutomschrijving.",
    )


def fetch_unquarantined_promote_errors(
    conn,
    *,
    shop_id: str,
    limit: int,
    lock_rows: bool,
) -> list[dict[str, Any]]:
    sql = """
        select
            s.id as staged_offer_id,
            s.shop_id,
            s.source_url,
            s.source_product_id,
            s.ean_normalized,
            s.ean_match_key,
            s.price,
            s.currency,
            s.availability,
            s.stage_status,
            s.stage_reason,
            s.created_at as staged_at,
            r.id as raw_scrape_id,
            r.title_raw,
            r.ean_raw,
            r.price_raw,
            r.availability_raw,
            r.image_url_raw,
            r.scraped_at,
            r.parse_status,
            r.payload as raw_payload
        from public.staged_offers s
        left join public.raw_shop_scrapes r
          on r.id = s.raw_scrape_id
        left join public.quarantine_offers q
          on q.staged_offer_id = s.id
         and q.resolved_at is null
        where s.shop_id = %s
          and s.stage_status = 'promote_error'
          and q.id is null
        order by s.created_at asc, s.id asc
        limit %s
    """

    if lock_rows:
        sql += " for update of s skip locked"

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (shop_id, limit))
        return [dict(row) for row in cur.fetchall()]


def build_payload(row: dict[str, Any]) -> dict[str, Any]:
    return json_safe(
        {
            "staged_offer_id": str(row["staged_offer_id"]),
            "raw_scrape_id": (
                str(row["raw_scrape_id"])
                if row.get("raw_scrape_id")
                else None
            ),
            "source_product_id": row.get("source_product_id"),
            "title_raw": row.get("title_raw"),
            "ean_raw": row.get("ean_raw"),
            "price_raw": row.get("price_raw"),
            "availability_raw": row.get("availability_raw"),
            "image_url_raw": row.get("image_url_raw"),
            "staged_price": row.get("price"),
            "staged_currency": row.get("currency"),
            "staged_availability": row.get("availability"),
            "stage_status": row.get("stage_status"),
            "stage_reason": row.get("stage_reason"),
            "staged_at": row.get("staged_at"),
            "scraped_at": row.get("scraped_at"),
            "parse_status": row.get("parse_status"),
            "raw_payload": row.get("raw_payload") or {},
        }
    )


def quarantine_promote_errors(
    *,
    shop_id: str,
    limit: int,
    write: bool,
) -> QuarantineResult:
    if limit < 1:
        raise ValueError("limit moet minimaal 1 zijn")

    items: list[QuarantineItem] = []
    inserted = 0

    with db_connection() as conn:
        rows = fetch_unquarantined_promote_errors(
            conn,
            shop_id=shop_id,
            limit=limit,
            lock_rows=write,
        )

        with conn.cursor() as cur:
            for row in rows:
                issue_type, issue_detail = classify_promote_error(
                    row.get("stage_reason")
                )

                quarantine_id: str | None = None

                if write:
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
                        values (%s, %s, %s, %s, %s, %s, %s, %s)
                        returning id
                        """,
                        (
                            row["staged_offer_id"],
                            shop_id,
                            row["source_url"],
                            row.get("ean_normalized"),
                            row.get("ean_match_key"),
                            issue_type,
                            issue_detail,
                            Jsonb(build_payload(row)),
                        ),
                    )
                    quarantine_id = str(cur.fetchone()[0])
                    inserted += 1

                items.append(
                    QuarantineItem(
                        staged_offer_id=str(row["staged_offer_id"]),
                        source_url=row["source_url"],
                        issue_type=issue_type,
                        issue_detail=issue_detail,
                        quarantine_id=quarantine_id,
                    )
                )

    return QuarantineResult(
        candidates=len(items),
        inserted=inserted,
        items=tuple(items),
    )
