from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from psycopg.rows import dict_row

from scripts.importers.common import (
    normalize_ean,
    normalize_text,
    parse_price,
)
from scripts.scrapers.usf.core.db import db_connection


VALID_AVAILABILITY = {
    "in_stock",
    "out_of_stock",
    "preorder",
    "unknown",
}


@dataclass(frozen=True)
class StageItem:
    raw_scrape_id: str
    staged_offer_id: str | None
    source_url: str
    ean_normalized: str | None
    price: Decimal | None
    availability: str
    stage_reason: str | None


@dataclass(frozen=True)
class StageResult:
    candidates: int
    inserted: int
    skipped: int
    items: tuple[StageItem, ...]


def normalize_title(value: Any) -> str | None:
    text = normalize_text(value)
    if not text:
        return None

    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


def normalize_availability(value: Any) -> str:
    text = normalize_text(value).lower()

    if not text:
        return "unknown"

    text = text.replace("-", "_").replace(" ", "_")

    mappings = {
        "instock": "in_stock",
        "in_stock": "in_stock",
        "op_voorraad": "in_stock",
        "available": "in_stock",
        "outofstock": "out_of_stock",
        "out_of_stock": "out_of_stock",
        "uitverkocht": "out_of_stock",
        "sold_out": "out_of_stock",
        "pre_order": "preorder",
        "preorder": "preorder",
        "unknown": "unknown",
    }

    normalized = mappings.get(text, text)

    if normalized not in VALID_AVAILABILITY:
        return "unknown"

    return normalized


def normalize_price(value: Any) -> Decimal | None:
    parsed = parse_price(value)
    if parsed is None:
        return None

    return Decimal(str(parsed)).quantize(Decimal("0.01"))


def fetch_latest_unstaged_raw_rows(
    conn,
    *,
    shop_id: str,
    limit: int,
    lock_rows: bool,
) -> list[dict[str, Any]]:
    sql = """
        with latest_raw as (
            select distinct on (r.shop_id, r.source_url)
                r.id
            from public.raw_shop_scrapes r
            where r.shop_id = %s
            order by
                r.shop_id,
                r.source_url,
                r.scraped_at desc nulls last,
                r.id desc
        )
        select
            r.id,
            r.run_id,
            r.shop_id,
            r.source_url,
            r.source_product_id,
            r.title_raw,
            r.ean_raw,
            r.price_raw,
            r.availability_raw,
            r.image_url_raw,
            r.payload,
            r.scraped_at
        from latest_raw latest
        join public.raw_shop_scrapes r
          on r.id = latest.id
        left join public.staged_offers s
          on s.raw_scrape_id = r.id
        where s.id is null
          and r.ean_raw is not null
          and r.price_raw is not null
        order by
            r.scraped_at asc nulls last,
            r.id asc
        limit %s
    """

    if lock_rows:
        sql += " for update of r skip locked"

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, (shop_id, limit))
        return [dict(row) for row in cur.fetchall()]


def build_stage_item(row: dict[str, Any]) -> StageItem:
    raw_scrape_id = str(row["id"])
    source_url = normalize_text(row.get("source_url"))

    ean_normalized = normalize_ean(row.get("ean_raw"))
    price = normalize_price(row.get("price_raw"))
    title_normalized = normalize_title(
        row.get("title_raw") or row.get("source_product_id")
    )
    availability = normalize_availability(
        row.get("availability_raw")
    )

    missing = []

    if not ean_normalized:
        missing.append("missing_ean")

    if price is None:
        missing.append("missing_price")

    if not title_normalized:
        missing.append("missing_title")

    stage_reason = ";".join(missing) if missing else None

    return StageItem(
        raw_scrape_id=raw_scrape_id,
        staged_offer_id=None,
        source_url=source_url,
        ean_normalized=ean_normalized,
        price=price,
        availability=availability,
        stage_reason=stage_reason,
    )


def stage_latest_raw_snapshots(
    *,
    shop_id: str,
    limit: int,
    write: bool,
) -> StageResult:
    if limit < 1:
        raise ValueError("limit moet minimaal 1 zijn")

    items: list[StageItem] = []
    inserted = 0
    skipped = 0

    with db_connection() as conn:
        rows = fetch_latest_unstaged_raw_rows(
            conn,
            shop_id=shop_id,
            limit=limit,
            lock_rows=write,
        )

        with conn.cursor() as cur:
            for row in rows:
                preview = build_stage_item(row)

                if preview.stage_reason:
                    skipped += 1
                    items.append(preview)
                    continue

                title_normalized = normalize_title(
                    row.get("title_raw")
                    or row.get("source_product_id")
                )
                image_url = normalize_text(
                    row.get("image_url_raw")
                ) or None

                staged_offer_id: str | None = None

                if write:
                    cur.execute(
                        """
                        insert into public.staged_offers (
                            raw_scrape_id,
                            run_id,
                            shop_id,
                            source_url,
                            source_product_id,
                            title_normalized,
                            ean_normalized,
                            ean_match_key,
                            price,
                            currency,
                            availability,
                            image_url,
                            stage_status,
                            stage_reason
                        )
                        values (
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            %s,
                            'EUR',
                            %s,
                            %s,
                            'staged',
                            null
                        )
                        returning id
                        """,
                        (
                            row["id"],
                            row.get("run_id"),
                            shop_id,
                            row.get("source_url"),
                            row.get("source_product_id"),
                            title_normalized,
                            preview.ean_normalized,
                            preview.ean_normalized,
                            preview.price,
                            preview.availability,
                            image_url,
                        ),
                    )
                    staged_offer_id = str(cur.fetchone()[0])
                    inserted += 1

                items.append(
                    StageItem(
                        raw_scrape_id=preview.raw_scrape_id,
                        staged_offer_id=staged_offer_id,
                        source_url=preview.source_url,
                        ean_normalized=preview.ean_normalized,
                        price=preview.price,
                        availability=preview.availability,
                        stage_reason=None,
                    )
                )

    return StageResult(
        candidates=len(items),
        inserted=inserted,
        skipped=skipped,
        items=tuple(items),
    )
