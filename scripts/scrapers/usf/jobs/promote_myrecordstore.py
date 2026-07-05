from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from scripts.importers.common import (
    CanonicalRecord,
    ImportConfig,
    ensure_shop,
    infer_artist_title,
    maybe_insert_history,
    maybe_upsert_cover_candidate,
    normalize_ean,
    normalize_gtin14,
    normalize_text,
    upsert_price,
    upsert_product,
)

SHOP_ID = "myrecordstore"
SHOP_NAME = "My Record Store"
SHOP_DOMAIN = "myrecordstore.nl"
SHOP_COUNTRY = "NL"


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
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def infer_format_label(title: str | None) -> str | None:
    text = normalize_text(title).lower()
    if "5 lp" in text or "5-lp" in text or "5lp" in text:
        return "5LP"
    if "4 lp" in text or "4-lp" in text or "4lp" in text:
        return "4LP"
    if "3 lp" in text or "3-lp" in text or "3lp" in text:
        return "3LP"
    if "2 lp" in text or "2-lp" in text or "2lp" in text:
        return "2LP"
    if "lp" in text or "vinyl" in text:
        return "LP"
    return "LP"


def clean_title_raw(value: str | None) -> str:
    text = normalize_text(value)
    text = re.sub(r"\s*(?:–|—|-|\|)\s*My\s*Record\s*Store\s*$", "", text, flags=re.I)
    return normalize_text(text)


def fetch_staged_rows(conn: psycopg.Connection, limit: int) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select
                s.id as staged_offer_id,
                s.raw_scrape_id,
                s.shop_id,
                s.source_url,
                s.source_product_id,
                s.title_normalized,
                s.ean_normalized,
                s.ean_match_key,
                s.price,
                s.currency,
                s.availability,
                s.image_url,
                s.stage_status,
                s.stage_reason,
                s.created_at as staged_at,
                r.title_raw,
                r.scraped_at,
                r.payload as raw_payload
            from public.staged_offers s
            left join public.raw_shop_scrapes r
              on r.id = s.raw_scrape_id
            where s.shop_id = %s
              and s.stage_status = 'staged'
              and s.ean_match_key is not null
              and s.price is not null
              and coalesce(s.availability, 'unknown') <> 'out_of_stock'
            order by s.created_at asc
            limit %s
            """,
            (SHOP_ID, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def staged_row_to_record(row: dict[str, Any], line_number: int) -> CanonicalRecord:
    ean = normalize_ean(row.get("ean_match_key") or row.get("ean_normalized"))
    if not ean:
        raise ValueError(f"missing usable EAN for staged_offer_id={row.get('staged_offer_id')}")

    raw_payload = row.get("raw_payload") or {}
    listing_payload = raw_payload.get("listing_payload") if isinstance(raw_payload, dict) else {}
    if not isinstance(listing_payload, dict):
        listing_payload = {}

    title_raw = (
        clean_title_raw(row.get("title_raw"))
        or normalize_text(listing_payload.get("title"))
        or normalize_text(row.get("title_normalized"))
    )

    artist = normalize_text(listing_payload.get("artist_raw"))
    title = normalize_text(listing_payload.get("title_raw"))

    if not title:
        inferred_artist, inferred_title = infer_artist_title(None, title_raw)
        artist = artist or normalize_text(inferred_artist)
        title = normalize_text(inferred_title) or title_raw

    if not artist:
        artist = ""

    price = row.get("price")
    if price is None:
        raise ValueError(f"missing price for staged_offer_id={row.get('staged_offer_id')}")

    availability = normalize_text(row.get("availability")).lower() or "unknown"
    if availability not in {"in_stock", "out_of_stock", "preorder", "unknown"}:
        availability = "unknown"

    captured_at = row.get("scraped_at") or row.get("staged_at") or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)

    image_url = normalize_text(row.get("image_url")) or normalize_text(listing_payload.get("image_url")) or None
    product_url = normalize_text(row.get("source_url"))

    return CanonicalRecord(
        source_row_number=line_number,
        shop_name=SHOP_NAME,
        shop_domain=SHOP_DOMAIN,
        shop_country=SHOP_COUNTRY,
        ean=ean,
        artist=artist,
        title=title,
        format_label=normalize_text(listing_payload.get("format")) or infer_format_label(title_raw),
        cover_url=image_url,
        product_url=product_url,
        price=float(price),
        currency=normalize_text(row.get("currency")) or "EUR",
        availability=availability,
        captured_at=captured_at,
        product_handle=normalize_text(row.get("source_product_id")) or None,
        detail_status="ok",
        is_secondhand=False,
        raw=dict(row),
        cover_candidate_url=image_url,
        cover_candidate_source_type="shop_listing_image" if image_url else None,
        cover_candidate_page_url=product_url if image_url else None,
        cover_candidate_queue_priority=90 if image_url else None,
        gtin_normalized=normalize_gtin14(ean),
    )


def mark_promoted(cur, staged_offer_id: str, *, status: str, reason: str | None) -> None:
    cur.execute(
        """
        update public.staged_offers
        set stage_status = %s,
            stage_reason = %s
        where id = %s
        """,
        (status, reason, staged_offer_id),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Promote My Record Store staged USF offers.")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    load_env()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("[ERROR] DATABASE_URL ontbreekt.")

    dry_run = not args.write
    config = ImportConfig(shop_name=SHOP_NAME, shop_domain=SHOP_DOMAIN, shop_country=SHOP_COUNTRY, currency="EUR")

    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        rows = fetch_staged_rows(conn, args.limit)
        print(f"[MYRECORDSTORE-PROMOTE] shop={SHOP_ID} queued={len(rows)} dry_run={dry_run}")

        if dry_run:
            for idx, row in enumerate(rows, start=1):
                record = staged_row_to_record(row, idx)
                print("[DRY-RUN]", {
                    "staged_offer_id": str(row["staged_offer_id"]),
                    "ean": record.ean,
                    "artist": record.artist,
                    "title": record.title,
                    "price": record.price,
                    "availability": record.availability,
                    "product_url": record.product_url,
                })
            return 0

        stats = {"processed": 0, "failed": 0, "history_rows": 0, "cover_candidates": 0}
        with conn.cursor() as cur:
            shop_uuid = ensure_shop(cur, config)
            imported_at = datetime.now(timezone.utc)

            for idx, row in enumerate(rows, start=1):
                staged_offer_id = str(row["staged_offer_id"])
                try:
                    record = staged_row_to_record(row, idx)
                    product_id, _ = upsert_product(cur, record)
                    upsert_price(cur, product_id, shop_uuid, record, imported_at)
                    history_inserted = maybe_insert_history(cur, product_id, shop_uuid, record)
                    cover_inserted = maybe_upsert_cover_candidate(cur, product_id, shop_uuid, record)
                    mark_promoted(cur, staged_offer_id, status="promoted", reason=None)
                    conn.commit()

                    stats["processed"] += 1
                    stats["history_rows"] += int(history_inserted)
                    stats["cover_candidates"] += int(cover_inserted)
                    print(f"[MYRECORDSTORE-PROMOTE] {idx}/{len(rows)} ok ean={record.ean} product_id={product_id} price={record.price}")
                except Exception as exc:
                    conn.rollback()
                    stats["failed"] += 1
                    with conn.cursor() as err_cur:
                        mark_promoted(err_cur, staged_offer_id, status="promote_error", reason=str(exc)[:500])
                    conn.commit()
                    print(f"[MYRECORDSTORE-PROMOTE][WARN] failed staged_offer_id={staged_offer_id} error={exc}")

        print(f"[MYRECORDSTORE-PROMOTE] done {stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
