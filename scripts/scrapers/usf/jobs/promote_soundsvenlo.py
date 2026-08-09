#!/usr/bin/env python3
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


SHOP_ID = "soundsvenlo"
SHOP_NAME = "Sounds Venlo"
SHOP_DOMAIN = "sounds-venlo.nl"
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
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def get_listing_payload(raw_payload: Any) -> dict[str, Any]:
    if not isinstance(raw_payload, dict):
        return {}
    listing_payload = raw_payload.get("listing_payload")
    return listing_payload if isinstance(listing_payload, dict) else {}


def clean_public_text(value: Any) -> str:
    text = normalize_text(value)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def split_artist_title_from_raw(title_raw: Any) -> tuple[str, str]:
    text = clean_public_text(title_raw)
    if not text:
        return "", ""

    # Sounds Venlo raw title is intentionally stored as:
    # "Artist | Album". Ignore any later format/EAN fragments if they ever occur.
    parts = [clean_public_text(part) for part in text.split("|")]
    parts = [part for part in parts if part]
    if len(parts) >= 2:
        return parts[0], parts[1]

    artist, title = infer_artist_title(None, text)
    return clean_public_text(artist), clean_public_text(title)


def infer_format_label(*values: Any) -> str | None:
    combined = " ".join(clean_public_text(value) for value in values if value)
    text = combined.lower()
    patterns = (
        (r"\b5\s*[-]?\s*lp\b", "5LP"),
        (r"\b4\s*[-]?\s*lp\b", "4LP"),
        (r"\b3\s*[-]?\s*lp\b", "3LP"),
        (r"\b2\s*[-]?\s*lp\b", "2LP"),
        (r"\blp\b", "LP"),
        (r"\b12\s*(?:inch|inches|\")\b", "12 inch"),
        (r"\b10\s*(?:inch|inches|\")\b", "10 inch"),
        (r"\b7\s*(?:inch|inches|\")\b", "7 inch"),
        (r"\bcd\b", "CD"),
    )
    for pattern, label in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return label
    return None


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
            order by s.created_at asc, s.id asc
            limit %s
            """,
            (SHOP_ID, limit),
        )
        return [dict(row) for row in cur.fetchall()]


def staged_row_to_record(row: dict[str, Any], line_number: int) -> CanonicalRecord:
    ean = normalize_ean(row.get("ean_match_key") or row.get("ean_normalized"))
    if not ean:
        raise ValueError(
            f"missing usable EAN for staged_offer_id={row.get('staged_offer_id')}"
        )

    listing_payload = get_listing_payload(row.get("raw_payload"))
    artist = clean_public_text(listing_payload.get("artist"))
    title = clean_public_text(listing_payload.get("title"))

    if not artist or not title:
        fallback_artist, fallback_title = split_artist_title_from_raw(row.get("title_raw"))
        artist = artist or fallback_artist
        title = title or fallback_title

    # Final guardrail: never publish EAN/format fragments as public album title.
    title_parts = [clean_public_text(part) for part in title.split("|")]
    title_parts = [part for part in title_parts if part]
    if title_parts:
        title = title_parts[0]

    if not artist:
        raise ValueError(
            f"missing artist after inference for staged_offer_id={row.get('staged_offer_id')}"
        )
    if not title:
        raise ValueError(
            f"missing title for staged_offer_id={row.get('staged_offer_id')}"
        )

    price = row.get("price")
    if price is None:
        raise ValueError(
            f"missing price for staged_offer_id={row.get('staged_offer_id')}"
        )

    availability = normalize_text(row.get("availability")).lower() or "unknown"
    if availability not in {"in_stock", "out_of_stock", "preorder", "unknown"}:
        availability = "unknown"

    captured_at = row.get("scraped_at") or row.get("staged_at") or datetime.now(timezone.utc)
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)

    image_url = normalize_text(row.get("image_url")) or None
    product_url = normalize_text(row.get("source_url"))

    return CanonicalRecord(
        source_row_number=line_number,
        shop_name=SHOP_NAME,
        shop_domain=SHOP_DOMAIN,
        shop_country=SHOP_COUNTRY,
        ean=ean,
        artist=artist,
        title=title,
        format_label=infer_format_label(
            listing_payload.get("format"),
            row.get("title_raw"),
        ),
        cover_url=None,
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
        cover_candidate_source_type="shop_detail_image" if image_url else None,
        cover_candidate_page_url=product_url if image_url else None,
        cover_candidate_queue_priority=60 if image_url else None,
        gtin_normalized=normalize_gtin14(ean),
    )


def mark_promoted(
    cur: psycopg.Cursor,
    staged_offer_id: str,
    *,
    status: str,
    reason: str | None,
) -> None:
    cur.execute(
        """
        update public.staged_offers
           set stage_status = %s,
               stage_reason = %s
         where id = %s
        """,
        (status, reason, staged_offer_id),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Promote Sounds Venlo staged offers naar products/prices."
    )
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Voer echte writes uit naar products/prices/price_history.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    load_env()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("[ERROR] DATABASE_URL ontbreekt. Zet DATABASE_URL in je environment of .env.")

    dry_run = not args.write
    config = ImportConfig(
        shop_name=SHOP_NAME,
        shop_domain=SHOP_DOMAIN,
        shop_country=SHOP_COUNTRY,
        currency="EUR",
    )

    with psycopg.connect(database_url, prepare_threshold=None) as conn:
        rows = fetch_staged_rows(conn, args.limit)
        print(
            "[PROMOTE]",
            {"shop": SHOP_ID, "queued": len(rows), "dry_run": dry_run},
            flush=True,
        )

        if not rows:
            return 0

        if dry_run:
            invalid = 0
            for index, row in enumerate(rows, start=1):
                try:
                    record = staged_row_to_record(row, index)
                except Exception as exc:
                    invalid += 1
                    print(
                        "[PROMOTE-INVALID]",
                        {
                            "staged_offer_id": str(row.get("staged_offer_id")),
                            "source_url": row.get("source_url"),
                            "error": f"{type(exc).__name__}: {exc}",
                        },
                        flush=True,
                    )
                    continue

                print(
                    "[PROMOTE-SAMPLE]",
                    {
                        "staged_offer_id": str(row["staged_offer_id"]),
                        "ean": record.ean,
                        "artist": record.artist,
                        "title": record.title,
                        "format": record.format_label,
                        "price": record.price,
                        "availability": record.availability,
                        "product_url": record.product_url,
                    },
                    flush=True,
                )

            print(
                "[PROMOTE] dry-run complete; geen databasewrites.",
                {"invalid": invalid},
                flush=True,
            )
            return 0 if invalid == 0 else 1

        stats = {
            "processed": 0,
            "failed": 0,
            "new_products": 0,
            "new_prices": 0,
            "changed_prices": 0,
            "history_rows": 0,
            "cover_candidates": 0,
        }

        with conn.cursor() as cur:
            shop_uuid = ensure_shop(cur, config)
            imported_at = datetime.now(timezone.utc)

            for index, row in enumerate(rows, start=1):
                staged_offer_id = str(row["staged_offer_id"])
                try:
                    record = staged_row_to_record(row, index)
                    product_id, product_inserted = upsert_product(cur, record)
                    price_inserted, price_changed = upsert_price(
                        cur,
                        product_id,
                        shop_uuid,
                        record,
                        imported_at,
                    )
                    history_inserted = maybe_insert_history(cur, product_id, shop_uuid, record)
                    cover_candidate_inserted = maybe_upsert_cover_candidate(
                        cur,
                        product_id,
                        shop_uuid,
                        record,
                    )
                    mark_promoted(cur, staged_offer_id, status="promoted", reason=None)
                    conn.commit()

                    stats["processed"] += 1
                    stats["new_products"] += int(product_inserted)
                    stats["new_prices"] += int(price_inserted)
                    stats["changed_prices"] += int(price_changed)
                    stats["history_rows"] += int(history_inserted)
                    stats["cover_candidates"] += int(cover_candidate_inserted)

                    print(
                        "[PROMOTE-WRITE]",
                        {
                            "index": index,
                            "staged_offer_id": staged_offer_id,
                            "ean": record.ean,
                            "artist": record.artist,
                            "title": record.title,
                            "price": record.price,
                        },
                        flush=True,
                    )
                except Exception as exc:
                    conn.rollback()
                    stats["failed"] += 1
                    with conn.cursor() as err_cur:
                        mark_promoted(
                            err_cur,
                            staged_offer_id,
                            status="promote_error",
                            reason=str(exc)[:500],
                        )
                    conn.commit()
                    print(
                        "[PROMOTE][WARN] failed",
                        {
                            "staged_offer_id": staged_offer_id,
                            "error": str(exc),
                        },
                        flush=True,
                    )

        print("[PROMOTE] done", stats, flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
