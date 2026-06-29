#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from scripts.importers.common import (
    ImportConfig,
    ensure_shop,
    identifier_candidates,
    normalize_ean,
    normalize_gtin14,
    normalize_text,
    parse_price,
)
from scripts.scrapers.usf.core.db import get_database_url

SHOP_ID = "imusic"
SHOP_NAME = "iMusic"
SHOP_DOMAIN = "imusic.nl"
SHOP_COUNTRY = "NL"
CURRENCY = "EUR"


@dataclass
class SyncStats:
    candidates: int = 0
    processed: int = 0
    updated: int = 0
    unchanged: int = 0
    changed_price: int = 0
    changed_availability: int = 0
    history_inserted: int = 0
    skipped_no_price: int = 0
    skipped_invalid_ean: int = 0
    skipped_no_existing_product: int = 0
    skipped_no_existing_price: int = 0
    skipped_unusable_availability: int = 0
    errors: int = 0


IN_STOCK_MARKERS = (
    "op voorraad",
    "weinig op voorraad",
    "koop",
    "verwachte levering",
    "besteld in een afgelegen magazijn",
)

OUT_OF_STOCK_MARKERS = (
    "uitverkocht",
    "niet leverbaar",
    "niet op voorraad",
)

PREORDER_MARKERS = (
    "pre-order",
    "preorder",
    "voorbestelling",
)


def availability_from_payload(payload: dict[str, Any]) -> str:
    raw = normalize_text(
        payload.get("listing_availability_raw")
        or payload.get("listing_availability_hint")
        or payload.get("availability_raw")
    )
    if not raw:
        return "unknown"

    low = raw.lower()

    if any(marker in low for marker in PREORDER_MARKERS):
        return "preorder"
    if any(marker in low for marker in OUT_OF_STOCK_MARKERS):
        return "out_of_stock"
    if any(marker in low for marker in IN_STOCK_MARKERS):
        return "in_stock"

    return "unknown"


def source_url_for_price(link: dict[str, Any], payload: dict[str, Any]) -> str:
    return (
        normalize_text(payload.get("listing_product_url"))
        or normalize_text(link.get("source_url"))
        or ""
    )


def fetch_listing_links(cur, *, limit: int) -> list[dict[str, Any]]:
    cur.execute(
        """
        select
          id,
          source_url,
          source_product_id,
          payload,
          last_seen_at,
          last_detail_scraped_at
        from public.shop_product_links
        where shop_id = %s
          and payload->>'source' = 'imusic_genre_listing'
          and coalesce(payload->>'listing_price_raw', payload->>'listing_price_hint', payload->>'price_raw') is not null
        order by last_seen_at desc nulls last, id asc
        limit %s
        """,
        (SHOP_ID, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def find_existing_product_id(cur, ean: str) -> str | None:
    candidates = identifier_candidates(ean, normalize_gtin14(ean))
    if not candidates:
        return None

    cur.execute(
        """
        select id
        from public.products
        where ean = any(%s)
           or gtin_normalized = any(%s)
        order by updated_at desc nulls last, created_at desc nulls last
        limit 1
        """,
        (candidates, candidates),
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


def fetch_existing_price(cur, *, product_id: str, shop_uuid: str) -> dict[str, Any] | None:
    cur.execute(
        """
        select
          product_id,
          shop_id,
          price,
          currency,
          product_url,
          availability,
          is_active
        from public.prices
        where product_id = %s
          and shop_id = %s
        limit 1
        """,
        (product_id, shop_uuid),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def insert_history_if_needed(
    cur,
    *,
    product_id: str,
    shop_uuid: str,
    price: float,
    currency: str,
    availability: str,
    captured_at: datetime,
) -> bool:
    cur.execute(
        """
        select price, availability, captured_at
        from public.price_history
        where product_id = %s
          and shop_id = %s
        order by captured_at desc
        limit 1
        """,
        (product_id, shop_uuid),
    )
    latest = cur.fetchone()

    if latest is not None:
        latest_price = float(latest["price"])
        latest_availability = str(latest["availability"])
        latest_captured_at = latest["captured_at"]
        same_day = latest_captured_at.date() == captured_at.date()
        unchanged = (
            float(latest_price) == float(price)
            and latest_availability == availability
        )
        if same_day and unchanged:
            return False

    cur.execute(
        """
        insert into public.price_history (
          product_id,
          shop_id,
          price,
          currency,
          availability,
          captured_at,
          created_at
        )
        values (%s, %s, %s, %s, %s, %s, now())
        """,
        (product_id, shop_uuid, price, currency, availability, captured_at),
    )
    return True


def sync_listing_prices(*, limit: int, write: bool) -> SyncStats:
    if limit < 1:
        raise ValueError("limit moet minimaal 1 zijn")

    stats = SyncStats()

    with psycopg.connect(get_database_url(), prepare_threshold=None) as conn:
        # ensure_shop() uit scripts.importers.common verwacht tuple-rows.
        # De rest van deze sync-job gebruikt dict_row voor expliciete kolomnamen.
        with conn.cursor() as shop_cur:
            shop_uuid = ensure_shop(
                shop_cur,
                ImportConfig(
                    shop_name=SHOP_NAME,
                    shop_domain=SHOP_DOMAIN,
                    shop_country=SHOP_COUNTRY,
                    currency=CURRENCY,
                ),
            )

        with conn.cursor(row_factory=dict_row) as cur:
            links = fetch_listing_links(cur, limit=limit)
            stats.candidates = len(links)

            captured_at = datetime.now(timezone.utc)

            for link in links:
                payload = link.get("payload") or {}
                if not isinstance(payload, dict):
                    payload = {}

                ean = normalize_ean(
                    link.get("source_product_id")
                    or payload.get("ean")
                    or link.get("source_url")
                )
                if not ean:
                    stats.skipped_invalid_ean += 1
                    print(
                        "[IMUSIC-PRICE-SYNC-SKIP]",
                        {
                            "reason": "invalid_ean",
                            "source_url": link.get("source_url"),
                            "source_product_id": link.get("source_product_id"),
                        },
                        flush=True,
                    )
                    continue

                price_raw = normalize_text(
                    payload.get("listing_price_raw")
                    or payload.get("listing_price_hint")
                    or payload.get("price_raw")
                )
                price = parse_price(price_raw)
                if price is None:
                    stats.skipped_no_price += 1
                    print(
                        "[IMUSIC-PRICE-SYNC-SKIP]",
                        {
                            "reason": "no_parseable_listing_price",
                            "ean": ean,
                            "price_raw": price_raw,
                            "source_url": link.get("source_url"),
                        },
                        flush=True,
                    )
                    continue

                availability = availability_from_payload(payload)
                if availability not in {"in_stock", "out_of_stock", "preorder", "unknown"}:
                    stats.skipped_unusable_availability += 1
                    continue

                product_url = source_url_for_price(link, payload)
                if not product_url:
                    product_url = f"https://imusic.nl/music/{ean}"

                product_id = find_existing_product_id(cur, ean)
                if not product_id:
                    stats.skipped_no_existing_product += 1
                    print(
                        "[IMUSIC-PRICE-SYNC-SKIP]",
                        {
                            "reason": "no_existing_product",
                            "ean": ean,
                            "price_raw": price_raw,
                            "product_url": product_url,
                        },
                        flush=True,
                    )
                    continue

                existing_price = fetch_existing_price(
                    cur,
                    product_id=product_id,
                    shop_uuid=shop_uuid,
                )
                if existing_price is None:
                    # Belangrijk: nieuwe iMusic offers mogen NIET via price-only live.
                    # Die moeten eerst door detail_imusic voor EAN-validatie.
                    stats.skipped_no_existing_price += 1
                    print(
                        "[IMUSIC-PRICE-SYNC-SKIP]",
                        {
                            "reason": "no_existing_imusic_price",
                            "ean": ean,
                            "product_id": product_id,
                            "price_raw": price_raw,
                            "product_url": product_url,
                        },
                        flush=True,
                    )
                    continue

                old_price = float(existing_price["price"])
                old_availability = str(existing_price["availability"])
                old_product_url = normalize_text(existing_price.get("product_url"))
                old_currency = str(existing_price["currency"])

                price_changed = old_price != float(price)
                availability_changed = old_availability != availability
                other_changed = (
                    old_product_url != product_url
                    or old_currency != CURRENCY
                    or existing_price.get("is_active") is not True
                )

                stats.processed += 1

                print(
                    "[IMUSIC-PRICE-SYNC]",
                    {
                        "ean": ean,
                        "product_id": product_id,
                        "old_price": old_price,
                        "new_price": price,
                        "old_availability": old_availability,
                        "new_availability": availability,
                        "product_url": product_url,
                        "write": write,
                    },
                    flush=True,
                )

                if not write:
                    if price_changed or availability_changed or other_changed:
                        stats.updated += 1
                    else:
                        stats.unchanged += 1
                    continue

                cur.execute(
                    """
                    update public.prices
                    set
                      price = %s,
                      currency = %s,
                      product_url = %s,
                      availability = %s,
                      last_seen_at = %s,
                      is_active = true,
                      updated_at = now()
                    where product_id = %s
                      and shop_id = %s
                    """,
                    (
                        price,
                        CURRENCY,
                        product_url,
                        availability,
                        captured_at,
                        product_id,
                        shop_uuid,
                    ),
                )

                if price_changed or availability_changed or other_changed:
                    stats.updated += 1
                else:
                    stats.unchanged += 1

                if price_changed:
                    stats.changed_price += 1
                if availability_changed:
                    stats.changed_availability += 1

                if price_changed or availability_changed:
                    if insert_history_if_needed(
                        cur,
                        product_id=product_id,
                        shop_uuid=shop_uuid,
                        price=price,
                        currency=CURRENCY,
                        availability=availability,
                        captured_at=captured_at,
                    ):
                        stats.history_inserted += 1

        if not write:
            conn.rollback()
        else:
            conn.commit()

    return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Sync iMusic listing prices to existing validated public.prices rows. "
            "Nieuwe iMusic offers worden bewust niet aangemaakt; die moeten eerst door detail/stage/promote."
        )
    )
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    stats = sync_listing_prices(limit=args.limit, write=args.write)

    print(
        "[IMUSIC-PRICE-SYNC-DONE]",
        {
            "candidates": stats.candidates,
            "processed": stats.processed,
            "updated": stats.updated,
            "unchanged": stats.unchanged,
            "changed_price": stats.changed_price,
            "changed_availability": stats.changed_availability,
            "history_inserted": stats.history_inserted,
            "skipped_no_price": stats.skipped_no_price,
            "skipped_invalid_ean": stats.skipped_invalid_ean,
            "skipped_no_existing_product": stats.skipped_no_existing_product,
            "skipped_no_existing_price": stats.skipped_no_existing_price,
            "skipped_unusable_availability": stats.skipped_unusable_availability,
            "errors": stats.errors,
            "databasewrites": bool(args.write),
        },
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
