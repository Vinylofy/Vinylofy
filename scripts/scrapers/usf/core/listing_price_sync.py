from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import re
from typing import Any

import psycopg
from psycopg.rows import dict_row

from scripts.importers.common import (
    CanonicalRecord,
    normalize_ean,
    normalize_gtin14,
    normalize_text,
    upsert_product,
)


ALLOWED_AVAILABILITY = {"in_stock", "out_of_stock", "preorder", "unknown"}


@dataclass(frozen=True)
class ListingOffer:
    shop_name: str
    shop_domain: str
    shop_country: str
    source_url: str
    price: str | float | Decimal | None
    availability: str | None = None
    currency: str = "EUR"
    ean: str | None = None
    seen_at: datetime | None = None
    raw: dict[str, Any] | None = None


@dataclass
class ListingSyncStats:
    total: int = 0
    with_price: int = 0
    matched_existing_url: int = 0
    matched_latest_raw_ean: int = 0
    matched_offer_ean: int = 0
    product_created_from_raw_ean: int = 0
    unmatched: int = 0
    inserted_prices: int = 0
    changed_prices: int = 0
    refreshed_prices: int = 0
    history_rows: int = 0
    skipped_no_price: int = 0
    skipped_bad_price: int = 0


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_url(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.split("#", 1)[0].split("?", 1)[0].rstrip("/")


def parse_price(value: str | float | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"))
    if isinstance(value, (int, float)):
        return Decimal(str(value)).quantize(Decimal("0.01"))

    text = str(value).strip()
    if not text:
        return None

    text = (
        text.replace("€", "")
        .replace("EUR", "")
        .replace("\\xa0", " ")
        .strip()
    )

    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")

    match = re.search(r"([0-9]+(?:\.[0-9]{1,2})?)", text)
    if not match:
        return None

    try:
        return Decimal(match.group(1)).quantize(Decimal("0.01"))
    except InvalidOperation:
        return None


def normalize_availability(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "unknown"

    text = text.replace("-", "_").replace(" ", "_")
    aliases = {
        "instock": "in_stock",
        "in_stock": "in_stock",
        "available": "in_stock",
        "op_voorraad": "in_stock",
        "voorraad": "in_stock",
        "outofstock": "out_of_stock",
        "out_of_stock": "out_of_stock",
        "sold_out": "out_of_stock",
        "uitverkocht": "out_of_stock",
        "pre_order": "preorder",
        "preorder": "preorder",
    }
    normalized = aliases.get(text, text)
    return normalized if normalized in ALLOWED_AVAILABILITY else "unknown"


def ensure_shop_for_listing_offer(
    cur: psycopg.Cursor,
    *,
    shop_name: str,
    shop_domain: str,
    shop_country: str,
) -> str:
    cur.execute(
        """
        select id, name, country
        from public.shops
        where domain = %s
        limit 1
        """,
        (shop_domain,),
    )
    row = cur.fetchone()

    if row is not None:
        shop_id = str(row["id"])
        if row["name"] != shop_name or row["country"] != shop_country:
            cur.execute(
                """
                update public.shops
                set name = %s,
                    country = %s,
                    updated_at = now()
                where id = %s
                """,
                (shop_name, shop_country, shop_id),
            )
        return shop_id

    cur.execute(
        """
        insert into public.shops (
            name,
            domain,
            country,
            is_active,
            created_at,
            updated_at
        )
        values (%s, %s, %s, true, now(), now())
        returning id
        """,
        (shop_name, shop_domain, shop_country),
    )
    return str(cur.fetchone()["id"])


def find_product_by_existing_url(
    cur: psycopg.Cursor,
    *,
    shop_id: str,
    source_url: str,
) -> str | None:
    normalized = normalize_url(source_url)
    if not normalized:
        return None

    cur.execute(
        """
        select product_id
        from public.prices
        where shop_id = %s
          and trim(trailing '/' from split_part(product_url, '?', 1)) = %s
        order by updated_at desc nulls last, last_seen_at desc nulls last
        limit 1
        """,
        (shop_id, normalized),
    )
    row = cur.fetchone()
    return str(row["product_id"]) if row else None


def find_product_by_ean(
    cur: psycopg.Cursor,
    *,
    ean: str | None,
) -> str | None:
    normalized = normalize_ean(ean)
    gtin = normalize_gtin14(normalized)

    candidates: list[str] = []
    for value in (normalized, gtin):
        if value and value not in candidates:
            candidates.append(value)

    if not candidates:
        return None

    cur.execute(
        """
        select id
        from public.products
        where ean = any(%s)
           or gtin_normalized = any(%s)
        order by updated_at desc nulls last
        limit 1
        """,
        (candidates, candidates),
    )
    row = cur.fetchone()
    return str(row["id"]) if row else None


def find_latest_raw_for_url(
    cur: psycopg.Cursor,
    *,
    shop_id: str,
    source_url: str,
) -> dict[str, Any] | None:
    normalized = normalize_url(source_url)
    if not normalized:
        return None

    cur.execute(
        """
        select
            id,
            ean_raw,
            title_raw,
            image_url_raw,
            source_product_id,
            scraped_at,
            payload
        from public.raw_shop_scrapes
        where shop_id = %s
          and trim(trailing '/' from split_part(source_url, '?', 1)) = %s
          and ean_raw is not null
        order by scraped_at desc nulls last, id desc
        limit 1
        """,
        (shop_id, normalized),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def title_from_url(source_url: str) -> str:
    slug = normalize_url(source_url).rstrip("/").split("/")[-1]
    text = re.sub(r"[-_]+", " ", slug).strip()
    return text or slug or "Unknown title"


def ensure_product_from_raw_ean(
    cur: psycopg.Cursor,
    *,
    raw_row: dict[str, Any],
    offer: ListingOffer,
) -> tuple[str, bool]:
    ean = normalize_ean(raw_row.get("ean_raw"))
    if not ean:
        raise ValueError("Cannot create product from raw row without usable EAN")

    title = normalize_text(raw_row.get("title_raw")) or title_from_url(offer.source_url)
    artist = ""
    image_url = normalize_text(raw_row.get("image_url_raw")) or None
    captured_at = raw_row.get("scraped_at") or offer.seen_at or now_utc()
    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)

    record = CanonicalRecord(
        source_row_number=1,
        shop_name=offer.shop_name,
        shop_domain=offer.shop_domain,
        shop_country=offer.shop_country,
        ean=ean,
        artist=artist,
        title=title,
        format_label=None,
        cover_url=image_url,
        product_url=normalize_url(offer.source_url),
        price=0.0,
        currency=offer.currency,
        availability="unknown",
        captured_at=captured_at,
        product_handle=normalize_text(raw_row.get("source_product_id")) or None,
        detail_status="ean_enriched_no_detail_price",
        is_secondhand=False,
        raw={
            "source": "listing_price_sync",
            "raw_shop_scrape_id": str(raw_row.get("id")),
            "policy": "product created from detail EAN; price comes from listing only",
        },
        cover_candidate_url=image_url,
        cover_candidate_source_type="shop_detail_image" if image_url else None,
        cover_candidate_page_url=normalize_url(offer.source_url),
        cover_candidate_queue_priority=60 if image_url else None,
        gtin_normalized=normalize_gtin14(ean),
    )
    return upsert_product(cur, record)


def get_existing_price(
    cur: psycopg.Cursor,
    *,
    product_id: str,
    shop_id: str,
) -> dict[str, Any] | None:
    cur.execute(
        """
        select price, currency, product_url, availability
        from public.prices
        where product_id = %s
          and shop_id = %s
        limit 1
        """,
        (product_id, shop_id),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def insert_history_if_needed(
    cur: psycopg.Cursor,
    *,
    product_id: str,
    shop_id: str,
    price: Decimal,
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
        (product_id, shop_id),
    )
    latest = cur.fetchone()

    if latest is not None:
        latest_price = Decimal(str(latest["price"])).quantize(Decimal("0.01"))
        latest_availability = str(latest["availability"])
        latest_captured_at = latest["captured_at"]

        same_day = latest_captured_at.date() == captured_at.date()
        unchanged = latest_price == price and latest_availability == availability

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
        (product_id, shop_id, price, currency, availability, captured_at),
    )
    return True


def upsert_listing_price(
    cur: psycopg.Cursor,
    *,
    product_id: str,
    shop_id: str,
    source_url: str,
    price: Decimal,
    currency: str,
    availability: str,
    seen_at: datetime,
) -> tuple[bool, bool]:
    existing = get_existing_price(cur, product_id=product_id, shop_id=shop_id)
    inserted = existing is None

    changed = False
    if existing is not None:
        changed = any(
            [
                Decimal(str(existing["price"])).quantize(Decimal("0.01")) != price,
                str(existing["currency"]) != currency,
                normalize_url(str(existing["product_url"])) != normalize_url(source_url),
                str(existing["availability"]) != availability,
            ]
        )

    cur.execute(
        """
        insert into public.prices (
            product_id,
            shop_id,
            price,
            currency,
            product_url,
            availability,
            first_seen_at,
            last_seen_at,
            is_active,
            created_at,
            updated_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, true, now(), now())
        on conflict (product_id, shop_id)
        do update set
            price = excluded.price,
            currency = excluded.currency,
            product_url = excluded.product_url,
            availability = excluded.availability,
            last_seen_at = excluded.last_seen_at,
            is_active = true,
            updated_at = now()
        """,
        (
            product_id,
            shop_id,
            price,
            currency,
            normalize_url(source_url),
            availability,
            seen_at,
            seen_at,
        ),
    )

    return inserted, changed


def sync_listing_offers(
    conn: psycopg.Connection,
    offers: list[ListingOffer],
    *,
    write: bool,
) -> ListingSyncStats:
    stats = ListingSyncStats(total=len(offers))

    with conn.cursor(row_factory=dict_row) as cur:
        shop_ids_by_domain: dict[str, str] = {}

        for offer in offers:
            price = parse_price(offer.price)

            if offer.price is None:
                stats.skipped_no_price += 1
                continue

            if price is None:
                stats.skipped_bad_price += 1
                continue

            stats.with_price += 1

            if offer.shop_domain not in shop_ids_by_domain:
                shop_ids_by_domain[offer.shop_domain] = ensure_shop_for_listing_offer(
                    cur,
                    shop_name=offer.shop_name,
                    shop_domain=offer.shop_domain,
                    shop_country=offer.shop_country,
                )

            shop_id = shop_ids_by_domain[offer.shop_domain]

            product_id = find_product_by_existing_url(
                cur,
                shop_id=shop_id,
                source_url=offer.source_url,
            )
            matched_kind: str | None = "existing_url" if product_id is not None else None

            raw_row: dict[str, Any] | None = None
            if product_id is None:
                raw_row = find_latest_raw_for_url(
                    cur,
                    shop_id=shop_id,
                    source_url=offer.source_url,
                )
                if raw_row:
                    raw_ean = normalize_ean(raw_row.get("ean_raw"))
                    product_id = find_product_by_ean(cur, ean=raw_ean)
                    matched_kind = "latest_raw_ean" if product_id is not None else None

                    if product_id is None and write and raw_ean:
                        product_id, created = ensure_product_from_raw_ean(
                            cur,
                            raw_row=raw_row,
                            offer=offer,
                        )
                        stats.product_created_from_raw_ean += int(created)
                        matched_kind = "latest_raw_ean"

            if product_id is None and offer.ean:
                product_id = find_product_by_ean(cur, ean=offer.ean)
                matched_kind = "offer_ean" if product_id is not None else None

            if product_id is None:
                stats.unmatched += 1
                continue

            if matched_kind == "existing_url":
                stats.matched_existing_url += 1
            elif matched_kind == "latest_raw_ean":
                stats.matched_latest_raw_ean += 1
            elif matched_kind == "offer_ean":
                stats.matched_offer_ean += 1

            availability = normalize_availability(offer.availability)
            seen_at = offer.seen_at or now_utc()
            if seen_at.tzinfo is None:
                seen_at = seen_at.replace(tzinfo=timezone.utc)

            if not write:
                stats.refreshed_prices += 1
                stats.changed_prices += 1
                continue

            inserted, changed = upsert_listing_price(
                cur,
                product_id=product_id,
                shop_id=shop_id,
                source_url=offer.source_url,
                price=price,
                currency=offer.currency,
                availability=availability,
                seen_at=seen_at,
            )

            if changed:
                if insert_history_if_needed(
                    cur,
                    product_id=product_id,
                    shop_id=shop_id,
                    price=price,
                    currency=offer.currency,
                    availability=availability,
                    captured_at=seen_at,
                ):
                    stats.history_rows += 1

            stats.inserted_prices += int(inserted)
            stats.changed_prices += int(changed)
            stats.refreshed_prices += 1

        if write:
            conn.commit()

    return stats
