from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from psycopg.rows import dict_row

from scripts.importers.common import (
    CanonicalRecord,
    infer_artist_title,
    normalize_ean,
    normalize_gtin14,
    normalize_text,
)
from scripts.scrapers.usf.core.db import db_connection


VALID_AVAILABILITY = {
    "in_stock",
    "out_of_stock",
    "preorder",
    "unknown",
}


@dataclass(frozen=True)
class PromotionConfig:
    shop_id: str
    shop_name: str
    shop_domain: str
    shop_country: str = "NL"
    currency: str = "EUR"
    cover_candidate_source_type: str = "shop_listing_image"
    cover_candidate_queue_priority: int = 100
    require_artist: bool = True


@dataclass(frozen=True)
class PromotionPreviewItem:
    staged_offer_id: str
    record: CanonicalRecord


def infer_format_label(title: str | None) -> str | None:
    text = normalize_text(title).lower()

    if not text:
        return None

    patterns = (
        (r"\b4\s*lp\b", "4LP"),
        (r"\b3\s*lp\b", "3LP"),
        (r"\b2\s*lp\b", "2LP"),
        (r"\blp\b", "LP"),
        (r"\b12\s*(?:inch|inches|\")\b", "12 inch"),
        (r"\b10\s*(?:inch|inches|\")\b", "10 inch"),
        (r"\b7\s*(?:inch|inches|\")\b", "7 inch"),
        (r"\bep\b", "EP"),
        (r"\bcd\b", "CD"),
    )

    for pattern, label in patterns:
        if re.search(pattern, text, flags=re.IGNORECASE):
            return label

    return None


def extract_listing_payload(raw_payload: Any) -> dict[str, Any]:
    if not isinstance(raw_payload, dict):
        return {}

    listing_payload = raw_payload.get("listing_payload")

    if isinstance(listing_payload, dict):
        return listing_payload

    return {}


def fetch_staged_rows(
    *,
    shop_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    if limit < 1:
        raise ValueError("limit moet minimaal 1 zijn")

    with db_connection() as conn:
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
                (shop_id, limit),
            )
            return [dict(row) for row in cur.fetchall()]


def staged_row_to_record(
    *,
    row: dict[str, Any],
    config: PromotionConfig,
    line_number: int,
) -> CanonicalRecord:
    ean = normalize_ean(
        row.get("ean_match_key") or row.get("ean_normalized")
    )

    if not ean:
        raise ValueError(
            "missing usable EAN for "
            f"staged_offer_id={row.get('staged_offer_id')}"
        )

    title_raw = (
        normalize_text(row.get("title_raw"))
        or normalize_text(row.get("title_normalized"))
    )

    artist, title = infer_artist_title(None, title_raw)

    artist = normalize_text(artist)
    title = (
        normalize_text(title)
        or normalize_text(row.get("title_normalized"))
    )

    if config.require_artist and not artist:
        raise ValueError(
            "missing artist after inference for "
            f"staged_offer_id={row.get('staged_offer_id')}"
        )

    if not title:
        raise ValueError(
            "missing title for "
            f"staged_offer_id={row.get('staged_offer_id')}"
        )

    price = row.get("price")

    if price is None:
        raise ValueError(
            "missing price for "
            f"staged_offer_id={row.get('staged_offer_id')}"
        )

    availability = (
        normalize_text(row.get("availability")).lower()
        or "unknown"
    )

    if availability not in VALID_AVAILABILITY:
        availability = "unknown"

    captured_at = (
        row.get("scraped_at")
        or row.get("staged_at")
        or datetime.now(timezone.utc)
    )

    if captured_at.tzinfo is None:
        captured_at = captured_at.replace(tzinfo=timezone.utc)

    image_url = normalize_text(row.get("image_url")) or None
    listing_payload = extract_listing_payload(
        row.get("raw_payload")
    )

    format_label = (
        normalize_text(listing_payload.get("format"))
        or infer_format_label(title_raw)
    )

    product_url = normalize_text(row.get("source_url"))

    return CanonicalRecord(
        source_row_number=line_number,
        shop_name=config.shop_name,
        shop_domain=config.shop_domain,
        shop_country=config.shop_country,
        ean=ean,
        artist=artist,
        title=title,
        format_label=format_label,
        cover_url=None,
        product_url=product_url,
        price=float(price),
        currency=(
            normalize_text(row.get("currency"))
            or config.currency
        ),
        availability=availability,
        captured_at=captured_at,
        product_handle=(
            normalize_text(row.get("source_product_id"))
            or None
        ),
        detail_status="ok",
        is_secondhand=False,
        raw=dict(row),
        cover_candidate_url=image_url,
        cover_candidate_source_type=(
            config.cover_candidate_source_type
            if image_url
            else None
        ),
        cover_candidate_page_url=(
            product_url if image_url else None
        ),
        cover_candidate_queue_priority=(
            config.cover_candidate_queue_priority
            if image_url
            else None
        ),
        gtin_normalized=normalize_gtin14(ean),
    )


def preview_staged_offers(
    *,
    config: PromotionConfig,
    limit: int,
) -> tuple[PromotionPreviewItem, ...]:
    rows = fetch_staged_rows(
        shop_id=config.shop_id,
        limit=limit,
    )

    items: list[PromotionPreviewItem] = []

    for line_number, row in enumerate(rows, start=1):
        record = staged_row_to_record(
            row=row,
            config=config,
            line_number=line_number,
        )

        items.append(
            PromotionPreviewItem(
                staged_offer_id=str(row["staged_offer_id"]),
                record=record,
            )
        )

    return tuple(items)
