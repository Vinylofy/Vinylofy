#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from scripts.importers.common import (
    ImportConfig,
    ensure_shop,
    maybe_insert_history,
    maybe_upsert_cover_candidate,
    normalize_text,
    upsert_price,
    upsert_product,
)
from scripts.scrapers.usf.core.db import (
    get_database_url,
)
from scripts.scrapers.usf.core.promotion import (
    PromotionConfig,
    fetch_staged_rows,
    staged_row_to_record,
)
from scripts.scrapers.usf.core.promotion_writer import (
    fetch_locked_staged_rows,
)


SHOP_ID = "colouredvinyl"

CONFIG = PromotionConfig(
    shop_id=SHOP_ID,
    shop_name="Coloured Vinyl",
    shop_domain="colouredvinyl.nl",
    shop_country="NL",
    currency="EUR",
    cover_candidate_source_type=(
        "shop_detail_image"
    ),
    cover_candidate_queue_priority=100,
    require_artist=True,
)


def prepare_row(
    row: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    prepared = dict(row)

    payload = row.get("raw_payload")

    if not isinstance(payload, dict):
        payload = {}

    listing_payload = payload.get(
        "listing_payload"
    )

    if not isinstance(
        listing_payload,
        dict,
    ):
        listing_payload = {}

    raw_format = normalize_text(
        payload.get("format")
    )

    if (
        raw_format
        and not normalize_text(
            listing_payload.get("format")
        )
    ):
        payload = dict(payload)
        listing_payload = dict(
            listing_payload
        )

        listing_payload["format"] = (
            raw_format
        )

        payload["listing_payload"] = (
            listing_payload
        )

        prepared["raw_payload"] = (
            payload
        )

    artist = normalize_text(
        payload.get("artist")
    )

    title = normalize_text(
        payload.get("title")
    )

    if artist and title:
        prepared["title_raw"] = (
            f"{artist} – {title}"
        )

        return (
            prepared,
            "raw_payload_artist_title",
        )

    return (
        prepared,
        "existing_title_raw",
    )


def record_item(
    *,
    staged_offer_id: str,
    title_source: str,
    record: Any,
) -> dict[str, Any]:
    return {
        "staged_offer_id": (
            staged_offer_id
        ),
        "title_source": title_source,
        "ean": record.ean,
        "gtin_normalized": (
            record.gtin_normalized
        ),
        "artist": record.artist,
        "title": record.title,
        "format_label": (
            record.format_label
        ),
        "price": str(record.price),
        "currency": record.currency,
        "availability": (
            record.availability
        ),
        "product_url": (
            record.product_url
        ),
        "product_handle": (
            record.product_handle
        ),
        "cover_url": record.cover_url,
        "cover_candidate_url": (
            record.cover_candidate_url
        ),
    }


def preview(
    *,
    limit: int,
) -> dict[str, Any]:
    rows = fetch_staged_rows(
        shop_id=SHOP_ID,
        limit=limit,
    )

    items: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for index, row in enumerate(
        rows,
        start=1,
    ):
        staged_offer_id = str(
            row["staged_offer_id"]
        )

        try:
            prepared, title_source = (
                prepare_row(row)
            )

            record = staged_row_to_record(
                row=prepared,
                config=CONFIG,
                line_number=index,
            )

            if record.cover_url is not None:
                raise ValueError(
                    "shop cover_url is niet toegestaan"
                )

            if (
                record.cover_candidate_url
                is not None
            ):
                raise ValueError(
                    "shop cover candidate is niet toegestaan"
                )

            items.append(
                record_item(
                    staged_offer_id=(
                        staged_offer_id
                    ),
                    title_source=(
                        title_source
                    ),
                    record=record,
                )
            )
        except Exception as exc:
            failures.append({
                "staged_offer_id": (
                    staged_offer_id
                ),
                "reason": (
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )[:500],
            })

    return {
        "summary": {
            "shop": SHOP_ID,
            "queued": len(rows),
            "processed": 0,
            "failed": len(failures),
            "new_products": 0,
            "new_prices": 0,
            "changed_prices": 0,
            "history_rows": 0,
            "cover_candidates": 0,
            "write": False,
        },
        "items": items,
        "failures": failures,
    }


def promote(
    *,
    limit: int,
) -> dict[str, Any]:
    importer_config = ImportConfig(
        shop_name=CONFIG.shop_name,
        shop_domain=CONFIG.shop_domain,
        shop_country=CONFIG.shop_country,
        currency=CONFIG.currency,
    )

    items: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    with psycopg.connect(
        get_database_url(),
        prepare_threshold=None,
    ) as connection:
        with connection.cursor(
            row_factory=dict_row,
        ) as cursor:
            rows = fetch_locked_staged_rows(
                cursor,
                shop_id=SHOP_ID,
                limit=limit,
            )

        if not rows:
            return {
                "summary": {
                    "shop": SHOP_ID,
                    "queued": 0,
                    "processed": 0,
                    "failed": 0,
                    "new_products": 0,
                    "new_prices": 0,
                    "changed_prices": 0,
                    "history_rows": 0,
                    "cover_candidates": 0,
                    "write": True,
                },
                "items": [],
                "failures": [],
            }

        with connection.cursor() as cursor:
            shop_uuid = ensure_shop(
                cursor,
                importer_config,
            )

        for index, row in enumerate(
            rows,
            start=1,
        ):
            staged_offer_id = str(
                row["staged_offer_id"]
            )

            try:
                with connection.transaction():
                    prepared, title_source = (
                        prepare_row(row)
                    )

                    record = staged_row_to_record(
                        row=prepared,
                        config=CONFIG,
                        line_number=index,
                    )

                    if record.cover_url is not None:
                        raise ValueError(
                            "shop cover_url is niet toegestaan"
                        )

                    if (
                        record.cover_candidate_url
                        is not None
                    ):
                        raise ValueError(
                            "shop cover candidate is niet toegestaan"
                        )

                    with connection.cursor() as cursor:
                        (
                            product_id,
                            product_inserted,
                        ) = upsert_product(
                            cursor,
                            record,
                        )

                        (
                            price_inserted,
                            price_changed,
                        ) = upsert_price(
                            cursor,
                            product_id,
                            shop_uuid,
                            record,
                            record.captured_at,
                        )

                        history_inserted = (
                            maybe_insert_history(
                                cursor,
                                product_id,
                                shop_uuid,
                                record,
                            )
                        )

                        cover_candidate_inserted = (
                            maybe_upsert_cover_candidate(
                                cursor,
                                product_id,
                                shop_uuid,
                                record,
                            )
                        )

                        if cover_candidate_inserted:
                            raise RuntimeError(
                                "onverwachte cover candidate"
                            )

                        cursor.execute(
                            """
                            update public.staged_offers
                            set
                              stage_status = 'promoted',
                              stage_reason = null
                            where id = %s
                              and stage_status = 'staged'
                            returning id
                            """,
                            (
                                staged_offer_id,
                            ),
                        )

                        if cursor.fetchone() is None:
                            raise RuntimeError(
                                "staged offer kon niet "
                                "als promoted worden gemarkeerd"
                            )

                        cursor.execute(
                            """
                            select 1
                            from public.prices
                            where product_id = %s
                              and shop_id = %s
                            """,
                            (
                                product_id,
                                shop_uuid,
                            ),
                        )

                        if cursor.fetchone() is None:
                            raise RuntimeError(
                                "price-upsert leverde "
                                "geen publieke prijs op"
                            )

                    item = record_item(
                        staged_offer_id=(
                            staged_offer_id
                        ),
                        title_source=(
                            title_source
                        ),
                        record=record,
                    )

                    item.update({
                        "product_id": str(
                            product_id
                        ),
                        "product_inserted": bool(
                            product_inserted
                        ),
                        "price_inserted": bool(
                            price_inserted
                        ),
                        "price_changed": bool(
                            price_changed
                        ),
                        "history_inserted": bool(
                            history_inserted
                        ),
                        "cover_candidate_inserted": (
                            False
                        ),
                    })

                    items.append(item)

            except Exception as exc:
                reason = (
                    f"{type(exc).__name__}: "
                    f"{exc}"
                )[:500]

                with connection.transaction():
                    with connection.cursor() as cursor:
                        cursor.execute(
                            """
                            update public.staged_offers
                            set
                              stage_status = 'promote_error',
                              stage_reason = %s
                            where id = %s
                              and stage_status = 'staged'
                            returning id
                            """,
                            (
                                reason,
                                staged_offer_id,
                            ),
                        )

                        if cursor.fetchone() is None:
                            raise RuntimeError(
                                "promotiefoutstatus "
                                "kon niet worden opgeslagen"
                            )

                failures.append({
                    "staged_offer_id": (
                        staged_offer_id
                    ),
                    "reason": reason,
                })

    return {
        "summary": {
            "shop": SHOP_ID,
            "queued": (
                len(items) + len(failures)
            ),
            "processed": len(items),
            "failed": len(failures),
            "new_products": sum(
                int(
                    item["product_inserted"]
                )
                for item in items
            ),
            "new_prices": sum(
                int(
                    item["price_inserted"]
                )
                for item in items
            ),
            "changed_prices": sum(
                int(
                    item["price_changed"]
                )
                for item in items
            ),
            "history_rows": sum(
                int(
                    item["history_inserted"]
                )
                for item in items
            ),
            "cover_candidates": 0,
            "write": True,
        },
        "items": items,
        "failures": failures,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Promote Coloured Vinyl staged offers "
            "naar products, prices en price_history."
        )
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=1,
    )

    parser.add_argument(
        "--write",
        action="store_true",
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit(
            "[ERROR] --limit moet minimaal 1 zijn."
        )

    result = (
        promote(limit=args.limit)
        if args.write
        else preview(limit=args.limit)
    )

    output_dir = Path(
        "output/usf-colouredvinyl"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / "promotion-diagnostics.json"
    )

    output_path.write_text(
        json.dumps(
            result,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(
        "[COLOUREDVINYL-PROMOTE-SUMMARY]",
        result["summary"],
        flush=True,
    )

    for item in result["items"]:
        print(
            "[COLOUREDVINYL-PROMOTE-WRITE]"
            if args.write
            else
            "[COLOUREDVINYL-PROMOTE-SAMPLE]",
            item,
            flush=True,
        )

    for failure in result["failures"]:
        print(
            "[COLOUREDVINYL-PROMOTE-ERROR]",
            failure,
            flush=True,
        )

    print(
        "[COLOUREDVINYL-PROMOTE-DIAGNOSTICS]",
        {
            "path": str(output_path)
        },
        flush=True,
    )

    return (
        0
        if int(
            result["summary"]["failed"]
        ) == 0
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
