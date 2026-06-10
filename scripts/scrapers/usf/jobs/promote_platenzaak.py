#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict
from typing import Any

from scripts.scrapers.usf.core.promotion import (
    PromotionConfig,
    fetch_staged_rows,
    staged_row_to_record,
)
from scripts.scrapers.usf.core.promotion_writer import (
    promote_staged_offers_atomically,
)


CONFIG = PromotionConfig(
    shop_id="platenzaak",
    shop_name="Platenzaak",
    shop_domain="platenzaak.nl",
    shop_country="NL",
    currency="EUR",
    cover_candidate_source_type="shop_listing_image",
    cover_candidate_queue_priority=100,
    require_artist=True,
)

DEFAULT_LIMIT = 5


def clean(value: Any) -> Any:
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass

    return str(value)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Promote Platenzaak staged offers via de centrale "
            "USF product-, prijs- en historie-logica."
        )
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help="Maximaal aantal staged offers; standaard 5.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help=(
            "Voer echte writes uit naar products, prices en price_history. "
            "Zonder deze vlag is dit een dry-run."
        ),
    )
    return parser


def run_preview(limit: int) -> int:
    rows = fetch_staged_rows(
        shop_id=CONFIG.shop_id,
        limit=limit,
    )

    print(
        "[PROMOTE]",
        {
            "shop": CONFIG.shop_id,
            "queued": len(rows),
            "write": False,
        },
        flush=True,
    )

    valid = 0
    invalid = 0

    for index, row in enumerate(rows, start=1):
        staged_offer_id = str(row["staged_offer_id"])

        try:
            record = staged_row_to_record(
                row=row,
                config=CONFIG,
                line_number=index,
            )
        except Exception as exc:
            invalid += 1
            print(
                "[PROMOTE-INVALID]",
                {
                    "index": index,
                    "staged_offer_id": staged_offer_id,
                    "source_url": row.get("source_url"),
                    "ean": row.get("ean_normalized"),
                    "error": f"{type(exc).__name__}: {exc}",
                },
                flush=True,
            )
            continue

        valid += 1

        print(
            "[PROMOTE-SAMPLE]",
            {
                "index": index,
                "staged_offer_id": staged_offer_id,
                "ean": clean(getattr(record, "ean", None)),
                "artist": clean(getattr(record, "artist", None)),
                "title": clean(getattr(record, "title", None)),
                "price": clean(getattr(record, "price", None)),
                "currency": clean(getattr(record, "currency", None)),
                "availability": clean(getattr(record, "availability", None)),
                "product_url": clean(getattr(record, "product_url", None)),
                "cover_candidate_url": clean(
                    getattr(record, "cover_candidate_url", None)
                ),
            },
            flush=True,
        )

    print(
        "[PROMOTE] preview-complete",
        {
            "queued": len(rows),
            "valid": valid,
            "invalid": invalid,
            "write": False,
        },
        flush=True,
    )

    print(
        "[PROMOTE] dry-run complete; geen databasewrites.",
        flush=True,
    )

    return 0 if invalid == 0 else 1


def run_write(limit: int) -> int:
    result = promote_staged_offers_atomically(
        config=CONFIG,
        limit=limit,
    )

    result_dict = asdict(result)

    summary = {
        key: value
        for key, value in result_dict.items()
        if key not in {"items", "failures"}
    }

    print(
        "[PROMOTE]",
        {
            "shop": CONFIG.shop_id,
            "write": True,
            **summary,
        },
        flush=True,
    )

    for item in result_dict.get("items", [])[:10]:
        print(
            "[PROMOTE-WRITE]",
            {
                key: clean(value)
                for key, value in item.items()
            },
            flush=True,
        )

    for failure in result_dict.get("failures", [])[:10]:
        print(
            "[PROMOTE-FAILURE]",
            {
                key: clean(value)
                for key, value in failure.items()
            },
            flush=True,
        )

    return 0 if result.failed == 0 else 1


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    if args.write:
        return run_write(args.limit)

    return run_preview(args.limit)


if __name__ == "__main__":
    raise SystemExit(main())
