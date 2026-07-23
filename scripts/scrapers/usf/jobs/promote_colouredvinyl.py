#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from scripts.importers.common import normalize_text
from scripts.scrapers.usf.core.promotion import (
    PromotionConfig,
    fetch_staged_rows,
    staged_row_to_record,
)


CONFIG = PromotionConfig(
    shop_id="colouredvinyl",
    shop_name="Coloured Vinyl",
    shop_domain="colouredvinyl.nl",
    shop_country="NL",
    currency="EUR",
    cover_candidate_source_type="shop_detail_image",
    cover_candidate_queue_priority=100,
    require_artist=True,
)

DEFAULT_LIMIT = 1


def clean(value: Any) -> Any:
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass

    return str(value)


def prepare_row(
    row: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    prepared = dict(row)

    payload = row.get("raw_payload")

    if not isinstance(payload, dict):
        payload = {}

    artist = normalize_text(
        payload.get("artist")
    )

    title = normalize_text(
        payload.get("title")
    )

    if artist and title:
        # De centrale infer_artist_title ondersteunt
        # een en-dash, maar niet de bestaande pipe.
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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Preview Coloured Vinyl staged offers "
            "via de centrale USF-promotiecore. "
            "Deze job schrijft nooit naar de database."
        )
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit(
            "[ERROR] --limit moet minimaal 1 zijn."
        )

    rows = fetch_staged_rows(
        shop_id=CONFIG.shop_id,
        limit=args.limit,
    )

    diagnostics: dict[str, Any] = {
        "summary": {
            "shop": CONFIG.shop_id,
            "queued": len(rows),
            "valid": 0,
            "invalid": 0,
            "write": False,
        },
        "items": [],
    }

    print(
        "[COLOUREDVINYL-PROMOTE]",
        {
            "shop": CONFIG.shop_id,
            "queued": len(rows),
            "write": False,
        },
        flush=True,
    )

    for index, row in enumerate(
        rows,
        start=1,
    ):
        staged_offer_id = str(
            row["staged_offer_id"]
        )

        prepared, title_source = (
            prepare_row(row)
        )

        try:
            record = staged_row_to_record(
                row=prepared,
                config=CONFIG,
                line_number=index,
            )
        except Exception as exc:
            diagnostics["summary"][
                "invalid"
            ] += 1

            item = {
                "index": index,
                "staged_offer_id": (
                    staged_offer_id
                ),
                "source_url": row.get(
                    "source_url"
                ),
                "ean": row.get(
                    "ean_normalized"
                ),
                "title_source": (
                    title_source
                ),
                "error": (
                    f"{type(exc).__name__}: "
                    f"{exc}"
                ),
            }

            diagnostics["items"].append(
                item
            )

            print(
                "[COLOUREDVINYL-PROMOTE-INVALID]",
                item,
                flush=True,
            )

            continue

        diagnostics["summary"]["valid"] += 1

        item = {
            "index": index,
            "staged_offer_id": (
                staged_offer_id
            ),
            "title_source": title_source,
            "ean": clean(record.ean),
            "gtin_normalized": clean(
                record.gtin_normalized
            ),
            "artist": clean(
                record.artist
            ),
            "title": clean(record.title),
            "format_label": clean(
                record.format_label
            ),
            "price": clean(record.price),
            "currency": clean(
                record.currency
            ),
            "availability": clean(
                record.availability
            ),
            "product_url": clean(
                record.product_url
            ),
            "product_handle": clean(
                record.product_handle
            ),
            "cover_url": clean(
                record.cover_url
            ),
            "cover_candidate_url": clean(
                record.cover_candidate_url
            ),
        }

        diagnostics["items"].append(item)

        print(
            "[COLOUREDVINYL-PROMOTE-SAMPLE]",
            item,
            flush=True,
        )

    output_dir = Path(
        "output/usf-colouredvinyl"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    path = (
        output_dir
        / "promotion-diagnostics.json"
    )

    path.write_text(
        json.dumps(
            diagnostics,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(
        "[COLOUREDVINYL-PROMOTE-SUMMARY]",
        diagnostics["summary"],
        flush=True,
    )

    print(
        "[COLOUREDVINYL-PROMOTE] "
        "dry-run compleet; geen databasewrites.",
        flush=True,
    )

    return (
        0
        if diagnostics["summary"]["invalid"] == 0
        else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
