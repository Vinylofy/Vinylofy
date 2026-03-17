#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.importers.common import (  # noqa: E402
    CanonicalRecord,
    ImportConfig,
    infer_artist_title,
    normalize_ean,
    normalize_text,
    parse_price,
    run_import,
)


CONFIG = ImportConfig(
    shop_name="Platomania",
    shop_domain="platomania.nl",
    shop_country="NL",
    currency="EUR",
)

_CAPTURED_AT: datetime | None = None


def resolve_captured_at(csv_path: str) -> datetime:
    path = Path(csv_path)
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return datetime.now(timezone.utc)


def is_vinyl_format(format_value: str | None) -> bool:
    value = normalize_text(format_value).upper()
    return any(token in value for token in ("LP", '12"', '10"', '7"', "VINYL"))


def build_format_label(drager: str | None, item_type: str | None) -> str | None:
    drager_clean = normalize_text(drager)
    type_clean = normalize_text(item_type)

    if drager_clean and type_clean:
        return f"{drager_clean} | {type_clean}"
    if drager_clean:
        return drager_clean
    if type_clean:
        return type_clean
    return None


def map_platomania_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    global _CAPTURED_AT

    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("prijs"))
    product_url = normalize_text(row.get("url"))
    availability_raw = normalize_text(row.get("op_voorraad")).upper()
    availability = "in_stock" if availability_raw == "JA" else "out_of_stock"
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    format_label = build_format_label(row.get("drager"), row.get("type"))
    detail_status = "ok"
    is_secondhand = False
    captured_at = _CAPTURED_AT or datetime.now(timezone.utc)
    product_handle = normalize_text(row.get("item_nr")) or None
    cover_url = None

    if not is_vinyl_format(row.get("drager")):
        return None, "non_vinyl_format"
    if not ean:
        return None, "missing_or_invalid_ean"
    if not product_url:
        return None, "missing_url"
    if price is None:
        return None, "invalid_price"
    if not title:
        return None, "missing_title"
    if not artist:
        return None, "missing_artist_after_inference"

    return CanonicalRecord(
        source_row_number=line_number,
        shop_name=CONFIG.shop_name,
        shop_domain=CONFIG.shop_domain,
        shop_country=CONFIG.shop_country,
        ean=ean,
        artist=artist,
        title=title,
        format_label=format_label,
        cover_url=cover_url,
        product_url=product_url,
        price=price,
        currency=CONFIG.currency,
        availability=availability,
        captured_at=captured_at,
        product_handle=product_handle,
        detail_status=detail_status,
        is_secondhand=is_secondhand,
        raw=row,
    ), None


def main() -> None:
    global _CAPTURED_AT

    parser = argparse.ArgumentParser(description="Import Platomania enriched CSV into Supabase/Postgres")
    parser.add_argument("csv_path", help="Path to platomania_step2_enriched.csv")
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without writing to the database")
    parser.add_argument("--rejects", default="output/platomania_rejects.csv", help="Path to write rejected rows CSV")
    parser.add_argument("--summary", default="output/platomania_import_summary.json", help="Path to write summary JSON")
    args = parser.parse_args()

    _CAPTURED_AT = resolve_captured_at(args.csv_path)

    run_import(
        config=CONFIG,
        csv_path=args.csv_path,
        row_mapper=map_platomania_row,
        dry_run=args.dry_run,
        rejects_path=args.rejects,
        summary_path=args.summary,
    )


if __name__ == "__main__":
    main()