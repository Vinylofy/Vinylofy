#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.importers.common import (  # noqa: E402
    CanonicalRecord,
    ImportConfig,
    infer_artist_title,
    normalize_text,
    normalize_ean,
    parse_price,
    parse_timestamp,
    run_import,
)


CONFIG = ImportConfig(
    shop_name="Bob's Vinyl",
    shop_domain="bobsvinyl.nl",
    shop_country="NL",
    currency="EUR",
)


def map_bobsvinyl_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("prijs"))
    product_url = normalize_text(row.get("url"))
    detail_status = normalize_text(row.get("detail_status")).lower()
    secondhand_raw = normalize_text(row.get("mogelijk_2e_hands")).upper()
    is_secondhand = secondhand_raw not in {"NEE", "NO", "FALSE", "0", ""}
    captured_at = parse_timestamp(row.get("detail_checked_at"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    format_label = normalize_text(row.get("drager")) or None
    cover_url = None

    if detail_status != "ok":
        return None, "detail_status_not_ok"
    if is_secondhand:
        return None, "secondhand"
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
        availability="in_stock",
        captured_at=captured_at,
        product_handle=normalize_text(row.get("product_handle")) or None,
        detail_status=detail_status,
        is_secondhand=is_secondhand,
        raw=row,
    ), None


def main() -> None:
    parser = argparse.ArgumentParser(description="Import Bob's Vinyl enriched CSV into Supabase/Postgres")
    parser.add_argument("csv_path", help="Path to bobsvinyl_step2_enriched.csv")
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without writing to the database")
    parser.add_argument("--rejects", default="output/bobsvinyl_rejects.csv", help="Path to write rejected rows CSV")
    parser.add_argument("--summary", default="output/bobsvinyl_import_summary.json", help="Path to write summary JSON")
    args = parser.parse_args()

    run_import(
        config=CONFIG,
        csv_path=args.csv_path,
        row_mapper=map_bobsvinyl_row,
        dry_run=args.dry_run,
        rejects_path=args.rejects,
        summary_path=args.summary,
    )


if __name__ == "__main__":
    main()