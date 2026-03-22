#!/usr/bin/env python3
from __future__ import annotations

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
    normalize_ean,
    normalize_text,
    parse_price,
    parse_timestamp,
)
from scripts.importers.contracts import ImportFileLayout, ShopImporterDefinition  # noqa: E402
from scripts.importers.runner import run_registered_importer  # noqa: E402

CONFIG = ImportConfig(
    shop_name="Groovespin",
    shop_domain="groovespin.nl",
    shop_country="NL",
    currency="EUR",
)


def map_groovespin_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("price_eur") or row.get("price_raw"))
    product_url = normalize_text(row.get("url"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    captured_at = parse_timestamp(row.get("timestamp"))
    product_handle = normalize_text(row.get("master_id")) or None

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
        format_label="Vinyl",
        cover_url=None,
        product_url=product_url,
        price=price,
        currency=CONFIG.currency,
        availability="in_stock",
        captured_at=captured_at,
        product_handle=product_handle,
        detail_status="ok",
        is_secondhand=False,
        raw=row,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="groovespin",
    config=CONFIG,
    importer_module="scripts.importers.import_groovespin",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_GROOVESPIN",
    storage_prefix="groovespin",
    files=ImportFileLayout(
        csv_output_path="data/raw/groovespin/groovespin_products.csv",
        rejects_path="output/groovespin_rejects.csv",
        summary_path="output/groovespin_import_summary.json",
    ),
    row_mapper=map_groovespin_row,
    description="Import Groovespin final CSV into Supabase/Postgres",
    required_columns=("title", "url", "ean"),
    optional_columns=("artist", "timestamp", "master_id", "year", "price_eur", "price_raw"),
    tags=("vinyl", "listing-plus-ean", "tilde-delimited"),
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
