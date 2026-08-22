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
    strict_normalize_gtin,
)
from scripts.importers.contracts import ImportFileLayout, ShopImporterDefinition  # noqa: E402
from scripts.importers.runner import run_registered_importer  # noqa: E402


CONFIG = ImportConfig(
    shop_name="At The Movies Shop",
    shop_domain="atthemoviesshop.com",
    shop_country="NL",
    currency="EUR",
)


def map_atthemovies_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean"))
    gtin_normalized = strict_normalize_gtin(row.get("ean"))
    price = parse_price(row.get("price"))
    product_url = normalize_text(row.get("product_url"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    availability = normalize_text(row.get("availability")).lower()
    detail_status = normalize_text(row.get("detail_status")) or "listing"

    if not ean or not gtin_normalized:
        return None, "missing_or_invalid_ean"
    if not product_url:
        return None, "missing_url"
    if price is None:
        return None, "invalid_price"
    if not title:
        return None, "missing_title"
    if not artist:
        return None, "missing_artist_after_inference"
    if availability not in {"in_stock", "out_of_stock"}:
        availability = "unknown"

    return CanonicalRecord(
        source_row_number=line_number,
        shop_name=CONFIG.shop_name,
        shop_domain=CONFIG.shop_domain,
        shop_country=CONFIG.shop_country,
        ean=ean,
        artist=artist,
        title=title,
        format_label=normalize_text(row.get("format")) or "Vinyl",
        cover_url=None,
        product_url=product_url,
        price=price,
        currency=CONFIG.currency,
        availability=availability,
        captured_at=parse_timestamp(row.get("scraped_at")),
        product_handle=normalize_text(row.get("handle")) or None,
        detail_status=detail_status,
        is_secondhand=False,
        raw=row,
        gtin_normalized=gtin_normalized,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="atthemovies",
    config=CONFIG,
    importer_module="scripts.importers.import_atthemovies",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_ATTHEMOVIES",
    storage_prefix="atthemovies",
    files=ImportFileLayout(
        csv_output_path="data/raw/atthemovies/atthemovies_products.csv",
        rejects_path="output/atthemovies_rejects.csv",
        summary_path="output/atthemovies_import_summary.json",
    ),
    row_mapper=map_atthemovies_row,
    description="Import At The Movies Shop listing-first CSV into Supabase/Postgres",
    required_columns=("scraped_at", "product_url", "ean", "artist", "title", "price", "availability"),
    optional_columns=(
        "handle",
        "product_id",
        "variant_id",
        "sku",
        "product_type",
        "format",
        "standard_price",
        "currency",
        "detail_status",
    ),
    tags=("vinyl", "shopify", "listing-first"),
    include_in_all=False,
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
