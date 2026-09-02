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
    shop_name="Blackvinyl",
    shop_domain="blackvinyl.nl",
    shop_country="NL",
    currency="EUR",
)


def resolve_availability(value: str | None) -> str:
    normalized = normalize_text(value).casefold()
    if normalized in {"in_stock", "out_of_stock"}:
        return normalized
    return "unknown"


def map_blackvinyl_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    raw_ean = normalize_text(row.get("ean"))
    ean = normalize_ean(raw_ean)
    gtin_normalized = strict_normalize_gtin(raw_ean)
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    product_url = normalize_text(row.get("product_url"))
    price = parse_price(row.get("price"))
    availability = resolve_availability(row.get("availability"))
    detail_status = normalize_text(row.get("detail_status")).casefold() or "api"

    if not ean or not gtin_normalized:
        return None, "missing_or_invalid_ean"
    if not product_url:
        return None, "missing_url"
    if price is None:
        return None, "invalid_price"
    if not title:
        return None, "missing_title"
    # Artist/title are source metadata.  An empty artist is allowed as a
    # placeholder because canonical masterdata matching happens downstream.
    if not artist:
        artist = ""

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
        product_handle=normalize_text(row.get("product_id")) or None,
        detail_status=detail_status,
        is_secondhand=False,
        raw=row,
        cover_candidate_url=normalize_text(row.get("image_url")) or None,
        cover_candidate_source_type="blackvinyl_product_image" if normalize_text(row.get("image_url")) else None,
        cover_candidate_page_url=product_url if normalize_text(row.get("image_url")) else None,
        gtin_normalized=gtin_normalized,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="blackvinyl",
    config=CONFIG,
    importer_module="scripts.importers.import_blackvinyl",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_BLACKVINYL",
    storage_prefix="blackvinyl",
    files=ImportFileLayout(
        csv_output_path="data/raw/blackvinyl/blackvinyl_products.csv",
        rejects_path="output/blackvinyl_rejects.csv",
        summary_path="output/blackvinyl_import_summary.json",
    ),
    row_mapper=map_blackvinyl_row,
    description="Import Blackvinyl LP Nieuw Store API CSV into Supabase/Postgres",
    required_columns=(
        "scraped_at",
        "product_url",
        "ean",
        "artist",
        "title",
        "price",
        "availability",
    ),
    optional_columns=(
        "product_id",
        "standard_price",
        "sale_price",
        "currency",
        "stock_text",
        "sku",
        "format",
        "category_id",
        "category_slug",
        "image_url",
        "gtin_normalized",
        "ean_source",
        "detail_ean",
        "detail_status",
        "detail_checked_at",
        "source_page",
        "source_api_url",
        "shipping_profile",
    ),
    tags=("vinyl", "woocommerce", "store-api", "listing-first", "barcode-gated"),
    include_in_all=False,
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
