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
    shop_name="Get Back Music",
    shop_domain="getbackmusic.nl",
    shop_country="NL",
    currency="EUR",
)


def resolve_ean_rejection(row: dict) -> str:
    status = normalize_text(row.get("detail_status")).casefold()
    if status in {"", "pending", "listing"}:
        return "detail_not_attempted"
    if status == "technical_error":
        return "detail_technical_error"
    return "missing_or_invalid_ean"


def resolve_availability(value: str | None) -> str:
    normalized = normalize_text(value).casefold()
    return normalized if normalized in {"in_stock", "out_of_stock", "preorder"} else "unknown"


def map_getbackmusic_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean"))
    gtin_normalized = strict_normalize_gtin(row.get("ean"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    product_url = normalize_text(row.get("product_url"))
    price = parse_price(row.get("price"))
    product_id = normalize_text(row.get("product_id"))
    variant_id = normalize_text(row.get("variant_id"))

    if not ean or not gtin_normalized:
        return None, resolve_ean_rejection(row)
    if not product_url:
        return None, "missing_url"
    if not product_id or not variant_id:
        return None, "missing_product_or_variant_id"
    if price is None:
        return None, "invalid_price"
    if not artist:
        return None, "missing_artist_after_inference"
    if not title:
        return None, "missing_title"

    image_url = normalize_text(row.get("image_url")) or None
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
        availability=resolve_availability(row.get("availability")),
        captured_at=parse_timestamp(row.get("scraped_at")),
        product_handle=f"{product_id}:{variant_id}",
        detail_status=normalize_text(row.get("detail_status")) or "ok",
        is_secondhand=False,
        raw=row,
        cover_candidate_url=image_url,
        cover_candidate_source_type="shop_listing_image" if image_url else None,
        cover_candidate_page_url=product_url if image_url else None,
        cover_candidate_queue_priority=100 if image_url else None,
        gtin_normalized=gtin_normalized,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="getbackmusic",
    config=CONFIG,
    importer_module="scripts.importers.import_getbackmusic",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_GETBACKMUSIC",
    storage_prefix="getbackmusic",
    files=ImportFileLayout(
        csv_output_path="data/raw/getbackmusic/getbackmusic_master.csv",
        rejects_path="output/getbackmusic_rejects.csv",
        summary_path="output/getbackmusic_import_summary.json",
    ),
    row_mapper=map_getbackmusic_row,
    description="Import Get Back Music listing-first LP/Vinyl CSV into Supabase/Postgres",
    required_columns=("scraped_at", "product_url", "product_id", "variant_id", "ean", "artist", "title", "price", "availability"),
    optional_columns=(
        "source_shop", "product_key", "standard_price", "is_sale", "currency", "format", "image_url", "page_found",
        "detail_title", "detail_description", "release_date", "label", "catalogue_number", "detail_status", "detail_error", "enriched_at",
    ),
    tags=("vinyl", "shopify", "listing-first", "barcode-gated", "variant-safe"),
    include_in_all=False,
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
