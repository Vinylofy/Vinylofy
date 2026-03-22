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
    shop_name="Records On Vinyl",
    shop_domain="recordsonvinyl.nl",
    shop_country="NL",
    currency="EUR",
)


def resolve_availability(available_raw: str | None, availability_label: str | None, price: float | None) -> str:
    value = normalize_text(available_raw).lower()
    label = normalize_text(availability_label).lower()

    if value in {"false", "0"}:
        return "out_of_stock"
    if value in {"true", "1"}:
        return "in_stock"
    if any(token in label for token in ("uitverkocht", "sold out")):
        return "out_of_stock"
    if any(token in label for token in ("op voorraad", "in stock", "preorder", "bestelbaar")):
        return "in_stock"
    return "in_stock" if price is not None else "unknown"


def map_recordsonvinyl_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean13"))
    price = parse_price(row.get("price_offer") or row.get("price_list"))
    product_url = normalize_text(row.get("product_url"))
    artist, title = infer_artist_title(row.get("artist"), row.get("album") or row.get("title_raw"))
    captured_at = parse_timestamp(row.get("scraped_at"))
    format_label = normalize_text(row.get("variant_title")) or "Vinyl"
    availability = resolve_availability(row.get("available"), row.get("availability_raw"), price)
    product_handle = normalize_text(row.get("handle")) or None

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
        cover_url=None,
        product_url=product_url,
        price=price,
        currency=CONFIG.currency,
        availability=availability,
        captured_at=captured_at,
        product_handle=product_handle,
        detail_status="ok",
        is_secondhand=False,
        raw=row,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="recordsonvinyl",
    config=CONFIG,
    importer_module="scripts.importers.import_recordsonvinyl",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_RECORDSONVINYL",
    storage_prefix="recordsonvinyl",
    files=ImportFileLayout(
        csv_output_path="data/raw/recordsonvinyl/recordsonvinyl_products.csv",
        rejects_path="output/recordsonvinyl_rejects.csv",
        summary_path="output/recordsonvinyl_import_summary.json",
    ),
    row_mapper=map_recordsonvinyl_row,
    description="Import RecordsOnVinyl export CSV into Supabase/Postgres",
    required_columns=("product_url", "ean13"),
    optional_columns=(
        "scraped_at",
        "handle",
        "title_raw",
        "artist",
        "album",
        "variant_id",
        "variant_title",
        "sku",
        "price_offer",
        "price_list",
        "available",
        "availability_raw",
        "currency",
    ),
    tags=("vinyl", "master-export", "shopify"),
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
