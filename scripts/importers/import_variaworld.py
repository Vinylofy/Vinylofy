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
    shop_name="Variaworld",
    shop_domain="variaworld.nl",
    shop_country="NL",
    currency="EUR",
)


def resolve_availability(listing_status: str | None, price: float | None) -> str:
    status = normalize_text(listing_status).casefold()
    if not status:
        return "in_stock" if price is not None else "unknown"
    if any(token in status for token in ("uitverkocht", "sold out", "niet leverbaar", "out_of_stock")):
        return "out_of_stock"
    if status == "ok":
        return "in_stock"
    return "in_stock" if price is not None else "unknown"


def build_format_label(carrier: str | None, carrier_raw: str | None) -> str | None:
    cleaned_carrier = normalize_text(carrier)
    cleaned_raw = normalize_text(carrier_raw)
    if cleaned_carrier:
        return cleaned_carrier
    if cleaned_raw:
        return cleaned_raw
    return "Vinyl"


def map_variaworld_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("price"))
    product_url = normalize_text(row.get("product_url"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    captured_at = parse_timestamp(
        row.get("updated_at") or row.get("last_seen_at") or row.get("created_at")
    )
    availability = resolve_availability(row.get("listing_status"), price)
    format_label = build_format_label(row.get("carrier"), row.get("carrier_raw"))
    product_handle = normalize_text(row.get("product_id")) or None

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
    key="variaworld",
    config=CONFIG,
    importer_module="scripts.importers.import_variaworld",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_VARIAWORLD",
    storage_prefix="variaworld",
    files=ImportFileLayout(
        csv_output_path="data/raw/variaworld/variaworld_products.csv",
        rejects_path="output/variaworld_rejects.csv",
        summary_path="output/variaworld_import_summary.json",
    ),
    row_mapper=map_variaworld_row,
    description="Import Variaworld product CSV into Supabase/Postgres",
    required_columns=(
        "title",
        "price",
        "product_url",
        "ean",
    ),
    optional_columns=(
        "artist",
        "source",
        "product_id",
        "carrier_raw",
        "carrier",
        "price_raw",
        "currency",
        "listing_page",
        "listing_status",
        "ean_status",
        "created_at",
        "updated_at",
        "last_seen_at",
    ),
    tags=("vinyl", "single-csv", "listing-plus-ean"),
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
