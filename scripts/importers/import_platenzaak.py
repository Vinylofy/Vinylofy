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
    shop_name="Platenzaak",
    shop_domain="platenzaak.nl",
    shop_country="NL",
    currency="EUR",
)


def resolve_availability(raw_value: str | None) -> str:
    value = normalize_text(raw_value).casefold()
    if not value:
        return "in_stock"
    if "uitverkocht" in value:
        return "out_of_stock"
    return "in_stock"


def build_format_label(
    product_type: str | None,
    contents: str | None,
    vinyl_details: str | None,
    edition: str | None,
) -> str | None:
    parts: list[str] = []
    for raw_value in (product_type, contents, vinyl_details, edition):
        cleaned = normalize_text(raw_value)
        if cleaned and cleaned not in parts:
            parts.append(cleaned)
    if parts:
        return " | ".join(parts)
    return "Vinyl"


def map_platenzaak_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("price"))
    product_url = normalize_text(row.get("product_url"))
    captured_at = parse_timestamp(row.get("enriched_at") or row.get("scraped_at"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    availability = resolve_availability(row.get("availability"))
    format_label = build_format_label(
        row.get("product_type"),
        row.get("contents"),
        row.get("vinyl_details"),
        row.get("edition"),
    )
    product_handle = normalize_text(row.get("product_key")) or None

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
    key="platenzaak",
    config=CONFIG,
    importer_module="scripts.importers.import_platenzaak",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_PLATENZAAK",
    storage_prefix="platenzaak",
    files=ImportFileLayout(
        csv_output_path="data/raw/platenzaak/platenzaak_master.csv",
        rejects_path="output/platenzaak_rejects.csv",
        summary_path="output/platenzaak_import_summary.json",
    ),
    row_mapper=map_platenzaak_row,
    description="Import Platenzaak master CSV into Supabase/Postgres",
    required_columns=(
        "title",
        "price",
        "availability",
        "product_url",
        "scraped_at",
        "ean",
    ),
    optional_columns=(
        "artist",
        "product_key",
        "product_type",
        "contents",
        "vinyl_details",
        "edition",
        "release_date",
        "coloured_vinyl",
        "exclusive",
        "reissue",
        "boxset",
        "enriched_at",
        "currency",
        "source_shop",
        "page_found",
    ),
    tags=("vinyl", "master-file", "listing-plus-enrichment"),
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
