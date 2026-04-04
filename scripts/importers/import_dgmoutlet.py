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
    shop_name="DGM Outlet",
    shop_domain="dgmoutlet.nl",
    shop_country="NL",
    currency="EUR",
)


def clean_title(raw_title: str | None, fallback_raw_name: str | None) -> str:
    title = normalize_text(raw_title)
    if not title:
        title = normalize_text(fallback_raw_name)
    title = title.removesuffix(" LP").strip()
    return title


def map_dgmoutlet_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("price_current"))
    product_url = normalize_text(row.get("url"))
    captured_at = parse_timestamp(row.get("scraped_at"))
    raw_artist = row.get("artist")
    raw_title = clean_title(row.get("title"), row.get("raw_name"))
    artist, inferred_title = infer_artist_title(raw_artist, raw_title)
    title = normalize_text(inferred_title) or raw_title
    format_label = normalize_text(row.get("format")) or None

    if not artist:
        return None, "missing_artist_after_inference"
    if not title:
        return None, "missing_title"
    if not ean:
        return None, "missing_or_invalid_ean"
    if not product_url:
        return None, "missing_url"
    if price is None:
        return None, "invalid_price"

    image_url = normalize_text(row.get("image_url")) or None
    image_source_page_url = normalize_text(row.get("image_source_page_url")) or None
    image_source_type = normalize_text(row.get("image_source_type")) or None

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
        availability="in_stock",
        captured_at=captured_at,
        product_handle=None,
        detail_status="ok",
        is_secondhand=False,
        raw=row,
        cover_candidate_url=image_url,
        cover_candidate_source_type=image_source_type,
        cover_candidate_page_url=image_source_page_url,
        cover_candidate_queue_priority=100,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="dgmoutlet",
    config=CONFIG,
    importer_module="scripts.importers.import_dgmoutlet",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_DGMOUTLET",
    storage_prefix="dgmoutlet",
    files=ImportFileLayout(
        csv_output_path="data/raw/dgmoutlet/dgmoutlet_products.csv",
        rejects_path="output/dgmoutlet_rejects.csv",
        summary_path="output/dgmoutlet_import_summary.json",
    ),
    row_mapper=map_dgmoutlet_row,
    description="Import DGM Outlet listing CSV into Supabase/Postgres",
    required_columns=(
        "title",
        "raw_name",
        "price_current",
        "url",
        "scraped_at",
        "ean",
    ),
    optional_columns=(
        "artist",
        "format",
        "image_url",
        "image_source_page_url",
        "image_source_type",
    ),
    tags=("vinyl", "listing-only"),
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
