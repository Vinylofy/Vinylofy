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
    shop_name="Bob's Vinyl",
    shop_domain="bobsvinyl.nl",
    shop_country="NL",
    currency="EUR",
)


VALID_AVAILABILITIES = {"in_stock", "out_of_stock", "preorder", "unknown"}


def normalize_availability(value: str | None) -> str:
    availability = normalize_text(value).lower().replace("-", "_")
    if availability in VALID_AVAILABILITIES:
        return availability
    return "in_stock"



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
    availability = normalize_availability(row.get("availability"))

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
        availability=availability,
        captured_at=captured_at,
        product_handle=normalize_text(row.get("product_handle")) or None,
        detail_status=detail_status,
        is_secondhand=is_secondhand,
        raw=row,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="bobsvinyl",
    config=CONFIG,
    importer_module="scripts.importers.import_bobsvinyl",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_BOBSVINYL",
    storage_prefix="bobsvinyl",
    files=ImportFileLayout(
        csv_output_path="data/raw/bobsvinyl/bobsvinyl_step2_enriched.csv",
        rejects_path="output/bobsvinyl_rejects.csv",
        summary_path="output/bobsvinyl_import_summary.json",
    ),
    row_mapper=map_bobsvinyl_row,
    description="Import Bob's Vinyl enriched CSV into Supabase/Postgres",
    required_columns=(
        "title",
        "prijs",
        "url",
        "detail_status",
        "mogelijk_2e_hands",
        "detail_checked_at",
        "ean",
    ),
    optional_columns=(
        "artist",
        "drager",
        "product_handle",
        "availability",
    ),
    tags=("vinyl", "enriched", "detail-check"),
)



def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
