#!/usr/bin/env python3
"""
Template voor een nieuwe shop-importer.

Gebruik dit bestand als startpunt voor shop 4 / shop 5 onboarding.
Vervang alle TODO-velden en registreer daarna SHOP_DEFINITION in scripts.importers.registry.
"""
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
    shop_name="TODO Shop Name",
    shop_domain="todo-shop-domain.tld",
    shop_country="NL",
    currency="EUR",
)


def map_template_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("price"))
    product_url = normalize_text(row.get("url"))
    captured_at = parse_timestamp(row.get("scraped_at"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    format_label = normalize_text(row.get("format")) or None

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
        availability="in_stock",
        captured_at=captured_at,
        product_handle=None,
        detail_status="ok",
        is_secondhand=False,
        raw=row,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="todo_shop_key",
    config=CONFIG,
    importer_module="scripts.importers.import_todo_shop",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_TODO_SHOP",
    storage_prefix="todo_shop_key",
    files=ImportFileLayout(
        csv_output_path="data/raw/todo_shop/todo_shop_products.csv",
        rejects_path="output/todo_shop_rejects.csv",
        summary_path="output/todo_shop_import_summary.json",
    ),
    row_mapper=map_template_row,
    description="Import TODO shop CSV into Supabase/Postgres",
    required_columns=(
        "title",
        "price",
        "url",
        "scraped_at",
        "ean",
    ),
    optional_columns=(
        "artist",
        "format",
    ),
    tags=("template",),
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
