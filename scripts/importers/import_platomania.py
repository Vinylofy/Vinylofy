#!/usr/bin/env python3
from __future__ import annotations

import sys
from datetime import datetime, timezone
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
)
from scripts.importers.contracts import ImportFileLayout, ShopImporterDefinition  # noqa: E402
from scripts.importers.runner import run_registered_importer  # noqa: E402

CONFIG = ImportConfig(
    shop_name="Platomania",
    shop_domain="platomania.nl",
    shop_country="NL",
    currency="EUR",
)

_CAPTURED_AT: datetime | None = None


def resolve_captured_at(csv_path: str) -> datetime:
    path = Path(csv_path)
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return datetime.now(timezone.utc)


def prepare_platomania_run(csv_path: str) -> None:
    global _CAPTURED_AT
    _CAPTURED_AT = resolve_captured_at(csv_path)


def is_vinyl_format(format_value: str | None) -> bool:
    value = normalize_text(format_value).upper()
    return any(token in value for token in ("LP", '12"', '10"', '7"', "VINYL"))


def build_format_label(drager: str | None, item_type: str | None) -> str | None:
    drager_clean = normalize_text(drager)
    type_clean = normalize_text(item_type)
    if drager_clean and type_clean:
        return f"{drager_clean} | {type_clean}"
    if drager_clean:
        return drager_clean
    if type_clean:
        return type_clean
    return None


def map_platomania_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    global _CAPTURED_AT

    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("prijs"))
    product_url = normalize_text(row.get("url"))
    availability_raw = normalize_text(row.get("op_voorraad")).upper()
    availability = "in_stock" if availability_raw == "JA" else "out_of_stock"
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    format_label = build_format_label(row.get("drager"), row.get("type"))
    captured_at = _CAPTURED_AT or datetime.now(timezone.utc)
    product_handle = normalize_text(row.get("item_nr")) or None

    if not is_vinyl_format(row.get("drager")):
        return None, "non_vinyl_format"
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
    key="platomania",
    config=CONFIG,
    importer_module="scripts.importers.import_platomania",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_PLATOMANIA",
    storage_prefix="platomania",
    files=ImportFileLayout(
        csv_output_path="data/raw/platomania/platomania_step2_enriched.csv",
        rejects_path="output/platomania_rejects.csv",
        summary_path="output/platomania_import_summary.json",
    ),
    row_mapper=map_platomania_row,
    description="Import Platomania enriched CSV into Supabase/Postgres",
    required_columns=(
        "title",
        "prijs",
        "url",
        "op_voorraad",
        "drager",
        "ean",
    ),
    optional_columns=(
        "artist",
        "type",
        "item_nr",
    ),
    tags=("vinyl", "enriched", "mtime-captured-at"),
    before_run=prepare_platomania_run,
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
