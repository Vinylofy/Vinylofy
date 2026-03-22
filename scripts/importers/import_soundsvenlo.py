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
    shop_name="Sounds Venlo",
    shop_domain="sounds-venlo.nl",
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


def prepare_soundsvenlo_run(csv_path: str) -> None:
    global _CAPTURED_AT
    _CAPTURED_AT = resolve_captured_at(csv_path)


def resolve_availability(raw_value: str | None) -> str:
    value = normalize_text(raw_value).upper()
    if value == "JA":
        return "in_stock"
    if value == "NEE":
        return "out_of_stock"
    return "unknown"


def map_soundsvenlo_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    global _CAPTURED_AT

    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("prijs"))
    product_url = normalize_text(row.get("url"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    format_label = normalize_text(row.get("drager")) or "Vinyl"
    availability = resolve_availability(row.get("op_voorraad"))
    captured_at = _CAPTURED_AT or datetime.now(timezone.utc)

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
        product_handle=None,
        detail_status="ok",
        is_secondhand=False,
        raw=row,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="soundsvenlo",
    config=CONFIG,
    importer_module="scripts.importers.import_soundsvenlo",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_SOUNDSVENLO",
    storage_prefix="soundsvenlo",
    files=ImportFileLayout(
        csv_output_path="data/raw/soundsvenlo/sounds_venlo_step2_enriched.csv",
        rejects_path="output/soundsvenlo_rejects.csv",
        summary_path="output/soundsvenlo_import_summary.json",
    ),
    row_mapper=map_soundsvenlo_row,
    description="Import Sounds Venlo step2 enriched CSV into Supabase/Postgres",
    required_columns=("title", "prijs", "url", "op_voorraad", "ean"),
    optional_columns=("artist", "drager", "genre", "release", "maatschappij", "bron_categorieen", "bron_listing_urls"),
    tags=("vinyl", "step2-enriched", "mtime-captured-at"),
    before_run=prepare_soundsvenlo_run,
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
