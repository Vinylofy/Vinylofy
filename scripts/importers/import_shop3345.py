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
    shop_name="3345",
    shop_domain="3345.nl",
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


def prepare_shop3345_run(csv_path: str) -> None:
    global _CAPTURED_AT
    _CAPTURED_AT = resolve_captured_at(csv_path)


def normalize_availability(value: str | None) -> str:
    raw = normalize_text(value).lower().replace("-", "_").replace(" ", "_")
    if raw in {"out_of_stock", "sold_out"}:
        return "out_of_stock"
    if raw in {"preorder", "pre_order", "coming_soon"}:
        return "preorder"
    if raw in {"in_stock", "available"}:
        return "in_stock"
    return "in_stock"


def parse_secondhand(value: str | None, artist: str, title: str, detail_status: str) -> bool:
    raw = normalize_text(value).lower()
    if raw in {"1", "true", "yes", "y"}:
        return True
    combined = f"{artist} {title} {detail_status}".lower()
    return combined.startswith("used") or "secondhand" in combined or "second_hand" in combined




def enrich_detail_status(base_status: str | None, *, artist: str, title: str) -> str:
    status = normalize_text(base_status) or "ok"
    if artist and title:
        return status

    parts = [segment for segment in status.split("|") if segment]
    if "metadata_incomplete" not in parts:
        parts.append("metadata_incomplete")
    return "|".join(parts)


def map_shop3345_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    global _CAPTURED_AT

    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("price"))
    product_url = normalize_text(row.get("url"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    format_label = normalize_text(row.get("format")) or "Vinyl"
    availability = normalize_availability(row.get("availability"))
    captured_at = _CAPTURED_AT or datetime.now(timezone.utc)
    detail_status = enrich_detail_status(
        row.get("detail_status"),
        artist=artist,
        title=title,
    )
    is_secondhand = parse_secondhand(row.get("is_secondhand"), artist, title, detail_status)

    if not ean:
        return None, "missing_or_invalid_ean"
    if not product_url:
        return None, "missing_url"
    if price is None:
        return None, "invalid_price"

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
        detail_status=detail_status,
        is_secondhand=is_secondhand,
        raw=row,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="shop3345",
    config=CONFIG,
    importer_module="scripts.importers.import_shop3345",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_SHOP3345",
    storage_prefix="shop3345",
    files=ImportFileLayout(
        csv_output_path="data/raw/shop3345/3345_products.csv",
        rejects_path="output/shop3345_rejects.csv",
        summary_path="output/shop3345_import_summary.json",
    ),
    row_mapper=map_shop3345_row,
    description="Import 3345 detail CSV into Supabase/Postgres",
    required_columns=("price", "url", "ean"),
    optional_columns=(
        "title",
        "artist",
        "release_date",
        "genre",
        "style",
        "format",
        "availability",
        "detail_status",
        "is_secondhand",
        "source_collection",
    ),
    tags=("vinyl", "detail-csv", "mtime-captured-at"),
    before_run=prepare_shop3345_run,
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
