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
    shop_name="Sounds Haarlem",
    shop_domain="soundshaarlem.nl",
    shop_country="NL",
    currency="EUR",
)


UNKNOWN_ARTIST = "Unknown Artist"
UNKNOWN_TITLE = "Unknown Title"


def slug_to_text(value: str | None) -> str:
    raw = normalize_text(value)
    if not raw:
        return ""
    return normalize_text(raw.replace("-", " "))


def resolve_ean(row: dict) -> str | None:
    normalized = normalize_text(row.get("ean_normalized"))
    if normalized:
        return normalize_ean(normalized)
    return normalize_ean(row.get("ean_raw"))


def resolve_availability(raw_status: str | None, raw_label: str | None, price: float | None) -> str:
    status = normalize_text(raw_status).lower()
    label = normalize_text(raw_label).lower()

    if status in {"out_of_stock", "outofstock"}:
        return "out_of_stock"
    if status in {"in_stock", "instock"}:
        return "in_stock"

    out_of_stock_tokens = (
        "niet op voorraad",
        "uitverkocht",
        "sold out",
        "not in stock",
        "out of stock",
        "temporarily unavailable",
    )
    in_stock_tokens = (
        "op voorraad",
        "leverkans",
        "levertijd",
        "preorder",
        "bestelbaar",
        "in stock",
        "estimated delivery",
        "delivery chance",
    )

    if any(token in label for token in out_of_stock_tokens):
        return "out_of_stock"
    if any(token in label for token in in_stock_tokens):
        return "in_stock"

    return "in_stock" if price is not None else "unknown"


def build_fallback_display_name(row: dict) -> str:
    candidates = [
        normalize_text(row.get("display_name_raw")),
        normalize_text(row.get("title_raw")),
        slug_to_text(row.get("title_slug_raw")),
    ]
    for value in candidates:
        if value:
            return value
    return ""



def normalize_artist_title(row: dict) -> tuple[str, str]:
    slug_artist = slug_to_text(row.get("artist_slug_raw"))
    slug_title = slug_to_text(row.get("title_slug_raw"))
    display_name = build_fallback_display_name(row)

    raw_artist = normalize_text(row.get("artist_raw")) or slug_artist
    raw_title = normalize_text(row.get("title_raw")) or slug_title or display_name

    artist, inferred_title = infer_artist_title(raw_artist, raw_title)
    title = normalize_text(inferred_title) or raw_title

    if not artist and display_name and slug_artist:
        compact_display = display_name.lower()
        compact_artist = slug_artist.lower()
        if compact_display.startswith(compact_artist + " "):
            remainder = normalize_text(display_name[len(slug_artist) :])
            if remainder:
                artist = slug_artist
                title = remainder

    if not artist and slug_artist:
        artist = slug_artist
    if not title and slug_title:
        title = slug_title
    if not title and display_name:
        title = display_name

    artist = normalize_text(artist) or UNKNOWN_ARTIST
    title = normalize_text(title) or UNKNOWN_TITLE
    return artist, title



def map_soundshaarlem_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = resolve_ean(row)
    price = parse_price(row.get("price_current"))
    product_url = normalize_text(row.get("detail_url"))
    captured_at = parse_timestamp(row.get("scraped_at"))

    artist, title = normalize_artist_title(row)

    format_label = normalize_text(row.get("format_label_raw")) or "Vinyl"
    availability = resolve_availability(row.get("availability"), row.get("availability_text"), price)
    product_handle = normalize_text(row.get("release_id_raw")) or None

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
        product_handle=product_handle,
        detail_status="ok",
        is_secondhand=False,
        raw=row,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="soundshaarlem",
    config=CONFIG,
    importer_module="scripts.importers.import_soundshaarlem",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_SOUNDSHAARLEM",
    storage_prefix="soundshaarlem",
    files=ImportFileLayout(
        csv_output_path="data/raw/soundshaarlem/soundshaarlem_products.csv",
        rejects_path="output/soundshaarlem_rejects.csv",
        summary_path="output/soundshaarlem_import_summary.json",
    ),
    row_mapper=map_soundshaarlem_row,
    description="Import Sounds Haarlem listing/detail CSV into Supabase/Postgres",
    required_columns=(
        "detail_url",
        "artist_slug_raw",
        "title_slug_raw",
        "price_current",
        "scraped_at",
    ),
    optional_columns=(
        "release_id_raw",
        "ean_raw",
        "ean_normalized",
        "ean_source",
        "display_name_raw",
        "title_raw",
        "artist_raw",
        "format_label_raw",
        "price_old",
        "currency",
        "availability",
        "availability_text",
        "release_date_raw",
        "import_text",
        "listing_url",
        "source_type",
        "shop_id",
    ),
    tags=("vinyl", "listing-first", "url-ean"),
)



def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
