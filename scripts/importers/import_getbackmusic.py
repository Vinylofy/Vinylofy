from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import psycopg

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
    strict_normalize_gtin,
    load_env,
)
from scripts.importers.contracts import ImportFileLayout, ShopImporterDefinition  # noqa: E402
from scripts.importers.runner import run_registered_importer  # noqa: E402


CONFIG = ImportConfig(
    shop_name="Get Back Music",
    shop_domain="getbackmusic.nl",
    shop_country="NL",
    currency="EUR",
)


def apply_existing_offer_ean_matches(
    rows: list[dict[str, str]],
    candidates_by_url: dict[str, tuple[tuple[str, str], ...]],
) -> int:
    """Reuse EAN evidence only for an unambiguous existing shop offer.

    This is deliberately narrower than product matching: a URL may refresh an
    already-published offer, but it may never create a new public product. URLs
    shared by multiple canonical products remain unresolved and still require
    detail-page EAN evidence.
    """
    resolved = 0
    for row in rows:
        if strict_normalize_gtin(row.get("ean")):
            continue
        product_url = normalize_text(row.get("product_url"))
        candidates = candidates_by_url.get(product_url, ())
        if len(candidates) != 1:
            continue
        _product_id, ean = candidates[0]
        if not strict_normalize_gtin(ean):
            continue
        row["ean"] = ean
        row["detail_status"] = "existing_offer_ean_reused"
        row["detail_error"] = ""
        resolved += 1
    return resolved


def resolve_existing_offer_eans(csv_path: str) -> int:
    """Recover EANs for existing Get Back offers after state loss.

    The lookup is exact on the shop's product URL and only accepts one existing
    product candidate. It is read-only and preserves the EAN gate for new
    products.
    """
    load_env()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        print("[GETBACK] DATABASE_URL ontbreekt; bestaande offer-EAN-resolutie overgeslagen", flush=True)
        return 0

    path = Path(csv_path)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = tuple(reader.fieldnames or ())
        rows = list(reader)

    urls = sorted({
        normalize_text(row.get("product_url"))
        for row in rows
        if not strict_normalize_gtin(row.get("ean")) and normalize_text(row.get("product_url"))
    })
    if not urls:
        return 0

    candidates_by_url: dict[str, set[tuple[str, str]]] = {}
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select pr.product_url, p.id::text, p.ean
                from public.prices pr
                join public.shops s on s.id = pr.shop_id
                join public.products p on p.id = pr.product_id
                where s.domain = %s
                  and pr.product_url = any(%s)
                  and p.ean is not null
                """,
                (CONFIG.shop_domain, urls),
            )
            for product_url, product_id, ean in cur.fetchall():
                candidates_by_url.setdefault(str(product_url), set()).add((str(product_id), str(ean)))

    normalized_candidates = {
        url: tuple(sorted(candidates))
        for url, candidates in candidates_by_url.items()
    }
    resolved = apply_existing_offer_ean_matches(rows, normalized_candidates)
    if not resolved:
        print(
            f"[GETBACK] bestaande offer-EAN-resolutie=0 urls={len(urls)}",
            flush=True,
        )
        return 0

    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    ambiguous = sum(1 for candidates in normalized_candidates.values() if len(candidates) > 1)
    print(
        f"[GETBACK] bestaande offer-EAN-resolutie={resolved} urls={len(urls)} ambiguous_urls={ambiguous}",
        flush=True,
    )
    return resolved


def resolve_ean_rejection(row: dict) -> str:
    status = normalize_text(row.get("detail_status")).casefold()
    if status in {"", "pending", "listing"}:
        return "detail_not_attempted"
    if status == "technical_error":
        return "detail_technical_error"
    return "missing_or_invalid_ean"


def resolve_availability(value: str | None) -> str:
    normalized = normalize_text(value).casefold()
    return normalized if normalized in {"in_stock", "out_of_stock", "preorder"} else "unknown"


def map_getbackmusic_row(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean"))
    gtin_normalized = strict_normalize_gtin(row.get("ean"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    product_url = normalize_text(row.get("product_url"))
    price = parse_price(row.get("price"))
    product_id = normalize_text(row.get("product_id"))
    variant_id = normalize_text(row.get("variant_id"))

    if not ean or not gtin_normalized:
        return None, resolve_ean_rejection(row)
    if not product_url:
        return None, "missing_url"
    if not product_id or not variant_id:
        return None, "missing_product_or_variant_id"
    if price is None:
        return None, "invalid_price"
    if not artist:
        return None, "missing_artist_after_inference"
    if not title:
        return None, "missing_title"

    image_url = normalize_text(row.get("image_url")) or None
    return CanonicalRecord(
        source_row_number=line_number,
        shop_name=CONFIG.shop_name,
        shop_domain=CONFIG.shop_domain,
        shop_country=CONFIG.shop_country,
        ean=ean,
        artist=artist,
        title=title,
        format_label=normalize_text(row.get("format")) or "Vinyl",
        cover_url=None,
        product_url=product_url,
        price=price,
        currency=CONFIG.currency,
        availability=resolve_availability(row.get("availability")),
        captured_at=parse_timestamp(row.get("scraped_at")),
        product_handle=f"{product_id}:{variant_id}",
        detail_status=normalize_text(row.get("detail_status")) or "ok",
        is_secondhand=False,
        raw=row,
        cover_candidate_url=image_url,
        cover_candidate_source_type="shop_listing_image" if image_url else None,
        cover_candidate_page_url=product_url if image_url else None,
        cover_candidate_queue_priority=100 if image_url else None,
        gtin_normalized=gtin_normalized,
    ), None


SHOP_DEFINITION = ShopImporterDefinition(
    key="getbackmusic",
    config=CONFIG,
    importer_module="scripts.importers.import_getbackmusic",
    scraper_command_env="VINYLOFY_SCRAPER_CMD_GETBACKMUSIC",
    storage_prefix="getbackmusic",
    files=ImportFileLayout(
        csv_output_path="data/raw/getbackmusic/getbackmusic_master.csv",
        rejects_path="output/getbackmusic_rejects.csv",
        summary_path="output/getbackmusic_import_summary.json",
    ),
    row_mapper=map_getbackmusic_row,
    before_run=resolve_existing_offer_eans,
    description="Import Get Back Music listing-first LP/Vinyl CSV into Supabase/Postgres",
    required_columns=("scraped_at", "product_url", "product_id", "variant_id", "ean", "artist", "title", "price", "availability"),
    optional_columns=(
        "source_shop", "product_key", "standard_price", "is_sale", "currency", "format", "image_url", "page_found",
        "detail_title", "detail_description", "release_date", "label", "catalogue_number", "detail_status", "detail_error", "enriched_at",
    ),
    tags=("vinyl", "shopify", "listing-first", "barcode-gated", "variant-safe"),
    include_in_all=False,
)


def main() -> None:
    run_registered_importer(SHOP_DEFINITION)


if __name__ == "__main__":
    main()
