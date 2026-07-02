#!/usr/bin/env python3
from __future__ import annotations

import argparse
from typing import Any

from scripts.scrapers.usf.core.models import RawProductData
from scripts.scrapers.usf.core.raw_materializer import materialize_queued_links

SHOP_ID = "dgmoutlet"


def clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def build_title(
    payload: dict[str, Any],
    *,
    fallback_url: str | None = None,
    source_product_id: str | None = None,
) -> str | None:
    raw_name = clean(payload.get("raw_name"))
    if raw_name:
        return raw_name

    title = clean(payload.get("title"))
    if title:
        return title

    ean = clean(payload.get("ean"))
    if ean:
        return f"DGM Outlet EAN {ean}"

    handle = clean(source_product_id)
    if handle:
        return f"DGM Outlet {handle}"

    url = clean(fallback_url)
    if url:
        return f"DGM Outlet {url.rstrip('/').split('/')[-1]}"

    return "DGM Outlet offer"


def map_link_to_raw(link: dict[str, Any]) -> RawProductData:
    listing_payload = dict(link.get("payload") or {})
    availability_raw = clean(listing_payload.get("availability")) or "unknown"
    is_out_of_stock = availability_raw == "out_of_stock"

    # DGM: OOS mag niet staged/promoted worden als actuele prijs.
    # stage_latest_raw_snapshots pakt alleen rows met price_raw != null.
    price_raw = None if is_out_of_stock else clean(listing_payload.get("price_current"))

    if is_out_of_stock:
        print(
            "[DGM-SEEN-OOS] "
            f"source_url={link['source_url']} "
            f"ean={clean(listing_payload.get('ean'))} "
            f"availability_source={clean(listing_payload.get('availability_source'))}",
            flush=True,
        )
    else:
        print(
            "[DGM-SEEN-INSTOCK] "
            f"source_url={link['source_url']} "
            f"ean={clean(listing_payload.get('ean'))} "
            f"price_raw={price_raw} availability_raw={availability_raw}",
            flush=True,
        )

    return RawProductData(
        shop_id=SHOP_ID,
        source_url=link["source_url"],
        source_product_id=link.get("source_product_id"),
        title_raw=build_title(
            listing_payload,
            fallback_url=link["source_url"],
            source_product_id=link.get("source_product_id"),
        ),
        ean_raw=clean(listing_payload.get("ean")),
        price_raw=price_raw,
        availability_raw=availability_raw,
        image_url_raw=clean(listing_payload.get("image_url")),
        payload={
            "source": "dgmoutlet_listing_payload",
            "shop_product_link_id": link["id"],
            "listing_payload": listing_payload,
        },
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialiseer DGM listingpayload atomair naar raw_shop_scrapes."
    )
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Schrijf raw snapshots en haal links atomair uit de queue.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    result = materialize_queued_links(
        shop_id=SHOP_ID,
        limit=args.limit,
        mapper=map_link_to_raw,
        write=args.write,
    )

    print(
        f"[MATERIALIZE] shop={SHOP_ID} queued={result.queued} "
        f"processed={result.processed} write={args.write}",
        flush=True,
    )

    for item in result.items[:5]:
        print(
            "[SAMPLE]",
            {
                "link_id": item.link_id,
                "raw_id": item.raw_id,
                "source_url": item.raw.source_url,
                "ean_raw": item.raw.ean_raw,
                "price_raw": item.raw.price_raw,
                "availability_raw": item.raw.availability_raw,
                "image_url_raw": item.raw.image_url_raw,
            },
            flush=True,
        )

    if not args.write:
        print("[MATERIALIZE] dry-run complete; geen databasewrites.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
