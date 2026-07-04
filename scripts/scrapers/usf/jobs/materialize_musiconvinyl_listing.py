from __future__ import annotations

import argparse
from typing import Any

from scripts.scrapers.musiconvinyl import SHOP_ID
from scripts.scrapers.usf.core.models import RawProductData
from scripts.scrapers.usf.core.raw_materializer import materialize_queued_links


def clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def title_from_payload(payload: dict[str, Any], source_url: str) -> str:
    title = clean(payload.get("title"))
    artist = clean(payload.get("artist"))
    if title and artist:
        return f"{artist} - {title}"
    if title:
        return title
    ean = clean(payload.get("ean"))
    if ean:
        return f"Music On Vinyl EAN {ean}"
    return f"Music On Vinyl {source_url.rstrip('/').split('/')[-1]}"


def map_link_to_raw(link: dict[str, Any]) -> RawProductData:
    payload = dict(link.get("payload") or {})
    availability = clean(payload.get("availability"))
    if availability == "preorder":
        # Bewust niet als normale actieve voorraad behandelen.
        availability_raw = "preorder"
    else:
        availability_raw = availability

    return RawProductData(
        shop_id=SHOP_ID,
        source_url=link["source_url"],
        source_product_id=clean(link.get("source_product_id")),
        title_raw=title_from_payload(payload, link["source_url"]),
        ean_raw=clean(payload.get("ean") or payload.get("barcode")),
        price_raw=clean(payload.get("chosen_price")),
        availability_raw=availability_raw,
        image_url_raw=clean(payload.get("image_url")),
        payload={
            "source": "musiconvinyl_listing_payload",
            "shop_product_link_id": link["id"],
            "listing_payload": payload,
            "price_source": payload.get("price_source"),
            "regular_price": payload.get("regular_price"),
            "sale_price": payload.get("sale_price"),
            "chosen_price": payload.get("chosen_price"),
        },
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Materialize Music On Vinyl listing payloads to raw_shop_scrapes."
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--write", action="store_true")
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
        f"[MATERIALIZE] shop={SHOP_ID} queued={result.queued} processed={result.processed} write={args.write}",
        flush=True,
    )
    for item in result.items[:10]:
        print(
            "[MATERIALIZE-SAMPLE]",
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
