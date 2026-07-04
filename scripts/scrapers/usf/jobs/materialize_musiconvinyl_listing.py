from __future__ import annotations

import argparse
import time
from typing import Any

from scripts.scrapers.musiconvinyl import SHOP_ID, fetch_detail_metadata
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


def fetch_detail_metadata_with_retry(
    source_url: str,
    *,
    max_attempts: int = 3,
    base_sleep: float = 2.0,
) -> dict[str, Any]:
    last_error: Exception | None = None

    for attempt in range(1, max_attempts + 1):
        try:
            return dict(fetch_detail_metadata(source_url))
        except Exception as exc:
            last_error = exc
            reason = str(exc)
            is_rate_limited = "429" in reason or "Too Many Requests" in reason
            if attempt >= max_attempts:
                break

            sleep_seconds = base_sleep * attempt
            if is_rate_limited:
                sleep_seconds = max(sleep_seconds, 10.0 * attempt)

            print(
                "[MUSICONVINYL-DETAIL-RETRY]",
                {
                    "source_url": source_url,
                    "attempt": attempt,
                    "sleep_seconds": sleep_seconds,
                    "reason": reason,
                },
                flush=True,
            )
            time.sleep(sleep_seconds)

    raise last_error or RuntimeError("unknown detail metadata error")


def map_link_to_raw(link: dict[str, Any]) -> RawProductData:
    payload = dict(link.get("payload") or {})
    detail_metadata: dict[str, Any] = {}

    ean_raw = clean(payload.get("ean") or payload.get("barcode"))
    if not ean_raw:
        try:
            detail_metadata = fetch_detail_metadata_with_retry(link["source_url"])
            ean_raw = clean(detail_metadata.get("ean"))

            if ean_raw and not ean_raw.isdigit():
                print(
                    "[MUSICONVINYL-DETAIL-WARN]",
                    {
                        "source_url": link["source_url"],
                        "reason": "discarded_non_numeric_ean",
                        "ean": ean_raw,
                    },
                    flush=True,
                )
                ean_raw = None

            print(
                "[MUSICONVINYL-DETAIL] parsed",
                {
                    "source_url": link["source_url"],
                    "ean": ean_raw,
                    "ean_source": detail_metadata.get("ean_source"),
                    "catalogue_number": detail_metadata.get("catalogue_number")
                    or detail_metadata.get("catalog_number"),
                },
                flush=True,
            )
        except Exception as exc:
            print(
                "[MUSICONVINYL-DETAIL-WARN]",
                {"source_url": link["source_url"], "reason": str(exc)},
                flush=True,
            )

        time.sleep(1.5)

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
        ean_raw=ean_raw,
        price_raw=clean(payload.get("chosen_price")),
        availability_raw=availability_raw,
        image_url_raw=clean(payload.get("image_url")),
        payload={
            "source": "musiconvinyl_listing_plus_detail_payload",
            "shop_product_link_id": link["id"],
            "listing_payload": payload,
            "detail_metadata": detail_metadata,
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
