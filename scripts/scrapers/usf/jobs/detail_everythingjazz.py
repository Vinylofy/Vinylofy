#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import requests
from psycopg.rows import dict_row

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import insert_raw_shop_scrape, mark_detail_scraped
from scripts.scrapers.usf.jobs.everythingjazz_product_type import (
    is_everythingjazz_vinyl_type,
)

SHOP_ID = "everythingjazz"
BASE_URL = "https://eustore.everythingjazz.com"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (compatible; Vinylofy-USF/1.0; +https://vinylofy.com)"
)


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def normalize_ean(value: object) -> str | None:
    digits = re.sub(r"\D", "", clean(value))
    if len(digits) in {8, 12, 13, 14}:
        return digits
    return None


def product_js_url(source_url: str) -> str:
    parsed = urlparse(source_url)
    if parsed.netloc.lower() != "eustore.everythingjazz.com":
        raise ValueError(f"onverwacht productdomein: {source_url}")
    path = parsed.path.rstrip("/")
    if not path.startswith("/products/"):
        raise ValueError(f"onverwachte product-URL: {source_url}")
    return f"{BASE_URL}{path}.js"


def fetch_detail_queue(
    *,
    limit: int,
    retry_days: int,
    refresh_days: int,
    max_missing_ean_attempts: int,
) -> list[dict[str, Any]]:
    with db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                with raw_stats as (
                    select
                        r.shop_id,
                        r.source_url,
                        count(*) as attempts,
                        max(r.scraped_at) as latest_scraped_at,
                        (array_agg(r.id order by r.scraped_at desc nulls last, r.id desc))[1] as latest_raw_id
                    from public.raw_shop_scrapes r
                    where r.shop_id = %s
                    group by r.shop_id, r.source_url
                )
                select
                    l.id,
                    l.shop_id,
                    l.source_url,
                    l.source_product_id,
                    l.payload,
                    l.first_seen_at,
                    l.last_seen_at,
                    l.last_detail_scraped_at,
                    coalesce(rs.attempts, 0) as detail_attempts,
                    latest.ean_raw as latest_ean_raw,
                    latest.payload as latest_raw_payload,
                    latest.scraped_at as latest_raw_scraped_at
                from public.shop_product_links l
                left join raw_stats rs
                    on rs.shop_id = l.shop_id and rs.source_url = l.source_url
                left join public.raw_shop_scrapes latest
                    on latest.id = rs.latest_raw_id
                where l.shop_id = %s
                  and l.status = 'active'
                  and (
                    latest.id is null
                    or (
                        latest.ean_raw is null
                        and coalesce(rs.attempts, 0) < %s
                        and (
                            l.last_detail_scraped_at is null
                            or l.last_detail_scraped_at < now() - (%s * interval '1 day')
                        )
                    )
                    or (
                        latest.ean_raw is not null
                        and l.last_detail_scraped_at < now() - (%s * interval '1 day')
                    )
                  )
                order by
                    case when latest.id is null then 0 else 1 end,
                    case when latest.ean_raw is null then 0 else 1 end,
                    l.first_seen_at asc,
                    l.id asc
                limit %s
                """,
                (
                    SHOP_ID,
                    SHOP_ID,
                    max_missing_ean_attempts,
                    retry_days,
                    refresh_days,
                    limit,
                ),
            )
            return [dict(row) for row in cur.fetchall()]


def extract_ean(payload: dict[str, Any]) -> tuple[str | None, str | None, list[str]]:
    variants = payload.get("variants")
    if not isinstance(variants, list):
        return None, "missing_variants", []

    available_eans: list[str] = []
    all_eans: list[str] = []
    for variant in variants:
        if not isinstance(variant, dict):
            continue
        ean = normalize_ean(variant.get("barcode"))
        if not ean:
            continue
        all_eans.append(ean)
        if variant.get("available") is True:
            available_eans.append(ean)

    candidates = sorted(set(available_eans or all_eans))
    if not candidates:
        return None, "missing_ean", []
    if len(candidates) > 1:
        return None, "ambiguous_variant_ean", candidates
    return candidates[0], None, candidates


def parse_detail(
    *,
    link: dict[str, Any],
    payload: dict[str, Any],
    detail_url: str,
) -> dict[str, Any]:
    listing_payload = link.get("payload")
    if not isinstance(listing_payload, dict):
        listing_payload = {}

    product_type = clean(payload.get("type"))
    title = clean(payload.get("title")) or clean(listing_payload.get("title"))
    vendor = clean(payload.get("vendor")) or clean(listing_payload.get("artist"))
    detail_issue: str | None = None
    ean: str | None = None
    candidate_eans: list[str] = []

    if not is_everythingjazz_vinyl_type(product_type):
        detail_issue = "unsupported_product_type"
    else:
        ean, detail_issue, candidate_eans = extract_ean(payload)

    listing_price = clean(listing_payload.get("price")) or None
    listing_availability = clean(listing_payload.get("availability")).lower() or "unknown"
    detail_available = payload.get("available") is True
    availability_mismatch = (
        listing_availability in {"in_stock", "preorder"} and not detail_available
    )

    image_value = payload.get("featured_image") or payload.get("image")
    if not image_value:
        images = payload.get("images")
        if isinstance(images, list) and images:
            image_value = images[0]

    return {
        "title_raw": " - ".join(item for item in (vendor, title) if item) or None,
        "ean_raw": ean,
        # Dit is uitsluitend de listing-snapshot uit de registry.
        # De detailprijs uit product.js wordt bewust niet gebruikt.
        "price_raw": listing_price,
        "availability_raw": listing_availability,
        # Shopafbeeldingen worden niet als image_url_raw aangeboden aan promotion.
        "image_url_raw": None,
        "payload": {
            "source": "everythingjazz_product_js",
            "detail_url": detail_url,
            "detail_issue": detail_issue,
            "candidate_eans": candidate_eans,
            "product_type": product_type,
            "vendor": vendor or None,
            "title": title or None,
            "sku_values": [
                clean(variant.get("sku"))
                for variant in payload.get("variants", [])
                if isinstance(variant, dict) and clean(variant.get("sku"))
            ],
            "detail_available": detail_available,
            "availability_mismatch": availability_mismatch,
            "image_url_observed_not_imported": clean(image_value) or None,
            "listing_payload": listing_payload,
            "price_policy": "listing_snapshot_only_detail_price_ignored",
            "raw_product": payload,
        },
    }


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="USF detailverrijking via Everything Jazz product.js."
    )
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--retry-days", type=int, default=30)
    parser.add_argument("--refresh-days", type=int, default=180)
    parser.add_argument("--max-missing-ean-attempts", type=int, default=3)
    parser.add_argument("--sleep", type=float, default=0.35)
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument(
        "--output",
        default="output/usf-everythingjazz/detail-summary.json",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")
    if args.retry_days < 1 or args.refresh_days < 1:
        raise SystemExit("[ERROR] retry- en refreshdagen moeten minimaal 1 zijn.")
    if args.max_missing_ean_attempts < 1:
        raise SystemExit("[ERROR] --max-missing-ean-attempts moet minimaal 1 zijn.")
    if args.sleep < 0 or args.timeout < 1:
        raise SystemExit("[ERROR] ongeldige timing-instelling.")

    queue = fetch_detail_queue(
        limit=args.limit,
        retry_days=args.retry_days,
        refresh_days=args.refresh_days,
        max_missing_ean_attempts=args.max_missing_ean_attempts,
    )
    print(
        "[EVERYTHINGJAZZ-DETAIL]",
        {"queued": len(queue), "write": args.write},
        flush=True,
    )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "application/json",
            "Accept-Language": "en-GB,en;q=0.9,nl;q=0.8",
        }
    )
    run_id = str(uuid4())
    stats = {
        "queued": len(queue),
        "processed": 0,
        "with_ean": 0,
        "missing_ean": 0,
        "ambiguous_variant_ean": 0,
        "unsupported_product_type": 0,
        "missing_listing_price": 0,
        "request_errors": 0,
        "writes": 0,
    }

    for index, link in enumerate(queue, start=1):
        source_url = clean(link.get("source_url"))
        source_product_id = clean(link.get("source_product_id")) or None
        try:
            detail_url = product_js_url(source_url)
            response = session.get(detail_url, timeout=args.timeout)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError("product.js gaf geen JSON-object")
            parsed = parse_detail(link=link, payload=payload, detail_url=detail_url)
        except (requests.RequestException, ValueError, RuntimeError) as exc:
            stats["request_errors"] += 1
            detail_url = source_url
            listing_payload = link.get("payload")
            if not isinstance(listing_payload, dict):
                listing_payload = {}
            parsed = {
                "title_raw": None,
                "ean_raw": None,
                "price_raw": clean(listing_payload.get("price")) or None,
                "availability_raw": clean(listing_payload.get("availability")) or "unknown",
                "image_url_raw": None,
                "payload": {
                    "source": "everythingjazz_product_js",
                    "detail_url": detail_url,
                    "detail_issue": "detail_request_error",
                    "error": str(exc)[:1000],
                    "listing_payload": listing_payload,
                    "price_policy": "listing_snapshot_only_detail_price_ignored",
                },
            }

        stats["processed"] += 1
        issue = clean(parsed["payload"].get("detail_issue"))
        if parsed.get("ean_raw"):
            stats["with_ean"] += 1
        else:
            stats["missing_ean"] += 1
        if issue in stats:
            stats[issue] += 1
        if not parsed.get("price_raw"):
            stats["missing_listing_price"] += 1

        print(
            "[EVERYTHINGJAZZ-DETAIL-ITEM]",
            {
                "index": index,
                "total": len(queue),
                "url": source_url,
                "ean": parsed.get("ean_raw"),
                "issue": issue or None,
                "listing_price": parsed.get("price_raw"),
                "write": args.write,
            },
            flush=True,
        )
        if args.debug:
            print(
                "[EVERYTHINGJAZZ-DETAIL-DEBUG]",
                parsed["payload"],
                flush=True,
            )

        if args.write:
            insert_raw_shop_scrape(
                run_id=None,
                shop_id=SHOP_ID,
                source_url=source_url,
                source_product_id=source_product_id,
                title_raw=parsed.get("title_raw"),
                ean_raw=parsed.get("ean_raw"),
                price_raw=parsed.get("price_raw"),
                availability_raw=parsed.get("availability_raw"),
                image_url_raw=parsed.get("image_url_raw"),
                payload=parsed["payload"],
            )
            mark_detail_scraped(str(link["id"]))
            stats["writes"] += 1
        time.sleep(args.sleep)

    summary = {
        "shop_id": SHOP_ID,
        "run_id": run_id,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "write": args.write,
        **stats,
    }
    write_summary(Path(args.output), summary)
    print("[EVERYTHINGJAZZ-DETAIL-SUMMARY]", summary, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
