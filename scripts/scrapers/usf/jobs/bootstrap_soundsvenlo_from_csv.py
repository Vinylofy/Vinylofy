#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink


SHOP_ID = "soundsvenlo"


def load_env() -> None:
    for env_file in (".env.local", ".env"):
        path = Path(env_file)
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "").replace("\xa0", " ")).strip()


def normalize_product_url(value: object) -> str:
    text = clean(value)
    if not text:
        return ""
    text = text.split("#", 1)[0].split("?", 1)[0].rstrip("/")
    return text + "/"


def normalize_price(value: object) -> str | None:
    text = clean(value)
    if not text:
        return None
    text = text.replace("€", "").replace("EUR", "").strip()
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    else:
        text = text.replace(",", ".")
    match = re.search(r"([0-9]+(?:\.[0-9]{1,2})?)", text)
    if not match:
        return None
    amount = match.group(1)
    if "." not in amount:
        return f"{amount}.00"
    whole, cents = amount.split(".", 1)
    return f"{whole}.{cents[:2].ljust(2, '0')}"


def normalize_availability(value: object) -> str:
    text = clean(value).lower()
    if text in {"ja", "yes", "true", "1", "op voorraad"}:
        return "in_stock"
    if text in {"nee", "no", "false", "0", "niet leverbaar", "uitverkocht"}:
        return "out_of_stock"
    if "verwacht" in text or "preorder" in text or "pre-order" in text:
        return "preorder"
    return "unknown"


def normalize_ean(value: object) -> str | None:
    text = clean(value)
    if not text or text.lower() == "nan":
        return None
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    digits = re.sub(r"\D", "", text)
    if len(digits) == 11:
        digits = "0" + digits
    if len(digits) in (8, 12, 13, 14):
        return digits
    return None


def source_product_id_from_url(url: str) -> str | None:
    slug = urlparse(url).path.strip("/").split("/")[-1]
    return slug[:240] if slug else None


def title_raw_from_row(row: dict[str, str]) -> str | None:
    artist = clean(row.get("artist"))
    title = clean(row.get("title"))
    if artist and title:
        return f"{artist} | {title}"
    return title or artist or None


def listing_payload_from_row(
    row: dict[str, str],
    *,
    source: str,
    seen_at: datetime,
) -> dict[str, Any]:
    return {
        "discovery_source": source,
        "source_file": source,
        "source_url": normalize_product_url(row.get("url")),
        "artist": clean(row.get("artist")) or None,
        "title": clean(row.get("title")) or None,
        "format": clean(row.get("drager")) or None,
        "price": normalize_price(row.get("prijs")),
        "price_source": "listing_csv_jumpstart",
        "availability": normalize_availability(row.get("op_voorraad")),
        "source_categories": clean(row.get("bron_categorieen")) or None,
        "discovery_urls": clean(row.get("bron_listing_urls")) or None,
        "listing_seen_at": seen_at.isoformat(),
    }


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def raw_enrichment_exists(
    cur,
    *,
    shop_id: str,
    source_url: str,
    ean: str,
) -> bool:
    cur.execute(
        """
        select 1
          from public.raw_shop_scrapes
         where shop_id = %s
           and trim(trailing '/' from split_part(source_url, '?', 1)) =
               trim(trailing '/' from split_part(%s, '?', 1))
           and ean_raw = %s
         limit 1
        """,
        (shop_id, source_url, ean),
    )
    return cur.fetchone() is not None


def insert_raw_enrichment(
    cur,
    *,
    row: dict[str, str],
    listing_payload: dict[str, Any],
    ean: str,
) -> str | None:
    source_url = normalize_product_url(row.get("url"))
    if not source_url:
        return None

    if raw_enrichment_exists(cur, shop_id=SHOP_ID, source_url=source_url, ean=ean):
        return None

    now = datetime.now(timezone.utc)
    cur.execute(
        """
        insert into public.raw_shop_scrapes (
            run_id,
            shop_id,
            source_url,
            source_product_id,
            title_raw,
            ean_raw,
            price_raw,
            availability_raw,
            image_url_raw,
            payload
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        returning id
        """,
        (
            None,
            SHOP_ID,
            source_url,
            source_product_id_from_url(source_url),
            title_raw_from_row(row),
            ean,
            normalize_price(row.get("prijs")),
            normalize_availability(row.get("op_voorraad")),
            None,
            Jsonb(
                {
                    "source": "bootstrap_soundsvenlo_from_csv",
                    "scraped_at": now.isoformat(),
                    "detail_price_policy": "current_price_from_listing_csv_only",
                    "listing_payload": listing_payload,
                    "genre": clean(row.get("genre")) or None,
                    "label": clean(row.get("maatschappij")) or None,
                    "format": clean(row.get("drager")) or None,
                    "release_date": clean(row.get("release")) or None,
                }
            ),
        ),
    )
    return str(cur.fetchone()["id"])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bootstrap Sounds Venlo shop_product_links/raw enrichment vanuit twee CSV-bestanden."
    )
    parser.add_argument("--step1-csv", required=True)
    parser.add_argument("--step2-csv")
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_env()

    if args.write and not os.environ.get("DATABASE_URL"):
        raise SystemExit("[ERROR] DATABASE_URL ontbreekt.")

    step1_path = Path(args.step1_csv)
    if not step1_path.exists():
        raise SystemExit(f"[ERROR] step1 CSV niet gevonden: {step1_path}")

    step2_path = Path(args.step2_csv) if args.step2_csv else None
    if step2_path and not step2_path.exists():
        raise SystemExit(f"[ERROR] step2 CSV niet gevonden: {step2_path}")

    seen_at = datetime.now(timezone.utc)

    step1_rows = read_csv_rows(step1_path)
    links: list[DiscoveredLink] = []
    skipped_no_url_or_price = 0
    seen_urls: set[str] = set()

    for row in step1_rows:
        source_url = normalize_product_url(row.get("url"))
        payload = listing_payload_from_row(
            row,
            source="soundsvenlo_step1_csv",
            seen_at=seen_at,
        )
        if not source_url or not payload.get("price"):
            skipped_no_url_or_price += 1
            continue
        if source_url in seen_urls:
            continue
        seen_urls.add(source_url)
        links.append(
            DiscoveredLink(
                shop_id=SHOP_ID,
                source_url=source_url,
                source_product_id=source_product_id_from_url(source_url),
                payload=payload,
            )
        )

    print(
        "[BOOTSTRAP] step1",
        {
            "rows": len(step1_rows),
            "links_with_price": len(links),
            "skipped_no_url_or_price": skipped_no_url_or_price,
            "write": args.write,
        },
        flush=True,
    )

    if args.write and links:
        result = upsert_discovered_links(links)
        print(
            "[BOOTSTRAP] shop_product_links",
            {
                "inserted": result.inserted,
                "updated": result.updated,
                "total": result.total,
            },
            flush=True,
        )
    else:
        print("[BOOTSTRAP] dry-run: shop_product_links niet geschreven.", flush=True)

    inserted_raw = 0
    skipped_raw = 0
    invalid_ean = 0

    if step2_path:
        step2_rows = read_csv_rows(step2_path)
        step1_payload_by_url = {
            link.source_url: link.payload
            for link in links
        }

        if args.write:
            with db_connection() as conn:
                with conn.cursor(row_factory=dict_row) as cur:
                    for row in step2_rows:
                        source_url = normalize_product_url(row.get("url"))
                        ean = normalize_ean(row.get("ean"))
                        if not source_url or not ean:
                            invalid_ean += 1
                            continue

                        listing_payload = step1_payload_by_url.get(source_url)
                        if not listing_payload:
                            listing_payload = listing_payload_from_row(
                                row,
                                source="soundsvenlo_step2_csv",
                                seen_at=seen_at,
                            )

                        raw_id = insert_raw_enrichment(
                            cur,
                            row=row,
                            listing_payload=listing_payload,
                            ean=ean,
                        )
                        if raw_id:
                            inserted_raw += 1
                        else:
                            skipped_raw += 1
        else:
            for row in step2_rows:
                source_url = normalize_product_url(row.get("url"))
                ean = normalize_ean(row.get("ean"))
                if not source_url or not ean:
                    invalid_ean += 1
                else:
                    skipped_raw += 1

        print(
            "[BOOTSTRAP] step2",
            {
                "rows": len(step2_rows),
                "inserted_raw": inserted_raw,
                "skipped_existing_or_dry_run": skipped_raw,
                "invalid_or_missing_ean": invalid_ean,
                "write": args.write,
            },
            flush=True,
        )

    print("[BOOTSTRAP] complete", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
