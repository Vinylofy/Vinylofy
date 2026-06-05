#!/usr/bin/env python3
"""
Reconcile Records on Vinyl prices from the scraper CSV into public.prices.

Purpose
-------
Use this after the normal Vinylofy importer for Records on Vinyl.

Why this exists
---------------
If the scraper CSV contains the correct current price, but public.prices still
shows an older/wrong price, the problem is no longer the scraper. This script
forces the current price from recordsonvinyl_products.csv into public.prices
for matching product IDs.

Design rules
------------
- Source of truth: data/raw/recordsonvinyl/recordsonvinyl_products.csv
- Match by normalized EAN/GTIN variants.
- Update all matching product rows for the Records on Vinyl shop.
- For duplicate rows with the same GTIN in the CSV, keep the lowest positive
  current price. For price comparison this is the safest shop-level result.
- Do not create products. The normal importer remains responsible for products.
- Do update/insert public.prices.
- Do insert price_history only if today's latest history row differs.

Usage
-----
python -u scripts/importers/reconcile_recordsonvinyl_prices.py \
  --csv data/raw/recordsonvinyl/recordsonvinyl_products.csv

Optional dry run:
python -u scripts/importers/reconcile_recordsonvinyl_prices.py \
  --csv data/raw/recordsonvinyl/recordsonvinyl_products.csv \
  --dry-run
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
import psycopg


SHOP_DOMAIN = "recordsonvinyl.nl"
SHOP_NAME = "Records on Vinyl"
SHOP_COUNTRY = "NL"
CURRENCY = "EUR"


@dataclass
class CsvPriceRow:
    source_row_number: int
    gtin14: str
    ean_display: str
    price: float
    product_url: str
    availability: str
    captured_at: datetime
    raw_title: str


def normalize_text(value: object | None) -> str:
    return re.sub(r"\s+", " ", "" if value is None else str(value)).strip()


def normalize_ean(value: object | None) -> str | None:
    raw = normalize_text(value)
    raw = re.sub(r"\.0$", "", raw)
    digits = re.sub(r"\D", "", raw)
    if not digits:
        return None
    if len(digits) == 11:
        digits = "0" + digits
    if len(digits) not in (8, 12, 13, 14):
        return None
    return digits


def normalize_gtin14(value: object | None) -> str | None:
    ean = normalize_ean(value)
    if not ean:
        return None
    return ean.zfill(14)


def identifier_variants(value: object | None) -> list[str]:
    variants: list[str] = []

    def add(v: object | None) -> None:
        text = normalize_text(v)
        if text and text not in variants:
            variants.append(text)

    raw = normalize_text(value)
    if raw:
        add(raw)

    ean = normalize_ean(value)
    if not ean:
        return variants

    add(ean)
    add(ean.zfill(14))

    if len(ean) == 14 and ean.startswith("0"):
        add(ean[1:])
    if len(ean) == 13:
        add("0" + ean)
    if len(ean) == 12:
        add("0" + ean)
        add("00" + ean)

    return variants


def parse_price(value: object | None) -> float | None:
    text = normalize_text(value)
    if not text:
        return None

    text = text.replace("€", "").replace("EUR", "").replace(" ", "").strip()

    # Dutch / mixed formats:
    # 37,95 -> 37.95
    # 1.234,56 -> 1234.56
    # 37.95 -> 37.95
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        price = float(text)
    except ValueError:
        return None

    if price <= 0 or price > 1000:
        return None

    return round(price, 2)


def parse_timestamp(value: object | None) -> datetime:
    raw = normalize_text(value)
    if not raw:
        return datetime.now(timezone.utc)

    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return datetime.now(timezone.utc)

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def first_existing(row: dict[str, str], names: Iterable[str]) -> str:
    for name in names:
        if name in row and normalize_text(row.get(name)):
            return normalize_text(row.get(name))
    return ""


def read_csv_rows(csv_path: Path) -> tuple[dict[str, CsvPriceRow], list[dict[str, object]]]:
    winners_by_gtin: dict[str, CsvPriceRow] = {}
    skipped: list[dict[str, object]] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)

        for line_number, row in enumerate(reader, start=2):
            ean_raw = first_existing(
                row,
                [
                    "ean13",
                    "ean",
                    "barcode",
                    "gtin",
                    "gtin_normalized",
                ],
            )
            gtin14 = normalize_gtin14(ean_raw)
            ean_display = normalize_ean(ean_raw)

            if not gtin14 or not ean_display:
                skipped.append(
                    {
                        "line_number": line_number,
                        "reason": "missing_or_invalid_ean",
                        "ean_raw": ean_raw,
                    }
                )
                continue

            price = parse_price(row.get("price_offer") or row.get("price") or row.get("price_list"))
            if price is None:
                skipped.append(
                    {
                        "line_number": line_number,
                        "reason": "missing_or_invalid_price",
                        "ean": ean_display,
                        "price_offer": row.get("price_offer"),
                        "price": row.get("price"),
                        "price_list": row.get("price_list"),
                    }
                )
                continue

            product_url = first_existing(row, ["product_url", "url", "link"])
            if not product_url:
                skipped.append(
                    {
                        "line_number": line_number,
                        "reason": "missing_product_url",
                        "ean": ean_display,
                        "price": price,
                    }
                )
                continue

            availability = first_existing(row, ["availability", "stock_status", "available"]) or "unknown"
            captured_at = parse_timestamp(
                first_existing(row, ["captured_at", "scraped_at", "last_seen_at", "updated_at"])
            )
            raw_title = first_existing(row, ["title", "product_title", "name"])

            candidate = CsvPriceRow(
                source_row_number=line_number,
                gtin14=gtin14,
                ean_display=ean_display,
                price=price,
                product_url=product_url,
                availability=availability,
                captured_at=captured_at,
                raw_title=raw_title,
            )

            existing = winners_by_gtin.get(gtin14)

            # Deterministic winner:
            # - lowest positive current price wins
            # - if same price, latest captured_at wins
            # This prevents a stale/higher duplicate from overwriting a valid cheaper shop price.
            if existing is None:
                winners_by_gtin[gtin14] = candidate
            elif (candidate.price, -candidate.captured_at.timestamp()) < (
                existing.price,
                -existing.captured_at.timestamp(),
            ):
                winners_by_gtin[gtin14] = candidate

    return winners_by_gtin, skipped


def ensure_shop(cur) -> str:
    cur.execute(
        """
        select id
        from public.shops
        where domain = %s
        limit 1
        """,
        (SHOP_DOMAIN,),
    )
    row = cur.fetchone()
    if row:
        return str(row[0])

    cur.execute(
        """
        insert into public.shops (name, domain, country, is_active)
        values (%s, %s, %s, true)
        returning id
        """,
        (SHOP_NAME, SHOP_DOMAIN, SHOP_COUNTRY),
    )
    return str(cur.fetchone()[0])


def find_matching_product_ids(cur, row: CsvPriceRow) -> list[str]:
    ean_candidates = identifier_variants(row.ean_display) + identifier_variants(row.gtin14)
    gtin_candidates = identifier_variants(row.gtin14) + identifier_variants(row.ean_display)

    # De-duplicate while keeping order.
    ean_candidates = list(dict.fromkeys(ean_candidates))
    gtin_candidates = list(dict.fromkeys(gtin_candidates))

    cur.execute(
        """
        select id
        from public.products
        where gtin_normalized = any(%s)
           or ean = any(%s)
        order by
          case when gtin_normalized = %s then 0 else 1 end,
          case when ean = %s then 0 else 1 end,
          updated_at desc nulls last,
          created_at desc nulls last
        """,
        (gtin_candidates, ean_candidates, row.gtin14, row.ean_display),
    )

    return [str(r[0]) for r in cur.fetchall()]


def upsert_current_price(cur, product_id: str, shop_id: str, row: CsvPriceRow) -> tuple[bool, bool]:
    cur.execute(
        """
        select price, currency, product_url, availability
        from public.prices
        where product_id = %s
          and shop_id = %s
        limit 1
        """,
        (product_id, shop_id),
    )
    existing = cur.fetchone()

    inserted = existing is None
    changed = True

    if existing is not None:
        existing_price, existing_currency, existing_url, existing_availability = existing
        changed = any(
            [
                round(float(existing_price), 2) != round(float(row.price), 2),
                str(existing_currency) != CURRENCY,
                normalize_text(existing_url) != row.product_url,
                normalize_text(existing_availability) != row.availability,
            ]
        )

    cur.execute(
        """
        insert into public.prices (
          product_id,
          shop_id,
          price,
          currency,
          product_url,
          availability,
          first_seen_at,
          last_seen_at,
          is_active,
          created_at,
          updated_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, now(), true, now(), now())
        on conflict (product_id, shop_id)
        do update set
          price = excluded.price,
          currency = excluded.currency,
          product_url = excluded.product_url,
          availability = excluded.availability,
          last_seen_at = excluded.last_seen_at,
          is_active = true,
          updated_at = now()
        """,
        (
            product_id,
            shop_id,
            row.price,
            CURRENCY,
            row.product_url,
            row.availability,
            row.captured_at,
        ),
    )

    return inserted, changed


def maybe_insert_history(cur, product_id: str, shop_id: str, row: CsvPriceRow) -> bool:
    cur.execute(
        """
        select price, availability, captured_at
        from public.price_history
        where product_id = %s
          and shop_id = %s
        order by captured_at desc
        limit 1
        """,
        (product_id, shop_id),
    )
    latest = cur.fetchone()

    if latest is not None:
        latest_price, latest_availability, latest_captured_at = latest
        same_day = latest_captured_at.date() == row.captured_at.date()
        unchanged = (
            round(float(latest_price), 2) == round(float(row.price), 2)
            and normalize_text(latest_availability) == row.availability
        )
        if same_day and unchanged:
            return False

    cur.execute(
        """
        insert into public.price_history (
          product_id,
          shop_id,
          price,
          currency,
          availability,
          captured_at,
          created_at
        )
        values (%s, %s, %s, %s, %s, %s, now())
        """,
        (product_id, shop_id, row.price, CURRENCY, row.availability, row.captured_at),
    )
    return True


def write_audit(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to recordsonvinyl_products.csv")
    parser.add_argument(
        "--audit",
        default="output/recordsonvinyl_reconcile_audit.csv",
        help="Path to write skipped/unmatched audit CSV",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_dotenv(".env.local", override=True)
    load_dotenv(override=True)

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    winners_by_gtin, skipped = read_csv_rows(csv_path)

    print(
        f"[ROV RECONCILE] rows_after_gtin_dedupe={len(winners_by_gtin)} "
        f"skipped_before_db={len(skipped)} dry_run={args.dry_run}",
        flush=True,
    )

    if args.dry_run:
        for row in list(winners_by_gtin.values())[:10]:
            print(
                f"[DRY RUN SAMPLE] gtin={row.gtin14} price={row.price:.2f} url={row.product_url}",
                flush=True,
            )
        write_audit(Path(args.audit), skipped)
        return

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set")

    unmatched: list[dict[str, object]] = []
    price_inserts = 0
    price_updates = 0
    unchanged = 0
    history_inserts = 0

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            shop_id = ensure_shop(cur)

            for row in winners_by_gtin.values():
                product_ids = find_matching_product_ids(cur, row)

                if not product_ids:
                    unmatched.append(
                        {
                            "line_number": row.source_row_number,
                            "reason": "no_matching_product",
                            "ean": row.ean_display,
                            "gtin14": row.gtin14,
                            "price": row.price,
                            "product_url": row.product_url,
                            "title": row.raw_title,
                        }
                    )
                    continue

                for product_id in product_ids:
                    inserted, changed = upsert_current_price(cur, product_id, shop_id, row)
                    if inserted:
                        price_inserts += 1
                    elif changed:
                        price_updates += 1
                    else:
                        unchanged += 1

                    if maybe_insert_history(cur, product_id, shop_id, row):
                        history_inserts += 1

        conn.commit()

    audit_rows = skipped + unmatched
    write_audit(Path(args.audit), audit_rows)

    print(
        "[ROV RECONCILE] DONE "
        f"price_inserts={price_inserts} "
        f"price_updates={price_updates} "
        f"unchanged={unchanged} "
        f"history_inserts={history_inserts} "
        f"audit_rows={len(audit_rows)} "
        f"audit_path={args.audit}",
        flush=True,
    )


if __name__ == "__main__":
    main()
