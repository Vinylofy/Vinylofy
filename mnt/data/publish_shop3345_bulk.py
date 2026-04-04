#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from dotenv import load_dotenv


SHOP_NAME = "3345"
SHOP_DOMAIN = "3345.nl"
SHOP_COUNTRY = "NL"
CURRENCY = "EUR"


@dataclass
class Record:
    ean: str
    gtin_normalized: str
    artist: str
    title: str
    format_label: str
    product_url: str
    price: float
    availability: str
    captured_at: datetime


def log(msg: str) -> None:
    print(msg, flush=True)


def load_env() -> None:
    load_dotenv(".env.local", override=True)
    load_dotenv(override=True)


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_text(value: str | None) -> str:
    value = "" if value is None else str(value)
    value = value.replace("\u200e", " ")
    return normalize_whitespace(value)


def normalize_ean(value: str | None) -> str | None:
    value = "" if value is None else str(value).strip()
    value = re.sub(r"\.0$", "", value)
    digits = re.sub(r"\D", "", value)
    if not digits:
        return None
    if len(digits) == 11:
        digits = "0" + digits
    if len(digits) not in (8, 12, 13, 14):
        return None
    return digits


def normalize_gtin14(value: str | None) -> str | None:
    digits = normalize_ean(value)
    if not digits:
        return None
    return digits.zfill(14)


def slugify(value: str) -> str:
    value = normalize_text(value).lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")


def parse_price(value: str | None) -> float | None:
    if value is None:
        return None
    cleaned = normalize_text(value)
    cleaned = cleaned.replace("€", "").replace("EUR", "").strip()
    cleaned = cleaned.replace(" ", "")
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    try:
        return float(cleaned)
    except ValueError:
        return None


def infer_artist_title(raw_artist: str | None, raw_title: str | None) -> tuple[str, str]:
    artist = normalize_text(raw_artist)
    title = normalize_text(raw_title)
    if artist:
        return artist, title
    for sep in [" – ", " - ", " — ", "–", "—"]:
        if sep in title:
            left, right = title.split(sep, 1)
            left = normalize_text(left)
            right = normalize_text(right)
            if left and right:
                return left, right
    m = re.match(r"^(.+?)-\s+(.+)$", title)
    if m:
        left, right = normalize_text(m.group(1)), normalize_text(m.group(2))
        if left and right:
            return left, right
    return "", title


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


def read_csv(csv_path: Path) -> list[Record]:
    captured_at = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
    deduped: "OrderedDict[str, Record]" = OrderedDict()
    raw_rows = 0
    rejected = 0
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw_rows += 1
            ean = normalize_ean(row.get("ean"))
            gtin_normalized = normalize_gtin14(ean)
            price = parse_price(row.get("price"))
            product_url = normalize_text(row.get("url"))
            artist, title = infer_artist_title(row.get("artist"), row.get("title"))
            format_label = normalize_text(row.get("format")) or "Vinyl"
            availability = normalize_availability(row.get("availability"))
            detail_status = normalize_text(row.get("detail_status")) or "ok"
            is_secondhand = parse_secondhand(row.get("is_secondhand"), artist, title, detail_status)

            if not ean or not gtin_normalized or not product_url or price is None or is_secondhand:
                rejected += 1
                continue

            deduped[gtin_normalized] = Record(
                ean=ean,
                gtin_normalized=gtin_normalized,
                artist=artist,
                title=title,
                format_label=format_label,
                product_url=product_url,
                price=price,
                availability=availability,
                captured_at=captured_at,
            )

    log(f"[CSV] raw_rows={raw_rows} | accepted_unique={len(deduped)} | rejected={rejected}")
    return list(deduped.values())


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
        insert into public.shops (name, domain, country, is_active, created_at, updated_at)
        values (%s, %s, %s, true, now(), now())
        returning id
        """,
        (SHOP_NAME, SHOP_DOMAIN, SHOP_COUNTRY),
    )
    return str(cur.fetchone()[0])


def stage_records(cur, records: list[Record]) -> None:
    cur.execute(
        """
        create temporary table stage_shop3345 (
            ean text not null,
            gtin_normalized text not null,
            artist text not null,
            title text not null,
            format_label text,
            product_url text not null,
            price numeric(12,2) not null,
            currency text not null,
            availability text not null,
            captured_at timestamptz not null
        ) on commit drop
        """
    )

    rows = [
        (
            r.ean,
            r.gtin_normalized,
            r.artist,
            r.title,
            r.format_label,
            r.product_url,
            r.price,
            CURRENCY,
            r.availability,
            r.captured_at,
        )
        for r in records
    ]
    cur.executemany(
        """
        insert into stage_shop3345 (
            ean, gtin_normalized, artist, title, format_label,
            product_url, price, currency, availability, captured_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )
    cur.execute("create index on stage_shop3345 (gtin_normalized)")


def upsert_products(cur) -> int:
    cur.execute(
        """
        insert into public.products (
            ean,
            gtin_normalized,
            artist,
            title,
            format_label,
            cover_url,
            canonical_key,
            artist_normalized,
            title_normalized,
            search_text,
            created_at,
            updated_at
        )
        select
            s.ean,
            s.gtin_normalized,
            s.artist,
            s.title,
            nullif(s.format_label, ''),
            null,
            concat(
                regexp_replace(lower(coalesce(s.artist, '')), '[^a-z0-9]+', '-', 'g'),
                '::',
                regexp_replace(lower(coalesce(s.title, '')), '[^a-z0-9]+', '-', 'g')
            ),
            lower(coalesce(s.artist, '')),
            lower(coalesce(s.title, '')),
            lower(trim(concat_ws(' ', s.artist, s.title, s.ean, s.gtin_normalized))),
            now(),
            now()
        from stage_shop3345 s
        on conflict (gtin_normalized) do update
        set
            ean = case
                    when public.products.ean is null then excluded.ean
                    when length(public.products.ean) = 12 and length(excluded.ean) = 13 then excluded.ean
                    else public.products.ean
                  end,
            format_label = coalesce(public.products.format_label, excluded.format_label),
            canonical_key = coalesce(public.products.canonical_key, excluded.canonical_key),
            search_text = excluded.search_text,
            updated_at = now()
        """
    )
    return cur.rowcount


def upsert_prices(cur, shop_id: str) -> int:
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
        select
            p.id,
            %s,
            s.price,
            s.currency,
            s.product_url,
            s.availability,
            s.captured_at,
            s.captured_at,
            true,
            now(),
            now()
        from stage_shop3345 s
        join public.products p
          on p.gtin_normalized = s.gtin_normalized
        on conflict (product_id, shop_id) do update
        set
            price = excluded.price,
            currency = excluded.currency,
            product_url = excluded.product_url,
            availability = excluded.availability,
            last_seen_at = excluded.last_seen_at,
            is_active = true,
            updated_at = now()
        """,
        (shop_id,),
    )
    return cur.rowcount


def insert_history(cur, shop_id: str) -> int:
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
        select
            p.id,
            %s,
            s.price,
            s.currency,
            s.availability,
            s.captured_at,
            now()
        from stage_shop3345 s
        join public.products p
          on p.gtin_normalized = s.gtin_normalized
        left join lateral (
            select ph.price, ph.availability, ph.captured_at
            from public.price_history ph
            where ph.product_id = p.id
              and ph.shop_id = %s
            order by ph.captured_at desc
            limit 1
        ) latest on true
        where latest.captured_at is null
           or latest.captured_at::date <> s.captured_at::date
           or latest.price <> s.price
           or latest.availability <> s.availability
        """,
        (shop_id, shop_id),
    )
    return cur.rowcount


def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk-publish 3345 CSV directly into Supabase")
    parser.add_argument(
        "--csv",
        default="data/raw/shop3345/3345_products.csv",
        help="Path to 3345 CSV",
    )
    args = parser.parse_args()

    load_env()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set")

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    records = read_csv(csv_path)
    if not records:
        raise SystemExit("No valid records found in CSV")

    log(f"[DB] connecting")
    with psycopg.connect(db_url, options="-c statement_timeout=0 -c lock_timeout=0 -c idle_in_transaction_session_timeout=0") as conn:
        with conn.cursor() as cur:
            shop_id = ensure_shop(cur)
            log(f"[DB] shop_id={shop_id}")
            stage_records(cur, records)
            log(f"[STAGE] loaded={len(records)}")
            products = upsert_products(cur)
            log(f"[PRODUCTS] affected={products}")
            prices = upsert_prices(cur, shop_id)
            log(f"[PRICES] affected={prices}")
            history = insert_history(cur, shop_id)
            log(f"[HISTORY] inserted={history}")
        conn.commit()

    log("[DONE] 3345 bulk publish complete")


if __name__ == "__main__":
    main()
