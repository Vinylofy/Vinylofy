#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Iterable

import psycopg


@dataclass
class StageRow:
    ean: str
    gtin_normalized: str
    artist: str
    title: str
    format_label: str
    price: Decimal
    currency: str
    url: str
    availability: str
    scraped_at: datetime
    artist_normalized: str
    title_normalized: str
    search_text: str


WS_RE = re.compile(r"\s+")
NON_DIGIT_RE = re.compile(r"\D+")
PRICE_RE = re.compile(r"-?\d+[\.,]?\d*")


def normalize_text(value: object | None) -> str:
    if value is None:
        return ""
    text = str(value).replace("\x00", " ").strip()
    return WS_RE.sub(" ", text)



def normalize_free_text(value: str) -> str:
    text = normalize_text(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return WS_RE.sub(" ", text).strip()



def normalize_ean(value: object | None) -> str:
    raw = normalize_text(value)
    digits = NON_DIGIT_RE.sub("", raw)
    if len(digits) == 12:
        digits = f"0{digits}"
    return digits if len(digits) == 13 else ""



def parse_price(value: object | None) -> Decimal | None:
    raw = normalize_text(value)
    if not raw:
        return None
    m = PRICE_RE.search(raw.replace("€", ""))
    if not m:
        return None
    token = m.group(0)
    if "," in token and "." in token:
        token = token.replace(".", "").replace(",", ".")
    else:
        token = token.replace(",", ".")
    try:
        val = Decimal(token).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except InvalidOperation:
        return None
    return val if val >= 0 else None



def normalize_availability(value: object | None) -> str:
    raw = normalize_text(value).lower().replace("-", "_").replace(" ", "_")
    if raw in {"out_of_stock", "sold_out", "uitverkocht"}:
        return "out_of_stock"
    if raw in {"preorder", "pre_order", "coming_soon"}:
        return "preorder"
    return "in_stock"



def resolve_scraped_at(csv_path: Path) -> datetime:
    try:
        return datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return datetime.now(timezone.utc)



def infer_artist_title(artist_raw: str, title_raw: str) -> tuple[str, str]:
    artist = normalize_text(artist_raw)
    title = normalize_text(title_raw)
    if artist and title:
        return artist, title
    combined = title or artist
    if " - " in combined:
        a, t = combined.split(" - ", 1)
        return normalize_text(a), normalize_text(t)
    return artist, title



def load_rows(csv_path: Path) -> tuple[list[StageRow], int, int]:
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    scraped_at = resolve_scraped_at(csv_path)
    by_gtin: dict[str, StageRow] = {}
    raw_rows = 0
    rejected = 0

    with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            raw_rows += 1
            ean = normalize_ean(row.get("ean"))
            price = parse_price(row.get("price"))
            url = normalize_text(row.get("url"))
            if not ean or price is None or not url:
                rejected += 1
                continue
            artist, title = infer_artist_title(row.get("artist", ""), row.get("title", ""))
            format_label = normalize_text(row.get("format")) or "Vinyl"
            availability = normalize_availability(row.get("availability"))
            artist_norm = normalize_free_text(artist)
            title_norm = normalize_free_text(title)
            search_text = normalize_text(" ".join(x for x in [artist, title, ean] if x))
            st = StageRow(
                ean=ean,
                gtin_normalized=ean,
                artist=artist,
                title=title,
                format_label=format_label,
                price=price,
                currency="EUR",
                url=url,
                availability=availability,
                scraped_at=scraped_at,
                artist_normalized=artist_norm,
                title_normalized=title_norm,
                search_text=search_text,
            )
            by_gtin[ean] = st

    rows = list(by_gtin.values())
    return rows, raw_rows, rejected



def ensure_shop(cur) -> uuid.UUID:
    cur.execute(
        """
        select id
        from public.shops
        where domain = %s
        limit 1
        """,
        ("3345.nl",),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        """
        insert into public.shops (name, domain, country, is_active, created_at, updated_at)
        values ('3345', '3345.nl', 'NL', true, now(), now())
        returning id
        """
    )
    return cur.fetchone()[0]



def create_stage(cur) -> None:
    cur.execute(
        """
        create temporary table if not exists temp_shop3345_stage (
          ean text not null,
          gtin_normalized text not null,
          artist text,
          title text,
          format_label text,
          price numeric(10,2) not null,
          currency text not null,
          url text not null,
          availability text not null,
          scraped_at timestamptz not null,
          artist_normalized text,
          title_normalized text,
          search_text text
        ) on commit drop
        """
    )
    cur.execute("truncate temp_shop3345_stage")



def load_stage(cur, rows: Iterable[StageRow]) -> int:
    payload = [
        (
            r.ean,
            r.gtin_normalized,
            r.artist,
            r.title,
            r.format_label,
            r.price,
            r.currency,
            r.url,
            r.availability,
            r.scraped_at,
            r.artist_normalized,
            r.title_normalized,
            r.search_text,
        )
        for r in rows
    ]
    cur.executemany(
        """
        insert into temp_shop3345_stage (
          ean, gtin_normalized, artist, title, format_label, price, currency, url,
          availability, scraped_at, artist_normalized, title_normalized, search_text
        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        payload,
    )
    return len(payload)



def insert_missing_products(cur) -> int:
    cur.execute(
        """
        insert into public.products (
          id,
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
          gen_random_uuid(),
          st.ean,
          st.gtin_normalized,
          nullif(st.artist, ''),
          nullif(st.title, ''),
          nullif(st.format_label, ''),
          null,
          null,
          nullif(st.artist_normalized, ''),
          nullif(st.title_normalized, ''),
          nullif(st.search_text, ''),
          now(),
          now()
        from temp_shop3345_stage st
        left join public.products p
          on p.gtin_normalized = st.gtin_normalized
        where p.id is null
        on conflict do nothing
        """
    )
    return cur.rowcount



def upsert_prices(cur, shop_id: uuid.UUID) -> int:
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
          p.id as product_id,
          %s::uuid as shop_id,
          st.price,
          st.currency,
          st.url as product_url,
          st.availability,
          st.scraped_at as first_seen_at,
          st.scraped_at as last_seen_at,
          true,
          now(),
          now()
        from temp_shop3345_stage st
        join public.products p
          on p.gtin_normalized = st.gtin_normalized
        on conflict (product_id, shop_id) do update
          set price = excluded.price,
              currency = excluded.currency,
              product_url = excluded.product_url,
              availability = excluded.availability,
              first_seen_at = least(public.prices.first_seen_at, excluded.first_seen_at),
              last_seen_at = greatest(public.prices.last_seen_at, excluded.last_seen_at),
              is_active = true,
              updated_at = now()
        """,
        (str(shop_id),),
    )
    return cur.rowcount



def main() -> None:
    parser = argparse.ArgumentParser(description="Bulk publish 3345 CSV directly into Supabase")
    parser.add_argument("--csv", required=True, help="Path to 3345 CSV file")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    rows, raw_rows, rejected = load_rows(csv_path)
    print(f"[CSV] raw_rows={raw_rows} | accepted_unique={len(rows)} | rejected={rejected}")
    if not rows:
        raise SystemExit("No valid rows to publish")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set")

    print("[DB] connecting")
    with psycopg.connect(
        db_url,
        options="-c statement_timeout=0 -c lock_timeout=0 -c idle_in_transaction_session_timeout=0",
        autocommit=False,
    ) as conn:
        with conn.cursor() as cur:
            shop_id = ensure_shop(cur)
            print(f"[DB] shop_id={shop_id}")
            conn.commit()

            create_stage(cur)
            loaded = load_stage(cur, rows)
            print(f"[STAGE] loaded={loaded}")
            conn.commit()

            products = insert_missing_products(cur)
            print(f"[PRODUCTS] affected={products}")
            conn.commit()

            prices = upsert_prices(cur, shop_id)
            print(f"[PRICES] affected={prices}")
            conn.commit()

    print("[DONE] 3345 bulk publish complete")


if __name__ == "__main__":
    main()
