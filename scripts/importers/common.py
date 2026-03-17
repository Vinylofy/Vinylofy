#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

from dotenv import load_dotenv
import psycopg


ALLOWED_SECONDHAND_DEFAULT = {"NEE", "NO", "FALSE", "0", ""}
ALLOWED_DETAIL_STATUS_DEFAULT = {"ok"}


def log(message: str) -> None:
    print(message, flush=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


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


def parse_timestamp(value: str | None) -> datetime:
    raw = normalize_text(value)
    if not raw:
        return now_utc()

    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


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


@dataclass
class CanonicalRecord:
    source_row_number: int
    shop_name: str
    shop_domain: str
    shop_country: str
    ean: str
    artist: str
    title: str
    format_label: str | None
    cover_url: str | None
    product_url: str
    price: float
    currency: str
    availability: str
    captured_at: datetime
    product_handle: str | None
    detail_status: str
    is_secondhand: bool
    raw: dict


@dataclass
class ImportConfig:
    shop_name: str
    shop_domain: str
    shop_country: str = "NL"
    currency: str = "EUR"
    allowed_secondhand: set[str] | None = None
    allowed_detail_status: set[str] | None = None


@dataclass
class ImportStats:
    rows_raw: int = 0
    rows_accepted: int = 0
    rows_rejected: int = 0
    new_products: int = 0
    new_price_rows: int = 0
    price_updates: int = 0
    unchanged_prices: int = 0
    upserted_products: int = 0
    upserted_prices: int = 0
    inserted_history_rows: int = 0


def canonical_product_key(record: CanonicalRecord) -> tuple[str, str]:
    return normalize_text(record.artist).lower(), normalize_text(record.title).lower()


def read_and_filter(
    csv_path: Path,
    row_mapper: Callable[[dict, int], tuple[CanonicalRecord | None, str | None]],
) -> tuple[list[CanonicalRecord], list[dict]]:
    accepted: list[CanonicalRecord] = []
    rejects: list[dict] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=2):
            record, reason = row_mapper(row, idx)
            if record is None:
                rejects.append({"line_number": idx, "reason": reason, **row})
                continue
            accepted.append(record)

    grouped: dict[str, list[CanonicalRecord]] = defaultdict(list)
    for record in accepted:
        grouped[record.ean].append(record)

    deduped: list[CanonicalRecord] = []
    for records in grouped.values():
        if len(records) == 1:
            deduped.append(records[0])
            continue

        distinct_keys = {canonical_product_key(r) for r in records}
        distinct_prices = {r.price for r in records}

        benign_duplicate = len(distinct_keys) == 1 and len(distinct_prices) == 1
        if benign_duplicate:
            selected = sorted(records, key=lambda r: r.captured_at, reverse=True)[0]
            deduped.append(selected)

            for loser in records:
                if loser is not selected:
                    rejects.append({
                        "line_number": loser.source_row_number,
                        "reason": "duplicate_same_product_same_price",
                        **loser.raw,
                    })
            continue

        for record in records:
            rejects.append({
                "line_number": record.source_row_number,
                "reason": "conflicting_duplicate_ean",
                **record.raw,
            })

    return deduped, rejects


def ensure_shop(cur, config: ImportConfig) -> str:
    cur.execute(
        """
        insert into public.shops (name, domain, country, is_active)
        values (%s, %s, %s, true)
        on conflict (domain) do update
          set name = excluded.name,
              country = excluded.country,
              updated_at = now()
        returning id
        """,
        (config.shop_name, config.shop_domain, config.shop_country),
    )
    return cur.fetchone()[0]


def upsert_product(cur, record: CanonicalRecord) -> tuple[str, bool]:
    artist_norm = normalize_text(record.artist).lower()
    title_norm = normalize_text(record.title).lower()
    search_text = normalize_whitespace(f"{record.artist} {record.title} {record.ean}").lower()
    canonical_key = f"{slugify(record.artist)}::{slugify(record.title)}"

    cur.execute(
        """
        insert into public.products (
          ean, artist, title, format_label, cover_url, canonical_key,
          artist_normalized, title_normalized, search_text, created_at, updated_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        on conflict on constraint products_ean_unique do update
          set format_label = coalesce(public.products.format_label, excluded.format_label),
              cover_url = coalesce(public.products.cover_url, excluded.cover_url),
              canonical_key = coalesce(public.products.canonical_key, excluded.canonical_key),
              updated_at = now()
        returning id, (xmax = 0) as inserted
        """,
        (
            record.ean,
            record.artist,
            record.title,
            record.format_label,
            record.cover_url,
            canonical_key,
            artist_norm,
            title_norm,
            search_text,
        ),
    )
    product_id, inserted = cur.fetchone()
    return product_id, bool(inserted)


def get_existing_price_state(cur, product_id: str, shop_id: str) -> tuple[float, str, str, str] | None:
    cur.execute(
        """
        select price, currency, product_url, availability
        from public.prices
        where product_id = %s and shop_id = %s
        limit 1
        """,
        (product_id, shop_id),
    )
    row = cur.fetchone()
    if row is None:
        return None
    return float(row[0]), str(row[1]), str(row[2]), str(row[3])


def upsert_price(
    cur,
    product_id: str,
    shop_id: str,
    record: CanonicalRecord,
    imported_at: datetime,
) -> tuple[bool, bool]:
    existing = get_existing_price_state(cur, product_id, shop_id)
    inserted = existing is None
    changed = False

    if existing is not None:
        existing_price, existing_currency, existing_product_url, existing_availability = existing
        changed = any(
            [
                float(existing_price) != float(record.price),
                existing_currency != record.currency,
                existing_product_url != record.product_url,
                existing_availability != record.availability,
            ]
        )

    cur.execute(
        """
        insert into public.prices (
          product_id, shop_id, price, currency, product_url, availability,
          first_seen_at, last_seen_at, is_active, created_at, updated_at
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, true, now(), now())
        on conflict (product_id, shop_id) do update
          set price = excluded.price,
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
            record.price,
            record.currency,
            record.product_url,
            record.availability,
            imported_at,
            imported_at,
        ),
    )

    return inserted, changed


def maybe_insert_history(cur, product_id: str, shop_id: str, record: CanonicalRecord) -> bool:
    cur.execute(
        """
        select price, availability, captured_at
        from public.price_history
        where product_id = %s and shop_id = %s
        order by captured_at desc
        limit 1
        """,
        (product_id, shop_id),
    )
    latest = cur.fetchone()

    if latest is not None:
        latest_price, latest_availability, latest_captured_at = latest
        same_day = latest_captured_at.date() == record.captured_at.date()
        unchanged = float(latest_price) == float(record.price) and latest_availability == record.availability

        if same_day and unchanged:
            return False

    cur.execute(
        """
        insert into public.price_history (
          product_id, shop_id, price, currency, availability, captured_at, created_at
        ) values (%s, %s, %s, %s, %s, %s, now())
        """,
        (
            product_id,
            shop_id,
            record.price,
            record.currency,
            record.availability,
            record.captured_at,
        ),
    )
    return True


def write_rejects(path: Path, rejects: Iterable[dict]) -> None:
    rejects = list(rejects)
    if not rejects:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = sorted({key for row in rejects for key in row.keys()})
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rejects)


def run_import(
    config: ImportConfig,
    csv_path: str,
    row_mapper: Callable[[dict, int], tuple[CanonicalRecord | None, str | None]],
    dry_run: bool = False,
    rejects_path: str = "output/import_rejects.csv",
    summary_path: str = "output/import_summary.json",
) -> None:
    load_env()

    db_url = os.getenv("DATABASE_URL")
    if not db_url and not dry_run:
        raise SystemExit("DATABASE_URL is not set. Add it to .env.local or export it in the shell.")

    path = Path(csv_path)
    if not path.exists():
        raise SystemExit(f"CSV not found: {path}")

    log(f"[START] {config.shop_name} importer gestart")
    log(f"[INPUT] Bestand: {path}")
    log("[STEP 1] CSV lezen en records normaliseren...")

    accepted, rejects = read_and_filter(path, row_mapper)

    stats = ImportStats(
        rows_raw=len(accepted) + len(rejects),
        rows_accepted=len(accepted),
        rows_rejected=len(rejects),
    )

    summary = {
        "source_file": str(path),
        "rows_raw": stats.rows_raw,
        "accepted_records": stats.rows_accepted,
        "rejected_records": stats.rows_rejected,
        "shop_domain": config.shop_domain,
        "dry_run": bool(dry_run),
        "generated_at": now_utc().isoformat(),
        "new_products": 0,
        "new_price_rows": 0,
        "price_updates": 0,
        "unchanged_prices": 0,
        "upserted_products": 0,
        "upserted_prices": 0,
        "inserted_history_rows": 0,
    }

    Path(rejects_path).parent.mkdir(parents=True, exist_ok=True)
    Path(summary_path).parent.mkdir(parents=True, exist_ok=True)

    write_rejects(Path(rejects_path), rejects)

    log(f"[STEP 1 DONE] Accepted: {stats.rows_accepted} | Rejected: {stats.rows_rejected}")
    log(f"[OUTPUT] Rejects: {rejects_path}")
    log(f"[OUTPUT] Summary: {summary_path}")

    if dry_run:
        Path(summary_path).write_text(json.dumps(summary, indent=2), encoding="utf-8")
        log("[DRY RUN] Geen database writes uitgevoerd")
        print(json.dumps(summary, indent=2), flush=True)
        return

    imported_at = now_utc()

    log("[STEP 2] Verbinden met database...")

    with psycopg.connect(db_url) as conn:
        log("[DB] Verbonden")

        with conn.cursor() as cur:
            log("[STEP 3] Shop opzoeken/aanmaken...")
            shop_id = ensure_shop(cur, config)
            log(f"[DB] shop_id = {shop_id}")

            total = len(accepted)
            log(f"[STEP 4] Import starten voor {total} records...")

            for i, record in enumerate(accepted, start=1):
                product_id, product_inserted = upsert_product(cur, record)
                stats.upserted_products += 1
                if product_inserted:
                    stats.new_products += 1

                price_inserted, price_changed = upsert_price(cur, product_id, shop_id, record, imported_at)
                stats.upserted_prices += 1
                if price_inserted:
                    stats.new_price_rows += 1
                elif price_changed:
                    stats.price_updates += 1
                else:
                    stats.unchanged_prices += 1

                if maybe_insert_history(cur, product_id, shop_id, record):
                    stats.inserted_history_rows += 1

                if i == 1 or i % 100 == 0 or i == total:
                    log(
                        f"[PROGRESS] {i}/{total} | "
                        f"new_products={stats.new_products} | "
                        f"new_price_rows={stats.new_price_rows} | "
                        f"price_updates={stats.price_updates} | "
                        f"unchanged_prices={stats.unchanged_prices} | "
                        f"history={stats.inserted_history_rows}"
                    )

        log("[STEP 5] Commit...")
        conn.commit()

    summary.update(
        {
            "new_products": stats.new_products,
            "new_price_rows": stats.new_price_rows,
            "price_updates": stats.price_updates,
            "unchanged_prices": stats.unchanged_prices,
            "upserted_products": stats.upserted_products,
            "upserted_prices": stats.upserted_prices,
            "inserted_history_rows": stats.inserted_history_rows,
        }
    )

    Path(summary_path).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log("[DONE] Import afgerond")
    print(json.dumps(summary, indent=2), flush=True)