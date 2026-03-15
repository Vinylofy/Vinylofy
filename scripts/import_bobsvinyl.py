#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
import psycopg


SHOP_NAME = "Bob's Vinyl"
SHOP_DOMAIN = "bobsvinyl.nl"
CURRENCY = "EUR"
ALLOWED_SECONDHAND = {"NEE", "NO", "FALSE", "0", ""}
ALLOWED_DETAIL_STATUS = {"ok"}


def log(message: str) -> None:
    print(message, flush=True)


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_ean(value: str | None) -> str | None:
    value = "" if value is None else str(value).strip()
    value = re.sub(r"\.0$", "", value)
    digits = re.sub(r"\D", "", value)

    if not digits:
        return None

    # 11 digits -> waarschijnlijk UPC met weggevallen voorloopnul
    if len(digits) == 11:
        digits = "0" + digits

    if len(digits) not in (8, 12, 13, 14):
        return None

    return digits


def normalize_text(value: str | None) -> str:
    value = "" if value is None else str(value)
    value = value.replace("\u200e", " ")
    return normalize_whitespace(value)


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

    # 1.234,56 -> 1234.56
    # 1234,56  -> 1234.56
    # 1234.56  -> 1234.56
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

    # Fallback: "Blues Pills- Birthday"
    m = re.match(r"^(.+?)-\s+(.+)$", title)
    if m:
        left, right = normalize_text(m.group(1)), normalize_text(m.group(2))
        if left and right:
            return left, right

    return "", title


@dataclass
class CanonicalRecord:
    source_row_number: int
    shop_domain: str
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


def to_canonical(row: dict, line_number: int) -> tuple[CanonicalRecord | None, str | None]:
    ean = normalize_ean(row.get("ean"))
    price = parse_price(row.get("prijs"))
    product_url = normalize_text(row.get("url"))
    detail_status = normalize_text(row.get("detail_status")).lower()
    secondhand_raw = normalize_text(row.get("mogelijk_2e_hands")).upper()
    is_secondhand = secondhand_raw not in ALLOWED_SECONDHAND
    captured_at = parse_timestamp(row.get("detail_checked_at"))
    artist, title = infer_artist_title(row.get("artist"), row.get("title"))
    format_label = normalize_text(row.get("drager")) or None
    cover_url = None

    if detail_status not in ALLOWED_DETAIL_STATUS:
        return None, "detail_status_not_ok"
    if is_secondhand:
        return None, "secondhand"
    if not ean:
        return None, "missing_or_invalid_ean"
    if not product_url:
        return None, "missing_url"
    if price is None:
        return None, "invalid_price"
    if not title:
        return None, "missing_title"
    if not artist:
        return None, "missing_artist_after_inference"

    return CanonicalRecord(
        source_row_number=line_number,
        shop_domain=SHOP_DOMAIN,
        ean=ean,
        artist=artist,
        title=title,
        format_label=format_label,
        cover_url=cover_url,
        product_url=product_url,
        price=price,
        currency=CURRENCY,
        availability="in_stock",
        captured_at=captured_at,
        product_handle=normalize_text(row.get("product_handle")) or None,
        detail_status=detail_status,
        is_secondhand=is_secondhand,
        raw=row,
    ), None


def canonical_product_key(record: CanonicalRecord) -> tuple[str, str]:
    return normalize_text(record.artist).lower(), normalize_text(record.title).lower()


def read_and_filter(csv_path: Path) -> tuple[list[CanonicalRecord], list[dict]]:
    accepted: list[CanonicalRecord] = []
    rejects: list[dict] = []

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=2):
            record, reason = to_canonical(row, idx)
            if record is None:
                rejects.append({"line_number": idx, "reason": reason, **row})
                continue
            accepted.append(record)

    grouped: dict[str, list[CanonicalRecord]] = defaultdict(list)
    for record in accepted:
        grouped[record.ean].append(record)

    deduped: list[CanonicalRecord] = []
    for ean, records in grouped.items():
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


def ensure_shop(cur) -> str:
    cur.execute(
        """
        insert into public.shops (name, domain, country, is_active)
        values (%s, %s, %s, true)
        on conflict (domain) do update
          set name = excluded.name,
              updated_at = now()
        returning id
        """,
        (SHOP_NAME, SHOP_DOMAIN, "NL"),
    )
    return cur.fetchone()[0]


def upsert_product(cur, record: CanonicalRecord) -> str:
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
        returning id
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
    return cur.fetchone()[0]


def upsert_price(
    cur,
    product_id: str,
    shop_id: str,
    record: CanonicalRecord,
    imported_at: datetime,
) -> None:
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
            imported_at,   # eerste keer gezien
            imported_at,   # laatst bevestigd door deze import-run
        ),
    )


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


def main() -> None:
    load_dotenv(".env.local", override=True)
    load_dotenv(override=True)

    parser = argparse.ArgumentParser(description="Import Bob's Vinyl enriched CSV into Supabase/Postgres")
    parser.add_argument("csv_path", help="Path to bobsvinyl_step2_enriched.csv")
    parser.add_argument("--dry-run", action="store_true", help="Validate and summarize without writing to the database")
    parser.add_argument("--rejects", default="output/bobsvinyl_rejects.csv", help="Path to write rejected rows CSV")
    parser.add_argument("--summary", default="output/bobsvinyl_import_summary.json", help="Path to write summary JSON")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url and not args.dry_run:
        raise SystemExit("DATABASE_URL is not set. Add it to .env.local or export it in the shell.")

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    log("[START] Bob's Vinyl importer gestart")
    log(f"[INPUT] Bestand: {csv_path}")
    log("[STEP 1] CSV lezen en records normaliseren...")

    accepted, rejects = read_and_filter(csv_path)

    summary = {
        "source_file": str(csv_path),
        "accepted_records": len(accepted),
        "rejected_records": len(rejects),
        "shop_domain": SHOP_DOMAIN,
        "dry_run": bool(args.dry_run),
        "generated_at": now_utc().isoformat(),
    }

    Path(args.rejects).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary).parent.mkdir(parents=True, exist_ok=True)
    write_rejects(Path(args.rejects), rejects)

    log(f"[STEP 1 DONE] Accepted: {len(accepted)} | Rejected: {len(rejects)}")
    log(f"[OUTPUT] Rejects: {args.rejects}")
    log(f"[OUTPUT] Summary: {args.summary}")

    if args.dry_run:
        Path(args.summary).write_text(json.dumps(summary, indent=2), encoding="utf-8")
        log("[DRY RUN] Geen database writes uitgevoerd")
        print(json.dumps(summary, indent=2), flush=True)
        return

    inserted_history = 0
    upserted_prices = 0
    upserted_products = 0
    imported_at = now_utc()

    log("[STEP 2] Verbinden met database...")

    with psycopg.connect(db_url) as conn:
        log("[DB] Verbonden")

        with conn.cursor() as cur:
            log("[STEP 3] Shop opzoeken/aanmaken...")
            shop_id = ensure_shop(cur)
            log(f"[DB] shop_id = {shop_id}")

            total = len(accepted)
            log(f"[STEP 4] Import starten voor {total} records...")

            for i, record in enumerate(accepted, start=1):
                product_id = upsert_product(cur, record)
                upserted_products += 1

                upsert_price(cur, product_id, shop_id, record, imported_at)
                upserted_prices += 1

                if maybe_insert_history(cur, product_id, shop_id, record):
                    inserted_history += 1

                if i == 1 or i % 100 == 0 or i == total:
                    log(
                        f"[PROGRESS] {i}/{total} | "
                        f"products={upserted_products} | "
                        f"prices={upserted_prices} | "
                        f"history={inserted_history}"
                    )

        log("[STEP 5] Commit...")
        conn.commit()

    summary.update(
        {
            "upserted_products": upserted_products,
            "upserted_prices": upserted_prices,
            "inserted_history_rows": inserted_history,
        }
    )

    Path(args.summary).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log("[DONE] Import afgerond")
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()