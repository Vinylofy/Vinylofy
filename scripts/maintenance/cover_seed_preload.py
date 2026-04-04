#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from pathlib import Path

import psycopg
from dotenv import load_dotenv


def normalize_ean(value: str | None) -> str | None:
    digits = re.sub(r"\D", "", value or "")
    if not digits:
        return None
    if len(digits) == 11:
        digits = "0" + digits
    if len(digits) not in (8, 12, 13, 14):
        return None
    return digits


def read_eans_from_text(raw: str) -> list[str]:
    values = re.split(r"[\s,;]+", raw.strip())
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values:
        ean = normalize_ean(value)
        if not ean or ean in seen:
            continue
        normalized.append(ean)
        seen.add(ean)
    return normalized


def read_eans_from_file(path: Path, csv_column: str) -> list[str]:
    if path.suffix.lower() == ".csv":
        values: list[str] = []
        seen: set[str] = set()
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                ean = normalize_ean(row.get(csv_column))
                if not ean or ean in seen:
                    continue
                values.append(ean)
                seen.add(ean)
        return values

    return read_eans_from_text(path.read_text(encoding="utf-8"))


def main() -> None:
    load_dotenv(".env.local", override=True)
    load_dotenv(override=True)

    parser = argparse.ArgumentParser(description="Seed a preload batch for cover fetching.")
    parser.add_argument("--batch", required=True, help="Logical batch name, e.g. home-april-01")
    parser.add_argument("--input", help="CSV or TXT file with EANs")
    parser.add_argument("--csv-column", default="ean", help="Column name when --input is CSV")
    parser.add_argument("--eans", help="Comma/newline separated list of EANs")
    parser.add_argument("--priority", type=int, default=5000, help="Requested priority for this batch")
    parser.add_argument("--source", default="manual_seed", help="Source label stored in staging")
    parser.add_argument("--note", default=None, help="Optional note stored on each staging row")
    parser.add_argument("--apply", action="store_true", help="Immediately queue matched products after staging")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set.")

    eans: list[str] = []
    if args.input:
        eans.extend(read_eans_from_file(Path(args.input), args.csv_column))
    if args.eans:
        eans.extend(read_eans_from_text(args.eans))

    deduped = list(dict.fromkeys(eans))
    if not deduped:
        raise SystemExit("No valid EANs supplied.")

    inserted = 0
    queued = 0

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            for ean in deduped:
                cur.execute(
                    """
                    insert into public.cover_preload_stage (
                      batch_name,
                      ean,
                      requested_priority,
                      source,
                      note
                    )
                    values (%s, %s, %s, %s, %s)
                    on conflict (batch_name, ean) do update
                      set requested_priority = greatest(public.cover_preload_stage.requested_priority, excluded.requested_priority),
                          source = excluded.source,
                          note = coalesce(excluded.note, public.cover_preload_stage.note)
                    """,
                    (args.batch, ean, args.priority, args.source, args.note),
                )
                inserted += 1

            if args.apply:
                cur.execute(
                    "select public.apply_cover_preload_batch(%s, %s)",
                    (args.batch, "seed-script"),
                )
                queued = int(cur.fetchone()[0] or 0)

        conn.commit()

    summary = {
        "batch": args.batch,
        "staged_rows": inserted,
        "queued_rows": queued,
        "apply": bool(args.apply),
    }
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
