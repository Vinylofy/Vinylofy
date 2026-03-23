#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import psycopg


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply approved EAN backfill rows to products.ean.")
    parser.add_argument("apply_csv", type=Path)
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--products-table", default="public.products")
    parser.add_argument("--product-pk-column", default="id")
    parser.add_argument("--product-ean-column", default="ean")
    parser.add_argument("--commit", action="store_true", help="Actually commit writes. Default is rollback-only preview.")
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL ontbreekt. Zet die in je environment of geef --database-url mee.")

    rows = read_rows(args.apply_csv)
    sql = f"""
        UPDATE {args.products_table}
        SET {args.product_ean_column} = %s
        WHERE {args.product_pk_column} = %s
          AND {args.product_ean_column} IS NULL
    """

    updated = 0
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(sql, (row["ean_new"], int(row["product_id"])))
                updated += cur.rowcount
        if args.commit:
            conn.commit()
            print(f"Committed updates: {updated}")
        else:
            conn.rollback()
            print(f"Preview only. Rolled back updates: {updated}")


if __name__ == "__main__":
    main()
