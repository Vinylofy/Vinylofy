#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

VIEW_BY_KIND = {
    "missing": "public.cover_candidates_missing_v1",
    "failed_review": "public.cover_candidates_failed_review_v1",
    "priority": "public.cover_priority_candidates_v1",
    "status": "public.cover_management_status_v1",
}


def main() -> None:
    load_dotenv(".env.local", override=True)
    load_dotenv(override=True)

    parser = argparse.ArgumentParser(description="Export cover management lists to CSV.")
    parser.add_argument(
        "--kind",
        required=True,
        choices=sorted(VIEW_BY_KIND.keys()),
        help="Which logical export to generate",
    )
    parser.add_argument("--out", required=True, help="Output CSV path")
    args = parser.parse_args()

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise SystemExit("DATABASE_URL is not set.")

    view_name = VIEW_BY_KIND[args.kind]
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"select * from {view_name}")
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]

    with out_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        writer.writerows(rows)

    print(f"[DONE] exported {len(rows)} rows from {view_name} -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
