#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os

import psycopg
from dotenv import load_dotenv


def load_env() -> None:
    load_dotenv(".env.local", override=False)
    load_dotenv(".env", override=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh the precomputed Vinylofy Top 45 deals snapshot."
    )
    parser.add_argument("--write", action="store_true", help="Actually refresh the snapshot.")
    parser.add_argument("--limit", type=int, default=45, help="Snapshot size. Default: 45.")
    parser.add_argument(
        "--current-window-hours",
        type=int,
        default=48,
        help="Only use prices seen within this many hours. Default: 48.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.limit < 1 or args.limit > 45:
        raise SystemExit("--limit must be between 1 and 45.")
    if args.current_window_hours < 1:
        raise SystemExit("--current-window-hours must be at least 1.")

    load_env()
    database_url = os.environ.get("DATABASE_URL")

    if not database_url:
        raise SystemExit("DATABASE_URL is not set.")

    with psycopg.connect(
        database_url,
        options="-c statement_timeout=0 -c lock_timeout=0 -c idle_in_transaction_session_timeout=0",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("set statement_timeout = 0")
            cur.execute("set lock_timeout = 0")
            cur.execute("set idle_in_transaction_session_timeout = 0")

            if not args.write:
                cur.execute(
                    """
                    select
                      count(*) as row_count,
                      max(refreshed_at) as refreshed_at
                    from public.top_deals_snapshot
                    where snapshot_key = 'current'
                    """
                )
                row_count, refreshed_at = cur.fetchone()
                print(
                    "[TOP-DEALS-SNAPSHOT] dry-run",
                    {
                        "current_rows": int(row_count or 0),
                        "last_refreshed_at": refreshed_at.isoformat() if refreshed_at else None,
                        "write": False,
                    },
                    flush=True,
                )
                conn.rollback()
                return 0

            cur.execute(
                "select refreshed_at, row_count from public.refresh_top_deals_snapshot(%s, %s)",
                (args.limit, args.current_window_hours),
            )
            refreshed_at, row_count = cur.fetchone()
            conn.commit()

            print(
                "[TOP-DEALS-SNAPSHOT] refreshed",
                {
                    "refreshed_at": refreshed_at.isoformat(),
                    "row_count": int(row_count),
                    "limit": args.limit,
                    "current_window_hours": args.current_window_hours,
                },
                flush=True,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
