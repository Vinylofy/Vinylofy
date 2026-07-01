#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from dotenv import load_dotenv


DEFAULT_CURRENT_WINDOW_HOURS = 48
DEFAULT_AVAILABILITIES = ("in_stock", "unknown")


def load_env() -> None:
    load_dotenv(".env.local", override=False)
    load_dotenv(".env", override=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Schrijf een dagelijkse snapshot van actuele Vinylofy-prijzen naar "
            "public.price_history, zodat de prijsgrafiek-dekking niet afhankelijk "
            "is van welke importer/promoter toevallig die dag draaide."
        )
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Voer echte databasewrites uit. Zonder deze vlag is het een dry-run.",
    )
    parser.add_argument(
        "--current-window-hours",
        type=int,
        default=DEFAULT_CURRENT_WINDOW_HOURS,
        help=(
            "Alleen prices meenemen waarvan last_seen_at binnen dit aantal uur valt. "
            "Default: 48, gelijk aan de current-offer logica."
        ),
    )
    parser.add_argument(
        "--day",
        default=None,
        help=(
            "UTC snapshotdag in YYYY-MM-DD. Default is vandaag UTC. "
            "Gebruik dit alleen bewust; voor normaal gebruik leeg laten."
        ),
    )
    return parser.parse_args()


def get_snapshot_day(day_arg: str | None) -> str:
    if day_arg:
        datetime.fromisoformat(f"{day_arg}T00:00:00+00:00")
        return day_arg

    return datetime.now(timezone.utc).date().isoformat()


def main() -> int:
    args = parse_args()
    if args.current_window_hours < 1:
        raise SystemExit("--current-window-hours moet minimaal 1 zijn.")

    load_env()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL ontbreekt.")

    snapshot_day = get_snapshot_day(args.day)
    dry_run = not args.write

    sql_count_eligible = """
        select count(*)
        from public.prices p
        where p.is_active = true
          and p.price is not null
          and p.last_seen_at >= now() - (%s * interval '1 hour')
          and coalesce(p.availability, 'unknown') = any(%s)
    """

    sql_count_to_insert = """
        with eligible as (
          select
            p.product_id,
            p.shop_id,
            p.price,
            coalesce(p.currency, 'EUR') as currency,
            coalesce(p.availability, 'unknown') as availability
          from public.prices p
          where p.is_active = true
            and p.price is not null
            and p.last_seen_at >= now() - (%s * interval '1 hour')
            and coalesce(p.availability, 'unknown') = any(%s)
        )
        select count(*)
        from eligible e
        where not exists (
          select 1
          from public.price_history h
          where h.product_id = e.product_id
            and h.shop_id = e.shop_id
            and h.captured_at >= (%s::date)::timestamptz
            and h.captured_at < ((%s::date + interval '1 day'))::timestamptz
            and h.price = e.price
            and h.availability = e.availability
        )
    """

    sql_insert = """
        with eligible as (
          select
            p.product_id,
            p.shop_id,
            p.price,
            coalesce(p.currency, 'EUR') as currency,
            coalesce(p.availability, 'unknown') as availability
          from public.prices p
          where p.is_active = true
            and p.price is not null
            and p.last_seen_at >= now() - (%s * interval '1 hour')
            and coalesce(p.availability, 'unknown') = any(%s)
        ),
        inserted as (
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
            e.product_id,
            e.shop_id,
            e.price,
            e.currency,
            e.availability,
            now(),
            now()
          from eligible e
          where not exists (
            select 1
            from public.price_history h
            where h.product_id = e.product_id
              and h.shop_id = e.shop_id
              and h.captured_at >= (%s::date)::timestamptz
              and h.captured_at < ((%s::date + interval '1 day'))::timestamptz
              and h.price = e.price
              and h.availability = e.availability
          )
          returning 1
        )
        select count(*) from inserted
    """

    with psycopg.connect(
        database_url,
        options="-c statement_timeout=0 -c lock_timeout=0 -c idle_in_transaction_session_timeout=0",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("set statement_timeout = 0")
            cur.execute("set lock_timeout = 0")
            cur.execute("set idle_in_transaction_session_timeout = 0")

            cur.execute(
                sql_count_eligible,
                (args.current_window_hours, list(DEFAULT_AVAILABILITIES)),
            )
            eligible_count = int(cur.fetchone()[0])

            cur.execute(
                sql_count_to_insert,
                (
                    args.current_window_hours,
                    list(DEFAULT_AVAILABILITIES),
                    snapshot_day,
                    snapshot_day,
                ),
            )
            to_insert_count = int(cur.fetchone()[0])

            print(
                "[PRICE-HISTORY-SNAPSHOT]",
                {
                    "snapshot_day": snapshot_day,
                    "current_window_hours": args.current_window_hours,
                    "eligible_current_prices": eligible_count,
                    "rows_to_insert": to_insert_count,
                    "dry_run": dry_run,
                },
                flush=True,
            )

            if dry_run:
                conn.rollback()
                return 0

            cur.execute(
                sql_insert,
                (
                    args.current_window_hours,
                    list(DEFAULT_AVAILABILITIES),
                    snapshot_day,
                    snapshot_day,
                ),
            )
            inserted_count = int(cur.fetchone()[0])
            conn.commit()

            print(
                "[PRICE-HISTORY-SNAPSHOT] done",
                {
                    "snapshot_day": snapshot_day,
                    "inserted_rows": inserted_count,
                },
                flush=True,
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
