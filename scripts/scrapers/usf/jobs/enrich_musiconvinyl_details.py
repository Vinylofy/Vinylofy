from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from scripts.scrapers.musiconvinyl import SHOP_ID, fetch_detail_metadata
from scripts.scrapers.usf.jobs.materialize_musiconvinyl_listing import map_link_to_raw


def load_env() -> None:
    for env_file in (".env.local", ".env"):
        path = Path(env_file)
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def connect() -> psycopg.Connection:
    load_env()
    return psycopg.connect(os.environ["DATABASE_URL"])


def fetch_queue(limit: int) -> list[dict[str, Any]]:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select
                  id,
                  shop_id,
                  source_url,
                  source_product_id,
                  payload
                from public.shop_product_links
                where shop_id = %s
                  and status = 'active'
                  and (
                    last_detail_scraped_at is null
                    or (
                      payload->>'last_detail_ean_status' = 'missing_ean'
                      and last_detail_scraped_at < now() - interval '14 days'
                    )
                    or (
                      payload->>'last_detail_ean_status' = 'rate_limited'
                      and last_detail_scraped_at < now() - interval '2 hours'
                    )
                    or (
                      payload->>'last_detail_ean_status' = 'detail_error'
                      and last_detail_scraped_at < now() - interval '2 hours'
                    )
                  )
                order by
                  case
                    when last_detail_scraped_at is null then 0
                    when payload->>'last_detail_ean_status' in ('rate_limited', 'detail_error') then 1
                    else 2
                  end,
                  last_detail_scraped_at asc nulls first,
                  first_seen_at asc,
                  id asc
                limit %s
                for update skip locked
                """,
                (SHOP_ID, limit),
            )
            rows = cur.fetchall()
            return [
                {
                    "id": str(row[0]),
                    "shop_id": row[1],
                    "source_url": row[2],
                    "source_product_id": row[3],
                    "payload": row[4] or {},
                }
                for row in rows
            ]


def merge_payload(payload: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    merged = dict(payload or {})
    merged.update(updates)
    return merged


def mark_link(link_id: str, payload: dict[str, Any], status: str) -> None:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.shop_product_links
                set
                  payload = %s,
                  last_detail_scraped_at = now()
                where id = %s
                """,
                (Jsonb(merge_payload(payload, {"last_detail_ean_status": status})), link_id),
            )


def insert_raw(link: dict[str, Any], raw: Any) -> str:
    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.raw_shop_scrapes (
                  run_id,
                  shop_id,
                  source_url,
                  source_product_id,
                  title_raw,
                  ean_raw,
                  price_raw,
                  availability_raw,
                  image_url_raw,
                  payload
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                returning id
                """,
                (
                    None,
                    raw.shop_id,
                    raw.source_url,
                    raw.source_product_id,
                    raw.title_raw,
                    raw.ean_raw,
                    raw.price_raw,
                    raw.availability_raw,
                    raw.image_url_raw,
                    Jsonb(raw.payload),
                ),
            )
            raw_id = str(cur.fetchone()[0])

            status = "found_ean" if raw.ean_raw else "missing_ean"
            new_payload = merge_payload(
                link.get("payload") or {},
                {
                    "last_detail_ean_status": status,
                    "last_detail_ean": raw.ean_raw,
                },
            )
            cur.execute(
                """
                update public.shop_product_links
                set
                  payload = %s,
                  last_detail_scraped_at = now()
                where id = %s
                """,
                (Jsonb(new_payload), link["id"]),
            )
            return raw_id


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Enrich Music On Vinyl details in small resumable batches."
    )
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--sleep", type=float, default=3.0)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")

    links = fetch_queue(args.limit)
    print(
        "[MUSICONVINYL-DETAIL-QUEUE]",
        {"queued": len(links), "limit": args.limit, "write": args.write},
        flush=True,
    )

    processed = 0
    found_ean = 0
    missing_ean = 0
    rate_limited = 0
    errors = 0

    for link in links:
        try:
            raw = map_link_to_raw(link)
            processed += 1
            if raw.ean_raw:
                found_ean += 1
            else:
                missing_ean += 1

            if args.write:
                raw_id = insert_raw(link, raw)
            else:
                raw_id = None

            print(
                "[MUSICONVINYL-DETAIL-ENRICHED]",
                {
                    "source_url": link["source_url"],
                    "raw_id": raw_id,
                    "ean": raw.ean_raw,
                    "price": raw.price_raw,
                    "availability": raw.availability_raw,
                    "write": args.write,
                },
                flush=True,
            )

        except Exception as exc:
            reason = str(exc)
            status = "rate_limited" if "429" in reason or "Too Many Requests" in reason else "detail_error"
            if status == "rate_limited":
                rate_limited += 1
            else:
                errors += 1

            print(
                "[MUSICONVINYL-DETAIL-ERROR]",
                {"source_url": link["source_url"], "status": status, "reason": reason},
                flush=True,
            )
            if args.write:
                mark_link(link["id"], link.get("payload") or {}, status)

        time.sleep(args.sleep)

    print(
        "[MUSICONVINYL-DETAIL-SUMMARY]",
        {
            "processed": processed,
            "found_ean": found_ean,
            "missing_ean": missing_ean,
            "rate_limited": rate_limited,
            "errors": errors,
            "write": args.write,
        },
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
