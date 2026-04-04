#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Optional

import psycopg
from psycopg.rows import dict_row


@dataclass
class Config:
    database_url: str
    limit: int
    min_priority: int
    source: str
    force_retry: bool
    only_missing_cover: bool


SQL_INSERT = """
insert into public.cover_lookup_queue (product_id, ean, priority, source, status, next_attempt_at, last_error)
select
  p.id,
  p.ean,
  greatest(coalesce(p.cover_priority, 100), %(min_priority)s),
  %(source)s,
  case when %(force_retry)s then 'retry' else 'queued' end,
  now(),
  null
from public.products p
where p.ean is not null
  and char_length(p.ean) in (8, 12, 13, 14)
  and (%(only_missing_cover)s = false or coalesce(p.cover_storage_path, '') = '')
  and not exists (
    select 1
    from public.cover_lookup_queue q
    where q.product_id = p.id
  )
order by coalesce(p.cover_priority, 100) desc, p.updated_at desc nulls last, p.created_at desc nulls last
limit %(limit)s
"""

SQL_BUMP_EXISTING = """
update public.cover_lookup_queue q
set
  priority = greatest(q.priority, greatest(coalesce(p.cover_priority, 100), %(min_priority)s)),
  status = case
    when %(force_retry)s then 'retry'
    when q.status = 'done' then 'queued'
    else q.status
  end,
  next_attempt_at = now(),
  last_error = null,
  updated_at = now()
from public.products p
where q.product_id = p.id
  and p.ean is not null
  and char_length(p.ean) in (8, 12, 13, 14)
  and (%(only_missing_cover)s = false or coalesce(p.cover_storage_path, '') = '')
  and q.product_id in (
    select p2.id
    from public.products p2
    where p2.ean is not null
      and char_length(p2.ean) in (8, 12, 13, 14)
      and (%(only_missing_cover)s = false or coalesce(p2.cover_storage_path, '') = '')
    order by coalesce(p2.cover_priority, 100) desc, p2.updated_at desc nulls last, p2.created_at desc nulls last
    limit %(limit)s
  )
"""


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Fill or reprioritize the MusicBrainz cover queue.")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--min-priority", type=int, default=100)
    parser.add_argument("--source", default="manual")
    parser.add_argument("--force-retry", action="store_true")
    parser.add_argument("--include-already-covered", action="store_true")
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL ontbreekt. Geef --database-url mee of zet de env var.")

    return Config(
        database_url=args.database_url,
        limit=max(1, args.limit),
        min_priority=args.min_priority,
        source=args.source,
        force_retry=bool(args.force_retry),
        only_missing_cover=not bool(args.include_already_covered),
    )


def main() -> int:
    cfg = parse_args()
    params = {
        "limit": cfg.limit,
        "min_priority": cfg.min_priority,
        "source": cfg.source,
        "force_retry": cfg.force_retry,
        "only_missing_cover": cfg.only_missing_cover,
    }

    with psycopg.connect(cfg.database_url, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL_INSERT, params)
            inserted = cur.rowcount or 0
            cur.execute(SQL_BUMP_EXISTING, params)
            bumped = cur.rowcount or 0
        conn.commit()

    print({
        "inserted": inserted,
        "reprioritized": bumped,
        "limit": cfg.limit,
        "force_retry": cfg.force_retry,
        "only_missing_cover": cfg.only_missing_cover,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
