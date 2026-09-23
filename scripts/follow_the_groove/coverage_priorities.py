#!/usr/bin/env python3
"""Read-only shortlist for bounded Follow the Groove source refreshes.

The proven-neighbor count is a prioritization proxy, not the rendered card count.
Every selected source still needs the collector dry-run and UI verification.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import psycopg

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None

from scripts.follow_the_groove.generic_collector import MAX_LASTFM_LIMIT, MAX_SOURCES


PRIORITIES_QUERY = """
with product_counts as (
  select artist_id, count(distinct product_id)::integer as product_count
  from product_artists
  group by artist_id
), prior as (
  select counters->>'source_artist_mbid' as mbid,
         max((counters->'input_limits'->>'lastfm_limit')::integer) as lastfm_limit
  from follow_the_groove_collection_runs
  where collector = 'GENERIC_BOUNDED_V1'
    and status = 'succeeded'
    and counters->'input_limits'->>'lastfm_limit' ~ '^[0-9]+$'
  group by 1
), related as (
  select e.artist_low_id as source_id, e.artist_high_id as target_id
  from artist_edges e
  where exists (
    select 1 from artist_relation_evidence ev
    where ev.edge_id = e.id and ev.classification = 'allowed'
  )
  union
  select e.artist_high_id, e.artist_low_id
  from artist_edges e
  where exists (
    select 1 from artist_relation_evidence ev
    where ev.edge_id = e.id and ev.classification = 'allowed'
  )
  union
  select source_artist_id, target_artist_id
  from artist_similarity
  where resolution_status = 'resolved'
), neighbor_counts as (
  select r.source_id,
         count(*) filter (where s.status = 'proven_output')::integer as proven_neighbors,
         count(*) filter (
           where s.status = 'proven_output'
             and target.entity_type = 'group'
             and target_products.product_count > 0
         )::integer as product_group_neighbors,
         count(*)::integer as related_neighbors
  from related r
  join artists target on target.id = r.target_id
  left join product_counts target_products on target_products.artist_id = r.target_id
  left join artist_output_status s on s.artist_id = r.target_id
  group by r.source_id
)
select a.musicbrainz_artist_mbid::text, a.display_name, pc.product_count,
       coalesce(n.proven_neighbors, 0), coalesce(n.product_group_neighbors, 0),
       coalesce(n.related_neighbors, 0),
       coalesce(p.lastfm_limit, 0)
from artists a
join product_counts pc on pc.artist_id = a.id
left join neighbor_counts n on n.source_id = a.id
left join prior p on p.mbid = a.musicbrainz_artist_mbid::text
where pc.product_count >= %(min_products)s
  and coalesce(n.proven_neighbors, 0) < 5
  and coalesce(p.lastfm_limit, 0) < %(max_lastfm_limit)s
order by coalesce(n.product_group_neighbors, 0),
         coalesce(n.proven_neighbors, 0), pc.product_count desc,
         lower(a.display_name), a.musicbrainz_artist_mbid
limit %(limit)s
"""


def select_priorities(conn: Any, *, min_products: int, limit: int) -> list[dict[str, Any]]:
    if min_products < 1:
        raise ValueError("min-products must be positive")
    if not 1 <= limit <= MAX_SOURCES:
        raise ValueError(f"limit must be 1..{MAX_SOURCES}")
    rows = conn.execute(
        PRIORITIES_QUERY,
        {"min_products": min_products, "max_lastfm_limit": MAX_LASTFM_LIMIT, "limit": limit},
    ).fetchall()
    return [
        {
            "artist_mbid": mbid,
            "name": name,
            "product_links": product_count,
            "proven_neighbors": proven_count,
            "product_group_neighbors": product_group_count,
            "related_neighbors": related_count,
            "prior_lastfm_limit": lastfm_limit,
            "proxy_deficit": 5 - product_group_count,
        }
        for mbid, name, product_count, proven_count, product_group_count, related_count, lastfm_limit in rows
    ]


def run(*, min_products: int, limit: int, database_url: str | None = None) -> dict[str, Any]:
    if min_products < 1 or not 1 <= limit <= MAX_SOURCES:
        raise ValueError("invalid priority selection bounds")
    if load_dotenv:
        load_dotenv(".env.local", override=False)
    url = database_url or os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is required")
    with psycopg.connect(url, autocommit=False) as conn:
        conn.execute("begin isolation level repeatable read read only")
        artists = select_priorities(conn, min_products=min_products, limit=limit)
        conn.rollback()
    return {
        "mode": "read-only",
        "heuristic_only": True,
        "database_writes": 0,
        "min_products": min_products,
        "limit": limit,
        "artists": artists,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-products", type=int, default=10)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = run(min_products=args.min_products, limit=args.limit)
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
