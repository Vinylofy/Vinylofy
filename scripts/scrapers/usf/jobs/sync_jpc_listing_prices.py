#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row

from scripts.scrapers.usf.core.db import get_database_url

SHOP_ID = "jpc"
SHOP_DOMAIN = "jpc.de"
CURRENCY = "EUR"


@dataclass
class JpcListingSyncStats:
    listing_links: int = 0
    known_ean_links: int = 0
    matched_existing_prices: int = 0
    changed_rows: int = 0
    max_rows_per_listing: int = 0
    history_rows: int = 0
    prices_updated: int = 0
    links_marked_price_refreshed: int = 0


BASE_CTE = r"""
with jpc_shop as (
  select id
  from public.shops
  where domain = %(shop_domain)s
  limit 1
),
listing_prices_raw as (
  select
    spl.id as link_id,
    spl.source_url,
    spl.source_product_id,
    spl.last_seen_at,
    replace(spl.payload->>'listing_price_raw', ',', '.')::numeric(10,2) as new_price,
    case lower(coalesce(spl.payload->>'listing_availability', 'unknown'))
      when 'in_stock' then 'in_stock'
      when 'out_of_stock' then 'out_of_stock'
      when 'preorder' then 'preorder'
      when 'pre-order' then 'preorder'
      else 'unknown'
    end as parsed_availability,
    case
      when regexp_replace(coalesce(spl.payload->>'last_successful_ean', ''), '\D', '', 'g')
        ~ '^(\d{8}|\d{12}|\d{13}|\d{14})$'
      then regexp_replace(coalesce(spl.payload->>'last_successful_ean', ''), '\D', '', 'g')
      else null
    end as payload_ean
  from public.shop_product_links spl
  where spl.shop_id = %(shop_id)s
    and spl.status = 'active'
    and spl.source_url is not null
    and spl.source_product_id is not null
    and replace(coalesce(spl.payload->>'listing_price_raw', ''), ',', '.')
      ~ '^[0-9]+(\.[0-9]{1,2})?$'
  order by spl.last_seen_at desc nulls last, spl.id asc
  limit %(limit)s
),
listing_prices as (
  select distinct on (source_product_id)
    link_id,
    source_url,
    source_product_id,
    new_price,
    parsed_availability,
    payload_ean
  from listing_prices_raw
  where source_product_id <> ''
  order by source_product_id, last_seen_at desc nulls last, link_id asc
),
known_raw_eans as (
  select distinct on (rss.source_product_id)
    rss.source_product_id,
    regexp_replace(coalesce(rss.ean_raw, ''), '\D', '', 'g') as raw_ean
  from public.raw_shop_scrapes rss
  where rss.shop_id = %(shop_id)s
    and rss.source_product_id is not null
    and regexp_replace(coalesce(rss.ean_raw, ''), '\D', '', 'g')
      ~ '^(\d{8}|\d{12}|\d{13}|\d{14})$'
  order by rss.source_product_id, rss.scraped_at desc nulls last, rss.id desc
),
listing_with_ean as (
  select
    lp.link_id,
    lp.source_url,
    lp.source_product_id,
    lp.new_price,
    lp.parsed_availability,
    coalesce(lp.payload_ean, kre.raw_ean) as ean
  from listing_prices lp
  left join known_raw_eans kre
    on kre.source_product_id = lp.source_product_id
  where coalesce(lp.payload_ean, kre.raw_ean) is not null
),
identifier_candidates as (
  select distinct
    lwe.link_id,
    lwe.source_url,
    lwe.source_product_id,
    lwe.new_price,
    lwe.parsed_availability,
    lwe.ean,
    candidate.identifier
  from listing_with_ean lwe
  cross join lateral unnest(array_remove(array[
    lwe.ean,
    lpad(lwe.ean, 14, '0'),
    case
      when length(lwe.ean) = 14 and left(lwe.ean, 1) = '0'
      then substring(lwe.ean from 2)
      else null
    end
  ], null::text)) as candidate(identifier)
),
candidates as (
  select
    pr.product_id,
    pr.shop_id,
    ic.link_id,
    ic.source_url,
    ic.source_product_id,
    ic.ean,
    pr.price::numeric(10,2) as old_price,
    ic.new_price,
    coalesce(nullif(pr.availability, ''), 'unknown') as old_availability,
    case
      when ic.parsed_availability = 'unknown'
        then coalesce(nullif(pr.availability, ''), 'unknown')
      else ic.parsed_availability
    end as new_availability
  from identifier_candidates ic
  join public.products prod
    on prod.ean = ic.identifier
    or prod.gtin_normalized = ic.identifier
  join jpc_shop js
    on true
  join public.prices pr
    on pr.product_id = prod.id
   and pr.shop_id = js.id
)
"""


def sync_listing_prices(
    *,
    limit: int,
    write: bool,
    max_matches_per_listing: int,
) -> JpcListingSyncStats:
    if limit < 1:
        raise ValueError("limit moet minimaal 1 zijn")
    if max_matches_per_listing < 1:
        raise ValueError("max_matches_per_listing moet minimaal 1 zijn")

    params = {
        "shop_id": SHOP_ID,
        "shop_domain": SHOP_DOMAIN,
        "currency": CURRENCY,
        "limit": limit,
    }

    with psycopg.connect(get_database_url(), prepare_threshold=None) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                BASE_CTE
                + """
                select
                  (select count(*) from listing_prices) as listing_links,
                  (select count(*) from listing_with_ean) as known_ean_links,
                  (select count(*) from candidates) as matched_existing_prices,
                  (
                    select count(*)
                    from candidates
                    where old_price is distinct from new_price
                       or old_availability is distinct from new_availability
                  ) as changed_rows,
                  coalesce((
                    select max(rows_per_listing)
                    from (
                      select source_product_id, count(*) as rows_per_listing
                      from candidates
                      group by source_product_id
                    ) fanout
                  ), 0) as max_rows_per_listing;
                """,
                params,
            )
            precheck = cur.fetchone() or {}

            stats = JpcListingSyncStats(
                listing_links=int(precheck.get("listing_links") or 0),
                known_ean_links=int(precheck.get("known_ean_links") or 0),
                matched_existing_prices=int(
                    precheck.get("matched_existing_prices") or 0
                ),
                changed_rows=int(precheck.get("changed_rows") or 0),
                max_rows_per_listing=int(
                    precheck.get("max_rows_per_listing") or 0
                ),
            )

            if stats.max_rows_per_listing > max_matches_per_listing:
                conn.rollback()
                raise RuntimeError(
                    "Unsafe JPC listing price sync fanout: "
                    f"max_rows_per_listing={stats.max_rows_per_listing}, "
                    f"limit={max_matches_per_listing}. Aborting before writes."
                )

            if not write:
                conn.rollback()
                return stats

            cur.execute(
                BASE_CTE
                + """
                , changed as (
                  select *
                  from candidates
                  where old_price is distinct from new_price
                     or old_availability is distinct from new_availability
                ),
                inserted_history as (
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
                    c.product_id,
                    c.shop_id,
                    c.new_price,
                    %(currency)s,
                    c.new_availability,
                    now(),
                    now()
                  from changed c
                  where not exists (
                    select 1
                    from public.price_history ph
                    where ph.product_id = c.product_id
                      and ph.shop_id = c.shop_id
                      and ph.captured_at::date = current_date
                      and ph.price::numeric(10,2) = c.new_price
                      and coalesce(ph.availability, 'unknown') = c.new_availability
                  )
                  returning 1
                )
                select count(*) as history_rows
                from inserted_history;
                """,
                params,
            )
            stats.history_rows = int((cur.fetchone() or {}).get("history_rows") or 0)

            cur.execute(
                BASE_CTE
                + """
                , updated as (
                  update public.prices pr
                  set
                    price = c.new_price,
                    currency = %(currency)s,
                    product_url = c.source_url,
                    availability = c.new_availability,
                    last_seen_at = now(),
                    is_active = true,
                    updated_at = now()
                  from candidates c
                  where pr.product_id = c.product_id
                    and pr.shop_id = c.shop_id
                  returning 1
                )
                select count(*) as prices_updated
                from updated;
                """,
                params,
            )
            stats.prices_updated = int(
                (cur.fetchone() or {}).get("prices_updated") or 0
            )

            cur.execute(
                BASE_CTE
                + """
                , touched as (
                  update public.shop_product_links spl
                  set last_price_refreshed_at = now()
                  from listing_with_ean lwe
                  where spl.id = lwe.link_id
                  returning 1
                )
                select count(*) as links_marked_price_refreshed
                from touched;
                """,
                params,
            )
            stats.links_marked_price_refreshed = int(
                (cur.fetchone() or {}).get("links_marked_price_refreshed") or 0
            )

            conn.commit()
            return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Sync JPC listing prices from shop_product_links to existing "
            "validated public.prices rows. Nieuwe JPC offers worden niet "
            "via price-only gepubliceerd."
        )
    )
    parser.add_argument("--limit", type=int, default=200000)
    parser.add_argument("--max-matches-per-listing", type=int, default=3)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")
    if args.max_matches_per_listing < 1:
        raise SystemExit("[ERROR] --max-matches-per-listing moet minimaal 1 zijn.")

    stats = sync_listing_prices(
        limit=args.limit,
        write=args.write,
        max_matches_per_listing=args.max_matches_per_listing,
    )

    print(
        "[JPC-PRICE-SYNC-DONE]",
        vars(stats) | {"databasewrites": bool(args.write)},
        flush=True,
    )

    if args.write and stats.listing_links == 0:
        raise SystemExit(
            "[JPC-PRICE-SYNC-FAIL] Geen JPC listinglinks met prijs gevonden."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
