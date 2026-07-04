from __future__ import annotations

import argparse
from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row

from scripts.scrapers.usf.core.db import get_database_url

SHOP_ID = "musiconvinyl"
SHOP_DOMAIN = "musiconvinyl.com"
CURRENCY = "EUR"


@dataclass
class SyncStats:
    listing_links: int = 0
    known_ean_links: int = 0
    matched_existing_prices: int = 0
    changed_rows: int = 0
    history_rows: int = 0
    prices_updated: int = 0


BASE_CTE = """
with mov_shop as (
  select id
  from public.shops
  where domain = %(shop_domain)s
  limit 1
),
listing_prices_raw as (
  select
    spl.source_url,
    spl.source_product_id,
    replace(spl.payload->>'chosen_price', ',', '.')::numeric(10,2) as new_price,
    case lower(coalesce(spl.payload->>'availability', 'unknown'))
      when 'in_stock' then 'in_stock'
      when 'out_of_stock' then 'out_of_stock'
      when 'preorder' then 'preorder'
      when 'pre-order' then 'preorder'
      else 'unknown'
    end as parsed_availability
  from public.shop_product_links spl
  where spl.shop_id = %(shop_id)s
    and spl.status = 'active'
    and spl.source_url is not null
    and spl.source_product_id is not null
    and replace(coalesce(spl.payload->>'chosen_price', ''), ',', '.') ~ '^[0-9]+(\\.[0-9]{1,2})?$'
),
listing_prices as (
  select distinct on (source_product_id)
    source_url,
    source_product_id,
    new_price,
    parsed_availability
  from listing_prices_raw
  order by source_product_id, source_url
),
known_eans as (
  select
    rss.source_product_id,
    max(regexp_replace(coalesce(rss.ean_raw, ''), '\\D', '', 'g')) as ean
  from public.raw_shop_scrapes rss
  where rss.shop_id = %(shop_id)s
    and rss.source_product_id is not null
    and regexp_replace(coalesce(rss.ean_raw, ''), '\\D', '', 'g') ~ '^\\d{12,13}$'
  group by rss.source_product_id
),
listing_with_ean as (
  select
    lp.source_url,
    lp.source_product_id,
    lp.new_price,
    lp.parsed_availability,
    ke.ean
  from listing_prices lp
  join known_eans ke
    on ke.source_product_id = lp.source_product_id
),
candidates as (
  select
    pr.product_id,
    pr.shop_id,
    lwe.source_url,
    lwe.source_product_id,
    lwe.ean,
    pr.price::numeric(10,2) as old_price,
    lwe.new_price,
    coalesce(nullif(pr.availability, ''), 'unknown') as old_availability,
    case
      when lwe.parsed_availability = 'unknown'
        then coalesce(nullif(pr.availability, ''), 'unknown')
      else lwe.parsed_availability
    end as new_availability
  from listing_with_ean lwe
  join public.products prod
    on prod.ean = lwe.ean
    or prod.gtin_normalized = lwe.ean
  join mov_shop ms
    on true
  join public.prices pr
    on pr.product_id = prod.id
   and pr.shop_id = ms.id
)
"""


def sync_listing_prices(*, write: bool) -> SyncStats:
    params = {
        "shop_id": SHOP_ID,
        "shop_domain": SHOP_DOMAIN,
        "currency": CURRENCY,
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
                  ) as changed_rows;
                """,
                params,
            )
            row = cur.fetchone() or {}
            stats = SyncStats(
                listing_links=int(row.get("listing_links") or 0),
                known_ean_links=int(row.get("known_ean_links") or 0),
                matched_existing_prices=int(row.get("matched_existing_prices") or 0),
                changed_rows=int(row.get("changed_rows") or 0),
            )

            print("[MUSICONVINYL-FAST-SYNC-PRECHECK]", vars(stats), flush=True)

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
            stats.prices_updated = int((cur.fetchone() or {}).get("prices_updated") or 0)

            conn.commit()
            return stats


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sync Music On Vinyl listing prices to existing live prices rows."
    )
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    stats = sync_listing_prices(write=args.write)
    print("[MUSICONVINYL-FAST-SYNC-DONE]", vars(stats) | {"write": args.write}, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
