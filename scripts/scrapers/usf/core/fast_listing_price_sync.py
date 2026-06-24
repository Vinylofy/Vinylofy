from __future__ import annotations

from dataclasses import dataclass

import psycopg
from psycopg.rows import dict_row


@dataclass
class FastListingPriceSyncStats:
    matched_existing_prices: int = 0
    changed_rows: int = 0
    history_rows: int = 0
    prices_updated: int = 0


BASE_CTE = """
with rov_shop as (
    select id
    from public.shops
    where domain = %(shop_domain)s
    limit 1
),
listing_prices_raw as (
    select
        trim(trailing '/' from split_part(source_url, '?', 1)) as source_url_norm,
        replace(payload->>'price', ',', '.')::numeric(10,2) as new_price,
        case lower(coalesce(payload->>'availability', 'unknown'))
            when 'in_stock' then 'in_stock'
            when 'out_of_stock' then 'out_of_stock'
            when 'preorder' then 'preorder'
            when 'pre-order' then 'preorder'
            else 'unknown'
        end as parsed_availability
    from public.shop_product_links
    where shop_id = %(shop_registry_id)s
      and source_url is not null
      and replace(coalesce(payload->>'price', ''), ',', '.') ~ '^[0-9]+(\.[0-9]{1,2})?$'
),
listing_prices as (
    select distinct on (source_url_norm)
        source_url_norm,
        new_price,
        parsed_availability
    from listing_prices_raw
    where source_url_norm <> ''
    order by source_url_norm
),
candidates as (
    select
        pr.product_id,
        pr.shop_id,
        lp.source_url_norm,
        pr.price::numeric(10,2) as old_price,
        lp.new_price,
        coalesce(nullif(pr.availability, ''), 'unknown') as old_availability,
        case
            when lp.parsed_availability = 'unknown'
                then coalesce(nullif(pr.availability, ''), 'unknown')
            else lp.parsed_availability
        end as new_availability
    from public.prices pr
    join rov_shop rs
      on rs.id = pr.shop_id
    join listing_prices lp
      on trim(trailing '/' from split_part(pr.product_url, '?', 1)) = lp.source_url_norm
)
"""


def bulk_update_prices_from_link_registry(
    conn: psycopg.Connection,
    *,
    shop_registry_id: str,
    shop_domain: str,
    write: bool,
    currency: str = "EUR",
) -> FastListingPriceSyncStats:
    params = {
        "shop_registry_id": shop_registry_id,
        "shop_domain": shop_domain,
        "currency": currency,
    }

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            BASE_CTE
            + """
            select
                count(*) as matched_existing_prices,
                count(*) filter (
                    where old_price is distinct from new_price
                       or old_availability is distinct from new_availability
                ) as changed_rows
            from candidates;
            """,
            params,
        )
        precheck = cur.fetchone() or {}

        stats = FastListingPriceSyncStats(
            matched_existing_prices=int(precheck.get("matched_existing_prices") or 0),
            changed_rows=int(precheck.get("changed_rows") or 0),
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
            select count(*) as history_rows from inserted_history;
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
                    product_url = c.source_url_norm,
                    availability = c.new_availability,
                    last_seen_at = now(),
                    is_active = true,
                    updated_at = now()
                from candidates c
                where pr.product_id = c.product_id
                  and pr.shop_id = c.shop_id
                returning 1
            )
            select count(*) as prices_updated from updated;
            """,
            params,
        )
        stats.prices_updated = int((cur.fetchone() or {}).get("prices_updated") or 0)

    conn.commit()
    return stats
