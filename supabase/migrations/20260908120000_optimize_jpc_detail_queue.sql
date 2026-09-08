-- Keep the JPC EAN detail queue bounded as the listing registry grows.
-- The predicates intentionally mirror detail_jpc.py exactly.

create index if not exists idx_jpc_detail_queue
on public.shop_product_links (
    shop_id,
    (
        case
            when payload->>'detail_priority' = 'high' then 0
            else 1
        end
    ),
    last_seen_at desc,
    first_seen_at
)
where status = 'active'
  and last_detail_scraped_at is null
  and payload->>'listing_availability' = 'in_stock'
  and nullif(payload->>'last_successful_ean', '') is null;

create index if not exists idx_raw_shop_scrapes_shop_product
on public.raw_shop_scrapes (shop_id, source_product_id);
