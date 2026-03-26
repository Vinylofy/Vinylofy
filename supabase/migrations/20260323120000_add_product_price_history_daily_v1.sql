begin;

create index if not exists price_history_product_id_captured_at_idx
  on public.price_history (product_id, captured_at desc);

create index if not exists price_history_product_id_shop_id_captured_at_idx
  on public.price_history (product_id, shop_id, captured_at desc);

create or replace view public.product_price_history_daily_v1 as
with normalized as (
  select
    ph.product_id,
    ph.shop_id,
    (ph.captured_at at time zone 'Europe/Amsterdam')::date as day,
    ph.price,
    ph.availability,
    ph.captured_at
  from public.price_history ph
)
select
  product_id,
  day,
  min(price) filter (where availability = 'in_stock') as min_instock_price,
  count(distinct shop_id) filter (where availability = 'in_stock') as instock_shop_count,
  max(captured_at) as last_captured_at
from normalized
group by product_id, day;

comment on view public.product_price_history_daily_v1 is
  'Dagelijkse geaggregeerde prijshistorie voor Vinylofy-producten op basis van de bestaande price_history tabel. Gebruikt laagste in-stock dagprijs per product.';

commit;
