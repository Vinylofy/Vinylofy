-- Counts visible current offers by product for every shop in one pass.
-- The product page accepts both in_stock and unknown; its base-price order
-- prefers in_stock when available. At The Movies prices are stored net of VAT.
create or replace function public.shop_highlights_v1()
returns table (shop_id uuid, lowest_price_count bigint, only_here_count bigint)
language sql
stable
security invoker
set search_path = public
as $$
  with current_offers as (
    select
      pr.product_id,
      pr.shop_id,
      pr.availability,
      case
        when s.domain = 'atthemoviesshop.com' then round(pr.price * 1.21, 2)
        else pr.price
      end as display_price
    from public.prices pr
    join public.products p on p.id = pr.product_id
    join public.shops s on s.id = pr.shop_id
    where pr.is_active = true
      and pr.availability in ('in_stock', 'unknown')
      and pr.last_seen_at >= now() - interval '48 hours'
      and pr.price > 0
      and pr.currency = 'EUR'
      and p.gtin_normalized is not null
      and coalesce(upper(btrim(p.format_label)), '') not in (
        'CD', 'POSTER', 'ACCESSORIES', 'PHOTOBOOK', 'BLUERAY', 'BLURAY'
      )
  ), product_prices as (
    select
      product_id,
      shop_id,
      display_price,
      count(*) over (partition by product_id) as offer_count,
      min(display_price) filter (where availability = 'in_stock')
        over (partition by product_id) as lowest_in_stock_price,
      min(display_price) over (partition by product_id) as lowest_any_price
    from current_offers
  )
  select
    shop_id,
    count(*) filter (
      where display_price = coalesce(lowest_in_stock_price, lowest_any_price)
    ) as lowest_price_count,
    count(*) filter (where offer_count = 1) as only_here_count
  from product_prices
  group by shop_id;
$$;

revoke all on function public.shop_highlights_v1() from public;
grant execute on function public.shop_highlights_v1() to anon, authenticated, service_role;
