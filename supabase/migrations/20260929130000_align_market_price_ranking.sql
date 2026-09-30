begin;

-- Match the existing public priceForDisplay rule for At The Movies. All
-- comparisons remain article prices, without shipping or currency conversion.
create or replace view public.market_product_best_prices_v1
with (security_invoker = true) as
select m.country_code as market_code, p.id as product_id,
  min(case when s.domain = 'atthemoviesshop.com' then round(pr.price * 1.21, 2)
    else pr.price end) filter (where pr.availability = 'in_stock') as lowest_fresh_price,
  count(distinct pr.shop_id) as total_active_shop_count,
  count(distinct pr.shop_id) filter (where pr.availability = 'in_stock') as fresh_instock_shop_count,
  max(pr.last_seen_at) as best_price_last_seen_at
from public.markets m
join public.shop_shipping_destinations d
  on d.country_code = m.country_code and d.status = 'confirmed'
join public.shops s on s.id = d.shop_id and s.is_active
join public.prices pr on pr.shop_id = s.id and pr.is_active
  and pr.price > 0 and pr.currency = m.currency
  and pr.availability in ('in_stock','unknown')
  and pr.last_seen_at >= now() - interval '48 hours'
join public.products p on p.id = pr.product_id and p.gtin_normalized is not null
  and coalesce(upper(btrim(p.format_label)), '') not in
    ('CD','POSTER','ACCESSORIES','PHOTOBOOK','BLUERAY','BLURAY')
where m.status = 'active' and m.released_at is not null
group by m.country_code, p.id;
grant select on public.market_product_best_prices_v1 to anon, authenticated;

create or replace function public.market_top_deals_v1(p_country_code text, p_limit integer default 45)
returns table (
  product_id uuid, ean text, artist text, title text, format_label text,
  cover_url text, offers jsonb, last_seen_at timestamptz
)
language sql stable security invoker set search_path = public as $$
  with eligible as (
    select pr.product_id, pr.shop_id, pr.price, pr.product_url,
      pr.currency, pr.last_seen_at, pr.availability,
      s.name as shop_name, s.domain as shop_domain,
      case when s.domain = 'atthemoviesshop.com' then round(pr.price * 1.21, 2)
        else pr.price end as display_price,
      row_number() over (partition by pr.product_id, pr.shop_id
        order by case when s.domain = 'atthemoviesshop.com'
          then round(pr.price * 1.21, 2) else pr.price end asc,
          pr.last_seen_at desc) as shop_rank
    from public.markets m
    join public.shop_shipping_destinations d
      on d.country_code = m.country_code and d.status = 'confirmed'
    join public.shops s on s.id = d.shop_id and s.is_active
    join public.prices pr on pr.shop_id = s.id and pr.is_active
      and pr.price > 0 and pr.currency = m.currency
      and pr.availability in ('in_stock','unknown')
      and pr.last_seen_at >= now() - interval '48 hours'
    join public.products p on p.id = pr.product_id and p.gtin_normalized is not null
      and coalesce(upper(btrim(p.format_label)), '') not in
        ('CD','POSTER','ACCESSORIES','PHOTOBOOK','BLUERAY','BLURAY')
    where m.country_code = p_country_code and m.status = 'active'
      and m.released_at is not null
  ), grouped as (
    select e.product_id,
      jsonb_agg(jsonb_build_object(
        'shopId',e.shop_id,'name',e.shop_name,'domain',e.shop_domain,
        'price',e.price,'currency',e.currency,'productUrl',e.product_url,
        'lastSeenAt',e.last_seen_at,'availability',e.availability
      ) order by e.display_price asc) as offers,
      max(e.display_price)-min(e.display_price) as difference,
      max(e.last_seen_at) as last_seen_at, count(*) as shop_count
    from eligible e where e.shop_rank = 1
    group by e.product_id
    having count(*) >= 2 and max(e.display_price) > min(e.display_price)
  )
  select p.id, p.ean, p.artist, p.title, p.format_label, p.cover_url,
    g.offers, g.last_seen_at
  from grouped g join public.products p on p.id = g.product_id
  order by g.difference desc, g.shop_count desc, p.artist, p.title
  limit greatest(1, least(p_limit, 45));
$$;
revoke all on function public.market_top_deals_v1(text, integer) from public;
grant execute on function public.market_top_deals_v1(text, integer) to anon, authenticated;

commit;
