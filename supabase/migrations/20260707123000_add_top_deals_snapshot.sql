create table if not exists public.top_deals_snapshot (
  snapshot_key text not null default 'current',
  rank integer not null,
  product_id uuid not null references public.products(id) on delete cascade,
  ean text,
  artist text not null,
  title text not null,
  format_label text,
  cover_url text,
  lowest_price numeric not null,
  highest_price numeric not null,
  price_difference numeric not null,
  shop_count integer not null,
  lowest_offer jsonb not null,
  highest_offer jsonb not null,
  offers jsonb not null,
  last_seen_at timestamptz,
  refreshed_at timestamptz not null default now(),
  primary key (snapshot_key, rank)
);

create unique index if not exists top_deals_snapshot_current_product_uidx
  on public.top_deals_snapshot (snapshot_key, product_id);

create index if not exists top_deals_snapshot_rank_idx
  on public.top_deals_snapshot (snapshot_key, rank);

create or replace function public.refresh_top_deals_snapshot(
  p_limit integer default 45,
  p_current_window_hours integer default 48
)
returns table(refreshed_at timestamptz, row_count integer)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_refreshed_at timestamptz := now();
  v_row_count integer := 0;
begin
  delete from public.top_deals_snapshot
  where snapshot_key = 'current';

  insert into public.top_deals_snapshot (
    snapshot_key,
    rank,
    product_id,
    ean,
    artist,
    title,
    format_label,
    cover_url,
    lowest_price,
    highest_price,
    price_difference,
    shop_count,
    lowest_offer,
    highest_offer,
    offers,
    last_seen_at,
    refreshed_at
  )
  with eligible as (
    select
      pr.product_id,
      pr.shop_id,
      pr.price::numeric as price,
      pr.product_url,
      pr.last_seen_at,
      coalesce(pr.availability, 'unknown') as availability,
      s.name as shop_name,
      s.domain as shop_domain,
      p.ean,
      p.artist,
      p.title,
      p.format_label,
      p.cover_url,
      row_number() over (
        partition by pr.product_id, pr.shop_id
        order by pr.price::numeric asc, pr.last_seen_at desc
      ) as shop_price_rank
    from public.prices pr
    join public.products p on p.id = pr.product_id
    join public.shops s on s.id = pr.shop_id
    where pr.is_active = true
      and pr.price is not null
      and pr.price::numeric > 0
      and pr.last_seen_at >= now() - (p_current_window_hours * interval '1 hour')
      and coalesce(pr.availability, 'unknown') in ('in_stock', 'unknown')
      and coalesce(nullif(upper(trim(p.format_label)), ''), 'VINYL') not in (
        'CD',
        'POSTER',
        'ACCESSORIES',
        'PHOTOBOOK',
        'BLUERAY',
        'BLURAY'
      )
  ),
  best_per_shop as (
    select *
    from eligible
    where shop_price_rank = 1
  ),
  grouped as (
    select
      product_id,
      ean,
      artist,
      title,
      format_label,
      cover_url,
      min(price) as lowest_price,
      max(price) as highest_price,
      max(price) - min(price) as price_difference,
      count(*)::integer as shop_count,
      max(last_seen_at) as last_seen_at,
      (jsonb_agg(
        jsonb_build_object(
          'name', shop_name,
          'domain', shop_domain,
          'shopId', shop_id,
          'price', price,
          'productUrl', product_url,
          'lastSeenAt', last_seen_at,
          'availability', availability,
          'estimatedShippingPrice', null,
          'estimatedTotalPrice', null,
          'freeShippingApplied', false,
          'shippingNote', null,
          'shippingConfidence', null,
          'freeShippingThresholdPrice', null
        )
        order by price asc, last_seen_at desc
      )->0) as lowest_offer,
      (jsonb_agg(
        jsonb_build_object(
          'name', shop_name,
          'domain', shop_domain,
          'shopId', shop_id,
          'price', price,
          'productUrl', product_url,
          'lastSeenAt', last_seen_at,
          'availability', availability,
          'estimatedShippingPrice', null,
          'estimatedTotalPrice', null,
          'freeShippingApplied', false,
          'shippingNote', null,
          'shippingConfidence', null,
          'freeShippingThresholdPrice', null
        )
        order by price desc, last_seen_at desc
      )->0) as highest_offer,
      jsonb_agg(
        jsonb_build_object(
          'name', shop_name,
          'domain', shop_domain,
          'shopId', shop_id,
          'price', price,
          'productUrl', product_url,
          'lastSeenAt', last_seen_at,
          'availability', availability,
          'estimatedShippingPrice', null,
          'estimatedTotalPrice', null,
          'freeShippingApplied', false,
          'shippingNote', null,
          'shippingConfidence', null,
          'freeShippingThresholdPrice', null
        )
        order by price asc, last_seen_at desc
      ) as offers
    from best_per_shop
    group by product_id, ean, artist, title, format_label, cover_url
    having count(*) >= 2
       and max(price) - min(price) > 0
  ),
  ranked as (
    select
      row_number() over (
        order by price_difference desc, shop_count desc, lowest_price asc, artist asc, title asc
      ) as rank,
      *
    from grouped
  )
  select
    'current',
    rank::integer,
    product_id,
    ean,
    artist,
    title,
    format_label,
    cover_url,
    lowest_price,
    highest_price,
    price_difference,
    shop_count,
    lowest_offer,
    highest_offer,
    offers,
    last_seen_at,
    v_refreshed_at
  from ranked
  where rank <= greatest(1, least(p_limit, 45));

  get diagnostics v_row_count = row_count;

  return query select v_refreshed_at, v_row_count;
end;
$$;

grant select on public.top_deals_snapshot to anon, authenticated;
grant execute on function public.refresh_top_deals_snapshot(integer, integer) to service_role;
