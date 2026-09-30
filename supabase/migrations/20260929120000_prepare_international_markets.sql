begin;

alter table public.shops drop constraint if exists shops_country_iso2_check;
alter table public.shops add constraint shops_country_iso2_check
  check (btrim(country) ~ '^[A-Z]{2}$');

create table if not exists public.markets (
  country_code text primary key check (country_code ~ '^[A-Z]{2}$'),
  name text not null,
  currency text not null check (currency ~ '^[A-Z]{3}$'),
  status text not null default 'prepared' check (status in ('prepared', 'active')),
  released_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint markets_release_check check ((status = 'active') = (released_at is not null))
);

insert into public.markets (country_code, name, currency, status, released_at)
values
  ('NL', 'Nederland', 'EUR', 'active', now()),
  ('BE', 'België', 'EUR', 'prepared', null),
  ('GB', 'Verenigd Koninkrijk', 'GBP', 'prepared', null),
  ('US', 'Verenigde Staten', 'USD', 'prepared', null)
on conflict (country_code) do nothing;

create table if not exists public.shop_shipping_destinations (
  shop_id uuid not null references public.shops(id) on delete cascade,
  country_code text not null check (country_code ~ '^[A-Z]{2}$'),
  status text not null default 'unknown' check (status in ('confirmed', 'unknown', 'not_supported')),
  source_url text,
  verified_at timestamptz,
  note text,
  updated_at timestamptz not null default now(),
  primary key (shop_id, country_code),
  constraint shipping_destination_evidence_check check (
    status = 'unknown' or
    (source_url ~ '^https://[^ ]+$' and verified_at is not null)
  )
);

create index if not exists shop_shipping_destinations_country_status_idx
  on public.shop_shipping_destinations (country_code, status, shop_id);

-- Explicit unknown rows make the initial audit complete. Missing rows remain unknown too.
insert into public.shop_shipping_destinations (shop_id, country_code)
select s.id, m.country_code
from public.shops s cross join public.markets m
where s.is_active and m.country_code in ('NL', 'BE', 'GB', 'US')
on conflict (shop_id, country_code) do nothing;

-- Existing rule provenance and fresh checks of official shop pages. A shop's
-- home country is deliberately irrelevant to its shipping destinations.
with evidence(domain, country_code, source_url, checked_at, note) as (
  values
    ('3345.nl','NL','https://3345.nl/pages/shipping_2022','2026-09-29'::timestamptz,null),
    ('3345.nl','BE','https://3345.nl/pages/shipping_2022','2026-09-29'::timestamptz,null),
    ('atthemoviesshop.com','NL','https://atthemoviesshop.com/pages/shipping','2026-09-29'::timestamptz,null),
    ('atthemoviesshop.com','BE','https://atthemoviesshop.com/pages/shipping','2026-09-29'::timestamptz,null),
    ('atthemoviesshop.com','GB','https://atthemoviesshop.com/pages/shipping','2026-09-29'::timestamptz,'Excludes British Overseas Territories and Channel Islands.'),
    ('atthemoviesshop.com','US','https://atthemoviesshop.com/pages/shipping','2026-09-29'::timestamptz,'Some US territories are excluded.'),
    ('blackvinyl.nl','NL','https://www.blackvinyl.nl/verzenden/','2026-09-29'::timestamptz,null),
    ('blackvinyl.nl','BE','https://www.blackvinyl.nl/verzenden/','2026-09-29'::timestamptz,'The page confirms other EU countries.'),
    ('bobsvinyl.nl','NL','https://bobsvinyl.nl/policies/shipping-policy','2026-09-29'::timestamptz,null),
    ('bobsvinyl.nl','BE','https://bobsvinyl.nl/policies/shipping-policy','2026-09-29'::timestamptz,null),
    ('bol.com','NL','https://www.bol.com/nl/nl/klantenservice/info/blt59015a4311d01e18/bezorgen-en-ontvangen','2026-09-29'::timestamptz,'Marketplace delivery can vary per seller and item.'),
    ('cdhal.nl','NL','https://www.cdhal.nl/leverings-informatie','2026-09-29'::timestamptz,null),
    ('cdhal.nl','BE','https://www.cdhal.nl/leverings-informatie','2026-09-29'::timestamptz,null),
    ('colouredvinyl.nl','NL','https://www.colouredvinyl.nl/levertijd-verzendkosten/','2026-09-29'::timestamptz,null),
    ('colouredvinyl.nl','BE','https://www.colouredvinyl.nl/levertijd-verzendkosten/','2026-09-29'::timestamptz,null),
    ('dgmoutlet.nl','NL','https://www.dgmoutlet.nl/bestellen-bezorgen-afhalen/','2026-09-29'::timestamptz,null),
    ('dgmoutlet.nl','BE','https://www.dgmoutlet.nl/bestellen-bezorgen-afhalen/','2026-09-29'::timestamptz,null),
    ('eustore.everythingjazz.com','NL','https://eustore.everythingjazz.com/policies/shipping-policy','2026-09-29'::timestamptz,null),
    ('eustore.everythingjazz.com','BE','https://eustore.everythingjazz.com/policies/shipping-policy','2026-09-29'::timestamptz,null),
    ('eustore.everythingjazz.com','GB','https://eustore.everythingjazz.com/policies/shipping-policy','2026-09-29'::timestamptz,null),
    ('eustore.everythingjazz.com','US','https://eustore.everythingjazz.com/policies/shipping-policy','2026-09-29'::timestamptz,'Shop warns of possible duties and processing fees.'),
    ('fiftiesstore.nl','NL','https://www.fiftiesstore.nl/verzending','2026-09-29'::timestamptz,null),
    ('fiftiesstore.nl','BE','https://www.fiftiesstore.nl/verzending','2026-09-29'::timestamptz,null),
    ('getbackmusic.nl','NL','https://www.getbackmusic.nl/policies/shipping-policy','2026-09-29'::timestamptz,null),
    ('groovespin.nl','NL','https://www.groovespin.nl/algemene-voorwaarden','2026-09-29'::timestamptz,'Country appears in official delivery-country table; shipping price is not asserted.'),
    ('groovespin.nl','BE','https://www.groovespin.nl/algemene-voorwaarden','2026-09-29'::timestamptz,'Country appears in official delivery-country table; shipping price is not asserted.'),
    ('groovespin.nl','GB','https://www.groovespin.nl/algemene-voorwaarden','2026-09-29'::timestamptz,'Country appears in official delivery-country table; shipping price is not asserted.'),
    ('groovespin.nl','US','https://www.groovespin.nl/algemene-voorwaarden','2026-09-29'::timestamptz,'Country appears in official delivery-country table; shipping price is not asserted.'),
    ('hhv.de','NL','https://www.hhv.de/hilfe/versand','2026-09-29'::timestamptz,null),
    ('hhv.de','BE','https://www.hhv.de/hilfe/versand','2026-09-29'::timestamptz,null),
    ('hhv.de','GB','https://www.hhv.de/hilfe/versand','2026-09-29'::timestamptz,null),
    ('hhv.de','US','https://www.hhv.de/hilfe/versand','2026-09-29'::timestamptz,null),
    ('imusic.nl','NL','https://imusic.nl/page/faq','2026-09-29'::timestamptz,null),
    ('imusic.nl','BE','https://imusic.nl/page/faq','2026-09-29'::timestamptz,null),
    ('jpc.de','NL','https://www.jpc.de/jpcng/home/static/-/page/porto.html','2026-09-29'::timestamptz,null),
    ('jpc.de','BE','https://www.jpc.de/jpcng/home/static/-/page/porto.html','2026-09-29'::timestamptz,null),
    ('jpc.de','GB','https://www.jpc.de/jpcng/home/static/-/page/porto.html','2026-09-29'::timestamptz,null),
    ('jpc.de','US','https://www.jpc.de/jpcng/home/static/-/page/porto.html','2026-09-29'::timestamptz,null),
    ('musiconvinyl.com','NL','https://www.musiconvinyl.com/pages/shipping','2026-09-29'::timestamptz,null),
    ('musiconvinyl.com','BE','https://www.musiconvinyl.com/pages/shipping','2026-09-29'::timestamptz,null),
    ('musiconvinyl.com','GB','https://www.musiconvinyl.com/pages/shipping','2026-09-29'::timestamptz,'Excludes overseas territories and Channel Islands.'),
    ('musiconvinyl.com','US','https://www.musiconvinyl.com/pages/shipping','2026-09-29'::timestamptz,'Excludes US territories.'),
    ('northendhaarlem.nl','NL','https://www.northendhaarlem.nl/c-4711050/versturen-shipping/','2026-09-29'::timestamptz,null),
    ('platenzaak.nl','NL','https://www.platenzaak.nl/pages/bestelinformatie','2026-09-29'::timestamptz,null),
    ('platomania.nl','NL','https://www.platomania.nl/verzendkosten','2026-09-29'::timestamptz,null),
    ('recordsonvinyl.nl','NL','https://recordsonvinyl.nl/pages/verzendkosten','2026-09-29'::timestamptz,null),
    ('recordsonvinyl.nl','BE','https://recordsonvinyl.nl/pages/verzendkosten','2026-09-29'::timestamptz,null),
    ('sounds-venlo.nl','NL','https://www.sounds-venlo.nl/service/','2026-09-29'::timestamptz,null),
    ('sounds-venlo.nl','BE','https://www.sounds-venlo.nl/service/','2026-09-29'::timestamptz,null),
    ('sounds-venlo.nl','GB','https://www.sounds-venlo.nl/service/','2026-09-29'::timestamptz,null),
    ('soundshaarlem.nl','NL','https://soundshaarlem.nl/nl/policies/shipping-policy','2026-09-29'::timestamptz,null),
    ('soundshaarlem.nl','BE','https://soundshaarlem.nl/nl/policies/shipping-policy','2026-09-29'::timestamptz,null),
    ('soundshaarlem.nl','GB','https://soundshaarlem.nl/nl/policies/shipping-policy','2026-09-29'::timestamptz,null),
    ('soundshaarlem.nl','US','https://soundshaarlem.nl/nl/policies/shipping-policy','2026-09-29'::timestamptz,null),
    ('suburban.nl','NL','https://suburban.nl/terms-conditions/','2026-09-29'::timestamptz,null),
    ('suburban.nl','BE','https://suburban.nl/terms-conditions/','2026-09-29'::timestamptz,'Official terms say worldwide shipping.'),
    ('suburban.nl','GB','https://suburban.nl/terms-conditions/','2026-09-29'::timestamptz,'Official terms say worldwide shipping.'),
    ('suburban.nl','US','https://suburban.nl/terms-conditions/','2026-09-29'::timestamptz,'Official terms say worldwide shipping.'),
    ('variaworld.nl','NL','https://www.variaworld.nl/info/verzendkosten/','2026-09-29'::timestamptz,null),
    ('variaworld.nl','BE','https://www.variaworld.nl/info/verzendkosten/','2026-09-29'::timestamptz,null)
)
update public.shop_shipping_destinations d
set status = 'confirmed', source_url = e.source_url,
    verified_at = e.checked_at, note = e.note, updated_at = now()
from public.shops s join evidence e on e.domain = s.domain
where d.shop_id = s.id and d.country_code = e.country_code
  and d.status = 'unknown';

alter table public.markets enable row level security;
alter table public.shop_shipping_destinations enable row level security;
drop policy if exists public_active_markets on public.markets;
create policy public_active_markets on public.markets for select to anon, authenticated
  using (status = 'active' and released_at is not null);
drop policy if exists public_confirmed_shipping_destinations on public.shop_shipping_destinations;
create policy public_confirmed_shipping_destinations on public.shop_shipping_destinations
  for select to anon, authenticated using (status = 'confirmed');
grant select on public.markets, public.shop_shipping_destinations to anon, authenticated;

create or replace function public.market_release_readiness_v1(p_country_code text)
returns table (country_code text, eligible_shop_count bigint, ready boolean, active boolean)
language sql stable security definer set search_path = public
as $$
  select m.country_code, count(distinct s.id), count(distinct s.id) >= 10,
    m.status = 'active'
  from public.markets m
  left join public.shop_shipping_destinations d
    on d.country_code = m.country_code and d.status = 'confirmed'
  left join public.shops s on s.id = d.shop_id and s.is_active
    and exists (
      select 1 from public.prices pr join public.products p on p.id = pr.product_id
      where pr.shop_id = s.id and pr.is_active and pr.price > 0
        and pr.currency = m.currency and pr.availability in ('in_stock','unknown')
        and pr.last_seen_at >= now() - interval '48 hours'
        and p.gtin_normalized is not null
        and coalesce(upper(btrim(p.format_label)), '') not in
          ('CD','POSTER','ACCESSORIES','PHOTOBOOK','BLUERAY','BLURAY')
    )
  where m.country_code = p_country_code
  group by m.country_code, m.status;
$$;
revoke all on function public.market_release_readiness_v1(text) from public, anon, authenticated;
grant execute on function public.market_release_readiness_v1(text) to service_role;

create or replace function public.require_market_release_readiness()
returns trigger language plpgsql security definer set search_path = public as $$
declare shop_count bigint;
begin
  if new.status = 'active' and old.status <> 'active' then
    select eligible_shop_count into shop_count
    from public.market_release_readiness_v1(new.country_code);
    if coalesce(shop_count, 0) < 10 then
      raise exception 'Market % needs 10 eligible shops; found %', new.country_code, coalesce(shop_count, 0);
    end if;
    if new.released_at is null then
      raise exception 'Market % requires an explicit released_at', new.country_code;
    end if;
  end if;
  new.updated_at := now();
  return new;
end;
$$;
drop trigger if exists markets_release_guard on public.markets;
create trigger markets_release_guard before update on public.markets
for each row execute function public.require_market_release_readiness();

create or replace view public.market_product_best_prices_v1
with (security_invoker = true) as
select m.country_code as market_code, p.id as product_id,
  min(pr.price) filter (where pr.availability = 'in_stock') as lowest_fresh_price,
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
      row_number() over (partition by pr.product_id, pr.shop_id
        order by pr.price asc, pr.last_seen_at desc) as shop_rank
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
      ) order by e.price asc) as offers,
      max(e.price)-min(e.price) as difference, max(e.last_seen_at) as last_seen_at,
      count(*) as shop_count
    from eligible e where e.shop_rank = 1
    group by e.product_id
    having count(*) >= 2 and max(e.price) > min(e.price)
  )
  select p.id, p.ean, p.artist, p.title, p.format_label, p.cover_url,
    g.offers, g.last_seen_at
  from grouped g join public.products p on p.id = g.product_id
  order by g.difference desc, g.shop_count desc, p.artist, p.title
  limit greatest(1, least(p_limit, 45));
$$;
revoke all on function public.market_top_deals_v1(text, integer) from public;
grant execute on function public.market_top_deals_v1(text, integer) to anon, authenticated;

create or replace function public.shop_highlights_market_v1(p_country_code text)
returns table (shop_id uuid, lowest_price_count bigint, only_here_count bigint)
language sql stable security invoker set search_path = public as $$
  with current_offers as (
    select pr.product_id, pr.shop_id, pr.availability,
      case when s.domain = 'atthemoviesshop.com' then round(pr.price * 1.21, 2)
        else pr.price end as display_price
    from public.markets m
    join public.shop_shipping_destinations d on d.country_code = m.country_code
      and d.status = 'confirmed'
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
  ), ranked as (
    select product_id, shop_id, display_price,
      count(*) over (partition by product_id) as offer_count,
      min(display_price) filter (where availability = 'in_stock')
        over (partition by product_id) as lowest_in_stock_price,
      min(display_price) over (partition by product_id) as lowest_any_price
    from current_offers
  )
  select shop_id,
    count(*) filter (where display_price = coalesce(lowest_in_stock_price, lowest_any_price)),
    count(*) filter (where offer_count = 1)
  from ranked group by shop_id;
$$;
revoke all on function public.shop_highlights_market_v1(text) from public;
grant execute on function public.shop_highlights_market_v1(text) to anon, authenticated;

commit;
