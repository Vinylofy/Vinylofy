-- Everything Jazz USF-validatie
-- Alleen tabellen/kolommen die door de bestaande USF- en shippingcode worden gebruikt.

-- 1. Registry: links, status, prijs en listingtype.
select
  status,
  payload->>'product_type' as product_type,
  count(*) as links,
  count(*) filter (where nullif(trim(payload->>'price'), '') is not null) as met_listingprijs,
  count(*) filter (where payload->>'availability' = 'in_stock') as in_stock,
  count(*) filter (where payload->>'availability' = 'preorder') as preorder,
  count(*) filter (where payload->>'availability' = 'out_of_stock') as out_of_stock
from public.shop_product_links
where shop_id = 'everythingjazz'
group by status, payload->>'product_type'
order by status, product_type;

-- 2. Detaildekking op de nieuwste raw snapshot per URL.
with latest as (
  select distinct on (shop_id, source_url)
    id, shop_id, source_url, source_product_id, title_raw, ean_raw,
    price_raw, availability_raw, scraped_at, payload
  from public.raw_shop_scrapes
  where shop_id = 'everythingjazz'
  order by shop_id, source_url, scraped_at desc nulls last, id desc
)
select
  count(*) as nieuwste_details,
  count(*) filter (where ean_raw is not null) as met_ean,
  count(*) filter (where ean_raw is null) as zonder_ean,
  count(*) filter (where price_raw is not null) as met_listing_snapshotprijs,
  count(*) filter (where price_raw is null) as zonder_listing_snapshotprijs,
  count(*) filter (where payload->>'detail_issue' = 'ambiguous_variant_ean') as ambigu_ean,
  count(*) filter (where payload->>'detail_issue' = 'unsupported_product_type') as niet_vinyl,
  count(*) filter (where payload->>'detail_issue' = 'detail_request_error') as request_errors
from latest;

-- 3. Stage- en promotestatus.
select
  stage_status,
  count(*) as records,
  count(*) filter (where ean_match_key is null) as zonder_ean_match,
  count(*) filter (where price is null) as zonder_prijs
from public.staged_offers
where shop_id = 'everythingjazz'
group by stage_status
order by stage_status;

-- 4. Quarantaineredenen.
select issue_type, count(*) as records
from public.quarantine_offers
where shop_id = 'everythingjazz'
  and resolved_at is null
group by issue_type
order by records desc, issue_type;

-- 5. Dubbele EAN's op verschillende actieve shoplinks.
with latest as (
  select distinct on (shop_id, source_url)
    source_url, ean_raw
  from public.raw_shop_scrapes
  where shop_id = 'everythingjazz'
    and ean_raw is not null
  order by shop_id, source_url, scraped_at desc nulls last, id desc
)
select ean_raw, count(distinct source_url) as urls, array_agg(distinct source_url order by source_url) as source_urls
from latest
group by ean_raw
having count(distinct source_url) > 1
order by urls desc, ean_raw;

-- 6. Publieke prijzen van de shop.
select
  s.name as shop_name,
  s.domain,
  count(*) as price_rows,
  count(*) filter (where p.availability = 'in_stock') as in_stock,
  count(*) filter (where p.availability = 'preorder') as preorder,
  count(*) filter (where p.availability = 'out_of_stock') as out_of_stock
from public.prices p
join public.shops s on s.id = p.shop_id
where s.domain = 'eustore.everythingjazz.com'
group by s.name, s.domain;

-- 7. Shipping: exact één actieve NL-regel, €9,95 en geen gratis grens.
select
  shop_slug,
  country_code,
  currency,
  shipping_cost_cents,
  free_shipping_threshold_cents,
  shipping_logic,
  active,
  confidence,
  source_url,
  verified_at
from public.shop_shipping_rules
where shop_slug = 'everythingjazz'
order by country_code, active desc;

-- 8. Harde shippingcheck; verwacht exact één resultaatregel met alles true.
select
  count(*) filter (where active) = 1 as exact_een_actieve_regel,
  bool_and(shipping_cost_cents = 995) filter (where active) as bedrag_is_995,
  bool_and(free_shipping_threshold_cents is null) filter (where active) as geen_gratis_grens,
  bool_and(shipping_logic = 'flat') filter (where active) as flat_rate
from public.shop_shipping_rules
where shop_slug = 'everythingjazz'
  and country_code = 'NL';
