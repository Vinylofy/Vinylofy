begin;

-- Belgium's first public release is deliberate. The existing trigger checks
-- that at least ten distinct shops have confirmed BE shipping and fresh,
-- publishable EUR offers. Reapplying this migration is a no-op once released.
do $$
declare
  current_status text;
  current_released_at timestamptz;
  eligible_shops bigint;
begin
  select status, released_at into current_status, current_released_at
  from public.markets
  where country_code = 'BE'
  for update;

  if not found then
    raise exception 'Belgium market is missing; apply market preparation first';
  end if;

  if current_status = 'active' and current_released_at is not null then
    return;
  end if;

  if current_status <> 'prepared' or current_released_at is not null then
    raise exception 'Unexpected Belgium market state: %, %', current_status, current_released_at;
  end if;

  select eligible_shop_count into eligible_shops
  from public.market_release_readiness_v1('BE');
  if coalesce(eligible_shops, 0) < 10 then
    raise exception 'Belgium release needs 10 eligible shops; found %', coalesce(eligible_shops, 0);
  end if;

  update public.markets
  set status = 'active', released_at = now()
  where country_code = 'BE' and status = 'prepared' and released_at is null;

  if not found then
    raise exception 'Belgium release did not update exactly one prepared market';
  end if;

  if not exists (
    select 1 from public.market_product_best_prices_v1
    where market_code = 'BE'
  ) then
    raise exception 'Belgium has no public eligible offers after release';
  end if;
end;
$$;

commit;
