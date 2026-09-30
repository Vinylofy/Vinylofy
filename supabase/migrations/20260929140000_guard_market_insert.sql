begin;

-- A new market must first be recorded as prepared. Its first activation is
-- then the explicit, guarded update; an active INSERT cannot skip readiness.
create or replace function public.require_market_release_readiness()
returns trigger language plpgsql security definer set search_path = public as $$
declare shop_count bigint;
begin
  if tg_op = 'INSERT' then
    if new.status = 'active' then
      raise exception 'Insert market % as prepared before its first release', new.country_code;
    end if;
  else
    if new.country_code is distinct from old.country_code then
      raise exception 'Market country_code cannot be changed';
    end if;
    if new.status = 'active' and old.status <> 'active' then
      if new.currency is distinct from old.currency then
        raise exception 'Market currency cannot change during first release';
      end if;
      select eligible_shop_count into shop_count
      from public.market_release_readiness_v1(new.country_code);
      if coalesce(shop_count, 0) < 10 then
        raise exception 'Market % needs 10 eligible shops; found %',
          new.country_code, coalesce(shop_count, 0);
      end if;
      if new.released_at is null then
        raise exception 'Market % requires an explicit released_at', new.country_code;
      end if;
    end if;
  end if;
  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists markets_release_guard on public.markets;
create trigger markets_release_guard before insert or update on public.markets
for each row execute function public.require_market_release_readiness();

commit;
