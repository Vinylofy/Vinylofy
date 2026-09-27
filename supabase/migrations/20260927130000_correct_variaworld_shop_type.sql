-- Variaworld lists visitable vinyl shops with addresses and opening hours:
-- https://www.variaworld.nl/info/contact/
begin;

do $$
declare
  shop_count integer;
  current_type text;
  updated_count integer;
begin
  select count(*), max(shop_type)
    into shop_count, current_type
  from public.shops
  where regexp_replace(lower(btrim(domain)), '^www[.]', '') = 'variaworld.nl'
    and is_active;

  if shop_count <> 1 then
    raise exception 'Expected one active Variaworld shop, found %', shop_count;
  end if;

  if current_type = 'online_record_store' then
    update public.shops
    set shop_type = 'physical_record_store'
    where regexp_replace(lower(btrim(domain)), '^www[.]', '') = 'variaworld.nl'
      and is_active
      and shop_type = 'online_record_store';

    get diagnostics updated_count = row_count;
    if updated_count <> 1 then
      raise exception 'Expected one Variaworld correction, updated %', updated_count;
    end if;
  elsif current_type is distinct from 'physical_record_store' then
    raise exception 'Unexpected Variaworld shop_type: %', current_type;
  end if;
end
$$;

commit;
