-- Requested shop classifications and home countries, 2026-09-27.
-- shop_type is display metadata and does not change prices or offer ordering.
begin;

do $$
declare
  constraint_definition text;
  target_domain text;
  previous_country text;
  previous_type text;
  desired_country text;
  desired_type text;
  target_count integer;
  current_country text;
  current_type text;
  current_active boolean;
  updated_count integer;
begin
  select pg_get_constraintdef(oid) into constraint_definition
  from pg_constraint
  where conrelid = 'public.shops'::regclass
    and conname = 'shops_shop_type_check';

  if constraint_definition is null
     or constraint_definition not like '%physical_record_store%'
     or constraint_definition not like '%online_record_store%'
     or constraint_definition not like '%online_retailer%'
     or constraint_definition not like '%record_label%' then
    raise exception 'Unexpected shops_shop_type_check constraint';
  end if;

  if constraint_definition not like '%online_shop%' then
    alter table public.shops drop constraint shops_shop_type_check;
    alter table public.shops add constraint shops_shop_type_check
      check (shop_type in (
        'physical_record_store',
        'online_record_store',
        'online_retailer',
        'record_label',
        'online_shop'
      ));
  end if;

  for target_domain, previous_country, previous_type, desired_country, desired_type in
    select domain, old_country, old_type, new_country, new_type
    from (values
      ('atthemoviesshop.com', 'NL', null::text, 'NL', 'online_shop'),
      ('blackvinyl.nl', 'NL', null::text, 'NL', 'physical_record_store'),
      ('eustore.everythingjazz.com', 'NL', null::text, 'NL', 'online_record_store'),
      ('fiftiesstore.nl', 'NL', null::text, 'NL', 'physical_record_store'),
      ('groovespin.nl', 'NL', null::text, 'CZ', 'online_record_store'),
      ('hhv.de', 'NL', 'physical_record_store', 'DE', 'physical_record_store'),
      ('imusic.nl', 'NL', 'online_record_store', 'DK', 'physical_record_store'),
      ('platenzaak.nl', 'NL', null::text, 'NL', 'online_record_store'),
      ('suburban.nl', 'NL', null::text, 'NL', 'record_label')
    ) as requested(domain, old_country, old_type, new_country, new_type)
  loop
    select count(*) into target_count
    from public.shops
    where regexp_replace(lower(btrim(domain)), '^www[.]', '') = target_domain;

    if target_count <> 1 then
      raise exception 'Expected one shop for %, found %', target_domain, target_count;
    end if;

    select country, shop_type, is_active
      into current_country, current_type, current_active
    from public.shops
    where regexp_replace(lower(btrim(domain)), '^www[.]', '') = target_domain
    for update;

    if current_active is distinct from true
       or (current_country is distinct from previous_country
           and current_country is distinct from desired_country)
       or (current_type is distinct from previous_type
           and current_type is distinct from desired_type) then
      raise exception 'Unexpected current values for %: country %, shop_type %, active %',
        target_domain, current_country, current_type, current_active;
    end if;

    if current_country is distinct from desired_country
       or current_type is distinct from desired_type then
      update public.shops
      set country = desired_country,
          shop_type = desired_type,
          updated_at = now()
      where regexp_replace(lower(btrim(domain)), '^www[.]', '') = target_domain
        and is_active;

      get diagnostics updated_count = row_count;
      if updated_count <> 1 then
        raise exception 'Expected one update for %, updated %', target_domain, updated_count;
      end if;
    end if;
  end loop;
end
$$;

commit;
