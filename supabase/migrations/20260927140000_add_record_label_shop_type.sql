-- Music On Vinyl describes itself as a vinyl record label:
-- https://www.musiconvinyl.com/pages/our-story
begin;

do $$
declare
  constraint_definition text;
  target_count integer;
  active_count integer;
  current_type text;
  updated_count integer;
begin
  select pg_get_constraintdef(oid) into constraint_definition
  from pg_constraint
  where conrelid = 'public.shops'::regclass
    and conname = 'shops_shop_type_check';

  if constraint_definition is null
     or constraint_definition not like '%physical_record_store%'
     or constraint_definition not like '%online_record_store%'
     or constraint_definition not like '%online_retailer%' then
    raise exception 'Unexpected shops_shop_type_check constraint';
  end if;

  if constraint_definition not like '%record_label%' then
    alter table public.shops drop constraint shops_shop_type_check;
    alter table public.shops add constraint shops_shop_type_check
      check (shop_type in (
        'physical_record_store',
        'online_record_store',
        'online_retailer',
        'record_label'
      ));
  end if;

  select count(*), count(*) filter (where is_active), max(shop_type)
    into target_count, active_count, current_type
  from public.shops
  where regexp_replace(lower(btrim(domain)), '^www[.]', '') = 'musiconvinyl.com';

  if target_count <> 1 or active_count <> 1 then
    raise exception 'Expected one active Music On Vinyl shop, found % total and % active',
      target_count, active_count;
  end if;

  if current_type is null then
    update public.shops
    set shop_type = 'record_label'
    where regexp_replace(lower(btrim(domain)), '^www[.]', '') = 'musiconvinyl.com'
      and is_active
      and shop_type is null;

    get diagnostics updated_count = row_count;
    if updated_count <> 1 then
      raise exception 'Expected one Music On Vinyl backfill, updated %', updated_count;
    end if;
  elsif current_type <> 'record_label' then
    raise exception 'Unexpected Music On Vinyl shop_type: %', current_type;
  end if;
end
$$;

commit;
