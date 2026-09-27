-- Seller classification is display metadata only. NULL means not yet verified.
begin;

alter table public.shops
  add column if not exists shop_type text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.shops'::regclass
      and conname = 'shops_shop_type_check'
  ) then
    alter table public.shops
      add constraint shops_shop_type_check
      check (shop_type in (
        'physical_record_store',
        'online_record_store',
        'online_retailer'
      ));
  end if;
end
$$;

-- Verified shop pages with visitable vinyl stores include:
-- 3345.nl/nl/pages/shipping_2022, northendhaarlem.nl/c-4711050/versturen/,
-- platomania.nl/stores, soundshaarlem.nl, sounds-venlo.nl/sounds-venlo/,
-- bobsvinyl.nl/pages/inkoop-tweedehands-lps, getbackmusic.nl, viprecords.nl,
-- myrecordstore.nl/contact, cdhal.nl/onze-winkel, hhv.de/store.
-- sounds.nl identifies itself as the webshop of Sounds Delft.
-- iMusic is online_record_store for Vinylofy per the requested product example;
-- its Danish physical shop is documented at imusic.nl/page/aboutstore.
-- Other online-only evidence: recordsonvinyl.nl/policies/terms-of-service,
-- colouredvinyl.nl/contact/, variaworld.nl/info/overons/.
-- Broad retailer evidence: over.bol.com/en/, dgmoutlet.nl/.
-- Read-only shop audit on 2026-09-27: 29 active rows, 19 mapped below.

do $$
declare
  target_domain text;
  target_type text;
  target_count integer;
  updated_count integer;
begin
  for target_domain, target_type in
    select domain, shop_type
    from (values
      ('3345.nl', 'physical_record_store'),
      ('northendhaarlem.nl', 'physical_record_store'),
      ('platomania.nl', 'physical_record_store'),
      ('sounds.nl', 'physical_record_store'),
      ('soundsdelft.nl', 'physical_record_store'),
      ('soundshaarlem.nl', 'physical_record_store'),
      ('sounds-venlo.nl', 'physical_record_store'),
      ('bobsvinyl.nl', 'physical_record_store'),
      ('getbackmusic.nl', 'physical_record_store'),
      ('viprecords.nl', 'physical_record_store'),
      ('myrecordstore.nl', 'physical_record_store'),
      ('cdhal.nl', 'physical_record_store'),
      ('hhv.de', 'physical_record_store'),
      ('imusic.nl', 'online_record_store'),
      ('recordsonvinyl.nl', 'online_record_store'),
      ('colouredvinyl.nl', 'online_record_store'),
      ('variaworld.nl', 'online_record_store'),
      ('bol.com', 'online_retailer'),
      ('dgmoutlet.nl', 'online_retailer')
    ) as classification(domain, shop_type)
  loop
    select count(*) into target_count
    from public.shops
    where regexp_replace(lower(btrim(domain)), '^www[.]', '') = target_domain
      and shop_type is null;

    update public.shops
    set shop_type = target_type
    where regexp_replace(lower(btrim(domain)), '^www[.]', '') = target_domain
      and shop_type is null;

    get diagnostics updated_count = row_count;
    if updated_count <> target_count then
      raise exception 'shop_type backfill mismatch for %: expected %, updated %',
        target_domain, target_count, updated_count;
    end if;
  end loop;
end
$$;

commit;
