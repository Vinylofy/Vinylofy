begin;

do $$
begin
  if to_regclass('public.product_cover_candidates') is null then
    raise exception
      'Vereiste tabel public.product_cover_candidates ontbreekt';
  end if;

  if not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'product_cover_candidates'
      and column_name = 'shop_id'
  ) then
    raise exception
      'Vereiste kolom public.product_cover_candidates.shop_id ontbreekt';
  end if;
end
$$;

-- Externe covermetadata zoals Cover Art Archive / MusicBrainz heeft
-- geen shop-eigenaarschap. Shop-candidates behouden hun shop_id en FK.
alter table public.product_cover_candidates
  alter column shop_id drop not null;

commit;
