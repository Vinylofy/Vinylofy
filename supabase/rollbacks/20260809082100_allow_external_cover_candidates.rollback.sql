begin;

do $$
begin
  if to_regclass('public.product_cover_candidates') is null then
    raise exception
      'Vereiste tabel public.product_cover_candidates ontbreekt';
  end if;

  if exists (
    select 1
    from public.product_cover_candidates
    where shop_id is null
  ) then
    raise exception
      'Rollback geweigerd: externe cover candidates met shop_id=NULL bestaan';
  end if;
end
$$;

alter table public.product_cover_candidates
  alter column shop_id set not null;

commit;
