begin;

do $$
begin
  if to_regclass('public.products') is null then
    raise exception 'Vereiste tabel public.products ontbreekt';
  end if;

  if not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'products'
      and column_name = 'cover_sha256'
  ) then
    raise exception 'Vereiste kolom public.products.cover_sha256 ontbreekt';
  end if;
end
$$;

alter table public.products
  add column if not exists cover_review_status text,
  add column if not exists cover_review_sha256 text,
  add column if not exists cover_reviewed_at timestamptz;

alter table public.products
  drop constraint if exists products_cover_review_status_chk,
  drop constraint if exists products_cover_review_sha256_chk,
  drop constraint if exists products_cover_review_consistency_chk,
  drop constraint if exists products_cover_review_approved_hash_chk;

alter table public.products
  add constraint products_cover_review_status_chk
    check (
      cover_review_status is null
      or cover_review_status in (
        'approved',
        'rejected'
      )
    ),

  add constraint products_cover_review_sha256_chk
    check (
      cover_review_sha256 is null
      or cover_review_sha256 ~ '^[0-9a-f]{64}$'
    ),

  add constraint products_cover_review_consistency_chk
    check (
      (
        cover_review_status is null
        and cover_review_sha256 is null
        and cover_reviewed_at is null
      )
      or
      (
        cover_review_status is not null
        and cover_reviewed_at is not null
      )
    ),

  add constraint products_cover_review_approved_hash_chk
    check (
      cover_review_status is distinct from 'approved'
      or cover_review_sha256 is not null
    );

create index if not exists products_cover_review_lookup_idx
  on public.products (
    cover_review_status,
    cover_review_sha256
  );

create or replace view public.cover_review_pending_products
with (security_invoker = true)
as
select
  p.id,
  p.ean,
  p.artist,
  p.title,
  p.format_label,
  p.cover_url,
  p.cover_storage_path,
  p.cover_sha256
from public.products p
where p.cover_status = 'ready'
  and nullif(
    btrim(p.cover_storage_path),
    ''
  ) is not null
  and not coalesce(
    (
      p.cover_review_status in (
        'approved',
        'rejected'
      )
      and p.cover_review_sha256 is not null
      and p.cover_sha256 is not null
      and p.cover_review_sha256 = p.cover_sha256
    ),
    false
  );

revoke all
  on public.cover_review_pending_products
  from anon, authenticated;

grant select
  on public.cover_review_pending_products
  to service_role;

comment on column public.products.cover_review_status is
  'Handmatige private cover-QA: approved of rejected; geen publicatiegate.';

comment on column public.products.cover_review_sha256 is
  'SHA256 van exact de afbeelding waarop de handmatige review betrekking had.';

comment on column public.products.cover_reviewed_at is
  'Moment waarop de huidige handmatige cover-QA-status is toegekend.';

comment on view public.cover_review_pending_products is
  'Private server-side QA-view: ready covers waarvan de huidige SHA nog niet exact is beoordeeld.';

commit;

select pg_notify(
  'pgrst',
  'reload schema'
);
