-- MusicBrainz / Cover Art Archive queue + cache for Vinylofy
-- Safe to run multiple times where possible.

create extension if not exists pgcrypto;

create table if not exists public.cover_lookup_queue (
  id uuid primary key default gen_random_uuid(),
  product_id uuid not null references public.products(id) on delete cascade,
  ean text not null,
  priority integer not null default 100,
  source text not null default 'system',
  status text not null default 'queued',
  attempts integer not null default 0,
  locked_at timestamptz null,
  locked_by text null,
  next_attempt_at timestamptz not null default now(),
  last_error text null,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint cover_lookup_queue_status_chk
    check (status in ('queued', 'processing', 'retry', 'done', 'failed')),
  constraint cover_lookup_queue_ean_len_chk
    check (char_length(ean) in (8, 12, 13, 14)),
  constraint cover_lookup_queue_product_unique unique (product_id)
);

create index if not exists idx_cover_lookup_queue_pick
  on public.cover_lookup_queue (status, next_attempt_at, priority desc, created_at);

create index if not exists idx_cover_lookup_queue_ean
  on public.cover_lookup_queue (ean);

create table if not exists public.musicbrainz_release_cache (
  ean text primary key,
  mb_release_id uuid null,
  mb_release_group_id uuid null,
  matched_title text null,
  matched_artist text null,
  matched_date text null,
  matched_country text null,
  match_score numeric(6,2) null,
  match_basis text null,
  status text not null default 'unknown',
  raw_result jsonb null,
  cover_json jsonb null,
  cover_front_url text null,
  last_error text null,
  checked_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint musicbrainz_release_cache_status_chk
    check (status in ('unknown', 'matched', 'no_match', 'ambiguous', 'failed')),
  constraint musicbrainz_release_cache_ean_len_chk
    check (char_length(ean) in (8, 12, 13, 14))
);

create index if not exists idx_musicbrainz_release_cache_status
  on public.musicbrainz_release_cache (status, checked_at desc);

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

DO $$
begin
  if not exists (
    select 1
    from pg_trigger
    where tgname = 'trg_cover_lookup_queue_updated_at'
  ) then
    create trigger trg_cover_lookup_queue_updated_at
    before update on public.cover_lookup_queue
    for each row execute function public.set_updated_at();
  end if;

  if not exists (
    select 1
    from pg_trigger
    where tgname = 'trg_musicbrainz_release_cache_updated_at'
  ) then
    create trigger trg_musicbrainz_release_cache_updated_at
    before update on public.musicbrainz_release_cache
    for each row execute function public.set_updated_at();
  end if;
end $$;

-- Helpful queue backfill. Keeps existing priority if the item is already queued.
insert into public.cover_lookup_queue (product_id, ean, priority, source)
select
  p.id,
  p.ean,
  coalesce(p.cover_priority, 100),
  'backfill'
from public.products p
where p.ean is not null
  and char_length(p.ean) in (8, 12, 13, 14)
  and coalesce(p.cover_storage_path, '') = ''
  and not exists (
    select 1
    from public.cover_lookup_queue q
    where q.product_id = p.id
  );
