create table if not exists public.release_calendar (
  id uuid primary key default gen_random_uuid(),
  ean text null,
  artist text not null,
  title text not null,
  release_date date not null,
  source_shop text not null,
  source_url text not null,
  image_url text null,
  format text null,
  label text null,
  status text not null default 'active',
  source_payload jsonb not null default '{}'::jsonb,
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists release_calendar_ean_source_date_uidx
on public.release_calendar (ean, source_shop, release_date)
where ean is not null;

create unique index if not exists release_calendar_source_url_uidx
on public.release_calendar (source_url);

create index if not exists release_calendar_release_date_idx
on public.release_calendar (release_date);

create index if not exists release_calendar_source_shop_idx
on public.release_calendar (source_shop);

create index if not exists release_calendar_ean_idx
on public.release_calendar (ean)
where ean is not null;
