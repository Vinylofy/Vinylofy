alter table public.products
  add column if not exists metadata_source text,
  add column if not exists metadata_status text default 'unknown',
  add column if not exists metadata_confidence numeric,
  add column if not exists metadata_last_enriched_at timestamptz,
  add column if not exists metadata_needs_review boolean default false,
  add column if not exists musicbrainz_release_id text,
  add column if not exists musicbrainz_release_group_id text,
  add column if not exists musicbrainz_artist text,
  add column if not exists musicbrainz_title text,
  add column if not exists musicbrainz_format text,
  add column if not exists musicbrainz_release_date text,
  add column if not exists musicbrainz_release_year integer,
  add column if not exists musicbrainz_country text,
  add column if not exists musicbrainz_label text,
  add column if not exists musicbrainz_match_score numeric,
  add column if not exists musicbrainz_match_basis text,
  add column if not exists musicbrainz_status text,
  add column if not exists musicbrainz_checked_at timestamptz,
  add column if not exists metadata_raw jsonb;

create index if not exists idx_products_musicbrainz_status
  on public.products (musicbrainz_status);

create index if not exists idx_products_musicbrainz_checked_at
  on public.products (musicbrainz_checked_at);

create index if not exists idx_products_metadata_status
  on public.products (metadata_status);

create index if not exists idx_products_musicbrainz_release_id
  on public.products (musicbrainz_release_id);
