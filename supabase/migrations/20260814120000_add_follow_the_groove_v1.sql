begin;

-- Follow the Groove v1 is intentionally isolated from the existing commercial
-- schema. public.products is referenced as a parent only and is not altered.
do $$
declare
  v_object text;
begin
  if current_setting('server_version_num')::integer < 150000 then
    raise exception 'Follow the Groove vereist PostgreSQL 15+ voor NULLS NOT DISTINCT';
  end if;

  if to_regclass('public.products') is null then
    raise exception 'Vereiste tabel public.products ontbreekt';
  end if;

  if not exists (
    select 1
    from pg_attribute a
    where a.attrelid = 'public.products'::regclass
      and a.attname = 'id'
      and a.atttypid = 'uuid'::regtype
      and a.attnotnull
      and not a.attisdropped
  ) then
    raise exception 'public.products.id moet UUID NOT NULL zijn';
  end if;

  if not exists (
    select 1
    from pg_constraint c
    join pg_attribute a
      on a.attrelid = c.conrelid
     and a.attnum = c.conkey[1]
    where c.conrelid = 'public.products'::regclass
      and c.contype = 'p'
      and cardinality(c.conkey) = 1
      and a.attname = 'id'
  ) then
    raise exception 'public.products.id moet de enkelvoudige primary key zijn';
  end if;

  if to_regprocedure('gen_random_uuid()') is null then
    raise exception 'Vereiste functie gen_random_uuid() ontbreekt in search_path';
  end if;

  foreach v_object in array array[
    'follow_the_groove_collection_runs',
    'artists',
    'artist_aliases',
    'artist_edges',
    'artist_relation_evidence',
    'artist_similarity',
    'product_artists'
  ]
  loop
    if to_regclass(format('public.%I', v_object)) is not null then
      raise exception 'Doelobject public.% bestaat al', v_object;
    end if;
  end loop;

  if to_regprocedure('public.ftg_canonical_uuid_array(uuid[])') is not null then
    raise exception 'Doelfunctie public.ftg_canonical_uuid_array(uuid[]) bestaat al';
  end if;
end
$$;

create function public.ftg_canonical_uuid_array(p_values uuid[])
returns uuid[]
language sql
immutable
parallel safe
set search_path = pg_catalog
as $$
  select coalesce(
    array_agg(distinct value order by value),
    '{}'::uuid[]
  )
  from unnest(coalesce(p_values, '{}'::uuid[])) as valueset(value);
$$;

create table public.follow_the_groove_collection_runs (
  id uuid primary key default gen_random_uuid(),
  collector text not null check (btrim(collector) <> ''),
  source_system text not null check (btrim(source_system) <> ''),
  scope text not null check (btrim(scope) <> ''),
  status text not null default 'running'
    check (status in ('running', 'succeeded', 'partial', 'failed')),
  counters jsonb not null default '{}'::jsonb
    check (jsonb_typeof(counters) = 'object'),
  error_summary jsonb not null default '[]'::jsonb
    check (jsonb_typeof(error_summary) = 'array'),
  started_at timestamptz not null default now(),
  finished_at timestamptz,
  check (finished_at is null or finished_at >= started_at),
  check (
    (status = 'running' and finished_at is null)
    or (status <> 'running' and finished_at is not null)
  )
);

create table public.artists (
  id uuid primary key default gen_random_uuid(),
  musicbrainz_artist_mbid uuid not null unique,
  display_name text not null check (btrim(display_name) <> ''),
  entity_type text not null check (entity_type in ('person', 'group')),
  musicbrainz_type_id uuid not null,
  wikidata_qid text,
  created_by_run_id uuid references public.follow_the_groove_collection_runs(id)
    on delete restrict,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  last_verified_at timestamptz not null default now(),
  check (wikidata_qid is null or wikidata_qid ~ '^Q[1-9][0-9]*$'),
  check (updated_at >= created_at)
);

create unique index artists_wikidata_qid_uidx
  on public.artists (wikidata_qid)
  where wikidata_qid is not null;

create table public.artist_aliases (
  id uuid primary key default gen_random_uuid(),
  artist_id uuid not null references public.artists(id) on delete cascade,
  alias_name text not null check (btrim(alias_name) <> ''),
  alias_normalized text not null check (btrim(alias_normalized) <> ''),
  source_system text not null check (btrim(source_system) <> ''),
  alias_type text,
  locale text,
  is_primary boolean not null default false,
  begin_date text,
  end_date text,
  provenance jsonb not null default '{}'::jsonb
    check (jsonb_typeof(provenance) = 'object'),
  created_by_run_id uuid references public.follow_the_groove_collection_runs(id)
    on delete restrict,
  created_at timestamptz not null default now(),
  last_verified_at timestamptz not null default now(),
  check (begin_date is null or begin_date ~ '^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$'),
  check (end_date is null or end_date ~ '^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$'),
  unique nulls not distinct (
    artist_id,
    source_system,
    alias_name,
    alias_type,
    locale,
    begin_date,
    end_date
  )
);

create index artist_aliases_resolution_idx
  on public.artist_aliases (alias_normalized, artist_id);

create table public.artist_edges (
  id uuid primary key default gen_random_uuid(),
  artist_low_id uuid not null references public.artists(id) on delete cascade,
  artist_high_id uuid not null references public.artists(id) on delete cascade,
  created_by_run_id uuid references public.follow_the_groove_collection_runs(id)
    on delete restrict,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (artist_low_id < artist_high_id),
  check (updated_at >= created_at),
  unique (artist_low_id, artist_high_id),
  unique (id, artist_low_id, artist_high_id)
);

create index artist_edges_high_idx
  on public.artist_edges (artist_high_id, artist_low_id);

create table public.artist_relation_evidence (
  id uuid primary key default gen_random_uuid(),
  source_artist_id uuid not null references public.artists(id) on delete cascade,
  target_artist_id uuid not null references public.artists(id) on delete cascade,
  pair_low_id uuid generated always as (
    least(source_artist_id, target_artist_id)
  ) stored,
  pair_high_id uuid generated always as (
    greatest(source_artist_id, target_artist_id)
  ) stored,
  edge_id uuid,
  created_by_run_id uuid references public.follow_the_groove_collection_runs(id)
    on delete restrict,
  last_seen_run_id uuid references public.follow_the_groove_collection_runs(id)
    on delete restrict,
  source_system text not null check (btrim(source_system) <> ''),
  source_entity_kind text not null check (btrim(source_entity_kind) <> ''),
  source_entity_id text not null check (btrim(source_entity_id) <> ''),
  evidence_kind text not null
    check (evidence_kind in (
      'membership',
      'artist_credit',
      'instrument',
      'vocal',
      'rejected',
      'insufficient'
    )),
  classification text not null
    check (classification in ('allowed', 'rejected', 'insufficient')),
  direction text not null
    check (direction in ('source_to_target', 'target_to_source', 'symmetric')),
  source_relation_name text,
  relation_type_id uuid,
  begin_date text,
  end_date text,
  ended boolean,
  recording_mbid uuid,
  release_mbid uuid,
  release_group_mbid uuid,
  work_mbid uuid,
  attribute_ids uuid[] not null default '{}'::uuid[],
  provenance jsonb not null default '{}'::jsonb
    check (jsonb_typeof(provenance) = 'object'),
  first_seen_at timestamptz not null default now(),
  last_seen_at timestamptz not null default now(),
  last_verified_at timestamptz not null default now(),
  check (source_artist_id <> target_artist_id),
  check (begin_date is null or begin_date ~ '^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$'),
  check (end_date is null or end_date ~ '^[0-9]{4}(-[0-9]{2}(-[0-9]{2})?)?$'),
  check (attribute_ids = public.ftg_canonical_uuid_array(attribute_ids)),
  check (first_seen_at <= last_seen_at),
  check (
    (classification = 'allowed' and evidence_kind in (
      'membership', 'artist_credit', 'instrument', 'vocal'
    ) and edge_id is not null)
    or (classification = 'rejected' and evidence_kind = 'rejected' and edge_id is null)
    or (classification = 'insufficient' and evidence_kind = 'insufficient' and edge_id is null)
  ),
  check (evidence_kind <> 'membership' or relation_type_id is not null),
  check (evidence_kind <> 'membership' or direction = 'source_to_target'),
  check (evidence_kind not in ('artist_credit', 'instrument', 'vocal') or recording_mbid is not null),
  check (evidence_kind not in ('artist_credit', 'instrument', 'vocal') or release_mbid is not null),
  check (evidence_kind <> 'artist_credit' or direction = 'symmetric'),
  check (evidence_kind <> 'artist_credit' or source_artist_id < target_artist_id),
  foreign key (edge_id, pair_low_id, pair_high_id)
    references public.artist_edges(id, artist_low_id, artist_high_id)
    on delete cascade
);

create unique index artist_relation_evidence_membership_uidx
  on public.artist_relation_evidence (
    source_system,
    relation_type_id,
    source_artist_id,
    target_artist_id,
    begin_date,
    end_date,
    attribute_ids
  ) nulls not distinct
  where evidence_kind = 'membership';

create unique index artist_relation_evidence_artist_credit_uidx
  on public.artist_relation_evidence (
    source_system,
    recording_mbid,
    source_artist_id,
    target_artist_id,
    evidence_kind
  ) nulls not distinct
  where evidence_kind = 'artist_credit';

create unique index artist_relation_evidence_performer_uidx
  on public.artist_relation_evidence (
    source_system,
    evidence_kind,
    recording_mbid,
    relation_type_id,
    source_artist_id,
    target_artist_id,
    attribute_ids
  ) nulls not distinct
  where evidence_kind in ('instrument', 'vocal');

create unique index artist_relation_evidence_decision_uidx
  on public.artist_relation_evidence (
    source_system,
    evidence_kind,
    source_entity_kind,
    source_entity_id,
    source_artist_id,
    target_artist_id,
    relation_type_id,
    recording_mbid,
    work_mbid,
    attribute_ids
  ) nulls not distinct
  where evidence_kind in ('rejected', 'insufficient');

create index artist_relation_evidence_edge_idx
  on public.artist_relation_evidence (edge_id)
  where edge_id is not null;

create index artist_relation_evidence_membership_group_person_idx
  on public.artist_relation_evidence (target_artist_id, source_artist_id)
  where classification = 'allowed'
    and evidence_kind = 'membership'
    and direction = 'source_to_target';

create index artist_relation_evidence_membership_person_group_idx
  on public.artist_relation_evidence (source_artist_id, target_artist_id)
  where classification = 'allowed'
    and evidence_kind = 'membership'
    and direction = 'source_to_target';

create table public.artist_similarity (
  id uuid primary key default gen_random_uuid(),
  source_artist_id uuid not null references public.artists(id) on delete cascade,
  target_artist_id uuid references public.artists(id) on delete cascade,
  created_by_run_id uuid references public.follow_the_groove_collection_runs(id)
    on delete restrict,
  last_seen_run_id uuid references public.follow_the_groove_collection_runs(id)
    on delete restrict,
  source_system text not null default 'lastfm'
    check (btrim(source_system) <> ''),
  requested_source_name text not null check (btrim(requested_source_name) <> ''),
  returned_target_name text not null check (btrim(returned_target_name) <> ''),
  returned_target_name_normalized text not null
    check (btrim(returned_target_name_normalized) <> ''),
  returned_mbid uuid,
  match_score numeric not null check (match_score >= 0),
  position integer not null check (position > 0),
  resolution_status text not null
    check (resolution_status in ('resolved', 'unresolved', 'conflict')),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  checked_at timestamptz not null default now(),
  check (
    (resolution_status = 'resolved' and target_artist_id is not null)
    or (resolution_status in ('unresolved', 'conflict') and target_artist_id is null)
  ),
  check (target_artist_id is null or source_artist_id <> target_artist_id),
  check (updated_at >= created_at)
);

create unique index artist_similarity_resolved_uidx
  on public.artist_similarity (source_system, source_artist_id, target_artist_id)
  where resolution_status = 'resolved';

create unique index artist_similarity_unresolved_uidx
  on public.artist_similarity (
    source_system,
    source_artist_id,
    returned_target_name_normalized
  )
  where resolution_status in ('unresolved', 'conflict');

create index artist_similarity_source_rank_idx
  on public.artist_similarity (
    source_artist_id,
    resolution_status,
    position,
    match_score desc
  );

create table public.product_artists (
  id uuid primary key default gen_random_uuid(),
  product_id uuid not null references public.products(id) on delete cascade,
  artist_id uuid not null references public.artists(id) on delete cascade,
  credited_name text,
  credit_position integer,
  source_system text not null check (btrim(source_system) <> ''),
  created_by_run_id uuid references public.follow_the_groove_collection_runs(id)
    on delete restrict,
  created_at timestamptz not null default now(),
  last_verified_at timestamptz not null default now(),
  check (credited_name is null or btrim(credited_name) <> ''),
  check (credit_position is null or credit_position > 0),
  unique (product_id, artist_id)
);

create index product_artists_artist_product_idx
  on public.product_artists (artist_id, product_id);

-- The live project has broad default table privileges. Reset every new object
-- explicitly before granting the narrow v1 contract.
revoke all on table
  public.follow_the_groove_collection_runs,
  public.artists,
  public.artist_aliases,
  public.artist_edges,
  public.artist_relation_evidence,
  public.artist_similarity,
  public.product_artists
from public, anon, authenticated, service_role;

revoke all on function public.ftg_canonical_uuid_array(uuid[])
from public, anon, authenticated, service_role;

grant select, insert, update, delete on table
  public.follow_the_groove_collection_runs,
  public.artists,
  public.artist_aliases,
  public.artist_edges,
  public.artist_relation_evidence,
  public.artist_similarity,
  public.product_artists
to service_role;

grant execute on function public.ftg_canonical_uuid_array(uuid[])
to service_role;

grant select on table
  public.artists,
  public.artist_aliases,
  public.artist_edges,
  public.artist_similarity,
  public.product_artists
to anon, authenticated;

alter table public.follow_the_groove_collection_runs enable row level security;
alter table public.artists enable row level security;
alter table public.artist_aliases enable row level security;
alter table public.artist_edges enable row level security;
alter table public.artist_relation_evidence enable row level security;
alter table public.artist_similarity enable row level security;
alter table public.product_artists enable row level security;

create policy follow_the_groove_collection_runs_service_role_all
  on public.follow_the_groove_collection_runs
  for all to service_role using (true) with check (true);

create policy artists_public_read
  on public.artists
  for select to anon, authenticated using (true);
create policy artists_service_role_all
  on public.artists
  for all to service_role using (true) with check (true);

create policy artist_aliases_public_read
  on public.artist_aliases
  for select to anon, authenticated using (true);
create policy artist_aliases_service_role_all
  on public.artist_aliases
  for all to service_role using (true) with check (true);

create policy artist_edges_public_read
  on public.artist_edges
  for select to anon, authenticated using (true);
create policy artist_edges_service_role_all
  on public.artist_edges
  for all to service_role using (true) with check (true);

create policy artist_relation_evidence_service_role_all
  on public.artist_relation_evidence
  for all to service_role using (true) with check (true);

create policy artist_similarity_resolved_public_read
  on public.artist_similarity
  for select to anon, authenticated
  using (resolution_status = 'resolved' and target_artist_id is not null);
create policy artist_similarity_service_role_all
  on public.artist_similarity
  for all to service_role using (true) with check (true);

create policy product_artists_public_read
  on public.product_artists
  for select to anon, authenticated using (true);
create policy product_artists_service_role_all
  on public.product_artists
  for all to service_role using (true) with check (true);

-- Pilot rollback contract:
-- 1. identify rows by created_by_run_id;
-- 2. remove run-created similarity, evidence, aliases and product links first;
-- 3. remove run-created edges only when no allowed evidence remains;
-- 4. remove run-created artists only when no evidence, edge, similarity or
--    product link from another run references them;
-- 5. remove the collection run last. public.products remains untouched.

commit;
