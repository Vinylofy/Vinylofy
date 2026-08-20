begin;

create table public.artist_output_evidence (
  id uuid primary key default gen_random_uuid(),
  artist_id uuid not null references public.artists(id) on delete cascade,
  evidence_type text not null check (evidence_type in (
    'release_group_primary_artist', 'release_primary_artist', 'recording_artist'
  )),
  source_system text not null check (source_system in ('musicbrainz', 'vinylofy_local_musicbrainz')),
  source_entity_kind text not null check (source_entity_kind in ('release_group', 'release', 'recording')),
  source_entity_id uuid not null,
  provenance jsonb not null default '{}'::jsonb check (jsonb_typeof(provenance) = 'object'),
  created_by_run_id uuid not null references public.follow_the_groove_collection_runs(id) on delete restrict,
  last_seen_run_id uuid not null references public.follow_the_groove_collection_runs(id) on delete restrict,
  verified_at timestamptz not null,
  created_at timestamptz not null default now(),
  unique (artist_id, evidence_type, source_entity_kind, source_entity_id),
  unique (id, artist_id)
);

create index artist_output_evidence_artist_idx on public.artist_output_evidence (artist_id);
create index artist_output_evidence_created_by_run_idx on public.artist_output_evidence (created_by_run_id);

create table public.artist_output_status (
  artist_id uuid primary key references public.artists(id) on delete cascade,
  status text not null check (status in ('proven_output', 'proven_bridge_only', 'unknown')),
  basis_evidence_id uuid,
  provenance jsonb not null default '{}'::jsonb check (jsonb_typeof(provenance) = 'object'),
  created_by_run_id uuid not null references public.follow_the_groove_collection_runs(id) on delete restrict,
  last_seen_run_id uuid not null references public.follow_the_groove_collection_runs(id) on delete restrict,
  verified_at timestamptz not null,
  updated_at timestamptz not null default now(),
  check (
    (status = 'proven_output' and basis_evidence_id is not null)
    or (status in ('proven_bridge_only', 'unknown') and basis_evidence_id is null)
  ),
  foreign key (basis_evidence_id, artist_id)
    references public.artist_output_evidence(id, artist_id) on delete restrict
);

revoke all on table public.artist_output_evidence, public.artist_output_status
from public, anon, authenticated, service_role;
grant select, insert, update, delete on table public.artist_output_evidence, public.artist_output_status
to service_role;

alter table public.artist_output_evidence enable row level security;
alter table public.artist_output_status enable row level security;

create policy artist_output_evidence_service_role_all on public.artist_output_evidence
  for all to service_role using (true) with check (true);
create policy artist_output_status_service_role_all on public.artist_output_status
  for all to service_role using (true) with check (true);

commit;
