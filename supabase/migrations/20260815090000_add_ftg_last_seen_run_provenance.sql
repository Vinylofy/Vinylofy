begin;

-- Follow the Groove MVP rollback provenance. Existing identity and relation
-- content remains immutable across runs; a later run may only record that it
-- reliably observed the same row again.
do $$
declare
  v_table text;
begin
  foreach v_table in array array[
    'artists',
    'artist_aliases',
    'artist_edges',
    'product_artists'
  ]
  loop
    if to_regclass(format('public.%I', v_table)) is null then
      raise exception 'Vereiste FTG-tabel public.% ontbreekt', v_table;
    end if;

    if exists (
      select 1
      from information_schema.columns
      where table_schema = 'public'
        and table_name = v_table
        and column_name = 'last_seen_run_id'
    ) then
      raise exception 'Doelkolom public.%.last_seen_run_id bestaat al', v_table;
    end if;
  end loop;

  if to_regclass('public.follow_the_groove_collection_runs') is null then
    raise exception 'Vereiste FTG-tabel public.follow_the_groove_collection_runs ontbreekt';
  end if;
end
$$;

alter table public.artists
  add column last_seen_run_id uuid,
  add constraint artists_last_seen_run_id_fkey
    foreign key (last_seen_run_id)
    references public.follow_the_groove_collection_runs(id)
    on delete restrict;

alter table public.artist_aliases
  add column last_seen_run_id uuid,
  add constraint artist_aliases_last_seen_run_id_fkey
    foreign key (last_seen_run_id)
    references public.follow_the_groove_collection_runs(id)
    on delete restrict;

alter table public.artist_edges
  add column last_seen_run_id uuid,
  add constraint artist_edges_last_seen_run_id_fkey
    foreign key (last_seen_run_id)
    references public.follow_the_groove_collection_runs(id)
    on delete restrict;

alter table public.product_artists
  add column last_seen_run_id uuid,
  add constraint product_artists_last_seen_run_id_fkey
    foreign key (last_seen_run_id)
    references public.follow_the_groove_collection_runs(id)
    on delete restrict;

-- Both access paths are required by rollback: created_by_run_id identifies
-- run-owned rows; last_seen_run_id identifies pre-existing rows seen by a run.
create index artists_created_by_run_idx
  on public.artists (created_by_run_id)
  where created_by_run_id is not null;
create index artists_last_seen_run_idx
  on public.artists (last_seen_run_id)
  where last_seen_run_id is not null;

create index artist_aliases_created_by_run_idx
  on public.artist_aliases (created_by_run_id)
  where created_by_run_id is not null;
create index artist_aliases_last_seen_run_idx
  on public.artist_aliases (last_seen_run_id)
  where last_seen_run_id is not null;

create index artist_edges_created_by_run_idx
  on public.artist_edges (created_by_run_id)
  where created_by_run_id is not null;
create index artist_edges_last_seen_run_idx
  on public.artist_edges (last_seen_run_id)
  where last_seen_run_id is not null;

create index product_artists_created_by_run_idx
  on public.product_artists (created_by_run_id)
  where created_by_run_id is not null;
create index product_artists_last_seen_run_idx
  on public.product_artists (last_seen_run_id)
  where last_seen_run_id is not null;

commit;
