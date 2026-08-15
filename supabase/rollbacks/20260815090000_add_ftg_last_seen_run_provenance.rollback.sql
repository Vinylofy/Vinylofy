-- Roll back only the additive Follow-the-Groove run-provenance fields.
-- Refuse destructive rollback after FTG data has been collected.
begin;

do $$
declare
  v_table text;
  v_count bigint;
begin
  foreach v_table in array array[
    'artists',
    'artist_aliases',
    'artist_edges',
    'product_artists'
  ]
  loop
    execute format('select count(*) from public.%I', v_table) into v_count;
    if v_count <> 0 then
      raise exception 'Rollback geweigerd: public.% bevat % rows', v_table, v_count;
    end if;
  end loop;
end
$$;

drop index if exists public.product_artists_last_seen_run_idx;
drop index if exists public.product_artists_created_by_run_idx;
drop index if exists public.artist_edges_last_seen_run_idx;
drop index if exists public.artist_edges_created_by_run_idx;
drop index if exists public.artist_aliases_last_seen_run_idx;
drop index if exists public.artist_aliases_created_by_run_idx;
drop index if exists public.artists_last_seen_run_idx;
drop index if exists public.artists_created_by_run_idx;

alter table public.product_artists drop column if exists last_seen_run_id;
alter table public.artist_edges drop column if exists last_seen_run_id;
alter table public.artist_aliases drop column if exists last_seen_run_id;
alter table public.artists drop column if exists last_seen_run_id;

commit;
