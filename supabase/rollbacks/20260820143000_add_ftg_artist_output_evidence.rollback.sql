begin;

do $$
begin
  if exists (
    select 1 from public.artist_output_evidence e
    join public.follow_the_groove_collection_runs r on r.id = e.created_by_run_id
    where r.source_system <> 'musicbrainz_output_evidence'
  ) then
    raise exception 'Rollback geweigerd: output evidence van een andere bron bestaat';
  end if;
end
$$;

drop table if exists public.artist_output_status;
drop table if exists public.artist_output_evidence;

commit;
