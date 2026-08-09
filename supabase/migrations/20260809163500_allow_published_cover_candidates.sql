begin;

do $$
declare
    constraint_count integer;
    invalid_count bigint;
begin
    select count(*)
    into constraint_count
    from pg_constraint c
    join pg_class t
      on t.oid = c.conrelid
    join pg_namespace n
      on n.oid = t.relnamespace
    where n.nspname = 'public'
      and t.relname = 'product_cover_candidates'
      and c.conname = 'product_cover_candidates_status_chk';

    if constraint_count <> 1 then
        raise exception
            'Expected exactly one product_cover_candidates_status_chk; found %',
            constraint_count;
    end if;

    select count(*)
    into invalid_count
    from public.product_cover_candidates
    where candidate_status is not null
      and candidate_status not in (
          'failed',
          'new',
          'pending',
          'published'
      );

    if invalid_count <> 0 then
        raise exception
            'Refusing candidate status constraint migration: % incompatible rows',
            invalid_count;
    end if;
end
$$;

alter table public.product_cover_candidates
    drop constraint product_cover_candidates_status_chk;

alter table public.product_cover_candidates
    add constraint product_cover_candidates_status_chk
    check (
        candidate_status is null
        or candidate_status = any (
            array[
                'failed'::text,
                'new'::text,
                'pending'::text,
                'published'::text
            ]
        )
    );

commit;
