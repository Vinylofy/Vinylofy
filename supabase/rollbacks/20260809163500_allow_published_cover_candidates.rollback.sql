begin;

do $$
declare
    published_count bigint;
    incompatible_count bigint;
begin
    select count(*)
    into published_count
    from public.product_cover_candidates
    where candidate_status = 'published';

    if published_count <> 0 then
        raise exception
            'Refusing rollback: % published cover candidates exist',
            published_count;
    end if;

    select count(*)
    into incompatible_count
    from public.product_cover_candidates
    where candidate_status is not null
      and candidate_status not in (
          'failed',
          'new',
          'pending'
      );

    if incompatible_count <> 0 then
        raise exception
            'Refusing rollback: % rows do not fit legacy status domain',
            incompatible_count;
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
                'pending'::text
            ]
        )
    );

commit;
