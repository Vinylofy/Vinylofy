begin;

create extension if not exists pgcrypto;

alter table if exists public.products
    add column if not exists cover_url text,
    add column if not exists cover_storage_path text,
    add column if not exists cover_source text,
    add column if not exists cover_source_url text,
    add column if not exists cover_status text,
    add column if not exists cover_confidence integer,
    add column if not exists cover_priority integer default 0,
    add column if not exists cover_mbid text,
    add column if not exists cover_last_attempt_at timestamptz,
    add column if not exists cover_source_shop_id uuid,
    add column if not exists cover_width integer,
    add column if not exists cover_height integer;

do $$
begin
    if not exists (select 1 from pg_constraint where conname = 'products_cover_confidence_chk') then
        alter table public.products
            add constraint products_cover_confidence_chk
            check (cover_confidence is null or (cover_confidence between 0 and 100));
    end if;
    if not exists (select 1 from pg_constraint where conname = 'products_cover_width_chk') then
        alter table public.products
            add constraint products_cover_width_chk
            check (cover_width is null or cover_width > 0);
    end if;
    if not exists (select 1 from pg_constraint where conname = 'products_cover_height_chk') then
        alter table public.products
            add constraint products_cover_height_chk
            check (cover_height is null or cover_height > 0);
    end if;
end $$;

create index if not exists products_cover_status_idx
    on public.products (cover_status);

create index if not exists products_cover_priority_idx
    on public.products (cover_priority desc, updated_at desc);

create table if not exists public.product_cover_candidates (
    id uuid primary key default gen_random_uuid(),
    product_id uuid,
    shop_id uuid,
    ean text,
    product_url text,
    image_url text,
    source_type text not null default 'unknown',
    source_rank integer not null default 0,
    is_primary boolean not null default false,
    mime_type text,
    width integer,
    height integer,
    candidate_status text not null default 'pending',
    discovered_at timestamptz not null default now(),
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    last_checked_at timestamptz,
    last_http_status integer,
    last_error_code text,
    last_error_message text,
    sha256 text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table if exists public.product_cover_candidates
    add column if not exists product_id uuid,
    add column if not exists shop_id uuid,
    add column if not exists ean text,
    add column if not exists product_url text,
    add column if not exists image_url text,
    add column if not exists source_type text,
    add column if not exists source_rank integer not null default 0,
    add column if not exists is_primary boolean not null default false,
    add column if not exists mime_type text,
    add column if not exists width integer,
    add column if not exists height integer,
    add column if not exists candidate_status text,
    add column if not exists discovered_at timestamptz not null default now(),
    add column if not exists first_seen_at timestamptz not null default now(),
    add column if not exists last_seen_at timestamptz not null default now(),
    add column if not exists last_checked_at timestamptz,
    add column if not exists last_http_status integer,
    add column if not exists last_error_code text,
    add column if not exists last_error_message text,
    add column if not exists sha256 text,
    add column if not exists created_at timestamptz not null default now(),
    add column if not exists updated_at timestamptz not null default now();

do $$
declare
    r record;
begin
    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'product_cover_candidates' and column_name = 'candidate_status'
    ) then
        for r in
            select conname
            from pg_constraint
            where conrelid = 'public.product_cover_candidates'::regclass
              and contype = 'c'
              and pg_get_constraintdef(oid) ilike '%candidate_status%'
        loop
            execute format('alter table public.product_cover_candidates drop constraint if exists %I', r.conname);
        end loop;
    end if;

    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'product_cover_candidates' and column_name = 'source_type'
    ) then
        update public.product_cover_candidates
           set source_type = coalesce(nullif(source_type, ''), 'unknown')
         where source_type is null or source_type = '';
        alter table public.product_cover_candidates alter column source_type set default 'unknown';
        alter table public.product_cover_candidates alter column source_type set not null;
    end if;

    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'product_cover_candidates' and column_name = 'candidate_status'
    ) then
        update public.product_cover_candidates
           set candidate_status = 'pending'
         where candidate_status is null
            or btrim(candidate_status) = ''
            or candidate_status not in ('pending', 'accepted', 'rejected', 'failed', 'published');
        alter table public.product_cover_candidates alter column candidate_status set default 'pending';
        alter table public.product_cover_candidates alter column candidate_status set not null;
    end if;

    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'product_cover_candidates' and column_name = 'width'
    ) then
        update public.product_cover_candidates set width = null where width is not null and width <= 0;
    end if;

    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'product_cover_candidates' and column_name = 'height'
    ) then
        update public.product_cover_candidates set height = null where height is not null and height <= 0;
    end if;

    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'product_cover_candidates' and column_name = 'candidate_status'
    ) then
        alter table public.product_cover_candidates
            add constraint product_cover_candidates_status_chk
            check (candidate_status in ('pending', 'accepted', 'rejected', 'failed', 'published'));
    end if;

    if not exists (select 1 from pg_constraint where conname = 'product_cover_candidates_width_chk') then
        alter table public.product_cover_candidates
            add constraint product_cover_candidates_width_chk
            check (width is null or width > 0);
    end if;

    if not exists (select 1 from pg_constraint where conname = 'product_cover_candidates_height_chk') then
        alter table public.product_cover_candidates
            add constraint product_cover_candidates_height_chk
            check (height is null or height > 0);
    end if;
end $$;

with ranked as (
    select ctid,
           row_number() over (
               partition by product_id, image_url
               order by coalesce(updated_at, created_at, now()) desc, ctid desc
           ) as rn
    from public.product_cover_candidates
    where product_id is not null and image_url is not null
)
delete from public.product_cover_candidates c
using ranked r
where c.ctid = r.ctid
  and r.rn > 1;

create unique index if not exists product_cover_candidates_product_image_key
    on public.product_cover_candidates (product_id, image_url)
    where product_id is not null and image_url is not null;

create index if not exists product_cover_candidates_product_idx
    on public.product_cover_candidates (product_id, source_rank desc, last_seen_at desc);

create index if not exists product_cover_candidates_shop_idx
    on public.product_cover_candidates (shop_id, last_seen_at desc);

create index if not exists product_cover_candidates_status_idx
    on public.product_cover_candidates (candidate_status, updated_at desc);

create table if not exists public.product_cover_queue (
    id uuid primary key default gen_random_uuid(),
    product_id uuid,
    priority integer not null default 0,
    candidate_count integer not null default 0,
    source_reason text,
    status text not null default 'pending',
    attempt_count integer not null default 0,
    last_error_code text,
    last_error_message text,
    claimed_by text,
    claimed_at timestamptz,
    next_attempt_at timestamptz,
    last_completed_at timestamptz,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

alter table if exists public.product_cover_queue
    add column if not exists product_id uuid,
    add column if not exists priority integer not null default 0,
    add column if not exists candidate_count integer not null default 0,
    add column if not exists source_reason text,
    add column if not exists status text,
    add column if not exists attempt_count integer not null default 0,
    add column if not exists last_error_code text,
    add column if not exists last_error_message text,
    add column if not exists claimed_by text,
    add column if not exists claimed_at timestamptz,
    add column if not exists next_attempt_at timestamptz,
    add column if not exists last_completed_at timestamptz,
    add column if not exists created_at timestamptz not null default now(),
    add column if not exists updated_at timestamptz not null default now();

do $$
begin
    if exists (
        select 1 from information_schema.columns
        where table_schema = 'public' and table_name = 'product_cover_queue' and column_name = 'status'
    ) then
        update public.product_cover_queue
           set status = 'pending'
         where status is null
            or status = ''
            or status not in ('pending', 'processing', 'published', 'failed', 'review', 'retry_later');
        alter table public.product_cover_queue alter column status set default 'pending';
    end if;
end $$;

do $$
begin
    if not exists (select 1 from pg_constraint where conname = 'product_cover_queue_status_chk') then
        alter table public.product_cover_queue
            add constraint product_cover_queue_status_chk
            check (status in ('pending', 'processing', 'published', 'failed', 'review', 'retry_later'));
    end if;
end $$;

with ranked as (
    select ctid,
           row_number() over (
               partition by product_id
               order by coalesce(updated_at, created_at, now()) desc, ctid desc
           ) as rn
    from public.product_cover_queue
    where product_id is not null
)
delete from public.product_cover_queue q
using ranked r
where q.ctid = r.ctid
  and r.rn > 1;

create unique index if not exists product_cover_queue_product_id_key
    on public.product_cover_queue (product_id)
    where product_id is not null;

create index if not exists product_cover_queue_status_priority_idx
    on public.product_cover_queue (status, priority desc, updated_at asc);

create index if not exists product_cover_queue_next_attempt_idx
    on public.product_cover_queue (next_attempt_at asc nulls first, status);

drop view if exists public.cover_candidates_missing_v1;
create view public.cover_candidates_missing_v1 as
select
    p.id,
    p.ean,
    p.artist,
    p.title,
    p.format_label,
    p.cover_status,
    p.cover_priority,
    q.status as queue_status,
    q.candidate_count,
    q.attempt_count,
    q.last_error_code,
    q.last_error_message,
    q.updated_at as queue_updated_at,
    max(pr.last_seen_at) as latest_seen_at,
    count(distinct pr.shop_id) filter (where coalesce(pr.is_active, true)) as active_offer_count
from public.products p
left join public.product_cover_queue q
    on q.product_id = p.id
left join public.prices pr
    on pr.product_id = p.id
where coalesce(nullif(p.cover_storage_path, ''), '') = ''
  and coalesce(nullif(p.cover_url, ''), '') = ''
  and p.ean is not null
group by p.id, p.ean, p.artist, p.title, p.format_label, p.cover_status, p.cover_priority,
         q.status, q.candidate_count, q.attempt_count, q.last_error_code, q.last_error_message, q.updated_at;

drop view if exists public.cover_candidates_failed_review_v1;
create view public.cover_candidates_failed_review_v1 as
select
    p.id,
    p.ean,
    p.artist,
    p.title,
    p.format_label,
    p.cover_status,
    p.cover_priority,
    p.cover_last_attempt_at,
    q.status as queue_status,
    q.attempt_count,
    q.last_error_code,
    q.last_error_message,
    q.updated_at as queue_updated_at
from public.products p
join public.product_cover_queue q
    on q.product_id = p.id
where q.status in ('failed', 'review', 'retry_later')
order by p.cover_priority desc, q.updated_at desc;

comment on table public.product_cover_candidates is 'Observed webshop image candidates per product. Final publication happens only through the central cover worker.';
comment on table public.product_cover_queue is 'Queue table for the central Vinylofy cover pipeline.';
comment on view public.cover_candidates_missing_v1 is 'Products without a stored cover asset yet, including queue and freshness context.';
comment on view public.cover_candidates_failed_review_v1 is 'Products whose cover acquisition failed or needs review.';

commit;
