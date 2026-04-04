begin;

-- 1) product-level cover fields
alter table public.products
  add column if not exists cover_storage_path text,
  add column if not exists cover_source text,
  add column if not exists cover_source_url text,
  add column if not exists cover_status text not null default 'missing',
  add column if not exists cover_confidence numeric(5,4),
  add column if not exists cover_priority integer not null default 0,
  add column if not exists cover_mbid uuid,
  add column if not exists cover_last_attempt_at timestamptz,
  add column if not exists cover_last_success_at timestamptz,
  add column if not exists cover_fail_count integer not null default 0,
  add column if not exists cover_needs_refresh boolean not null default false,
  add column if not exists cover_locked_at timestamptz,
  add column if not exists cover_locked_by text,
  add column if not exists cover_error_code text,
  add column if not exists cover_error_message text;

alter table public.products
  drop constraint if exists products_cover_status_chk;

alter table public.products
  add constraint products_cover_status_chk
  check (cover_status in ('missing', 'queued', 'resolving', 'ready', 'failed', 'review'));

create index if not exists products_cover_status_priority_idx
  on public.products (cover_status, cover_priority desc, updated_at desc);

create index if not exists products_cover_queueable_idx
  on public.products (cover_priority desc, id)
  where (
    cover_url is null
    or cover_needs_refresh is true
    or cover_status in ('missing', 'queued', 'failed', 'review')
  );

create index if not exists products_cover_mbid_idx
  on public.products (cover_mbid);

-- 2) queue table
create table if not exists public.product_cover_queue (
  id uuid primary key default gen_random_uuid(),
  product_id uuid not null references public.products(id) on delete cascade,
  ean text,
  trigger_source text not null,
  requested_priority integer not null default 0,
  requested_by text,
  request_count integer not null default 1,
  state text not null default 'pending',
  available_at timestamptz not null default now(),
  attempts integer not null default 0,
  locked_at timestamptz,
  locked_by text,
  last_requested_at timestamptz not null default now(),
  last_error_code text,
  last_error_message text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint product_cover_queue_product_id_key unique (product_id),
  constraint product_cover_queue_state_chk
    check (state in ('pending', 'processing', 'done', 'failed', 'review'))
);

create index if not exists product_cover_queue_claim_idx
  on public.product_cover_queue (state, available_at, requested_priority desc, last_requested_at asc);

create index if not exists product_cover_queue_locked_idx
  on public.product_cover_queue (state, locked_at);

-- 3) manual preload staging
create table if not exists public.cover_preload_stage (
  id bigint generated always as identity primary key,
  batch_name text not null,
  ean text not null,
  requested_priority integer not null default 5000,
  source text not null default 'manual_seed',
  note text,
  queued_at timestamptz,
  created_at timestamptz not null default now()
);

create unique index if not exists cover_preload_stage_batch_ean_idx
  on public.cover_preload_stage (batch_name, ean);

-- 4) helper functions
create or replace function public.normalize_cover_ean(value text)
returns text
language plpgsql
immutable
as $$
declare
  digits text;
begin
  digits := regexp_replace(coalesce(value, ''), '\D', '', 'g');
  if digits = '' then
    return null;
  end if;

  if length(digits) = 11 then
    digits := '0' || digits;
  end if;

  if length(digits) not in (8, 12, 13, 14) then
    return null;
  end if;

  return digits;
end
$$;

create or replace function public.queue_cover_for_products(
  _product_ids uuid[],
  _source text default 'system',
  _priority_bump integer default 1000,
  _requested_by text default null
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  _queued_count integer := 0;
begin
  if coalesce(array_length(_product_ids, 1), 0) = 0 then
    return 0;
  end if;

  update public.products p
     set cover_status = case
                          when p.cover_url is not null and p.cover_needs_refresh is false then p.cover_status
                          else 'queued'
                        end,
         cover_priority = greatest(coalesce(p.cover_priority, 0), _priority_bump),
         cover_locked_at = null,
         cover_locked_by = null,
         updated_at = now()
   where p.id = any(_product_ids)
     and (
       p.cover_url is null
       or p.cover_needs_refresh is true
       or p.cover_status in ('missing', 'queued', 'failed', 'review')
     );

  insert into public.product_cover_queue (
    product_id,
    ean,
    trigger_source,
    requested_priority,
    requested_by,
    state,
    available_at,
    last_requested_at,
    updated_at
  )
  select
    p.id,
    p.ean,
    _source,
    greatest(coalesce(p.cover_priority, 0), _priority_bump),
    _requested_by,
    'pending',
    now(),
    now(),
    now()
  from public.products p
  where p.id = any(_product_ids)
    and (
      p.cover_url is null
      or p.cover_needs_refresh is true
      or p.cover_status in ('missing', 'queued', 'failed', 'review')
    )
  on conflict (product_id) do update
    set ean = excluded.ean,
        trigger_source = excluded.trigger_source,
        requested_priority = greatest(public.product_cover_queue.requested_priority, excluded.requested_priority),
        requested_by = excluded.requested_by,
        state = case
                  when public.product_cover_queue.state = 'processing' then public.product_cover_queue.state
                  else 'pending'
                end,
        available_at = case
                         when public.product_cover_queue.state = 'processing' then public.product_cover_queue.available_at
                         else now()
                       end,
        request_count = public.product_cover_queue.request_count + 1,
        last_requested_at = now(),
        updated_at = now();

  get diagnostics _queued_count = row_count;
  return _queued_count;
end
$$;

create or replace function public.queue_cover_for_eans(
  _eans text[],
  _source text default 'system',
  _priority_bump integer default 1000,
  _requested_by text default null
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  _product_ids uuid[];
begin
  if coalesce(array_length(_eans, 1), 0) = 0 then
    return 0;
  end if;

  select array_agg(p.id)
    into _product_ids
  from public.products p
  where public.normalize_cover_ean(p.ean) = any (
    select public.normalize_cover_ean(x)
    from unnest(_eans) as x
  );

  return public.queue_cover_for_products(_product_ids, _source, _priority_bump, _requested_by);
end
$$;

create or replace function public.apply_cover_preload_batch(
  _batch_name text,
  _requested_by text default 'seed-script'
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  _queued_count integer := 0;
begin
  with matched as (
    select
      s.batch_name,
      s.ean,
      s.requested_priority,
      s.source,
      p.id as product_id
    from public.cover_preload_stage s
    join public.products p
      on public.normalize_cover_ean(p.ean) = public.normalize_cover_ean(s.ean)
    where s.batch_name = _batch_name
  ),
  product_updates as (
    update public.products p
       set cover_status = case
                            when p.cover_url is not null and p.cover_needs_refresh is false then p.cover_status
                            else 'queued'
                          end,
           cover_priority = greatest(coalesce(p.cover_priority, 0), m.requested_priority),
           cover_locked_at = null,
           cover_locked_by = null,
           updated_at = now()
      from matched m
     where p.id = m.product_id
       and (
         p.cover_url is null
         or p.cover_needs_refresh is true
         or p.cover_status in ('missing', 'queued', 'failed', 'review')
       )
    returning p.id
  )
  insert into public.product_cover_queue (
    product_id,
    ean,
    trigger_source,
    requested_priority,
    requested_by,
    state,
    available_at,
    last_requested_at,
    updated_at
  )
  select
    m.product_id,
    m.ean,
    m.source,
    m.requested_priority,
    _requested_by,
    'pending',
    now(),
    now(),
    now()
  from matched m
  on conflict (product_id) do update
    set ean = excluded.ean,
        trigger_source = excluded.trigger_source,
        requested_priority = greatest(public.product_cover_queue.requested_priority, excluded.requested_priority),
        requested_by = excluded.requested_by,
        state = case
                  when public.product_cover_queue.state = 'processing' then public.product_cover_queue.state
                  else 'pending'
                end,
        available_at = case
                         when public.product_cover_queue.state = 'processing' then public.product_cover_queue.available_at
                         else now()
                       end,
        request_count = public.product_cover_queue.request_count + 1,
        last_requested_at = now(),
        updated_at = now();

  get diagnostics _queued_count = row_count;

  update public.cover_preload_stage
     set queued_at = now()
   where batch_name = _batch_name
     and queued_at is null;

  return _queued_count;
end
$$;

-- 5) queue claim helper for worker
create or replace function public.claim_next_cover_job(_worker_id text)
returns table (
  queue_id uuid,
  product_id uuid,
  ean text,
  artist text,
  title text,
  format_label text,
  trigger_source text,
  requested_priority integer,
  attempts integer
)
language plpgsql
security definer
set search_path = public
as $$
begin
  return query
  with next_job as (
    select q.id
    from public.product_cover_queue q
    join public.products p on p.id = q.product_id
    where q.state = 'pending'
      and q.available_at <= now()
      and (
        p.cover_url is null
        or p.cover_needs_refresh is true
        or p.cover_status in ('missing', 'queued', 'failed', 'review')
      )
    order by q.requested_priority desc, q.last_requested_at asc, q.created_at asc
    limit 1
    for update skip locked
  ),
  q_upd as (
    update public.product_cover_queue q
       set state = 'processing',
           locked_at = now(),
           locked_by = _worker_id,
           attempts = q.attempts + 1,
           updated_at = now()
      from next_job nj
     where q.id = nj.id
    returning q.id, q.product_id, q.ean, q.trigger_source, q.requested_priority, q.attempts
  ),
  p_upd as (
    update public.products p
       set cover_status = 'resolving',
           cover_last_attempt_at = now(),
           cover_locked_at = now(),
           cover_locked_by = _worker_id,
           updated_at = now()
      from q_upd
     where p.id = q_upd.product_id
    returning p.id, p.artist, p.title, p.format_label
  )
  select
    q_upd.id,
    q_upd.product_id,
    q_upd.ean,
    p_upd.artist,
    p_upd.title,
    p_upd.format_label,
    q_upd.trigger_source,
    q_upd.requested_priority,
    q_upd.attempts
  from q_upd
  join p_upd on p_upd.id = q_upd.product_id;
end
$$;

-- 6) operational views for exports/dashboard
create or replace view public.cover_management_status_v1 as
select
  cover_status,
  count(*) as product_count,
  count(*) filter (where cover_url is not null) as with_cover_count,
  count(*) filter (where cover_url is null) as without_cover_count
from public.products
group by cover_status
order by cover_status;

create or replace view public.cover_candidates_missing_v1 as
select
  p.id,
  p.ean,
  p.artist,
  p.title,
  p.format_label,
  p.cover_status,
  p.cover_priority,
  p.cover_fail_count,
  p.cover_last_attempt_at,
  p.cover_error_code,
  p.cover_error_message
from public.products p
where p.cover_url is null
  and p.cover_status in ('missing', 'queued', 'resolving')
order by p.cover_priority desc, p.updated_at desc;

create or replace view public.cover_candidates_failed_review_v1 as
select
  p.id,
  p.ean,
  p.artist,
  p.title,
  p.format_label,
  p.cover_status,
  p.cover_priority,
  p.cover_fail_count,
  p.cover_last_attempt_at,
  q.last_error_code,
  q.last_error_message,
  q.updated_at as queue_updated_at
from public.products p
left join public.product_cover_queue q
  on q.product_id = p.id
where p.cover_status in ('failed', 'review')
order by p.cover_priority desc, p.cover_last_attempt_at desc nulls last;

create or replace view public.cover_priority_candidates_v1 as
select
  p.id,
  p.ean,
  p.artist,
  p.title,
  p.format_label,
  p.cover_status,
  p.cover_priority,
  p.cover_fail_count,
  p.cover_last_success_at,
  p.cover_needs_refresh
from public.products p
where p.cover_url is null
   or p.cover_needs_refresh is true
order by p.cover_priority desc, p.updated_at desc;

-- 7) storage bucket
insert into storage.buckets (id, name, public, file_size_limit, allowed_mime_types)
values (
  'product-covers',
  'product-covers',
  true,
  5242880,
  array['image/jpeg', 'image/png', 'image/webp']
)
on conflict (id) do update
  set public = excluded.public,
      file_size_limit = excluded.file_size_limit,
      allowed_mime_types = excluded.allowed_mime_types;

commit;
