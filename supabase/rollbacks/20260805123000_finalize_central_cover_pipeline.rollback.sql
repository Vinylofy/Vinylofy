begin;

-- Vinylofy centrale coverpipeline — niet-destructieve compatibiliteitsrollback.
--
-- Forward migration:
--   supabase/migrations/20260805123000_finalize_central_cover_pipeline.sql
--   SHA256 872aed286523d3b5cb32b11db2c0d901a365a81d60ccb9437097f7d854d9d1be
--
-- Deze rollback:
-- - voert geen Storage-mutaties uit;
-- - herstelt de pre-migration database-API en legacy queuekolommen;
-- - behoudt nieuwe metadata om verlies van na-migration data te voorkomen;
-- - reconstrueert legacywaarden zo veilig mogelijk uit canonieke waarden.
--
-- Voor exacte historische veldwaarden moet de vooraf gemaakte export van
-- scripts/maintenance/export_cover_pipeline_pre_migration.py worden gebruikt.

do $$
begin
  if to_regclass('public.products') is null
     or to_regclass('public.product_cover_candidates') is null
     or to_regclass('public.product_cover_queue') is null
     or to_regclass('public.release_calendar') is null then
    raise exception 'Vereiste centrale covertabellen ontbreken';
  end if;

  if not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'products'
      and column_name = 'cover_sha256'
  ) then
    raise exception 'Forward migration lijkt niet toegepast: cover_sha256 ontbreekt';
  end if;

  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'product_cover_queue'
      and column_name = 'state'
  ) then
    raise exception 'Legacy queuekolommen bestaan al; rollback wordt niet herhaald';
  end if;
end
$$;

-- Herstel extern covergedrag voor de oude frontend zonder een inmiddels
-- gepubliceerde lokale URL te overschrijven.
update public.products
set
  cover_url = cover_source_url,
  cover_status = case
    when cover_status = 'blocked' then 'review'
    when nullif(btrim(cover_source_url), '') is not null then 'ready'
    else cover_status
  end,
  updated_at = now()
where nullif(btrim(cover_url), '') is null
  and nullif(btrim(cover_source_url), '') is not null
  and cover_source_url ~* '^https?://';

update public.products
set
  cover_status = 'review',
  updated_at = now()
where cover_status = 'blocked';

alter table public.products
  drop constraint if exists products_cover_ready_storage_chk,
  drop constraint if exists products_cover_status_chk,
  drop constraint if exists products_cover_confidence_chk;

alter table public.products
  alter column cover_confidence type numeric(5,4)
  using (
    case
      when cover_confidence is null then null
      else least(
        1.0000,
        greatest(0.0000, cover_confidence::numeric / 100.0)
      )
    end
  );

alter table public.products
  add constraint products_cover_status_chk
    check (
      cover_status in (
        'missing',
        'queued',
        'resolving',
        'ready',
        'failed',
        'review'
      )
    ),
  add constraint products_cover_confidence_chk
    check (
      cover_confidence is null
      or cover_confidence between 0 and 100
    );

drop index if exists public.products_cover_localization_idx;
drop index if exists public.product_cover_queue_claim_v2_idx;
drop index if exists public.product_cover_queue_stale_claim_idx;

-- Herstel de volledige legacy queue-interface. Canonieke kolommen blijven
-- bestaan, zoals in de werkelijke pre-migration situatie na beide historische
-- covermigrations.
alter table public.product_cover_queue
  add column if not exists ean text,
  add column if not exists trigger_source text,
  add column if not exists requested_priority integer,
  add column if not exists requested_by text,
  add column if not exists request_count integer,
  add column if not exists state text,
  add column if not exists available_at timestamptz,
  add column if not exists attempts integer,
  add column if not exists locked_at timestamptz,
  add column if not exists locked_by text,
  add column if not exists last_requested_at timestamptz;

update public.product_cover_queue q
set
  ean = p.ean,
  trigger_source = coalesce(
    nullif(btrim(q.source_reason), ''),
    'rollback'
  ),
  requested_priority = greatest(0, coalesce(q.priority, 0)),
  request_count = greatest(1, coalesce(q.attempt_count, 0)),
  state = case
    when q.status = 'processing' then 'processing'
    when q.status = 'published' then 'done'
    when q.status = 'failed' then 'failed'
    when q.status = 'review' then 'review'
    else 'pending'
  end,
  available_at = coalesce(q.next_attempt_at, now()),
  attempts = greatest(0, coalesce(q.attempt_count, 0)),
  locked_at = q.claimed_at,
  locked_by = q.claimed_by,
  last_requested_at = coalesce(q.updated_at, q.created_at, now())
from public.products p
where p.id = q.product_id;

alter table public.product_cover_queue
  alter column trigger_source set default 'rollback',
  alter column trigger_source set not null,
  alter column requested_priority set default 0,
  alter column requested_priority set not null,
  alter column request_count set default 1,
  alter column request_count set not null,
  alter column state set default 'pending',
  alter column state set not null,
  alter column available_at set default now(),
  alter column available_at set not null,
  alter column attempts set default 0,
  alter column attempts set not null,
  alter column last_requested_at set default now(),
  alter column last_requested_at set not null;

alter table public.product_cover_queue
  drop constraint if exists product_cover_queue_state_chk;

alter table public.product_cover_queue
  add constraint product_cover_queue_state_chk
    check (
      state in ('pending', 'processing', 'done', 'failed', 'review')
    );

create index if not exists product_cover_queue_claim_idx
  on public.product_cover_queue (
    state,
    available_at,
    requested_priority desc,
    last_requested_at asc
  );

create index if not exists product_cover_queue_locked_idx
  on public.product_cover_queue (state, locked_at);

-- De centrale stale-claimfunctie hoort niet bij de historische API.
drop function if exists public.recover_stale_cover_claims(interval);

-- Historische queue-RPC's.
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
  set
    cover_status = case
      when p.cover_url is not null
       and p.cover_needs_refresh is false
        then p.cover_status
      else 'queued'
    end,
    cover_priority = greatest(
      coalesce(p.cover_priority, 0),
      _priority_bump
    ),
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
  set
    ean = excluded.ean,
    trigger_source = excluded.trigger_source,
    requested_priority = greatest(
      public.product_cover_queue.requested_priority,
      excluded.requested_priority
    ),
    requested_by = excluded.requested_by,
    state = case
      when public.product_cover_queue.state = 'processing'
        then public.product_cover_queue.state
      else 'pending'
    end,
    available_at = case
      when public.product_cover_queue.state = 'processing'
        then public.product_cover_queue.available_at
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
    select public.normalize_cover_ean(input.value)
    from unnest(_eans) as input(value)
  );

  return public.queue_cover_for_products(
    _product_ids,
    _source,
    _priority_bump,
    _requested_by
  );
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
      on public.normalize_cover_ean(p.ean)
       = public.normalize_cover_ean(s.ean)
    where s.batch_name = _batch_name
  ),
  product_updates as (
    update public.products p
    set
      cover_status = case
        when p.cover_url is not null
         and p.cover_needs_refresh is false
          then p.cover_status
        else 'queued'
      end,
      cover_priority = greatest(
        coalesce(p.cover_priority, 0),
        m.requested_priority
      ),
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
  set
    ean = excluded.ean,
    trigger_source = excluded.trigger_source,
    requested_priority = greatest(
      public.product_cover_queue.requested_priority,
      excluded.requested_priority
    ),
    requested_by = excluded.requested_by,
    state = case
      when public.product_cover_queue.state = 'processing'
        then public.product_cover_queue.state
      else 'pending'
    end,
    available_at = case
      when public.product_cover_queue.state = 'processing'
        then public.product_cover_queue.available_at
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
    join public.products p
      on p.id = q.product_id
    where q.state = 'pending'
      and q.available_at <= now()
      and (
        p.cover_url is null
        or p.cover_needs_refresh is true
        or p.cover_status in ('missing', 'queued', 'failed', 'review')
      )
    order by
      q.requested_priority desc,
      q.last_requested_at asc,
      q.created_at asc
    limit 1
    for update of q skip locked
  ),
  queue_update as (
    update public.product_cover_queue q
    set
      state = 'processing',
      locked_at = now(),
      locked_by = _worker_id,
      attempts = q.attempts + 1,
      updated_at = now()
    from next_job n
    where q.id = n.id
    returning
      q.id,
      q.product_id,
      q.ean,
      q.trigger_source,
      q.requested_priority,
      q.attempts
  ),
  product_update as (
    update public.products p
    set
      cover_status = 'resolving',
      cover_last_attempt_at = now(),
      cover_locked_at = now(),
      cover_locked_by = _worker_id,
      updated_at = now()
    from queue_update q
    where p.id = q.product_id
    returning
      p.id,
      p.artist,
      p.title,
      p.format_label
  )
  select
    q.id,
    q.product_id,
    q.ean,
    p.artist,
    p.title,
    p.format_label,
    q.trigger_source,
    q.requested_priority,
    q.attempts
  from queue_update q
  join product_update p
    on p.id = q.product_id;
end
$$;

-- Herstel de werkelijke gemengde pre-migration viewstaat:
-- management/priority uit MVP en missing/failed uit de latere v1-migration.
create or replace view public.cover_management_status_v1 as
select
  p.cover_status,
  count(*) as product_count,
  count(*) filter (
    where p.cover_url is not null
  ) as with_cover_count,
  count(*) filter (
    where p.cover_url is null
  ) as without_cover_count
from public.products p
group by p.cover_status
order by p.cover_status;

create or replace view public.cover_candidates_missing_v1 as
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
  count(distinct pr.shop_id) filter (
    where coalesce(pr.is_active, true)
  ) as active_offer_count
from public.products p
left join public.product_cover_queue q
  on q.product_id = p.id
left join public.prices pr
  on pr.product_id = p.id
where coalesce(nullif(p.cover_storage_path, ''), '') = ''
  and coalesce(nullif(p.cover_url, ''), '') = ''
  and p.ean is not null
group by
  p.id,
  p.ean,
  p.artist,
  p.title,
  p.format_label,
  p.cover_status,
  p.cover_priority,
  q.status,
  q.candidate_count,
  q.attempt_count,
  q.last_error_code,
  q.last_error_message,
  q.updated_at;

create or replace view public.cover_candidates_failed_review_v1 as
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
order by
  p.cover_priority desc,
  q.updated_at desc;

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
order by
  p.cover_priority desc,
  p.updated_at desc;

-- Herstel voor de oude Nieuwe Releases-weergave alleen ontbrekende image_url.
-- Nieuwe releasekolommen blijven behouden om na-migration data niet te verliezen.
update public.release_calendar
set
  image_url = image_source_url,
  updated_at = now()
where nullif(btrim(image_url), '') is null
  and nullif(btrim(image_source_url), '') is not null
  and image_source_url ~* '^https?://';

comment on table public.product_cover_queue is
  'Queue table for the central Vinylofy cover pipeline.';

comment on view public.cover_candidates_missing_v1 is
  'Products without a stored cover asset yet, including queue and freshness context.';

comment on view public.cover_candidates_failed_review_v1 is
  'Products whose cover acquisition failed or needs review.';

comment on view public.cover_management_status_v1 is
  'Historisch coverstatusoverzicht op basis van cover_url.';

comment on view public.cover_priority_candidates_v1 is
  'Historische prioriteitslijst voor ontbrekende of te verversen covers.';

commit;
