begin;

-- Definitieve centrale Vinylofy-coverpipeline.
-- Deze migration publiceert geen binaries en voert geen Storage-mutaties uit.
-- Pre-migration database-export en exact Storage-manifest zijn vereist vóór
-- toepassing buiten een disposable testdatabase.

do $$
begin
  if to_regclass('public.products') is null then
    raise exception 'Vereiste tabel public.products ontbreekt';
  end if;

  if to_regclass('public.product_cover_candidates') is null then
    raise exception 'Vereiste tabel public.product_cover_candidates ontbreekt';
  end if;

  if to_regclass('public.product_cover_queue') is null then
    raise exception 'Vereiste tabel public.product_cover_queue ontbreekt';
  end if;

  if to_regclass('public.release_calendar') is null then
    raise exception 'Vereiste tabel public.release_calendar ontbreekt';
  end if;
end
$$;

-- Lokale Storage is de enige gepubliceerde productcoverautoriteit.
alter table public.products
  add column if not exists cover_sha256 text,
  add column if not exists cover_mime_type text,
  add column if not exists cover_byte_size bigint;

-- Externe product-URL's blijven bronmetadata en gelden niet als lokale cover.
update public.products
set
  cover_source_url = coalesce(
    nullif(btrim(cover_source_url), ''),
    nullif(btrim(cover_url), '')
  ),
  cover_url = null,
  cover_status = case
    when cover_status = 'blocked' then 'blocked'
    when nullif(btrim(cover_storage_path), '') is not null then cover_status
    else 'missing'
  end,
  updated_at = now()
where nullif(btrim(cover_url), '') is not null
  and cover_url ~* '^https?://';

update public.products
set
  cover_status = case
    when cover_status = 'blocked' then 'blocked'
    when nullif(btrim(cover_storage_path), '') is not null then 'ready'
    else 'missing'
  end
where cover_status is null
   or cover_status not in (
     'missing', 'queued', 'resolving', 'ready', 'failed', 'review', 'blocked'
   );

update public.products
set
  cover_sha256 = case
    when cover_sha256 ~ '^[0-9a-f]{64}$' then cover_sha256
    else null
  end,
  cover_mime_type = case
    when cover_mime_type in ('image/jpeg', 'image/png', 'image/webp')
      then cover_mime_type
    else null
  end,
  cover_byte_size = case
    when cover_byte_size > 0 then cover_byte_size
    else null
  end;

alter table public.products
  drop constraint if exists products_cover_status_chk,
  drop constraint if exists products_cover_sha256_chk,
  drop constraint if exists products_cover_mime_type_chk,
  drop constraint if exists products_cover_byte_size_chk;

alter table public.products
  add constraint products_cover_status_chk
    check (
      cover_status in (
        'missing',
        'queued',
        'resolving',
        'ready',
        'failed',
        'review',
        'blocked'
      )
    ),
  add constraint products_cover_sha256_chk
    check (
      cover_sha256 is null
      or cover_sha256 ~ '^[0-9a-f]{64}$'
    ),
  add constraint products_cover_mime_type_chk
    check (
      cover_mime_type is null
      or cover_mime_type in ('image/jpeg', 'image/png', 'image/webp')
    ),
  add constraint products_cover_byte_size_chk
    check (
      cover_byte_size is null
      or cover_byte_size > 0
    );

create index if not exists products_cover_storage_path_idx
  on public.products (cover_storage_path)
  where nullif(btrim(cover_storage_path), '') is not null;

create index if not exists products_cover_localization_idx
  on public.products (cover_priority desc, id)
  where (
    cover_status <> 'blocked'
    and nullif(btrim(cover_storage_path), '') is null
  );

-- Kandidaten blijven bronmetadata; exact één kandidaat kan per product
-- de gepubliceerde lokale binary vertegenwoordigen.
alter table public.product_cover_candidates
  add column if not exists is_selected boolean not null default false,
  add column if not exists byte_size bigint;

update public.product_cover_candidates
set byte_size = null
where byte_size is not null
  and byte_size <= 0;

with ranked_selected as (
  select
    id,
    row_number() over (
      partition by product_id
      order by
        (candidate_status = 'published') desc,
        updated_at desc nulls last,
        id desc
    ) as selection_rank
  from public.product_cover_candidates
  where product_id is not null
    and is_selected is true
)
update public.product_cover_candidates c
set is_selected = false
from ranked_selected r
where c.id = r.id
  and r.selection_rank > 1;

update public.product_cover_candidates
set candidate_status = 'published'
where is_selected is true
  and candidate_status <> 'published';

alter table public.product_cover_candidates
  drop constraint if exists product_cover_candidates_byte_size_chk,
  drop constraint if exists product_cover_candidates_selected_status_chk;

alter table public.product_cover_candidates
  add constraint product_cover_candidates_byte_size_chk
    check (
      byte_size is null
      or byte_size > 0
    ),
  add constraint product_cover_candidates_selected_status_chk
    check (
      is_selected is false
      or candidate_status = 'published'
    );

create unique index if not exists product_cover_candidates_one_selected_idx
  on public.product_cover_candidates (product_id)
  where product_id is not null and is_selected is true;

comment on column public.products.cover_storage_path is
  'Canoniek objectpad in bucket product-covers; lokale Storage is coverautoriteit.';

comment on column public.products.cover_url is
  'Publieke lokale cover-URL; externe bron-URL hoort in cover_source_url.';

comment on column public.product_cover_candidates.is_selected is
  'Waar voor maximaal één gepubliceerde kandidaat per product.';
-- Versterk de product- en kandidaat-invarianten voordat de queue wordt omgezet.
alter table public.products
  drop constraint if exists products_cover_ready_storage_chk;

alter table public.products
  add constraint products_cover_ready_storage_chk
    check (
      cover_status <> 'ready'
      or nullif(btrim(cover_storage_path), '') is not null
    );

update public.product_cover_candidates
set is_selected = false
where is_selected is true
  and product_id is null;

alter table public.product_cover_candidates
  drop constraint if exists product_cover_candidates_selected_status_chk;

alter table public.product_cover_candidates
  add constraint product_cover_candidates_selected_status_chk
    check (
      is_selected is false
      or (
        product_id is not null
        and candidate_status = 'published'
      )
    );

-- Consolideer de twee historische queuegeneraties naar één canoniek contract.
-- Legacykolommen blijven binnen deze transaction bestaan totdat alle RPC's
-- in een volgend migrationdeel zijn vervangen.
update public.product_cover_queue
set
  priority = greatest(
    0,
    coalesce(priority, 0),
    coalesce(requested_priority, 0)
  ),
  candidate_count = greatest(0, coalesce(candidate_count, 0)),
  source_reason = coalesce(
    nullif(btrim(source_reason), ''),
    nullif(btrim(trigger_source), '')
  ),
  status = case
    when status in ('published', 'failed', 'review', 'retry_later')
      then status
    when status = 'processing' or state = 'processing'
      then 'processing'
    when state = 'done' then 'published'
    when state = 'failed' then 'failed'
    when state = 'review' then 'review'
    else 'pending'
  end,
  attempt_count = greatest(
    0,
    coalesce(attempt_count, 0),
    coalesce(attempts, 0)
  ),
  claimed_by = coalesce(
    nullif(btrim(claimed_by), ''),
    nullif(btrim(locked_by), '')
  ),
  claimed_at = coalesce(claimed_at, locked_at),
  next_attempt_at = coalesce(next_attempt_at, available_at),
  updated_at = greatest(
    coalesce(updated_at, '-infinity'::timestamptz),
    coalesce(last_requested_at, '-infinity'::timestamptz),
    coalesce(created_at, '-infinity'::timestamptz)
  );

alter table public.product_cover_queue
  alter column priority set default 0,
  alter column priority set not null,
  alter column candidate_count set default 0,
  alter column candidate_count set not null,
  alter column status set default 'pending',
  alter column status set not null,
  alter column attempt_count set default 0,
  alter column attempt_count set not null;

alter table public.product_cover_queue
  drop constraint if exists product_cover_queue_status_chk,
  drop constraint if exists product_cover_queue_priority_chk,
  drop constraint if exists product_cover_queue_candidate_count_chk,
  drop constraint if exists product_cover_queue_attempt_count_chk;

alter table public.product_cover_queue
  add constraint product_cover_queue_status_chk
    check (
      status in (
        'pending',
        'processing',
        'published',
        'failed',
        'review',
        'retry_later'
      )
    ),
  add constraint product_cover_queue_priority_chk
    check (priority >= 0),
  add constraint product_cover_queue_candidate_count_chk
    check (candidate_count >= 0),
  add constraint product_cover_queue_attempt_count_chk
    check (attempt_count >= 0);

create index if not exists product_cover_queue_claim_v2_idx
  on public.product_cover_queue (
    status,
    next_attempt_at,
    priority desc,
    updated_at asc,
    id
  );

create index if not exists product_cover_queue_stale_claim_idx
  on public.product_cover_queue (claimed_at, id)
  where status = 'processing';

comment on column public.product_cover_queue.status is
  'Canonieke queuestatus voor de centrale coverworker.';

comment on column public.product_cover_queue.next_attempt_at is
  'Eerstvolgende moment waarop pending of retry_later opnieuw claimbaar is.';

-- Releasekalender: externe bronmetadata en lokale publicatiemetadata worden
-- strikt gescheiden. Gekoppelde regels renderen uitsluitend de productcover.
alter table public.release_calendar
  add column if not exists product_id uuid,
  add column if not exists image_source_url text,
  add column if not exists image_storage_path text,
  add column if not exists image_status text not null default 'missing',
  add column if not exists image_sha256 text,
  add column if not exists image_mime_type text,
  add column if not exists image_byte_size bigint,
  add column if not exists image_last_attempt_at timestamptz,
  add column if not exists image_error_code text,
  add column if not exists image_error_message text;

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conrelid = 'public.release_calendar'::regclass
      and conname = 'release_calendar_product_id_fkey'
  ) then
    alter table public.release_calendar
      add constraint release_calendar_product_id_fkey
      foreign key (product_id)
      references public.products(id)
      on delete set null;
  end if;
end
$$;

-- Iedere externe release-URL wordt uitsluitend bronmetadata.
update public.release_calendar
set
  image_source_url = coalesce(
    nullif(btrim(image_source_url), ''),
    nullif(btrim(image_url), '')
  ),
  image_url = null,
  image_status = case
    when image_status = 'blocked' then 'blocked'
    when nullif(btrim(image_storage_path), '') is not null then 'review'
    else 'missing'
  end,
  updated_at = now()
where nullif(btrim(image_url), '') is not null
  and image_url ~* '^https?://';

update public.release_calendar
set
  image_status = case
    when image_status in (
      'missing',
      'queued',
      'processing',
      'ready',
      'failed',
      'review',
      'retry_later',
      'blocked'
    ) then image_status
    else 'missing'
  end,
  image_sha256 = case
    when image_sha256 ~ '^[0-9a-f]{64}$' then image_sha256
    else null
  end,
  image_mime_type = case
    when image_mime_type in ('image/jpeg', 'image/png', 'image/webp')
      then image_mime_type
    else null
  end,
  image_byte_size = case
    when image_byte_size > 0 then image_byte_size
    else null
  end;

update public.release_calendar
set image_status = 'review'
where image_status = 'ready'
  and (
    nullif(btrim(image_url), '') is null
    or nullif(btrim(image_storage_path), '') is null
  );

alter table public.release_calendar
  alter column image_status set default 'missing',
  alter column image_status set not null;

alter table public.release_calendar
  drop constraint if exists release_calendar_image_status_chk,
  drop constraint if exists release_calendar_image_sha256_chk,
  drop constraint if exists release_calendar_image_mime_type_chk,
  drop constraint if exists release_calendar_image_byte_size_chk,
  drop constraint if exists release_calendar_ready_local_image_chk;

alter table public.release_calendar
  add constraint release_calendar_image_status_chk
    check (
      image_status in (
        'missing',
        'queued',
        'processing',
        'ready',
        'failed',
        'review',
        'retry_later',
        'blocked'
      )
    ),
  add constraint release_calendar_image_sha256_chk
    check (
      image_sha256 is null
      or image_sha256 ~ '^[0-9a-f]{64}$'
    ),
  add constraint release_calendar_image_mime_type_chk
    check (
      image_mime_type is null
      or image_mime_type in ('image/jpeg', 'image/png', 'image/webp')
    ),
  add constraint release_calendar_image_byte_size_chk
    check (
      image_byte_size is null
      or image_byte_size > 0
    ),
  add constraint release_calendar_ready_local_image_chk
    check (
      image_status <> 'ready'
      or (
        nullif(btrim(image_url), '') is not null
        and nullif(btrim(image_storage_path), '') is not null
      )
    );

create index if not exists release_calendar_product_id_idx
  on public.release_calendar (product_id)
  where product_id is not null;

create index if not exists release_calendar_image_localization_idx
  on public.release_calendar (image_status, updated_at, id)
  where (
    product_id is null
    and image_status <> 'blocked'
    and nullif(btrim(image_storage_path), '') is null
    and nullif(btrim(image_source_url), '') is not null
  );

comment on column public.release_calendar.product_id is
  'Enige toegestane koppeling van een releasekalenderrij aan een product.';

comment on column public.release_calendar.image_source_url is
  'Externe bronmetadata; nooit rechtstreeks renderen.';

comment on column public.release_calendar.image_storage_path is
  'Tijdelijk lokaal objectpad voor uitsluitend ongekoppelde releases.';

comment on column public.release_calendar.image_url is
  'Publieke lokale URL voor uitsluitend een ongekoppelde releasecover.';
-- Herstel het historische typeconflict: de MVP maakte cover_confidence
-- numeric(5,4), terwijl de latere worker en constraint 0..100 integers gebruiken.
do $$
declare
  confidence_type text;
begin
  select data_type
    into confidence_type
  from information_schema.columns
  where table_schema = 'public'
    and table_name = 'products'
    and column_name = 'cover_confidence';

  if confidence_type is null then
    raise exception 'Vereiste kolom products.cover_confidence ontbreekt';
  end if;

  if confidence_type not in ('integer', 'numeric') then
    raise exception 'Onverwacht type voor products.cover_confidence: %',
      confidence_type;
  end if;

  alter table public.products
    drop constraint if exists products_cover_confidence_chk;

  if confidence_type = 'numeric' then
    alter table public.products
      alter column cover_confidence type integer
      using (
        case
          when cover_confidence is null then null
          when cover_confidence between 0 and 1
            then round(cover_confidence * 100)::integer
          else least(
            100,
            greatest(0, round(cover_confidence)::integer)
          )
        end
      );
  end if;

  update public.products
  set cover_confidence = least(100, greatest(0, cover_confidence))
  where cover_confidence is not null
    and cover_confidence not between 0 and 100;

  alter table public.products
    add constraint products_cover_confidence_chk
    check (
      cover_confidence is null
      or cover_confidence between 0 and 100
    );
end
$$;

-- Alleen een exacte, unieke genormaliseerde EAN mag een bestaande release
-- aan een product koppelen. Titel of artiest wordt nooit als join gebruikt.
with normalized_products as (
  select
    p.id,
    public.normalize_cover_ean(p.ean) as normalized_ean
  from public.products p
  where public.normalize_cover_ean(p.ean) is not null
),
unique_eans as (
  select normalized_ean
  from normalized_products
  group by normalized_ean
  having count(*) = 1
),
exact_mapping as (
  select
    np.normalized_ean,
    np.id as product_id
  from normalized_products np
  join unique_eans ue
    on ue.normalized_ean = np.normalized_ean
)
update public.release_calendar r
set
  product_id = m.product_id,
  updated_at = now()
from exact_mapping m
where r.product_id is null
  and public.normalize_cover_ean(r.ean) = m.normalized_ean;

-- Alle queue-ingangspunten gebruiken vanaf hier uitsluitend het canonieke
-- statuscontract en slaan blocked of reeds lokaal gepubliceerde producten over.
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
  _safe_source text := coalesce(nullif(btrim(_source), ''), 'system');
begin
  if coalesce(array_length(_product_ids, 1), 0) = 0 then
    return 0;
  end if;

  with eligible as (
    select
      p.id,
      greatest(
        0,
        coalesce(p.cover_priority, 0),
        coalesce(_priority_bump, 0)
      ) as effective_priority
    from public.products p
    where p.id = any(_product_ids)
      and p.cover_status <> 'blocked'
      and nullif(btrim(p.cover_storage_path), '') is null
      and public.normalize_cover_ean(p.ean) is not null
  ),
  queue_upsert as (
    insert into public.product_cover_queue (
      product_id,
      priority,
      source_reason,
      status,
      next_attempt_at,
      updated_at
    )
    select
      e.id,
      e.effective_priority,
      _safe_source,
      'pending',
      now(),
      now()
    from eligible e
    on conflict (product_id) do update
      set priority = greatest(
            public.product_cover_queue.priority,
            excluded.priority
          ),
          source_reason = excluded.source_reason,
          status = case
            when public.product_cover_queue.status = 'processing'
              then 'processing'
            else 'pending'
          end,
          claimed_by = case
            when public.product_cover_queue.status = 'processing'
              then public.product_cover_queue.claimed_by
            else null
          end,
          claimed_at = case
            when public.product_cover_queue.status = 'processing'
              then public.product_cover_queue.claimed_at
            else null
          end,
          next_attempt_at = case
            when public.product_cover_queue.status = 'processing'
              then public.product_cover_queue.next_attempt_at
            else now()
          end,
          last_error_code = case
            when public.product_cover_queue.status = 'processing'
              then public.product_cover_queue.last_error_code
            else null
          end,
          last_error_message = case
            when public.product_cover_queue.status = 'processing'
              then public.product_cover_queue.last_error_message
            else null
          end,
          updated_at = now()
    returning product_id, priority, status
  ),
  product_updates as (
    update public.products p
    set
      cover_status = case
        when q.status = 'processing' then 'resolving'
        else 'queued'
      end,
      cover_priority = greatest(
        coalesce(p.cover_priority, 0),
        q.priority
      ),
      cover_locked_at = case
        when q.status = 'processing' then p.cover_locked_at
        else null
      end,
      cover_locked_by = case
        when q.status = 'processing' then p.cover_locked_by
        else null
      end,
      cover_error_code = case
        when q.status = 'processing' then p.cover_error_code
        else null
      end,
      cover_error_message = case
        when q.status = 'processing' then p.cover_error_message
        else null
      end,
      updated_at = now()
    from queue_upsert q
    where p.id = q.product_id
    returning p.id
  )
  select count(*)::integer
    into _queued_count
  from queue_upsert;

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

  select array_agg(distinct p.id)
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
  batch_group record;
  queued_total integer := 0;
begin
  for batch_group in
    select
      s.requested_priority,
      s.source,
      array_agg(distinct p.id) as product_ids
    from public.cover_preload_stage s
    join public.products p
      on public.normalize_cover_ean(p.ean)
       = public.normalize_cover_ean(s.ean)
    where s.batch_name = _batch_name
    group by s.requested_priority, s.source
  loop
    queued_total := queued_total + public.queue_cover_for_products(
      batch_group.product_ids,
      batch_group.source,
      batch_group.requested_priority,
      _requested_by
    );
  end loop;

  update public.cover_preload_stage
  set queued_at = now()
  where batch_name = _batch_name
    and queued_at is null;

  return queued_total;
end
$$;

create or replace function public.recover_stale_cover_claims(
  _stale_after interval default interval '90 minutes'
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  recovered_count integer := 0;
begin
  if _stale_after is null or _stale_after <= interval '0 seconds' then
    raise exception '_stale_after moet groter zijn dan nul';
  end if;

  with recovered as (
    update public.product_cover_queue q
    set
      status = 'retry_later',
      last_error_code = 'stale_claim_recovered',
      last_error_message = 'Verouderde coverclaim automatisch vrijgegeven.',
      claimed_by = null,
      claimed_at = null,
      next_attempt_at = now(),
      updated_at = now()
    where q.status = 'processing'
      and q.claimed_at is not null
      and q.claimed_at < now() - _stale_after
    returning q.product_id
  ),
  product_updates as (
    update public.products p
    set
      cover_status = case
        when p.cover_status = 'blocked' then 'blocked'
        when nullif(btrim(p.cover_storage_path), '') is not null then 'ready'
        else 'queued'
      end,
      cover_locked_at = null,
      cover_locked_by = null,
      cover_error_code = 'stale_claim_recovered',
      cover_error_message = 'Verouderde coverclaim automatisch vrijgegeven.',
      updated_at = now()
    from recovered r
    where p.id = r.product_id
    returning p.id
  )
  select count(*)::integer
    into recovered_count
  from recovered;

  return recovered_count;
end
$$;

-- Zelfde RPC-signatuur als de MVP voor compatibiliteit, maar volledig gevoed
-- uit de canonieke queuekolommen. Iedere call claimt maximaal één item.
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
  if nullif(btrim(_worker_id), '') is null then
    raise exception '_worker_id mag niet leeg zijn';
  end if;

  perform public.recover_stale_cover_claims(interval '90 minutes');

  return query
  with next_job as (
    select q.id
    from public.product_cover_queue q
    join public.products p
      on p.id = q.product_id
    where q.status in ('pending', 'retry_later', 'review')
      and (
        q.next_attempt_at is null
        or q.next_attempt_at <= now()
      )
      and p.cover_status <> 'blocked'
      and nullif(btrim(p.cover_storage_path), '') is null
      and public.normalize_cover_ean(p.ean) is not null
    order by
      q.priority desc,
      q.updated_at asc,
      q.id
    limit 1
    for update of q skip locked
  ),
  queue_update as (
    update public.product_cover_queue q
    set
      status = 'processing',
      claimed_by = _worker_id,
      claimed_at = now(),
      attempt_count = coalesce(q.attempt_count, 0) + 1,
      updated_at = now()
    from next_job n
    where q.id = n.id
    returning
      q.id,
      q.product_id,
      q.source_reason,
      q.priority,
      q.attempt_count
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
      p.ean,
      p.artist,
      p.title,
      p.format_label
  )
  select
    q.id,
    q.product_id,
    p.ean,
    p.artist,
    p.title,
    p.format_label,
    q.source_reason,
    q.priority,
    q.attempt_count
  from queue_update q
  join product_update p
    on p.id = q.product_id;
end
$$;

-- Nu alle database-ingangspunten canoniek zijn, vervallen de oude
-- state/requested_priority/lock-kolommen.
drop index if exists public.product_cover_queue_claim_idx;
drop index if exists public.product_cover_queue_locked_idx;

alter table public.product_cover_queue
  drop constraint if exists product_cover_queue_state_chk;

alter table public.product_cover_queue
  drop column if exists ean,
  drop column if exists trigger_source,
  drop column if exists requested_priority,
  drop column if exists requested_by,
  drop column if exists request_count,
  drop column if exists state,
  drop column if exists available_at,
  drop column if exists attempts,
  drop column if exists locked_at,
  drop column if exists locked_by,
  drop column if exists last_requested_at;

-- Operationele views gebruiken uitsluitend lokale Storage als autoriteit.
create or replace view public.cover_management_status_v1 as
select
  p.cover_status,
  count(*) as product_count,
  count(*) filter (
    where nullif(btrim(p.cover_storage_path), '') is not null
  ) as with_cover_count,
  count(*) filter (
    where nullif(btrim(p.cover_storage_path), '') is null
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
where nullif(btrim(p.cover_storage_path), '') is null
  and p.cover_status <> 'blocked'
  and public.normalize_cover_ean(p.ean) is not null
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
  and p.cover_status <> 'blocked'
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
where nullif(btrim(p.cover_storage_path), '') is null
  and p.cover_status <> 'blocked'
  and public.normalize_cover_ean(p.ean) is not null
order by
  p.cover_priority desc,
  p.updated_at desc;

comment on function public.recover_stale_cover_claims(interval) is
  'Geeft processing-claims ouder dan standaard 90 minuten veilig vrij.';

comment on function public.claim_next_cover_job(text) is
  'Claimt atomair maximaal één canoniek coverqueue-item met SKIP LOCKED.';

comment on view public.cover_management_status_v1 is
  'Coverstatusoverzicht waarbij uitsluitend cover_storage_path lokaal telt.';

comment on view public.cover_candidates_missing_v1 is
  'Queuebare EAN-producten zonder lokaal Storage-objectpad.';

comment on view public.cover_candidates_failed_review_v1 is
  'Niet-geblokkeerde coverjobs met fout-, review- of retrystatus.';

comment on view public.cover_priority_candidates_v1 is
  'Prioriteitslijst van EAN-producten zonder lokale cover.';

insert into storage.buckets (
  id,
  name,
  public,
  file_size_limit,
  allowed_mime_types
)
values (
  'product-covers',
  'product-covers',
  true,
  5242880,
  array['image/jpeg', 'image/png', 'image/webp']
)
on conflict (id) do update
set
  public = excluded.public,
  file_size_limit = excluded.file_size_limit,
  allowed_mime_types = excluded.allowed_mime_types;

commit;
