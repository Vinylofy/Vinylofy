-- Restore claim_next_cover_job to its pre-fix definition.

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
