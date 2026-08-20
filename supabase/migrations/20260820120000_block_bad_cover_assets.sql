begin;

create table if not exists public.cover_asset_blocklist (
  id uuid primary key default gen_random_uuid(),
  image_url text,
  sha256 text,
  reason_code text not null,
  reason_message text not null,
  created_at timestamptz not null default now(),
  constraint cover_asset_blocklist_identifier_chk check (
    image_url is not null or sha256 is not null
  ),
  constraint cover_asset_blocklist_sha256_chk check (
    sha256 is null or sha256 ~ '^[0-9a-f]{64}$'
  )
);

create unique index if not exists cover_asset_blocklist_url_key
  on public.cover_asset_blocklist (image_url)
  where image_url is not null;

create unique index if not exists cover_asset_blocklist_sha256_key
  on public.cover_asset_blocklist (sha256)
  where sha256 is not null;

insert into public.cover_asset_blocklist (
  image_url, reason_code, reason_message
) values (
  'https://www.platomania.nl/fbmania.png',
  'blocked_retailer_placeholder',
  'Globally blocked retailer placeholder/logo asset.'
)
on conflict (image_url) where image_url is not null do update
set reason_code = excluded.reason_code,
    reason_message = excluded.reason_message;

insert into public.cover_asset_blocklist (
  sha256, reason_code, reason_message
) values (
  '162468189d7fa6d6481f1c80ef1861bb45af5cb5ae6e5cd317e0a6c9aa5e4e18',
  'blocked_retailer_placeholder',
  'Globally blocked retailer placeholder/logo asset.'
)
on conflict (sha256) where sha256 is not null do update
set reason_code = excluded.reason_code,
    reason_message = excluded.reason_message;

create or replace function public.enforce_cover_candidate_blocklist()
returns trigger
language plpgsql
set search_path = public
as $$
declare
  blocked public.cover_asset_blocklist%rowtype;
begin
  -- Rejection is terminal. Discovery refreshes may update provenance, but
  -- cannot resurrect or select a rejected candidate.
  if tg_op = 'UPDATE' and old.candidate_status = 'rejected' then
    new.candidate_status := 'rejected';
    new.is_selected := false;
  end if;

  select b.* into blocked
  from public.cover_asset_blocklist b
  where (b.image_url is not null and b.image_url = new.image_url)
     or (b.sha256 is not null and b.sha256 = lower(new.sha256))
  order by (b.sha256 is not null) desc, b.created_at, b.id
  limit 1;

  if found then
    new.candidate_status := 'rejected';
    new.is_selected := false;
    new.last_error_code := blocked.reason_code;
    new.last_error_message := blocked.reason_message;
  end if;
  return new;
end
$$;

drop trigger if exists product_cover_candidates_blocklist_guard
  on public.product_cover_candidates;
create trigger product_cover_candidates_blocklist_guard
before insert or update on public.product_cover_candidates
for each row execute function public.enforce_cover_candidate_blocklist();

-- Reject only the audited asset. Other candidates for the products remain
-- byte-for-byte untouched.
update public.product_cover_candidates
set candidate_status = 'rejected',
    is_selected = false,
    last_error_code = 'blocked_retailer_placeholder',
    last_error_message = 'Globally blocked retailer placeholder/logo asset.',
    updated_at = now()
where (
  image_url = 'https://www.platomania.nl/fbmania.png'
  or lower(coalesce(sha256, '')) =
     '162468189d7fa6d6481f1c80ef1861bb45af5cb5ae6e5cd317e0a6c9aa5e4e18'
)
and (
  candidate_status <> 'rejected'
  or is_selected is true
  or last_error_code is distinct from 'blocked_retailer_placeholder'
  or last_error_message is distinct from
     'Globally blocked retailer placeholder/logo asset.'
);

create temporary table bad_platomania_cover_products on commit drop as
select id as product_id
from public.products
where cover_source_url = 'https://www.platomania.nl/fbmania.png'
   or lower(coalesce(cover_sha256, '')) =
      '162468189d7fa6d6481f1c80ef1861bb45af5cb5ae6e5cd317e0a6c9aa5e4e18';

update public.products p
set cover_url = null,
    cover_storage_path = null,
    cover_source = null,
    cover_source_url = null,
    cover_source_shop_id = null,
    cover_sha256 = null,
    cover_mime_type = null,
    cover_byte_size = null,
    cover_width = null,
    cover_height = null,
    cover_status = 'queued',
    cover_needs_refresh = true,
    cover_locked_at = null,
    cover_locked_by = null,
    cover_error_code = null,
    cover_error_message = null,
    updated_at = now()
from bad_platomania_cover_products b
where p.id = b.product_id;

insert into public.product_cover_queue (
  product_id, priority, candidate_count, source_reason, status,
  attempt_count, last_error_code, last_error_message, claimed_by,
  claimed_at, next_attempt_at, created_at, updated_at
)
select
  b.product_id,
  coalesce(p.cover_priority, 0),
  (select count(*)::integer
   from public.product_cover_candidates c
   where c.product_id = b.product_id
     and c.candidate_status <> 'rejected'),
  'blocked_cover_asset_cleanup',
  'pending', 0, null, null, null, null, now(), now(), now()
from bad_platomania_cover_products b
join public.products p on p.id = b.product_id
on conflict (product_id) where product_id is not null do update
set priority = greatest(public.product_cover_queue.priority, excluded.priority),
    candidate_count = excluded.candidate_count,
    source_reason = excluded.source_reason,
    status = case
      when public.product_cover_queue.status = 'processing'
        then public.product_cover_queue.status
      else 'pending'
    end,
    attempt_count = case
      when public.product_cover_queue.status = 'processing'
        then public.product_cover_queue.attempt_count
      else 0
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
    updated_at = now();

comment on table public.cover_asset_blocklist is
  'Exact globally rejected cover URLs and content hashes; never a domain-wide block.';

commit;
