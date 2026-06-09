-- Generiek EAN-verrijkingsbeleid voor grote catalogi.
--
-- Bestaande links blijven bewust ongeclassificeerd (NULL).
-- Nieuwe discovery/detailjobs moeten de status expliciet beheren.
--
-- Beleid:
--   inhoudelijke EAN-missers 1-2: normale vervolgqueue
--   inhoudelijke EAN-misser 3: 90 dagen pauze
--   inhoudelijke EAN-misser 4: 180 dagen pauze
--   inhoudelijke EAN-misser 5+: maximaal jaarlijks of event-driven
--   technische fouten verhogen uitsluitend ean_technical_failure_count

alter table public.shop_product_links
    add column if not exists ean_enrichment_status text,
    add column if not exists ean_content_miss_count integer not null default 0,
    add column if not exists ean_technical_failure_count integer not null default 0,
    add column if not exists ean_last_attempt_at timestamptz,
    add column if not exists ean_next_attempt_at timestamptz,
    add column if not exists ean_last_result text,
    add column if not exists ean_last_error text,
    add column if not exists ean_last_http_status integer;

-- Alleen toekomstige, nieuw ingevoegde links krijgen automatisch 'pending'.
-- Bestaande links blijven NULL totdat een shopspecifieke backfill ze classificeert.
alter table public.shop_product_links
    alter column ean_enrichment_status set default 'pending';

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'shop_product_links_ean_enrichment_status_chk'
          and conrelid = 'public.shop_product_links'::regclass
    ) then
        alter table public.shop_product_links
            add constraint shop_product_links_ean_enrichment_status_chk
            check (
                ean_enrichment_status is null
                or ean_enrichment_status in (
                    'pending',
                    'found',
                    'not_found',
                    'technical_error',
                    'second_hand_no_ean',
                    'not_applicable'
                )
            );
    end if;
end
$$;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'shop_product_links_ean_content_miss_count_chk'
          and conrelid = 'public.shop_product_links'::regclass
    ) then
        alter table public.shop_product_links
            add constraint shop_product_links_ean_content_miss_count_chk
            check (ean_content_miss_count >= 0);
    end if;
end
$$;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'shop_product_links_ean_technical_failure_count_chk'
          and conrelid = 'public.shop_product_links'::regclass
    ) then
        alter table public.shop_product_links
            add constraint shop_product_links_ean_technical_failure_count_chk
            check (ean_technical_failure_count >= 0);
    end if;
end
$$;

do $$
begin
    if not exists (
        select 1
        from pg_constraint
        where conname = 'shop_product_links_ean_last_http_status_chk'
          and conrelid = 'public.shop_product_links'::regclass
    ) then
        alter table public.shop_product_links
            add constraint shop_product_links_ean_last_http_status_chk
            check (
                ean_last_http_status is null
                or ean_last_http_status between 100 and 599
            );
    end if;
end
$$;

create index if not exists idx_shop_product_links_ean_queue
on public.shop_product_links (
    shop_id,
    ean_enrichment_status,
    ean_next_attempt_at,
    first_seen_at
)
where status = 'active';

comment on column public.shop_product_links.ean_enrichment_status is
'Status van EAN-detailverrijking; NULL betekent nog niet gemigreerd naar het large-catalog beleid.';

comment on column public.shop_product_links.ean_content_miss_count is
'Aantal inhoudelijk geldige detailcontroles waarbij geen EAN is gevonden.';

comment on column public.shop_product_links.ean_technical_failure_count is
'Aantal technische detailfouten; telt niet mee als inhoudelijke EAN-misser.';

comment on column public.shop_product_links.ean_next_attempt_at is
'Eerstvolgende moment waarop automatische EAN-verrijking weer is toegestaan.';
