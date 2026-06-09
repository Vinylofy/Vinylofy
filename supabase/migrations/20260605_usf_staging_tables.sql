create table if not exists scrape_runs (
    id uuid primary key default gen_random_uuid(),
    shop_id text not null,
    job_type text not null,
    status text not null default 'running',
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    items_found int default 0,
    items_processed int default 0,
    items_success int default 0,
    items_failed int default 0,
    error_message text,
    config jsonb default '{}'::jsonb
);

create table if not exists shop_product_links (
    id uuid primary key default gen_random_uuid(),
    shop_id text not null,
    source_url text not null,
    source_product_id text,
    first_seen_at timestamptz not null default now(),
    last_seen_at timestamptz not null default now(),
    last_detail_scraped_at timestamptz,
    last_price_refreshed_at timestamptz,
    status text not null default 'active',
    payload jsonb default '{}'::jsonb,
    unique(shop_id, source_url)
);

create table if not exists raw_shop_scrapes (
    id uuid primary key default gen_random_uuid(),
    run_id uuid references scrape_runs(id),
    shop_id text not null,
    source_url text not null,
    source_product_id text,
    title_raw text,
    ean_raw text,
    price_raw text,
    availability_raw text,
    image_url_raw text,
    payload jsonb default '{}'::jsonb,
    scraped_at timestamptz not null default now(),
    parse_status text not null default 'raw'
);

create table if not exists staged_offers (
    id uuid primary key default gen_random_uuid(),
    raw_scrape_id uuid references raw_shop_scrapes(id),
    run_id uuid references scrape_runs(id),
    shop_id text not null,
    source_url text not null,
    source_product_id text,
    title_normalized text,
    ean_normalized text,
    ean_match_key text,
    price numeric(10,2),
    currency text default 'EUR',
    availability text,
    image_url text,
    stage_status text not null default 'ready',
    stage_reason text,
    created_at timestamptz not null default now()
);

create table if not exists quarantine_offers (
    id uuid primary key default gen_random_uuid(),
    staged_offer_id uuid references staged_offers(id),
    shop_id text not null,
    source_url text not null,
    ean_normalized text,
    ean_match_key text,
    issue_type text not null,
    issue_detail text,
    payload jsonb default '{}'::jsonb,
    created_at timestamptz not null default now(),
    resolved_at timestamptz
);

create index if not exists idx_scrape_runs_shop_job_started
on scrape_runs(shop_id, job_type, started_at desc);

create index if not exists idx_shop_product_links_shop_status
on shop_product_links(shop_id, status);

create index if not exists idx_raw_shop_scrapes_shop_scraped
on raw_shop_scrapes(shop_id, scraped_at desc);

create index if not exists idx_staged_offers_status_shop
on staged_offers(stage_status, shop_id);

create index if not exists idx_staged_offers_ean_match_key
on staged_offers(ean_match_key);

create index if not exists idx_quarantine_offers_issue
on quarantine_offers(issue_type, created_at desc);
