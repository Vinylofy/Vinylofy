-- Support the generic USF staging query as raw and staged history grows.
-- The column order mirrors staging.fetch_latest_unstaged_raw_rows exactly.

create index if not exists idx_raw_shop_scrapes_shop_url_latest
on public.raw_shop_scrapes (
    shop_id,
    source_url,
    scraped_at desc nulls last,
    id desc
);

-- PostgreSQL does not create an index automatically for this foreign key.
-- Staging uses raw_scrape_id to exclude snapshots already staged.
create index if not exists idx_staged_offers_raw_scrape_id
on public.staged_offers (raw_scrape_id);
