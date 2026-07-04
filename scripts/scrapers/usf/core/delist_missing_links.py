from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Iterable

import psycopg


def mark_missing_links_out_of_stock(
    *,
    shop_id: str,
    seen_source_urls: Iterable[str],
    run_started_at: datetime | None = None,
    write: bool = False,
) -> dict[str, int]:
    urls = sorted({u for u in seen_source_urls if u})
    if not urls:
        return {"candidates": 0, "updated": 0}

    db = os.environ.get("DATABASE_URL")
    if not db:
        raise RuntimeError("DATABASE_URL ontbreekt")

    now = run_started_at or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    with psycopg.connect(db, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select count(*)
                from public.shop_product_links
                where shop_id = %s
                  and source_url <> all(%s)
                  and coalesce(payload->>'availability', 'unknown') <> 'out_of_stock'
                """,
                (shop_id, urls),
            )
            candidates = int(cur.fetchone()[0] or 0)

            if not write:
                return {"candidates": candidates, "updated": 0}

            cur.execute(
                """
                update public.shop_product_links
                set payload = jsonb_set(
                        coalesce(payload, '{}'::jsonb),
                        '{availability}',
                        '"out_of_stock"'::jsonb,
                        true
                    ),
                    updated_at = now()
                where shop_id = %s
                  and source_url <> all(%s)
                  and coalesce(payload->>'availability', 'unknown') <> 'out_of_stock'
                """,
                (shop_id, urls),
            )
            updated = cur.rowcount or 0

        conn.commit()

    return {"candidates": candidates, "updated": updated}
