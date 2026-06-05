from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg.types.json import Jsonb

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.models import DiscoveredLink


@dataclass(frozen=True)
class LinkRegistryResult:
    inserted: int = 0
    updated: int = 0
    total: int = 0


def upsert_discovered_links(links: list[DiscoveredLink]) -> LinkRegistryResult:
    inserted = 0
    updated = 0

    with db_connection() as conn:
        with conn.cursor() as cur:
            for link in links:
                cur.execute(
                    """
                    insert into public.shop_product_links (
                        shop_id,
                        source_url,
                        source_product_id,
                        first_seen_at,
                        last_seen_at,
                        status,
                        payload
                    )
                    values (%s, %s, %s, now(), now(), 'active', %s)
                    on conflict (shop_id, source_url)
                    do update set
                        source_product_id = excluded.source_product_id,
                        last_seen_at = now(),
                        status = 'active',
                        payload = excluded.payload
                    returning (xmax = 0) as inserted
                    """,
                    (
                        link.shop_id,
                        link.source_url,
                        link.source_product_id,
                        Jsonb(link.payload),
                    ),
                )
                was_inserted = bool(cur.fetchone()[0])
                if was_inserted:
                    inserted += 1
                else:
                    updated += 1

    return LinkRegistryResult(
        inserted=inserted,
        updated=updated,
        total=inserted + updated,
    )


def get_links_for_detail_scrape(shop_id: str, limit: int = 100) -> list[dict[str, Any]]:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id, shop_id, source_url, source_product_id, payload
                from public.shop_product_links
                where shop_id = %s
                  and status = 'active'
                  and last_detail_scraped_at is null
                order by first_seen_at asc
                limit %s
                """,
                (shop_id, limit),
            )
            rows = cur.fetchall()

    return [
        {
            "id": str(row[0]),
            "shop_id": row[1],
            "source_url": row[2],
            "source_product_id": row[3],
            "payload": row[4],
        }
        for row in rows
    ]


def mark_detail_scraped(link_id: str) -> None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.shop_product_links
                set last_detail_scraped_at = now()
                where id = %s
                """,
                (link_id,),
            )
