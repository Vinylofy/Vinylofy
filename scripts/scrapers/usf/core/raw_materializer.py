from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from psycopg.types.json import Jsonb

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.models import RawProductData


RawMapper = Callable[[dict[str, Any]], RawProductData]


@dataclass(frozen=True)
class MaterializedRawItem:
    link_id: str
    raw_id: str | None
    raw: RawProductData


@dataclass(frozen=True)
class RawMaterializeResult:
    queued: int
    processed: int
    items: tuple[MaterializedRawItem, ...]


def materialize_queued_links(
    *,
    shop_id: str,
    limit: int,
    mapper: RawMapper,
    write: bool = False,
    run_id: str | None = None,
) -> RawMaterializeResult:
    if limit < 1:
        raise ValueError("limit moet minimaal 1 zijn")

    items: list[MaterializedRawItem] = []

    with db_connection() as conn:
        with conn.cursor() as cur:
            query = """
                select
                    id,
                    shop_id,
                    source_url,
                    source_product_id,
                    payload
                from public.shop_product_links
                where shop_id = %s
                  and status = 'active'
                  and last_detail_scraped_at is null
                order by first_seen_at asc, id asc
                limit %s
            """

            if write:
                query += " for update skip locked"

            cur.execute(query, (shop_id, limit))
            rows = cur.fetchall()

            for row in rows:
                link = {
                    "id": str(row[0]),
                    "shop_id": row[1],
                    "source_url": row[2],
                    "source_product_id": row[3],
                    "payload": row[4] or {},
                }

                raw = mapper(link)

                if raw.shop_id != shop_id:
                    raise RuntimeError(
                        f"mapper gaf shop_id={raw.shop_id!r}; verwacht {shop_id!r}"
                    )

                if raw.source_url != link["source_url"]:
                    raise RuntimeError(
                        "mapper wijzigde source_url; raw snapshot kan niet veilig "
                        "aan de registry-link worden gekoppeld"
                    )

                raw_id: str | None = None

                if write:
                    cur.execute(
                        """
                        insert into public.raw_shop_scrapes (
                            run_id,
                            shop_id,
                            source_url,
                            source_product_id,
                            title_raw,
                            ean_raw,
                            price_raw,
                            availability_raw,
                            image_url_raw,
                            payload
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        returning id
                        """,
                        (
                            run_id,
                            raw.shop_id,
                            raw.source_url,
                            raw.source_product_id,
                            raw.title_raw,
                            raw.ean_raw,
                            raw.price_raw,
                            raw.availability_raw,
                            raw.image_url_raw,
                            Jsonb(raw.payload),
                        ),
                    )
                    raw_id = str(cur.fetchone()[0])

                    cur.execute(
                        """
                        update public.shop_product_links
                        set last_detail_scraped_at = now()
                        where id = %s
                          and last_detail_scraped_at is null
                        """,
                        (link["id"],),
                    )

                    if cur.rowcount != 1:
                        raise RuntimeError(
                            f"queue-link {link['id']} kon niet atomair worden verwerkt"
                        )

                items.append(
                    MaterializedRawItem(
                        link_id=link["id"],
                        raw_id=raw_id,
                        raw=raw,
                    )
                )

    return RawMaterializeResult(
        queued=len(items),
        processed=len(items),
        items=tuple(items),
    )
