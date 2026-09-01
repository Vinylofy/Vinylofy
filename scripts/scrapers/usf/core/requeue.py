from __future__ import annotations

from dataclasses import dataclass

from scripts.scrapers.usf.core.db import db_connection


@dataclass(frozen=True)
class RequeueResult:
    shop_id: str
    current_queue: int
    eligible: int
    target_queue: int
    requested_limit: int
    planned: int
    requeued: int
    link_ids: tuple[str, ...]


def requeue_stale_links(
    *,
    shop_id: str,
    stale_hours: float,
    limit: int,
    target_queue: int,
    exclude_successful_ean: bool = False,
    write: bool,
) -> RequeueResult:
    if not shop_id.strip():
        raise ValueError("shop_id mag niet leeg zijn")

    if stale_hours < 0:
        raise ValueError("stale_hours mag niet negatief zijn")

    if limit < 1:
        raise ValueError("limit moet minimaal 1 zijn")

    if target_queue < 0:
        raise ValueError("target_queue mag niet negatief zijn")

    resolved_ean_filter = ""
    if exclude_successful_ean:
        resolved_ean_filter = r"""
                  and nullif(payload->>'last_successful_ean', '') is null
                  and not exists (
                      select 1
                      from public.raw_shop_scrapes raw
                      where raw.shop_id = shop_product_links.shop_id
                        and raw.source_product_id = shop_product_links.source_product_id
                        and regexp_replace(coalesce(raw.ean_raw, ''), '\D', '', 'g')
                            ~ '^(\d{8}|\d{12}|\d{13}|\d{14})$'
                  )
        """

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select count(*)
                from public.shop_product_links
                where shop_id = %s
                  and status = 'active'
                  and last_detail_scraped_at is null
                """,
                (shop_id,),
            )
            current_queue = int(cur.fetchone()[0])

            cur.execute(
                """
                select count(*)
                from public.shop_product_links
                where shop_id = %s
                  and status = 'active'
                  and last_detail_scraped_at is not null
                  and last_detail_scraped_at
                      <= now() - (%s * interval '1 hour')
                """ + resolved_ean_filter + """
                """,
                (shop_id, stale_hours),
            )
            eligible = int(cur.fetchone()[0])

            available_room = max(0, target_queue - current_queue)
            planned = min(limit, eligible, available_room)

            if planned == 0:
                return RequeueResult(
                    shop_id=shop_id,
                    current_queue=current_queue,
                    eligible=eligible,
                    target_queue=target_queue,
                    requested_limit=limit,
                    planned=0,
                    requeued=0,
                    link_ids=(),
                )

            if write:
                cur.execute(
                    """
                    with candidates as (
                        select id
                        from public.shop_product_links
                        where shop_id = %s
                          and status = 'active'
                          and last_detail_scraped_at is not null
                          and last_detail_scraped_at
                              <= now() - (%s * interval '1 hour')
                        """ + resolved_ean_filter + """
                        order by
                            last_detail_scraped_at asc,
                            id asc
                        limit %s
                        for update skip locked
                    )
                    update public.shop_product_links link
                    set last_detail_scraped_at = null
                    from candidates
                    where link.id = candidates.id
                    returning link.id
                    """,
                    (shop_id, stale_hours, planned),
                )
            else:
                cur.execute(
                    """
                    select id
                    from public.shop_product_links
                    where shop_id = %s
                      and status = 'active'
                      and last_detail_scraped_at is not null
                      and last_detail_scraped_at
                          <= now() - (%s * interval '1 hour')
                    """ + resolved_ean_filter + """
                    order by
                        last_detail_scraped_at asc,
                        id asc
                    limit %s
                    """,
                    (shop_id, stale_hours, planned),
                )

            link_ids = tuple(str(row[0]) for row in cur.fetchall())

    return RequeueResult(
        shop_id=shop_id,
        current_queue=current_queue,
        eligible=eligible,
        target_queue=target_queue,
        requested_limit=limit,
        planned=planned,
        requeued=len(link_ids) if write else 0,
        link_ids=link_ids,
    )
