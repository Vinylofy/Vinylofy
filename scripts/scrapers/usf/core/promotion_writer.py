from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row

from scripts.importers.common import (
    ImportConfig,
    ensure_shop,
    maybe_insert_history,
    maybe_upsert_cover_candidate,
    upsert_price,
    upsert_product,
)
from scripts.scrapers.usf.core.db import get_database_url
from scripts.scrapers.usf.core.promotion import (
    PromotionConfig,
    staged_row_to_record,
)


@dataclass(frozen=True)
class PromotedItem:
    staged_offer_id: str
    product_id: str
    ean: str
    price: float
    product_inserted: bool
    price_inserted: bool
    price_changed: bool
    history_inserted: bool
    cover_candidate_inserted: bool


@dataclass(frozen=True)
class PromotionFailure:
    staged_offer_id: str
    reason: str


@dataclass(frozen=True)
class PromotionWriteResult:
    queued: int
    processed: int
    failed: int
    new_products: int
    new_prices: int
    changed_prices: int
    history_rows: int
    cover_candidates: int
    items: tuple[PromotedItem, ...]
    failures: tuple[PromotionFailure, ...]


def fetch_locked_staged_rows(
    cur,
    *,
    shop_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    cur.execute(
        """
        select
            s.id as staged_offer_id,
            s.raw_scrape_id,
            s.shop_id,
            s.source_url,
            s.source_product_id,
            s.title_normalized,
            s.ean_normalized,
            s.ean_match_key,
            s.price,
            s.currency,
            s.availability,
            s.image_url,
            s.stage_status,
            s.stage_reason,
            s.created_at as staged_at,
            r.title_raw,
            r.scraped_at,
            r.payload as raw_payload
        from public.staged_offers s
        left join public.raw_shop_scrapes r
          on r.id = s.raw_scrape_id
        where s.shop_id = %s
          and s.stage_status = 'staged'
          and s.ean_match_key is not null
          and s.price is not null
          and coalesce(s.availability, 'unknown') <> 'out_of_stock'
        order by s.created_at asc, s.id asc
        limit %s
        for update of s skip locked
        """,
        (shop_id, limit),
    )
    return [dict(row) for row in cur.fetchall()]


def promote_staged_offers_atomically(
    *,
    config: PromotionConfig,
    limit: int,
) -> PromotionWriteResult:
    if limit < 1:
        raise ValueError("limit moet minimaal 1 zijn")

    importer_config = ImportConfig(
        shop_name=config.shop_name,
        shop_domain=config.shop_domain,
        shop_country=config.shop_country,
        currency=config.currency,
    )

    promoted_items: list[PromotedItem] = []
    failures: list[PromotionFailure] = []

    with psycopg.connect(
        get_database_url(),
        prepare_threshold=None,
    ) as conn:
        with conn.cursor(row_factory=dict_row) as read_cur:
            rows = fetch_locked_staged_rows(
                read_cur,
                shop_id=config.shop_id,
                limit=limit,
            )

        if not rows:
            return PromotionWriteResult(
                queued=0,
                processed=0,
                failed=0,
                new_products=0,
                new_prices=0,
                changed_prices=0,
                history_rows=0,
                cover_candidates=0,
                items=(),
                failures=(),
            )

        with conn.cursor() as cur:
            shop_uuid = ensure_shop(cur, importer_config)

        imported_at = datetime.now(timezone.utc)

        for index, row in enumerate(rows, start=1):
            staged_offer_id = str(row["staged_offer_id"])

            try:
                # Nested transaction = savepoint. Een fout rolt alleen
                # dit ene offer terug, niet de overige geldige offers.
                with conn.transaction():
                    record = staged_row_to_record(
                        row=row,
                        config=config,
                        line_number=index,
                    )

                    with conn.cursor() as cur:
                        product_id, product_inserted = upsert_product(
                            cur,
                            record,
                        )

                        price_inserted, price_changed = upsert_price(
                            cur,
                            product_id,
                            shop_uuid,
                            record,
                            imported_at,
                        )

                        history_inserted = maybe_insert_history(
                            cur,
                            product_id,
                            shop_uuid,
                            record,
                        )

                        cover_candidate_inserted = (
                            maybe_upsert_cover_candidate(
                                cur,
                                product_id,
                                shop_uuid,
                                record,
                            )
                        )

                        cur.execute(
                            """
                            update public.staged_offers
                            set
                                stage_status = 'promoted',
                                stage_reason = null
                            where id = %s
                              and stage_status = 'staged'
                            returning id
                            """,
                            (staged_offer_id,),
                        )

                        if cur.fetchone() is None:
                            raise RuntimeError(
                                "staged offer kon niet als promoted "
                                f"worden gemarkeerd: {staged_offer_id}"
                            )

                        cur.execute(
                            """
                            select 1
                            from public.prices
                            where product_id = %s
                              and shop_id = %s
                            """,
                            (product_id, shop_uuid),
                        )

                        if cur.fetchone() is None:
                            raise RuntimeError(
                                "price-upsert leverde geen prijs op "
                                f"voor product_id={product_id}"
                            )

                    promoted_items.append(
                        PromotedItem(
                            staged_offer_id=staged_offer_id,
                            product_id=str(product_id),
                            ean=record.ean,
                            price=record.price,
                            product_inserted=bool(product_inserted),
                            price_inserted=bool(price_inserted),
                            price_changed=bool(price_changed),
                            history_inserted=bool(history_inserted),
                            cover_candidate_inserted=bool(
                                cover_candidate_inserted
                            ),
                        )
                    )

            except Exception as exc:
                reason = str(exc)[:500]

                # De mislukte offer-savepoint is al teruggedraaid.
                # Leg vervolgens de foutstatus apart vast.
                with conn.transaction():
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            update public.staged_offers
                            set
                                stage_status = 'promote_error',
                                stage_reason = %s
                            where id = %s
                              and stage_status = 'staged'
                            returning id
                            """,
                            (reason, staged_offer_id),
                        )

                        if cur.fetchone() is None:
                            raise RuntimeError(
                                "foutstatus kon niet worden vastgelegd "
                                f"voor staged_offer_id={staged_offer_id}"
                            )

                failures.append(
                    PromotionFailure(
                        staged_offer_id=staged_offer_id,
                        reason=reason,
                    )
                )

    return PromotionWriteResult(
        queued=len(rows),
        processed=len(promoted_items),
        failed=len(failures),
        new_products=sum(
            int(item.product_inserted)
            for item in promoted_items
        ),
        new_prices=sum(
            int(item.price_inserted)
            for item in promoted_items
        ),
        changed_prices=sum(
            int(item.price_changed)
            for item in promoted_items
        ),
        history_rows=sum(
            int(item.history_inserted)
            for item in promoted_items
        ),
        cover_candidates=sum(
            int(item.cover_candidate_inserted)
            for item in promoted_items
        ),
        items=tuple(promoted_items),
        failures=tuple(failures),
    )
