#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any

from cover_common import (
    CandidateRecord,
    CoverPipelineError,
    DEFAULT_MAX_OFFERS_PER_PRODUCT,
    OfferSource,
    build_cover_missing_condition,
    build_cover_priority_expression,
    connect_db,
    ensure_runtime_directories,
    fetch_page_candidates,
    get_table_columns,
    log,
    make_session,
    normalize_ean,
    normalize_text,
    rank_candidate,
    require_table_columns,
    safe_parse_datetime,
    serialize_json,
    utc_now,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh cover candidates from existing offers or a candidate CSV.")
    parser.add_argument("--limit", type=int, default=75, help="Maximum aantal producten om te verwerken.")
    parser.add_argument(
        "--max-offers-per-product",
        type=int,
        default=DEFAULT_MAX_OFFERS_PER_PRODUCT,
        help="Maximum aantal shop-URL's per product dat wordt bekeken.",
    )
    parser.add_argument(
        "--include-covered",
        action="store_true",
        help="Neem ook producten mee die al een cover hebben. Handig voor backfill of candidate refresh.",
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="",
        help="Optioneel pad naar CSV met scraper-aangeleverde candidates. Verwachte kolommen: ean/product_id, shop_domain, product_url, image_url, source_type.",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="output/cover_pipeline/candidate_refresh_summary.json",
        help="Pad voor de JSON-samenvatting.",
    )
    return parser.parse_args()


def load_offer_sources(conn, limit: int, include_covered: bool, max_offers_per_product: int) -> dict[str, list[OfferSource]]:
    products_columns = require_table_columns(conn, "products", ["id", "ean"])
    prices_columns = require_table_columns(conn, "prices", ["product_id", "shop_id", "product_url"])
    require_table_columns(conn, "shops", ["id", "domain"])
    require_table_columns(conn, "product_cover_queue", ["product_id", "status"])

    cover_missing_condition = build_cover_missing_condition(products_columns, alias="p")
    cover_priority_expression = build_cover_priority_expression(products_columns, alias="p")
    last_seen_expression = "pr.last_seen_at" if "last_seen_at" in prices_columns else "null"
    is_active_condition = "coalesce(pr.is_active, true) = true" if "is_active" in prices_columns else "true"

    with conn.cursor() as cur:
        cur.execute(
            f"""
            with eligible as (
                select
                    p.id as product_id,
                    p.ean,
                    {cover_priority_expression} as cover_priority,
                    max({last_seen_expression}) as latest_seen_at,
                    count(distinct pr.shop_id) as offer_count
                from public.products p
                join public.prices pr
                  on pr.product_id = p.id
                 and {is_active_condition}
                 and coalesce(nullif(pr.product_url, ''), '') <> ''
                left join public.product_cover_queue q
                  on q.product_id = p.id
                where p.ean is not null
                  and p.ean <> ''
                  and ({cover_missing_condition} or %s)
                  and (q.status is null or q.status <> 'processing')
                group by p.id, p.ean, {cover_priority_expression}
                order by {cover_priority_expression} desc, latest_seen_at desc nulls last, offer_count desc, p.id
                limit %s
            )
            select
                e.product_id,
                e.ean,
                e.cover_priority,
                pr.shop_id,
                s.domain,
                s.name,
                pr.product_url,
                {last_seen_expression} as last_seen_at
            from eligible e
            join public.prices pr
              on pr.product_id = e.product_id
             and {is_active_condition}
             and coalesce(nullif(pr.product_url, ''), '') <> ''
            join public.shops s
              on s.id = pr.shop_id
            order by e.product_id, e.cover_priority desc, {last_seen_expression} desc nulls last, s.domain
            """,
            (include_covered, limit),
        )
        rows = cur.fetchall()

    grouped: dict[str, list[OfferSource]] = defaultdict(list)
    for row in rows:
        offer = OfferSource(
            product_id=str(row[0]),
            ean=normalize_text(row[1]),
            cover_priority=int(row[2] or 0),
            shop_id=str(row[3]) if row[3] is not None else None,
            shop_domain=normalize_text(row[4]),
            shop_name=normalize_text(row[5]) or None,
            product_url=normalize_text(row[6]),
            last_seen_at=safe_parse_datetime(row[7]),
            offer_rank=0,
        )
        bucket = grouped[offer.product_id]
        if len(bucket) >= max_offers_per_product:
            continue
        offer.offer_rank = len(bucket) + 1
        bucket.append(offer)
    return grouped


def upsert_candidate_rows(conn, candidates: list[CandidateRecord]) -> int:
    if not candidates:
        return 0

    with conn.cursor() as cur:
        for candidate in candidates:
            cur.execute(
                """
                insert into public.product_cover_candidates (
                    product_id,
                    shop_id,
                    ean,
                    product_url,
                    image_url,
                    source_type,
                    source_rank,
                    is_primary,
                    mime_type,
                    width,
                    height,
                    candidate_status,
                    discovered_at,
                    first_seen_at,
                    last_seen_at,
                    created_at,
                    updated_at
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'pending', now(), now(), now(), now(), now()
                )
                on conflict (product_id, image_url)
                do update set
                    shop_id = coalesce(excluded.shop_id, public.product_cover_candidates.shop_id),
                    ean = coalesce(excluded.ean, public.product_cover_candidates.ean),
                    product_url = excluded.product_url,
                    source_type = excluded.source_type,
                    source_rank = greatest(excluded.source_rank, public.product_cover_candidates.source_rank),
                    is_primary = public.product_cover_candidates.is_primary or excluded.is_primary,
                    mime_type = coalesce(excluded.mime_type, public.product_cover_candidates.mime_type),
                    width = coalesce(excluded.width, public.product_cover_candidates.width),
                    height = coalesce(excluded.height, public.product_cover_candidates.height),
                    last_seen_at = now(),
                    updated_at = now()
                """,
                (
                    candidate.product_id,
                    candidate.shop_id,
                    candidate.ean,
                    candidate.product_url,
                    candidate.image_url,
                    candidate.source_type,
                    candidate.source_rank,
                    candidate.is_primary,
                    candidate.mime_type,
                    candidate.width,
                    candidate.height,
                ),
            )
    return len(candidates)


def upsert_queue_rows(conn, offer_sources: dict[str, list[OfferSource]], candidate_counts: dict[str, int]) -> int:
    if not offer_sources:
        return 0
    touched = 0
    with conn.cursor() as cur:
        for product_id, offers in offer_sources.items():
            base_priority = max((offer.cover_priority for offer in offers), default=0)
            priority = base_priority + (len(offers) * 5) + (10 if candidate_counts.get(product_id, 0) > 0 else 0)
            cur.execute(
                """
                insert into public.product_cover_queue (
                    product_id,
                    priority,
                    candidate_count,
                    source_reason,
                    status,
                    next_attempt_at,
                    created_at,
                    updated_at
                )
                values (%s, %s, %s, %s, 'pending', now(), now(), now())
                on conflict (product_id)
                do update set
                    priority = greatest(public.product_cover_queue.priority, excluded.priority),
                    candidate_count = greatest(public.product_cover_queue.candidate_count, excluded.candidate_count),
                    source_reason = excluded.source_reason,
                    status = case
                        when public.product_cover_queue.status = 'processing' then public.product_cover_queue.status
                        when public.product_cover_queue.status = 'published' and excluded.candidate_count = 0 then public.product_cover_queue.status
                        else 'pending'
                    end,
                    next_attempt_at = case
                        when public.product_cover_queue.status = 'processing' then public.product_cover_queue.next_attempt_at
                        else now()
                    end,
                    updated_at = now()
                """,
                (product_id, priority, candidate_counts.get(product_id, 0), "candidate_refresh"),
            )
            touched += 1
    return touched


def discover_candidates_for_offer(offer: OfferSource, session) -> list[CandidateRecord]:
    discovered, _, _ = fetch_page_candidates(session, offer.product_url)
    candidates: list[CandidateRecord] = []
    for item in discovered:
        candidate = CandidateRecord(
            product_id=offer.product_id,
            ean=offer.ean,
            shop_id=offer.shop_id,
            shop_domain=offer.shop_domain,
            shop_name=offer.shop_name,
            product_url=offer.product_url,
            image_url=normalize_text(item.get("image_url")),
            source_type=normalize_text(item.get("source_type")) or "unknown",
            source_rank=0,
            is_primary=bool(item.get("is_primary")),
            mime_type=None,
            width=item.get("width"),
            height=item.get("height"),
        )
        candidate.source_rank = rank_candidate(candidate, recency_reference=offer.last_seen_at)
        candidates.append(candidate)
    return candidates


def load_candidates_from_csv(path: Path) -> tuple[dict[str, list[CandidateRecord]], dict[str, list[OfferSource]]]:
    if not path.exists():
        raise CoverPipelineError(f"CSV niet gevonden: {path}")
    candidates_by_product: dict[str, list[CandidateRecord]] = defaultdict(list)
    offers_by_product: dict[str, list[OfferSource]] = defaultdict(list)
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader, start=2):
            product_id = normalize_text(row.get("product_id"))
            ean = normalize_ean(row.get("ean")) or ""
            if not product_id and not ean:
                raise CoverPipelineError(f"CSV regel {row_number} mist zowel product_id als ean.")
            shop_domain = normalize_text(row.get("shop_domain"))
            image_url = normalize_text(row.get("image_url"))
            product_url = normalize_text(row.get("product_url"))
            if not image_url or not product_url:
                raise CoverPipelineError(f"CSV regel {row_number} mist image_url of product_url.")
            key = product_id or ean
            candidate = CandidateRecord(
                product_id=product_id or key,
                ean=ean,
                shop_id=normalize_text(row.get("shop_id")) or None,
                shop_domain=shop_domain,
                shop_name=normalize_text(row.get("shop_name")) or None,
                product_url=product_url,
                image_url=image_url,
                source_type=normalize_text(row.get("source_type")) or "listing",
                source_rank=int(row.get("source_rank") or 0),
                is_primary=str(row.get("is_primary") or "").lower() in {"1", "true", "yes", "y"},
                mime_type=normalize_text(row.get("mime_type")) or None,
                width=int(row["width"]) if str(row.get("width") or "").isdigit() else None,
                height=int(row["height"]) if str(row.get("height") or "").isdigit() else None,
            )
            if candidate.source_rank <= 0:
                candidate.source_rank = rank_candidate(candidate)
            candidates_by_product[key].append(candidate)
            if key not in offers_by_product:
                offers_by_product[key].append(
                    OfferSource(
                        product_id=product_id or key,
                        ean=ean,
                        shop_id=candidate.shop_id,
                        shop_domain=shop_domain,
                        shop_name=candidate.shop_name,
                        product_url=product_url,
                        cover_priority=0,
                        offer_rank=1,
                        last_seen_at=None,
                    )
                )
    return candidates_by_product, offers_by_product


def resolve_missing_product_ids(conn, candidates_by_key: dict[str, list[CandidateRecord]], offers_by_key: dict[str, list[OfferSource]]) -> tuple[dict[str, list[CandidateRecord]], dict[str, list[OfferSource]]]:
    unresolved_eans = [key for key in candidates_by_key if key and all(c.product_id == key for c in candidates_by_key[key]) and normalize_ean(key)]
    if not unresolved_eans:
        return candidates_by_key, offers_by_key

    with conn.cursor() as cur:
        cur.execute(
            "select id, ean from public.products where ean = any(%s)",
            (unresolved_eans,),
        )
        mapping = {normalize_text(row[1]): str(row[0]) for row in cur.fetchall()}

    unresolved_missing = [key for key in unresolved_eans if key not in mapping]
    if unresolved_missing:
        raise CoverPipelineError(
            "CSV bevat EANs die niet aan bestaande producten gekoppeld konden worden: " + ", ".join(unresolved_missing[:10])
        )

    resolved_candidates: dict[str, list[CandidateRecord]] = defaultdict(list)
    resolved_offers: dict[str, list[OfferSource]] = defaultdict(list)
    for key, values in candidates_by_key.items():
        resolved_id = mapping.get(key, key)
        for candidate in values:
            candidate.product_id = resolved_id
            resolved_candidates[resolved_id].append(candidate)
    for key, values in offers_by_key.items():
        resolved_id = mapping.get(key, key)
        for offer in values:
            offer.product_id = resolved_id
            resolved_offers[resolved_id].append(offer)
    return resolved_candidates, resolved_offers


def main() -> None:
    args = parse_args()
    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_runtime_directories()

    summary: dict[str, Any] = {
        "started_at": utc_now().isoformat(),
        "mode": "csv" if args.csv else "db",
        "limit": args.limit,
        "max_offers_per_product": args.max_offers_per_product,
        "include_covered": args.include_covered,
        "products_considered": 0,
        "products_touched": 0,
        "candidates_upserted": 0,
        "queue_rows_touched": 0,
        "errors": [],
        "products": [],
    }

    conn = connect_db()
    conn.autocommit = False
    try:
        require_table_columns(conn, "product_cover_candidates", ["product_id", "image_url", "source_type", "source_rank"])
        require_table_columns(conn, "product_cover_queue", ["product_id", "status", "priority"])

        if args.csv:
            candidates_by_product, offers_by_product = load_candidates_from_csv(Path(args.csv))
            candidates_by_product, offers_by_product = resolve_missing_product_ids(conn, candidates_by_product, offers_by_product)
            summary["products_considered"] = len(candidates_by_product)
            all_candidates = [candidate for values in candidates_by_product.values() for candidate in values]
            summary["candidates_upserted"] = upsert_candidate_rows(conn, all_candidates)
            queue_counts = {product_id: len(values) for product_id, values in candidates_by_product.items()}
            summary["queue_rows_touched"] = upsert_queue_rows(conn, offers_by_product, queue_counts)
            summary["products_touched"] = len(queue_counts)
            for product_id, values in candidates_by_product.items():
                summary["products"].append(
                    {
                        "product_id": product_id,
                        "candidate_count": len(values),
                        "top_candidates": [asdict(candidate) for candidate in sorted(values, key=lambda c: c.source_rank, reverse=True)[:3]],
                    }
                )
        else:
            offer_sources = load_offer_sources(conn, args.limit, args.include_covered, args.max_offers_per_product)
            summary["products_considered"] = len(offer_sources)
            session = make_session()
            discovered_by_product: dict[str, list[CandidateRecord]] = defaultdict(list)
            for product_id, offers in offer_sources.items():
                product_errors: list[str] = []
                for offer in offers:
                    try:
                        discovered_by_product[product_id].extend(discover_candidates_for_offer(offer, session))
                    except Exception as exc:
                        product_errors.append(f"{offer.shop_domain}: {exc}")
                ranked = sorted(discovered_by_product.get(product_id, []), key=lambda c: c.source_rank, reverse=True)
                summary["products"].append(
                    {
                        "product_id": product_id,
                        "ean": offers[0].ean if offers else "",
                        "offer_count": len(offers),
                        "candidate_count": len(ranked),
                        "top_candidates": [asdict(candidate) for candidate in ranked[:3]],
                        "errors": product_errors,
                    }
                )
                if product_errors:
                    summary["errors"].append({"product_id": product_id, "errors": product_errors})
            all_candidates = [candidate for values in discovered_by_product.values() for candidate in values]
            summary["candidates_upserted"] = upsert_candidate_rows(conn, all_candidates)
            queue_counts = {product_id: len(values) for product_id, values in discovered_by_product.items()}
            summary["queue_rows_touched"] = upsert_queue_rows(conn, offer_sources, queue_counts)
            summary["products_touched"] = len(offer_sources)

        conn.commit()
    except Exception as exc:
        conn.rollback()
        summary["failed_at"] = utc_now().isoformat()
        summary["fatal_error"] = str(exc)
        output_path.write_text(serialize_json(summary), encoding="utf-8")
        raise
    finally:
        conn.close()

    summary["finished_at"] = utc_now().isoformat()
    output_path.write_text(serialize_json(summary), encoding="utf-8")
    log(f"[DONE] candidate refresh klaar | {summary['products_touched']} producten | {summary['candidates_upserted']} candidates")


if __name__ == "__main__":
    main()
