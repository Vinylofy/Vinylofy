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


def get_candidate_table_profile(conn) -> tuple[set[str], list[str], str]:
    columns = get_table_columns(conn, "product_cover_candidates")
    url_columns = [column for column in ("image_url", "source_url", "candidate_url") if column in columns]
    if not url_columns:
        raise CoverPipelineError("Tabel public.product_cover_candidates mist image_url/source_url/candidate_url.")
    preferred_url_column = "image_url" if "image_url" in columns else url_columns[0]
    return columns, url_columns, preferred_url_column


def build_candidate_insert_payload(candidate: CandidateRecord, columns: set[str]) -> dict[str, Any]:
    now = utc_now()
    payload: dict[str, Any] = {}
    values = {
        "product_id": candidate.product_id,
        "shop_id": candidate.shop_id,
        "ean": candidate.ean,
        "product_url": candidate.product_url,
        "image_url": candidate.image_url,
        "source_url": candidate.image_url,
        "candidate_url": candidate.image_url,
        "source_type": candidate.source_type,
        "source_rank": candidate.source_rank,
        "is_primary": candidate.is_primary,
        "mime_type": candidate.mime_type,
        "width": candidate.width,
        "height": candidate.height,
        "candidate_status": "pending",
        "discovered_at": now,
        "first_seen_at": now,
        "last_seen_at": now,
        "created_at": now,
        "updated_at": now,
    }
    for column, value in values.items():
        if column in columns:
            payload[column] = value
    return payload


def find_existing_candidate(cur, product_id: str, image_url: str, url_columns: list[str], columns: set[str]) -> dict[str, Any] | None:
    select_columns = ["id"]
    for column in ("source_rank", "is_primary", "mime_type", "width", "height", "candidate_status"):
        if column in columns:
            select_columns.append(column)
    predicates = " or ".join(f"{column} = %s" for column in url_columns)
    cur.execute(
        f"select {', '.join(select_columns)} from public.product_cover_candidates where product_id = %s and ({predicates}) order by updated_at desc nulls last, created_at desc nulls last, id limit 1",
        [product_id, *([image_url] * len(url_columns))],
    )
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip(select_columns, row))


def insert_candidate_row(cur, payload: dict[str, Any]) -> None:
    columns = list(payload.keys())
    placeholders = ", ".join(["%s"] * len(columns))
    cur.execute(
        f"insert into public.product_cover_candidates ({', '.join(columns)}) values ({placeholders})",
        [payload[column] for column in columns],
    )


def update_candidate_row(cur, row_id: Any, candidate: CandidateRecord, existing: dict[str, Any], columns: set[str]) -> None:
    now = utc_now()
    payload: dict[str, Any] = {}
    if "shop_id" in columns and candidate.shop_id:
        payload["shop_id"] = candidate.shop_id
    if "ean" in columns and candidate.ean:
        payload["ean"] = candidate.ean
    if "product_url" in columns and candidate.product_url:
        payload["product_url"] = candidate.product_url
    for column in ("image_url", "source_url", "candidate_url"):
        if column in columns and candidate.image_url:
            payload[column] = candidate.image_url
    if "source_type" in columns and candidate.source_type:
        payload["source_type"] = candidate.source_type
    if "source_rank" in columns:
        payload["source_rank"] = max(int(existing.get("source_rank") or 0), int(candidate.source_rank or 0))
    if "is_primary" in columns:
        payload["is_primary"] = bool(existing.get("is_primary")) or bool(candidate.is_primary)
    if "mime_type" in columns and candidate.mime_type and not existing.get("mime_type"):
        payload["mime_type"] = candidate.mime_type
    if "width" in columns:
        payload["width"] = existing.get("width") or candidate.width
    if "height" in columns:
        payload["height"] = existing.get("height") or candidate.height
    if "candidate_status" in columns:
        current_status = normalize_text(existing.get("candidate_status"))
        payload["candidate_status"] = current_status if current_status in {"published", "accepted"} else "pending"
    if "last_seen_at" in columns:
        payload["last_seen_at"] = now
    if "updated_at" in columns:
        payload["updated_at"] = now
    if not payload:
        return
    assignments = ", ".join(f"{column} = %s" for column in payload)
    cur.execute(
        f"update public.product_cover_candidates set {assignments} where id = %s",
        [*payload.values(), row_id],
    )


def get_queue_table_columns(conn) -> set[str]:
    return get_table_columns(conn, "product_cover_queue")


def upsert_candidate_rows(conn, candidates: list[CandidateRecord]) -> int:
    if not candidates:
        return 0

    columns, url_columns, _ = get_candidate_table_profile(conn)
    with conn.cursor() as cur:
        for candidate in candidates:
            existing = find_existing_candidate(cur, candidate.product_id, candidate.image_url, url_columns, columns)
            if existing is None:
                insert_candidate_row(cur, build_candidate_insert_payload(candidate, columns))
            else:
                update_candidate_row(cur, existing["id"], candidate, existing, columns)
    return len(candidates)


def upsert_queue_rows(conn, offer_sources: dict[str, list[OfferSource]], candidate_counts: dict[str, int]) -> int:
    if not offer_sources:
        return 0
    columns = get_queue_table_columns(conn)
    touched = 0
    with conn.cursor() as cur:
        for product_id, offers in offer_sources.items():
            base_priority = max((offer.cover_priority for offer in offers), default=0)
            priority = base_priority + (len(offers) * 5) + (10 if candidate_counts.get(product_id, 0) > 0 else 0)
            cur.execute(
                "select id, status, priority, candidate_count from public.product_cover_queue where product_id = %s order by updated_at desc nulls last, created_at desc nulls last, id limit 1",
                (product_id,),
            )
            existing = cur.fetchone()
            now = utc_now()
            if existing is None:
                payload: dict[str, Any] = {}
                values = {
                    "product_id": product_id,
                    "priority": priority,
                    "candidate_count": candidate_counts.get(product_id, 0),
                    "source_reason": "candidate_refresh",
                    "status": "pending",
                    "next_attempt_at": now,
                    "created_at": now,
                    "updated_at": now,
                }
                for column, value in values.items():
                    if column in columns:
                        payload[column] = value
                insert_columns = list(payload.keys())
                cur.execute(
                    f"insert into public.product_cover_queue ({', '.join(insert_columns)}) values ({', '.join(['%s'] * len(insert_columns))})",
                    [payload[column] for column in insert_columns],
                )
            else:
                row_id, current_status, current_priority, current_candidate_count = existing
                payload = {}
                if "priority" in columns:
                    payload["priority"] = max(int(current_priority or 0), int(priority or 0))
                if "candidate_count" in columns:
                    payload["candidate_count"] = max(int(current_candidate_count or 0), int(candidate_counts.get(product_id, 0) or 0))
                if "source_reason" in columns:
                    payload["source_reason"] = "candidate_refresh"
                if "status" in columns:
                    payload["status"] = current_status if current_status == "processing" else (current_status if current_status == "published" and candidate_counts.get(product_id, 0) == 0 else "pending")
                if "next_attempt_at" in columns and current_status != "processing":
                    payload["next_attempt_at"] = now
                if "updated_at" in columns:
                    payload["updated_at"] = now
                assignments = ", ".join(f"{column} = %s" for column in payload)
                cur.execute(
                    f"update public.product_cover_queue set {assignments} where id = %s",
                    [*payload.values(), row_id],
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
        require_table_columns(conn, "product_cover_candidates", ["product_id", "source_type", "source_rank"])
        get_candidate_table_profile(conn)
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
