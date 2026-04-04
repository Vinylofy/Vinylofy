#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path
from typing import Any

from cover_common import (
    CandidateRecord,
    CoverPipelineError,
    build_cover_missing_condition,
    build_storage_path,
    connect_db,
    ensure_runtime_directories,
    fetch_binary,
    fetch_page_candidates,
    get_supabase_credentials,
    get_table_columns,
    log,
    make_session,
    next_retry_timestamp,
    normalize_ean,
    normalize_text,
    prepare_image_for_storage,
    rank_candidate,
    require_table_columns,
    safe_parse_datetime,
    serialize_json,
    upload_bytes_to_storage,
    utc_now,
)


VALID_QUEUE_SELECTIONS = {
    "publish": ("pending", "retry_later", "review"),
    "retry-failed": ("failed", "retry_later", "review"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download, validate and publish covers from product_cover_queue.")
    parser.add_argument("--mode", choices=sorted(VALID_QUEUE_SELECTIONS), default="publish")
    parser.add_argument("--limit", type=int, default=25, help="Maximum aantal queue-jobs in deze run.")
    parser.add_argument(
        "--worker-id",
        type=str,
        default="",
        help="Optionele worker identifier. Standaard hostname/pid-achtig gedrag is niet nodig; handmatige string is genoeg.",
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="output/cover_pipeline/cover_publish_summary.json",
        help="Pad voor de JSON-samenvatting.",
    )
    return parser.parse_args()


def claim_jobs(conn, mode: str, limit: int, worker_id: str) -> list[dict[str, Any]]:
    require_table_columns(conn, "product_cover_queue", ["id", "product_id", "status", "attempt_count", "priority"])
    products_columns = require_table_columns(conn, "products", ["id", "ean"])
    cover_missing_condition = build_cover_missing_condition(products_columns, alias="p")
    allowed_statuses = VALID_QUEUE_SELECTIONS[mode]

    with conn.cursor() as cur:
        cur.execute(
            f"""
            with selected as (
                select q.id
                from public.product_cover_queue q
                join public.products p
                  on p.id = q.product_id
                where q.status = any(%s)
                  and (q.next_attempt_at is null or q.next_attempt_at <= now())
                  and ({cover_missing_condition} or q.status <> 'published')
                order by q.priority desc, q.updated_at asc, q.id
                limit %s
                for update skip locked
            )
            update public.product_cover_queue q
               set status = 'processing',
                   claimed_by = %s,
                   claimed_at = now(),
                   updated_at = now()
              from selected s
             where q.id = s.id
         returning q.id, q.product_id, q.priority, q.attempt_count, q.candidate_count, q.source_reason
            """,
            (list(allowed_statuses), limit, worker_id),
        )
        rows = cur.fetchall()

    jobs = []
    for row in rows:
        jobs.append(
            {
                "queue_id": str(row[0]),
                "product_id": str(row[1]),
                "priority": int(row[2] or 0),
                "attempt_count": int(row[3] or 0),
                "candidate_count": int(row[4] or 0),
                "source_reason": normalize_text(row[5]),
            }
        )
    return jobs


def load_product_context(conn, product_id: str) -> dict[str, Any]:
    products_columns = require_table_columns(conn, "products", ["id", "ean"])
    prices_columns = require_table_columns(conn, "prices", ["product_id", "shop_id", "product_url"])
    require_table_columns(conn, "shops", ["id", "domain"])

    title_column = "title" if "title" in products_columns else "null"
    artist_column = "artist" if "artist" in products_columns else "null"
    cover_priority_column = "coalesce(p.cover_priority, 0)" if "cover_priority" in products_columns else "0"
    last_seen_expression = "pr.last_seen_at" if "last_seen_at" in prices_columns else "null"
    is_active_condition = "coalesce(pr.is_active, true) = true" if "is_active" in prices_columns else "true"

    with conn.cursor() as cur:
        cur.execute(
            f"""
            select
                p.id,
                p.ean,
                {artist_column} as artist,
                {title_column} as title,
                {cover_priority_column} as cover_priority
            from public.products p
            where p.id = %s
            limit 1
            """,
            (product_id,),
        )
        product_row = cur.fetchone()
        if product_row is None:
            raise CoverPipelineError(f"Product niet gevonden voor id {product_id}")

        cur.execute(
            f"""
            select
                pr.shop_id,
                s.domain,
                s.name,
                pr.product_url,
                {last_seen_expression} as last_seen_at
            from public.prices pr
            join public.shops s on s.id = pr.shop_id
            where pr.product_id = %s
              and {is_active_condition}
              and coalesce(nullif(pr.product_url, ''), '') <> ''
            order by {last_seen_expression} desc nulls last, s.domain
            """,
            (product_id,),
        )
        offers = cur.fetchall()

    return {
        "product_id": str(product_row[0]),
        "ean": normalize_text(product_row[1]),
        "artist": normalize_text(product_row[2]),
        "title": normalize_text(product_row[3]),
        "cover_priority": int(product_row[4] or 0),
        "offers": [
            {
                "shop_id": str(row[0]) if row[0] is not None else None,
                "shop_domain": normalize_text(row[1]),
                "shop_name": normalize_text(row[2]) or None,
                "product_url": normalize_text(row[3]),
                "last_seen_at": safe_parse_datetime(row[4]),
            }
            for row in offers
        ],
    }


def load_candidates(conn, product_context: dict[str, Any]) -> list[CandidateRecord]:
    require_table_columns(
        conn,
        "product_cover_candidates",
        ["product_id", "shop_id", "ean", "product_url", "image_url", "source_type", "source_rank", "is_primary"],
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                c.product_id,
                c.ean,
                c.shop_id,
                s.domain,
                s.name,
                c.product_url,
                c.image_url,
                c.source_type,
                c.source_rank,
                c.is_primary,
                c.mime_type,
                c.width,
                c.height,
                c.last_seen_at
            from public.product_cover_candidates c
            left join public.shops s
              on s.id = c.shop_id
            where c.product_id = %s
            order by c.source_rank desc, c.last_seen_at desc nulls last, c.updated_at desc nulls last
            """,
            (product_context["product_id"],),
        )
        rows = cur.fetchall()

    candidates: list[CandidateRecord] = []
    for row in rows:
        candidate = CandidateRecord(
            product_id=str(row[0]),
            ean=normalize_text(row[1]) or product_context["ean"],
            shop_id=str(row[2]) if row[2] is not None else None,
            shop_domain=normalize_text(row[3]),
            shop_name=normalize_text(row[4]) or None,
            product_url=normalize_text(row[5]),
            image_url=normalize_text(row[6]),
            source_type=normalize_text(row[7]) or "unknown",
            source_rank=int(row[8] or 0),
            is_primary=bool(row[9]),
            mime_type=normalize_text(row[10]) or None,
            width=int(row[11]) if row[11] is not None else None,
            height=int(row[12]) if row[12] is not None else None,
        )
        if candidate.source_rank <= 0:
            candidate.source_rank = rank_candidate(candidate, recency_reference=safe_parse_datetime(row[13]))
        candidates.append(candidate)
    return candidates


def discover_candidates_on_the_fly(conn, product_context: dict[str, Any], session) -> list[CandidateRecord]:
    discovered: list[CandidateRecord] = []
    for offer in product_context["offers"]:
        try:
            raw_candidates, _, _ = fetch_page_candidates(session, offer["product_url"])
        except Exception:
            continue
        for item in raw_candidates:
            candidate = CandidateRecord(
                product_id=product_context["product_id"],
                ean=product_context["ean"],
                shop_id=offer["shop_id"],
                shop_domain=offer["shop_domain"],
                shop_name=offer["shop_name"],
                product_url=offer["product_url"],
                image_url=normalize_text(item.get("image_url")),
                source_type=normalize_text(item.get("source_type")) or "unknown",
                source_rank=0,
                is_primary=bool(item.get("is_primary")),
                mime_type=None,
                width=item.get("width"),
                height=item.get("height"),
            )
            candidate.source_rank = rank_candidate(candidate, recency_reference=offer["last_seen_at"])
            discovered.append(candidate)

    if not discovered:
        return []

    with conn.cursor() as cur:
        for candidate in discovered:
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
                on conflict (product_id, image_url) where product_id is not null and image_url is not null
                do update set
                    source_type = excluded.source_type,
                    source_rank = greatest(excluded.source_rank, public.product_cover_candidates.source_rank),
                    is_primary = public.product_cover_candidates.is_primary or excluded.is_primary,
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
    return discovered


def mark_candidate_result(conn, product_id: str, image_url: str, *, status: str, mime_type: str | None, width: int | None, height: int | None, error: str | None = None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.product_cover_candidates
               set candidate_status = %s,
                   mime_type = coalesce(%s, mime_type),
                   width = coalesce(%s, width),
                   height = coalesce(%s, height),
                   last_checked_at = now(),
                   last_error_message = %s,
                   updated_at = now()
             where product_id = %s
               and image_url = %s
            """,
            (status, mime_type, width, height, error, product_id, image_url),
        )


def build_product_update_statement(product_columns: set[str], payload: dict[str, Any]) -> tuple[str, list[Any]]:
    assignments: list[str] = []
    values: list[Any] = []
    ordered = [
        ("cover_storage_path", payload.get("cover_storage_path")),
        ("cover_source", payload.get("cover_source")),
        ("cover_source_url", payload.get("cover_source_url")),
        ("cover_status", payload.get("cover_status")),
        ("cover_confidence", payload.get("cover_confidence")),
        ("cover_last_attempt_at", payload.get("cover_last_attempt_at")),
        ("cover_source_shop_id", payload.get("cover_source_shop_id")),
        ("cover_width", payload.get("cover_width")),
        ("cover_height", payload.get("cover_height")),
        ("cover_url", payload.get("cover_url")),
    ]
    for column, value in ordered:
        if column in product_columns:
            assignments.append(f"{column} = %s")
            values.append(value)
    if "updated_at" in product_columns:
        assignments.append("updated_at = now()")
    if not assignments:
        raise CoverPipelineError("Tabel public.products bevat geen bruikbare cover-kolommen.")
    return ", ".join(assignments), values


def publish_cover(conn, product_context: dict[str, Any], candidate: CandidateRecord, session) -> dict[str, Any]:
    _, _, bucket, prefix = get_supabase_credentials()
    product_columns = get_table_columns(conn, "products")

    binary, original_mime_type = fetch_binary(session, candidate.image_url)
    prepared = prepare_image_for_storage(binary, original_mime_type=original_mime_type)
    remote_path = build_storage_path(prefix, product_context["ean"], prepared.sha256, prepared.extension)
    public_url = upload_bytes_to_storage(remote_path, prepared)

    update_payload = {
        "cover_storage_path": remote_path,
        "cover_source": f"shop:{candidate.shop_domain}:{candidate.source_type}",
        "cover_source_url": candidate.image_url,
        "cover_status": "published",
        "cover_confidence": min(100, max(0, candidate.source_rank)),
        "cover_last_attempt_at": utc_now(),
        "cover_source_shop_id": candidate.shop_id,
        "cover_width": prepared.width,
        "cover_height": prepared.height,
        "cover_url": public_url,
    }
    assignments_sql, values = build_product_update_statement(product_columns, update_payload)

    with conn.cursor() as cur:
        cur.execute(
            f"update public.products set {assignments_sql} where id = %s",
            [*values, product_context["product_id"]],
        )
        cur.execute(
            """
            update public.product_cover_queue
               set status = 'published',
                   attempt_count = coalesce(attempt_count, 0) + 1,
                   last_completed_at = now(),
                   last_error_code = null,
                   last_error_message = null,
                   claimed_at = null,
                   claimed_by = null,
                   next_attempt_at = null,
                   updated_at = now()
             where product_id = %s
            """,
            (product_context["product_id"],),
        )
    mark_candidate_result(
        conn,
        product_context["product_id"],
        candidate.image_url,
        status="published",
        mime_type=prepared.mime_type,
        width=prepared.width,
        height=prepared.height,
        error=None,
    )
    return {
        "bucket": bucket,
        "remote_path": remote_path,
        "public_url": public_url,
        "mime_type": prepared.mime_type,
        "width": prepared.width,
        "height": prepared.height,
        "sha256": prepared.sha256,
    }


def mark_job_failure(conn, product_id: str, *, error_code: str, error_message: str, permanent: bool) -> None:
    status = "failed" if permanent else "retry_later"
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.product_cover_queue
               set status = %s,
                   attempt_count = coalesce(attempt_count, 0) + 1,
                   last_error_code = %s,
                   last_error_message = %s,
                   claimed_at = null,
                   claimed_by = null,
                   next_attempt_at = %s,
                   updated_at = now()
             where product_id = %s
            """,
            (status, error_code, error_message[:2000], None if permanent else next_retry_timestamp(), product_id),
        )


def main() -> None:
    args = parse_args()
    worker_id = normalize_text(args.worker_id) or f"cover-worker-{utc_now().strftime('%Y%m%dT%H%M%SZ')}"
    output_path = Path(args.output_json)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ensure_runtime_directories()

    summary: dict[str, Any] = {
        "started_at": utc_now().isoformat(),
        "mode": args.mode,
        "limit": args.limit,
        "worker_id": worker_id,
        "claimed": 0,
        "published": 0,
        "failed": 0,
        "retried_later": 0,
        "jobs": [],
    }

    conn = connect_db()
    conn.autocommit = False
    session = make_session()
    try:
        jobs = claim_jobs(conn, args.mode, args.limit, worker_id)
        summary["claimed"] = len(jobs)
        conn.commit()

        for job in jobs:
            job_result: dict[str, Any] = {**job, "status": "processing", "attempts": []}
            try:
                product_context = load_product_context(conn, job["product_id"])
                candidates = load_candidates(conn, product_context)
                if not candidates:
                    candidates = discover_candidates_on_the_fly(conn, product_context, session)
                    conn.commit()
                if not candidates:
                    raise CoverPipelineError("Geen bruikbare cover candidates gevonden voor product.")

                ranked_candidates = sorted(candidates, key=lambda item: item.source_rank, reverse=True)
                published_payload = None
                last_error = None
                for candidate in ranked_candidates:
                    try:
                        published_payload = publish_cover(conn, product_context, candidate, session)
                        conn.commit()
                        job_result["status"] = "published"
                        job_result["published_candidate"] = asdict(candidate)
                        job_result["asset"] = published_payload
                        summary["published"] += 1
                        break
                    except Exception as exc:
                        conn.rollback()
                        last_error = str(exc)
                        try:
                            mark_candidate_result(
                                conn,
                                product_context["product_id"],
                                candidate.image_url,
                                status="failed",
                                mime_type=None,
                                width=None,
                                height=None,
                                error=last_error,
                            )
                            conn.commit()
                        except Exception:
                            conn.rollback()
                        job_result["attempts"].append(
                            {
                                "candidate": asdict(candidate),
                                "error": last_error,
                            }
                        )
                if published_payload is None:
                    mark_job_failure(
                        conn,
                        product_context["product_id"],
                        error_code="no_candidate_succeeded",
                        error_message=last_error or "Geen candidate kon gepubliceerd worden.",
                        permanent=False,
                    )
                    conn.commit()
                    job_result["status"] = "retry_later"
                    summary["retried_later"] += 1
            except Exception as exc:
                conn.rollback()
                error_message = str(exc)
                try:
                    mark_job_failure(
                        conn,
                        job["product_id"],
                        error_code="job_failure",
                        error_message=error_message,
                        permanent=False,
                    )
                    conn.commit()
                except Exception:
                    conn.rollback()
                job_result["status"] = "retry_later"
                job_result["fatal_error"] = error_message
                summary["retried_later"] += 1
            summary["jobs"].append(job_result)
    except Exception as exc:
        conn.rollback()
        summary["fatal_error"] = str(exc)
        output_path.write_text(serialize_json(summary), encoding="utf-8")
        raise
    finally:
        conn.close()

    summary["finished_at"] = utc_now().isoformat()
    output_path.write_text(serialize_json(summary), encoding="utf-8")
    log(
        f"[DONE] cover worker klaar | claimed={summary['claimed']} | published={summary['published']} | retry_later={summary['retried_later']}"
    )


if __name__ == "__main__":
    main()
