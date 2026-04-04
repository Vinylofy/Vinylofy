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


def get_candidate_table_profile(conn) -> tuple[set[str], list[str], str]:
    columns = get_table_columns(conn, "product_cover_candidates")
    url_columns = [column for column in ("image_url", "source_url", "candidate_url") if column in columns]
    if not url_columns:
        raise CoverPipelineError("Tabel public.product_cover_candidates mist image_url/source_url/candidate_url.")
    preferred_url_column = "image_url" if "image_url" in columns else url_columns[0]
    return columns, url_columns, preferred_url_column


def candidate_url_expression(alias: str, url_columns: list[str]) -> str:
    if len(url_columns) == 1:
        return f"{alias}.{url_columns[0]}"
    return "coalesce(" + ", ".join(f"{alias}.{column}" for column in url_columns) + ")"


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


def insert_candidate_row(cur, payload: dict[str, Any]) -> None:
    columns = list(payload.keys())
    cur.execute(
        f"insert into public.product_cover_candidates ({', '.join(columns)}) values ({', '.join(['%s'] * len(columns))})",
        [payload[column] for column in columns],
    )


def update_candidate_row(cur, row_id: Any, candidate: CandidateRecord, existing: dict[str, Any], columns: set[str]) -> None:
    now = utc_now()
    payload: dict[str, Any] = {}
    if "source_type" in columns and candidate.source_type:
        payload["source_type"] = candidate.source_type
    if "source_rank" in columns:
        payload["source_rank"] = max(int(existing.get("source_rank") or 0), int(candidate.source_rank or 0))
    if "is_primary" in columns:
        payload["is_primary"] = bool(existing.get("is_primary")) or bool(candidate.is_primary)
    if "width" in columns:
        payload["width"] = existing.get("width") or candidate.width
    if "height" in columns:
        payload["height"] = existing.get("height") or candidate.height
    if "mime_type" in columns and candidate.mime_type and not existing.get("mime_type"):
        payload["mime_type"] = candidate.mime_type
    if "last_seen_at" in columns:
        payload["last_seen_at"] = now
    if "updated_at" in columns:
        payload["updated_at"] = now
    assignments = ", ".join(f"{column} = %s" for column in payload)
    cur.execute(f"update public.product_cover_candidates set {assignments} where id = %s", [*payload.values(), row_id])


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
    columns, url_columns, _ = get_candidate_table_profile(conn)
    require_table_columns(
        conn,
        "product_cover_candidates",
        ["product_id", "shop_id", "ean", "product_url", "source_type", "source_rank", "is_primary"],
    )
    url_expr = candidate_url_expression("c", url_columns)
    mime_expr = "c.mime_type" if "mime_type" in columns else "null"
    width_expr = "c.width" if "width" in columns else "null"
    height_expr = "c.height" if "height" in columns else "null"
    last_seen_expr = "c.last_seen_at" if "last_seen_at" in columns else "null"
    updated_expr = "c.updated_at" if "updated_at" in columns else "null"
    with conn.cursor() as cur:
        cur.execute(
            f"""
            select
                c.product_id,
                c.ean,
                c.shop_id,
                s.domain,
                s.name,
                c.product_url,
                {url_expr} as image_url,
                c.source_type,
                c.source_rank,
                c.is_primary,
                {mime_expr} as mime_type,
                {width_expr} as width,
                {height_expr} as height,
                {last_seen_expr} as last_seen_at,
                {updated_expr} as updated_at
            from public.product_cover_candidates c
            left join public.shops s
              on s.id = c.shop_id
            where c.product_id = %s
            order by c.source_rank desc, {last_seen_expr} desc nulls last, {updated_expr} desc nulls last
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

    columns, url_columns, _ = get_candidate_table_profile(conn)
    with conn.cursor() as cur:
        for candidate in discovered:
            existing = find_existing_candidate(cur, candidate.product_id, candidate.image_url, url_columns, columns)
            if existing is None:
                insert_candidate_row(cur, build_candidate_insert_payload(candidate, columns))
            else:
                update_candidate_row(cur, existing["id"], candidate, existing, columns)
    return discovered


def mark_candidate_result(conn, product_id: str, image_url: str, *, status: str, mime_type: str | None, width: int | None, height: int | None, error: str | None = None) -> None:
    columns, url_columns, _ = get_candidate_table_profile(conn)
    payload: dict[str, Any] = {}
    if "candidate_status" in columns:
        payload["candidate_status"] = status
    if "mime_type" in columns and mime_type:
        payload["mime_type"] = mime_type
    if "width" in columns and width is not None:
        payload["width"] = width
    if "height" in columns and height is not None:
        payload["height"] = height
    if "last_checked_at" in columns:
        payload["last_checked_at"] = utc_now()
    if "last_error_message" in columns:
        payload["last_error_message"] = error
    if "updated_at" in columns:
        payload["updated_at"] = utc_now()
    if not payload:
        return
    predicates = " or ".join(f"{column} = %s" for column in url_columns)
    with conn.cursor() as cur:
        assignments = ", ".join(f"{column} = %s" for column in payload)
        cur.execute(
            f"update public.product_cover_candidates set {assignments} where product_id = %s and ({predicates})",
            [*payload.values(), product_id, *([image_url] * len(url_columns))],
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
