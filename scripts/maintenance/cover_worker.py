#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import os
from pathlib import Path
from uuid import UUID
import socket
from typing import Any
from urllib.parse import urlsplit

from cover_common import (
    CandidateRecord,
    CoverPipelineError,
    build_product_storage_path,
    build_public_storage_url,
    compensate_storage_upload,
    connect_db,
    download_binary,
    ensure_runtime_directories,
    get_storage_bucket_api,
    inspect_storage_object,
    make_session,
    next_retry_timestamp,
    normalize_ean,
    normalize_source_type,
    normalize_text,
    prepare_image_for_storage,
    rank_candidate,
    require_table_columns,
    serialize_json,
    upsert_bytes_to_storage,
    utc_now,
)


QUEUEABLE_STATUSES = ("pending", "retry_later", "review")
RETRYABLE_CANDIDATE_STATUSES = ("pending", "accepted", "published")
MAX_STORAGE_OBJECT_BYTES = 5 * 1024 * 1024

RETRY_FAILED_CANDIDATE_STATUSES = (
    "pending",
    "accepted",
    "published",
    "failed",
)


@dataclass(slots=True)
class CoverJob:
    queue_id: str
    product_id: str
    ean: str
    artist: str
    title: str
    format_label: str
    source_reason: str
    priority: int
    attempt_count: int


@dataclass(slots=True)
class ProductState:
    product_id: str
    ean: str
    artist: str
    title: str
    format_label: str
    cover_status: str
    cover_url: str
    cover_storage_path: str
    cover_source_url: str
    cover_source_shop_id: str | None
    cover_sha256: str
    cover_mime_type: str
    cover_byte_size: int | None
    cover_width: int | None
    cover_height: int | None
    cover_needs_refresh: bool
    cover_fail_count: int


@dataclass(slots=True)
class CandidateChoice:
    candidate_id: str
    candidate: CandidateRecord
    candidate_status: str


@dataclass(slots=True)
class LocalObject:
    remote_path: str
    public_url: str
    mime_type: str | None
    byte_size: int | None


def default_worker_id() -> str:
    return (
        f"cover-worker:{socket.gethostname()}:{os.getpid()}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Claim, validate and publish central Vinylofy product covers."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("publish", "retry-failed"),
        default="publish",
        help=(
            "publish verwerkt normale queue-items; retry-failed zet eerst "
            "een beperkte batch failed-items opnieuw klaar."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=25,
        help="Maximum aantal claims; iedere iteratie claimt maximaal één item.",
    )
    parser.add_argument(
        "--worker-id",
        type=str,
        default="",
        help="Expliciete worker-ID. Standaard hostname en proces-ID.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=5,
        help="Na dit aantal mislukte claims wordt de queue-status failed.",
    )
    parser.add_argument(
        "--stale-after-minutes",
        type=int,
        default=90,
        help="Leeftijd waarna een processing-claim wordt vrijgegeven.",
    )
    parser.add_argument(
        "--reconcile-limit",
        type=int,
        default=100,
        help="Maximum bestaande lokale producten om zonder download te herstellen.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Toont selectie en acties zonder claims, databasewrites, "
            "downloads of Storage-aanroepen."
        ),
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="output/cover_pipeline/cover_publish_summary.json",
        help="Pad voor het JSON-runrapport.",
    )
    parser.add_argument(
        "--product-id",
        type=str,
        default="",
        help=(
            "Maintenance/testmodus: claim uitsluitend exact dit "
            "products.id. Vereist --mode publish en --limit 1. "
            "Globale recovery/reconciliation wordt overgeslagen."
        ),
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    for name in (
        "limit",
        "max_attempts",
        "stale_after_minutes",
        "reconcile_limit",
    ):
        if int(getattr(args, name)) <= 0:
            raise CoverPipelineError(
                f"--{name.replace('_', '-')} moet groter zijn dan nul."
            )

    if args.product_id:
        try:
            UUID(args.product_id)
        except ValueError as exc:
            raise CoverPipelineError(
                "--product-id moet een geldige UUID zijn."
            ) from exc

        if args.mode != "publish":
            raise CoverPipelineError(
                "--product-id is uitsluitend toegestaan "
                "met --mode publish."
            )

        if args.limit != 1:
            raise CoverPipelineError(
                "--product-id vereist exact --limit 1."
            )

        if args.dry_run:
            raise CoverPipelineError(
                "--product-id wordt in deze onderhoudsroute "
                "niet gecombineerd met --dry-run."
            )


def require_schema(conn) -> None:
    require_table_columns(
        conn,
        "products",
        [
            "id",
            "ean",
            "cover_url",
            "cover_storage_path",
            "cover_source_url",
            "cover_source_shop_id",
            "cover_status",
            "cover_confidence",
            "cover_sha256",
            "cover_mime_type",
            "cover_byte_size",
            "cover_needs_refresh",
            "cover_fail_count",
            "cover_last_attempt_at",
            "cover_last_success_at",
            "cover_locked_at",
            "cover_locked_by",
            "cover_error_code",
            "cover_error_message",
            "cover_width",
            "cover_height",
        ],
    )
    require_table_columns(
        conn,
        "product_cover_candidates",
        [
            "id",
            "product_id",
            "image_url",
            "source_type",
            "source_rank",
            "candidate_status",
            "is_selected",
            "mime_type",
            "width",
            "height",
            "byte_size",
            "sha256",
            "last_checked_at",
            "last_http_status",
            "last_error_code",
            "last_error_message",
            "updated_at",
        ],
    )
    require_table_columns(
        conn,
        "product_cover_queue",
        [
            "id",
            "product_id",
            "priority",
            "candidate_count",
            "source_reason",
            "status",
            "attempt_count",
            "last_error_code",
            "last_error_message",
            "claimed_by",
            "claimed_at",
            "next_attempt_at",
            "last_completed_at",
            "updated_at",
        ],
    )


def preview_jobs(
    conn,
    *,
    mode: str,
    limit: int,
) -> list[dict[str, Any]]:
    statuses = (
        (*QUEUEABLE_STATUSES, "failed")
        if mode == "retry-failed"
        else QUEUEABLE_STATUSES
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                q.id,
                q.product_id,
                p.ean,
                p.artist,
                p.title,
                p.format_label,
                q.source_reason,
                q.priority,
                q.attempt_count,
                q.status,
                (
                    select count(*)
                    from public.product_cover_candidates c
                    where c.product_id = p.id
                      and c.candidate_status <> 'rejected'
                      and nullif(btrim(c.image_url), '') is not null
                ) as candidate_count
            from public.product_cover_queue q
            join public.products p
              on p.id = q.product_id
            where q.status = any(%s)
              and (
                    q.next_attempt_at is null
                    or q.next_attempt_at <= now()
              )
              and p.cover_status <> 'blocked'
              and nullif(btrim(p.cover_storage_path), '') is null
              and public.normalize_cover_ean(p.ean) is not null
            order by q.priority desc, q.updated_at asc, q.id
            limit %s
            """,
            (list(statuses), limit),
        )
        rows = cur.fetchall()

    return [
        {
            "queue_id": str(row[0]),
            "product_id": str(row[1]),
            "ean": normalize_text(row[2]),
            "artist": normalize_text(row[3]),
            "title": normalize_text(row[4]),
            "format_label": normalize_text(row[5]),
            "source_reason": normalize_text(row[6]),
            "priority": int(row[7] or 0),
            "attempt_count": int(row[8] or 0),
            "queue_status": normalize_text(row[9]),
            "candidate_count": int(row[10] or 0),
            "planned_action": "claim_one_then_preflight",
        }
        for row in rows
    ]



def preview_local_actions(
    conn,
    *,
    mode: str,
    limit: int,
    max_attempts: int,
) -> list[dict[str, Any]]:
    statuses = (
        (*QUEUEABLE_STATUSES, "published", "failed")
        if mode == "retry-failed"
        else (*QUEUEABLE_STATUSES, "published")
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                p.id,
                p.ean,
                p.cover_storage_path,
                p.cover_url,
                p.cover_status,
                p.cover_needs_refresh,
                p.cover_fail_count,
                q.status,
                q.attempt_count,
                case
                    when p.cover_needs_refresh is true
                        then 'claim_local_repair_then_exact_path_preflight'
                    else 'reconcile_existing_local_object'
                end as planned_action
            from public.products p
            left join public.product_cover_queue q
              on q.product_id = p.id
            where p.cover_status <> 'blocked'
              and nullif(btrim(p.cover_storage_path), '') is not null
              and (
                    (
                        p.cover_needs_refresh is true
                        and (
                            %s
                            or coalesce(p.cover_fail_count, 0) < %s
                        )
                        and (
                            q.status is null
                            or q.status = any(%s)
                        )
                        and (
                            q.next_attempt_at is null
                            or q.next_attempt_at <= now()
                        )
                    )
                    or (
                        p.cover_needs_refresh is false
                        and (
                            nullif(btrim(p.cover_url), '') is null
                            or p.cover_status <> 'ready'
                            or p.cover_url not like (
                                '%/storage/v1/object/public/product-covers/'
                                || p.cover_storage_path
                            )
                        )
                    )
              )
            order by
                p.cover_needs_refresh desc,
                p.cover_priority desc,
                p.updated_at asc,
                p.id
            limit %s
            """,
            (
                mode == "retry-failed",
                max_attempts,
                list(statuses),
                limit,
            ),
        )
        rows = cur.fetchall()

    return [
        {
            "product_id": str(row[0]),
            "ean": normalize_text(row[1]),
            "storage_path": normalize_text(row[2]),
            "cover_url": normalize_text(row[3]),
            "cover_status": normalize_text(row[4]),
            "cover_needs_refresh": bool(row[5]),
            "cover_fail_count": int(row[6] or 0),
            "queue_status": normalize_text(row[7]),
            "attempt_count": int(row[8] or 0),
            "planned_action": normalize_text(row[9]),
        }
        for row in rows
    ]

def count_stale_claims(
    conn,
    *,
    stale_after_minutes: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            select count(*)
            from public.product_cover_queue
            where status = 'processing'
              and claimed_at is not null
              and claimed_at < now() - make_interval(mins => %s)
            """,
            (stale_after_minutes,),
        )
        row = cur.fetchone()
    return int(row[0] or 0) if row else 0


def recover_stale_claims(
    conn,
    *,
    stale_after_minutes: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            select public.recover_stale_cover_claims(
                make_interval(mins => %s)
            )
            """,
            (stale_after_minutes,),
        )
        row = cur.fetchone()
    conn.commit()
    return int(row[0] or 0) if row else 0


def requeue_failed_jobs(
    conn,
    *,
    limit: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            with selected as (
                select q.id
                from public.product_cover_queue q
                join public.products p
                  on p.id = q.product_id
                where q.status = 'failed'
                  and p.cover_status <> 'blocked'
                  and nullif(btrim(p.cover_storage_path), '') is null
                  and public.normalize_cover_ean(p.ean) is not null
                order by q.priority desc, q.updated_at asc, q.id
                limit %s
                for update of q skip locked
            ),
            queue_update as (
                update public.product_cover_queue q
                set
                    status = 'retry_later',
                    claimed_by = null,
                    claimed_at = null,
                    next_attempt_at = now(),
                    last_error_code = null,
                    last_error_message = null,
                    updated_at = now()
                from selected s
                where q.id = s.id
                returning q.product_id
            )
            update public.products p
            set
                cover_status = 'queued',
                cover_locked_at = null,
                cover_locked_by = null,
                cover_error_code = null,
                cover_error_message = null,
                updated_at = now()
            from queue_update q
            where p.id = q.product_id
            returning p.id
            """,
            (limit,),
        )
        rows = cur.fetchall()
    conn.commit()
    return len(rows)



def claim_one_local_repair(
    conn,
    *,
    worker_id: str,
    stale_after_minutes: int,
    max_attempts: int,
    retry_failed: bool,
) -> CoverJob | None:
    statuses = (
        (*QUEUEABLE_STATUSES, "published", "failed")
        if retry_failed
        else (*QUEUEABLE_STATUSES, "published")
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            with selected as (
                select p.id
                from public.products p
                left join public.product_cover_queue existing_q
                  on existing_q.product_id = p.id
                where p.cover_status <> 'blocked'
                  and nullif(btrim(p.cover_storage_path), '') is not null
                  and p.cover_needs_refresh is true
                  and (
                        %s
                        or coalesce(p.cover_fail_count, 0) < %s
                  )
                  and public.normalize_cover_ean(p.ean) is not null
                  and (
                        existing_q.status is null
                        or existing_q.status = any(%s)
                  )
                  and (
                        existing_q.next_attempt_at is null
                        or existing_q.next_attempt_at <= now()
                  )
                  and (
                        p.cover_locked_at is null
                        or p.cover_locked_at
                           < now() - make_interval(mins => %s)
                  )
                order by
                    p.cover_priority desc,
                    p.updated_at asc,
                    p.id
                limit 1
                for update of p skip locked
            ),
            queue_claim as (
                insert into public.product_cover_queue (
                    product_id,
                    priority,
                    candidate_count,
                    source_reason,
                    status,
                    attempt_count,
                    claimed_by,
                    claimed_at,
                    next_attempt_at,
                    updated_at
                )
                select
                    p.id,
                    greatest(0, coalesce(p.cover_priority, 0)),
                    (
                        select count(*)::integer
                        from public.product_cover_candidates c
                        where c.product_id = p.id
                          and c.candidate_status <> 'rejected'
                          and nullif(btrim(c.image_url), '') is not null
                    ),
                    'local_repair',
                    'processing',
                    1,
                    %s,
                    now(),
                    null,
                    now()
                from selected s
                join public.products p
                  on p.id = s.id
                on conflict (product_id) do update
                set
                    priority = greatest(
                        public.product_cover_queue.priority,
                        excluded.priority
                    ),
                    candidate_count = excluded.candidate_count,
                    source_reason = 'local_repair',
                    status = 'processing',
                    attempt_count = (
                        coalesce(
                            public.product_cover_queue.attempt_count,
                            0
                        ) + 1
                    ),
                    claimed_by = excluded.claimed_by,
                    claimed_at = excluded.claimed_at,
                    next_attempt_at = null,
                    last_error_code = null,
                    last_error_message = null,
                    updated_at = now()
                where public.product_cover_queue.status <> 'processing'
                returning
                    id,
                    product_id,
                    priority,
                    attempt_count
            ),
            product_claim as (
                update public.products p
                set
                    cover_status = 'resolving',
                    cover_last_attempt_at = now(),
                    cover_locked_at = now(),
                    cover_locked_by = %s,
                    updated_at = now()
                from queue_claim q
                where p.id = q.product_id
                returning
                    p.id,
                    p.ean,
                    p.artist,
                    p.title,
                    p.format_label
            )
            select
                q.id,
                q.product_id,
                p.ean,
                p.artist,
                p.title,
                p.format_label,
                'local_repair',
                q.priority,
                q.attempt_count
            from queue_claim q
            join product_claim p
              on p.id = q.product_id
            """,
            (
                retry_failed,
                max_attempts,
                list(statuses),
                stale_after_minutes,
                worker_id,
                worker_id,
            ),
        )
        row = cur.fetchone()
    conn.commit()

    if row is None:
        return None
    return CoverJob(
        queue_id=str(row[0]),
        product_id=str(row[1]),
        ean=normalize_text(row[2]),
        artist=normalize_text(row[3]),
        title=normalize_text(row[4]),
        format_label=normalize_text(row[5]),
        source_reason="local_repair",
        priority=int(row[7] or 0),
        attempt_count=int(row[8] or 0),
    )

def claim_one_job(
    conn,
    *,
    worker_id: str,
) -> CoverJob | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                queue_id,
                product_id,
                ean,
                artist,
                title,
                format_label,
                trigger_source,
                requested_priority,
                attempts
            from public.claim_next_cover_job(%s)
            """,
            (worker_id,),
        )
        row = cur.fetchone()
    conn.commit()

    if row is None:
        return None
    return CoverJob(
        queue_id=str(row[0]),
        product_id=str(row[1]),
        ean=normalize_text(row[2]),
        artist=normalize_text(row[3]),
        title=normalize_text(row[4]),
        format_label=normalize_text(row[5]),
        source_reason=normalize_text(row[6]),
        priority=int(row[7] or 0),
        attempt_count=int(row[8] or 0),
    )


def claim_one_job_for_product(
    conn,
    *,
    worker_id: str,
    product_id: str,
) -> CoverJob | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            with target_job as (
                select q.id
                from public.product_cover_queue q
                join public.products p
                  on p.id = q.product_id
                where q.product_id = %s
                  and q.status in (
                        'pending',
                        'retry_later',
                        'review'
                  )
                  and (
                        q.next_attempt_at is null
                        or q.next_attempt_at <= now()
                  )
                  and p.cover_status <> 'blocked'
                  and nullif(
                        btrim(p.cover_storage_path),
                        ''
                      ) is null
                  and public.normalize_cover_ean(p.ean)
                      is not null
                limit 1
                for update of q skip locked
            ),
            queue_update as (
                update public.product_cover_queue q
                set
                    status = 'processing',
                    claimed_by = %s,
                    claimed_at = now(),
                    attempt_count = coalesce(
                        q.attempt_count,
                        0
                    ) + 1,
                    updated_at = now()
                from target_job t
                where q.id = t.id
                returning
                    q.id,
                    q.product_id,
                    q.source_reason,
                    q.priority,
                    q.attempt_count
            ),
            product_update as (
                update public.products p
                set
                    cover_status = 'resolving',
                    cover_last_attempt_at = now(),
                    cover_locked_at = now(),
                    cover_locked_by = %s,
                    updated_at = now()
                from queue_update q
                where p.id = q.product_id
                returning
                    p.id,
                    p.ean,
                    p.artist,
                    p.title,
                    p.format_label
            )
            select
                q.id,
                q.product_id,
                p.ean,
                p.artist,
                p.title,
                p.format_label,
                q.source_reason,
                q.priority,
                q.attempt_count
            from queue_update q
            join product_update p
              on p.id = q.product_id
            """,
            (
                product_id,
                worker_id,
                worker_id,
            ),
        )
        row = cur.fetchone()

    conn.commit()

    if row is None:
        return None

    return CoverJob(
        queue_id=str(row[0]),
        product_id=str(row[1]),
        ean=normalize_text(row[2]),
        artist=normalize_text(row[3]),
        title=normalize_text(row[4]),
        format_label=normalize_text(row[5]),
        source_reason=normalize_text(row[6]),
        priority=int(row[7] or 0),
        attempt_count=int(row[8] or 0),
    )


def load_product_state(
    conn,
    product_id: str,
) -> ProductState:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                id,
                ean,
                artist,
                title,
                format_label,
                cover_status,
                cover_url,
                cover_storage_path,
                cover_source_url,
                cover_source_shop_id,
                cover_sha256,
                cover_mime_type,
                cover_byte_size,
                cover_width,
                cover_height,
                cover_needs_refresh,
                cover_fail_count
            from public.products
            where id = %s
            limit 1
            """,
            (product_id,),
        )
        row = cur.fetchone()
    if row is None:
        raise CoverPipelineError(
            f"Product ontbreekt na claim: {product_id}"
        )

    return ProductState(
        product_id=str(row[0]),
        ean=normalize_ean(row[1]) or "",
        artist=normalize_text(row[2]),
        title=normalize_text(row[3]),
        format_label=normalize_text(row[4]),
        cover_status=normalize_text(row[5]),
        cover_url=normalize_text(row[6]),
        cover_storage_path=normalize_text(row[7]),
        cover_source_url=normalize_text(row[8]),
        cover_source_shop_id=(
            str(row[9]) if row[9] is not None else None
        ),
        cover_sha256=normalize_text(row[10]),
        cover_mime_type=normalize_text(row[11]),
        cover_byte_size=(
            int(row[12]) if row[12] is not None else None
        ),
        cover_width=(
            int(row[13]) if row[13] is not None else None
        ),
        cover_height=(
            int(row[14]) if row[14] is not None else None
        ),
        cover_needs_refresh=bool(row[15]),
        cover_fail_count=int(row[16] or 0),
    )


def load_candidates(
    conn,
    product: ProductState,
    *,
    retry_failed: bool,
) -> list[CandidateChoice]:
    statuses = (
        RETRY_FAILED_CANDIDATE_STATUSES
        if retry_failed
        else RETRYABLE_CANDIDATE_STATUSES
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                c.id,
                c.product_id,
                c.ean,
                c.shop_id,
                coalesce(s.domain, ''),
                s.name,
                c.product_url,
                c.image_url,
                c.source_type,
                c.source_rank,
                c.is_primary,
                c.mime_type,
                c.width,
                c.height,
                c.candidate_status,
                c.last_seen_at
            from public.product_cover_candidates c
            left join public.shops s
              on s.id = c.shop_id
            where c.product_id = %s
              and c.candidate_status = any(%s)
            order by
                c.is_selected desc,
                c.source_rank desc,
                c.is_primary desc,
                c.last_seen_at desc nulls last,
                c.updated_at desc,
                c.id
            """,
            (product.product_id, list(statuses)),
        )
        rows = cur.fetchall()

    choices: list[CandidateChoice] = []
    seen_urls: set[str] = set()
    for row in rows:
        image_url = normalize_text(row[7])
        parts = urlsplit(image_url)
        if (
            parts.scheme.lower() not in {"http", "https"}
            or not parts.netloc
            or image_url in seen_urls
        ):
            continue
        seen_urls.add(image_url)

        candidate = CandidateRecord(
            product_id=str(row[1]),
            ean=normalize_ean(row[2]) or product.ean,
            shop_id=str(row[3]) if row[3] is not None else None,
            shop_domain=normalize_text(row[4]),
            shop_name=normalize_text(row[5]) or None,
            product_url=normalize_text(row[6]) or image_url,
            image_url=image_url,
            source_type=normalize_source_type(
                normalize_text(row[8])
            ),
            source_rank=int(row[9] or 0),
            is_primary=bool(row[10]),
            mime_type=normalize_text(row[11]) or None,
            width=int(row[12]) if row[12] is not None else None,
            height=int(row[13]) if row[13] is not None else None,
        )
        if candidate.source_rank <= 0:
            candidate.source_rank = rank_candidate(
                candidate,
                recency_reference=row[15],
            )
        choices.append(
            CandidateChoice(
                candidate_id=str(row[0]),
                candidate=candidate,
                candidate_status=normalize_text(row[14]),
            )
        )
    return choices


def _metadata_mime_type(metadata: dict[str, Any]) -> str | None:
    value = normalize_text(
        metadata.get("mimetype")
        or metadata.get("contentType")
        or metadata.get("content_type")
    ).lower()
    return value or None


def _metadata_byte_size(metadata: dict[str, Any]) -> int | None:
    value = metadata.get("size")
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def preflight_local_object(
    remote_path: str,
) -> LocalObject | None:
    normalized_path = normalize_text(remote_path).strip("/")
    if not normalized_path:
        return None

    supabase_url, bucket, bucket_api = get_storage_bucket_api()
    state = inspect_storage_object(
        normalized_path,
        bucket_api=bucket_api,
    )
    if not state.exists:
        return None

    mime_type = _metadata_mime_type(state.metadata)
    if mime_type and mime_type not in {
        "image/jpeg",
        "image/png",
        "image/webp",
    }:
        return None

    return LocalObject(
        remote_path=state.remote_path,
        public_url=build_public_storage_url(
            supabase_url,
            bucket,
            state.remote_path,
        ),
        mime_type=mime_type,
        byte_size=_metadata_byte_size(state.metadata),
    )


def candidate_for_reused_object(
    choices: list[CandidateChoice],
) -> CandidateChoice | None:
    return choices[0] if choices else None


def publish_database_state(
    conn,
    *,
    job: CoverJob,
    product: ProductState,
    selected: CandidateChoice | None,
    remote_path: str,
    public_url: str,
    source_url: str,
    source_shop_id: str | None,
    sha256: str | None,
    mime_type: str | None,
    byte_size: int | None,
    width: int | None,
    height: int | None,
    worker_id: str,
) -> None:
    confidence = (
        min(100, max(0, selected.candidate.source_rank))
        if selected is not None
        else None
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            update public.product_cover_candidates
            set
                is_selected = false,
                candidate_status = case
                    when candidate_status = 'published'
                        then 'accepted'
                    else candidate_status
                end,
                updated_at = now()
            where product_id = %s
              and is_selected is true
            """,
            (product.product_id,),
        )

        if selected is not None:
            cur.execute(
                """
                update public.product_cover_candidates
                set
                    is_selected = true,
                    candidate_status = 'published',
                    source_type = %s,
                    source_rank = %s,
                    mime_type = coalesce(%s, mime_type),
                    width = coalesce(%s, width),
                    height = coalesce(%s, height),
                    byte_size = coalesce(%s, byte_size),
                    sha256 = coalesce(%s, sha256),
                    last_checked_at = now(),
                    last_http_status = 200,
                    last_error_code = null,
                    last_error_message = null,
                    updated_at = now()
                where id = %s
                  and product_id = %s
                """,
                (
                    selected.candidate.source_type,
                    selected.candidate.source_rank,
                    mime_type,
                    width,
                    height,
                    byte_size,
                    sha256,
                    selected.candidate_id,
                    product.product_id,
                ),
            )
            if cur.rowcount != 1:
                raise CoverPipelineError(
                    "Geselecteerde kandidaat kon niet exact worden bijgewerkt."
                )

        cur.execute(
            """
            update public.products
            set
                cover_url = %s,
                cover_storage_path = %s,
                cover_source = 'central-cover-worker',
                cover_source_url = nullif(%s, ''),
                cover_source_shop_id = %s,
                cover_status = 'ready',
                cover_confidence = %s,
                cover_sha256 = nullif(%s, ''),
                cover_mime_type = nullif(%s, ''),
                cover_byte_size = %s,
                cover_width = %s,
                cover_height = %s,
                cover_needs_refresh = false,
                cover_fail_count = 0,
                cover_last_success_at = now(),
                cover_last_attempt_at = now(),
                cover_locked_at = null,
                cover_locked_by = null,
                cover_error_code = null,
                cover_error_message = null,
                updated_at = now()
            where id = %s
              and cover_status <> 'blocked'
              and cover_locked_by = %s
            """,
            (
                public_url,
                remote_path,
                source_url,
                source_shop_id,
                confidence,
                sha256 or "",
                mime_type or "",
                byte_size,
                width,
                height,
                product.product_id,
                worker_id,
            ),
        )
        if cur.rowcount != 1:
            raise CoverPipelineError(
                "Product werd tijdens publicatie geblokkeerd of ontbreekt."
            )

        if selected is not None:
            cur.execute(
                """
                select count(*)
                from public.product_cover_candidates
                where product_id = %s
                  and is_selected is true
                  and candidate_status = 'published'
                """,
                (product.product_id,),
            )
            selected_count = int(cur.fetchone()[0] or 0)
            if selected_count != 1:
                raise CoverPipelineError(
                    "Publicatie resulteerde niet in exact één "
                    "geselecteerde kandidaat."
                )

        cur.execute(
            """
            update public.product_cover_queue
            set
                status = 'published',
                claimed_by = null,
                claimed_at = null,
                next_attempt_at = null,
                last_completed_at = now(),
                last_error_code = null,
                last_error_message = null,
                updated_at = now()
            where id = %s
              and product_id = %s
              and status = 'processing'
              and claimed_by = %s
            """,
            (job.queue_id, product.product_id, worker_id),
        )
        if cur.rowcount != 1:
            raise CoverPipelineError(
                "Queueclaim is tijdens publicatie verloren gegaan."
            )
    conn.commit()


def mark_candidate_failed(
    conn,
    *,
    selected: CandidateChoice,
    error_code: str,
    error_message: str,
    preserve_selected_publication: bool = False,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.product_cover_candidates
            set
                candidate_status = case
                    when %s and is_selected is true
                        then 'published'
                    else 'failed'
                end,
                is_selected = case
                    when %s and is_selected is true
                        then true
                    else false
                end,
                last_checked_at = now(),
                last_error_code = %s,
                last_error_message = %s,
                updated_at = now()
            where id = %s
            """,
            (
                preserve_selected_publication,
                preserve_selected_publication,
                error_code,
                error_message[:2000],
                selected.candidate_id,
            ),
        )
    conn.commit()


def finish_job_without_publication(
    conn,
    *,
    job: CoverJob,
    product: ProductState | None,
    worker_id: str,
    status: str,
    error_code: str,
    error_message: str,
    max_attempts: int,
    preserve_local_cover: bool = False,
) -> str:
    if status not in {"retry_later", "review", "failed"}:
        raise CoverPipelineError(
            f"Ongeldige eindstatus zonder publicatie: {status}"
        )

    final_status = status
    if (
        status == "retry_later"
        and job.attempt_count >= max_attempts
    ):
        final_status = "failed"

    next_attempt_at = (
        next_retry_timestamp()
        if final_status == "retry_later"
        else None
    )
    product_status = (
        "ready"
        if preserve_local_cover
        else (
            "queued"
            if final_status == "retry_later"
            else final_status
        )
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            update public.product_cover_queue
            set
                status = %s,
                claimed_by = null,
                claimed_at = null,
                next_attempt_at = %s,
                last_completed_at = case
                    when %s in ('review', 'failed') then now()
                    else last_completed_at
                end,
                last_error_code = %s,
                last_error_message = %s,
                updated_at = now()
            where id = %s
              and status = 'processing'
              and claimed_by = %s
            """,
            (
                final_status,
                next_attempt_at,
                final_status,
                error_code,
                error_message[:2000],
                job.queue_id,
                worker_id,
            ),
        )
        if cur.rowcount != 1:
            raise CoverPipelineError(
                "Queueclaim kon niet gecontroleerd worden afgerond."
            )

        if product is not None:
            cur.execute(
                """
                update public.products
                set
                    cover_status = case
                        when cover_status = 'blocked'
                            then 'blocked'
                        else %s
                    end,
                    cover_needs_refresh = case
                        when %s then true
                        else cover_needs_refresh
                    end,
                    cover_fail_count = coalesce(cover_fail_count, 0) + 1,
                    cover_last_attempt_at = now(),
                    cover_locked_at = null,
                    cover_locked_by = null,
                    cover_error_code = %s,
                    cover_error_message = %s,
                    updated_at = now()
                where id = %s
                """,
                (
                    product_status,
                    preserve_local_cover,
                    error_code,
                    error_message[:2000],
                    product.product_id,
                ),
            )
    conn.commit()
    return final_status


def reconcile_local_products(
    conn,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                id,
                ean,
                cover_storage_path,
                cover_url,
                cover_status,
                cover_source_url
            from public.products
            where cover_status <> 'blocked'
              and nullif(btrim(cover_storage_path), '') is not null
              and cover_needs_refresh is false
              and (
                    nullif(btrim(cover_url), '') is null
                    or cover_status <> 'ready'
                    or cover_url not like (
                        '%/storage/v1/object/public/product-covers/'
                        || cover_storage_path
                    )
              )
            order by id
            limit %s
            """,
            (limit,),
        )
        rows = cur.fetchall()

    results: list[dict[str, Any]] = []
    for row in rows:
        product_id = str(row[0])
        remote_path = normalize_text(row[2])
        local = preflight_local_object(remote_path)
        if local is None:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    update public.products
                    set
                        cover_status = 'review',
                        cover_needs_refresh = true,
                        cover_error_code = 'local_object_missing',
                        cover_error_message = %s,
                        updated_at = now()
                    where id = %s
                      and cover_status <> 'blocked'
                    """,
                    (
                        f"Storage-object ontbreekt op exact pad {remote_path}.",
                        product_id,
                    ),
                )
            conn.commit()
            results.append(
                {
                    "product_id": product_id,
                    "status": "local_object_missing",
                    "storage_path": remote_path,
                }
            )
            continue

        with conn.cursor() as cur:
            cur.execute(
                """
                update public.products
                set
                    cover_url = %s,
                    cover_status = 'ready',
                    cover_mime_type = coalesce(
                        nullif(cover_mime_type, ''),
                        %s
                    ),
                    cover_byte_size = coalesce(
                        cover_byte_size,
                        %s
                    ),
                    cover_needs_refresh = false,
                    cover_last_success_at = coalesce(
                        cover_last_success_at,
                        now()
                    ),
                    cover_locked_at = null,
                    cover_locked_by = null,
                    cover_error_code = null,
                    cover_error_message = null,
                    updated_at = now()
                where id = %s
                  and cover_status <> 'blocked'
                """,
                (
                    local.public_url,
                    local.mime_type,
                    local.byte_size,
                    product_id,
                ),
            )
            cur.execute(
                """
                update public.product_cover_queue
                set
                    status = 'published',
                    claimed_by = null,
                    claimed_at = null,
                    next_attempt_at = null,
                    last_completed_at = coalesce(
                        last_completed_at,
                        now()
                    ),
                    last_error_code = null,
                    last_error_message = null,
                    updated_at = now()
                where product_id = %s
                  and status <> 'processing'
                """,
                (product_id,),
            )
        conn.commit()
        results.append(
            {
                "product_id": product_id,
                "status": "reused_local_cover",
                "storage_path": local.remote_path,
                "public_url": local.public_url,
            }
        )
    return results



def process_claimed_job(
    conn,
    *,
    job: CoverJob,
    worker_id: str,
    session,
    retry_failed: bool,
    max_attempts: int,
) -> dict[str, Any]:
    product: ProductState | None = None
    is_local_repair = job.source_reason == "local_repair"
    existing: LocalObject | None = None

    try:
        product = load_product_state(conn, job.product_id)
        if product.cover_status == "blocked":
            final_status = finish_job_without_publication(
                conn,
                job=job,
                product=product,
                worker_id=worker_id,
                status="review",
                error_code="blocked_after_claim",
                error_message=(
                    "Product werd na claim geblokkeerd; "
                    "geen download uitgevoerd."
                ),
                max_attempts=max_attempts,
                preserve_local_cover=bool(
                    product.cover_storage_path
                ),
            )
            return {
                "queue_id": job.queue_id,
                "product_id": job.product_id,
                "status": final_status,
                "error_code": "blocked_after_claim",
            }

        if not product.ean:
            final_status = finish_job_without_publication(
                conn,
                job=job,
                product=product,
                worker_id=worker_id,
                status="review",
                error_code="invalid_ean",
                error_message="Product heeft geen geldige EAN.",
                max_attempts=max_attempts,
                preserve_local_cover=bool(
                    product.cover_storage_path
                ),
            )
            return {
                "queue_id": job.queue_id,
                "product_id": job.product_id,
                "status": final_status,
                "error_code": "invalid_ean",
            }

        choices = load_candidates(
            conn,
            product,
            retry_failed=retry_failed,
        )
        target_path = (
            product.cover_storage_path
            or build_product_storage_path(product.ean)
        )
        existing = preflight_local_object(target_path)
        active_local_cover = bool(
            product.cover_storage_path and existing is not None
        )

        selected_for_reuse = candidate_for_reused_object(choices)
        reusable_metadata_complete = bool(
            product.cover_sha256
            and (product.cover_mime_type or (
                existing.mime_type if existing else None
            ))
            and (product.cover_byte_size or (
                existing.byte_size if existing else None
            ))
        )
        if (
            existing is not None
            and not is_local_repair
            and selected_for_reuse is not None
            and reusable_metadata_complete
        ):
            publish_database_state(
                conn,
                job=job,
                product=product,
                selected=selected_for_reuse,
                remote_path=existing.remote_path,
                public_url=existing.public_url,
                source_url=selected_for_reuse.candidate.image_url,
                source_shop_id=(
                    selected_for_reuse.candidate.shop_id
                ),
                sha256=product.cover_sha256,
                mime_type=(
                    product.cover_mime_type
                    or existing.mime_type
                ),
                byte_size=(
                    product.cover_byte_size
                    or existing.byte_size
                ),
                width=product.cover_width,
                height=product.cover_height,
                worker_id=worker_id,
            )
            return {
                "queue_id": job.queue_id,
                "product_id": job.product_id,
                "status": "reused_local_cover",
                "storage_path": existing.remote_path,
                "selected_candidate_id": (
                    selected_for_reuse.candidate_id
                ),
                "downloads": 0,
                "uploads": 0,
            }

        if not choices:
            final_status = finish_job_without_publication(
                conn,
                job=job,
                product=product,
                worker_id=worker_id,
                status="review",
                error_code=(
                    "local_repair_without_candidates"
                    if is_local_repair
                    else "no_candidates"
                ),
                error_message=(
                    "Geen bruikbare HTTP(S)-coverkandidaten beschikbaar."
                ),
                max_attempts=max_attempts,
                preserve_local_cover=active_local_cover,
            )
            return {
                "queue_id": job.queue_id,
                "product_id": job.product_id,
                "status": final_status,
                "error_code": (
                    "local_repair_without_candidates"
                    if is_local_repair
                    else "no_candidates"
                ),
                "preserved_existing_object": active_local_cover,
            }

        candidate_errors: list[dict[str, str]] = []
        for selected in choices:
            try:
                downloaded = download_binary(
                    session,
                    selected.candidate.image_url,
                )
                prepared = prepare_image_for_storage(
                    downloaded.content,
                    original_mime_type=downloaded.mime_type,
                )
                if len(prepared.output_bytes) > MAX_STORAGE_OBJECT_BYTES:
                    raise CoverPipelineError(
                        "Voorbereide WebP overschrijdt de Storage-limiet "
                        f"van {MAX_STORAGE_OBJECT_BYTES} bytes."
                    )
                receipt = upsert_bytes_to_storage(
                    target_path,
                    prepared,
                )
                try:
                    publish_database_state(
                        conn,
                        job=job,
                        product=product,
                        selected=selected,
                        remote_path=receipt.remote_path,
                        public_url=receipt.public_url,
                        source_url=selected.candidate.image_url,
                        source_shop_id=selected.candidate.shop_id,
                        sha256=prepared.sha256,
                        mime_type=prepared.mime_type,
                        byte_size=len(prepared.output_bytes),
                        width=prepared.width,
                        height=prepared.height,
                        worker_id=worker_id,
                    )
                except Exception:
                    conn.rollback()
                    compensate_storage_upload(receipt)
                    raise

                return {
                    "queue_id": job.queue_id,
                    "product_id": job.product_id,
                    "status": (
                        "repaired_local_cover"
                        if is_local_repair
                        else "published"
                    ),
                    "selected_candidate_id": selected.candidate_id,
                    "selected_candidate": asdict(
                        selected.candidate
                    ),
                    "storage_path": receipt.remote_path,
                    "public_url": receipt.public_url,
                    "sha256": prepared.sha256,
                    "mime_type": prepared.mime_type,
                    "byte_size": len(prepared.output_bytes),
                    "width": prepared.width,
                    "height": prepared.height,
                    "downloads": 0 if downloaded.reused else 1,
                    "uploads": 1,
                    "replaced_existing_object": (
                        receipt.existed_before
                    ),
                }
            except Exception as exc:
                conn.rollback()
                error_code = type(exc).__name__.lower()
                error_message = (
                    normalize_text(str(exc))
                    or type(exc).__name__
                )
                try:
                    mark_candidate_failed(
                        conn,
                        selected=selected,
                        error_code=error_code,
                        error_message=error_message,
                        preserve_selected_publication=active_local_cover,
                    )
                except Exception:
                    conn.rollback()
                candidate_errors.append(
                    {
                        "candidate_id": selected.candidate_id,
                        "image_url": selected.candidate.image_url,
                        "error_code": error_code,
                        "error_message": error_message,
                    }
                )

        final_status = finish_job_without_publication(
            conn,
            job=job,
            product=product,
            worker_id=worker_id,
            status="retry_later",
            error_code="all_candidates_failed",
            error_message=(
                f"Alle {len(candidate_errors)} coverkandidaten faalden."
            ),
            max_attempts=max_attempts,
            preserve_local_cover=active_local_cover,
        )
        return {
            "queue_id": job.queue_id,
            "product_id": job.product_id,
            "status": final_status,
            "error_code": "all_candidates_failed",
            "candidate_errors": candidate_errors,
            "preserved_existing_object": active_local_cover,
        }

    except Exception as exc:
        conn.rollback()
        error_code = type(exc).__name__.lower()
        error_message = (
            normalize_text(str(exc)) or type(exc).__name__
        )
        try:
            final_status = finish_job_without_publication(
                conn,
                job=job,
                product=product,
                worker_id=worker_id,
                status="retry_later",
                error_code=error_code,
                error_message=error_message,
                max_attempts=max_attempts,
                preserve_local_cover=active_local_cover,
            )
        except Exception:
            conn.rollback()
            raise

        return {
            "queue_id": job.queue_id,
            "product_id": job.product_id,
            "status": final_status,
            "error_code": error_code,
            "error_message": error_message,
            "preserved_existing_object": active_local_cover,
        }


def write_summary(
    path: Path,
    summary: dict[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        serialize_json(summary) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    validate_args(args)
    worker_id = normalize_text(args.worker_id) or default_worker_id()
    output_path = Path(args.output_json)
    ensure_runtime_directories()

    summary: dict[str, Any] = {
        "started_at": utc_now().isoformat(),
        "worker_id": worker_id,
        "mode": args.mode,
        "target_product_id": args.product_id or None,
        "dry_run": bool(args.dry_run),
        "limit": args.limit,
        "max_attempts": args.max_attempts,
        "stale_after_minutes": args.stale_after_minutes,
        "reconcile_limit": args.reconcile_limit,
        "metrics": {
            "stale_claims_found": 0,
            "stale_claims_recovered": 0,
            "failed_jobs_requeued": 0,
            "local_products_reconciled": 0,
            "local_objects_missing": 0,
            "claims": 0,
            "local_repair_claims": 0,
            "published": 0,
            "repaired_local_cover": 0,
            "reused_local_cover": 0,
            "retry_later": 0,
            "review": 0,
            "failed": 0,
            "downloads": 0,
            "uploads": 0,
        },
        "preview": [],
        "local_preview": [],
        "reconciliation": [],
        "jobs": [],
    }

    conn = connect_db()
    conn.autocommit = False
    try:
        require_schema(conn)

        if args.product_id:
            job = claim_one_job_for_product(
                conn,
                worker_id=worker_id,
                product_id=args.product_id,
            )

            if job is not None:
                summary["metrics"]["claims"] += 1

                session = make_session()

                result = process_claimed_job(
                    conn,
                    job=job,
                    worker_id=worker_id,
                    session=session,
                    retry_failed=False,
                    max_attempts=args.max_attempts,
                )
                summary["jobs"].append(result)

                status = normalize_text(
                    result.get("status")
                )
                if status in summary["metrics"]:
                    summary["metrics"][status] += 1

                summary["metrics"]["downloads"] += int(
                    result.get("downloads") or 0
                )
                summary["metrics"]["uploads"] += int(
                    result.get("uploads") or 0
                )

        elif args.dry_run:
            with conn.cursor() as cur:
                cur.execute(
                    "set transaction read only"
                )
            summary["metrics"]["stale_claims_found"] = count_stale_claims(
                conn,
                stale_after_minutes=args.stale_after_minutes,
            )
            summary["local_preview"] = preview_local_actions(
                conn,
                mode=args.mode,
                limit=max(args.limit, args.reconcile_limit),
                max_attempts=args.max_attempts,
            )
            summary["preview"] = preview_jobs(
                conn,
                mode=args.mode,
                limit=args.limit,
            )
            conn.rollback()
        else:
            summary["metrics"]["stale_claims_recovered"] = (
                recover_stale_claims(
                    conn,
                    stale_after_minutes=args.stale_after_minutes,
                )
            )
            if args.mode == "retry-failed":
                summary["metrics"]["failed_jobs_requeued"] = (
                    requeue_failed_jobs(
                        conn,
                        limit=args.limit,
                    )
                )

            reconciliation = reconcile_local_products(
                conn,
                limit=args.reconcile_limit,
            )
            summary["reconciliation"] = reconciliation
            summary["metrics"]["local_products_reconciled"] = sum(
                item["status"] == "reused_local_cover"
                for item in reconciliation
            )
            summary["metrics"]["local_objects_missing"] = sum(
                item["status"] == "local_object_missing"
                for item in reconciliation
            )

            session = make_session()
            for _ in range(args.limit):
                job = claim_one_local_repair(
                    conn,
                    worker_id=worker_id,
                    stale_after_minutes=args.stale_after_minutes,
                    max_attempts=args.max_attempts,
                    retry_failed=(args.mode == "retry-failed"),
                )
                if job is not None:
                    summary["metrics"]["local_repair_claims"] += 1
                else:
                    job = claim_one_job(
                        conn,
                        worker_id=worker_id,
                    )
                if job is None:
                    break

                summary["metrics"]["claims"] += 1
                result = process_claimed_job(
                    conn,
                    job=job,
                    worker_id=worker_id,
                    session=session,
                    retry_failed=(args.mode == "retry-failed"),
                    max_attempts=args.max_attempts,
                )
                summary["jobs"].append(result)

                status = normalize_text(result.get("status"))
                if status in summary["metrics"]:
                    summary["metrics"][status] += 1
                summary["metrics"]["downloads"] += int(
                    result.get("downloads") or 0
                )
                summary["metrics"]["uploads"] += int(
                    result.get("uploads") or 0
                )

    except Exception as exc:
        conn.rollback()
        summary["failed_at"] = utc_now().isoformat()
        summary["fatal_error"] = normalize_text(str(exc))
        write_summary(output_path, summary)
        raise
    finally:
        conn.close()

    summary["finished_at"] = utc_now().isoformat()
    write_summary(output_path, summary)
    log(
        "[DONE] cover worker | "
        f"dry_run={args.dry_run} | "
        f"claims={summary['metrics']['claims']} | "
        f"published={summary['metrics']['published']} | "
        f"reused={summary['metrics']['reused_local_cover']} | "
        f"downloads={summary['metrics']['downloads']} | "
        f"uploads={summary['metrics']['uploads']}"
    )


if __name__ == "__main__":
    main()
