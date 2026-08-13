#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit
from uuid import UUID

import psycopg
import requests
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

VERSION = "2026-08-05-candidate-only-1"
MB_BASE = "https://musicbrainz.org/ws/2"
CAA_BASE = "https://coverartarchive.org"
DEFAULT_USER_AGENT = (
    "VinylofyMusicBrainzCandidateWorker/1.0 "
    "(https://vinylofy.com; contact: info@vinylofy.com)"
)
DEFAULT_MB_INTERVAL_SECONDS = 1.1
DEFAULT_TIMEOUT_SECONDS = 30
SOURCE_TYPE = "meta"


@dataclass(frozen=True, slots=True)
class Config:
    database_url: str
    user_agent: str
    worker_id: str
    sleep_seconds: float
    timeout_seconds: int
    limit: int
    force_retry_days: int
    max_attempts: int
    dry_run: bool
    prefer_release_group_front: bool
    stale_claim_minutes: int
    output_json: str | None


@dataclass(frozen=True, slots=True)
class MatchResult:
    decision: str
    release_id: str | None
    release_group_id: str | None
    title: str | None
    artist: str | None
    date: str | None
    country: str | None
    match_score: float
    match_basis: str
    raw_result: Any
    cover_json: Any
    cover_front_url: str | None


class TemporaryRemoteError(RuntimeError):
    pass


class PermanentDecisionError(RuntimeError):
    pass


class RateLimiter:
    def __init__(self, min_interval_seconds: float) -> None:
        self.min_interval_seconds = min_interval_seconds
        self._last_call = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self.min_interval_seconds:
            time.sleep(self.min_interval_seconds - elapsed)
        self._last_call = time.monotonic()


class MusicBrainzClient:
    def __init__(
        self,
        user_agent: str,
        timeout_seconds: int,
        limiter: RateLimiter,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.limiter = limiter
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json",
            }
        )

    def _get_json(
        self,
        url: str,
        *,
        allow_not_found: bool = False,
    ) -> dict[str, Any] | None:
        self.limiter.wait()
        response = self.session.get(
            url,
            timeout=self.timeout_seconds,
            allow_redirects=True,
        )
        if response.status_code == 404 and allow_not_found:
            return None
        if response.status_code in {429, 503}:
            raise TemporaryRemoteError(
                f"remote_rate_limited_or_unavailable:{response.status_code}"
            )
        response.raise_for_status()
        content_type = (
            response.headers.get("Content-Type") or ""
        ).split(";", 1)[0].strip().lower()
        if content_type not in {
            "application/json",
            "application/ld+json",
        }:
            raise PermanentDecisionError(
                f"unexpected_json_mime:{content_type or 'missing'}"
            )
        payload = response.json()
        if not isinstance(payload, dict):
            raise PermanentDecisionError("unexpected_json_payload")
        return payload

    def search_release_by_barcode(
        self,
        ean: str,
        *,
        limit: int = 10,
    ) -> dict[str, Any]:
        query = quote(f"barcode:{ean}")
        url = (
            f"{MB_BASE}/release?query={query}"
            f"&fmt=json&limit={max(1, limit)}"
        )
        payload = self._get_json(url)
        if payload is None:
            return {}
        return payload

    def get_release_detail(
        self,
        release_id: str,
    ) -> dict[str, Any]:
        url = (
            f"{MB_BASE}/release/{release_id}"
            "?inc=release-groups+artists&fmt=json"
        )
        payload = self._get_json(url)
        if payload is None:
            return {}
        return payload

    def resolve_front_image(
        self,
        release_id: str,
        release_group_id: str | None,
        *,
        prefer_release_group_front: bool,
    ) -> tuple[str | None, dict[str, Any] | None]:
        targets: list[tuple[str, str]] = []
        if prefer_release_group_front and release_group_id:
            targets.append(
                (
                    "release-group",
                    f"{CAA_BASE}/release-group/{release_group_id}",
                )
            )
        targets.append(
            (
                "release",
                f"{CAA_BASE}/release/{release_id}",
            )
        )
        if not prefer_release_group_front and release_group_id:
            targets.append(
                (
                    "release-group",
                    f"{CAA_BASE}/release-group/{release_group_id}",
                )
            )

        for _, url in targets:
            payload = self._get_json(
                url,
                allow_not_found=True,
            )
            if payload is None:
                continue
            front_url = extract_front_url(payload)
            if front_url:
                return front_url, payload
        return None, None


SQL_PREVIEW = """
select
    q.*,
    p.artist,
    p.title,
    p.cover_storage_path,
    p.cover_status,
    coalesce(p.cover_priority, 0) as cover_priority
from public.cover_lookup_queue q
join public.products p
  on p.id = q.product_id
left join public.musicbrainz_release_cache c
  on c.ean = q.ean
where q.status in ('queued', 'retry')
  and q.next_attempt_at <= now()
  and q.attempts < %(max_attempts)s
  and (
      c.ean is null
      or c.status not in ('matched', 'no_match', 'ambiguous')
      or c.checked_at
          < now() - make_interval(days => %(force_retry_days)s)
  )
order by q.priority desc, q.created_at asc
limit %(limit)s
"""

SQL_CLAIM = """
with next_job as (
    select q.id
    from public.cover_lookup_queue q
    join public.products p
      on p.id = q.product_id
    left join public.musicbrainz_release_cache c
      on c.ean = q.ean
    where q.status in ('queued', 'retry')
      and q.next_attempt_at <= now()
      and q.attempts < %(max_attempts)s
      and (
          c.ean is null
          or c.status not in ('matched', 'no_match', 'ambiguous')
          or c.checked_at
              < now() - make_interval(days => %(force_retry_days)s)
      )
    order by q.priority desc, q.created_at asc
    limit 1
    for update of q skip locked
)
update public.cover_lookup_queue q
set
    status = 'processing',
    locked_at = now(),
    locked_by = %(worker_id)s,
    attempts = q.attempts + 1,
    updated_at = now()
from next_job
where q.id = next_job.id
returning q.*
"""

SQL_PRODUCT_META = """
select
    p.id,
    p.ean,
    p.artist,
    p.title,
    p.cover_storage_path,
    p.cover_status,
    coalesce(p.cover_priority, 0) as cover_priority
from public.products p
where p.id = %(product_id)s
"""

SQL_CACHE_GET = """
select *
from public.musicbrainz_release_cache
where ean = %(ean)s
"""

SQL_CACHE_UPSERT = """
insert into public.musicbrainz_release_cache (
    ean,
    mb_release_id,
    mb_release_group_id,
    matched_title,
    matched_artist,
    matched_date,
    matched_country,
    match_score,
    match_basis,
    status,
    raw_result,
    cover_json,
    cover_front_url,
    last_error,
    checked_at,
    updated_at
)
values (
    %(ean)s,
    %(mb_release_id)s,
    %(mb_release_group_id)s,
    %(matched_title)s,
    %(matched_artist)s,
    %(matched_date)s,
    %(matched_country)s,
    %(match_score)s,
    %(match_basis)s,
    %(status)s,
    %(raw_result)s,
    %(cover_json)s,
    %(cover_front_url)s,
    %(last_error)s,
    now(),
    now()
)
on conflict (ean) do update
set
    mb_release_id = excluded.mb_release_id,
    mb_release_group_id = excluded.mb_release_group_id,
    matched_title = excluded.matched_title,
    matched_artist = excluded.matched_artist,
    matched_date = excluded.matched_date,
    matched_country = excluded.matched_country,
    match_score = excluded.match_score,
    match_basis = excluded.match_basis,
    status = excluded.status,
    raw_result = excluded.raw_result,
    cover_json = excluded.cover_json,
    cover_front_url = excluded.cover_front_url,
    last_error = excluded.last_error,
    checked_at = now(),
    updated_at = now()
"""

SQL_FIND_CANDIDATE = """
select
    id,
    source_rank,
    is_primary,
    candidate_status
from public.product_cover_candidates
where product_id = %(product_id)s
  and image_url = %(image_url)s
order by updated_at desc nulls last, id
limit 1
"""

SQL_INSERT_CANDIDATE = """
insert into public.product_cover_candidates (
    product_id,
    shop_id,
    ean,
    product_url,
    image_url,
    source_type,
    source_rank,
    is_primary,
    candidate_status,
    discovered_at,
    first_seen_at,
    last_seen_at,
    last_checked_at,
    last_http_status,
    last_error_code,
    last_error_message,
    created_at,
    updated_at
)
values (
    %(product_id)s,
    null,
    %(ean)s,
    %(product_url)s,
    %(image_url)s,
    %(source_type)s,
    %(source_rank)s,
    true,
    'pending',
    now(),
    now(),
    now(),
    now(),
    200,
    null,
    null,
    now(),
    now()
)
returning id
"""

SQL_UPDATE_CANDIDATE = """
update public.product_cover_candidates
set
    ean = %(ean)s,
    product_url = %(product_url)s,
    image_url = %(image_url)s,
    source_type = %(source_type)s,
    source_rank = greatest(source_rank, %(source_rank)s),
    is_primary = true,
    candidate_status = case
        when candidate_status in ('published', 'rejected')
            then candidate_status
        else 'pending'
    end,
    last_seen_at = now(),
    last_checked_at = now(),
    last_http_status = 200,
    last_error_code = null,
    last_error_message = null,
    updated_at = now()
where id = %(candidate_id)s
"""

SQL_QUEUE_CENTRAL = """
select public.queue_cover_for_products(
    %(product_ids)s::uuid[],
    'musicbrainz',
    %(priority_bump)s,
    'cover_mb_worker'
)
"""

SQL_JOB_UPDATE = """
update public.cover_lookup_queue
set
    status = %(status)s,
    locked_at = null,
    locked_by = null,
    next_attempt_at = %(next_attempt_at)s,
    last_error = %(last_error)s,
    updated_at = now()
where id = %(job_id)s
"""

SQL_RECOVER_STALE = """
update public.cover_lookup_queue
set
    status = 'retry',
    locked_at = null,
    locked_by = null,
    next_attempt_at = now(),
    last_error = 'stale_claim_recovered',
    updated_at = now()
where status = 'processing'
  and locked_at
      < now() - make_interval(mins => %(stale_claim_minutes)s)
"""


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve MusicBrainz/CAA cover URLs as candidates only. "
            "This worker never downloads image binaries or writes Storage."
        )
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
    )
    parser.add_argument(
        "--user-agent",
        default=os.getenv(
            "MUSICBRAINZ_USER_AGENT",
            DEFAULT_USER_AGENT,
        ),
    )
    parser.add_argument(
        "--worker-id",
        default=f"{socket.gethostname()}-{os.getpid()}",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=DEFAULT_MB_INTERVAL_SECONDS,
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--force-retry-days", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--prefer-release-group-front",
        action="store_true",
    )
    parser.add_argument(
        "--stale-claim-minutes",
        type=int,
        default=90,
    )
    parser.add_argument("--output-json", default=None)
    args = parser.parse_args()

    database_url = (args.database_url or "").strip()
    if not database_url:
        raise SystemExit(
            "DATABASE_URL ontbreekt. "
            "Geef --database-url mee of zet de env var."
        )

    user_agent = (args.user_agent or "").strip()
    if not user_agent:
        raise SystemExit("MUSICBRAINZ_USER_AGENT is leeg.")

    return Config(
        database_url=database_url,
        user_agent=user_agent,
        worker_id=args.worker_id,
        sleep_seconds=max(1.0, args.sleep_seconds),
        timeout_seconds=max(5, args.timeout_seconds),
        limit=max(1, args.limit),
        force_retry_days=max(1, args.force_retry_days),
        max_attempts=max(1, args.max_attempts),
        dry_run=bool(args.dry_run),
        prefer_release_group_front=bool(
            args.prefer_release_group_front
        ),
        stale_claim_minutes=max(1, args.stale_claim_minutes),
        output_json=args.output_json,
    )


def log_json(event: str, **payload: Any) -> None:
    print(
        json.dumps(
            {"event": event, **payload},
            ensure_ascii=False,
            default=str,
        ),
        flush=True,
    )


def normalize_text(value: str | None) -> str:
    return " ".join((value or "").strip().lower().split())


def similarity(
    first: str | None,
    second: str | None,
) -> float:
    left = normalize_text(first)
    right = normalize_text(second)
    if not left or not right:
        return 0.0
    return SequenceMatcher(None, left, right).ratio()


def extract_artist_name(artist_credit: Any) -> str:
    if not artist_credit:
        return ""
    parts: list[str] = []
    for item in artist_credit:
        if not isinstance(item, dict):
            continue
        name = (
            item.get("name")
            or (item.get("artist") or {}).get("name")
            or ""
        )
        joinphrase = item.get("joinphrase") or ""
        if name:
            parts.append(f"{name}{joinphrase}")
    return "".join(parts).strip()


def choose_candidate(
    search_payload: dict[str, Any],
    product_meta: dict[str, Any],
) -> tuple[str, dict[str, Any] | None, float, str]:
    releases = search_payload.get("releases") or []
    if not releases:
        return "no_match", None, 0.0, "no_results"

    product_title = product_meta.get("title") or ""
    product_artist = product_meta.get("artist") or ""
    product_ean = str(product_meta.get("ean") or "")
    scored: list[tuple[float, dict[str, Any], str]] = []

    for candidate in releases[:10]:
        if not isinstance(candidate, dict):
            continue
        score = 0.0
        reasons: list[str] = []

        barcode = str(candidate.get("barcode") or "")
        if barcode and barcode == product_ean:
            score += 100.0
            reasons.append("barcode_exact")

        mb_score = float(candidate.get("score") or 0.0)
        if mb_score:
            score += min(mb_score / 10.0, 10.0)
            reasons.append("mb_score")

        candidate_title = candidate.get("title") or ""
        candidate_artist = extract_artist_name(
            candidate.get("artist-credit")
        )
        title_similarity = similarity(
            product_title,
            candidate_title,
        )
        artist_similarity = similarity(
            product_artist,
            candidate_artist,
        )
        score += round(title_similarity * 25.0, 2)
        score += round(artist_similarity * 20.0, 2)

        if title_similarity >= 0.92:
            reasons.append("title_strong")
        if artist_similarity >= 0.92:
            reasons.append("artist_strong")
        if candidate.get("status") == "Official":
            score += 5.0
            reasons.append("official")

        scored.append(
            (
                score,
                candidate,
                "+".join(reasons) or "barcode_lookup",
            )
        )

    if not scored:
        return "no_match", None, 0.0, "no_valid_results"

    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, best_candidate, basis = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0

    if (
        best_score >= 115.0
        and (
            len(scored) == 1
            or best_score - second_score >= 12.0
        )
    ):
        return "matched", best_candidate, best_score, basis
    if best_score >= 100.0:
        return "ambiguous", best_candidate, best_score, basis
    return "no_match", None, best_score, basis


def validate_http_url(value: str | None) -> str | None:
    url = (value or "").strip()
    if not url:
        return None
    parts = urlsplit(url)
    if parts.scheme.lower() not in {"http", "https"}:
        return None
    if not parts.netloc:
        return None
    return url


def extract_front_url(
    cover_payload: dict[str, Any],
) -> str | None:
    images = cover_payload.get("images") or []
    if not isinstance(images, list):
        return None

    front_images = [
        image
        for image in images
        if isinstance(image, dict)
        and bool(image.get("front"))
    ]
    candidates = front_images or [
        image
        for image in images
        if isinstance(image, dict)
    ]

    for image in candidates:
        thumbnails = image.get("thumbnails") or {}
        if not isinstance(thumbnails, dict):
            thumbnails = {}
        for key in ("1200", "large", "500", "250", "small"):
            url = validate_http_url(thumbnails.get(key))
            if url:
                return url
        url = validate_http_url(image.get("image"))
        if url:
            return url
    return None


def musicbrainz_candidate_rank(match_score: float) -> int:
    confidence_points = round(
        max(0.0, min(25.0, match_score - 100.0))
    )
    return 20 + confidence_points


def backoff_minutes(attempts: int) -> int:
    return min(
        24 * 60,
        10 * (2 ** max(0, attempts - 1)),
    )


def preview_jobs(
    conn: psycopg.Connection[Any],
    cfg: Config,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            SQL_PREVIEW,
            {
                "max_attempts": cfg.max_attempts,
                "force_retry_days": cfg.force_retry_days,
                "limit": cfg.limit,
            },
        )
        return list(cur.fetchall())


def recover_stale_claims(
    conn: psycopg.Connection[Any],
    cfg: Config,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            SQL_RECOVER_STALE,
            {
                "stale_claim_minutes": cfg.stale_claim_minutes,
            },
        )
        recovered = cur.rowcount or 0
    conn.commit()
    return recovered


def claim_one_job(
    conn: psycopg.Connection[Any],
    cfg: Config,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            SQL_CLAIM,
            {
                "worker_id": cfg.worker_id,
                "max_attempts": cfg.max_attempts,
                "force_retry_days": cfg.force_retry_days,
            },
        )
        row = cur.fetchone()
    conn.commit()
    return row


def load_product_meta(
    conn: psycopg.Connection[Any],
    product_id: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            SQL_PRODUCT_META,
            {"product_id": product_id},
        )
        row = cur.fetchone()
    conn.commit()
    return row


def load_cache(
    conn: psycopg.Connection[Any],
    ean: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(SQL_CACHE_GET, {"ean": ean})
        row = cur.fetchone()
    conn.commit()
    return row


def cache_upsert(
    cur: psycopg.Cursor[Any],
    *,
    ean: str,
    result: MatchResult,
    status: str,
    last_error: str | None,
) -> None:
    cur.execute(
        SQL_CACHE_UPSERT,
        {
            "ean": ean,
            "mb_release_id": result.release_id,
            "mb_release_group_id": result.release_group_id,
            "matched_title": result.title,
            "matched_artist": result.artist,
            "matched_date": result.date,
            "matched_country": result.country,
            "match_score": round(result.match_score, 2),
            "match_basis": result.match_basis,
            "status": status,
            "raw_result": (
                Jsonb(result.raw_result)
                if result.raw_result is not None
                else None
            ),
            "cover_json": (
                Jsonb(result.cover_json)
                if result.cover_json is not None
                else None
            ),
            "cover_front_url": result.cover_front_url,
            "last_error": last_error,
        },
    )


def job_update(
    cur: psycopg.Cursor[Any],
    *,
    job_id: str,
    status: str,
    next_attempt_at: datetime,
    last_error: str | None,
) -> None:
    cur.execute(
        SQL_JOB_UPDATE,
        {
            "job_id": job_id,
            "status": status,
            "next_attempt_at": next_attempt_at,
            "last_error": last_error,
        },
    )


def upsert_candidate_and_queue(
    cur: psycopg.Cursor[Any],
    *,
    product_meta: dict[str, Any],
    result: MatchResult,
) -> tuple[str, int]:
    product_id = str(product_meta["id"])
    ean = str(product_meta["ean"])
    image_url = validate_http_url(result.cover_front_url)
    if not image_url:
        raise PermanentDecisionError("invalid_cover_front_url")
    if not result.release_id:
        raise PermanentDecisionError("missing_release_id")

    try:
        UUID(result.release_id)
    except ValueError as exc:
        raise PermanentDecisionError(
            "invalid_musicbrainz_release_id"
        ) from exc

    source_rank = musicbrainz_candidate_rank(
        result.match_score
    )
    product_url = (
        f"https://musicbrainz.org/release/{result.release_id}"
    )
    params = {
        "product_id": product_id,
        "ean": ean,
        "product_url": product_url,
        "image_url": image_url,
        "source_type": SOURCE_TYPE,
        "source_rank": source_rank,
    }

    cur.execute(
        SQL_FIND_CANDIDATE,
        {
            "product_id": product_id,
            "image_url": image_url,
        },
    )
    existing = cur.fetchone()
    if existing is None:
        cur.execute(SQL_INSERT_CANDIDATE, params)
        inserted = cur.fetchone()
        candidate_id = str(inserted["id"])
        action = "inserted"
    else:
        candidate_id = str(existing["id"])
        cur.execute(
            SQL_UPDATE_CANDIDATE,
            {
                **params,
                "candidate_id": candidate_id,
            },
        )
        action = "updated"

    priority_bump = max(
        int(product_meta.get("cover_priority") or 0),
        source_rank,
    )
    cur.execute(
        SQL_QUEUE_CENTRAL,
        {
            "product_ids": [product_id],
            "priority_bump": priority_bump,
        },
    )
    queue_result = cur.fetchone()
    queued = int(
        next(iter(queue_result.values())) or 0
    ) if queue_result else 0
    return action, queued


def cached_match_result(
    cache_row: dict[str, Any],
) -> MatchResult:
    return MatchResult(
        decision="matched",
        release_id=(
            str(cache_row["mb_release_id"])
            if cache_row.get("mb_release_id")
            else None
        ),
        release_group_id=(
            str(cache_row["mb_release_group_id"])
            if cache_row.get("mb_release_group_id")
            else None
        ),
        title=cache_row.get("matched_title"),
        artist=cache_row.get("matched_artist"),
        date=cache_row.get("matched_date"),
        country=cache_row.get("matched_country"),
        match_score=float(
            cache_row.get("match_score") or 0.0
        ),
        match_basis=str(
            cache_row.get("match_basis") or "cache"
        ),
        raw_result=cache_row.get("raw_result") or {},
        cover_json=cache_row.get("cover_json"),
        cover_front_url=cache_row.get("cover_front_url"),
    )


def resolve_match(
    mb: MusicBrainzClient,
    *,
    ean: str,
    product_meta: dict[str, Any],
    cache_row: dict[str, Any] | None,
    prefer_release_group_front: bool,
) -> MatchResult:
    if (
        cache_row
        and cache_row.get("status") == "matched"
        and cache_row.get("cover_front_url")
        and cache_row.get("mb_release_id")
    ):
        return cached_match_result(cache_row)

    search_payload = mb.search_release_by_barcode(
        ean,
        limit=10,
    )
    (
        decision,
        candidate,
        match_score,
        match_basis,
    ) = choose_candidate(
        search_payload,
        product_meta,
    )

    if decision != "matched" or not candidate:
        return MatchResult(
            decision=decision,
            release_id=(
                str(candidate.get("id"))
                if candidate and candidate.get("id")
                else None
            ),
            release_group_id=None,
            title=(
                candidate.get("title")
                if candidate
                else None
            ),
            artist=(
                extract_artist_name(
                    candidate.get("artist-credit")
                )
                if candidate
                else None
            ),
            date=(
                candidate.get("date")
                if candidate
                else None
            ),
            country=(
                candidate.get("country")
                if candidate
                else None
            ),
            match_score=match_score,
            match_basis=match_basis,
            raw_result=search_payload,
            cover_json=None,
            cover_front_url=None,
        )

    release_id = str(candidate["id"])
    detail = mb.get_release_detail(release_id)
    release_group_id = (
        (detail.get("release-group") or {}).get("id")
        or None
    )
    front_url, cover_json = mb.resolve_front_image(
        release_id,
        release_group_id,
        prefer_release_group_front=(
            prefer_release_group_front
        ),
    )
    return MatchResult(
        decision=(
            "matched"
            if front_url
            else "no_match"
        ),
        release_id=release_id,
        release_group_id=(
            str(release_group_id)
            if release_group_id
            else None
        ),
        title=detail.get("title"),
        artist=extract_artist_name(
            detail.get("artist-credit")
        ),
        date=detail.get("date"),
        country=detail.get("country"),
        match_score=match_score,
        match_basis=match_basis,
        raw_result=detail,
        cover_json=cover_json,
        cover_front_url=front_url,
    )


def process_preview_job(
    cfg: Config,
    mb: MusicBrainzClient,
    job: dict[str, Any],
) -> dict[str, Any]:
    product_meta = {
        "id": job["product_id"],
        "ean": job["ean"],
        "artist": job.get("artist"),
        "title": job.get("title"),
        "cover_storage_path": job.get(
            "cover_storage_path"
        ),
        "cover_status": job.get("cover_status"),
        "cover_priority": job.get("cover_priority"),
    }
    if product_meta.get("cover_storage_path"):
        return {
            "product_id": str(job["product_id"]),
            "ean": str(job["ean"]),
            "decision": "already_has_local_cover",
        }
    if product_meta.get("cover_status") == "blocked":
        return {
            "product_id": str(job["product_id"]),
            "ean": str(job["ean"]),
            "decision": "blocked",
        }

    result = resolve_match(
        mb,
        ean=str(job["ean"]),
        product_meta=product_meta,
        cache_row=None,
        prefer_release_group_front=(
            cfg.prefer_release_group_front
        ),
    )
    return {
        "product_id": str(job["product_id"]),
        "ean": str(job["ean"]),
        "decision": result.decision,
        "release_id": result.release_id,
        "cover_front_url": result.cover_front_url,
        "source_rank": musicbrainz_candidate_rank(
            result.match_score
        ),
    }


def process_claimed_job(
    conn: psycopg.Connection[Any],
    cfg: Config,
    mb: MusicBrainzClient,
    job: dict[str, Any],
) -> dict[str, Any]:
    job_id = str(job["id"])
    product_id = str(job["product_id"])
    ean = str(job["ean"])
    attempts = int(job["attempts"])

    try:
        product_meta = load_product_meta(
            conn,
            product_id,
        )
        if product_meta is None:
            raise PermanentDecisionError(
                "product_not_found"
            )

        if product_meta.get("cover_status") == "blocked":
            with conn.cursor() as cur:
                job_update(
                    cur,
                    job_id=job_id,
                    status="failed",
                    next_attempt_at=(
                        datetime.now(timezone.utc)
                        + timedelta(
                            days=cfg.force_retry_days
                        )
                    ),
                    last_error="blocked",
                )
            conn.commit()
            return {
                "job_id": job_id,
                "product_id": product_id,
                "ean": ean,
                "decision": "blocked",
            }

        if product_meta.get("cover_storage_path"):
            with conn.cursor() as cur:
                job_update(
                    cur,
                    job_id=job_id,
                    status="done",
                    next_attempt_at=datetime.now(
                        timezone.utc
                    ),
                    last_error="already_has_local_cover",
                )
            conn.commit()
            return {
                "job_id": job_id,
                "product_id": product_id,
                "ean": ean,
                "decision": "already_has_local_cover",
            }

        cache_row = load_cache(conn, ean)
        result = resolve_match(
            mb,
            ean=ean,
            product_meta=product_meta,
            cache_row=cache_row,
            prefer_release_group_front=(
                cfg.prefer_release_group_front
            ),
        )

        if result.decision == "no_match":
            error_code = (
                "caa_front_not_found"
                if result.release_id
                else "no_match"
            )
            with conn.cursor() as cur:
                cache_upsert(
                    cur,
                    ean=ean,
                    result=result,
                    status="no_match",
                    last_error=error_code,
                )
                job_update(
                    cur,
                    job_id=job_id,
                    status="failed",
                    next_attempt_at=(
                        datetime.now(timezone.utc)
                        + timedelta(
                            days=cfg.force_retry_days
                        )
                    ),
                    last_error=error_code,
                )
            conn.commit()
            return {
                "job_id": job_id,
                "product_id": product_id,
                "ean": ean,
                "decision": error_code,
            }

        if result.decision == "ambiguous":
            with conn.cursor() as cur:
                cache_upsert(
                    cur,
                    ean=ean,
                    result=result,
                    status="ambiguous",
                    last_error="ambiguous_match",
                )
                job_update(
                    cur,
                    job_id=job_id,
                    status="failed",
                    next_attempt_at=(
                        datetime.now(timezone.utc)
                        + timedelta(
                            days=cfg.force_retry_days
                        )
                    ),
                    last_error="ambiguous_match",
                )
            conn.commit()
            return {
                "job_id": job_id,
                "product_id": product_id,
                "ean": ean,
                "decision": "ambiguous",
            }

        if (
            result.decision != "matched"
            or not result.cover_front_url
        ):
            raise PermanentDecisionError(
                "matched_result_without_cover_url"
            )

        with conn.cursor() as cur:
            cache_upsert(
                cur,
                ean=ean,
                result=result,
                status="matched",
                last_error=None,
            )
            candidate_action, queued = (
                upsert_candidate_and_queue(
                    cur,
                    product_meta=product_meta,
                    result=result,
                )
            )
            job_update(
                cur,
                job_id=job_id,
                status="done",
                next_attempt_at=datetime.now(
                    timezone.utc
                ),
                last_error=None,
            )
        conn.commit()
        return {
            "job_id": job_id,
            "product_id": product_id,
            "ean": ean,
            "decision": "candidate_ready",
            "candidate_action": candidate_action,
            "queued": queued,
            "cover_front_url": result.cover_front_url,
        }

    except TemporaryRemoteError as exc:
        conn.rollback()
        next_attempt = (
            datetime.now(timezone.utc)
            + timedelta(
                minutes=backoff_minutes(attempts)
            )
        )
        with conn.cursor() as cur:
            job_update(
                cur,
                job_id=job_id,
                status=(
                    "retry"
                    if attempts < cfg.max_attempts
                    else "failed"
                ),
                next_attempt_at=next_attempt,
                last_error=str(exc)[:1000],
            )
        conn.commit()
        return {
            "job_id": job_id,
            "product_id": product_id,
            "ean": ean,
            "decision": "retry",
            "error": str(exc),
        }

    except PermanentDecisionError as exc:
        conn.rollback()
        with conn.cursor() as cur:
            job_update(
                cur,
                job_id=job_id,
                status="failed",
                next_attempt_at=(
                    datetime.now(timezone.utc)
                    + timedelta(
                        days=cfg.force_retry_days
                    )
                ),
                last_error=str(exc)[:1000],
            )
        conn.commit()
        return {
            "job_id": job_id,
            "product_id": product_id,
            "ean": ean,
            "decision": "failed",
            "error": str(exc),
        }

    except Exception as exc:
        conn.rollback()
        next_attempt = (
            datetime.now(timezone.utc)
            + timedelta(
                minutes=backoff_minutes(attempts)
            )
        )
        with conn.cursor() as cur:
            job_update(
                cur,
                job_id=job_id,
                status=(
                    "retry"
                    if attempts < cfg.max_attempts
                    else "failed"
                ),
                next_attempt_at=next_attempt,
                last_error=str(exc)[:1000],
            )
        conn.commit()
        return {
            "job_id": job_id,
            "product_id": product_id,
            "ean": ean,
            "decision": "retry_or_failed",
            "error": str(exc),
        }


def write_summary(
    path_value: str | None,
    summary: dict[str, Any],
) -> None:
    if not path_value:
        return
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            summary,
            ensure_ascii=False,
            indent=2,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    cfg = parse_args()
    summary: dict[str, Any] = {
        "version": VERSION,
        "worker_id": cfg.worker_id,
        "dry_run": cfg.dry_run,
        "started_at": datetime.now(
            timezone.utc
        ).isoformat(),
        "processed": 0,
        "candidate_ready": 0,
        "errors": 0,
        "results": [],
    }
    log_json(
        "worker_start",
        version=VERSION,
        worker_id=cfg.worker_id,
        dry_run=cfg.dry_run,
        limit=cfg.limit,
        candidate_only=True,
    )

    limiter = RateLimiter(cfg.sleep_seconds)
    mb = MusicBrainzClient(
        cfg.user_agent,
        cfg.timeout_seconds,
        limiter,
    )

    with psycopg.connect(
        cfg.database_url,
        row_factory=dict_row,
    ) as conn:
        conn.autocommit = False

        if cfg.dry_run:
            with conn.cursor() as cur:
                cur.execute("set transaction read only")
            jobs = preview_jobs(conn, cfg)
            for job in jobs:
                try:
                    result = process_preview_job(
                        cfg,
                        mb,
                        job,
                    )
                except Exception as exc:
                    result = {
                        "product_id": str(
                            job.get("product_id")
                        ),
                        "ean": str(job.get("ean")),
                        "decision": "preview_error",
                        "error": str(exc),
                    }
                    summary["errors"] += 1
                summary["results"].append(result)
                summary["processed"] += 1
                if result.get("decision") == "matched":
                    summary["candidate_ready"] += 1
                log_json("preview_result", **result)
            conn.rollback()
        else:
            recovered = recover_stale_claims(
                conn,
                cfg,
            )
            summary["stale_claims_recovered"] = recovered
            for _ in range(cfg.limit):
                job = claim_one_job(conn, cfg)
                if job is None:
                    break
                result = process_claimed_job(
                    conn,
                    cfg,
                    mb,
                    job,
                )
                summary["results"].append(result)
                summary["processed"] += 1
                if result.get("decision") == "candidate_ready":
                    summary["candidate_ready"] += 1
                if result.get("error"):
                    summary["errors"] += 1
                log_json("job_result", **result)

    summary["finished_at"] = datetime.now(
        timezone.utc
    ).isoformat()
    write_summary(cfg.output_json, summary)
    log_json(
        "worker_done",
        version=VERSION,
        worker_id=cfg.worker_id,
        processed=summary["processed"],
        candidate_ready=summary["candidate_ready"],
        errors=summary["errors"],
        dry_run=cfg.dry_run,
        candidate_only=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
