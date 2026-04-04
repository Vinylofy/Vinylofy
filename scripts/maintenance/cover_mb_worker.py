#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import socket
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Any, Optional

import psycopg
import requests
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb


VERSION = "2026-04-04-stable-1"
MB_BASE = "https://musicbrainz.org/ws/2"
CAA_BASE = "https://coverartarchive.org"
DEFAULT_BUCKET = "covers"
DEFAULT_USER_AGENT = "VinylofyCoverWorker/1.0 (contact: admin@vinylofy.com)"
DEFAULT_MB_INTERVAL_SECONDS = 1.1
DEFAULT_TIMEOUT_SECONDS = 30


@dataclass
class Config:
    database_url: str
    supabase_url: str
    supabase_service_key: str
    storage_bucket: str
    user_agent: str
    worker_id: str
    sleep_seconds: float
    timeout_seconds: int
    limit: int
    force_retry_days: int
    max_attempts: int
    dry_run: bool
    prefer_release_group_front: bool


class TemporaryRemoteError(Exception):
    pass


class PermanentDecisionError(Exception):
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
    def __init__(self, user_agent: str, timeout_seconds: int, limiter: RateLimiter) -> None:
        self.timeout_seconds = timeout_seconds
        self.limiter = limiter
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json",
        })

    def _get(self, url: str, *, allow_redirects: bool = True) -> requests.Response:
        self.limiter.wait()
        resp = self.session.get(url, timeout=self.timeout_seconds, allow_redirects=allow_redirects)
        if resp.status_code == 503:
            raise TemporaryRemoteError(f"Remote service unavailable / rate limited: {url}")
        return resp

    def search_release_by_barcode(self, ean: str, limit: int = 10) -> dict[str, Any]:
        query = f"barcode:{ean}"
        url = f"{MB_BASE}/release?query={requests.utils.quote(query)}&fmt=json&limit={limit}"
        resp = self._get(url)
        resp.raise_for_status()
        return resp.json()

    def get_release_detail(self, release_id: str) -> dict[str, Any]:
        url = f"{MB_BASE}/release/{release_id}?inc=release-groups+artists&fmt=json"
        resp = self._get(url)
        resp.raise_for_status()
        return resp.json()

    def resolve_front_image(self, release_id: str, release_group_id: Optional[str], *, prefer_release_group_front: bool) -> tuple[Optional[str], Optional[dict[str, Any]]]:
        json_urls: list[str] = []
        direct_urls: list[str] = []

        if prefer_release_group_front and release_group_id:
            json_urls.append(f"{CAA_BASE}/release-group/{release_group_id}")
            direct_urls.append(f"{CAA_BASE}/release-group/{release_group_id}/front-500")

        json_urls.append(f"{CAA_BASE}/release/{release_id}")
        direct_urls.append(f"{CAA_BASE}/release/{release_id}/front-500")

        if not prefer_release_group_front and release_group_id:
            json_urls.append(f"{CAA_BASE}/release-group/{release_group_id}")
            direct_urls.append(f"{CAA_BASE}/release-group/{release_group_id}/front-500")

        cover_json = None
        for url in json_urls:
            resp = self._get(url)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            cover_json = resp.json()
            break

        for url in direct_urls:
            resp = self._get(url, allow_redirects=False)
            if resp.status_code == 404:
                continue
            if resp.is_redirect or resp.status_code in (301, 302, 303, 307, 308):
                return resp.headers.get("Location"), cover_json
            resp.raise_for_status()
            return url, cover_json

        return None, cover_json


SQL_CLAIM = """
with next_job as (
  select q.id
  from public.cover_lookup_queue q
  left join public.musicbrainz_release_cache c on c.ean = q.ean
  where q.status in ('queued', 'retry')
    and q.next_attempt_at <= now()
    and q.attempts < %(max_attempts)s
    and (
      c.ean is null
      or c.status not in ('matched', 'no_match', 'ambiguous')
      or c.checked_at < now() - make_interval(days => %(force_retry_days)s)
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
  p.cover_source,
  p.cover_priority
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
on conflict (ean) do update set
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

# Deliberately does NOT touch products.cover_status.
SQL_PRODUCT_TOUCH = """
update public.products
set cover_last_attempt_at = now()
where id = %(product_id)s
"""

# Deliberately does NOT touch products.cover_status.
SQL_PRODUCT_SUCCESS = """
update public.products
set
  cover_storage_path = %(cover_storage_path)s,
  cover_source = %(cover_source)s,
  cover_source_url = %(cover_source_url)s,
  cover_confidence = %(cover_confidence)s,
  cover_mbid = %(cover_mbid)s,
  cover_last_attempt_at = now()
where id = %(product_id)s
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


def parse_args() -> Config:
    parser = argparse.ArgumentParser(description="Stable MusicBrainz cover worker for Vinylofy")
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--supabase-url", default=os.getenv("SUPABASE_URL", ""))
    parser.add_argument("--supabase-service-key", default=os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_SECRET_KEY", ""))
    parser.add_argument("--storage-bucket", default=os.getenv("SUPABASE_STORAGE_BUCKET", DEFAULT_BUCKET))
    parser.add_argument("--user-agent", default=os.getenv("MUSICBRAINZ_USER_AGENT", DEFAULT_USER_AGENT))
    parser.add_argument("--worker-id", default=f"{socket.gethostname()}-{os.getpid()}")
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_MB_INTERVAL_SECONDS)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--force-retry-days", type=int, default=30)
    parser.add_argument("--max-attempts", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--prefer-release-group-front", action="store_true")
    args = parser.parse_args()

    missing = []
    if not args.database_url:
        missing.append("DATABASE_URL")
    if not args.supabase_url:
        missing.append("SUPABASE_URL")
    if not args.supabase_service_key:
        missing.append("SUPABASE_SERVICE_ROLE_KEY of SUPABASE_SECRET_KEY")
    if missing:
        raise SystemExit(f"Ontbrekende configuratie: {', '.join(missing)}")

    return Config(
        database_url=args.database_url,
        supabase_url=args.supabase_url.rstrip("/"),
        supabase_service_key=args.supabase_service_key,
        storage_bucket=args.storage_bucket,
        user_agent=args.user_agent,
        worker_id=args.worker_id,
        sleep_seconds=max(1.05, args.sleep_seconds),
        timeout_seconds=max(5, args.timeout_seconds),
        limit=max(1, args.limit),
        force_retry_days=max(1, args.force_retry_days),
        max_attempts=max(1, args.max_attempts),
        dry_run=bool(args.dry_run),
        prefer_release_group_front=bool(args.prefer_release_group_front),
    )


def log_json(event: str, **payload: Any) -> None:
    print(json.dumps({"event": event, **payload}, ensure_ascii=False))


def normalize_text(value: Optional[str]) -> str:
    return " ".join((value or "").strip().lower().split())


def similarity(a: Optional[str], b: Optional[str]) -> float:
    aa = normalize_text(a)
    bb = normalize_text(b)
    if not aa or not bb:
        return 0.0
    return SequenceMatcher(None, aa, bb).ratio()


def extract_artist_name(artist_credit: Any) -> str:
    if not artist_credit:
        return ""
    out: list[str] = []
    for item in artist_credit:
        if not isinstance(item, dict):
            continue
        name = item.get("name") or (item.get("artist") or {}).get("name") or ""
        joinphrase = item.get("joinphrase") or ""
        if name:
            out.append(f"{name}{joinphrase}")
    return "".join(out).strip()


def choose_candidate(search_payload: dict[str, Any], product_meta: dict[str, Any]) -> tuple[str, Optional[dict[str, Any]], float, str]:
    releases = search_payload.get("releases") or []
    if not releases:
        return "no_match", None, 0.0, "no_results"

    product_title = product_meta.get("title") or ""
    product_artist = product_meta.get("artist") or ""
    product_ean = str(product_meta.get("ean") or "")

    scored: list[tuple[float, dict[str, Any], str]] = []
    for candidate in releases[:10]:
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

        c_title = candidate.get("title") or ""
        c_artist = extract_artist_name(candidate.get("artist-credit"))

        title_sim = similarity(product_title, c_title)
        artist_sim = similarity(product_artist, c_artist)
        score += round(title_sim * 25.0, 2)
        score += round(artist_sim * 20.0, 2)

        if title_sim >= 0.92:
            reasons.append("title_strong")
        if artist_sim >= 0.92:
            reasons.append("artist_strong")
        if candidate.get("status") == "Official":
            score += 5.0
            reasons.append("official")

        scored.append((score, candidate, "+".join(reasons) or "barcode_lookup"))

    scored.sort(key=lambda item: item[0], reverse=True)
    best_score, best_candidate, basis = scored[0]
    second_score = scored[1][0] if len(scored) > 1 else 0.0

    if best_score >= 115.0 and (len(scored) == 1 or best_score - second_score >= 12.0):
        return "matched", best_candidate, best_score, basis
    if best_score >= 100.0:
        return "ambiguous", best_candidate, best_score, basis
    return "no_match", None, best_score, basis


def download_image(url: str, timeout_seconds: int) -> tuple[bytes, str, str]:
    resp = requests.get(url, timeout=timeout_seconds)
    resp.raise_for_status()
    content_type = (resp.headers.get("Content-Type") or "image/jpeg").split(";")[0].strip()
    ext = mimetypes.guess_extension(content_type) or ".jpg"
    if ext == ".jpe":
        ext = ".jpg"
    return resp.content, content_type, ext


def storage_upload(cfg: Config, ean: str, image_bytes: bytes, mime_type: str, extension: str) -> str:
    object_path = f"ean/{ean}/front{extension}"
    url = f"{cfg.supabase_url}/storage/v1/object/{cfg.storage_bucket}/{object_path}"
    headers = {
        "apikey": cfg.supabase_service_key,
        "Authorization": f"Bearer {cfg.supabase_service_key}",
        "Content-Type": mime_type,
        "x-upsert": "true",
    }
    resp = requests.post(url, headers=headers, data=image_bytes, timeout=cfg.timeout_seconds)
    if resp.status_code >= 400:
        raise RuntimeError(f"storage_upload_failed: {resp.status_code} {resp.text[:400]}")
    return object_path


def backoff_minutes(attempts: int) -> int:
    return min(24 * 60, 10 * (2 ** max(0, attempts - 1)))


def cache_upsert(cur: psycopg.Cursor[Any], *, ean: str, release_id: Optional[str], release_group_id: Optional[str], matched_title: Optional[str], matched_artist: Optional[str], matched_date: Optional[str], matched_country: Optional[str], match_score: float, match_basis: str, status: str, raw_result: Any, cover_json: Any, cover_front_url: Optional[str], last_error: Optional[str]) -> None:
    cur.execute(SQL_CACHE_UPSERT, {
        "ean": ean,
        "mb_release_id": release_id,
        "mb_release_group_id": release_group_id,
        "matched_title": matched_title,
        "matched_artist": matched_artist,
        "matched_date": matched_date,
        "matched_country": matched_country,
        "match_score": round(match_score, 2),
        "match_basis": match_basis,
        "status": status,
        "raw_result": Jsonb(raw_result) if raw_result is not None else None,
        "cover_json": Jsonb(cover_json) if cover_json is not None else None,
        "cover_front_url": cover_front_url,
        "last_error": last_error,
    })


def job_update(cur: psycopg.Cursor[Any], *, job_id: str, status: str, next_attempt_at: datetime, last_error: Optional[str]) -> None:
    cur.execute(SQL_JOB_UPDATE, {
        "job_id": job_id,
        "status": status,
        "next_attempt_at": next_attempt_at,
        "last_error": last_error,
    })


def touch_product(cur: psycopg.Cursor[Any], *, product_id: str) -> None:
    cur.execute(SQL_PRODUCT_TOUCH, {"product_id": product_id})


def process_one(conn: psycopg.Connection[Any], cfg: Config, mb: MusicBrainzClient) -> bool:
    with conn.cursor() as cur:
        cur.execute(SQL_CLAIM, {
            "worker_id": cfg.worker_id,
            "max_attempts": cfg.max_attempts,
            "force_retry_days": cfg.force_retry_days,
        })
        job = cur.fetchone()
        conn.commit()

    if not job:
        return False

    job_id = str(job["id"])
    product_id = str(job["product_id"])
    ean = str(job["ean"])
    attempts = int(job["attempts"])
    log_json("job_claimed", job_id=job_id, product_id=product_id, ean=ean, attempts=attempts, version=VERSION)

    try:
        with conn.cursor() as cur:
            cur.execute(SQL_PRODUCT_META, {"product_id": product_id})
            product_meta = cur.fetchone()
            if not product_meta:
                raise PermanentDecisionError("product_not_found")
            cur.execute(SQL_CACHE_GET, {"ean": ean})
            cache_row = cur.fetchone()
            conn.commit()

        if product_meta.get("cover_storage_path"):
            raise PermanentDecisionError("already_has_cover")

        release_payload: dict[str, Any] | None = None
        cover_json: dict[str, Any] | None = None
        front_url: Optional[str] = None
        release_id: Optional[str] = None
        release_group_id: Optional[str] = None
        matched_title: Optional[str] = None
        matched_artist: Optional[str] = None
        matched_date: Optional[str] = None
        matched_country: Optional[str] = None
        match_score = 0.0
        match_basis = ""

        if cache_row and cache_row.get("status") == "matched" and cache_row.get("cover_front_url"):
            front_url = cache_row.get("cover_front_url")
            release_id = str(cache_row.get("mb_release_id")) if cache_row.get("mb_release_id") else None
            release_group_id = str(cache_row.get("mb_release_group_id")) if cache_row.get("mb_release_group_id") else None
            matched_title = cache_row.get("matched_title")
            matched_artist = cache_row.get("matched_artist")
            matched_date = cache_row.get("matched_date")
            matched_country = cache_row.get("matched_country")
            match_score = float(cache_row.get("match_score") or 0.0)
            match_basis = str(cache_row.get("match_basis") or "cache")
            release_payload = cache_row.get("raw_result") or {}
            cover_json = cache_row.get("cover_json") or None
        else:
            search_payload = mb.search_release_by_barcode(ean, limit=10)
            decision, candidate, match_score, match_basis = choose_candidate(search_payload, product_meta)

            if decision == "no_match":
                with conn.cursor() as cur:
                    cache_upsert(
                        cur,
                        ean=ean,
                        release_id=None,
                        release_group_id=None,
                        matched_title=None,
                        matched_artist=None,
                        matched_date=None,
                        matched_country=None,
                        match_score=match_score,
                        match_basis=match_basis,
                        status="no_match",
                        raw_result=search_payload,
                        cover_json=None,
                        cover_front_url=None,
                        last_error="no_match",
                    )
                    touch_product(cur, product_id=product_id)
                    job_update(
                        cur,
                        job_id=job_id,
                        status="failed",
                        next_attempt_at=datetime.now(timezone.utc) + timedelta(days=cfg.force_retry_days),
                        last_error="no_match",
                    )
                    conn.commit()
                log_json("job_no_match", job_id=job_id, ean=ean)
                return True

            if decision == "ambiguous":
                with conn.cursor() as cur:
                    cache_upsert(
                        cur,
                        ean=ean,
                        release_id=str(candidate.get("id")) if candidate else None,
                        release_group_id=None,
                        matched_title=(candidate or {}).get("title"),
                        matched_artist=extract_artist_name((candidate or {}).get("artist-credit")),
                        matched_date=(candidate or {}).get("date"),
                        matched_country=(candidate or {}).get("country"),
                        match_score=match_score,
                        match_basis=match_basis,
                        status="ambiguous",
                        raw_result=search_payload,
                        cover_json=None,
                        cover_front_url=None,
                        last_error="ambiguous_match",
                    )
                    touch_product(cur, product_id=product_id)
                    job_update(
                        cur,
                        job_id=job_id,
                        status="failed",
                        next_attempt_at=datetime.now(timezone.utc) + timedelta(days=cfg.force_retry_days),
                        last_error="ambiguous_match",
                    )
                    conn.commit()
                log_json("job_ambiguous", job_id=job_id, ean=ean, score=round(match_score, 2))
                return True

            if not candidate:
                raise PermanentDecisionError("candidate_missing")

            release_id = str(candidate["id"])
            release_payload = mb.get_release_detail(release_id)
            release_group_id = ((release_payload.get("release-group") or {}).get("id")) or None
            front_url, cover_json = mb.resolve_front_image(
                release_id,
                release_group_id,
                prefer_release_group_front=cfg.prefer_release_group_front,
            )

            if not front_url:
                with conn.cursor() as cur:
                    cache_upsert(
                        cur,
                        ean=ean,
                        release_id=release_id,
                        release_group_id=release_group_id,
                        matched_title=release_payload.get("title"),
                        matched_artist=extract_artist_name(release_payload.get("artist-credit")),
                        matched_date=release_payload.get("date"),
                        matched_country=release_payload.get("country"),
                        match_score=match_score,
                        match_basis=match_basis,
                        status="no_match",
                        raw_result=release_payload,
                        cover_json=cover_json,
                        cover_front_url=None,
                        last_error="caa_front_not_found",
                    )
                    touch_product(cur, product_id=product_id)
                    job_update(
                        cur,
                        job_id=job_id,
                        status="failed",
                        next_attempt_at=datetime.now(timezone.utc) + timedelta(days=cfg.force_retry_days),
                        last_error="caa_front_not_found",
                    )
                    conn.commit()
                log_json("job_no_cover", job_id=job_id, ean=ean, release_id=release_id)
                return True

            matched_title = release_payload.get("title")
            matched_artist = extract_artist_name(release_payload.get("artist-credit"))
            matched_date = release_payload.get("date")
            matched_country = release_payload.get("country")

        if cfg.dry_run:
            with conn.cursor() as cur:
                cache_upsert(
                    cur,
                    ean=ean,
                    release_id=release_id,
                    release_group_id=release_group_id,
                    matched_title=matched_title,
                    matched_artist=matched_artist,
                    matched_date=matched_date,
                    matched_country=matched_country,
                    match_score=match_score,
                    match_basis=match_basis,
                    status="matched",
                    raw_result=release_payload,
                    cover_json=cover_json,
                    cover_front_url=front_url,
                    last_error=None,
                )
                job_update(
                    cur,
                    job_id=job_id,
                    status="done",
                    next_attempt_at=datetime.now(timezone.utc),
                    last_error=None,
                )
                conn.commit()
            log_json("job_dry_run_matched", job_id=job_id, ean=ean, front_url=front_url)
            return True

        image_bytes, mime_type, extension = download_image(front_url, cfg.timeout_seconds)
        storage_path = storage_upload(cfg, ean, image_bytes, mime_type, extension)

        with conn.cursor() as cur:
            cache_upsert(
                cur,
                ean=ean,
                release_id=release_id,
                release_group_id=release_group_id,
                matched_title=matched_title,
                matched_artist=matched_artist,
                matched_date=matched_date,
                matched_country=matched_country,
                match_score=match_score,
                match_basis=match_basis,
                status="matched",
                raw_result=release_payload,
                cover_json=cover_json,
                cover_front_url=front_url,
                last_error=None,
            )
            cur.execute(SQL_PRODUCT_SUCCESS, {
                "product_id": product_id,
                "cover_storage_path": storage_path,
                "cover_source": "cover_art_archive",
                "cover_source_url": front_url,
                "cover_confidence": min(1.0, round(match_score / 125.0, 4)),
                "cover_mbid": release_id,
            })
            job_update(
                cur,
                job_id=job_id,
                status="done",
                next_attempt_at=datetime.now(timezone.utc),
                last_error=None,
            )
            conn.commit()

        log_json("job_success", job_id=job_id, ean=ean, storage_path=storage_path)
        return True

    except TemporaryRemoteError as exc:
        conn.rollback()
        next_attempt = datetime.now(timezone.utc) + timedelta(minutes=backoff_minutes(attempts))
        with conn.cursor() as cur:
            touch_product(cur, product_id=product_id)
            job_update(
                cur,
                job_id=job_id,
                status="retry",
                next_attempt_at=next_attempt,
                last_error=str(exc)[:1000],
            )
            conn.commit()
        log_json("job_retry", job_id=job_id, ean=ean, error=str(exc), next_attempt_at=next_attempt.isoformat())
        return True

    except PermanentDecisionError as exc:
        conn.rollback()
        with conn.cursor() as cur:
            job_update(
                cur,
                job_id=job_id,
                status="failed",
                next_attempt_at=datetime.now(timezone.utc) + timedelta(days=cfg.force_retry_days),
                last_error=str(exc)[:1000],
            )
            conn.commit()
        log_json("job_failed_permanent", job_id=job_id, ean=ean, error=str(exc))
        return True

    except Exception as exc:
        conn.rollback()
        next_attempt = datetime.now(timezone.utc) + timedelta(minutes=backoff_minutes(attempts))
        with conn.cursor() as cur:
            touch_product(cur, product_id=product_id)
            job_update(
                cur,
                job_id=job_id,
                status="retry" if attempts < cfg.max_attempts else "failed",
                next_attempt_at=next_attempt,
                last_error=str(exc)[:1000],
            )
            conn.commit()
        log_json("job_failed", job_id=job_id, ean=ean, error=str(exc), retry=attempts < cfg.max_attempts)
        return True


def main() -> int:
    cfg = parse_args()
    log_json("worker_start", version=VERSION, worker_id=cfg.worker_id, dry_run=cfg.dry_run, limit=cfg.limit)
    limiter = RateLimiter(cfg.sleep_seconds)
    mb = MusicBrainzClient(cfg.user_agent, cfg.timeout_seconds, limiter)

    processed = 0
    with psycopg.connect(cfg.database_url, row_factory=dict_row) as conn:
        for _ in range(cfg.limit):
            if not process_one(conn, cfg, mb):
                break
            processed += 1

    log_json("worker_done", version=VERSION, worker_id=cfg.worker_id, processed=processed, dry_run=cfg.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
