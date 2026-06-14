#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg
import requests
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


MB_BASE = "https://musicbrainz.org/ws/2"
DEFAULT_USER_AGENT = "VinylofyMasterdataWorker/1.0 (https://vinylofy.com; contact: info@vinylofy.com)"
DEFAULT_SLEEP_SECONDS = 1.1
DEFAULT_TIMEOUT_SECONDS = 30


@dataclass(frozen=True)
class Config:
    database_url: str
    limit: int
    ean: str | None
    write: bool
    force: bool
    retry_days: int
    min_score: int
    update_canonical: bool
    overwrite_shop: bool
    include_verified: bool
    user_agent: str
    sleep_seconds: float
    timeout_seconds: int
    output_json: str | None


def load_env() -> None:
    if load_dotenv:
        load_dotenv(".env.local", override=False)
        load_dotenv(override=False)


def normalize_ean(value: str | None) -> str | None:
    digits = re.sub(r"\D", "", value or "")
    if not digits:
        return None
    if len(digits) == 11:
        digits = "0" + digits
    if len(digits) not in (8, 12, 13, 14):
        return None
    return digits


def parse_args() -> Config:
    load_env()

    parser = argparse.ArgumentParser(
        description="Enrich Vinylofy products masterdata via MusicBrainz barcode/EAN lookup."
    )
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--ean", default=None, help="Alleen één EAN verwerken")
    parser.add_argument("--write", action="store_true", help="Werkelijk naar products schrijven")
    parser.add_argument("--force", action="store_true", help="Negeer retry window")
    parser.add_argument("--retry-days", type=int, default=30)
    parser.add_argument("--min-score", type=int, default=90)
    parser.add_argument(
        "--update-canonical",
        action="store_true",
        help="Vul artist/title/format_label in products aan vanuit MusicBrainz wanneer veilig",
    )
    parser.add_argument(
        "--overwrite-shop",
        action="store_true",
        help="Sta toe dat shop/importer/unknown metadata wordt overschreven door MusicBrainz",
    )
    parser.add_argument(
        "--include-verified",
        action="store_true",
        help="Neem ook manual/verified records mee; default worden die overgeslagen",
    )
    parser.add_argument("--user-agent", default=os.getenv("MUSICBRAINZ_USER_AGENT", DEFAULT_USER_AGENT))
    parser.add_argument("--sleep-seconds", type=float, default=DEFAULT_SLEEP_SECONDS)
    parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--output-json", default=None)

    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL ontbreekt. Zet env var of geef --database-url mee.")

    return Config(
        database_url=args.database_url,
        limit=max(1, args.limit),
        ean=normalize_ean(args.ean),
        write=bool(args.write),
        force=bool(args.force),
        retry_days=max(1, args.retry_days),
        min_score=max(0, min(100, args.min_score)),
        update_canonical=bool(args.update_canonical),
        overwrite_shop=bool(args.overwrite_shop),
        include_verified=bool(args.include_verified),
        user_agent=(args.user_agent or DEFAULT_USER_AGENT).strip() or DEFAULT_USER_AGENT,
        sleep_seconds=max(1.0, args.sleep_seconds),
        timeout_seconds=max(5, args.timeout_seconds),
        output_json=args.output_json,
    )


class MusicBrainzClient:
    def __init__(self, user_agent: str, timeout_seconds: int, sleep_seconds: float) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json",
            }
        )
        self.timeout_seconds = timeout_seconds
        self.sleep_seconds = sleep_seconds
        self._last_call = 0.0

    def _rate_limit(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self.sleep_seconds:
            time.sleep(self.sleep_seconds - elapsed)
        self._last_call = time.monotonic()

    def get(self, path: str, params: dict[str, str]) -> dict[str, Any]:
        self._rate_limit()
        response = self.session.get(
            f"{MB_BASE}{path}",
            params=params,
            timeout=self.timeout_seconds,
        )
        if response.status_code == 403:
            ua = self.session.headers.get("User-Agent", "")
            raise RuntimeError(
                "MusicBrainz 403 Forbidden. Waarschijnlijk ontbreekt een geldige User-Agent. "
                f"Huidige User-Agent={ua!r}"
            )
        if response.status_code == 403:
            ua = self.session.headers.get("User-Agent", "")
            raise RuntimeError(
                "MusicBrainz 403 Forbidden. Waarschijnlijk ontbreekt een geldige User-Agent. "
                f"Huidige User-Agent={ua!r}"
            )
        if response.status_code == 503:
            raise RuntimeError("MusicBrainz 503/rate limited; verlaag batch of verhoog sleep.")
        response.raise_for_status()
        return response.json()

    def search_release_by_barcode(self, ean: str, limit: int = 5) -> dict[str, Any]:
        return self.get(
            "/release",
            {
                "query": f"barcode:{ean}",
                "fmt": "json",
                "limit": str(limit),
            },
        )

    def get_release_detail(self, release_id: str) -> dict[str, Any]:
        return self.get(
            f"/release/{release_id}",
            {
                "inc": "artists+labels+media+release-groups",
                "fmt": "json",
            },
        )


def get_table_columns(conn: psycopg.Connection, table_name: str, schema: str = "public") -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = %s
              and table_name = %s
            """,
            (schema, table_name),
        )
        return {str(row["column_name"]) for row in cur.fetchall()}


def require_products_columns(conn: psycopg.Connection) -> None:
    required = {
        "id",
        "ean",
        "artist",
        "title",
        "metadata_source",
        "metadata_status",
        "metadata_confidence",
        "metadata_last_enriched_at",
        "metadata_needs_review",
        "musicbrainz_release_id",
        "musicbrainz_release_group_id",
        "musicbrainz_artist",
        "musicbrainz_title",
        "musicbrainz_format",
        "musicbrainz_release_date",
        "musicbrainz_release_year",
        "musicbrainz_country",
        "musicbrainz_label",
        "musicbrainz_match_score",
        "musicbrainz_match_basis",
        "musicbrainz_status",
        "musicbrainz_checked_at",
        "metadata_raw",
    }
    columns = get_table_columns(conn, "products")
    missing = sorted(required - columns)
    if missing:
        raise SystemExit(
            "products mist kolommen voor masterdata enrichment: "
            + ", ".join(missing)
            + ". Draai eerst de migration."
        )


def artist_credit_name(release: dict[str, Any]) -> str | None:
    parts: list[str] = []
    for item in release.get("artist-credit") or []:
        if isinstance(item, dict):
            artist = item.get("artist") or {}
            name = artist.get("name") or item.get("name")
            if name:
                parts.append(str(name))
            joinphrase = item.get("joinphrase")
            if joinphrase:
                parts.append(str(joinphrase))
        elif isinstance(item, str):
            parts.append(item)
    result = "".join(parts).strip()
    return result or None


def unique_join(values: list[str]) -> str | None:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = re.sub(r"\s+", " ", str(value or "")).strip()
        if cleaned and cleaned.lower() not in seen:
            seen.add(cleaned.lower())
            result.append(cleaned)
    return ", ".join(result) if result else None


def parse_year(date_value: str | None) -> int | None:
    if not date_value:
        return None
    match = re.match(r"^(\d{4})", date_value)
    return int(match.group(1)) if match else None


def release_group_id_from(release: dict[str, Any]) -> str | None:
    release_group = release.get("release-group") or {}
    return release_group.get("id")


def extract_label(detail: dict[str, Any]) -> str | None:
    labels: list[str] = []
    for item in detail.get("label-info") or []:
        label = item.get("label") or {}
        name = label.get("name")
        if name:
            labels.append(str(name))
    return unique_join(labels)


def extract_format(detail: dict[str, Any]) -> str | None:
    formats: list[str] = []
    for medium in detail.get("media") or []:
        fmt = medium.get("format")
        if fmt:
            formats.append(str(fmt))
    return unique_join(formats)


def choose_best_release(search_result: dict[str, Any]) -> tuple[dict[str, Any] | None, str, int | None]:
    releases = search_result.get("releases") or []
    if not releases:
        return None, "no_match", None

    def score_of(item: dict[str, Any]) -> int:
        try:
            return int(item.get("score") or 0)
        except (TypeError, ValueError):
            return 0

    releases = sorted(releases, key=score_of, reverse=True)
    top = releases[0]
    top_score = score_of(top)

    if len(releases) > 1:
        second = releases[1]
        second_score = score_of(second)
        top_title = str(top.get("title") or "").strip().lower()
        second_title = str(second.get("title") or "").strip().lower()
        if second_score == top_score and second.get("id") != top.get("id") and top_title != second_title:
            return top, "ambiguous", top_score

    return top, "matched", top_score


def should_replace_canonical(row: dict[str, Any], existing_value: Any, cfg: Config) -> bool:
    if not cfg.update_canonical:
        return False
    if existing_value is None or str(existing_value).strip() == "":
        return True
    if not cfg.overwrite_shop:
        return False

    status = str(row.get("metadata_status") or "").lower()
    source = str(row.get("metadata_source") or "").lower()
    protected = {"manual", "verified"}
    if status in protected or source in protected:
        return False

    weak_sources = {"", "unknown", "shop", "shop_observed", "raw_shop", "importer", "fallback"}
    weak_statuses = {"", "unknown", "placeholder", "needs_enrichment", "shop", "fallback_enriched"}

    return source in weak_sources or status in weak_statuses


def select_candidates(conn: psycopg.Connection, cfg: Config) -> list[dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select
              id,
              ean,
              artist,
              title,
              case
                when exists (
                  select 1 from information_schema.columns
                  where table_schema = 'public'
                    and table_name = 'products'
                    and column_name = 'format_label'
                )
                then format_label
                else null
              end as format_label,
              metadata_source,
              metadata_status,
              musicbrainz_checked_at
            from public.products
            where ean is not null
              and char_length(regexp_replace(ean::text, '\\D', '', 'g')) in (8, 12, 13, 14)
              and (%(ean)s::text is null or regexp_replace(ean::text, '\\D', '', 'g') = %(ean)s::text)
              and (
                %(include_verified)s = true
                or coalesce(metadata_status, '') not in ('manual', 'verified')
                or coalesce(metadata_source, '') not in ('manual', 'verified')
              )
              and (
                %(force)s = true
                or musicbrainz_checked_at is null
                or musicbrainz_checked_at < now() - make_interval(days => %(retry_days)s)
                or coalesce(nullif(artist, ''), '') = ''
                or coalesce(nullif(title, ''), '') = ''
                or coalesce(metadata_status, '') in ('', 'unknown', 'placeholder', 'needs_enrichment', 'error', 'no_match')
              )
            order by musicbrainz_checked_at asc nulls first,
                     updated_at desc nulls last,
                     created_at desc nulls last
            limit %(limit)s
            """,
            {
                "ean": cfg.ean,
                "include_verified": cfg.include_verified,
                "force": cfg.force,
                "retry_days": cfg.retry_days,
                "limit": cfg.limit,
            },
        )
        return list(cur.fetchall())


def update_product(
    conn: psycopg.Connection,
    row: dict[str, Any],
    mb: dict[str, Any],
    cfg: Config,
) -> None:
    columns = get_table_columns(conn, "products")
    has_format_label = "format_label" in columns

    canonical_artist = row.get("artist")
    canonical_title = row.get("title")
    canonical_format = row.get("format_label")

    can_promote_canonical = (
        mb.get("status") == "matched"
        and int(mb.get("score") or 0) >= 95
    )

    if can_promote_canonical and should_replace_canonical(row, canonical_artist, cfg) and mb.get("artist"):
        canonical_artist = mb["artist"]
    if can_promote_canonical and should_replace_canonical(row, canonical_title, cfg) and mb.get("title"):
        canonical_title = mb["title"]
    if has_format_label and can_promote_canonical and should_replace_canonical(row, canonical_format, cfg) and mb.get("format"):
        canonical_format = mb["format"]

    base_sql = """
        update public.products
        set
          musicbrainz_release_id = %(release_id)s,
          musicbrainz_release_group_id = %(release_group_id)s,
          musicbrainz_artist = %(artist)s,
          musicbrainz_title = %(title)s,
          musicbrainz_format = %(format)s,
          musicbrainz_release_date = %(release_date)s,
          musicbrainz_release_year = %(release_year)s,
          musicbrainz_country = %(country)s,
          musicbrainz_label = %(label)s,
          musicbrainz_match_score = %(score)s,
          musicbrainz_match_basis = %(basis)s,
          musicbrainz_status = %(status)s,
          musicbrainz_checked_at = now(),
          metadata_raw = %(raw)s,
          metadata_last_enriched_at = now(),
          metadata_needs_review = %(needs_review)s,
          metadata_confidence = %(confidence)s,
          metadata_source = case
            when %(matched)s = true then 'musicbrainz'
            else coalesce(metadata_source, 'unknown')
          end,
          metadata_status = case
            when %(matched)s = true and coalesce(metadata_status, '') not in ('manual', 'verified') then 'enriched'
            when %(matched)s = false and coalesce(metadata_status, '') in ('', 'unknown') then 'needs_enrichment'
            else metadata_status
          end,
          artist = %(canonical_artist)s,
          title = %(canonical_title)s
          {format_update}
        where id = %(product_id)s
    """

    format_update = ", format_label = %(canonical_format)s" if has_format_label else ""

    params = {
        "product_id": row["id"],
        "release_id": mb.get("release_id"),
        "release_group_id": mb.get("release_group_id"),
        "artist": mb.get("artist"),
        "title": mb.get("title"),
        "format": mb.get("format"),
        "release_date": mb.get("release_date"),
        "release_year": mb.get("release_year"),
        "country": mb.get("country"),
        "label": mb.get("label"),
        "score": mb.get("score"),
        "basis": mb.get("basis"),
        "status": mb.get("status"),
        "raw": Jsonb(mb.get("raw") or {}),
        "needs_review": mb.get("needs_review", False),
        "confidence": mb.get("confidence"),
        "matched": mb.get("status") == "matched",
        "canonical_artist": canonical_artist,
        "canonical_title": canonical_title,
        "canonical_format": canonical_format,
    }

    with conn.cursor() as cur:
        cur.execute(base_sql.format(format_update=format_update), params)


def lookup_one(client: MusicBrainzClient, ean: str, cfg: Config) -> dict[str, Any]:
    now = datetime.now(timezone.utc).isoformat()
    search_result = client.search_release_by_barcode(ean)
    release, basis, score = choose_best_release(search_result)

    if not release:
        return {
            "ean": ean,
            "status": "no_match",
            "basis": basis,
            "score": score,
            "confidence": 0,
            "needs_review": True,
            "raw": {
                "checked_at": now,
                "search_result": search_result,
            },
        }

    if score is None or score < cfg.min_score:
        return {
            "ean": ean,
            "status": "low_score",
            "basis": basis,
            "score": score,
            "confidence": score or 0,
            "needs_review": True,
            "raw": {
                "checked_at": now,
                "selected_release": release,
                "search_result": search_result,
            },
        }

    release_id = release.get("id")
    detail: dict[str, Any] = {}
    if release_id:
        detail = client.get_release_detail(str(release_id))

    merged = dict(release)
    merged.update(detail or {})

    title = merged.get("title")
    artist = artist_credit_name(merged)
    release_date = merged.get("date")
    release_year = parse_year(release_date)
    country = merged.get("country")
    label = extract_label(merged)
    fmt = extract_format(merged)
    release_group_id = release_group_id_from(merged)

    confidence = score or 0
    status = "matched" if basis == "matched" else basis

    return {
        "ean": ean,
        "status": status,
        "basis": basis,
        "score": score,
        "confidence": confidence,
        "needs_review": basis != "matched" or confidence < 95,
        "release_id": release_id,
        "release_group_id": release_group_id,
        "artist": artist,
        "title": title,
        "format": fmt,
        "release_date": release_date,
        "release_year": release_year,
        "country": country,
        "label": label,
        "raw": {
            "checked_at": now,
            "selected_release": release,
            "release_detail": detail,
        },
    }


def main() -> int:
    cfg = parse_args()
    client = MusicBrainzClient(
        user_agent=cfg.user_agent,
        timeout_seconds=cfg.timeout_seconds,
        sleep_seconds=cfg.sleep_seconds,
    )

    summary: dict[str, Any] = {
        "write": cfg.write,
        "limit": cfg.limit,
        "update_canonical": cfg.update_canonical,
        "overwrite_shop": cfg.overwrite_shop,
        "processed": 0,
        "written": 0,
        "matched": 0,
        "no_match": 0,
        "low_score": 0,
        "ambiguous": 0,
        "errors": 0,
        "items": [],
    }

    with psycopg.connect(cfg.database_url, row_factory=dict_row) as conn:
        require_products_columns(conn)
        candidates = select_candidates(conn, cfg)

        print(
            json.dumps(
                {
                    "selected": len(candidates),
                    "write": cfg.write,
                    "update_canonical": cfg.update_canonical,
                    "overwrite_shop": cfg.overwrite_shop,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

        for row in candidates:
            ean = normalize_ean(str(row["ean"]))
            if not ean:
                continue

            try:
                mb = lookup_one(client, ean, cfg)
                summary["processed"] += 1
                summary[mb["status"]] = int(summary.get(mb["status"], 0)) + 1

                item = {
                    "product_id": str(row["id"]),
                    "ean": ean,
                    "current_artist": row.get("artist"),
                    "current_title": row.get("title"),
                    "mb_status": mb.get("status"),
                    "mb_score": mb.get("score"),
                    "mb_artist": mb.get("artist"),
                    "mb_title": mb.get("title"),
                    "mb_format": mb.get("format"),
                    "mb_release_id": mb.get("release_id"),
                }
                summary["items"].append(item)
                print(json.dumps(item, ensure_ascii=False), flush=True)

                if cfg.write:
                    update_product(conn, row, mb, cfg)
                    conn.commit()
                    summary["written"] += 1
                else:
                    conn.rollback()

            except Exception as exc:
                conn.rollback()
                summary["processed"] += 1
                summary["errors"] += 1
                error_item = {
                    "product_id": str(row.get("id")),
                    "ean": ean,
                    "error": str(exc),
                }
                summary["items"].append(error_item)
                print(json.dumps(error_item, ensure_ascii=False), flush=True)

    if cfg.output_json:
        os.makedirs(os.path.dirname(cfg.output_json), exist_ok=True)
        with open(cfg.output_json, "w", encoding="utf-8") as handle:
            json.dump(summary, handle, ensure_ascii=False, indent=2, default=str)

    print(json.dumps({k: v for k, v in summary.items() if k != "items"}, ensure_ascii=False), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
