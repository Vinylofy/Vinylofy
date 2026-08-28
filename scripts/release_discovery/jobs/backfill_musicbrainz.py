from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable

from scripts.importers.common import normalize_ean, strict_normalize_gtin
from scripts.release_discovery.core.candidates import ReleaseObservation, union_release_observations
from scripts.scrapers.usf.core.db import db_connection


SOURCE_SHOP = "musicbrainz"
FULL_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
VINYL_MARKERS = (
    re.compile(r"\bvinyl\b", re.I),
    re.compile(r"(?<![a-z0-9])lp(?:s)?(?![a-z0-9])", re.I),
    re.compile(r"\b(?:12|10|7)\s*(?:\"|inch|inches)\b", re.I),
)


@dataclass(frozen=True)
class MusicBrainzReleaseCandidate:
    product_id: str
    ean: str
    gtin_normalized: str
    artist: str
    title: str
    release_date: date
    source_url: str
    format: str | None
    label: str | None
    source_payload: dict[str, object]


def parse_full_release_date(value: object) -> date | None:
    text = "" if value is None else str(value).strip()
    if not FULL_DATE_RE.match(text):
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def is_vinyl_release(*values: object) -> bool:
    combined = " ".join("" if value is None else str(value) for value in values)
    return any(marker.search(combined) for marker in VINYL_MARKERS)


def source_url_for_release(release_id: object, product_id: str) -> str:
    release_id_text = "" if release_id is None else str(release_id).strip()
    if release_id_text:
        return f"https://musicbrainz.org/release/{release_id_text}"
    return f"vinylofy:musicbrainz-product:{product_id}"


def candidate_from_product_row(row: dict[str, object], *, min_date: date, max_date: date) -> MusicBrainzReleaseCandidate | None:
    release_date = parse_full_release_date(row.get("musicbrainz_release_date"))
    if release_date is None or not (min_date <= release_date <= max_date):
        return None

    format_label = row.get("musicbrainz_format") or row.get("format_label")
    if not is_vinyl_release(format_label):
        return None

    ean = normalize_ean(row.get("ean"))
    gtin_normalized = strict_normalize_gtin(ean)
    if not ean or not gtin_normalized:
        return None

    product_id = str(row["id"])
    artist = str(row.get("musicbrainz_artist") or row.get("artist") or "").strip()
    title = str(row.get("musicbrainz_title") or row.get("title") or "").strip()
    if not artist or not title:
        return None

    source_payload = {
        "source": "products.musicbrainz_release_date",
        "musicbrainz_release_id": row.get("musicbrainz_release_id"),
        "musicbrainz_release_group_id": row.get("musicbrainz_release_group_id"),
        "musicbrainz_match_score": row.get("musicbrainz_match_score"),
        "musicbrainz_match_basis": row.get("musicbrainz_match_basis"),
        "musicbrainz_status": row.get("musicbrainz_status"),
        "gtin_normalized": gtin_normalized,
    }

    return MusicBrainzReleaseCandidate(
        product_id=product_id,
        ean=ean,
        gtin_normalized=gtin_normalized,
        artist=artist,
        title=title,
        release_date=release_date,
        source_url=source_url_for_release(row.get("musicbrainz_release_id"), product_id),
        format=str(format_label).strip() if format_label else None,
        label=str(row.get("musicbrainz_label")).strip() if row.get("musicbrainz_label") else None,
        source_payload=source_payload,
    )


def release_window(*, past_days: int, future_days: int, anchor: date | None = None) -> tuple[date, date]:
    today = anchor or date.today()
    return today - timedelta(days=past_days), today + timedelta(days=future_days)


def select_product_rows(cur, *, min_date: date, max_date: date, limit: int) -> list[dict[str, object]]:
    cur.execute(
        """
        select
          id,
          ean,
          artist,
          title,
          format_label,
          musicbrainz_release_id,
          musicbrainz_release_group_id,
          musicbrainz_artist,
          musicbrainz_title,
          musicbrainz_format,
          musicbrainz_release_date,
          musicbrainz_label,
          musicbrainz_match_score,
          musicbrainz_match_basis,
          musicbrainz_status
        from public.products
        where musicbrainz_release_date ~ '^\\d{4}-\\d{2}-\\d{2}$'
          and musicbrainz_release_date::date between %s and %s
          and musicbrainz_release_id is not null
          and coalesce(musicbrainz_status, '') = 'matched'
        order by musicbrainz_release_date::date, artist, title, id
        limit %s
        """,
        (min_date, max_date, limit),
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def find_date_conflicts(cur, candidates: Iterable[MusicBrainzReleaseCandidate]) -> list[dict[str, object]]:
    candidate_list = list(candidates)
    if not candidate_list:
        return []

    eans = [candidate.ean for candidate in candidate_list]
    cur.execute(
        """
        select ean, source_shop, release_date
        from public.release_calendar
        where ean = any(%s)
          and status = 'active'
        order by ean, source_shop, release_date
        """,
        (eans,),
    )
    existing_by_ean: dict[str, list[tuple[str, date]]] = {}
    for ean, source_shop, release_date in cur.fetchall():
        existing_by_ean.setdefault(str(ean), []).append((str(source_shop), release_date))

    conflicts: list[dict[str, object]] = []
    for candidate in candidate_list:
        for source_shop, existing_date in existing_by_ean.get(candidate.ean, []):
            if existing_date != candidate.release_date:
                conflicts.append(
                    {
                        "ean": candidate.ean,
                        "existing_source": source_shop,
                        "existing_release_date": existing_date.isoformat(),
                        "incoming_source": SOURCE_SHOP,
                        "incoming_release_date": candidate.release_date.isoformat(),
                    }
                )
    return conflicts


def upsert_release(cur, candidate: MusicBrainzReleaseCandidate) -> bool:
    params = {
        "ean": candidate.ean,
        "artist": candidate.artist,
        "title": candidate.title,
        "release_date": candidate.release_date.isoformat(),
        "source_shop": SOURCE_SHOP,
        "source_url": candidate.source_url,
        "format": candidate.format,
        "label": candidate.label,
        "product_id": candidate.product_id,
        "source_payload": json.dumps(candidate.source_payload, ensure_ascii=False),
    }
    cur.execute(
        """
        with existing as (
          select id
          from public.release_calendar
          where source_url = %(source_url)s
             or (
                  ean = %(ean)s
                  and source_shop = %(source_shop)s
                  and release_date = %(release_date)s::date
             )
          order by id
          for update
        )
        select id from existing
        """,
        params,
    )
    existing_rows = cur.fetchall()
    if len(existing_rows) > 1:
        raise RuntimeError("MusicBrainz release-identiteitsconflict in release_calendar.")

    if existing_rows:
        params["existing_id"] = existing_rows[0][0]
        cur.execute(
            """
            update public.release_calendar
            set
              ean = coalesce(public.release_calendar.ean, %(ean)s),
              artist = case when public.release_calendar.artist = '' then %(artist)s else public.release_calendar.artist end,
              title = case when public.release_calendar.title = '' then %(title)s else public.release_calendar.title end,
              format = coalesce(public.release_calendar.format, %(format)s),
              label = coalesce(public.release_calendar.label, %(label)s),
              product_id = coalesce(public.release_calendar.product_id, %(product_id)s),
              status = 'active',
              source_payload = public.release_calendar.source_payload || %(source_payload)s::jsonb,
              last_seen_at = now(),
              updated_at = now()
            where id = %(existing_id)s
            """,
            params,
        )
        return False

    cur.execute(
        """
        insert into public.release_calendar (
          ean,
          artist,
          title,
          release_date,
          source_shop,
          source_url,
          format,
          label,
          product_id,
          status,
          source_payload,
          first_seen_at,
          last_seen_at,
          created_at,
          updated_at
        )
        values (
          %(ean)s,
          %(artist)s,
          %(title)s,
          %(release_date)s::date,
          %(source_shop)s,
          %(source_url)s,
          %(format)s,
          %(label)s,
          %(product_id)s,
          'active',
          %(source_payload)s::jsonb,
          now(),
          now(),
          now(),
          now()
        )
        """,
        params,
    )
    return True


def run(args: argparse.Namespace) -> int:
    min_date, max_date = release_window(
        past_days=args.past_days,
        future_days=args.future_days,
    )
    summary = {
        "source": SOURCE_SHOP,
        "write": args.write,
        "min_date": min_date.isoformat(),
        "max_date": max_date.isoformat(),
        "raw_product_rows": 0,
        "candidates": 0,
        "unique_eans_after_union": 0,
        "date_conflicts": 0,
        "inserted": 0,
        "updated": 0,
        "databasewrites": int(args.write),
    }

    with db_connection() as conn:
        with conn.cursor() as cur:
            rows = select_product_rows(cur, min_date=min_date, max_date=max_date, limit=args.limit)
            summary["raw_product_rows"] = len(rows)
            candidates = [
                candidate
                for row in rows
                if (candidate := candidate_from_product_row(row, min_date=min_date, max_date=max_date))
            ]
            summary["candidates"] = len(candidates)
            unioned = union_release_observations(
                [
                    ReleaseObservation(
                        source=SOURCE_SHOP,
                        ean=candidate.ean,
                        release_date=candidate.release_date,
                        product_id=candidate.product_id,
                    )
                    for candidate in candidates
                ]
            )
            summary["unique_eans_after_union"] = len(unioned)
            conflicts = find_date_conflicts(cur, candidates)
            summary["date_conflicts"] = len(conflicts)

            for conflict in conflicts[:25]:
                print("[RELEASE-MB-CONFLICT]", conflict, flush=True)

            if args.write:
                for candidate in candidates:
                    inserted = upsert_release(cur, candidate)
                    if inserted:
                        summary["inserted"] += 1
                    else:
                        summary["updated"] += 1
                conn.commit()
            else:
                conn.rollback()

    print("[RELEASE-MB] summary", summary, flush=True)
    if not args.write:
        print("[RELEASE-MB] dry-run complete; geen databasewrites.", flush=True)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill release_calendar from existing MusicBrainz product metadata."
    )
    parser.add_argument("--past-days", type=int, default=14)
    parser.add_argument("--future-days", type=int, default=0)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.past_days < 0 or args.future_days < 0:
        raise SystemExit("[ERROR] datumvenster mag niet negatief zijn.")
    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
