#!/usr/bin/env python3
"""Bounded, resumable MusicBrainz output-evidence backfill for FTG artists."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol

import psycopg
import requests

try:
    from scripts.follow_the_groove.collector import DEFAULT_USER_AGENT, HttpJsonClient, MB_BASE
except ModuleNotFoundError:  # direct ``python scripts/follow_the_groove/output_evidence.py``
    from collector import DEFAULT_USER_AGENT, HttpJsonClient, MB_BASE

MAX_BATCH_SIZE = 25
ENTITY_CHECKS = (
    ("release-group", "release-groups", "release_group", "release_group_primary_artist"),
    ("release", "releases", "release", "release_primary_artist"),
    ("recording", "recordings", "recording", "recording_artist"),
)
PILOT_NAMES = (
    "Foo Fighters", "Queens of the Stone Age", "Dave Grohl", "Them Crooked Vultures",
    "Kyuss", "Nirvana", "Miles Davis", "Prince",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Artist:
    id: str
    mbid: str
    name: str


@dataclass(frozen=True)
class Evidence:
    artist_id: str
    artist_mbid: str
    evidence_type: str
    source_system: str
    source_entity_kind: str
    source_entity_id: str
    provenance: dict[str, Any]
    verified_at: datetime

    @property
    def natural_key(self) -> tuple[str, str, str, str]:
        return (self.artist_id, self.evidence_type, self.source_entity_kind, self.source_entity_id)


class MBClient(Protocol):
    request_count: int
    retry_count: int
    cache_hits: int

    def get(self, url: str, params: dict[str, str]) -> dict[str, Any]: ...


def direct_credit(entity: dict[str, Any], artist_mbid: str) -> bool:
    credits = entity.get("artist-credit")
    if not isinstance(credits, list):
        return False
    return any(
        isinstance(credit, dict)
        and isinstance(credit.get("artist"), dict)
        and credit["artist"].get("id") == artist_mbid
        for credit in credits
    )


def evidence_from_entity(
    artist: Artist, entity: dict[str, Any], entity_kind: str, evidence_type: str,
    source_system: str, verified_at: datetime, provenance: dict[str, Any],
) -> Evidence | None:
    entity_id = entity.get("id")
    if not entity_id or not direct_credit(entity, artist.mbid):
        return None
    try:
        uuid.UUID(str(entity_id))
    except (ValueError, TypeError, AttributeError):
        return None
    return Evidence(artist.id, artist.mbid, evidence_type, source_system, entity_kind,
                    str(entity_id), provenance, verified_at)


def local_release_evidence(artist: Artist, detail: Any, verified_at: datetime) -> Evidence | None:
    if not isinstance(detail, dict) or not direct_credit(detail, artist.mbid):
        return None
    release_group = detail.get("release-group")
    if isinstance(release_group, dict) and release_group.get("id"):
        entity = {"id": release_group["id"], "artist-credit": detail["artist-credit"]}
        return evidence_from_entity(
            artist, entity, "release_group", "release_group_primary_artist",
            "vinylofy_local_musicbrainz", verified_at, {"path": "products.metadata_raw.release_detail"},
        )
    return evidence_from_entity(
        artist, detail, "release", "release_primary_artist", "vinylofy_local_musicbrainz",
        verified_at, {"path": "products.metadata_raw.release_detail"},
    )


def local_recording_evidence(artist: Artist, row: dict[str, Any], verified_at: datetime) -> Evidence | None:
    if row.get("evidence_kind") != "artist_credit" or row.get("classification") != "allowed":
        return None
    if artist.id not in {row.get("source_artist_id"), row.get("target_artist_id")}:
        return None
    recording_mbid = row.get("recording_mbid")
    if not recording_mbid:
        return None
    return Evidence(
        artist.id, artist.mbid, "recording_artist", "vinylofy_local_musicbrainz",
        "recording", str(recording_mbid), {"path": "artist_relation_evidence.artist_credit"}, verified_at,
    )


def lookup_musicbrainz(artist: Artist, client: MBClient, verified_at: datetime) -> Evidence | None:
    for endpoint, collection, entity_kind, evidence_type in ENTITY_CHECKS:
        payload = client.get(
            f"{MB_BASE}/{endpoint}",
            {"artist": artist.mbid, "inc": "artist-credits", "limit": "1", "fmt": "json"},
        )
        entities = payload.get(collection)
        if not isinstance(entities, list):
            continue
        for entity in entities[:1]:
            if isinstance(entity, dict):
                evidence = evidence_from_entity(
                    artist, entity, entity_kind, evidence_type, "musicbrainz", verified_at,
                    {"endpoint": endpoint, "query": "browse_by_artist", "limit": 1},
                )
                if evidence:
                    return evidence
    return None


def classify(evidence: Evidence | None) -> str:
    return "proven_output" if evidence else "unknown"


def dedupe_evidence(rows: Iterable[Evidence]) -> list[Evidence]:
    result: list[Evidence] = []
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        if row.natural_key not in seen:
            seen.add(row.natural_key)
            result.append(row)
    return result


def select_artists(conn: Any, limit: int, after_mbid: str | None, explicit_mbids: list[str], pilot: bool) -> list[Artist]:
    if explicit_mbids:
        rows = conn.execute(
            "select id::text,musicbrainz_artist_mbid::text,display_name from artists "
            "where musicbrainz_artist_mbid=any(%s::uuid[]) order by musicbrainz_artist_mbid limit %s",
            (explicit_mbids, limit),
        ).fetchall()
    elif pilot:
        rows = conn.execute(
            "select id::text,musicbrainz_artist_mbid::text,display_name from artists "
            "order by case lower(display_name) "
            + " ".join(f"when %s then {i}" for i, _ in enumerate(PILOT_NAMES))
            + " else 100 end,musicbrainz_artist_mbid limit %s",
            (*[name.lower() for name in PILOT_NAMES], limit),
        ).fetchall()
    else:
        rows = conn.execute(
            "select id::text,musicbrainz_artist_mbid::text,display_name from artists "
            "where (%s::uuid is null or musicbrainz_artist_mbid>%s::uuid) "
            "order by musicbrainz_artist_mbid limit %s",
            (after_mbid, after_mbid, limit),
        ).fetchall()
    return [Artist(*row) for row in rows]


def load_local_evidence(conn: Any, artists: list[Artist], verified_at: datetime) -> list[Evidence]:
    by_mbid = {artist.mbid: artist for artist in artists}
    mbids = list(by_mbid)
    products = conn.execute(
        "select p.metadata_raw->'release_detail' from products p where exists "
        "(select 1 from jsonb_array_elements(case when jsonb_typeof(p.metadata_raw->'release_detail'->'artist-credit')='array' "
        "then p.metadata_raw->'release_detail'->'artist-credit' else '[]'::jsonb end) c "
        "where c->'artist'->>'id'=any(%s))",
        (mbids,),
    ).fetchall()
    evidence: list[Evidence] = []
    for (detail,) in products:
        for mbid, artist in by_mbid.items():
            hit = local_release_evidence(artist, detail, verified_at)
            if hit:
                evidence.append(hit)
    artist_ids = [artist.id for artist in artists]
    relations = conn.execute(
        "select source_artist_id::text,target_artist_id::text,evidence_kind,classification,recording_mbid::text "
        "from artist_relation_evidence where evidence_kind='artist_credit' and classification='allowed' "
        "and (source_artist_id=any(%s::uuid[]) or target_artist_id=any(%s::uuid[]))",
        (artist_ids, artist_ids),
    ).fetchall()
    by_id = {artist.id: artist for artist in artists}
    for source_id, target_id, kind, classification, recording_mbid in relations:
        row = {"source_artist_id": source_id, "target_artist_id": target_id, "evidence_kind": kind,
               "classification": classification, "recording_mbid": recording_mbid}
        for artist_id in (source_id, target_id):
            if artist_id in by_id:
                hit = local_recording_evidence(by_id[artist_id], row, verified_at)
                if hit:
                    evidence.append(hit)
    return dedupe_evidence(evidence)


def load_existing(conn: Any, artists: list[Artist]) -> tuple[dict[str, Evidence], set[tuple[str, str, str, str]], dict[str, str]]:
    ids = [artist.id for artist in artists]
    rows = conn.execute(
        "select e.artist_id::text,a.musicbrainz_artist_mbid::text,e.evidence_type,e.source_system,e.source_entity_kind,"
        "e.source_entity_id::text,e.provenance,e.verified_at from artist_output_evidence e "
        "join artists a on a.id=e.artist_id where e.artist_id=any(%s::uuid[]) order by e.verified_at desc",
        (ids,),
    ).fetchall()
    evidence = [Evidence(*row) for row in rows]
    first = {}
    for row in evidence:
        first.setdefault(row.artist_id, row)
    status_rows = conn.execute(
        "select artist_id::text,status from artist_output_status where artist_id=any(%s::uuid[])", (ids,),
    ).fetchall()
    return first, {row.natural_key for row in evidence}, dict(status_rows)


def persist(conn: Any, run_id: str, rows: list[dict[str, Any]]) -> None:
    conn.execute(
        "insert into follow_the_groove_collection_runs(id,collector,source_system,scope,status,counters,finished_at) "
        "values (%s,'output_evidence','musicbrainz_output_evidence',%s,'succeeded',%s::jsonb,now())",
        (run_id, f"bounded pilot batch ({len(rows)} artists)", json.dumps({"artists_processed": len(rows)})),
    )
    for row in rows:
        evidence = row.get("evidence")
        evidence_id = None
        if evidence:
            evidence_id = str(uuid.uuid5(uuid.NAMESPACE_URL, "ftg-output:" + repr(evidence.natural_key)))
            if row["evidence_write"]:
                conn.execute(
                    "insert into artist_output_evidence(id,artist_id,evidence_type,source_system,source_entity_kind,source_entity_id,"
                    "provenance,created_by_run_id,last_seen_run_id,verified_at) values(%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s) "
                    "on conflict(artist_id,evidence_type,source_entity_kind,source_entity_id) do update set "
                    "last_seen_run_id=excluded.last_seen_run_id,verified_at=greatest(artist_output_evidence.verified_at,excluded.verified_at) returning id::text",
                    (evidence_id, evidence.artist_id, evidence.evidence_type, evidence.source_system,
                     evidence.source_entity_kind, evidence.source_entity_id, json.dumps(evidence.provenance),
                     run_id, run_id, evidence.verified_at),
                )
            evidence_id = conn.execute(
                "select id::text from artist_output_evidence where artist_id=%s and evidence_type=%s "
                "and source_entity_kind=%s and source_entity_id=%s",
                (evidence.artist_id, evidence.evidence_type, evidence.source_entity_kind, evidence.source_entity_id),
            ).fetchone()[0]
        if row["status_write"]:
            conn.execute(
                "insert into artist_output_status(artist_id,status,basis_evidence_id,provenance,created_by_run_id,last_seen_run_id,verified_at) "
                "values(%s,%s,%s,%s::jsonb,%s,%s,%s) on conflict(artist_id) do update set "
                "status=excluded.status,basis_evidence_id=excluded.basis_evidence_id,provenance=excluded.provenance,"
                "last_seen_run_id=excluded.last_seen_run_id,verified_at=excluded.verified_at,updated_at=now()",
                (row["artist"].id, row["status"], evidence_id, json.dumps({"classifier": "positive_evidence_only"}),
                 run_id, run_id, row["verified_at"]),
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--write", action="store_true")
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--after-mbid")
    parser.add_argument("--artist-mbid", action="append", default=[])
    parser.add_argument("--pilot", action="store_true")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser


def run(args: argparse.Namespace, *, client: MBClient | None = None) -> dict[str, Any]:
    if not 1 <= args.batch_size <= MAX_BATCH_SIZE:
        raise ValueError(f"batch-size must be 1..{MAX_BATCH_SIZE}")
    if len(args.artist_mbid) > args.batch_size:
        raise ValueError("explicit artist count exceeds batch-size")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    started = time.monotonic()
    verified_at = utc_now()
    http = client or HttpJsonClient(os.getenv("MUSICBRAINZ_USER_AGENT", DEFAULT_USER_AGENT))
    with psycopg.connect(database_url, autocommit=False) as conn:
        conn.execute("begin isolation level repeatable read read only" if args.dry_run else "begin isolation level serializable")
        artists = select_artists(conn, args.batch_size, args.after_mbid, args.artist_mbid, args.pilot)
        existing_by_artist, existing_keys, existing_status = load_existing(conn, artists)
        local = load_local_evidence(conn, artists, verified_at)
        local_by_artist = {row.artist_id: row for row in local}
        rows = []
        for artist in artists:
            evidence = existing_by_artist.get(artist.id) or local_by_artist.get(artist.id)
            source = "existing" if artist.id in existing_status else "local" if evidence else "musicbrainz"
            if not evidence and (args.refresh or artist.id not in existing_status):
                evidence = lookup_musicbrainz(artist, http, verified_at)
            status = classify(evidence) if evidence or artist.id not in existing_status or args.refresh else existing_status[artist.id]
            rows.append({"artist": artist, "evidence": evidence, "status": status,
                         "source": source, "verified_at": verified_at,
                         "evidence_write": bool(evidence and (evidence.natural_key not in existing_keys or args.refresh)),
                         "status_write": bool(artist.id not in existing_status or existing_status[artist.id] != status or args.refresh)})
        conflicts = 0
        expected_new_evidence = sum(row["evidence_write"] for row in rows)
        expected_status_writes = sum(row["status_write"] for row in rows)
        if args.write:
            persist(conn, str(uuid.uuid4()), rows)
            conn.commit()
        else:
            conn.rollback()
    counts = {status: sum(row["status"] == status for row in rows)
              for status in ("proven_output", "unknown", "proven_bridge_only")}
    report = {
        "mode": "write" if args.write else "dry-run", "artists_processed": len(rows),
        "artists_already_proven": sum(row["source"] == "existing" for row in rows),
        "local_evidence_hits": sum(row["source"] == "local" for row in rows),
        "artists_requiring_mb_lookup": sum(row["source"] == "musicbrainz" for row in rows),
        "expected_writes": {"evidence": expected_new_evidence, "status": expected_status_writes,
                            "total": expected_new_evidence + expected_status_writes},
        "database_writes": 0 if args.dry_run else expected_new_evidence + expected_status_writes,
        "conflicts": conflicts, "classification_counts": counts,
        "api_calls": http.request_count, "retries": http.retry_count, "cache_hits": http.cache_hits,
        "calls_per_artist": round(http.request_count / len(rows), 3) if rows else 0,
        "runtime_seconds": round(time.monotonic() - started, 3),
        "next_after_mbid": artists[-1].mbid if artists else args.after_mbid,
        "artists": [{"name": row["artist"].name, "artist_mbid": row["artist"].mbid,
                     "status": row["status"], "source": row["source"],
                     "evidence_type": row["evidence"].evidence_type if row["evidence"] else None,
                     "source_entity_id": row["evidence"].source_entity_id if row["evidence"] else None}
                    for row in rows],
    }
    return report


def main() -> None:
    args = build_parser().parse_args()
    report = run(args)
    output = json.dumps(report, indent=2, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
