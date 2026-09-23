#!/usr/bin/env python3
"""Resolve bounded Last.fm rows against already known FTG MusicBrainz artists.

The dry-run only reads PostgreSQL. Writes require explicit similarity IDs copied
from a dry-run and update no rows outside those IDs.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import psycopg

try:
    from scripts.follow_the_groove import collector, persistence
except ModuleNotFoundError:  # direct CLI invocation
    import collector  # type: ignore[no-redef]
    import persistence  # type: ignore[no-redef]


MAX_BATCH_SIZE = 25
COLLECTOR = "ftg_existing_similarity_resolution"
LOCK_KEY = "follow-the-groove:existing-similarity-resolution:v1"


class ResolutionBlocked(RuntimeError):
    pass


@dataclass(frozen=True)
class Candidate:
    id: str
    source_artist_id: str
    source_mbid: str
    source_system: str
    returned_target_name: str
    returned_target_name_normalized: str
    returned_mbid: str
    match_score: str
    position: int
    resolution_status: str
    target_artist_id: str | None
    last_seen_run_id: str | None
    updated_at: Any
    checked_at: Any
    matched_artist_id: str
    matched_artist_mbid: str
    matched_artist_name: str
    matched_artist_type: str
    matched_artist_type_id: str


def validate_args(args: argparse.Namespace) -> list[str]:
    if not 1 <= args.limit <= MAX_BATCH_SIZE:
        raise ValueError(f"limit must be 1..{MAX_BATCH_SIZE}")
    explicit = [str(uuid.UUID(value)) for value in args.similarity_id]
    if len(explicit) != len(set(explicit)) or len(explicit) > args.limit:
        raise ValueError("similarity IDs must be unique and fit within limit")
    if args.write and not explicit:
        raise ValueError("write requires explicit --similarity-id values from a dry-run")
    if args.after_id and explicit:
        raise ValueError("--after-id cannot be combined with explicit similarity IDs")
    if args.after_id:
        uuid.UUID(args.after_id)
    return explicit


def load_candidates(conn: Any, limit: int, explicit: list[str], after_id: str | None, *, lock: bool) -> list[Candidate]:
    scope = "si.id=any(%s::uuid[])" if explicit else "(%s::uuid is null or si.id>%s::uuid)"
    statuses = "si.resolution_status in ('unresolved','resolved')" if explicit else "si.resolution_status='unresolved'"
    params: tuple[Any, ...] = (explicit, limit) if explicit else (after_id, after_id, limit)
    rows = conn.execute(
        "select si.id::text,si.source_artist_id::text,src.musicbrainz_artist_mbid::text,"
        "si.source_system,si.returned_target_name,si.returned_target_name_normalized,"
        "si.returned_mbid::text,si.match_score::text,si.position,si.resolution_status,"
        "si.target_artist_id::text,si.last_seen_run_id::text,si.updated_at,si.checked_at,"
        "target.id::text,target.musicbrainz_artist_mbid::text,target.display_name,"
        "target.entity_type,target.musicbrainz_type_id::text "
        "from artist_similarity si "
        "join artists src on src.id=si.source_artist_id "
        "join artists target on target.musicbrainz_artist_mbid=si.returned_mbid "
        f"where {scope} and si.source_system='lastfm' and {statuses} "
        "and (si.target_artist_id is null or si.target_artist_id=target.id) "
        "order by si.id limit %s"
        + (" for update of si" if lock else ""),
        params,
    ).fetchall()
    if explicit and len(rows) != len(explicit):
        raise ResolutionBlocked("explicit target set changed or contains ineligible rows")
    return [Candidate(*row) for row in rows]


def classify(candidate: Candidate, existing_pair: bool) -> str:
    if candidate.source_artist_id == candidate.matched_artist_id:
        return "self_similarity"
    if candidate.returned_mbid != candidate.matched_artist_mbid:
        return "mbid_mismatch"
    if collector.normalize_name(candidate.returned_target_name) != collector.normalize_name(candidate.matched_artist_name):
        return "name_mismatch"
    if candidate.returned_target_name_normalized != collector.normalize_name(candidate.returned_target_name):
        return "stored_name_mismatch"
    expected_type_id = (
        collector.PERSON_TYPE_ID if candidate.matched_artist_type == "person"
        else collector.GROUP_TYPE_ID if candidate.matched_artist_type == "group" else None
    )
    if candidate.matched_artist_type_id != expected_type_id:
        return "artist_type_mismatch"
    if candidate.resolution_status == "resolved" and candidate.target_artist_id == candidate.matched_artist_id:
        return "already_resolved"
    if candidate.resolution_status != "unresolved" or candidate.target_artist_id is not None:
        return "state_conflict"
    if existing_pair:
        return "resolved_pair_exists"
    return "resolve"


def plan(conn: Any, candidates: list[Candidate]) -> list[dict[str, Any]]:
    if not candidates:
        return []
    existing = conn.execute(
        "select source_artist_id::text,target_artist_id::text from artist_similarity "
        "where resolution_status='resolved' and source_artist_id=any(%s::uuid[]) "
        "and target_artist_id=any(%s::uuid[])",
        (list({row.source_artist_id for row in candidates}), list({row.matched_artist_id for row in candidates})),
    ).fetchall()
    existing_pairs = set(existing)
    seen_pairs: set[tuple[str, str]] = set()
    result = []
    for candidate in candidates:
        pair = (candidate.source_artist_id, candidate.matched_artist_id)
        reason = classify(candidate, pair in existing_pairs or pair in seen_pairs)
        if reason == "resolve":
            old = {
                "id": candidate.id,
                "source_system": candidate.source_system,
                "source_mbid": candidate.source_mbid,
                "returned_target_name_normalized": candidate.returned_target_name_normalized,
                "returned_mbid": candidate.returned_mbid,
                "resolution_status": candidate.resolution_status,
                "target_artist_id": candidate.target_artist_id,
                "match_score": candidate.match_score,
                "position": candidate.position,
                "last_seen_run_id": candidate.last_seen_run_id,
                "updated_at": candidate.updated_at,
                "checked_at": candidate.checked_at,
            }
            incoming = {**old, "target_mbid": candidate.matched_artist_mbid}
            guarded = persistence.plan_similarity_resolution(incoming, old, identity_proven=True)
            if guarded["action"] != "RESOLVE_EXISTING_UNRESOLVED":
                reason = "persistence_conflict"
            else:
                seen_pairs.add(pair)
        result.append({
            "similarity_id": candidate.id,
            "source_mbid": candidate.source_mbid,
            "target_mbid": candidate.matched_artist_mbid,
            "target_artist_id": candidate.matched_artist_id,
            "target_name": candidate.matched_artist_name,
            "action": reason,
            "preimage": persistence.similarity_resolution_preimage(asdict(candidate)),
        })
    return result


def write_plan(conn: Any, candidates: list[Candidate], planned: list[dict[str, Any]]) -> str:
    if not candidates or len(candidates) != len(planned) or any(row["action"] != "resolve" for row in planned):
        raise ResolutionBlocked("write scope contains an unproven or conflicting similarity")
    run_id = str(uuid.uuid4())
    counters = {
        "similarity_ids": [row.id for row in candidates],
        "preimages": planned,
        "resolved_count": len(candidates),
    }
    conn.execute(
        "insert into follow_the_groove_collection_runs "
        "(id,collector,source_system,scope,status,counters,finished_at) "
        "values (%s,%s,'lastfm+musicbrainz','bounded_existing_identity_resolution',"
        "'succeeded',%s::jsonb,now())",
        (run_id, COLLECTOR, json.dumps(counters, default=str)),
    )
    for candidate in candidates:
        changed = conn.execute(
            "update artist_similarity set target_artist_id=%s,resolution_status='resolved',"
            "last_seen_run_id=%s,updated_at=now(),checked_at=now() "
            "where id=%s and source_artist_id=%s and source_system='lastfm' "
            "and resolution_status='unresolved' and target_artist_id is null "
            "and returned_mbid=%s and last_seen_run_id is not distinct from %s "
            "and updated_at=%s and checked_at=%s",
            (candidate.matched_artist_id, run_id, candidate.id, candidate.source_artist_id,
             candidate.returned_mbid, candidate.last_seen_run_id, candidate.updated_at, candidate.checked_at),
        )
        if changed.rowcount != 1:
            raise ResolutionBlocked(f"similarity preimage changed: {candidate.id}")
    verified = conn.execute(
        "select count(*) from artist_similarity where id=any(%s::uuid[]) "
        "and resolution_status='resolved' and last_seen_run_id=%s",
        ([row.id for row in candidates], run_id),
    ).fetchone()[0]
    if verified != len(candidates):
        raise ResolutionBlocked("pre-commit postcondition failed")
    return run_id


def run(args: argparse.Namespace) -> dict[str, Any]:
    explicit = validate_args(args)
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    with psycopg.connect(database_url, autocommit=False, prepare_threshold=None) as conn:
        conn.execute("begin isolation level serializable" if args.write else "begin read only")
        if args.write:
            locked = conn.execute("select pg_try_advisory_xact_lock(hashtext(%s))", (LOCK_KEY,)).fetchone()[0]
            if not locked:
                raise ResolutionBlocked("another similarity resolution is running")
        candidates = load_candidates(conn, args.limit, explicit, args.after_id, lock=args.write)
        planned = plan(conn, candidates)
        already_resolved = args.write and candidates and all(row["action"] == "already_resolved" for row in planned)
        run_id = write_plan(conn, candidates, planned) if args.write and not already_resolved else None
        if run_id:
            conn.commit()
        else:
            conn.rollback()
    if run_id:
        with psycopg.connect(database_url, autocommit=False, prepare_threshold=None) as check:
            check.execute("begin read only")
            verified = check.execute(
                "select count(*) from artist_similarity where id=any(%s::uuid[]) "
                "and resolution_status='resolved' and last_seen_run_id=%s",
                ([row.id for row in candidates], run_id),
            ).fetchone()[0]
            check.rollback()
        if verified != len(candidates):
            raise ResolutionBlocked("post-commit audit failed")
    return {
        "mode": "write" if run_id else "already-resolved" if args.write else "dry-run",
        "run_id": run_id,
        "selected": len(candidates),
        "resolvable": sum(row["action"] == "resolve" for row in planned),
        "next_after_id": candidates[-1].id if candidates else args.after_id,
        "rows": [{key: value for key, value in row.items() if key != "preimage"} for row in planned],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--write", action="store_true")
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--after-id")
    parser.add_argument("--similarity-id", action="append", default=[])
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = run(args)
    rendered = json.dumps(result, indent=2, ensure_ascii=False, default=str)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
