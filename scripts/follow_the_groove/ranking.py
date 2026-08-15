#!/usr/bin/env python3
"""Read-only, deterministic Follow the Groove V3 ranking."""

from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import asdict, dataclass, replace
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Literal

import psycopg

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


Mode = Literal["trail", "search"]
RECORDING_MECHANISMS = frozenset({"artist_credit", "instrument", "vocal"})


@dataclass(frozen=True)
class Candidate:
    source_artist_id: str
    target_artist_id: str
    display_name: str
    musicbrainz_artist_mbid: str
    entity_type: str
    factual: bool = False
    factual_mechanisms: tuple[str, ...] = ()
    allowed_evidence_count: int = 0
    unique_recording_count: int = 0
    similarity: bool = False
    similarity_position: int | None = None
    similarity_match_score: Decimal | None = None
    search_eligible: bool = False
    product_count: int = 0

    @property
    def multi_signal(self) -> bool:
        return self.factual and self.similarity

    @property
    def tier(self) -> int:
        if self.multi_signal:
            return 1
        if self.factual:
            return 2
        return 3

    @property
    def reason_codes(self) -> tuple[str, ...]:
        reasons: list[str] = []
        if self.multi_signal:
            reasons.append("factual_and_similarity")
        if "membership" in self.factual_mechanisms:
            reasons.append("membership")
        if RECORDING_MECHANISMS.intersection(self.factual_mechanisms):
            reasons.append("recording_collaboration")
        if self.similarity and not self.factual:
            reasons.append("similar_artist")
        return tuple(reasons)


@dataclass(frozen=True)
class RankResult:
    source_artist_id: str
    source_display_name: str
    source_mbid: str
    mode: Mode
    limit: int
    candidates: tuple[Candidate, ...]
    query_count: int
    query_duration_ms: float


def _technical_key(candidate: Candidate) -> tuple[str, str]:
    return candidate.display_name.casefold(), candidate.musicbrainz_artist_mbid


def _base_key(candidate: Candidate) -> tuple[Any, ...]:
    if candidate.tier in {1, 3}:
        position = candidate.similarity_position if candidate.similarity_position is not None else 2**31
        return candidate.tier, position, *_technical_key(candidate)
    return candidate.tier, *_technical_key(candidate)


def deduplicate_candidates(candidates: Iterable[Candidate]) -> list[Candidate]:
    """Merge duplicate source/target rows without counting duplicated evidence."""
    merged: dict[tuple[str, str], Candidate] = {}
    for candidate in candidates:
        key = (candidate.source_artist_id, candidate.target_artist_id)
        previous = merged.get(key)
        if previous is None:
            merged[key] = replace(candidate, factual_mechanisms=tuple(sorted(set(candidate.factual_mechanisms))))
            continue
        if (
            previous.display_name,
            previous.musicbrainz_artist_mbid,
            previous.entity_type,
        ) != (
            candidate.display_name,
            candidate.musicbrainz_artist_mbid,
            candidate.entity_type,
        ):
            raise ValueError(f"Conflicting identity for candidate {candidate.target_artist_id}")
        positions = [value for value in (previous.similarity_position, candidate.similarity_position) if value is not None]
        score = previous.similarity_match_score if previous.similarity_match_score is not None else candidate.similarity_match_score
        merged[key] = replace(
            previous,
            factual=previous.factual or candidate.factual,
            factual_mechanisms=tuple(sorted(set(previous.factual_mechanisms) | set(candidate.factual_mechanisms))),
            allowed_evidence_count=max(
                previous.allowed_evidence_count if previous.factual else 0,
                candidate.allowed_evidence_count if candidate.factual else 0,
            ),
            unique_recording_count=max(
                previous.unique_recording_count if previous.factual else 0,
                candidate.unique_recording_count if candidate.factual else 0,
            ),
            similarity=previous.similarity or candidate.similarity,
            similarity_position=min(positions) if positions else None,
            similarity_match_score=score,
            search_eligible=previous.search_eligible or candidate.search_eligible,
            product_count=max(previous.product_count, candidate.product_count),
        )
    return list(merged.values())


def rank_candidates(candidates: Iterable[Candidate], *, mode: Mode = "trail", limit: int = 3) -> tuple[Candidate, ...]:
    """Apply V3 tiers and the bounded position-1 discovery selection."""
    if mode not in {"trail", "search"}:
        raise ValueError(f"Unknown ranking mode: {mode}")
    if limit < 0:
        raise ValueError("limit must be non-negative")
    pool = deduplicate_candidates(candidates)
    if mode == "search":
        pool = [candidate for candidate in pool if candidate.search_eligible]
    ranked = sorted(pool, key=_base_key)
    if limit >= 3 and len(ranked) >= 3 and all(candidate.factual for candidate in ranked[:3]):
        discovery = next(
            (
                candidate
                for candidate in ranked
                if not candidate.factual and candidate.similarity and candidate.similarity_position == 1
            ),
            None,
        )
        if discovery is not None:
            ranked = ranked[:2] + [discovery] + [candidate for candidate in ranked[2:] if candidate != discovery]
    return tuple(ranked[:limit])


CANDIDATE_QUERY = """
with source as (
  select id, display_name, musicbrainz_artist_mbid
  from artists
  where musicbrainz_artist_mbid = %(source_mbid)s::uuid
), factual as (
  select
    case when e.artist_low_id = s.id then e.artist_high_id else e.artist_low_id end as target_id,
    array_agg(distinct ev.evidence_kind order by ev.evidence_kind)
      filter (where ev.classification = 'allowed') as mechanisms,
    count(distinct ev.id) filter (where ev.classification = 'allowed') as allowed_evidence_count,
    count(distinct ev.recording_mbid) filter (
      where ev.classification = 'allowed' and ev.recording_mbid is not null
    ) as unique_recording_count
  from source s
  join artist_edges e on s.id in (e.artist_low_id, e.artist_high_id)
  left join artist_relation_evidence ev
    on ev.edge_id = e.id and ev.classification = 'allowed'
  group by 1
), similarity as (
  select target_artist_id as target_id, position, match_score
  from artist_similarity si
  join source s on s.id = si.source_artist_id
  where si.resolution_status = 'resolved'
), candidate_ids as (
  select target_id from factual
  union
  select target_id from similarity
), availability as (
  select pa.artist_id as target_id, count(*) as product_count
  from product_artists pa
  join candidate_ids ci on ci.target_id = pa.artist_id
  group by pa.artist_id
)
select
  s.id::text,
  s.display_name,
  s.musicbrainz_artist_mbid::text,
  a.id::text,
  a.display_name,
  a.musicbrainz_artist_mbid::text,
  a.entity_type,
  (f.target_id is not null) as factual,
  coalesce(f.mechanisms, '{}'::text[]),
  coalesce(f.allowed_evidence_count, 0)::integer,
  coalesce(f.unique_recording_count, 0)::integer,
  (si.target_id is not null) as similarity,
  si.position,
  si.match_score,
  coalesce(av.product_count, 0)::integer
from source s
left join candidate_ids ci on true
left join artists a on a.id = ci.target_id
left join factual f on f.target_id = ci.target_id
left join similarity si on si.target_id = ci.target_id
left join availability av on av.target_id = ci.target_id
order by a.id
"""


def load_candidates(conn: psycopg.Connection[Any], source_mbid: str) -> tuple[dict[str, str], list[Candidate]]:
    rows = conn.execute(CANDIDATE_QUERY, {"source_mbid": source_mbid}).fetchall()
    if not rows:
        source = conn.execute(
            "select id::text,display_name,musicbrainz_artist_mbid::text from artists where musicbrainz_artist_mbid=%s::uuid",
            (source_mbid,),
        ).fetchone()
        if source is None:
            raise KeyError(f"Unknown artist MBID: {source_mbid}")
        return {"id": source[0], "display_name": source[1], "mbid": source[2]}, []
    source = {"id": rows[0][0], "display_name": rows[0][1], "mbid": rows[0][2]}
    candidates = [
        Candidate(
            source_artist_id=row[0],
            target_artist_id=row[3],
            display_name=row[4],
            musicbrainz_artist_mbid=row[5],
            entity_type=row[6],
            factual=row[7],
            factual_mechanisms=tuple(row[8]),
            allowed_evidence_count=row[9],
            unique_recording_count=row[10],
            similarity=row[11],
            similarity_position=row[12],
            similarity_match_score=row[13],
            search_eligible=row[14] > 0,
            product_count=row[14],
        )
        for row in rows
        if row[3] is not None
    ]
    return source, candidates


def rank_from_database(
    conn: psycopg.Connection[Any], source_mbid: str, *, mode: Mode = "trail", limit: int = 3
) -> RankResult:
    started = time.perf_counter()
    source, candidates = load_candidates(conn, source_mbid)
    duration_ms = (time.perf_counter() - started) * 1000
    return RankResult(
        source_artist_id=source["id"],
        source_display_name=source["display_name"],
        source_mbid=source["mbid"],
        mode=mode,
        limit=limit,
        candidates=rank_candidates(candidates, mode=mode, limit=limit),
        query_count=1,
        query_duration_ms=duration_ms,
    )


def render_result(result: RankResult) -> dict[str, Any]:
    return {
        "source": {
            "artist_id": result.source_artist_id,
            "display_name": result.source_display_name,
            "musicbrainz_artist_mbid": result.source_mbid,
        },
        "mode": result.mode,
        "limit": result.limit,
        "query_count": result.query_count,
        "query_duration_ms": round(result.query_duration_ms, 3),
        "candidates": [
            {
                **asdict(candidate),
                "multi_signal": candidate.multi_signal,
                "tier": candidate.tier,
                "reason_codes": list(candidate.reason_codes),
            }
            for candidate in result.candidates
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Read-only Follow the Groove V3 ranking")
    parser.add_argument("--artist-mbid", required=True)
    parser.add_argument("--mode", choices=("trail", "search"), default="trail")
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--output", type=Path)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if load_dotenv:
        load_dotenv(".env.local", override=False)
    with psycopg.connect(os.environ["DATABASE_URL"], autocommit=False) as conn:
        conn.execute("begin read only")
        result = rank_from_database(conn, args.artist_mbid, mode=args.mode, limit=args.limit)
        conn.rollback()
    rendered = json.dumps(render_result(result), ensure_ascii=False, indent=2, default=str)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
