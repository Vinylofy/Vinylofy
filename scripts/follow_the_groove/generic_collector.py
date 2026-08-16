#!/usr/bin/env python3
"""Bounded, depth-one Follow-the-Groove collector with explicit write gates."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterable

import psycopg

try:
    from scripts.follow_the_groove import collector, local_resolution, persistence
except ModuleNotFoundError:  # direct ``python scripts/...`` CLI invocation
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from follow_the_groove import collector, local_resolution, persistence

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


GENERIC_COLLECTOR = "GENERIC_BOUNDED_V1"
MAX_LASTFM_LIMIT = 25
FAILURES = frozenset({
    "SOURCE_IDENTITY_CONFLICT", "MUSICBRAINZ_TRANSIENT", "MUSICBRAINZ_PERMANENT",
    "LASTFM_TRANSIENT", "LASTFM_PERMANENT", "LOCAL_RESOLUTION_CONFLICT",
    "PERSISTENCE_CONFLICT", "GLOBAL_CONFIGURATION_ERROR",
})


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class BoundedConfig:
    max_sources: int = 10
    max_direct_targets: int = 25
    lastfm_limit: int = 5
    graph_depth: int = 1
    recording_release_seeds: tuple[str, ...] = ()
    dry_run: bool = True

    def __post_init__(self) -> None:
        if self.max_sources < 1:
            raise ValueError("max_sources must be positive")
        if self.max_direct_targets < 1:
            raise ValueError("max_direct_targets must be positive")
        if not 1 <= self.lastfm_limit <= MAX_LASTFM_LIMIT:
            raise ValueError(f"lastfm_limit must be between 1 and {MAX_LASTFM_LIMIT}")
        if self.graph_depth != 1:
            raise ValueError("generic collector v1 requires graph_depth=1")
        if not self.dry_run:
            raise persistence.PersistenceDisabled("Generic collector write mode is disabled")
        for value in self.recording_release_seeds:
            uuid.UUID(value)


@dataclass(frozen=True)
class Source:
    artist_id: str
    mbid: str
    display_name: str
    entity_type: str
    musicbrainz_type_id: str
    product_count: int
    selection_reason: str
    prior_successful_source_run: bool = False


@dataclass
class SourceResult:
    source: Source
    status: str = "succeeded"
    identity: dict[str, Any] = field(default_factory=dict)
    collected: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    plans: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    counters: dict[str, Any] = field(default_factory=dict)
    errors: list[dict[str, str]] = field(default_factory=list)
    rollback: dict[str, Any] = field(default_factory=dict)
    run_plan: dict[str, Any] = field(default_factory=dict)


def select_sources(
    conn: psycopg.Connection[Any], limit: int, source_mbids: tuple[str, ...] = (),
    *, include_successful: bool = False,
) -> list[Source]:
    if limit < 1:
        raise ValueError("limit must be positive")
    params: dict[str, Any] = {"limit": limit, "collector": GENERIC_COLLECTOR}
    filter_sql = ""
    explicit_order_sql = ""
    if source_mbids:
        for value in source_mbids:
            uuid.UUID(value)
        filter_sql = "and a.musicbrainz_artist_mbid = any(%(source_mbids)s::uuid[])"
        explicit_order_sql = "array_position(%(source_mbids)s::uuid[],a.musicbrainz_artist_mbid),"
        params["source_mbids"] = list(source_mbids)
    if include_successful and not source_mbids:
        raise ValueError("refresh requires explicit source MBIDs")
    params["include_successful"] = include_successful
    rows = conn.execute(
        f"""
        with product_counts as (
          select artist_id, count(*)::integer product_count from product_artists group by artist_id
        ), prior as (
          select distinct counters->>'source_artist_mbid' mbid
          from follow_the_groove_collection_runs
          where collector=%(collector)s and status='succeeded'
        )
        select a.id::text,a.musicbrainz_artist_mbid::text,a.display_name,a.entity_type,
               a.musicbrainz_type_id::text,pc.product_count,
               case when p.mbid is not null then 'explicit_refresh'
                    when cr.collector='ftg_local_resolution' then 'resolved_frontier'
                    else 'ftg_with_products_uncollected' end selection_reason,
               (p.mbid is not null) prior_success
        from artists a
        join product_counts pc on pc.artist_id=a.id
        left join follow_the_groove_collection_runs cr on cr.id=a.created_by_run_id
        left join prior p on p.mbid=a.musicbrainz_artist_mbid::text
        where (p.mbid is null or %(include_successful)s) {filter_sql}
        order by {explicit_order_sql}(cr.collector='ftg_local_resolution') desc,pc.product_count desc,
                 lower(a.display_name),a.musicbrainz_artist_mbid
        limit %(limit)s
        """,
        params,
    ).fetchall()
    return [Source(*row) for row in rows]


def validate_source(source: Source, payload: dict[str, Any]) -> dict[str, Any]:
    expected_type = source.entity_type.capitalize()
    reasons = []
    if payload.get("id") != source.mbid:
        reasons.append("mbid_mismatch")
    if payload.get("name") != source.display_name:
        reasons.append("canonical_name_mismatch")
    if payload.get("type") != expected_type:
        reasons.append("entity_type_mismatch")
    if payload.get("type-id") != source.musicbrainz_type_id:
        reasons.append("type_id_mismatch")
    return {
        "musicbrainz_artist_mbid": source.mbid,
        "display_name": payload.get("name"),
        "entity_type": str(payload.get("type") or "").lower(),
        "musicbrainz_type_id": payload.get("type-id"),
        "wikidata_qid": collector.wikidata_qid(payload),
        "status": "resolved" if not reasons else "conflict",
        "reasons": reasons,
    }


def _target_identity(target: dict[str, Any]) -> dict[str, Any] | None:
    mbid = target.get("id")
    name = str(target.get("name") or "").strip()
    entity_type = target.get("type")
    type_id = target.get("type-id")
    expected = collector.PERSON_TYPE_ID if entity_type == "Person" else collector.GROUP_TYPE_ID if entity_type == "Group" else None
    if not mbid or not name or expected is None or type_id != expected:
        return None
    try:
        uuid.UUID(mbid)
    except ValueError:
        return None
    return {"musicbrainz_artist_mbid": mbid, "display_name": name, "entity_type": entity_type.lower(),
            "musicbrainz_type_id": type_id, "wikidata_qid": None, "node_role": "DISCOVERED_TARGET"}


def extract_direct_memberships(source: Source, payload: dict[str, Any], limit: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    received = []
    for relation in payload.get("relations") or []:
        if relation.get("target-type") == "artist" and collector.normalize_name(relation.get("type") or "") == "member of band":
            target_identity = _target_identity(relation.get("artist") or {})
            received.append((str((relation.get("artist") or {}).get("id") or ""), relation, target_identity))
    selected = sorted(received, key=lambda item: item[0])[:limit]
    targets: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    for _, relation, target in selected:
        if target is None:
            continue
        targets.append(target)
        if {source.entity_type, target["entity_type"]} != {"person", "group"}:
            continue
        person = source.mbid if source.entity_type == "person" else target["musicbrainz_artist_mbid"]
        group = source.mbid if source.entity_type == "group" else target["musicbrainz_artist_mbid"]
        evidence.append({
            "source_system": "musicbrainz", "source_entity_kind": "artist_relation",
            "source_entity_id": f"{source.mbid}:{relation.get('type-id')}:{target['musicbrainz_artist_mbid']}",
            "source_mbid": person, "target_mbid": group, "relation_type_id": relation.get("type-id"),
            "source_relation_name": relation.get("type"), "direction": "source_to_target",
            "begin_date": relation.get("begin"), "end_date": relation.get("end"), "ended": relation.get("ended"),
            "attribute_ids": sorted((relation.get("attribute-ids") or {}).values()),
            "classification": "allowed", "evidence_kind": "membership",
            "provenance": {"fetched_for_mbid": source.mbid, "source_direction": relation.get("direction")},
        })
    targets = collector.dedupe(targets, ("musicbrainz_artist_mbid",))
    evidence = collector.dedupe(evidence, collector.UPSERT_KEYS["membership"])
    return targets, evidence, {"received": len(received), "selected": len(selected), "truncated": max(0, len(received) - len(selected))}


def load_local_bridge(conn: psycopg.Connection[Any], mbids: Iterable[str]) -> dict[str, dict[str, Any]]:
    values = sorted({value for value in mbids if value})
    if not values:
        return {}
    rows = conn.execute(
        "select p.id::text,p.metadata_raw->'release_detail' from products p "
        "where exists(select 1 from jsonb_array_elements(case when jsonb_typeof(p.metadata_raw->'release_detail'->'artist-credit')='array' "
        "then p.metadata_raw->'release_detail'->'artist-credit' else '[]'::jsonb end) c where c->'artist'->>'id'=any(%s))",
        (values,),
    ).fetchall()
    bridge: dict[str, dict[str, Any]] = {}
    for product_id, detail in rows:
        for credit in local_resolution.direct_artist_credits(detail):
            if credit["mbid"] not in values:
                continue
            item = bridge.setdefault(credit["mbid"], {**credit, "product_credits": []})
            if any(item.get(field) != credit.get(field) for field in ("canonical_name", "entity_type", "musicbrainz_type_id")):
                item["conflict"] = True
            item["product_credits"].append({"product_id": product_id, "credited_name": credit["credited_name"],
                                             "credit_position": credit["credit_position"]})
    return bridge


def resolve_lastfm_rows(conn: psycopg.Connection[Any], source: Source, returned: list[dict[str, Any]],
                        existing_artists: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    existing = {row["musicbrainz_artist_mbid"]: {"artist_id": row["id"], **row} for row in existing_artists}
    returned_mbids = [str(row.get("mbid") or "").strip() for row in returned]
    bridge = load_local_bridge(conn, returned_mbids)
    similarities: list[dict[str, Any]] = []
    targets: list[dict[str, Any]] = []
    product_links: list[dict[str, Any]] = []
    for position, row in enumerate(returned, 1):
        name = str(row.get("name") or "").strip()
        mbid = str(row.get("mbid") or "").strip() or None
        target_mbid = None
        status = "unresolved"
        resolution_route = None
        if mbid:
            try:
                uuid.UUID(mbid)
            except ValueError:
                status = "conflict"
            else:
                known = existing.get(mbid)
                local = bridge.get(mbid)
                if known and collector.normalize_name(name) == collector.normalize_name(known["display_name"]):
                    status, target_mbid, resolution_route = "resolved", mbid, "existing_ftg_mbid"
                elif local and not local.get("conflict") and not local_resolution.validate_target({
                    "mbid": mbid, "canonical_name": local.get("canonical_name"), "entity_type": local.get("entity_type"),
                    "musicbrainz_type_id": local.get("musicbrainz_type_id"), "local_product_count": len(local["product_credits"]),
                }) and collector.normalize_name(name) in {collector.normalize_name(local["canonical_name"]), collector.normalize_name(local["credited_name"])}:
                    status, target_mbid, resolution_route = "resolved", mbid, "local_category_a"
                    targets.append({"musicbrainz_artist_mbid": mbid, "display_name": local["canonical_name"],
                                    "entity_type": local["entity_type"].lower(), "musicbrainz_type_id": local["musicbrainz_type_id"],
                                    "wikidata_qid": None, "node_role": "DISCOVERED_TARGET"})
                    product_links.extend({"product_id": item["product_id"], "artist_mbid": mbid,
                                          "credited_name": item["credited_name"], "credit_position": item["credit_position"],
                                          "source_system": "musicbrainz_artist_credit"} for item in local["product_credits"])
                elif known or local:
                    status = "conflict"
        similarities.append({"source_system": "lastfm", "source_mbid": source.mbid,
                             "requested_source_name": source.display_name, "returned_target_name": name,
                             "returned_target_name_normalized": collector.normalize_name(name), "returned_mbid": mbid,
                             "match_score": str(row.get("match") or "0"), "position": position,
                             "resolution_status": status, "target_mbid": target_mbid,
                             "resolution_route": resolution_route})
    return similarities, collector.dedupe(targets, ("musicbrainz_artist_mbid",)), collector.dedupe(product_links, collector.UPSERT_KEYS["product_artists"])


def plan_product_matches(conn: psycopg.Connection[Any], artists: list[dict[str, Any]], aliases: list[dict[str, Any]],
                         product_rows: list[tuple[str, str]]) -> list[dict[str, Any]]:
    mbids = [row["musicbrainz_artist_mbid"] for row in artists]
    bridge = load_local_bridge(conn, mbids)
    links = [
        {"product_id": credit["product_id"], "artist_mbid": mbid, "credited_name": credit["credited_name"],
         "credit_position": credit["credit_position"], "source_system": "musicbrainz_artist_credit"}
        for mbid, local in bridge.items() for credit in local["product_credits"] if not local.get("conflict")
    ]
    safe_names: dict[str, set[str]] = {
        row["musicbrainz_artist_mbid"]: {collector.normalize_name(row["display_name"])} for row in artists
    }
    for alias in aliases:
        if collector.is_safe_product_alias(alias):
            safe_names.setdefault(alias["artist_mbid"], set()).add(alias["alias_normalized"])
    name_to_mbids: dict[str, list[str]] = {}
    for mbid, names in safe_names.items():
        for name in names:
            name_to_mbids.setdefault(name, []).append(mbid)
    direct_keys = {(row["product_id"], row["artist_mbid"]) for row in links}
    for product_id, name in product_rows:
        candidates = name_to_mbids.get(collector.normalize_name(str(name)), [])
        if len(candidates) == 1 and (product_id, candidates[0]) not in direct_keys:
            links.append({"product_id": product_id, "artist_mbid": candidates[0], "credited_name": str(name),
                          "credit_position": None, "source_system": "vinylofy_exact"})
    return collector.dedupe(links, collector.UPSERT_KEYS["product_artists"])


def read_existing(conn: psycopg.Connection[Any]) -> dict[str, list[dict[str, Any]]]:
    artists = [dict(zip(("id","musicbrainz_artist_mbid","display_name","entity_type","musicbrainz_type_id","wikidata_qid","last_seen_run_id","last_verified_at"), row))
               for row in conn.execute("select id::text,musicbrainz_artist_mbid::text,display_name,entity_type,musicbrainz_type_id::text,wikidata_qid,last_seen_run_id::text,last_verified_at::text from artists")]
    id_to_mbid = {row["id"]: row["musicbrainz_artist_mbid"] for row in artists}
    aliases = [dict(zip(("id","artist_id","source_system","alias_name","alias_type","locale","begin_date","end_date","alias_normalized","is_primary","provenance","last_seen_run_id","last_verified_at"), row))
               for row in conn.execute("select id::text,artist_id::text,source_system,alias_name,alias_type,locale,begin_date,end_date,alias_normalized,is_primary,provenance,last_seen_run_id::text,last_verified_at::text from artist_aliases")]
    for row in aliases: row["artist_mbid"] = id_to_mbid[row.pop("artist_id")]
    edges = [dict(zip(("id","low","high","last_seen_run_id","updated_at"), row)) for row in conn.execute("select id::text,artist_low_id::text,artist_high_id::text,last_seen_run_id::text,updated_at::text from artist_edges")]
    for row in edges:
        row["artist_low_mbid"], row["artist_high_mbid"] = snapshot_edge_mbids(
            id_to_mbid[row.pop("low")], id_to_mbid[row.pop("high")]
        )
    evidence = []
    fields = ("id","source_artist_id","target_artist_id","source_system","source_entity_kind","source_entity_id","evidence_kind","classification","direction","source_relation_name","relation_type_id","begin_date","end_date","ended","recording_mbid","release_mbid","release_group_mbid","work_mbid","attribute_ids","provenance","last_seen_run_id","last_seen_at","last_verified_at")
    for raw in conn.execute("select id::text,source_artist_id::text,target_artist_id::text,source_system,source_entity_kind,source_entity_id,evidence_kind,classification,direction,source_relation_name,relation_type_id::text,begin_date,end_date,ended,recording_mbid::text,release_mbid::text,release_group_mbid::text,work_mbid::text,attribute_ids::text[],provenance,last_seen_run_id::text,last_seen_at::text,last_verified_at::text from artist_relation_evidence"):
        row = dict(zip(fields, raw)); row["source_mbid"] = id_to_mbid[row.pop("source_artist_id")]; row["target_mbid"] = id_to_mbid[row.pop("target_artist_id")]; evidence.append(row)
    sims = []
    sf=("id","source_artist_id","target_artist_id","source_system","requested_source_name","returned_target_name","returned_target_name_normalized","returned_mbid","match_score","position","resolution_status","last_seen_run_id","updated_at","checked_at")
    for raw in conn.execute("select id::text,source_artist_id::text,target_artist_id::text,source_system,requested_source_name,returned_target_name,returned_target_name_normalized,returned_mbid::text,match_score::text,position,resolution_status,last_seen_run_id::text,updated_at::text,checked_at::text from artist_similarity"):
        row=dict(zip(sf,raw)); row["source_mbid"]=id_to_mbid[row.pop("source_artist_id")]; target=row.pop("target_artist_id"); row["target_mbid"]=id_to_mbid.get(target); sims.append(row)
    links=[]
    for raw in conn.execute("select pa.id::text,pa.product_id::text,pa.artist_id::text,pa.credited_name,pa.credit_position,pa.source_system,pa.last_seen_run_id::text,pa.last_verified_at::text from product_artists pa"):
        row=dict(zip(("id","product_id","artist_id","credited_name","credit_position","source_system","last_seen_run_id","last_verified_at"),raw)); row["artist_mbid"]=id_to_mbid[row.pop("artist_id")]; links.append(row)
    return {"artists":artists,"aliases":aliases,"edges":edges,"evidence":evidence,"similarities":sims,"product_artists":links}


def snapshot_edge_mbids(first_mbid: str, second_mbid: str) -> tuple[str, str]:
    """Return the logical edge key, independent of physical database-ID order."""
    if first_mbid == second_mbid:
        raise ValueError("self edge is forbidden")
    return tuple(sorted((first_mbid, second_mbid)))


def build_plans(collected: dict[str, list[dict[str, Any]]], existing: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    # Wikidata is optional enrichment, not the MBID identity authority.  A
    # generic run never fills or clears it in-place; it preserves the stored
    # value and exposes a newly observed QID only as audit context.  Two
    # different non-null QIDs remain a real identity conflict.
    existing_artists_by_mbid={row["musicbrainz_artist_mbid"]:row for row in existing["artists"]}
    artist_rows=[]
    for raw in collected["artists"]:
        row=dict(raw); old=existing_artists_by_mbid.get(row["musicbrainz_artist_mbid"])
        if old and (old.get("wikidata_qid") is None or row.get("wikidata_qid") is None):
            if row.get("wikidata_qid") != old.get("wikidata_qid"):
                row["observed_wikidata_qid"] = row.get("wikidata_qid")
            row["wikidata_qid"] = old.get("wikidata_qid")
        artist_rows.append(row)
    plans = {
        "artists": persistence.plan_rows("artists", artist_rows, existing["artists"]),
        "aliases": persistence.plan_rows("aliases", collected["aliases"], existing["aliases"]),
        "edges": persistence.plan_rows("edges", collected["edges"], existing["edges"]),
        "evidence": [], "similarities": [],
        "product_artists": persistence.plan_rows("product_artists", collected["product_artists"], existing["product_artists"]),
    }
    for row in collected["edges"]: persistence.validate_edge(row)
    for row in collected["evidence"]:
        kind=persistence.evidence_contract(row)
        plans["evidence"].extend(persistence.plan_rows(kind,[row],[old for old in existing["evidence"] if persistence.evidence_contract(old)==kind]))
    unresolved_by_key={(row["source_system"],row["source_mbid"],row["returned_target_name_normalized"]):row for row in existing["similarities"] if row["resolution_status"] in {"unresolved","conflict"}}
    for row in collected["similarities"]:
        if row["resolution_status"]=="resolved":
            resolved_existing=[old for old in existing["similarities"] if old["resolution_status"]=="resolved"]
            direct=persistence.plan_rows("similarity_resolved",[row],resolved_existing)[0]
            if direct["action"]=="CREATE":
                old=unresolved_by_key.get((row["source_system"],row["source_mbid"],row["returned_target_name_normalized"]))
                if old: direct=persistence.plan_similarity_resolution(row,old,identity_proven=True)
            plans["similarities"].append(direct)
        else:
            plans["similarities"].extend(persistence.plan_rows("similarity_unresolved",[row],existing["similarities"]))
    return plans


def _failure(source: str, exc: Exception) -> str:
    text=str(exc).lower()
    if source=="musicbrainz": return "MUSICBRAINZ_TRANSIENT" if any(x in text for x in ("429","500","502","503","504","timeout")) else "MUSICBRAINZ_PERMANENT"
    if source=="lastfm": return "LASTFM_TRANSIENT" if any(x in text for x in ("429","500","502","503","504","timeout")) else "LASTFM_PERMANENT"
    return "GLOBAL_CONFIGURATION_ERROR"


def collect_source(conn: psycopg.Connection[Any], source: Source, config: BoundedConfig,
                   mb: collector.MusicBrainzClient, lastfm: collector.LastFmClient | None,
                   existing_snapshot: dict[str, list[dict[str, Any]]], product_rows: list[tuple[str, str]]) -> SourceResult:
    started=time.perf_counter(); result=SourceResult(source)
    mb_before=(mb.http.request_count,mb.http.retry_count,mb.http.cache_hits)
    lf_before=(lastfm.http.request_count,lastfm.http.retry_count,lastfm.http.cache_hits) if lastfm else (0,0,0)
    try:
        try:
            payload=mb.artist(source.mbid)
        except Exception as exc:
            result.status="failed"; result.errors.append({"classification":_failure("musicbrainz",exc),"detail":f"{type(exc).__name__}: {exc}"}); return result
        identity=validate_source(source,payload); result.identity=identity
        if identity["status"]!="resolved":
            result.status="failed"; result.errors.append({"classification":"SOURCE_IDENTITY_CONFLICT","detail":",".join(identity["reasons"])}); return result
        aliases=collector.extract_aliases(source.mbid,payload)
        targets,memberships,membership_counts=extract_direct_memberships(source,payload,config.max_direct_targets)
        similarities=[]; local_targets=[]; local_links=[]
        if lastfm is not None:
            try:
                returned=lastfm.similar(source.display_name,config.lastfm_limit)
            except Exception as exc:
                result.status="failed"; result.errors.append({"classification":_failure("lastfm",exc),"detail":f"{type(exc).__name__}: {exc}"}); return result
            similarities,local_targets,local_links=resolve_lastfm_rows(conn,source,returned,existing_snapshot["artists"])
        artists=[{key:identity[key] for key in ("musicbrainz_artist_mbid","display_name","entity_type","musicbrainz_type_id","wikidata_qid")}] + targets + local_targets
        artists=collector.dedupe(artists,("musicbrainz_artist_mbid",))
        evidence=list(memberships); recording_calls=0
        if config.recording_release_seeds:
            dynamic=collector.PilotConfig("GENERIC_SOURCE_V1",tuple(collector.PilotArtist(a["display_name"],a["musicbrainz_artist_mbid"],a["entity_type"].capitalize()) for a in artists),tuple())
            for release_mbid in config.recording_release_seeds:
                release=collector.hydrate_release_recordings(mb,mb.release(release_mbid),dynamic); rows,_=collector.extract_recording_evidence(release,dynamic); evidence.extend(rows); recording_calls+=1
        edges=collector.derive_edges(evidence)
        product_links=collector.dedupe(local_links+plan_product_matches(conn,artists,aliases,product_rows),collector.UPSERT_KEYS["product_artists"])
        collected={"artists":artists,"aliases":aliases,"edges":edges,"evidence":evidence,"similarities":similarities,"product_artists":product_links}
        plans=build_plans(collected,existing_snapshot); rollback=persistence.rollback_manifest(plans)
        result.collected=collected; result.plans=plans; result.rollback=rollback
        result.counters={"membership":membership_counts,"aliases":len(aliases),"direct_targets":len(targets),"lastfm_results":len(similarities),
                         "local_category_a_resolutions":sum(row.get("resolution_route")=="local_category_a" for row in similarities),
                         "unresolved_similarity":sum(row["resolution_status"]=="unresolved" for row in similarities),
                         "recording_api_calls":recording_calls,"same_run_recursion":0,
                         "actions":{family:{action:sum(row.get("action")==action for row in rows) for action in ("CREATE","SEEN_AGAIN","RESOLVE_EXISTING_UNRESOLVED","CONFLICT","SKIP")} for family,rows in plans.items()}}
        if any(row.get("action")=="CONFLICT" for rows in plans.values() for row in rows):
            result.status="failed"; result.errors.append({"classification":"PERSISTENCE_CONFLICT","detail":"one or more planned rows conflict"})
    except Exception as exc:
        result.status="failed"; result.errors.append({"classification":"PERSISTENCE_CONFLICT","detail":f"{type(exc).__name__}: {exc}"})
    finally:
        result.counters.update({
            "source_artist_id":source.artist_id,"source_artist_mbid":source.mbid,"source_display_name":source.display_name,
            "collector_phases":["identity","aliases","membership","lastfm","local_category_a","product_matching"] + (["recording"] if config.recording_release_seeds else []),
            "input_limits":{"max_direct_targets":config.max_direct_targets,"lastfm_limit":config.lastfm_limit,"graph_depth":config.graph_depth,"recording_release_seeds":list(config.recording_release_seeds)},
            "requests":{"musicbrainz":mb.http.request_count-mb_before[0],"lastfm":((lastfm.http.request_count if lastfm else 0)-lf_before[0])},
            "retries":{"musicbrainz":mb.http.retry_count-mb_before[1],"lastfm":((lastfm.http.retry_count if lastfm else 0)-lf_before[1])},
            "cache_hits":{"musicbrainz":mb.http.cache_hits-mb_before[2],"lastfm":((lastfm.http.cache_hits if lastfm else 0)-lf_before[2])},
            "errors":len(result.errors),"elapsed_ms":round((time.perf_counter()-started)*1000,3),
        })
        result.run_plan={"id":str(uuid.uuid4()),"collector":GENERIC_COLLECTOR,"source_system":"musicbrainz+lastfm+vinylofy",
                         "scope":"bounded_source_depth_1","status":"planned_dry_run","counters":result.counters,
                         "created_ids":{family:[row.get("planned_id") for row in rows if row.get("action")=="CREATE"] for family,rows in result.plans.items()},
                         "mutable_preimages":{family:[row["preimage"] for row in rows if row.get("preimage")] for family,rows in result.plans.items()}}
    return result


def orchestrate(sources: list[Source], collect: Callable[[Source], SourceResult]) -> list[SourceResult]:
    results=[]
    for source in sources:
        try: results.append(collect(source))
        except Exception as exc:
            results.append(SourceResult(source,status="failed",errors=[{"classification":_failure("orchestration",exc),"detail":str(exc)}]))
    return results


def build_parser() -> argparse.ArgumentParser:
    parser=argparse.ArgumentParser(description="Generic bounded Follow-the-Groove collector")
    mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run",action="store_true")
    mode.add_argument("--write",action="store_true")
    source_mode=parser.add_mutually_exclusive_group()
    source_mode.add_argument("--frontier",action="store_true",help="select unprocessed bounded frontier sources")
    source_mode.add_argument("--refresh",action="store_true",help="allow explicit successful sources to be replayed")
    parser.add_argument("--max-sources",type=int,default=10)
    parser.add_argument("--max-direct-targets",type=int,default=25)
    parser.add_argument("--lastfm-limit",type=int,default=5)
    parser.add_argument("--graph-depth",type=int,default=1)
    parser.add_argument("--source-mbid",action="append",default=[])
    parser.add_argument("--recording-release-seed",action="append",default=[])
    parser.add_argument("--output",type=Path)
    return parser


def validate_write_scope(args: argparse.Namespace) -> None:
    if not bool(getattr(args,"write",False)):
        return
    refresh=bool(getattr(args,"refresh",False)); frontier=bool(getattr(args,"frontier",False))
    if refresh:
        if not 1 <= args.max_sources <= 10:
            raise persistence.PersistenceDisabled("refresh write max_sources must be between 1 and 10")
        if not args.source_mbid or len(args.source_mbid)!=args.max_sources:
            raise persistence.PersistenceDisabled("refresh write requires one explicit --source-mbid per bounded source")
        return
    if frontier:
        if args.source_mbid:
            raise persistence.PersistenceDisabled("frontier write does not accept explicit source MBIDs")
        if not 1 <= args.max_sources <= 10:
            raise persistence.PersistenceDisabled("frontier write max_sources must be between 1 and 10")
        return
    raise persistence.PersistenceDisabled("write requires explicit --frontier or --refresh mode")


def run(args: argparse.Namespace) -> dict[str, Any]:
    if load_dotenv: load_dotenv(".env.local",override=False)
    write=bool(getattr(args,"write",False))
    validate_write_scope(args)
    config=BoundedConfig(args.max_sources,args.max_direct_targets,args.lastfm_limit,args.graph_depth,tuple(args.recording_release_seed),True)
    started=now(); started_perf=time.perf_counter()
    conn=psycopg.connect(os.environ["DATABASE_URL"],autocommit=False); conn.execute("begin read only")
    try:
        sources=select_sources(conn,config.max_sources,tuple(args.source_mbid),include_successful=bool(getattr(args,"refresh",False)))
        existing_snapshot=read_existing(conn)
        product_rows=[(str(row[0]),str(row[1])) for row in conn.execute("select id::text,artist from products").fetchall()]
        http=collector.HttpJsonClient(os.getenv("MUSICBRAINZ_USER_AGENT",collector.DEFAULT_USER_AGENT)); mb=collector.MusicBrainzClient(http)
        lastfm_http=None; lastfm=None; api_key=os.getenv("LASTFM_API_KEY","")
        if api_key:
            lastfm_http=collector.HttpJsonClient(collector.DEFAULT_USER_AGENT,sleep_seconds=1.0); lastfm=collector.LastFmClient(lastfm_http,api_key)
        results=orchestrate(sources,lambda source: collect_source(conn,source,config,mb,lastfm,existing_snapshot,product_rows))
        output={"mode":"write-preflight" if write else "dry-run","started_at":started,"finished_at":now(),"elapsed_ms":round((time.perf_counter()-started_perf)*1000,3),
                "config":asdict(config),"selected_sources":[asdict(source) for source in sources],
                "database_query_plan":{"fixed_queries":8,"per_source_queries":2,"estimated_total":8+2*len(sources),"obvious_n_plus_one":False},
                "api_counters":{"musicbrainz_requests":http.request_count,"musicbrainz_retries":http.retry_count,"musicbrainz_cache_hits":http.cache_hits,
                                "lastfm_requests":lastfm_http.request_count if lastfm_http else 0,"lastfm_retries":lastfm_http.retry_count if lastfm_http else 0},
                "same_run_recursion":0,"recording_api_calls":sum(item.counters.get("recording_api_calls",0) for item in results),
                "sources":[asdict(item) for item in results]}
        conn.rollback()
    finally: conn.close()
    if write:
        if len(results)!=args.max_sources or any(item.status!="succeeded" or item.rollback.get("status")!="PROVEN" for item in results):
            raise persistence.PersistenceConflict("write preflight did not produce a successful proven plan for every source")
        output["writes"]=execute_writes(os.environ["DATABASE_URL"],results)
        output["mode"]="write"
    if args.output: args.output.write_text(json.dumps(output,ensure_ascii=False,indent=2,default=str)+"\n",encoding="utf-8")
    return output


def execute_writes(database_url: str, results: list[SourceResult]) -> list[dict[str, Any]]:
    writes=[]
    for result in results:
        run_id=persistence.register_source_run(database_url,result.run_plan)
        conn=psycopg.connect(database_url,autocommit=False)
        try:
            conn.execute("begin isolation level serializable")
            conn.execute("lock table artists,artist_aliases,artist_edges,artist_relation_evidence,artist_similarity,product_artists in share row exclusive mode")
            products_before=conn.execute("select count(*) from products").fetchone()[0]
            live_plans=build_plans(result.collected,read_existing(conn))
            live_rollback=persistence.rollback_manifest(live_plans)
            if live_rollback["status"]!="PROVEN":
                raise persistence.PersistenceConflict("live per-source rollback plan is not proven")
            audit=persistence.apply_source_plan(conn,run_id=run_id,source=asdict(result.source),plans=live_plans,counters=result.counters,products_before=products_before)
            conn.commit()
        except Exception as exc:
            conn.rollback(); conn.close()
            persistence.mark_source_run_failed(database_url,run_id,"PERSISTENCE_CONFLICT",f"{type(exc).__name__}: {exc}")
            raise
        else:
            conn.close()
        with psycopg.connect(database_url,autocommit=False) as check:
            check.execute("begin read only")
            post=persistence.audit_source_run(check,run_id)
            check.rollback()
        if not post["proven"]:
            raise persistence.PersistenceConflict(f"post-commit source audit failed for {run_id}: {post}")
        writes.append({**audit,"postcommit":post})
    return writes


def main() -> int:
    args=build_parser().parse_args(); result=run(args); print(json.dumps(result,ensure_ascii=False,indent=2,default=lambda v:str(v) if isinstance(v,Decimal) else v)); return 0 if all(item["status"]=="succeeded" for item in result["sources"]) else 1


if __name__ == "__main__": raise SystemExit(main())
