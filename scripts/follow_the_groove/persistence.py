"""Persistence planning and gated writing for bounded Follow-the-Groove runs.

Incoming rows are classified deterministically before the separately gated
writer applies an already validated plan with exact rollback provenance.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence
from datetime import datetime, timezone
import json
import os
import uuid

import psycopg


class PersistenceDisabled(RuntimeError):
    pass


class DisabledWriter:
    @staticmethod
    def persist(*_args: Any, **_kwargs: Any) -> None:
        raise PersistenceDisabled("Generic FTG persistence is disabled; use --dry-run")


ALLOWED_WRITE_TABLES = frozenset({
    "follow_the_groove_collection_runs", "artists", "artist_aliases",
    "artist_edges", "artist_relation_evidence", "artist_similarity",
    "product_artists",
})


class PersistenceConflict(RuntimeError):
    pass


@dataclass(frozen=True)
class EntityContract:
    key: tuple[str, ...]
    immutable: tuple[str, ...]


CONTRACTS: dict[str, EntityContract] = {
    "artists": EntityContract(
        ("musicbrainz_artist_mbid",),
        ("musicbrainz_artist_mbid", "display_name", "entity_type", "musicbrainz_type_id", "wikidata_qid"),
    ),
    "aliases": EntityContract(
        ("artist_mbid", "source_system", "alias_name", "alias_type", "locale", "begin_date", "end_date"),
        ("artist_mbid", "source_system", "alias_name", "alias_type", "locale", "begin_date", "end_date", "alias_normalized", "is_primary", "provenance"),
    ),
    "edges": EntityContract(
        ("artist_low_mbid", "artist_high_mbid"),
        ("artist_low_mbid", "artist_high_mbid"),
    ),
    "membership": EntityContract(
        ("source_system", "relation_type_id", "source_mbid", "target_mbid", "begin_date", "end_date", "attribute_ids"),
        ("source_system", "relation_type_id", "source_mbid", "target_mbid", "begin_date", "end_date", "attribute_ids", "ended", "direction", "classification", "evidence_kind", "provenance"),
    ),
    "artist_credit": EntityContract(
        ("source_system", "recording_mbid", "source_mbid", "target_mbid", "evidence_kind"),
        ("source_system", "recording_mbid", "source_mbid", "target_mbid", "evidence_kind", "release_mbid", "direction", "classification"),
    ),
    "performer": EntityContract(
        ("source_system", "evidence_kind", "recording_mbid", "relation_type_id", "source_mbid", "target_mbid", "attribute_ids"),
        ("source_system", "evidence_kind", "recording_mbid", "relation_type_id", "source_mbid", "target_mbid", "attribute_ids", "release_mbid", "direction", "classification"),
    ),
    "decision": EntityContract(
        ("source_system", "evidence_kind", "source_entity_kind", "source_entity_id", "source_mbid", "target_mbid", "relation_type_id", "recording_mbid", "work_mbid", "attribute_ids"),
        ("source_system", "evidence_kind", "source_entity_kind", "source_entity_id", "source_mbid", "target_mbid", "relation_type_id", "recording_mbid", "work_mbid", "attribute_ids", "classification", "reason"),
    ),
    "similarity_resolved": EntityContract(
        ("source_system", "source_mbid", "target_mbid"),
        ("source_system", "source_mbid", "target_mbid", "returned_mbid", "resolution_status", "returned_target_name_normalized", "position", "match_score"),
    ),
    "similarity_unresolved": EntityContract(
        ("source_system", "source_mbid", "returned_target_name_normalized"),
        ("source_system", "source_mbid", "returned_target_name_normalized", "returned_mbid", "resolution_status", "position", "match_score"),
    ),
    "product_artists": EntityContract(
        ("product_id", "artist_mbid"),
        ("product_id", "artist_mbid", "credited_name", "credit_position", "source_system"),
    ),
}


def _hashable(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_hashable(item) for item in value)
    if isinstance(value, dict):
        return tuple(sorted((key, _hashable(item)) for key, item in value.items()))
    return value


def natural_key(kind: str, row: Mapping[str, Any]) -> tuple[Any, ...]:
    return tuple(_hashable(row.get(field)) for field in CONTRACTS[kind].key)


def seen_again_preimage(row: Mapping[str, Any]) -> dict[str, Any]:
    preimage = {"id": row.get("id"), "last_seen_run_id": row.get("last_seen_run_id")}
    for field in ("last_verified_at", "last_seen_at", "updated_at", "checked_at"):
        if field in row:
            preimage[field] = row.get(field)
    return preimage


def similarity_resolution_preimage(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "id": row.get("id"),
        "target_artist_id": row.get("target_artist_id"),
        "resolution_status": row.get("resolution_status"),
        "last_seen_run_id": row.get("last_seen_run_id"),
        "updated_at": row.get("updated_at"),
    }


def plan_rows(kind: str, incoming: Iterable[Mapping[str, Any]], existing: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    contract = CONTRACTS[kind]
    current = {natural_key(kind, row): dict(row) for row in existing}
    planned: list[dict[str, Any]] = []
    observed: set[tuple[Any, ...]] = set()
    for raw in incoming:
        row = dict(raw)
        key = natural_key(kind, row)
        if key in observed:
            planned.append({**row, "action": "SKIP", "reason": "duplicate_incoming"})
            continue
        observed.add(key)
        old = current.get(key)
        if old is None:
            planned.append({**row, "action": "CREATE", "planned_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"ftg:{kind}:{key!r}"))})
            continue
        conflicts = {
            field: {"existing": old.get(field), "incoming": row.get(field)}
            for field in contract.immutable
            if _hashable(old.get(field)) != _hashable(row.get(field))
        }
        if conflicts:
            planned.append({**row, "action": "CONFLICT", "conflicts": conflicts, "existing_id": old.get("id")})
        else:
            planned.append({**row, "action": "SEEN_AGAIN", "existing_id": old.get("id"), "preimage": seen_again_preimage(old)})
    return planned


def plan_similarity_resolution(incoming: Mapping[str, Any], unresolved: Mapping[str, Any], *, identity_proven: bool) -> dict[str, Any]:
    immutable = ("source_system", "source_mbid", "returned_target_name_normalized", "returned_mbid", "position", "match_score")
    conflicts = {
        field: {"existing": unresolved.get(field), "incoming": incoming.get(field)}
        for field in immutable
        if _hashable(unresolved.get(field)) != _hashable(incoming.get(field))
    }
    if not identity_proven or unresolved.get("resolution_status") != "unresolved" or unresolved.get("target_artist_id") is not None or conflicts:
        return {**dict(incoming), "action": "CONFLICT", "conflicts": conflicts or {"identity": "not_proven"}}
    return {**dict(incoming), "action": "RESOLVE_EXISTING_UNRESOLVED", "existing_id": unresolved.get("id"), "preimage": similarity_resolution_preimage(unresolved)}


def validate_edge(row: Mapping[str, Any]) -> None:
    if row.get("artist_low_mbid") == row.get("artist_high_mbid"):
        raise ValueError("self edge is forbidden")
    if str(row.get("artist_low_mbid")) > str(row.get("artist_high_mbid")):
        raise ValueError("edge endpoints must be canonical")


def evidence_contract(row: Mapping[str, Any]) -> str:
    kind = row.get("evidence_kind")
    if kind == "membership":
        return "membership"
    if kind == "artist_credit":
        return "artist_credit"
    if kind in {"instrument", "vocal"}:
        return "performer"
    return "decision"


DELETE_ORDER = ("product_artists", "similarities", "evidence", "edges", "aliases", "artists", "collection_run")
ROLLBACK_OUTCOMES = frozenset({"DELETE_SAFE", "PRESERVE_SHARED", "RESTORE_PREIMAGE", "BLOCKED_UNSAFE"})


def rollback_manifest(plans: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    created: dict[str, list[Any]] = {}
    restored: dict[str, list[dict[str, Any]]] = {}
    conflicts = 0
    for family, rows in plans.items():
        created[family] = [row.get("planned_id") or row.get("id") or natural_key(_family_contract(family, row), row) for row in rows if row.get("action") == "CREATE"]
        restored[family] = [dict(row["preimage"]) for row in rows if row.get("preimage")]
        conflicts += sum(row.get("action") == "CONFLICT" for row in rows)
    return {
        "status": "PROVEN" if conflicts == 0 else "NOT_PROVEN",
        "created_rows_to_delete": created,
        "mutable_rows_to_restore": restored,
        "dependency_delete_order": list(DELETE_ORDER),
        "artist_delete_condition": "created_by_run_id=current_run AND no remaining dependencies AND no later run dependency",
        "preexisting_artists_protected": True,
        "collection_run_deleted_last": True,
        "late_revalidation_required": True,
    }


def _family_contract(family: str, row: Mapping[str, Any]) -> str:
    if family == "evidence":
        return evidence_contract(row)
    if family == "similarities":
        return "similarity_resolved" if row.get("resolution_status") == "resolved" else "similarity_unresolved"
    return family


def _json(value: Any) -> str:
    return json.dumps(value, default=str, ensure_ascii=False)


def _row_id(row: Mapping[str, Any]) -> str:
    value = row.get("planned_id") if row.get("action") == "CREATE" else row.get("existing_id")
    if not value:
        raise PersistenceConflict(f"missing database id for {row.get('action')}")
    return str(value)


def register_source_run(database_url: str, run_plan: Mapping[str, Any]) -> str:
    """Durably register one running source before its atomic data transaction."""
    run_id = str(run_plan["id"])
    with psycopg.connect(database_url, autocommit=False) as conn:
        conn.execute("begin isolation level serializable")
        conn.execute(
            "insert into follow_the_groove_collection_runs "
            "(id,collector,source_system,scope,status,counters,error_summary) "
            "values (%s,%s,%s,%s,'running',%s::jsonb,'[]'::jsonb)",
            (run_id, run_plan["collector"], run_plan["source_system"], run_plan["scope"], _json(run_plan["counters"])),
        )
        conn.commit()
    return run_id


def mark_source_run_failed(database_url: str, run_id: str, classification: str, detail: str) -> None:
    with psycopg.connect(database_url, autocommit=False) as conn:
        conn.execute("begin isolation level serializable")
        changed = conn.execute(
            "update follow_the_groove_collection_runs set status='failed',finished_at=now(),"
            "error_summary=%s::jsonb where id=%s and status='running'",
            (_json([{"classification": classification, "detail": detail}]), run_id),
        )
        if changed.rowcount != 1:
            raise PersistenceConflict("running source run could not be marked failed")
        conn.commit()


def _counts(conn: psycopg.Connection[Any]) -> dict[str, int]:
    return {table: conn.execute(f"select count(*) from {table}").fetchone()[0] for table in (
        "follow_the_groove_collection_runs", "artists", "artist_aliases", "artist_edges",
        "artist_relation_evidence", "artist_similarity", "product_artists",
    )}


def _action_counts(plans: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, dict[str, int]]:
    return {family: {action: sum(row.get("action") == action for row in rows) for action in
                     ("CREATE", "SEEN_AGAIN", "RESOLVE_EXISTING_UNRESOLVED", "CONFLICT", "SKIP")}
            for family, rows in plans.items()}


def referenced_artist_mbids(plans: Mapping[str, Sequence[Mapping[str, Any]]]) -> set[str]:
    """Return every MBID needed as an artist foreign key by this write plan."""
    fields = {
        "artists": ("musicbrainz_artist_mbid",),
        "aliases": ("artist_mbid",),
        "edges": ("artist_low_mbid", "artist_high_mbid"),
        "evidence": ("source_mbid", "target_mbid"),
        "similarities": ("source_mbid", "target_mbid"),
        "product_artists": ("artist_mbid",),
    }
    required: set[str] = set()
    for family, family_fields in fields.items():
        for row in plans.get(family, ()):
            if row.get("action") in {"CONFLICT", "SKIP"}:
                continue
            for field in family_fields:
                value = row.get(field)
                if value:
                    required.add(str(value))
    return required


def resolve_referenced_artist_ids(
    conn: psycopg.Connection[Any], plans: Mapping[str, Sequence[Mapping[str, Any]]]
) -> dict[str, str]:
    """Build one authoritative MBID-to-ID map for all writer families."""
    artist_ids: dict[str, str] = {}
    for row in plans.get("artists", ()):
        if row.get("action") in {"CREATE", "SEEN_AGAIN"}:
            artist_ids[str(row["musicbrainz_artist_mbid"])] = _row_id(row)

    missing = sorted(referenced_artist_mbids(plans) - artist_ids.keys())
    if missing:
        rows = conn.execute(
            "select id,musicbrainz_artist_mbid,display_name,entity_type,musicbrainz_type_id "
            "from artists where musicbrainz_artist_mbid=any(%s::uuid[])",
            (missing,),
        ).fetchall()
        for row in rows:
            artist_ids[str(row[1])] = str(row[0])

    unresolved = sorted(referenced_artist_mbids(plans) - artist_ids.keys())
    if unresolved:
        raise PersistenceConflict(f"missing referenced artist MBID(s): {', '.join(unresolved)}")
    return artist_ids


def canonical_artist_id_pair(
    source_mbid: str, target_mbid: str, artist_ids: Mapping[str, str]
) -> tuple[str, str]:
    """Canonical physical edge endpoints using the resolved database UUIDs."""
    try:
        source_id = str(artist_ids[source_mbid])
        target_id = str(artist_ids[target_mbid])
    except KeyError as exc:
        raise PersistenceConflict(f"missing referenced artist MBID: {exc.args[0]}") from exc
    if source_id == target_id:
        raise PersistenceConflict("self edge database IDs are forbidden")
    return tuple(sorted((source_id, target_id), key=uuid.UUID))


def apply_source_plan(
    conn: psycopg.Connection[Any], *, run_id: str, source: Mapping[str, Any],
    plans: Mapping[str, Sequence[Mapping[str, Any]]], counters: Mapping[str, Any], products_before: int,
) -> dict[str, Any]:
    """Execute an already validated plan inside the caller's source transaction."""
    conflicts = [(family, row) for family, rows in plans.items() for row in rows if row.get("action") == "CONFLICT"]
    if conflicts:
        raise PersistenceConflict(f"plan contains {len(conflicts)} conflicts")
    before = _counts(conn)
    artist_ids = resolve_referenced_artist_ids(conn, plans)
    created_ids: dict[str, list[str]] = {family: [] for family in plans}
    preimages: dict[str, list[dict[str, Any]]] = {family: [] for family in plans}

    for row in plans["artists"]:
        action = row["action"]
        if action == "CREATE":
            row_id = _row_id(row)
            conn.execute(
                "insert into artists (id,musicbrainz_artist_mbid,display_name,entity_type,musicbrainz_type_id,wikidata_qid,created_by_run_id,last_seen_run_id) "
                "values (%s,%s,%s,%s,%s,%s,%s,%s)",
                (row_id,row["musicbrainz_artist_mbid"],row["display_name"],row["entity_type"],row["musicbrainz_type_id"],row.get("wikidata_qid"),run_id,run_id),
            )
            created_ids["artists"].append(row_id)
        elif action == "SEEN_AGAIN":
            row_id = _row_id(row); preimages["artists"].append(dict(row["preimage"]))
            changed=conn.execute("update artists set last_seen_run_id=%s,last_verified_at=now() where id=%s",(run_id,row_id))
            if changed.rowcount != 1: raise PersistenceConflict("artist freshness update failed")
        else:
            continue
        artist_ids[row["musicbrainz_artist_mbid"]] = row_id

    for row in plans["aliases"]:
        action=row["action"]
        if action=="CREATE":
            row_id=_row_id(row); artist_id=artist_ids[row["artist_mbid"]]
            conn.execute(
                "insert into artist_aliases (id,artist_id,alias_name,alias_normalized,source_system,alias_type,locale,is_primary,begin_date,end_date,provenance,created_by_run_id,last_seen_run_id) "
                "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)",
                (row_id,artist_id,row["alias_name"],row["alias_normalized"],row["source_system"],row.get("alias_type"),row.get("locale"),row.get("is_primary",False),row.get("begin_date"),row.get("end_date"),_json(row.get("provenance",{})),run_id,run_id),
            ); created_ids["aliases"].append(row_id)
        elif action=="SEEN_AGAIN":
            row_id=_row_id(row); preimages["aliases"].append(dict(row["preimage"])); conn.execute("update artist_aliases set last_seen_run_id=%s,last_verified_at=now() where id=%s",(run_id,row_id))

    edge_ids: dict[tuple[str,str],str] = {}
    for row in plans["edges"]:
        action=row["action"]
        if action not in {"CREATE", "SEEN_AGAIN"}:
            continue
        database_pair=canonical_artist_id_pair(row["artist_low_mbid"],row["artist_high_mbid"],artist_ids)
        if action=="CREATE":
            row_id=_row_id(row); conn.execute("insert into artist_edges (id,artist_low_id,artist_high_id,created_by_run_id,last_seen_run_id) values (%s,%s,%s,%s,%s)",(row_id,database_pair[0],database_pair[1],run_id,run_id)); created_ids["edges"].append(row_id)
        elif action=="SEEN_AGAIN":
            row_id=_row_id(row); preimages["edges"].append(dict(row["preimage"])); conn.execute("update artist_edges set last_seen_run_id=%s,updated_at=now() where id=%s",(run_id,row_id))
        edge_ids[database_pair]=row_id

    for row in plans["evidence"]:
        action=row["action"]
        if action=="CREATE":
            row_id=_row_id(row); database_pair=canonical_artist_id_pair(row["source_mbid"],row["target_mbid"],artist_ids); edge_id=edge_ids.get(database_pair) if row["classification"]=="allowed" else None
            if row["classification"]=="allowed" and not edge_id: raise PersistenceConflict("allowed evidence has no planned edge")
            conn.execute(
                "insert into artist_relation_evidence (id,source_artist_id,target_artist_id,edge_id,created_by_run_id,last_seen_run_id,source_system,source_entity_kind,source_entity_id,evidence_kind,classification,direction,source_relation_name,relation_type_id,begin_date,end_date,ended,recording_mbid,release_mbid,release_group_mbid,work_mbid,attribute_ids,provenance) "
                "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                (row_id,artist_ids[row["source_mbid"]],artist_ids[row["target_mbid"]],edge_id,run_id,run_id,row["source_system"],row["source_entity_kind"],row["source_entity_id"],row["evidence_kind"],row["classification"],row["direction"],row.get("source_relation_name"),row.get("relation_type_id"),row.get("begin_date"),row.get("end_date"),row.get("ended"),row.get("recording_mbid"),row.get("release_mbid"),row.get("release_group_mbid"),row.get("work_mbid"),row.get("attribute_ids",[]),_json(row.get("provenance",{}))),
            ); created_ids["evidence"].append(row_id)
        elif action=="SEEN_AGAIN":
            row_id=_row_id(row); preimages["evidence"].append(dict(row["preimage"])); conn.execute("update artist_relation_evidence set last_seen_run_id=%s,last_seen_at=now(),last_verified_at=now() where id=%s",(run_id,row_id))

    for row in plans["similarities"]:
        action=row["action"]
        if action=="CREATE":
            row_id=_row_id(row); target_id=artist_ids.get(row.get("target_mbid")) if row["resolution_status"]=="resolved" else None
            if row["resolution_status"]=="resolved" and not target_id: raise PersistenceConflict("resolved similarity target missing")
            conn.execute(
                "insert into artist_similarity (id,source_artist_id,target_artist_id,created_by_run_id,last_seen_run_id,source_system,requested_source_name,returned_target_name,returned_target_name_normalized,returned_mbid,match_score,position,resolution_status) "
                "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (row_id,artist_ids[row["source_mbid"]],target_id,run_id,run_id,row["source_system"],row["requested_source_name"],row["returned_target_name"],row["returned_target_name_normalized"],row.get("returned_mbid"),row["match_score"],row["position"],row["resolution_status"]),
            ); created_ids["similarities"].append(row_id)
        elif action=="SEEN_AGAIN":
            row_id=_row_id(row); preimages["similarities"].append(dict(row["preimage"])); conn.execute("update artist_similarity set last_seen_run_id=%s,checked_at=now() where id=%s",(run_id,row_id))
        elif action=="RESOLVE_EXISTING_UNRESOLVED":
            row_id=_row_id(row); preimages["similarities"].append(dict(row["preimage"])); target_id=artist_ids[row["target_mbid"]]
            changed=conn.execute("update artist_similarity set target_artist_id=%s,resolution_status='resolved',last_seen_run_id=%s,updated_at=now() where id=%s and resolution_status='unresolved' and target_artist_id is null",(target_id,run_id,row_id))
            if changed.rowcount != 1: raise PersistenceConflict("similarity resolution precondition changed")

    for row in plans["product_artists"]:
        action=row["action"]
        if action=="CREATE":
            row_id=_row_id(row); conn.execute("insert into product_artists (id,product_id,artist_id,credited_name,credit_position,source_system,created_by_run_id,last_seen_run_id) values (%s,%s,%s,%s,%s,%s,%s,%s)",(row_id,row["product_id"],artist_ids[row["artist_mbid"]],row.get("credited_name"),row.get("credit_position"),row["source_system"],run_id,run_id)); created_ids["product_artists"].append(row_id)
        elif action=="SEEN_AGAIN":
            row_id=_row_id(row); preimages["product_artists"].append(dict(row["preimage"])); conn.execute("update product_artists set last_seen_run_id=%s,last_verified_at=now() where id=%s",(run_id,row_id))

    after=_counts(conn); action_counts=_action_counts(plans)
    expected_delta={"artists":action_counts["artists"]["CREATE"],"artist_aliases":action_counts["aliases"]["CREATE"],"artist_edges":action_counts["edges"]["CREATE"],"artist_relation_evidence":action_counts["evidence"]["CREATE"],"artist_similarity":action_counts["similarities"]["CREATE"],"product_artists":action_counts["product_artists"]["CREATE"]}
    for table,delta in expected_delta.items():
        if after[table]-before[table] != delta: raise PersistenceConflict(f"unexpected {table} delta")
    checks={
        "products_unchanged":conn.execute("select count(*) from products").fetchone()[0]==products_before,
        "self_edges":conn.execute("select count(*) from artist_edges where artist_low_id>=artist_high_id").fetchone()[0]==0,
        "rejected_unlinked":conn.execute("select count(*) from artist_relation_evidence where classification<>'allowed' and edge_id is not null").fetchone()[0]==0,
        "similarity_target_contract":conn.execute("select count(*) from artist_similarity where (resolution_status='resolved')<>(target_artist_id is not null)").fetchone()[0]==0,
        "duplicate_product_links":conn.execute("select count(*) from (select product_id,artist_id from product_artists group by 1,2 having count(*)>1)x").fetchone()[0]==0,
    }
    if not all(checks.values()): raise PersistenceConflict(f"precommit invariant failed: {checks}")
    rollback=rollback_manifest(plans)
    final_counters={**dict(counters),"source_selection_reason":source["selection_reason"],"actions":action_counts,"created_ids":created_ids,"mutable_preimages":preimages,"rollback_manifest":rollback,"precommit_checks":checks}
    changed=conn.execute("update follow_the_groove_collection_runs set status='succeeded',counters=%s::jsonb,finished_at=now() where id=%s and status='running'",(_json(final_counters),run_id))
    if changed.rowcount != 1: raise PersistenceConflict("source run success transition failed")
    return {"run_id":run_id,"before":before,"after":after,"actions":action_counts,"created_ids":created_ids,"preimages":preimages,"rollback":rollback,"checks":checks}


def classify_created_rollback_row(
    *, run_id: str, last_seen_run_id: str | None, external_dependencies: Sequence[Mapping[str, Any]] = (),
) -> str:
    """Classify a created row using current state, never creator ownership alone."""
    if external_dependencies or (last_seen_run_id is not None and str(last_seen_run_id) != str(run_id)):
        return "PRESERVE_SHARED"
    return "DELETE_SAFE"


def classify_preimage_rollback_row(
    *, run_id: str, row_exists: bool, last_seen_run_id: str | None,
) -> str:
    """Restore only when the rollback run is still the latest observer."""
    if not row_exists:
        return "BLOCKED_UNSAFE"
    if last_seen_run_id is not None and str(last_seen_run_id) != str(run_id):
        return "PRESERVE_SHARED"
    return "RESTORE_PREIMAGE"


def revalidate_rollback_plan(conn: psycopg.Connection[Any], run_id: str) -> dict[str, Any]:
    """Replan rollback against current DB dependencies without mutating data.

    The durable original manifest remains audit evidence. This late plan is the
    authority for execution: rows used by later runs are preserved, preimages
    are restored only while this run remains the latest observer, and unknown
    or missing mutable rows block rollback.
    """
    run=conn.execute("select status,counters from follow_the_groove_collection_runs where id=%s",(run_id,)).fetchone()
    if run is None:
        return {"proven":False,"reason":"run_missing","outcomes":{},"counts":{"BLOCKED_UNSAFE":1}}
    status,counters=run
    expected=counters.get("created_ids",{})
    preimages=counters.get("mutable_preimages",{})
    table_for={"artists":"artists","aliases":"artist_aliases","edges":"artist_edges","evidence":"artist_relation_evidence","similarities":"artist_similarity","product_artists":"product_artists"}
    outcomes: dict[str,list[dict[str,Any]]]={family:[] for family in (*table_for,"collection_run")}

    # Children are classified before artists so the artist decision can use
    # the exact set of rows that would survive this rollback.
    for family in ("product_artists","similarities","evidence","edges","aliases"):
        table=table_for[family]
        for row_id in expected.get(family,[]):
            row=conn.execute(f"select last_seen_run_id::text from {table} where id=%s and created_by_run_id=%s",(row_id,run_id)).fetchone()
            if row is None:
                outcomes[family].append({"id":str(row_id),"outcome":"BLOCKED_UNSAFE","reason":"owned_row_missing_or_creator_changed"})
                continue
            dependencies=[]
            if family=="edges":
                dependencies=[{"family":"evidence","id":str(r[0]),"created_by_run_id":str(r[1])} for r in conn.execute(
                    "select id,created_by_run_id from artist_relation_evidence where edge_id=%s and created_by_run_id is distinct from %s",(row_id,run_id)).fetchall()]
            outcome=classify_created_rollback_row(run_id=run_id,last_seen_run_id=row[0],external_dependencies=dependencies)
            outcomes[family].append({"id":str(row_id),"outcome":outcome,"dependencies":dependencies})

    preserved_child_ids={family:{item["id"] for item in rows if item["outcome"]=="PRESERVE_SHARED"} for family,rows in outcomes.items()}
    for row_id in expected.get("artists",[]):
        row=conn.execute("select last_seen_run_id::text from artists where id=%s and created_by_run_id=%s",(row_id,run_id)).fetchone()
        if row is None:
            outcomes["artists"].append({"id":str(row_id),"outcome":"BLOCKED_UNSAFE","reason":"owned_row_missing_or_creator_changed"})
            continue
        dependencies=[]
        specs=(
          ("aliases","artist_aliases","artist_id"),("edges","artist_edges","artist_low_id"),("edges","artist_edges","artist_high_id"),
          ("evidence","artist_relation_evidence","source_artist_id"),("evidence","artist_relation_evidence","target_artist_id"),
          ("similarities","artist_similarity","source_artist_id"),("similarities","artist_similarity","target_artist_id"),
          ("product_artists","product_artists","artist_id"),
        )
        for family,table,column in specs:
            for dep_id,creator in conn.execute(f"select id::text,created_by_run_id::text from {table} where {column}=%s",(row_id,)).fetchall():
                if str(creator)!=str(run_id) or dep_id in preserved_child_ids.get(family,set()):
                    dependencies.append({"family":family,"id":dep_id,"created_by_run_id":creator})
        outcome=classify_created_rollback_row(run_id=run_id,last_seen_run_id=row[0],external_dependencies=dependencies)
        outcomes["artists"].append({"id":str(row_id),"outcome":outcome,"dependencies":dependencies})

    for family,rows in preimages.items():
        table=table_for.get(family)
        if table is None:
            outcomes.setdefault(family,[]).append({"outcome":"BLOCKED_UNSAFE","reason":"unknown_preimage_family"})
            continue
        for image in rows:
            row=conn.execute(f"select last_seen_run_id::text from {table} where id=%s",(image.get("id"),)).fetchone()
            outcome=classify_preimage_rollback_row(run_id=run_id,row_exists=row is not None,last_seen_run_id=row[0] if row else None)
            outcomes[family].append({"id":str(image.get("id")),"outcome":outcome,"preimage":dict(image)})

    preserved_owned=any(item["outcome"]=="PRESERVE_SHARED" for family in table_for for item in outcomes[family] if item.get("id") in {str(x) for x in expected.get(family,[])})
    outcomes["collection_run"].append({"id":str(run_id),"outcome":"PRESERVE_SHARED" if preserved_owned else "DELETE_SAFE","reason":"immutable creator provenance" if preserved_owned else "no surviving owned rows"})
    counts={name:sum(item["outcome"]==name for rows in outcomes.values() for item in rows) for name in ROLLBACK_OUTCOMES}
    return {"proven":status=="succeeded" and counts["BLOCKED_UNSAFE"]==0,"status":status,"outcomes":outcomes,"counts":counts,"rollback_order":list(DELETE_ORDER),"late_revalidated":True,"opportunistic_cleanup":False}


def audit_source_run(conn: psycopg.Connection[Any], run_id: str) -> dict[str, Any]:
    run=conn.execute("select status,counters from follow_the_groove_collection_runs where id=%s",(run_id,)).fetchone()
    if run is None:
        return {"proven":False,"reason":"run_missing"}
    counters=run[1]; expected=counters.get("created_ids",{})
    table_for={"artists":"artists","aliases":"artist_aliases","edges":"artist_edges","evidence":"artist_relation_evidence","similarities":"artist_similarity","product_artists":"product_artists"}
    created={family:conn.execute(f"select count(*) from {table} where created_by_run_id=%s",(run_id,)).fetchone()[0] for family,table in table_for.items()}
    expected_counts={family:len(expected.get(family,[])) for family in table_for}
    restored={family:len(rows) for family,rows in counters.get("mutable_preimages",{}).items()}
    late_plan=revalidate_rollback_plan(conn,run_id)
    checks={
        "status_succeeded":run[0]=="succeeded",
        "created_counts":created==expected_counts,
        "generated_pairs":conn.execute("select count(*) from artist_relation_evidence where created_by_run_id=%s and (pair_low_id<>least(source_artist_id,target_artist_id) or pair_high_id<>greatest(source_artist_id,target_artist_id))",(run_id,)).fetchone()[0]==0,
        "allowed_edges_supported":conn.execute("select count(*) from artist_edges e where e.created_by_run_id=%s and not exists(select 1 from artist_relation_evidence ev where ev.edge_id=e.id and ev.classification='allowed')",(run_id,)).fetchone()[0]==0,
        "rejected_unlinked":conn.execute("select count(*) from artist_relation_evidence where created_by_run_id=%s and classification<>'allowed' and edge_id is not null",(run_id,)).fetchone()[0]==0,
        "product_fk":conn.execute("select count(*) from product_artists pa left join products p on p.id=pa.product_id where pa.created_by_run_id=%s and p.id is null",(run_id,)).fetchone()[0]==0,
        "late_dependency_plan":late_plan["proven"],
    }
    return {"proven":all(checks.values()),"status":run[0],"created":created,"freshness_restores":restored,"checks":checks,
            "rollback_order":list(DELETE_ORDER),"collection_run":1,"preexisting_rows_delete":0,"late_plan":late_plan}
