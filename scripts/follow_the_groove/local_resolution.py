#!/usr/bin/env python3
"""Resolve frozen local-catalog FTG similarity targets, transactionally.

This deliberately narrow persistence path accepts only the audited Phase 5
Step 1B/1C artifacts. It never calls external APIs and never touches edges,
evidence, aliases, products, or non-FTG tables.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


STEP1B_SHA256 = "5811e6aaf578056aa6ac52619f7c7d271efb8e7d43907680a4e4e1b9a1acd47c"
STEP1C_SHA256 = "bc5f57cf9a3e3efe169b0f8853f2bbe92197f7a2d221b035beb2dd877f87b2bd"
CATEGORY_A = "RESOLVABLE_EXISTING_VINYLOFY_MUSICBRAINZ_IDENTITY"
PERSON_TYPE_ID = "b6e035f4-3ce9-331c-97df-83397230b0df"
GROUP_TYPE_ID = "e431f5f6-b5d2-343d-8b36-72607fffb74b"
EXPECTED_BEFORE = {
    "follow_the_groove_collection_runs": 2,
    "artists": 20,
    "artist_aliases": 193,
    "artist_edges": 14,
    "artist_relation_evidence": 45,
    "artist_similarity": 95,
    "product_artists": 492,
}
EXPECTED_AFTER = {**EXPECTED_BEFORE, "follow_the_groove_collection_runs": 3, "artists": 46, "product_artists": 653}


class ResolutionBlocked(RuntimeError):
    pass


def normalize_name(value: str) -> str:
    return re.sub(r"\s+", " ", value.lower()).strip()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_frozen(path: Path, expected: str) -> dict[str, Any]:
    actual = sha256(path)
    if actual != expected:
        raise ResolutionBlocked(f"artifact hash mismatch for {path}: {actual}")
    return json.loads(path.read_text(encoding="utf-8"))


def classify_category(row: dict[str, Any]) -> str:
    return "ALLOW" if row.get("classification") == CATEGORY_A else "SKIP"


def validate_target(target: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if not target.get("mbid"):
        reasons.append("missing_mbid")
    try:
        uuid.UUID(str(target.get("mbid")))
    except (ValueError, TypeError, AttributeError):
        reasons.append("invalid_mbid")
    if not str(target.get("canonical_name") or "").strip():
        reasons.append("missing_canonical_name")
    entity_type = target.get("entity_type")
    type_id = target.get("musicbrainz_type_id")
    if entity_type not in {"Person", "Group"}:
        reasons.append("ambiguous_type")
    expected_type_id = PERSON_TYPE_ID if entity_type == "Person" else GROUP_TYPE_ID if entity_type == "Group" else None
    if type_id != expected_type_id:
        reasons.append("type_id_mismatch")
    if int(target.get("local_product_count") or 0) < 1:
        reasons.append("no_local_mbid_credit")
    return reasons


def classify_artist(target: dict[str, Any], existing: dict[str, Any] | None, name_collisions: list[dict[str, Any]]) -> str:
    if validate_target(target) or name_collisions:
        return "CONFLICT"
    if existing is None:
        return "CREATE"
    expected = {
        "musicbrainz_artist_mbid": target["mbid"],
        "display_name": target["canonical_name"],
        "entity_type": target["entity_type"].lower(),
        "musicbrainz_type_id": target["musicbrainz_type_id"],
        "wikidata_qid": target.get("wikidata_qid"),
    }
    return "SEEN_AGAIN" if all(existing.get(key) == value for key, value in expected.items()) else "CONFLICT"


def classify_similarity(
    current: dict[str, Any],
    target_artist_id: str,
    target_mbid: str,
    resolved_collision: bool,
    identity_name_valid: bool = True,
) -> str:
    if not identity_name_valid:
        return "CONFLICT"
    if resolved_collision and not (
        current.get("resolution_status") == "resolved" and current.get("target_artist_id") == target_artist_id
    ):
        return "CONFLICT"
    if current.get("resolution_status") == "unresolved" and current.get("target_artist_id") is None:
        return "RESOLVE" if current.get("returned_mbid") == target_mbid else "CONFLICT"
    if current.get("resolution_status") == "resolved" and current.get("target_artist_id") == target_artist_id:
        return "ALREADY_RESOLVED"
    return "CONFLICT"


def classify_product_link(incoming: dict[str, Any], existing: dict[str, Any] | None, local_credit_valid: bool) -> str:
    if not local_credit_valid:
        return "CONFLICT"
    if existing is None:
        return "CREATE"
    fields = ("credited_name", "credit_position", "source_system")
    return "SEEN_AGAIN" if all(existing.get(field) == incoming.get(field) for field in fields) else "CONFLICT"


def rollback_preimage(current: dict[str, Any]) -> dict[str, Any]:
    return {
        "similarity_id": current["id"],
        "target_artist_id": current.get("target_artist_id"),
        "resolution_status": current["resolution_status"],
        "last_seen_run_id": current.get("last_seen_run_id"),
        "updated_at": current["updated_at"],
    }


def direct_artist_credits(detail: Any) -> list[dict[str, Any]]:
    if not isinstance(detail, dict) or not isinstance(detail.get("artist-credit"), list):
        return []
    result = []
    for position, credit in enumerate(detail["artist-credit"], 1):
        artist = credit.get("artist") if isinstance(credit, dict) else None
        if isinstance(artist, dict) and artist.get("id"):
            result.append({
                "mbid": artist["id"],
                "canonical_name": artist.get("name"),
                "credited_name": credit.get("name") or artist.get("name"),
                "entity_type": artist.get("type"),
                "musicbrainz_type_id": artist.get("type-id"),
                "credit_position": position,
            })
    return result


@dataclass
class LivePlan:
    result: dict[str, Any]
    target_artist_ids: dict[str, str]


def table_counts(conn: psycopg.Connection[Any]) -> dict[str, int]:
    return {table: conn.execute(f"select count(*) from {table}").fetchone()[0] for table in EXPECTED_BEFORE}


def build_live_plan(conn: psycopg.Connection[Any], step1b: dict[str, Any], frozen: dict[str, Any]) -> LivePlan:
    category_a = [row for row in step1b["rows"] if row["classification"] == CATEGORY_A]
    protected = [row for row in step1b["rows"] if row["classification"] != CATEGORY_A]
    if len(category_a) != 30 or len({row["resolved_identity_mbid"] for row in category_a}) != 26 or len(protected) != 48:
        raise ResolutionBlocked("frozen category counts changed")
    targets = frozen["target_artists"]
    target_by_mbid = {row["mbid"]: row for row in targets}
    product_rows = frozen["product_artist_plan"]
    similarity_rows = frozen["similarity_resolution_plan"]
    if len(targets) != 26 or len(product_rows) != 161 or len(similarity_rows) != 30:
        raise ResolutionBlocked("frozen write scope changed")

    existing_rows = conn.execute(
        "select id::text, musicbrainz_artist_mbid::text, display_name, entity_type, musicbrainz_type_id::text, wikidata_qid "
        "from artists"
    ).fetchall()
    by_mbid = {
        row[1]: {"id": row[0], "musicbrainz_artist_mbid": row[1], "display_name": row[2], "entity_type": row[3],
                 "musicbrainz_type_id": row[4], "wikidata_qid": row[5]}
        for row in existing_rows
    }
    by_name: dict[str, list[dict[str, Any]]] = {}
    for artist in by_mbid.values():
        by_name.setdefault(normalize_name(artist["display_name"]), []).append(artist)

    artist_plan = []
    target_ids = {}
    for target in targets:
        existing = by_mbid.get(target["mbid"])
        collisions = [row for row in by_name.get(normalize_name(target["canonical_name"]), []) if row["musicbrainz_artist_mbid"] != target["mbid"]]
        action = classify_artist(target, existing, collisions)
        target_id = existing["id"] if existing else str(uuid.uuid4())
        target_ids[target["mbid"]] = target_id
        artist_plan.append({**target, "action": action, "artist_id": target_id, "conflicting_mbids": [row["musicbrainz_artist_mbid"] for row in collisions]})

    product_ids = sorted({row["product_id"] for row in product_rows})
    db_products = conn.execute(
        "select id::text, metadata_raw->'release_detail' from products where id = any(%s::uuid[])", (product_ids,)
    ).fetchall()
    credits = {
        (product_id, credit["mbid"]): credit
        for product_id, detail in db_products
        for credit in direct_artist_credits(detail)
    }
    existing_links = conn.execute(
        "select pa.product_id::text, a.musicbrainz_artist_mbid::text, pa.credited_name, pa.credit_position, pa.source_system "
        "from product_artists pa join artists a on a.id=pa.artist_id "
        "where a.musicbrainz_artist_mbid = any(%s::uuid[])", ([row["mbid"] for row in targets],)
    ).fetchall()
    links = {(row[0], row[1]): {"credited_name": row[2], "credit_position": row[3], "source_system": row[4]} for row in existing_links}
    product_plan = []
    for item in product_rows:
        key = (item["product_id"], item["target_mbid"])
        credit = credits.get(key)
        target = target_by_mbid[item["target_mbid"]]
        valid = bool(
            credit
            and credit["credited_name"] == item["credited_name"]
            and credit["credit_position"] == item["credit_position"]
            and credit["canonical_name"] == target["canonical_name"]
            and credit["entity_type"] == target["entity_type"]
            and credit["musicbrainz_type_id"] == target["musicbrainz_type_id"]
        )
        incoming = {**item, "source_system": "musicbrainz_artist_credit"}
        action = classify_product_link(incoming, links.get(key), valid)
        product_plan.append({**incoming, "action": action, "artist_id": target_ids[item["target_mbid"]], "product_artist_id": str(uuid.uuid4())})

    sim_ids = [row["similarity_id"] for row in similarity_rows]
    db_sims = conn.execute(
        "select id::text, source_artist_id::text, target_artist_id::text, created_by_run_id::text, last_seen_run_id::text, "
        "source_system, requested_source_name, returned_target_name, returned_target_name_normalized, returned_mbid::text, "
        "match_score::text, position, resolution_status, created_at::text, updated_at::text, checked_at::text "
        "from artist_similarity where id = any(%s::uuid[])", (sim_ids,)
    ).fetchall()
    db_sim_by_id = {
        row[0]: {"id": row[0], "source_artist_id": row[1], "target_artist_id": row[2], "created_by_run_id": row[3],
                 "last_seen_run_id": row[4], "source_system": row[5], "requested_source_name": row[6],
                 "returned_target_name": row[7], "returned_target_name_normalized": row[8], "returned_mbid": row[9],
                 "match_score": row[10], "position": row[11], "resolution_status": row[12], "created_at": row[13],
                 "updated_at": row[14], "checked_at": row[15]}
        for row in db_sims
    }
    if len(db_sim_by_id) != 30:
        raise ResolutionBlocked("not all frozen similarity rows exist")
    similarity_plan = []
    for item in similarity_rows:
        current = db_sim_by_id[item["similarity_id"]]
        target_id = target_ids[item["target_mbid"]]
        collision = conn.execute(
            "select exists(select 1 from artist_similarity where source_system=%s and source_artist_id=%s::uuid "
            "and target_artist_id=%s::uuid and resolution_status='resolved' and id<>%s::uuid)",
            (current["source_system"], current["source_artist_id"], target_id, current["id"]),
        ).fetchone()[0]
        accepted_names = {normalize_name(target_by_mbid[item["target_mbid"]]["canonical_name"])}
        accepted_names.update(
            normalize_name(row["credited_name"])
            for row in product_rows
            if row["target_mbid"] == item["target_mbid"]
        )
        action = classify_similarity(
            current,
            target_id,
            item["target_mbid"],
            collision,
            normalize_name(current["returned_target_name"]) in accepted_names,
        )
        similarity_plan.append({**item, "action": action, "target_artist_id": target_id, "current": current,
                                "rollback_preimage": rollback_preimage(current)})

    summaries = {
        "artists": _actions(artist_plan),
        "similarities": _actions(similarity_plan),
        "product_artists": _actions(product_plan),
        "protected_non_a": len(protected),
    }
    conflicts = sum(group.get("CONFLICT", 0) for group in summaries.values() if isinstance(group, dict))
    return LivePlan({"summaries": summaries, "conflicts": conflicts, "artists": artist_plan,
                     "similarities": similarity_plan, "product_artists": product_plan}, target_ids)


def _actions(rows: list[dict[str, Any]]) -> dict[str, int]:
    result: dict[str, int] = {}
    for row in rows:
        result[row["action"]] = result.get(row["action"], 0) + 1
    return result


def exact_first_write(plan: LivePlan) -> bool:
    return plan.result["summaries"] == {
        "artists": {"CREATE": 26},
        "similarities": {"RESOLVE": 30},
        "product_artists": {"CREATE": 161},
        "protected_non_a": 48,
    } and plan.result["conflicts"] == 0


def exact_idempotent(plan: LivePlan) -> bool:
    return plan.result["summaries"] == {
        "artists": {"SEEN_AGAIN": 26},
        "similarities": {"ALREADY_RESOLVED": 30},
        "product_artists": {"SEEN_AGAIN": 161},
        "protected_non_a": 48,
    } and plan.result["conflicts"] == 0


def persist_r3(conn: psycopg.Connection[Any], plan: LivePlan, input_sha: str, plan_sha: str) -> str:
    if not exact_first_write(plan):
        raise ResolutionBlocked("live plan is not the exact approved R3 scope")
    run_id = str(uuid.uuid4())
    conn.execute(
        "insert into follow_the_groove_collection_runs "
        "(id,collector,source_system,scope,status,counters,error_summary) values (%s,%s,%s,%s,'running','{}'::jsonb,'[]'::jsonb)",
        (run_id, "ftg_local_resolution", "vinylofy_musicbrainz_metadata", "local_catalog_identity_resolution_v1"),
    )
    created_artist_ids = []
    for row in plan.result["artists"]:
        conn.execute(
            "insert into artists (id,musicbrainz_artist_mbid,display_name,entity_type,musicbrainz_type_id,wikidata_qid,created_by_run_id,last_seen_run_id) "
            "values (%s,%s,%s,%s,%s,%s,%s,%s)",
            (row["artist_id"], row["mbid"], row["canonical_name"], row["entity_type"].lower(),
             row["musicbrainz_type_id"], row.get("wikidata_qid"), run_id, run_id),
        )
        created_artist_ids.append(row["artist_id"])
    created_link_ids = []
    for row in plan.result["product_artists"]:
        conn.execute(
            "insert into product_artists (id,product_id,artist_id,credited_name,credit_position,source_system,created_by_run_id,last_seen_run_id) "
            "values (%s,%s,%s,%s,%s,%s,%s,%s)",
            (row["product_artist_id"], row["product_id"], row["artist_id"], row["credited_name"],
             row["credit_position"], row["source_system"], run_id, run_id),
        )
        created_link_ids.append(row["product_artist_id"])
    preimages = []
    for row in plan.result["similarities"]:
        current = row["current"]
        result = conn.execute(
            "update artist_similarity set target_artist_id=%s, resolution_status='resolved', last_seen_run_id=%s, updated_at=now() "
            "where id=%s and target_artist_id is null and resolution_status='unresolved' and returned_mbid=%s "
            "and last_seen_run_id is not distinct from %s and updated_at=%s::timestamptz",
            (row["target_artist_id"], run_id, row["similarity_id"], row["target_mbid"],
             current["last_seen_run_id"], current["updated_at"]),
        )
        if result.rowcount != 1:
            raise ResolutionBlocked(f"similarity preimage changed: {row['similarity_id']}")
        preimages.append(row["rollback_preimage"])
    counters = {
        "input_artifact_sha256": input_sha,
        "resolution_plan_sha256": plan_sha,
        "category_a_similarity_ids": [row["similarity_id"] for row in plan.result["similarities"]],
        "target_mbids": [row["mbid"] for row in plan.result["artists"]],
        "similarity_preimages": preimages,
        "created_artist_ids": created_artist_ids,
        "created_product_artist_ids": created_link_ids,
        "bridge_proof": [{"product_id": row["product_id"], "target_mbid": row["target_mbid"],
                          "credited_name": row["credited_name"], "credit_position": row["credit_position"]}
                         for row in plan.result["product_artists"]],
        "counts": {"artists_created": 26, "similarities_resolved": 30, "product_artists_created": 161,
                   "aliases_created": 0, "conflicts": 0, "non_a_skipped": 48},
    }
    conn.execute(
        "update follow_the_groove_collection_runs set status='succeeded', counters=%s::jsonb, finished_at=now() where id=%s",
        (json.dumps(counters), run_id),
    )
    return run_id


def precommit_audit(conn: psycopg.Connection[Any], run_id: str, before_products: int) -> dict[str, Any]:
    counts = table_counts(conn)
    products = conn.execute("select count(*) from products").fetchone()[0]
    checks = {
        "counts": counts == EXPECTED_AFTER,
        "products_unchanged": products == before_products,
        "r3_artists": conn.execute("select count(*) from artists where created_by_run_id=%s", (run_id,)).fetchone()[0] == 26,
        "r3_product_links": conn.execute("select count(*) from product_artists where created_by_run_id=%s", (run_id,)).fetchone()[0] == 161,
        "r3_last_seen_similarity": conn.execute("select count(*) from artist_similarity where last_seen_run_id=%s and resolution_status='resolved'", (run_id,)).fetchone()[0] == 30,
        "duplicate_products": conn.execute("select count(*) from (select product_id,artist_id from product_artists group by 1,2 having count(*)>1) x").fetchone()[0] == 0,
        "bad_product_fk": conn.execute("select count(*) from product_artists pa left join products p on p.id=pa.product_id where p.id is null").fetchone()[0] == 0,
        "edges_unchanged": counts["artist_edges"] == 14,
        "evidence_unchanged": counts["artist_relation_evidence"] == 45,
    }
    if not all(checks.values()):
        raise ResolutionBlocked(f"precommit audit failed: {checks}")
    return {"counts": counts, "products": products, "checks": checks}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Bounded FTG local catalog identity resolution")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--apply-r3", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    if load_dotenv:
        load_dotenv(".env.local", override=False)
    step1b = load_frozen(args.input, STEP1B_SHA256)
    frozen = load_frozen(args.plan, STEP1C_SHA256)
    conn = psycopg.connect(os.environ["DATABASE_URL"], autocommit=False)
    run_id = None
    try:
        if args.dry_run:
            conn.execute("begin read only")
            plan = build_live_plan(conn, step1b, frozen)
            result = {"mode": "dry-run", "database_counts": table_counts(conn), **plan.result}
            conn.rollback()
        else:
            conn.execute("begin isolation level serializable")
            conn.execute("lock table follow_the_groove_collection_runs, artists, artist_similarity, product_artists in share row exclusive mode")
            before = table_counts(conn)
            before_products = conn.execute("select count(*) from products").fetchone()[0]
            if before != EXPECTED_BEFORE:
                raise ResolutionBlocked(f"database baseline drift: {before}")
            plan = build_live_plan(conn, step1b, frozen)
            run_id = persist_r3(conn, plan, STEP1B_SHA256, STEP1C_SHA256)
            audit = precommit_audit(conn, run_id, before_products)
            conn.commit()
            result = {"mode": "apply-r3", "run_id": run_id, "transaction": "COMMIT", "plan": plan.result["summaries"], "audit": audit}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    if args.output:
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return result


def main() -> int:
    args = build_parser().parse_args()
    result = run(args)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
