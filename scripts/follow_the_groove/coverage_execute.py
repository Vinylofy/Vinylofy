#!/usr/bin/env python3
"""Apply an explicit FTG coverage pilot and verify its rendered standalone cards.

The pilot is a shortlist, never authority to publish an unreviewed source. This
command re-collects each explicit source through the existing transactional
collector, proves only positive output evidence, and measures the existing UI.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

import psycopg

from scripts.follow_the_groove import generic_collector, output_evidence, persistence

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


MAX_WRITE_SOURCES = 5


class CardLinkParser(HTMLParser):
    def __init__(self, source_mbid: str):
        super().__init__()
        self.source_mbid = source_mbid
        self.targets: set[str] = set()

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if not href:
            return
        parts = urlsplit(href)
        if parts.scheme or parts.netloc:
            return
        segments = parts.path.strip("/").split("/")
        if len(segments) != 3 or segments[:2] != ["follow-the-groove", self.source_mbid]:
            return
        try:
            self.targets.add(str(uuid.UUID(segments[2])))
        except ValueError:
            return


def validate_base_url(value: str) -> str:
    parts = urlsplit(value)
    if parts.scheme != "http" or parts.hostname not in {"localhost", "127.0.0.1"}:
        raise ValueError("base-url must be a local HTTP server")
    if parts.username or parts.password or parts.path not in {"", "/"} or parts.query or parts.fragment:
        raise ValueError("base-url must contain only a local origin")
    return value.rstrip("/")


def fetch_cards(base_url: str, source_mbid: str) -> list[str]:
    url = f"{validate_base_url(base_url)}/follow-the-groove/{source_mbid}"
    request = Request(url, headers={"User-Agent": "Vinylofy-FTG-Coverage-Audit/1.0"})
    with urlopen(request, timeout=45) as response:
        if response.status != 200:
            raise RuntimeError(f"FTG page returned HTTP {response.status}")
        body = response.read(2_000_001)
    if len(body) > 2_000_000:
        raise RuntimeError("FTG page exceeds audit size limit")
    parser = CardLinkParser(source_mbid)
    parser.feed(body.decode("utf-8"))
    if len(parser.targets) > 5:
        raise RuntimeError("rendered FTG card count exceeds V1 limit")
    return sorted(parser.targets)


def validate_pilot(pilot: dict[str, Any], requested: list[str]) -> dict[str, int]:
    if not 1 <= len(requested) <= MAX_WRITE_SOURCES:
        raise ValueError(f"write requires 1..{MAX_WRITE_SOURCES} unique explicit sources")
    requested[:] = [str(uuid.UUID(value)) for value in requested]
    if len(set(requested)) != len(requested):
        raise ValueError("write requires unique explicit sources")
    if pilot.get("mode") != "dry-run" or pilot.get("database_writes") != 0:
        raise ValueError("pilot must be a zero-write dry-run")
    selected = {row["artist_mbid"] for row in pilot.get("selected_sources", [])}
    rows = {row["artist_mbid"]: row for row in pilot.get("sources", [])}
    if len(rows) != len(pilot.get("sources", [])) or not set(requested) <= selected & rows.keys():
        raise ValueError("requested source is missing or duplicated in pilot")
    for mbid in requested:
        row = rows[mbid]
        if not row.get("preflight_safe") or row.get("conflicts") or row.get("unsafe_product_sources"):
            raise ValueError(f"source {mbid} is not preflight-safe")
        if row.get("new_product_links", 0) and not row.get("new_product_target_mbids"):
            raise ValueError(f"source {mbid} has untracked product targets")
        if row.get("resolved_similarity_targets", 0) and not row.get("resolved_similarity_mbids"):
            raise ValueError(f"source {mbid} has untracked similarity targets")
        for field in ("new_artist_mbids", "new_product_target_mbids", "resolved_similarity_mbids"):
            for target in row.get(field, []):
                uuid.UUID(target)
    limits = pilot.get("collector_limits") or {}
    lastfm = limits.get("lastfm_limit")
    direct = limits.get("max_direct_targets")
    if not isinstance(lastfm, int) or not 1 <= lastfm <= generic_collector.MAX_LASTFM_LIMIT:
        raise ValueError("pilot has invalid Last.fm limit")
    if not isinstance(direct, int) or not 1 <= direct <= 25:
        raise ValueError("pilot has invalid direct-target limit")
    return {"lastfm_limit": lastfm, "max_direct_targets": direct}


def audit_collector(result: dict[str, Any], requested: list[str]) -> list[str]:
    if result.get("mode") == "existing-execution":
        if result.get("status") != "succeeded":
            raise RuntimeError(f"collector execution requires recovery: {result.get('status')}")
        counters = result.get("counters") or {}
        sources = counters.get("selected_sources") or []
        writes = counters.get("writes") or []
        skipped = counters.get("skipped_sources") or []
    elif result.get("mode") == "write" and result.get("status") == "succeeded":
        sources = result.get("selected_sources") or []
        writes = result.get("writes") or []
        skipped = result.get("skipped_sources") or []
    else:
        raise RuntimeError("collector write did not complete successfully")
    if [row["mbid"] for row in sources] != requested or len(writes) != len(requested) or skipped:
        raise RuntimeError("collector write source set or write count differs from pilot")
    for write in writes:
        post = write.get("postcommit") or {}
        if not post.get("proven") or post.get("status") != "succeeded":
            raise RuntimeError("collector postcommit is not proven")
        checks = write.get("checks") or {}
        post_checks = post.get("checks") or {}
        if not checks or not post_checks or not all(checks.values()) or not all(post_checks.values()):
            raise RuntimeError("collector postcommit check failed")
    return [write["run_id"] for write in writes]


def reconcile_committed_execution(database_url: str, execution_id: str,
                                  requested: list[str], limits: dict[str, int]) -> dict[str, Any]:
    """Recover only a fully committed batch whose post-commit audit was interrupted."""
    lock = generic_collector.acquire_write_lock(database_url)
    try:
        with psycopg.connect(database_url, autocommit=False, prepare_threshold=None) as conn:
            conn.execute("begin read only")
            control = conn.execute(
                "select status,counters from follow_the_groove_collection_runs "
                "where id=%s and collector=%s",
                (execution_id, generic_collector.BATCH_COLLECTOR),
            ).fetchone()
            if control is None or control[0] != "running":
                raise RuntimeError("execution is not an interrupted running batch")
            config = (control[1] or {}).get("config") or {}
            if (config.get("max_sources") != len(requested)
                    or config.get("lastfm_limit") != limits["lastfm_limit"]
                    or config.get("max_direct_targets") != limits["max_direct_targets"]):
                raise RuntimeError("interrupted execution configuration differs from pilot")
            rows = conn.execute(
                "select id::text,status,counters from follow_the_groove_collection_runs "
                "where collector=%s and counters->>'execution_id'=%s",
                (generic_collector.GENERIC_COLLECTOR, execution_id),
            ).fetchall()
            by_mbid = {row[2].get("source_artist_mbid"): row for row in rows}
            if len(rows) != len(requested) or set(by_mbid) != set(requested):
                raise RuntimeError("interrupted execution has an incomplete source set")
            writes = []
            for mbid in requested:
                run_id, status, counters = by_mbid[mbid]
                checks = counters.get("precommit_checks") or {}
                if status != "succeeded" or not checks or not all(checks.values()):
                    raise RuntimeError(f"source {mbid} did not commit with proven prechecks")
                post = persistence.audit_source_run(conn, run_id)
                if not post.get("proven") or post.get("status") != "succeeded":
                    raise RuntimeError(f"source {mbid} failed independent postcommit audit")
                writes.append({"run_id": run_id, "checks": checks, "postcommit": post})
            conn.rollback()
        generic_collector.update_execution_state(
            database_url, execution_id, status="succeeded",
            counters={"execution_id": execution_id, "config": config,
                      "selected_sources": [{"mbid": mbid} for mbid in requested],
                      "source_run_ids": [row["run_id"] for row in writes],
                      "writes": writes, "skipped_sources": []},
        )
        return {"mode": "existing-execution", "status": "succeeded",
                "counters": {"selected_sources": [{"mbid": mbid} for mbid in requested],
                             "writes": writes, "skipped_sources": []}}
    finally:
        lock.close()


def evidence_args(mbids: list[str], *, write: bool, refresh: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        dry_run=not write, write=write, batch_size=len(mbids),
        after_mbid=None, artist_mbid=mbids, pilot=False,
        reachable_missing_status=False, refresh=refresh,
        require_proven_output=write, output=None,
    )


def prove_targets(mbids: list[str]) -> dict[str, Any]:
    proven: set[str] = set()
    unknown: set[str] = set()
    writes = 0
    for offset in range(0, len(mbids), output_evidence.MAX_BATCH_SIZE):
        batch = mbids[offset:offset + output_evidence.MAX_BATCH_SIZE]
        preview = output_evidence.run(evidence_args(batch, write=False))
        if preview["artists_processed"] != len(batch) or preview["conflicts"]:
            raise RuntimeError("output evidence target set changed or conflicted")
        ready = [row["artist_mbid"] for row in preview["artists"]
                 if row["status"] == "proven_output" and row["write_required"]]
        retry = [row["artist_mbid"] for row in preview["artists"]
                 if row["status"] != "proven_output"]
        if ready:
            result = output_evidence.run(evidence_args(ready, write=True))
            if result["artists_processed"] != len(ready) or result["classification_counts"]["proven_output"] != len(ready):
                raise RuntimeError("positive output evidence write changed classification")
            writes += result["database_writes"]
        newly_ready: list[str] = []
        if retry:
            refreshed = output_evidence.run(evidence_args(retry, write=False, refresh=True))
            if refreshed["artists_processed"] != len(retry) or refreshed["conflicts"]:
                raise RuntimeError("refreshed output evidence target set changed")
            newly_ready = [row["artist_mbid"] for row in refreshed["artists"]
                           if row["status"] == "proven_output"]
            if newly_ready:
                result = output_evidence.run(evidence_args(newly_ready, write=True, refresh=True))
                if result["artists_processed"] != len(newly_ready) or result["classification_counts"]["proven_output"] != len(newly_ready):
                    raise RuntimeError("refreshed output write changed classification")
                writes += result["database_writes"]
            unknown.update(set(retry) - set(newly_ready))
        proven.update(row["artist_mbid"] for row in preview["artists"] if row["status"] == "proven_output")
        proven.update(newly_ready)
    for offset in range(0, len(proven), output_evidence.MAX_BATCH_SIZE):
        batch = sorted(proven)[offset:offset + output_evidence.MAX_BATCH_SIZE]
        post = output_evidence.run(evidence_args(batch, write=False))
        if (post["artists_processed"] != len(batch)
                or post["classification_counts"]["proven_output"] != len(batch)
                or post["expected_writes"]["total"] != 0):
            raise RuntimeError("positive output evidence postcondition failed")
    return {"targets_checked": len(mbids), "proven": sorted(proven),
            "unknown": sorted(unknown), "database_writes": writes,
            "postcondition_verified": True}


def save_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    temporary.replace(path)


def run(*, pilot_file: Path, source_mbids: list[str], execution_id: str,
        base_url: str, output: Path) -> dict[str, Any]:
    execution_id = str(uuid.UUID(execution_id))
    base_url = validate_base_url(base_url)
    pilot_bytes = pilot_file.read_bytes()
    pilot = json.loads(pilot_bytes)
    limits = validate_pilot(pilot, source_mbids)
    pilot_hash = hashlib.sha256(pilot_bytes).hexdigest()
    previous = json.loads(output.read_text()) if output.exists() else None
    if previous is not None and (
        previous.get("execution_id") != execution_id or
        previous.get("pilot_sha256") != pilot_hash or
        previous.get("source_mbids") != source_mbids
    ):
        raise ValueError("output journal belongs to a different execution")
    if previous is not None and previous.get("status") == "complete":
        return previous
    report = previous or {
        "execution_id": execution_id, "pilot_sha256": pilot_hash,
        "source_mbids": source_mbids, "status": "preflight",
        "before": {}, "after": {}, "collector_run_ids": [],
        "evidence": {}, "scope": "standalone_ftg_cards",
    }
    if load_dotenv:
        load_dotenv(".env", override=False)
        load_dotenv(".env.local", override=False)
    if not os.environ.get("DATABASE_URL"):
        raise RuntimeError("DATABASE_URL is required")
    pilot_rows = {row["artist_mbid"]: row for row in pilot["sources"]}
    try:
        for mbid in source_mbids:
            if mbid not in report["before"]:
                report["before"][mbid] = fetch_cards(base_url, mbid)
        report["status"] = "before_verified"
        save_report(output, report)
        collector_args = argparse.Namespace(
            dry_run=False, write=True, frontier=False, refresh=True,
            max_sources=len(source_mbids), max_direct_targets=limits["max_direct_targets"],
            lastfm_limit=limits["lastfm_limit"], graph_depth=1,
            source_mbid=source_mbids, recording_release_seed=[],
            execution_id=execution_id, output=None,
        )
        collection = generic_collector.run(collector_args)
        if collection.get("mode") == "existing-execution" and collection.get("status") == "recovery_required":
            collection = reconcile_committed_execution(
                os.environ["DATABASE_URL"], execution_id, source_mbids, limits,
            )
        report["collector_run_ids"] = audit_collector(collection, source_mbids)
        report["status"] = "collector_audited"
        save_report(output, report)
        target_mbids = sorted({
            mbid for source in source_mbids
            for field in ("new_artist_mbids", "new_product_target_mbids", "resolved_similarity_mbids")
            for mbid in pilot_rows[source].get(field, [])
        })
        report["evidence"] = prove_targets(target_mbids)
        report["status"] = "evidence_audited"
        save_report(output, report)
        for mbid in source_mbids:
            report["after"][mbid] = fetch_cards(base_url, mbid)
        report["changes"] = {mbid: {
            "before": len(report["before"][mbid]),
            "after": len(report["after"][mbid]),
            "delta": len(report["after"][mbid]) - len(report["before"][mbid]),
        } for mbid in source_mbids}
        if any(change["delta"] < 0 for change in report["changes"].values()):
            raise RuntimeError("rendered FTG card count decreased")
        if "error" in report:
            report["recovered_error"] = report.pop("error")
        report["status"] = "complete"
        save_report(output, report)
        return report
    except Exception as exc:
        report["status"] = "needs_review"
        report["error"] = f"{type(exc).__name__}: {exc}"
        save_report(output, report)
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pilot-file", type=Path, required=True)
    parser.add_argument("--source-mbid", action="append", required=True)
    parser.add_argument("--execution-id", required=True)
    parser.add_argument("--base-url", default="http://127.0.0.1:3001")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = run(pilot_file=args.pilot_file, source_mbids=args.source_mbid,
                 execution_id=args.execution_id, base_url=args.base_url,
                 output=args.output)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
