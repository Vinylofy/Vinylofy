#!/usr/bin/env python3
"""Run a prioritized, bounded FTG collector pilot without database writes."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.follow_the_groove import coverage_priorities, generic_collector


def summarize_source(source: dict[str, Any]) -> dict[str, Any]:
    plans = source.get("plans") or {}
    actions = source.get("counters", {}).get("actions") or {}
    new_product_links = [
        row for row in plans.get("product_artists", []) if row.get("action") == "CREATE"
    ]
    resolved_similarity_mbids = sorted({
        row["target_mbid"] for row in plans.get("similarities", [])
        if row.get("target_mbid")
    })
    unsafe_product_sources = sorted({
        str(row.get("source_system") or "<missing>") for row in new_product_links
        if row.get("source_system") != "musicbrainz_artist_credit"
    })
    conflicts = sum(
        row.get("action") == "CONFLICT"
        for rows in plans.values() for row in rows
    )
    rollback_proven = source.get("rollback", {}).get("status") == "PROVEN"
    return {
        "artist_mbid": source["source"]["mbid"],
        "name": source["source"]["display_name"],
        "status": source.get("status"),
        "rollback_proven": rollback_proven,
        "conflicts": conflicts,
        "new_similarities": actions.get("similarities", {}).get("CREATE", 0),
        "new_edges": actions.get("edges", {}).get("CREATE", 0),
        "new_product_links": len(new_product_links),
        "new_product_target_mbids": sorted({
            row["artist_mbid"] for row in new_product_links if row.get("artist_mbid")
        }),
        "new_artist_mbids": sorted({
            row["musicbrainz_artist_mbid"] for row in plans.get("artists", [])
            if row.get("action") == "CREATE" and row.get("musicbrainz_artist_mbid")
        }),
        "unsafe_product_sources": unsafe_product_sources,
        "resolved_similarity_targets": len(resolved_similarity_mbids),
        "resolved_similarity_mbids": resolved_similarity_mbids,
        "preflight_safe": (
            source.get("status") == "succeeded"
            and rollback_proven and conflicts == 0 and not unsafe_product_sources
        ),
        "errors": source.get("errors") or [],
    }


def run(*, min_products: int, limit: int, lastfm_limit: int = 25,
        max_direct_targets: int = 25) -> dict[str, Any]:
    if not 1 <= limit <= generic_collector.MAX_SOURCES:
        raise ValueError(f"limit must be 1..{generic_collector.MAX_SOURCES}")
    if not 1 <= lastfm_limit <= generic_collector.MAX_LASTFM_LIMIT:
        raise ValueError(f"lastfm-limit must be 1..{generic_collector.MAX_LASTFM_LIMIT}")
    if max_direct_targets < 1:
        raise ValueError("max-direct-targets must be positive")
    priorities = coverage_priorities.run(min_products=min_products, limit=limit)
    selected = priorities["artists"]
    if not selected:
        return {"mode": "dry-run", "generated_at": datetime.now(timezone.utc).isoformat(),
                "database_writes": 0, "sources": [], "selected_sources": [],
                "collector_limits": {"lastfm_limit": lastfm_limit,
                                     "max_direct_targets": max_direct_targets},
                "api_counters": {}}
    source_mbids = [row["artist_mbid"] for row in selected]
    collector_args = argparse.Namespace(
        dry_run=True, write=False, frontier=False, refresh=True,
        max_sources=len(source_mbids), max_direct_targets=max_direct_targets,
        lastfm_limit=lastfm_limit, graph_depth=1,
        source_mbid=source_mbids, recording_release_seed=[],
        execution_id=None, output=None,
    )
    collected = generic_collector.run(collector_args)
    if collected.get("mode") != "dry-run":
        raise RuntimeError("collector did not return a dry-run")
    actual_mbids = [row["source"]["mbid"] for row in collected["sources"]]
    if actual_mbids != source_mbids:
        raise RuntimeError("collector source set changed during pilot")
    return {
        "mode": "dry-run",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_writes": 0,
        "heuristic_only": True,
        "collector_limits": {"lastfm_limit": lastfm_limit,
                             "max_direct_targets": max_direct_targets},
        "selected_sources": selected,
        "api_counters": collected.get("api_counters", {}),
        "sources": [summarize_source(row) for row in collected["sources"]],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-products", type=int, default=10)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--lastfm-limit", type=int, default=25)
    parser.add_argument("--max-direct-targets", type=int, default=25)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = run(min_products=args.min_products, limit=args.limit,
                 lastfm_limit=args.lastfm_limit,
                 max_direct_targets=args.max_direct_targets)
    output = json.dumps(report, ensure_ascii=False, indent=2)
    if args.output:
        args.output.write_text(output + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
