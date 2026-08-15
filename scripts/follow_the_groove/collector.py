#!/usr/bin/env python3
"""Strictly read-only Follow the Groove PILOTSET V1 collector.

This module prepares a deterministic plan. It deliberately exposes no database
write implementation; persistence belongs to a later, separately authorised step.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import time
import unicodedata
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

import psycopg
import requests

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


MB_BASE = "https://musicbrainz.org/ws/2"
LASTFM_BASE = "https://ws.audioscrobbler.com/2.0/"
DEFAULT_USER_AGENT = "VinylofyMasterdataWorker/1.0 (https://vinylofy.com; contact: info@vinylofy.com)"
PERSON_TYPE_ID = "b6e035f4-3ce9-331c-97df-83397230b0df"
GROUP_TYPE_ID = "e431f5f6-b5d2-343d-8b36-72607fffb74b"
MEMBER_OF_BAND_TYPE_ID = "5be4c609-9afa-4ea0-910b-12ffb71e3821"
TRANSIENT_STATUS = {429, 500, 502, 503, 504}

ALLOWED_RELATIONS = {
    "member of band": "membership",
    "instrument": "instrument",
    "performer": "instrument",
    "vocal": "vocal",
    "guest vocal": "vocal",
    "guest vocals": "vocal",
}
DENIED_RELATIONS = {
    "producer": "producer",
    "additional producer": "producer",
    "co-producer": "producer",
    "mixer": "technical_credit",
    "mastering": "technical_credit",
    "mastering engineer": "technical_credit",
    "recording engineer": "technical_credit",
    "engineer": "technical_credit",
    "songwriter": "composition",
    "writer": "composition",
    "lyricist": "composition",
    "composer": "composition",
    "lyricist and composer": "composition",
    "arranger": "arrangement",
    "cover": "cover_only",
    "cover recording of": "cover_only",
    "samples": "sample_only",
    "sampled by": "sample_only",
    "tribute": "tribute",
    "interpolation": "composition",
    "touring member": "touring_only",
    "live member": "live_only",
}

# These mirror the unique indexes in 20260814120000_add_follow_the_groove_v1.sql.
UPSERT_KEYS = {
    "artists": ("musicbrainz_artist_mbid",),
    "aliases": ("artist_mbid", "source_system", "alias_name", "alias_type", "locale", "begin_date", "end_date"),
    "edges": ("artist_low_mbid", "artist_high_mbid"),
    "membership": ("source_system", "relation_type_id", "source_mbid", "target_mbid", "begin_date", "end_date", "attribute_ids"),
    "artist_credit": ("source_system", "recording_mbid", "source_mbid", "target_mbid", "evidence_kind"),
    "performer": ("source_system", "evidence_kind", "recording_mbid", "relation_type_id", "source_mbid", "target_mbid", "attribute_ids"),
    "decision": ("source_system", "evidence_kind", "source_entity_kind", "source_entity_id", "source_mbid", "target_mbid", "relation_type_id", "recording_mbid", "work_mbid", "attribute_ids"),
    "similarity_resolved": ("source_system", "source_mbid", "target_mbid"),
    "similarity_unresolved": ("source_system", "source_mbid", "returned_target_name_normalized"),
    "product_artists": ("product_id", "artist_mbid"),
}

# A later run may only mark an identical row as seen again. Any difference in
# these fields is a conflict and must never become an in-place overwrite.
PERSISTENCE_IMMUTABLE_FIELDS = {
    "artists": ("musicbrainz_artist_mbid", "display_name", "entity_type", "musicbrainz_type_id", "wikidata_qid"),
    "aliases": UPSERT_KEYS["aliases"] + ("alias_normalized", "is_primary", "provenance"),
    "edges": UPSERT_KEYS["edges"],
    "membership": UPSERT_KEYS["membership"] + ("ended", "direction", "classification", "evidence_kind", "provenance"),
    "artist_credit": UPSERT_KEYS["artist_credit"] + ("release_mbid", "direction", "classification"),
    "performer": UPSERT_KEYS["performer"] + ("release_mbid", "direction", "classification"),
    "decision": UPSERT_KEYS["decision"] + ("classification", "reason"),
    "similarity_resolved": UPSERT_KEYS["similarity_resolved"] + ("returned_mbid", "resolution_status"),
    "similarity_unresolved": UPSERT_KEYS["similarity_unresolved"] + ("returned_mbid", "resolution_status"),
    "product_artists": UPSERT_KEYS["product_artists"] + ("credited_name", "credit_position", "source_system"),
}

NON_DESTRUCTIVE_SEEN_AGAIN_UPDATES = {
    "last_seen_run_id",
    "last_verified_at",
    "last_seen_at",
    "checked_at",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_name(value: str) -> str:
    """Existing application contract: lowercase, whitespace collapse, trim."""
    return re.sub(r"\s+", " ", value.lower()).strip()


def canonical_pair(left: str, right: str) -> tuple[str, str]:
    return tuple(sorted((left, right)))  # type: ignore[return-value]


def dedupe(rows: Iterable[dict[str, Any]], fields: Sequence[str]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    for row in rows:
        key = tuple(tuple(row.get(field) or ()) if isinstance(row.get(field), list) else row.get(field) for field in fields)
        if key not in seen:
            seen.add(key)
            result.append(row)
    return result


def plan_persistence_action(
    entity_kind: str,
    incoming: dict[str, Any],
    existing: dict[str, Any] | None,
) -> dict[str, Any]:
    """Classify a future persistence operation without mutating either row."""
    if entity_kind not in PERSISTENCE_IMMUTABLE_FIELDS:
        raise ValueError(f"Onbekend persistence entity kind: {entity_kind}")
    if existing is None:
        return {"action": "CREATE", "conflicts": {}}
    conflicts = {
        field: {"existing": existing.get(field), "incoming": incoming.get(field)}
        for field in PERSISTENCE_IMMUTABLE_FIELDS[entity_kind]
        if existing.get(field) != incoming.get(field)
    }
    if conflicts:
        return {"action": "CONFLICT", "conflicts": conflicts}
    return {
        "action": "SEEN_AGAIN",
        "conflicts": {},
        "allowed_updates": sorted(NON_DESTRUCTIVE_SEEN_AGAIN_UPDATES),
    }


def plan_rollback_row(
    *,
    created_by_run_id: str | None,
    last_seen_run_id: str | None,
    rollback_run_id: str,
    dependency_count: int,
) -> str:
    """Classify one row for rollback; this function never deletes anything."""
    if created_by_run_id != rollback_run_id:
        if last_seen_run_id == rollback_run_id:
            return "RETAIN_PREEXISTING_CLEAR_LAST_SEEN"
        return "RETAIN_PREEXISTING"
    if last_seen_run_id and last_seen_run_id != rollback_run_id:
        return "RETAIN_SHARED"
    if dependency_count > 0:
        return "RETAIN_SHARED"
    return "DELETE_CANDIDATE"


@dataclass(frozen=True)
class PilotArtist:
    name: str
    mbid: str
    entity_type: str


@dataclass(frozen=True)
class PilotConfig:
    version: str
    artists: tuple[PilotArtist, ...]
    evidence_releases: tuple[dict[str, str], ...]

    @property
    def by_mbid(self) -> dict[str, PilotArtist]:
        return {artist.mbid: artist for artist in self.artists}


@dataclass
class RunContext:
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    mode: str = "dry-run"
    started_at: str = field(default_factory=utc_now)
    completed_at: str | None = None
    counters: dict[str, int] = field(default_factory=dict)
    errors: list[dict[str, str]] = field(default_factory=list)

    def finish(self) -> None:
        self.completed_at = utc_now()


def load_pilot_config(path: Path) -> PilotConfig:
    raw = json.loads(path.read_text(encoding="utf-8"))
    artists = tuple(PilotArtist(**item) for item in raw["artists"])
    mbids = [artist.mbid for artist in artists]
    if len(artists) != 20 or len(set(mbids)) != 20:
        raise ValueError("PILOTSET V1 moet exact 20 unieke MBIDs bevatten")
    for artist in artists:
        uuid.UUID(artist.mbid)
        if artist.entity_type not in {"Person", "Group"}:
            raise ValueError(f"Ongeldig expected entity type voor {artist.mbid}")
    return PilotConfig(raw["version"], artists, tuple(raw.get("evidence_releases", [])))


class HttpJsonClient:
    def __init__(self, user_agent: str, timeout: int = 30, sleep_seconds: float = 1.1, retries: int = 3) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent, "Accept": "application/json"})
        self.timeout = timeout
        self.sleep_seconds = max(1.0, sleep_seconds)
        self.retries = max(0, retries)
        self._last_call = 0.0
        self._cache: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}
        self.request_count = 0
        self.retry_count = 0
        self.cache_hits = 0

    def get(self, url: str, params: dict[str, str]) -> dict[str, Any]:
        key = (url, tuple(sorted(params.items())))
        if key in self._cache:
            self.cache_hits += 1
            return self._cache[key]
        for attempt in range(self.retries + 1):
            elapsed = time.monotonic() - self._last_call
            if elapsed < self.sleep_seconds:
                time.sleep(self.sleep_seconds - elapsed)
            try:
                self.request_count += 1
                response = self.session.get(url, params=params, timeout=self.timeout)
                self._last_call = time.monotonic()
                if response.status_code in TRANSIENT_STATUS and attempt < self.retries:
                    self.retry_count += 1
                    time.sleep(min(8.0, 2.0**attempt))
                    continue
                response.raise_for_status()
                payload = response.json()
                self._cache[key] = payload
                return payload
            except (requests.Timeout, requests.ConnectionError):
                self._last_call = time.monotonic()
                if attempt >= self.retries:
                    raise
                self.retry_count += 1
                time.sleep(min(8.0, 2.0**attempt))
        raise AssertionError("unreachable")


class MusicBrainzClient:
    def __init__(self, http: HttpJsonClient) -> None:
        self.http = http

    def artist(self, mbid: str) -> dict[str, Any]:
        return self.http.get(
            f"{MB_BASE}/artist/{mbid}",
            {"fmt": "json", "inc": "aliases+url-rels+artist-rels"},
        )

    def release(self, mbid: str) -> dict[str, Any]:
        return self.http.get(
            f"{MB_BASE}/release/{mbid}",
            {"fmt": "json", "inc": "recordings+artist-credits+artist-rels+work-rels+release-groups"},
        )

    def recording(self, mbid: str) -> dict[str, Any]:
        return self.http.get(
            f"{MB_BASE}/recording/{mbid}",
            {"fmt": "json", "inc": "artist-credits+artist-rels+work-rels+releases"},
        )

    def work(self, mbid: str) -> dict[str, Any]:
        return self.http.get(
            f"{MB_BASE}/work/{mbid}",
            {"fmt": "json", "inc": "artist-rels"},
        )


def hydrate_release_recordings(
    client: MusicBrainzClient,
    release: dict[str, Any],
    pilot: PilotConfig,
) -> dict[str, Any]:
    """Fetch relation-bearing recording/work entities only for pilot credits."""
    pilot_mbids = set(pilot.by_mbid)
    for medium in release.get("media") or []:
        for track in medium.get("tracks") or []:
            recording = track.get("recording") or {}
            credit = track.get("artist-credit") or recording.get("artist-credit") or []
            credit_ids = {
                item.get("artist", {}).get("id")
                for item in credit
                if isinstance(item, dict) and item.get("artist", {}).get("id")
            }
            recording_mbid = recording.get("id")
            if not recording_mbid or not (credit_ids & pilot_mbids):
                continue
            detail = client.recording(recording_mbid)
            for relation in detail.get("relations") or []:
                if relation.get("target-type") != "work":
                    continue
                work = relation.get("work") or {}
                work_mbid = work.get("id")
                if work_mbid:
                    relation["work"] = client.work(work_mbid)
            track["recording"] = detail
    return release


class LastFmClient:
    def __init__(self, http: HttpJsonClient, api_key: str) -> None:
        self.http = http
        self.api_key = api_key

    def similar(self, name: str, limit: int) -> list[dict[str, Any]]:
        payload = self.http.get(
            LASTFM_BASE,
            {"method": "artist.getSimilar", "artist": name, "api_key": self.api_key, "format": "json", "limit": str(limit), "autocorrect": "0"},
        )
        return list((payload.get("similarartists") or {}).get("artist") or [])


def wikidata_qid(payload: dict[str, Any]) -> str | None:
    for relation in payload.get("relations") or []:
        if relation.get("target-type") == "url" and relation.get("type") == "wikidata":
            resource = ((relation.get("url") or {}).get("resource") or "").rstrip("/")
            candidate = resource.rsplit("/", 1)[-1]
            if re.fullmatch(r"Q[1-9][0-9]*", candidate):
                return candidate
    return None


def validate_identity(expected: PilotArtist, payload: dict[str, Any]) -> dict[str, Any]:
    actual_id = payload.get("id")
    actual_name = payload.get("name")
    actual_type = payload.get("type")
    expected_type_id = PERSON_TYPE_ID if expected.entity_type == "Person" else GROUP_TYPE_ID
    errors = []
    if actual_id != expected.mbid:
        errors.append("mbid_mismatch")
    if actual_name != expected.name:
        errors.append("canonical_name_mismatch")
    if actual_type != expected.entity_type:
        errors.append("entity_type_mismatch")
    if payload.get("type-id") != expected_type_id:
        errors.append("type_id_mismatch")
    return {
        "mbid": expected.mbid,
        "expected_name": expected.name,
        "actual_name": actual_name,
        "entity_type": actual_type,
        "type_id": payload.get("type-id"),
        "wikidata_qid": wikidata_qid(payload),
        "status": "resolved" if not errors else "rejected",
        "reasons": errors,
    }


def extract_aliases(artist_mbid: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for alias in payload.get("aliases") or []:
        name = str(alias.get("name") or "").strip()
        if not name:
            continue
        rows.append({
            "artist_mbid": artist_mbid,
            "alias_name": name,
            "alias_normalized": normalize_name(name),
            "source_system": "musicbrainz",
            "alias_type": alias.get("type"),
            "locale": alias.get("locale"),
            "is_primary": bool(alias.get("primary")),
            "begin_date": alias.get("begin"),
            "end_date": alias.get("end"),
            "provenance": {"musicbrainz_artist_mbid": artist_mbid},
        })
    return dedupe(rows, UPSERT_KEYS["aliases"])


def classify_relation(name: str | None, *, official_recording: bool = False) -> tuple[str, str, str]:
    normalized = normalize_name(name or "")
    if normalized in DENIED_RELATIONS:
        return "rejected", "rejected", DENIED_RELATIONS[normalized]
    if normalized in {"touring", "live"} or (normalized.startswith("live ") and not official_recording):
        return "insufficient", "insufficient", "live_or_touring_without_recording"
    if normalized in ALLOWED_RELATIONS:
        if normalized != "member of band" and not official_recording:
            return "insufficient", "insufficient", "missing_official_recording"
        return "allowed", ALLOWED_RELATIONS[normalized], "allowed_relation"
    return "insufficient", "insufficient", "relation_not_allowlisted"


def classify_external(target_mbid: str | None, pilot_mbids: set[str]) -> str:
    if not target_mbid:
        return "UNRESOLVED"
    return "PILOT_NODE" if target_mbid in pilot_mbids else "EXTERNAL_RELATED_NODE"


def extract_memberships(source: PilotArtist, payload: dict[str, Any], pilot: PilotConfig) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    evidence: list[dict[str, Any]] = []
    external: list[dict[str, Any]] = []
    by_mbid = pilot.by_mbid
    for relation in payload.get("relations") or []:
        if relation.get("target-type") != "artist" or normalize_name(relation.get("type") or "") != "member of band":
            continue
        target = relation.get("artist") or {}
        target_mbid = target.get("id")
        scope = classify_external(target_mbid, set(by_mbid))
        if scope != "PILOT_NODE":
            external.append({"source_mbid": source.mbid, "target_mbid": target_mbid, "target_name": target.get("name"), "classification": scope})
            continue
        target_expected = by_mbid[target_mbid]
        if source.entity_type == "Person" and target_expected.entity_type == "Group":
            person_mbid, group_mbid = source.mbid, target_mbid
        elif source.entity_type == "Group" and target_expected.entity_type == "Person":
            person_mbid, group_mbid = target_mbid, source.mbid
        else:
            continue
        evidence.append({
            "source_system": "musicbrainz",
            "source_entity_kind": "artist_relation",
            "source_entity_id": f"{source.mbid}:{relation.get('type-id')}:{target_mbid}",
            "source_mbid": person_mbid,
            "target_mbid": group_mbid,
            "relation_type_id": relation.get("type-id"),
            "source_relation_name": relation.get("type"),
            "direction": "source_to_target",
            "begin_date": relation.get("begin"),
            "end_date": relation.get("end"),
            "ended": relation.get("ended"),
            "attribute_ids": sorted((relation.get("attribute-ids") or {}).values()),
            "classification": "allowed",
            "evidence_kind": "membership",
            "provenance": {"fetched_for_mbid": source.mbid, "source_direction": relation.get("direction")},
        })
    return dedupe(evidence, UPSERT_KEYS["membership"]), dedupe(external, ("source_mbid", "target_mbid", "classification"))


def artist_credit_mbids(recording: dict[str, Any]) -> list[str]:
    return [item.get("artist", {}).get("id") for item in recording.get("artist-credit") or [] if isinstance(item, dict) and item.get("artist", {}).get("id")]


def extract_recording_evidence(release: dict[str, Any], pilot: PilotConfig) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    multicredits: list[dict[str, Any]] = []
    pilot_mbids = set(pilot.by_mbid)
    release_mbid = release.get("id")
    release_group_mbid = (release.get("release-group") or {}).get("id")
    for medium in release.get("media") or []:
        for track in medium.get("tracks") or []:
            recording = track.get("recording") or {}
            recording_mbid = recording.get("id")
            credit = track.get("artist-credit") or recording.get("artist-credit") or []
            credit_ids = [item.get("artist", {}).get("id") for item in credit if isinstance(item, dict) and item.get("artist", {}).get("id")]
            pilot_credit_ids = sorted(set(credit_ids) & pilot_mbids)
            if len(pilot_credit_ids) > 1:
                multicredits.append({"recording_mbid": recording_mbid, "release_mbid": release_mbid, "artist_mbids": pilot_credit_ids, "rendered_credit": "".join((str(item.get("name") or item.get("artist", {}).get("name") or "") + str(item.get("joinphrase") or "")) for item in credit if isinstance(item, dict)).strip()})
                for index, left in enumerate(pilot_credit_ids):
                    for right in pilot_credit_ids[index + 1 :]:
                        low, high = canonical_pair(left, right)
                        rows.append({"source_system": "musicbrainz", "source_entity_kind": "recording", "source_entity_id": recording_mbid, "source_mbid": low, "target_mbid": high, "classification": "allowed", "evidence_kind": "artist_credit", "direction": "symmetric", "recording_mbid": recording_mbid, "release_mbid": release_mbid, "release_group_mbid": release_group_mbid, "relation_type_id": None, "work_mbid": None, "attribute_ids": [], "source_relation_name": "artist credit"})
            for relation in recording.get("relations") or []:
                if relation.get("target-type") == "work":
                    work = relation.get("work") or {}
                    for work_relation in work.get("relations") or []:
                        if work_relation.get("target-type") != "artist":
                            continue
                        writer_mbid = (work_relation.get("artist") or {}).get("id")
                        if writer_mbid not in pilot_mbids:
                            continue
                        classification, _, reason = classify_relation(work_relation.get("type"))
                        if classification != "rejected":
                            continue
                        for credited_mbid in pilot_credit_ids:
                            if credited_mbid == writer_mbid:
                                continue
                            rows.append({"source_system": "musicbrainz", "source_entity_kind": "work_relation", "source_entity_id": work.get("id"), "source_mbid": writer_mbid, "target_mbid": credited_mbid, "classification": "rejected", "evidence_kind": "rejected", "direction": "source_to_target", "recording_mbid": recording_mbid, "release_mbid": release_mbid, "release_group_mbid": release_group_mbid, "relation_type_id": work_relation.get("type-id"), "work_mbid": work.get("id"), "attribute_ids": sorted((work_relation.get("attribute-ids") or {}).values()), "source_relation_name": work_relation.get("type"), "reason": reason})
                    continue
                if relation.get("target-type") != "artist":
                    continue
                performer = relation.get("artist") or {}
                performer_mbid = performer.get("id")
                if performer_mbid not in pilot_mbids:
                    continue
                classification, kind, reason = classify_relation(relation.get("type"), official_recording=bool(recording_mbid and release_mbid))
                for credited_mbid in pilot_credit_ids:
                    if credited_mbid == performer_mbid:
                        continue
                    low, high = canonical_pair(performer_mbid, credited_mbid)
                    rows.append({"source_system": "musicbrainz", "source_entity_kind": "recording", "source_entity_id": recording_mbid, "source_mbid": low if classification == "allowed" else performer_mbid, "target_mbid": high if classification == "allowed" else credited_mbid, "classification": classification, "evidence_kind": kind, "direction": "source_to_target", "recording_mbid": recording_mbid, "release_mbid": release_mbid, "release_group_mbid": release_group_mbid, "relation_type_id": relation.get("type-id"), "work_mbid": None, "attribute_ids": sorted((relation.get("attribute-ids") or {}).values()), "source_relation_name": relation.get("type"), "reason": reason})
    keys = {"membership": UPSERT_KEYS["membership"], "artist_credit": UPSERT_KEYS["artist_credit"], "instrument": UPSERT_KEYS["performer"], "vocal": UPSERT_KEYS["performer"], "rejected": UPSERT_KEYS["decision"], "insufficient": UPSERT_KEYS["decision"]}
    final: list[dict[str, Any]] = []
    for kind, group in _group_by(rows, "evidence_kind").items():
        final.extend(dedupe(group, keys[kind]))
    return final, dedupe(multicredits, ("recording_mbid", "artist_mbids"))


def _group_by(rows: Iterable[dict[str, Any]], field_name: str) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        result.setdefault(str(row[field_name]), []).append(row)
    return result


def derive_edges(evidence: Iterable[dict[str, Any]]) -> list[dict[str, str]]:
    pairs = {canonical_pair(row["source_mbid"], row["target_mbid"]) for row in evidence if row.get("classification") == "allowed"}
    return [{"artist_low_mbid": low, "artist_high_mbid": high} for low, high in sorted(pairs)]


def resolve_similarity(source: PilotArtist, returned: dict[str, Any], rank: int, pilot: PilotConfig) -> dict[str, Any]:
    returned_mbid = (returned.get("mbid") or "").strip() or None
    target_name = str(returned.get("name") or "").strip()
    status = "unresolved"
    resolved = None
    target_scope = "UNRESOLVED"
    if returned_mbid:
        try:
            uuid.UUID(returned_mbid)
        except ValueError:
            status = "conflict"
        else:
            target_scope = classify_external(returned_mbid, set(pilot.by_mbid))
            if target_scope == "PILOT_NODE":
                expected = pilot.by_mbid[returned_mbid]
                if normalize_name(target_name) == normalize_name(expected.name):
                    status, resolved = "resolved", returned_mbid
                else:
                    status = "conflict"
    return {"source_system": "lastfm", "source_mbid": source.mbid, "requested_source_name": source.name, "returned_target_name": target_name, "returned_target_name_normalized": normalize_name(target_name), "returned_mbid": returned_mbid, "match_score": str(returned.get("match") or "0"), "position": rank, "resolution_status": status, "target_mbid": resolved, "target_scope": target_scope}


def is_safe_product_alias(row: dict[str, Any]) -> bool:
    safe_alias_types = {"Artist name", "Legal name"}
    return bool(
        row.get("source_system") == "musicbrainz"
        and row.get("end_date") is None
        and (row.get("is_primary") or row.get("alias_type") in safe_alias_types)
    )


def proven_names(artist: PilotArtist, aliases: Iterable[dict[str, Any]]) -> set[str]:
    return {normalize_name(artist.name)} | {
        row["alias_normalized"]
        for row in aliases
        if row["artist_mbid"] == artist.mbid
        and is_safe_product_alias(row)
    }


def match_product_artist(value: str, artist: PilotArtist, aliases: Iterable[dict[str, Any]]) -> str:
    return "HIGH_CONFIDENCE" if normalize_name(value) in proven_names(artist, aliases) else "NO_MATCH"


def match_product_artist_route(value: str, artist: PilotArtist, aliases: Iterable[dict[str, Any]]) -> str | None:
    normalized = normalize_name(value)
    if normalized == normalize_name(artist.name):
        return "vinylofy_exact"
    if normalized in proven_names(artist, aliases) - {normalize_name(artist.name)}:
        return "musicbrainz_alias"
    return None


def classify_multi_artist(value: str, proven_credits: Iterable[dict[str, Any]]) -> tuple[str, list[str]]:
    normalized = normalize_name(value)
    for credit in proven_credits:
        if normalized == normalize_name(credit.get("rendered_credit") or ""):
            return "HIGH_CONFIDENCE_MULTI_CREDIT", list(credit["artist_mbids"])
    return "AMBIGUOUS", []


def is_pilot_relevant_multi_artist(
    value: str,
    pilot: PilotConfig,
    aliases: Iterable[dict[str, Any]],
) -> bool:
    components = {
        normalize_name(component)
        for component in re.split(r"\s+(?:&|feat\.?|featuring|x|\+)\s+|\s*,\s*", value, flags=re.IGNORECASE)
        if component.strip()
    }
    return any(components & proven_names(artist, aliases) for artist in pilot.artists)


def read_products(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor() as cursor:
        cursor.execute("set transaction read only")
        cursor.execute("select id::text, artist from public.products")
        return [{"id": str(row[0]), "artist": str(row[1])} for row in cursor.fetchall()]


def plan_product_matches(products: Iterable[dict[str, Any]], pilot: PilotConfig, aliases: list[dict[str, Any]], credits: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    exact: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    for product in products:
        matched: list[tuple[str, str]] = []
        for artist in pilot.artists:
            route = match_product_artist_route(product["artist"], artist, aliases)
            if route:
                matched.append((artist.mbid, route))
        if matched:
            exact.extend({"product_id": product["id"], "artist_mbid": mbid, "credited_name": product["artist"], "source_system": route} for mbid, route in matched)
            continue
        normalized_product_artist = normalize_name(product["artist"])
        unsafe_alias_candidates = sorted({
            row["artist_mbid"]
            for row in aliases
            if row["alias_normalized"] == normalized_product_artist
            and not is_safe_product_alias(row)
        })
        if unsafe_alias_candidates:
            ambiguous.append({"product_id": product["id"], "artist": product["artist"], "reason": "unsafe_musicbrainz_alias", "candidate_mbids": unsafe_alias_candidates})
            continue
        if re.search(r"\s(?:&|feat\.?|featuring|x|,|\+)\s", product["artist"], re.IGNORECASE) and is_pilot_relevant_multi_artist(product["artist"], pilot, aliases):
            status, mbids = classify_multi_artist(product["artist"], credits)
            if status == "HIGH_CONFIDENCE_MULTI_CREDIT":
                exact.extend({"product_id": product["id"], "artist_mbid": mbid, "credited_name": product["artist"], "source_system": "musicbrainz_artist_credit"} for mbid in mbids)
            else:
                ambiguous.append({"product_id": product["id"], "artist": product["artist"], "reason": "unproven_multi_artist_credit"})
    return dedupe(exact, UPSERT_KEYS["product_artists"]), dedupe(ambiguous, ("product_id", "reason"))


class PersistenceUnavailable(RuntimeError):
    pass


class PersistencePlan:
    """Future interface marker. Step 2 intentionally cannot persist anything."""

    @staticmethod
    def persist(*_args: Any, **_kwargs: Any) -> None:
        raise PersistenceUnavailable("Databasewrites zijn niet geïmplementeerd; gebruik uitsluitend --dry-run")


def rollback_assessment() -> dict[str, Any]:
    return {
        "status": "PROVEN_CONTRACT",
        "message": "ROLLBACK-BY-RUN = PROVEN voor het niet-destructieve MVP-contract",
        "reason": "created_by_run_id identificeert run-owned rows; last_seen_run_id identificeert pre-existing rows die alleen opnieuw zijn gezien; inhoudsconflicten worden niet overschreven",
        "safe_future_actions": [
            "delete run-created leaf rows after dependency checks",
            "clear only last_seen_run_id on pre-existing rows seen by the rolled-back run; never delete those rows",
            "delete shared artists only when no remaining dependency exists",
            "retain the collection_run when retained shared rows still reference it as creator",
            "delete the collection_run only after all references are removed",
        ],
        "unsafe_actions": [
            "blind delete shared artists",
            "overwrite canonical identity or immutable relation content",
            "delete a pre-existing row with only last_seen_run_id equal to the rolled-back run",
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Follow the Groove PILOTSET V1 read-only collector")
    parser.add_argument("--pilot", type=Path, default=Path(__file__).with_name("pilot_v1.json"))
    parser.add_argument("--dry-run", action="store_true", required=True)
    parser.add_argument("--smoke-mbid", action="append", default=[])
    parser.add_argument("--skip-recordings", action="store_true")
    parser.add_argument("--skip-lastfm", action="store_true")
    parser.add_argument("--lastfm-limit", type=int, default=5)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--include-plan", action="store_true", help="Neem het volledige machineleesbare plan op; standaard blijft de uitvoer compact")
    return parser


def run(args: argparse.Namespace) -> dict[str, Any]:
    if load_dotenv:
        load_dotenv(".env.local", override=False)
    pilot = load_pilot_config(args.pilot)
    selected = set(args.smoke_mbid) or set(pilot.by_mbid)
    unknown = selected - set(pilot.by_mbid)
    if unknown:
        raise SystemExit(f"Smoke MBID niet in pilot: {sorted(unknown)}")
    http = HttpJsonClient(os.getenv("MUSICBRAINZ_USER_AGENT", DEFAULT_USER_AGENT))
    mb = MusicBrainzClient(http)
    context = RunContext()
    identities: list[dict[str, Any]] = []
    aliases: list[dict[str, Any]] = []
    memberships: list[dict[str, Any]] = []
    external: list[dict[str, Any]] = []
    for artist in pilot.artists:
        if artist.mbid not in selected:
            continue
        try:
            payload = mb.artist(artist.mbid)
            identity = validate_identity(artist, payload)
            identities.append(identity)
            if identity["status"] == "resolved":
                aliases.extend(extract_aliases(artist.mbid, payload))
                evidence, outside = extract_memberships(artist, payload, pilot)
                memberships.extend(evidence)
                external.extend(outside)
        except Exception as exc:  # compact audit output; no raw response/secrets
            context.errors.append({"source": "musicbrainz", "mbid": artist.mbid, "error": f"{type(exc).__name__}: {exc}"})
    recording_rows: list[dict[str, Any]] = []
    credits: list[dict[str, Any]] = []
    if not args.skip_recordings and not args.smoke_mbid:
        for seed in pilot.evidence_releases:
            try:
                release = hydrate_release_recordings(mb, mb.release(seed["mbid"]), pilot)
                rows, release_credits = extract_recording_evidence(release, pilot)
                recording_rows.extend(rows)
                credits.extend(release_credits)
            except Exception as exc:
                context.errors.append({"source": "musicbrainz_release", "mbid": seed["mbid"], "error": f"{type(exc).__name__}: {exc}"})
    similarities: list[dict[str, Any]] = []
    api_key = os.getenv("LASTFM_API_KEY", "")
    lastfm_http: HttpJsonClient | None = None
    if not args.skip_lastfm and api_key:
        lastfm_http = HttpJsonClient(DEFAULT_USER_AGENT, sleep_seconds=1.0)
        lastfm = LastFmClient(lastfm_http, api_key)
        for artist in pilot.artists:
            if artist.mbid not in selected:
                continue
            try:
                similarities.extend(resolve_similarity(artist, row, rank, pilot) for rank, row in enumerate(lastfm.similar(artist.name, args.lastfm_limit), 1))
            except Exception as exc:
                context.errors.append({"source": "lastfm", "mbid": artist.mbid, "error": f"{type(exc).__name__}: {exc}"})
    products: list[dict[str, Any]] = []
    database_url = os.getenv("DATABASE_URL", "")
    if database_url and not args.smoke_mbid:
        with psycopg.connect(database_url) as conn:
            products = read_products(conn)
            conn.rollback()
    all_evidence = dedupe(memberships, UPSERT_KEYS["membership"]) + recording_rows
    product_matches, ambiguous_products = plan_product_matches(products, pilot, aliases, credits)
    edges = derive_edges(all_evidence)
    context.counters = {
        "pilot_artists_requested": len(selected),
        "artist_identities_resolved": sum(row["status"] == "resolved" for row in identities),
        "artist_identities_rejected": sum(row["status"] == "rejected" for row in identities),
        "aliases": len(dedupe(aliases, UPSERT_KEYS["aliases"])),
        "membership_evidence": len([row for row in all_evidence if row["evidence_kind"] == "membership"]),
        "recording_evidence": len([row for row in all_evidence if row["evidence_kind"] != "membership" and row["classification"] == "allowed"]),
        "rejected_evidence": len([row for row in all_evidence if row["classification"] == "rejected"]),
        "factual_edges": len(edges),
        "lastfm_similarity_rows": len(similarities),
        "unresolved_lastfm_targets": len([row for row in similarities if row["resolution_status"] != "resolved"]),
        "product_high_confidence": len(product_matches),
        "product_ambiguous": len(ambiguous_products),
        "external_related_nodes": len(dedupe(external, ("target_mbid", "classification"))),
        "errors": len(context.errors),
        "musicbrainz_http_requests": http.request_count,
        "musicbrainz_retries": http.retry_count,
        "musicbrainz_cache_hits": http.cache_hits,
        "lastfm_http_requests": lastfm_http.request_count if lastfm_http else 0,
        "lastfm_retries": lastfm_http.retry_count if lastfm_http else 0,
    }
    context.finish()
    result = {
        "run": asdict(context),
        "rollback": rollback_assessment(),
        "counts": context.counters,
        "examples": {
            "rejected_identities": [row for row in identities if row["status"] == "rejected"][:10],
            "rejected_evidence": [row for row in all_evidence if row["classification"] != "allowed"][:10],
            "unresolved_similarity": [row for row in similarities if row["resolution_status"] != "resolved"][:10],
            "ambiguous_products": ambiguous_products[:10],
            "external_related_nodes": external[:10],
        },
    }
    if args.include_plan:
        result["plan"] = {
            "artists": identities,
            "aliases": dedupe(aliases, UPSERT_KEYS["aliases"]),
            "evidence": all_evidence,
            "edges": edges,
            "similarities": similarities,
            "product_artists": product_matches,
            "multi_credits": dedupe(credits, ("recording_mbid", "artist_mbids")),
            "ambiguous_products": ambiguous_products,
            "external_related_nodes": dedupe(external, ("target_mbid", "classification")),
        }
    return result


def main() -> int:
    args = build_parser().parse_args()
    result = run(args)
    rendered = json.dumps(result, ensure_ascii=False, indent=2, default=lambda value: str(value) if isinstance(value, Decimal) else value)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0 if not result["run"]["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
