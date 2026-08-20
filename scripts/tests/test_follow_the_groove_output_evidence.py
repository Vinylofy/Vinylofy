from __future__ import annotations

import argparse
import inspect
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from scripts.follow_the_groove import output_evidence as subject


ARTIST = subject.Artist("10000000-0000-0000-0000-000000000001", "20000000-0000-0000-0000-000000000002", "Fixture")
NOW = datetime(2026, 8, 20, tzinfo=timezone.utc)


def credit(entity_id: str) -> dict:
    return {"id": entity_id, "artist-credit": [{"artist": {"id": ARTIST.mbid}}]}


class FakeClient:
    def __init__(self, replies):
        self.replies = iter(replies)
        self.request_count = 0
        self.retry_count = 0
        self.cache_hits = 0

    def get(self, _url, _params):
        self.request_count += 1
        return next(self.replies)


class FakeConnection:
    def __init__(self):
        self.commands = []
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self): return self
    def __exit__(self, *_args): return False
    def execute(self, sql, params=None):
        self.commands.append((sql, params))
        return self
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1


class OutputEvidenceTest(unittest.TestCase):
    def test_local_release_credit_is_proven_output(self):
        detail = credit("30000000-0000-0000-0000-000000000003")
        evidence = subject.local_release_evidence(ARTIST, detail, NOW)
        self.assertEqual(subject.classify(evidence), "proven_output")
        self.assertEqual(evidence.evidence_type, "release_primary_artist")

    def test_local_recording_artist_credit_is_proven_output(self):
        row = {"source_artist_id": ARTIST.id, "target_artist_id": "40000000-0000-0000-0000-000000000004",
               "evidence_kind": "artist_credit", "classification": "allowed",
               "recording_mbid": "50000000-0000-0000-0000-000000000005"}
        evidence = subject.local_recording_evidence(ARTIST, row, NOW)
        self.assertEqual(subject.classify(evidence), "proven_output")
        self.assertEqual(evidence.evidence_type, "recording_artist")

    def test_zero_product_musicbrainz_release_group_is_proven_output(self):
        client = FakeClient([{"release-groups": [credit("60000000-0000-0000-0000-000000000006")] }])
        evidence = subject.lookup_musicbrainz(ARTIST, client, NOW)
        self.assertEqual(subject.classify(evidence), "proven_output")
        self.assertEqual(client.request_count, 1)

    def test_no_positive_evidence_is_unknown_and_bounded(self):
        client = FakeClient([{"release-groups": []}, {"releases": []}, {"recordings": []}])
        self.assertEqual(subject.classify(subject.lookup_musicbrainz(ARTIST, client, NOW)), "unknown")
        self.assertEqual(client.request_count, 3)

    def test_membership_only_is_not_output_or_bridge_proof(self):
        self.assertEqual(subject.classify(None), "unknown")
        self.assertNotEqual(subject.classify(None), "proven_bridge_only")

    def test_product_count_is_not_a_classifier(self):
        source = inspect.getsource(subject)
        self.assertNotIn("productCount", source)
        self.assertNotIn("product_count", source)

    def test_duplicate_evidence_prevention_and_idempotency(self):
        item = subject.Evidence(ARTIST.id, ARTIST.mbid, "recording_artist", "musicbrainz", "recording",
                                "70000000-0000-0000-0000-000000000007", {}, NOW)
        self.assertEqual(subject.dedupe_evidence([item, item]), [item])

    def test_existing_unknown_is_not_requeried_without_refresh(self):
        args = argparse.Namespace(dry_run=True, write=False, batch_size=1, after_mbid=None,
                                  artist_mbid=[], pilot=False, refresh=False, output=None)
        conn = FakeConnection()
        client = FakeClient([])
        with patch.dict(subject.os.environ, {"DATABASE_URL": "postgres://fixture"}), \
             patch.object(subject.psycopg, "connect", return_value=conn), \
             patch.object(subject, "select_artists", return_value=[ARTIST]), \
             patch.object(subject, "load_existing", return_value=({}, set(), {ARTIST.id: "unknown"})), \
             patch.object(subject, "load_local_evidence", return_value=[]):
            report = subject.run(args, client=client)
        self.assertEqual(report["api_calls"], 0)
        self.assertEqual(report["expected_writes"]["total"], 0)

    def test_dry_run_performs_zero_writes(self):
        args = argparse.Namespace(dry_run=True, write=False, batch_size=1, after_mbid=None,
                                  artist_mbid=[], pilot=False, refresh=False, output=None)
        conn = FakeConnection()
        client = FakeClient([{"release-groups": []}, {"releases": []}, {"recordings": []}])
        with patch.dict(subject.os.environ, {"DATABASE_URL": "postgres://fixture"}), \
             patch.object(subject.psycopg, "connect", return_value=conn), \
             patch.object(subject, "select_artists", return_value=[ARTIST]), \
             patch.object(subject, "load_existing", return_value=({}, set(), {})), \
             patch.object(subject, "load_local_evidence", return_value=[]), \
             patch.object(subject, "persist") as persist:
            report = subject.run(args, client=client)
        persist.assert_not_called()
        self.assertEqual(report["database_writes"], 0)
        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.rollbacks, 1)

    def test_no_product_mutation_or_unrelated_metadata_writes(self):
        source = inspect.getsource(subject.persist).lower()
        for forbidden in ("update products", "insert into products", "delete from products", "metadata_raw"):
            self.assertNotIn(forbidden, source)

    def test_persistence_uses_conflict_safe_evidence_upsert(self):
        source = inspect.getsource(subject.persist).lower()
        self.assertIn("on conflict(artist_id,evidence_type,source_entity_kind,source_entity_id)", source)

    def test_no_unrelated_table_writes(self):
        source = inspect.getsource(subject.persist).lower()
        self.assertNotIn("artist_relation_evidence", source)
        self.assertNotIn("artist_similarity", source)
        self.assertNotIn("product_artists", source)

    def test_batch_size_hard_limit(self):
        args = argparse.Namespace(dry_run=True, write=False, batch_size=26, after_mbid=None,
                                  artist_mbid=[], pilot=False, refresh=False, output=None)
        with self.assertRaisesRegex(ValueError, "1..25"):
            subject.run(args)


class OutputEvidenceMigrationTest(unittest.TestCase):
    def test_schema_contract_and_unique_evidence(self):
        root = Path(__file__).resolve().parents[2]
        sql = (root / "supabase/migrations/20260820143000_add_ftg_artist_output_evidence.sql").read_text().lower()
        self.assertIn("create table public.artist_output_evidence", sql)
        self.assertIn("create table public.artist_output_status", sql)
        self.assertIn("'proven_output', 'proven_bridge_only', 'unknown'", sql)
        self.assertIn("unique (artist_id, evidence_type, source_entity_kind, source_entity_id)", sql)
        self.assertNotRegex(sql, r"(?:alter|update|insert into|delete from) public\.products")


if __name__ == "__main__":
    unittest.main()
