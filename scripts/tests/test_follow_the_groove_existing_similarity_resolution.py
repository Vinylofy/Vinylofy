from __future__ import annotations

import argparse
import unittest
from datetime import datetime, timezone

from scripts.follow_the_groove import resolve_existing_similarity as subject
from scripts.follow_the_groove.collector import GROUP_TYPE_ID


SOURCE = "10000000-0000-4000-8000-000000000001"
TARGET = "20000000-0000-4000-8000-000000000002"
SOURCE_MBID = "30000000-0000-4000-8000-000000000003"
TARGET_MBID = "40000000-0000-4000-8000-000000000004"
SIMILARITY = "50000000-0000-4000-8000-000000000005"
UPDATED = datetime(2026, 9, 1, tzinfo=timezone.utc)


def candidate(**changes):
    values = {
        "id": SIMILARITY,
        "source_artist_id": SOURCE,
        "source_mbid": SOURCE_MBID,
        "source_system": "lastfm",
        "returned_target_name": "Example Band",
        "returned_target_name_normalized": "example band",
        "returned_mbid": TARGET_MBID,
        "match_score": "0.8",
        "position": 2,
        "resolution_status": "unresolved",
        "target_artist_id": None,
        "last_seen_run_id": None,
        "updated_at": UPDATED,
        "checked_at": UPDATED,
        "matched_artist_id": TARGET,
        "matched_artist_mbid": TARGET_MBID,
        "matched_artist_name": "Example Band",
        "matched_artist_type": "group",
        "matched_artist_type_id": GROUP_TYPE_ID,
    }
    values.update(changes)
    return subject.Candidate(**values)


class FakeConnection:
    def __init__(self, existing=()):
        self.existing = list(existing)
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self

    def fetchall(self):
        return self.existing

    @property
    def rowcount(self):
        return 1

    def fetchone(self):
        return (1,)


class ExistingSimilarityResolutionTest(unittest.TestCase):
    def test_only_exact_known_identity_is_resolvable(self):
        item = candidate(returned_target_name="  EXAMPLE   BAND ")
        self.assertEqual(subject.classify(item, False), "resolve")
        self.assertEqual(subject.classify(item, True), "resolved_pair_exists")
        self.assertEqual(subject.classify(candidate(returned_mbid=SOURCE_MBID), False), "mbid_mismatch")
        self.assertEqual(subject.classify(candidate(returned_target_name="Other Band"), False), "name_mismatch")
        self.assertEqual(subject.classify(candidate(returned_target_name_normalized="other"), False), "stored_name_mismatch")
        self.assertEqual(subject.classify(candidate(matched_artist_type_id=SOURCE_MBID), False), "artist_type_mismatch")
        self.assertEqual(subject.classify(candidate(matched_artist_id=SOURCE), False), "self_similarity")
        self.assertEqual(subject.classify(candidate(resolution_status="resolved", target_artist_id=TARGET), True), "already_resolved")

    def test_plan_rejects_an_existing_or_duplicate_pair(self):
        first = candidate()
        second = candidate(id="60000000-0000-4000-8000-000000000006")
        planned = subject.plan(FakeConnection(), [first, second])
        self.assertEqual([row["action"] for row in planned], ["resolve", "resolved_pair_exists"])
        self.assertEqual(planned[0]["preimage"]["resolution_status"], "unresolved")
        existing = subject.plan(FakeConnection(existing=[(SOURCE, TARGET)]), [first])
        self.assertEqual(existing[0]["action"], "resolved_pair_exists")

    def test_write_requires_explicit_bounded_ids(self):
        args = argparse.Namespace(write=True, dry_run=False, limit=25, similarity_id=[], after_id=None)
        with self.assertRaisesRegex(ValueError, "explicit --similarity-id"):
            subject.validate_args(args)
        args.similarity_id = [SIMILARITY, SIMILARITY]
        with self.assertRaisesRegex(ValueError, "unique"):
            subject.validate_args(args)
        args.similarity_id = [SIMILARITY]
        self.assertEqual(subject.validate_args(args), [SIMILARITY])
        args.limit = 26
        with self.assertRaisesRegex(ValueError, "1..25"):
            subject.validate_args(args)

    def test_write_rejects_any_non_resolvable_row_before_insert(self):
        conn = FakeConnection()
        with self.assertRaisesRegex(subject.ResolutionBlocked, "unproven"):
            subject.write_plan(conn, [candidate()], [{"action": "name_mismatch"}])
        self.assertEqual(conn.calls, [])

    def test_write_records_preimage_and_checks_exact_postcondition(self):
        conn = FakeConnection()
        item = candidate()
        planned = subject.plan(FakeConnection(), [item])
        run_id = subject.write_plan(conn, [item], planned)
        self.assertTrue(run_id)
        self.assertEqual(len(conn.calls), 3)
        self.assertIn("follow_the_groove_collection_runs", conn.calls[0][0])
        update_sql, update_params = conn.calls[1]
        self.assertIn("where id=%s and source_artist_id=%s", update_sql)
        self.assertIn("last_seen_run_id is not distinct from %s", update_sql)
        self.assertEqual(update_params[2], SIMILARITY)
        self.assertIn("select count(*)", conn.calls[2][0])

    def test_read_query_is_bounded_and_write_query_locks_only_similarity(self):
        conn = FakeConnection(existing=[tuple(candidate().__dict__.values())])
        self.assertEqual(subject.load_candidates(conn, 1, [], None, lock=False), [candidate()])
        self.assertIn("limit %s", conn.calls[0][0])
        self.assertNotIn("for update", conn.calls[0][0])
        conn = FakeConnection(existing=[tuple(candidate().__dict__.values())])
        subject.load_candidates(conn, 1, [SIMILARITY], None, lock=True)
        self.assertIn("for update of si", conn.calls[0][0])


if __name__ == "__main__":
    unittest.main()
