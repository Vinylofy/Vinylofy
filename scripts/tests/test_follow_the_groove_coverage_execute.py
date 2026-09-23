from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from scripts.follow_the_groove import coverage_execute as subject


SOURCE = "10000000-0000-0000-0000-000000000001"
TARGET = "20000000-0000-0000-0000-000000000002"
OTHER = "30000000-0000-0000-0000-000000000003"
EXECUTION = "40000000-0000-0000-0000-000000000004"


def pilot() -> dict:
    return {
        "mode": "dry-run", "database_writes": 0,
        "collector_limits": {"lastfm_limit": 25, "max_direct_targets": 25},
        "selected_sources": [{"artist_mbid": SOURCE}],
        "sources": [{"artist_mbid": SOURCE, "preflight_safe": True, "conflicts": 0,
                     "unsafe_product_sources": [], "new_product_links": 1,
                     "new_product_target_mbids": [TARGET],
                     "resolved_similarity_targets": 1,
                     "resolved_similarity_mbids": [TARGET]}],
    }


def collector_result() -> dict:
    return {
        "mode": "write", "status": "succeeded", "skipped_sources": [],
        "selected_sources": [{"mbid": SOURCE}],
        "writes": [{"run_id": OTHER, "checks": {"products_unchanged": True},
                    "postcommit": {"proven": True, "status": "succeeded",
                                   "checks": {"created_counts": True}}}],
    }


class CoverageExecuteTest(unittest.TestCase):
    def test_reconcile_only_marks_fully_audited_committed_batch_succeeded(self):
        class Connection:
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def execute(self, sql, _params=None):
                self.rows = ("running", {"config": {"max_sources": 1, "lastfm_limit": 25,
                                                       "max_direct_targets": 25}}) if "where id=%s" in sql else [
                    (OTHER, "succeeded", {"source_artist_mbid": SOURCE,
                                          "precommit_checks": {"products_unchanged": True}})]
                return self
            def fetchone(self): return self.rows
            def fetchall(self): return self.rows
            def rollback(self): pass
        lock = Mock()
        with patch.object(subject.generic_collector, "acquire_write_lock", return_value=lock), \
             patch.object(subject.psycopg, "connect", return_value=Connection()), \
             patch.object(subject.persistence, "audit_source_run", return_value={
                 "proven": True, "status": "succeeded", "checks": {"created_counts": True}}), \
             patch.object(subject.generic_collector, "update_execution_state") as update:
            result = subject.reconcile_committed_execution(
                "postgres://fixture", EXECUTION, [SOURCE],
                {"lastfm_limit": 25, "max_direct_targets": 25},
            )
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["counters"]["writes"][0]["run_id"], OTHER)
        self.assertEqual(update.call_args.kwargs["status"], "succeeded")
        lock.close.assert_called_once()

    def test_reconcile_rejects_unproven_source_without_status_update(self):
        class Connection:
            def __enter__(self): return self
            def __exit__(self, *_args): return False
            def execute(self, sql, _params=None):
                self.rows = ("running", {"config": {"max_sources": 1, "lastfm_limit": 25,
                                                       "max_direct_targets": 25}}) if "where id=%s" in sql else [
                    (OTHER, "failed", {"source_artist_mbid": SOURCE,
                                       "precommit_checks": {"products_unchanged": True}})]
                return self
            def fetchone(self): return self.rows
            def fetchall(self): return self.rows
        with patch.object(subject.generic_collector, "acquire_write_lock", return_value=Mock()), \
             patch.object(subject.psycopg, "connect", return_value=Connection()), \
             patch.object(subject.generic_collector, "update_execution_state") as update:
            with self.assertRaisesRegex(RuntimeError, "did not commit"):
                subject.reconcile_committed_execution(
                    "postgres://fixture", EXECUTION, [SOURCE],
                    {"lastfm_limit": 25, "max_direct_targets": 25},
                )
        update.assert_not_called()

    def test_card_parser_counts_only_exact_source_destination_anchors(self):
        parser = subject.CardLinkParser(SOURCE)
        parser.feed(f'<a href="/follow-the-groove/{SOURCE}/{TARGET}">yes</a>'
                    f'<a href="/follow-the-groove/{SOURCE}/{TARGET}">duplicate</a>'
                    f'<a href="/follow-the-groove/{OTHER}/{TARGET}">other source</a>'
                    f'<a href="/follow-the-groove/{SOURCE}">breadcrumb</a>')
        self.assertEqual(parser.targets, {TARGET})

    def test_base_url_must_be_local(self):
        for url in ("https://localhost:3001", "http://example.com", "http://localhost:3001/path"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                subject.validate_base_url(url)
        self.assertEqual(subject.validate_base_url("http://127.0.0.1:3001/"), "http://127.0.0.1:3001")

    def test_pilot_rejects_unreviewed_or_untracked_source(self):
        changed = pilot()
        changed["sources"][0]["preflight_safe"] = False
        with self.assertRaisesRegex(ValueError, "not preflight-safe"):
            subject.validate_pilot(changed, [SOURCE])
        changed = pilot()
        changed["sources"][0]["new_product_target_mbids"] = []
        with self.assertRaisesRegex(ValueError, "untracked product"):
            subject.validate_pilot(changed, [SOURCE])

    def test_collector_audit_requires_nonempty_positive_checks(self):
        self.assertEqual(subject.audit_collector(collector_result(), [SOURCE]), [OTHER])
        bad = collector_result()
        bad["writes"][0]["postcommit"]["checks"] = {}
        with self.assertRaisesRegex(RuntimeError, "check failed"):
            subject.audit_collector(bad, [SOURCE])

    def test_existing_execution_is_audited_not_rewritten(self):
        prior = collector_result()
        replay = {"mode": "existing-execution", "status": "succeeded",
                  "counters": {"selected_sources": prior["selected_sources"],
                               "writes": prior["writes"], "skipped_sources": []}}
        self.assertEqual(subject.audit_collector(replay, [SOURCE]), [OTHER])

    def test_evidence_only_writes_proven_targets_and_refreshes_unknown(self):
        results = [
            {"artists_processed": 2, "conflicts": 0, "artists": [
                {"artist_mbid": TARGET, "status": "proven_output", "source": "local", "write_required": True},
                {"artist_mbid": OTHER, "status": "unknown", "source": "musicbrainz", "write_required": True}]},
            {"artists_processed": 1, "classification_counts": {"proven_output": 1}, "database_writes": 2},
            {"artists_processed": 1, "conflicts": 0, "artists": [
                {"artist_mbid": OTHER, "status": "proven_output", "source": "musicbrainz"}]},
            {"artists_processed": 1, "classification_counts": {"proven_output": 1}, "database_writes": 2},
            {"artists_processed": 2, "classification_counts": {"proven_output": 2},
             "expected_writes": {"total": 0}},
        ]
        with patch.object(subject.output_evidence, "run", side_effect=results) as execute:
            report = subject.prove_targets([TARGET, OTHER])
        self.assertEqual(report["database_writes"], 4)
        self.assertEqual(report["proven"], [TARGET, OTHER])
        self.assertTrue(report["postcondition_verified"])
        self.assertTrue(execute.call_args_list[1].args[0].require_proven_output)
        self.assertTrue(execute.call_args_list[3].args[0].refresh)

    def test_evidence_requires_read_only_postcondition(self):
        results = [
            {"artists_processed": 1, "conflicts": 0, "artists": [
                {"artist_mbid": TARGET, "status": "proven_output", "write_required": True}]},
            {"artists_processed": 1, "classification_counts": {"proven_output": 1},
             "database_writes": 2},
            {"artists_processed": 1, "classification_counts": {"proven_output": 0},
             "expected_writes": {"total": 0}},
        ]
        with patch.object(subject.output_evidence, "run", side_effect=results):
            with self.assertRaisesRegex(RuntimeError, "postcondition failed"):
                subject.prove_targets([TARGET])

    def test_full_execution_journals_and_replay_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            pilot_file = root / "pilot.json"
            output = root / "execution.json"
            pilot_file.write_text(json.dumps(pilot()), encoding="utf-8")
            cards = [[], [TARGET]]
            with patch.object(subject, "load_dotenv"), \
                 patch.dict(subject.os.environ, {"DATABASE_URL": "postgres://fixture"}), \
                 patch.object(subject, "fetch_cards", side_effect=cards), \
                 patch.object(subject.generic_collector, "run", return_value=collector_result()) as collector, \
                 patch.object(subject, "prove_targets", return_value={"proven": [TARGET], "unknown": [],
                                                                      "database_writes": 2}):
                report = subject.run(pilot_file=pilot_file, source_mbids=[SOURCE],
                                     execution_id=EXECUTION, base_url="http://localhost:3001",
                                     output=output)
                replay = subject.run(pilot_file=pilot_file, source_mbids=[SOURCE],
                                     execution_id=EXECUTION, base_url="http://localhost:3001",
                                     output=output)
            self.assertEqual(report["status"], "complete")
            self.assertEqual(report["changes"][SOURCE]["delta"], 1)
            self.assertEqual(replay, report)
            self.assertEqual(collector.call_count, 1)
            args = collector.call_args.args[0]
            self.assertTrue(args.write)
            self.assertEqual(args.source_mbid, [SOURCE])
            self.assertEqual(args.execution_id, EXECUTION)


if __name__ == "__main__":
    unittest.main()
