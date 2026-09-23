from __future__ import annotations

import unittest
from unittest.mock import patch

from scripts.follow_the_groove import coverage_pilot as subject


MBID = "10000000-0000-0000-0000-000000000001"
SELECTED = [{"artist_mbid": MBID, "name": "Fixture"}]


class CoveragePilotTest(unittest.TestCase):
    def test_pilot_reuses_bounded_dry_run_and_exact_source_set(self):
        collected = {
            "mode": "dry-run",
            "api_counters": {"musicbrainz_requests": 1},
            "sources": [{
                "source": {"mbid": MBID, "display_name": "Fixture"},
                "status": "succeeded", "errors": [],
                "rollback": {"status": "PROVEN"},
                "counters": {"actions": {"similarities": {"CREATE": 2}}},
                "plans": {"similarities": [{"target_mbid": MBID, "action": "CREATE"}],
                          "product_artists": [{"action": "CREATE", "artist_mbid": MBID,
                                               "source_system": "musicbrainz_artist_credit"}],
                          "artists": [{"action": "CREATE", "musicbrainz_artist_mbid": MBID}]},
            }],
        }
        with patch.object(subject.coverage_priorities, "run", return_value={"artists": SELECTED}), \
             patch.object(subject.generic_collector, "run", return_value=collected) as collector:
            report = subject.run(min_products=10, limit=1)
        args = collector.call_args.args[0]
        self.assertTrue(args.dry_run)
        self.assertFalse(args.write)
        self.assertEqual(args.source_mbid, [MBID])
        self.assertEqual(args.max_sources, 1)
        self.assertEqual(report["database_writes"], 0)
        self.assertTrue(report["sources"][0]["preflight_safe"])
        self.assertEqual(report["sources"][0]["resolved_similarity_targets"], 1)
        self.assertEqual(report["sources"][0]["resolved_similarity_mbids"], [MBID])
        self.assertEqual(report["sources"][0]["new_product_target_mbids"], [MBID])
        self.assertEqual(report["sources"][0]["new_artist_mbids"], [MBID])

    def test_unsafe_new_product_source_fails_preflight_summary(self):
        row = {
            "source": {"mbid": MBID, "display_name": "Fixture"},
            "status": "succeeded", "rollback": {"status": "PROVEN"},
            "plans": {"product_artists": [{"action": "CREATE", "source_system": "vinylofy_exact"}]},
        }
        summary = subject.summarize_source(row)
        self.assertFalse(summary["preflight_safe"])
        self.assertEqual(summary["unsafe_product_sources"], ["vinylofy_exact"])

    def test_missing_new_product_source_fails_closed(self):
        summary = subject.summarize_source({
            "source": {"mbid": MBID, "display_name": "Fixture"},
            "status": "succeeded", "rollback": {"status": "PROVEN"},
            "plans": {"product_artists": [{"action": "CREATE"}]},
        })
        self.assertFalse(summary["preflight_safe"])
        self.assertEqual(summary["unsafe_product_sources"], ["<missing>"])

    def test_empty_shortlist_does_not_call_collector(self):
        with patch.object(subject.coverage_priorities, "run", return_value={"artists": []}), \
             patch.object(subject.generic_collector, "run") as collector:
            report = subject.run(min_products=10, limit=5)
        collector.assert_not_called()
        self.assertEqual(report["sources"], [])
        self.assertEqual(report["database_writes"], 0)

    def test_mismatched_source_set_is_rejected(self):
        with patch.object(subject.coverage_priorities, "run", return_value={"artists": SELECTED}), \
             patch.object(subject.generic_collector, "run", return_value={"mode": "dry-run", "sources": []}):
            with self.assertRaisesRegex(RuntimeError, "source set changed"):
                subject.run(min_products=10, limit=1)

    def test_rejects_oversized_batch_before_queries(self):
        with patch.object(subject.coverage_priorities, "run") as priorities:
            with self.assertRaises(ValueError):
                subject.run(min_products=10, limit=26)
            priorities.assert_not_called()


if __name__ == "__main__":
    unittest.main()
