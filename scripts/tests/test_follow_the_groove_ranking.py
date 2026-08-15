from __future__ import annotations

import unittest
from dataclasses import replace
from decimal import Decimal

from scripts.follow_the_groove import ranking


def candidate(name: str, *, target_id: str | None = None, factual=False, mechanisms=(), similarity=False,
              position=None, score="0.5", eligible=True, products=1, evidence=1, recordings=0):
    return ranking.Candidate(
        source_artist_id="source", target_artist_id=target_id or name.lower().replace(" ", "-"), display_name=name,
        musicbrainz_artist_mbid=(target_id or name.lower().replace(" ", "-")) + "-mbid", entity_type="person",
        factual=factual, factual_mechanisms=tuple(mechanisms), allowed_evidence_count=evidence,
        unique_recording_count=recordings, similarity=similarity, similarity_position=position,
        similarity_match_score=Decimal(score) if similarity else None, search_eligible=eligible, product_count=products,
    )


class TierAndOrderingTests(unittest.TestCase):
    def test_tier_one_before_tier_two_before_tier_three(self):
        rows = [candidate("Similar", similarity=True, position=1), candidate("Factual", factual=True),
                candidate("Multi", factual=True, similarity=True, position=5)]
        self.assertEqual([x.display_name for x in ranking.rank_candidates(rows)], ["Multi", "Factual", "Similar"])

    def test_similarity_position_orders_tier_one_and_three(self):
        rows = [candidate("Later", similarity=True, position=4), candidate("First", similarity=True, position=1)]
        self.assertEqual([x.display_name for x in ranking.rank_candidates(rows)], ["First", "Later"])
        multi = [replace(row, factual=True) for row in rows]
        self.assertEqual([x.display_name for x in ranking.rank_candidates(multi)], ["First", "Later"])

    def test_match_score_does_not_change_order(self):
        low = candidate("A", similarity=True, position=2, score="0.1")
        high = candidate("B", similarity=True, position=2, score="1")
        self.assertEqual([x.display_name for x in ranking.rank_candidates([high, low])], ["A", "B"])

    def test_product_count_does_not_change_order(self):
        many = candidate("B", factual=True, products=1000)
        few = candidate("A", factual=True, products=1)
        self.assertEqual([x.display_name for x in ranking.rank_candidates([many, few])], ["A", "B"])

    def test_evidence_and_recording_counts_do_not_change_order(self):
        many = candidate("B", factual=True, mechanisms=("artist_credit",), evidence=99, recordings=50)
        one = candidate("A", factual=True, mechanisms=("membership",), evidence=1, recordings=0)
        self.assertEqual([x.display_name for x in ranking.rank_candidates([many, one])], ["A", "B"])

    def test_current_and_former_are_not_ranking_fields(self):
        fields = ranking.Candidate.__dataclass_fields__
        self.assertNotIn("ended", fields)
        self.assertNotIn("membership_status", fields)

    def test_technical_tiebreak_name_then_mbid_is_deterministic(self):
        b = candidate("Same", target_id="b", factual=True)
        a = candidate("Same", target_id="a", factual=True)
        self.assertEqual([x.target_artist_id for x in ranking.rank_candidates([b, a])], ["a", "b"])
        self.assertEqual(ranking.rank_candidates([b, a]), ranking.rank_candidates([b, a]))


class DiscoveryTests(unittest.TestCase):
    def factual_three(self):
        return [candidate("A", factual=True), candidate("B", factual=True), candidate("C", factual=True)]

    def test_position_one_replaces_only_third_place(self):
        discovery = candidate("Discovery", similarity=True, position=1)
        result = ranking.rank_candidates(self.factual_three() + [discovery])
        self.assertEqual([x.display_name for x in result], ["A", "B", "Discovery"])

    def test_position_two_does_not_replace(self):
        result = ranking.rank_candidates(self.factual_three() + [candidate("Discovery", similarity=True, position=2)])
        self.assertEqual([x.display_name for x in result], ["A", "B", "C"])

    def test_position_five_does_not_replace(self):
        result = ranking.rank_candidates(self.factual_three() + [candidate("Discovery", similarity=True, position=5)])
        self.assertEqual([x.display_name for x in result], ["A", "B", "C"])

    def test_no_similarity_means_no_replacement(self):
        self.assertEqual([x.display_name for x in ranking.rank_candidates(self.factual_three())], ["A", "B", "C"])

    def test_less_than_three_candidates_forces_nothing(self):
        rows = self.factual_three()[:2] + [candidate("Discovery", similarity=True, position=1)]
        self.assertEqual([x.display_name for x in ranking.rank_candidates(rows, limit=2)], ["A", "B"])

    def test_limit_above_three_preserves_full_pool(self):
        rows = self.factual_three() + [candidate("Discovery", similarity=True, position=1), candidate("Other", similarity=True, position=2)]
        result = ranking.rank_candidates(rows, limit=5)
        self.assertEqual([x.display_name for x in result], ["A", "B", "Discovery", "C", "Other"])


class EligibilityAndDedupeTests(unittest.TestCase):
    def test_search_filters_before_ranking(self):
        unavailable = candidate("A", factual=True, eligible=False, products=0)
        available = candidate("B", similarity=True, position=2, eligible=True)
        self.assertEqual([x.display_name for x in ranking.rank_candidates([unavailable, available], mode="search")], ["B"])

    def test_trail_ignores_availability(self):
        row = candidate("A", factual=True, eligible=False, products=0)
        self.assertEqual(ranking.rank_candidates([row], mode="trail"), (row,))

    def test_candidate_dedupe_merges_signals_without_inflating_counts(self):
        factual = candidate("A", target_id="a", factual=True, mechanisms=("membership",), evidence=1)
        similar = candidate("A", target_id="a", similarity=True, position=1, evidence=99)
        result = ranking.rank_candidates([factual, similar])
        self.assertEqual(len(result), 1)
        self.assertTrue(result[0].multi_signal)
        self.assertEqual(result[0].allowed_evidence_count, 1)

    def test_reverse_similarity_is_not_inferred(self):
        forward = candidate("Target", similarity=True, position=1)
        self.assertEqual(ranking.rank_candidates([forward])[0].source_artist_id, "source")
        self.assertFalse(hasattr(forward, "reverse_similarity"))

    def test_zero_candidates(self):
        self.assertEqual(ranking.rank_candidates([]), ())

    def test_invalid_mode_and_limit_rejected(self):
        with self.assertRaises(ValueError): ranking.rank_candidates([], mode="other")
        with self.assertRaises(ValueError): ranking.rank_candidates([], limit=-1)


class ReasonAndNegativeContractTests(unittest.TestCase):
    def test_reason_codes(self):
        membership = candidate("M", factual=True, mechanisms=("membership",))
        recording = candidate("R", factual=True, mechanisms=("artist_credit", "vocal"))
        multi = replace(membership, similarity=True, similarity_position=1)
        similar = candidate("S", similarity=True, position=1)
        self.assertEqual(membership.reason_codes, ("membership",))
        self.assertEqual(recording.reason_codes, ("recording_collaboration",))
        self.assertEqual(multi.reason_codes, ("factual_and_similarity", "membership"))
        self.assertEqual(similar.reason_codes, ("similar_artist",))

    def test_query_uses_edges_and_only_allowed_evidence(self):
        sql = ranking.CANDIDATE_QUERY.lower()
        self.assertIn("join artist_edges", sql)
        self.assertIn("ev.classification = 'allowed'", sql)
        self.assertNotIn("producer", sql)
        self.assertNotIn("songwriter", sql)

    def test_query_uses_only_resolved_directional_similarity(self):
        sql = ranking.CANDIDATE_QUERY.lower()
        self.assertIn("si.resolution_status = 'resolved'", sql)
        self.assertIn("s.id = si.source_artist_id", sql)
        self.assertNotIn("returned_target_name", sql)

    def test_query_uses_product_links_only_for_availability(self):
        self.assertIn("from product_artists", ranking.CANDIDATE_QUERY.lower())
        self.assertNotIn("product_count desc", ranking.CANDIDATE_QUERY.lower())

    def test_cli_has_no_write_mode(self):
        parser = ranking.build_parser()
        self.assertNotIn("--write", parser._option_string_actions)


if __name__ == "__main__":
    unittest.main()
