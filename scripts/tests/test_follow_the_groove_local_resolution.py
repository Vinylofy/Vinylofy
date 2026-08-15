from __future__ import annotations

import copy
import contextlib
import io
import unittest

from scripts.follow_the_groove import local_resolution as resolution


def target(**overrides):
    value = {
        "mbid": "075ded80-c694-4157-9a97-4d42ca8b9e8e",
        "canonical_name": "Yawning Man",
        "entity_type": "Group",
        "musicbrainz_type_id": resolution.GROUP_TYPE_ID,
        "local_product_count": 1,
        "wikidata_qid": None,
    }
    value.update(overrides)
    return value


def similarity(**overrides):
    value = {
        "id": "sim",
        "target_artist_id": None,
        "returned_mbid": target()["mbid"],
        "resolution_status": "unresolved",
        "last_seen_run_id": "r2",
        "updated_at": "2026-01-01T00:00:00+00:00",
    }
    value.update(overrides)
    return value


class CategoryContractTests(unittest.TestCase):
    def test_category_a_positive(self):
        self.assertEqual(resolution.classify_category({"classification": resolution.CATEGORY_A}), "ALLOW")

    def test_category_b_exact_name_is_rejected(self):
        self.assertEqual(resolution.classify_category({"classification": "EXISTING_CATALOG_NAME_BUT_IDENTITY_NOT_PROVEN"}), "SKIP")

    def test_category_c_composite_is_rejected(self):
        self.assertEqual(resolution.classify_category({"classification": "COMPOSITE_TARGET"}), "SKIP")

    def test_category_d_mbid_conflict_is_rejected(self):
        self.assertEqual(resolution.classify_category({"classification": "MBID_CONFLICT"}), "SKIP")

    def test_category_e_no_match_is_rejected(self):
        self.assertEqual(resolution.classify_category({"classification": "NO_VINYLOFY_CATALOG_MATCH"}), "SKIP")


class ArtistContractTests(unittest.TestCase):
    def test_missing_mbid_rejected(self):
        self.assertIn("missing_mbid", resolution.validate_target(target(mbid=None)))

    def test_new_person(self):
        row = target(entity_type="Person", musicbrainz_type_id=resolution.PERSON_TYPE_ID)
        self.assertEqual(resolution.classify_artist(row, None, []), "CREATE")

    def test_new_group(self):
        self.assertEqual(resolution.classify_artist(target(), None, []), "CREATE")

    def test_existing_artist_by_mbid_reused(self):
        row = target()
        existing = {"musicbrainz_artist_mbid": row["mbid"], "display_name": row["canonical_name"], "entity_type": "group",
                    "musicbrainz_type_id": row["musicbrainz_type_id"], "wikidata_qid": None}
        self.assertEqual(resolution.classify_artist(row, existing, []), "SEEN_AGAIN")

    def test_same_name_other_mbid_conflict(self):
        self.assertEqual(resolution.classify_artist(target(), None, [{"musicbrainz_artist_mbid": "other"}]), "CONFLICT")

    def test_missing_or_ambiguous_type_rejected(self):
        self.assertIn("ambiguous_type", resolution.validate_target(target(entity_type=None, musicbrainz_type_id=None)))


class ProductContractTests(unittest.TestCase):
    def test_direct_mbid_credit_extracted(self):
        detail = {"artist-credit": [{"name": "Credit", "artist": {"id": target()["mbid"], "name": "Canonical", "type": "Group", "type-id": resolution.GROUP_TYPE_ID}}]}
        self.assertEqual(resolution.direct_artist_credits(detail)[0]["mbid"], target()["mbid"])

    def test_release_group_only_credit_not_used(self):
        self.assertEqual(resolution.direct_artist_credits({"release-group": {"artist-credit": []}}), [])

    def test_name_only_product_link_rejected(self):
        self.assertEqual(resolution.classify_product_link({}, None, False), "CONFLICT")

    def test_product_link_create_and_dedupe(self):
        row = {"credited_name": "A", "credit_position": 1, "source_system": "musicbrainz_artist_credit"}
        self.assertEqual(resolution.classify_product_link(row, None, True), "CREATE")
        self.assertEqual(resolution.classify_product_link(row, copy.deepcopy(row), True), "SEEN_AGAIN")


class SimilarityContractTests(unittest.TestCase):
    def test_unresolved_becomes_resolve(self):
        self.assertEqual(resolution.classify_similarity(similarity(), "artist", target()["mbid"], False), "RESOLVE")

    def test_mbid_mismatch_conflicts(self):
        self.assertEqual(resolution.classify_similarity(similarity(), "artist", "other", False), "CONFLICT")

    def test_returned_name_must_match_local_canonical_or_credit(self):
        self.assertEqual(
            resolution.classify_similarity(similarity(), "artist", target()["mbid"], False, False),
            "CONFLICT",
        )

    def test_resolved_key_collision_rejected(self):
        self.assertEqual(resolution.classify_similarity(similarity(), "artist", target()["mbid"], True), "CONFLICT")

    def test_second_dry_plan_is_already_resolved(self):
        row = similarity(target_artist_id="artist", resolution_status="resolved")
        self.assertEqual(resolution.classify_similarity(row, "artist", target()["mbid"], True), "ALREADY_RESOLVED")

    def test_rollback_preimage_is_exact_and_nonmutating(self):
        row = similarity()
        original = copy.deepcopy(row)
        image = resolution.rollback_preimage(row)
        self.assertEqual(image["resolution_status"], "unresolved")
        self.assertEqual(image["last_seen_run_id"], "r2")
        self.assertEqual(row, original)

    def test_similarity_immutable_fields_are_not_in_preimage_update_contract(self):
        image = resolution.rollback_preimage(similarity(source_artist_id="source", match_score="1"))
        self.assertNotIn("source_artist_id", image)
        self.assertNotIn("match_score", image)


class RunContractTests(unittest.TestCase):
    def test_exact_first_write_gate(self):
        plan = resolution.LivePlan({"summaries": {"artists": {"CREATE": 26}, "similarities": {"RESOLVE": 30},
                                                   "product_artists": {"CREATE": 161}, "protected_non_a": 48}, "conflicts": 0}, {})
        self.assertTrue(resolution.exact_first_write(plan))

    def test_exact_idempotent_gate(self):
        plan = resolution.LivePlan({"summaries": {"artists": {"SEEN_AGAIN": 26}, "similarities": {"ALREADY_RESOLVED": 30},
                                                   "product_artists": {"SEEN_AGAIN": 161}, "protected_non_a": 48}, "conflicts": 0}, {})
        self.assertTrue(resolution.exact_idempotent(plan))

    def test_dry_run_and_apply_are_mutually_exclusive(self):
        parser = resolution.build_parser()
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["--input", "a", "--plan", "b"])

    def test_dry_run_opens_read_only_transaction(self):
        import inspect
        self.assertIn('conn.execute("begin read only")', inspect.getsource(resolution.run))

    def test_persistence_sql_is_limited_to_approved_tables(self):
        import inspect
        source = inspect.getsource(resolution.persist_r3).lower()
        self.assertNotIn("delete ", source)
        self.assertNotIn("insert into artist_edges", source)
        self.assertNotIn("insert into artist_relation_evidence", source)
        self.assertNotIn("insert into artist_aliases", source)
        self.assertNotIn("update products", source)


if __name__ == "__main__":
    unittest.main()
