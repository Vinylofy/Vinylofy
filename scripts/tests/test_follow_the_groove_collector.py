from __future__ import annotations

import unittest
from pathlib import Path

from scripts.follow_the_groove import collector


ROOT = Path(__file__).resolve().parents[2]
PILOT_PATH = ROOT / "scripts/follow_the_groove/pilot_v1.json"


class PilotConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pilot = collector.load_pilot_config(PILOT_PATH)

    def test_pilot_has_exactly_twenty_unique_mbids(self) -> None:
        self.assertEqual(len(self.pilot.artists), 20)
        self.assertEqual(len(self.pilot.by_mbid), 20)

    def test_expected_person_and_group_types(self) -> None:
        self.assertEqual(self.pilot.by_mbid["dbecd515-aac9-4e7b-808e-bca22809c316"].entity_type, "Person")
        self.assertEqual(self.pilot.by_mbid["7dc8f5bd-9d0b-4087-9f73-dc164950bbd8"].entity_type, "Group")

    def test_identity_mismatch_is_rejected_not_silently_corrected(self) -> None:
        expected = self.pilot.by_mbid["070d193a-845c-479f-980e-bef15710653e"]
        result = collector.validate_identity(expected, {"id": expected.mbid, "name": "Prince Far I", "type": "Person", "type-id": collector.PERSON_TYPE_ID})
        self.assertEqual(result["status"], "rejected")
        self.assertIn("canonical_name_mismatch", result["reasons"])


class RelationContractTests(unittest.TestCase):
    def test_deny_contract_is_central_and_complete_for_required_classes(self) -> None:
        cases = {
            "producer": "producer",
            "mixer": "technical_credit",
            "mastering engineer": "technical_credit",
            "recording engineer": "technical_credit",
            "songwriter": "composition",
            "composer": "composition",
            "arranger": "arrangement",
            "cover recording of": "cover_only",
            "samples": "sample_only",
            "tribute": "tribute",
            "interpolation": "composition",
            "touring member": "touring_only",
        }
        for relation, reason in cases.items():
            with self.subTest(relation=relation):
                classification, kind, actual_reason = collector.classify_relation(relation)
                self.assertEqual((classification, kind, actual_reason), ("rejected", "rejected", reason))

    def test_prince_sinead_songwriting_and_cover_create_no_edge(self) -> None:
        rows = []
        for relation in ("composer", "cover recording of"):
            classification, kind, reason = collector.classify_relation(relation)
            rows.append({"source_mbid": "prince", "target_mbid": "sinead", "classification": classification, "evidence_kind": kind, "reason": reason})
        self.assertEqual(collector.derive_edges(rows), [])

    def test_producer_neither_creates_nor_strengthens_edge(self) -> None:
        allowed = {"source_mbid": "a", "target_mbid": "b", "classification": "allowed"}
        rejected = {"source_mbid": "a", "target_mbid": "b", "classification": collector.classify_relation("producer")[0]}
        self.assertEqual(collector.derive_edges([allowed]), collector.derive_edges([allowed, rejected]))

    def test_touring_and_live_without_recording_are_insufficient(self) -> None:
        for relation in ("touring", "live", "live guest"):
            classification, kind, _ = collector.classify_relation(relation, official_recording=False)
            self.assertEqual((classification, kind), ("insufficient", "insufficient"))

    def test_allowed_performance_requires_official_recording(self) -> None:
        for relation, kind in (("instrument", "instrument"), ("vocal", "vocal"), ("guest vocals", "vocal")):
            self.assertEqual(collector.classify_relation(relation, official_recording=True)[:2], ("allowed", kind))
            self.assertEqual(collector.classify_relation(relation, official_recording=False)[:2], ("insufficient", "insufficient"))

    def test_allowed_membership_is_person_to_group_and_preserves_dates(self) -> None:
        pilot = collector.load_pilot_config(PILOT_PATH)
        source = pilot.by_mbid["dbecd515-aac9-4e7b-808e-bca22809c316"]
        payload = {"relations": [{"target-type": "artist", "type": "member of band", "type-id": collector.MEMBER_OF_BAND_TYPE_ID, "direction": "forward", "begin": "1987", "end": "1995-10", "ended": True, "attribute-ids": {}, "artist": {"id": "ea0f2a37-7007-4217-a812-396227f5013a", "name": "Kyuss"}}]}
        evidence, external = collector.extract_memberships(source, payload, pilot)
        self.assertEqual(external, [])
        self.assertEqual(len(evidence), 1)
        self.assertEqual((evidence[0]["source_mbid"], evidence[0]["target_mbid"]), (source.mbid, "ea0f2a37-7007-4217-a812-396227f5013a"))
        self.assertEqual((evidence[0]["begin_date"], evidence[0]["end_date"], evidence[0]["ended"]), ("1987", "1995-10", True))

    def test_artist_credit_is_allowed_and_symmetric(self) -> None:
        pilot = collector.load_pilot_config(PILOT_PATH)
        release = {"id": "00000000-0000-0000-0000-000000000010", "release-group": {"id": "00000000-0000-0000-0000-000000000011"}, "media": [{"tracks": [{"artist-credit": [{"name": "Tony Bennett", "artist": {"id": "8be0594f-8c13-46bb-ab06-f93ffba5c776"}, "joinphrase": " & "}, {"name": "Lady Gaga", "artist": {"id": "650e7db6-b795-4eb5-a702-5ea2fc46c848"}, "joinphrase": ""}], "recording": {"id": "00000000-0000-0000-0000-000000000012", "relations": []}}]}]}
        evidence, credits = collector.extract_recording_evidence(release, pilot)
        self.assertEqual(len(evidence), 1)
        self.assertEqual((evidence[0]["classification"], evidence[0]["evidence_kind"], evidence[0]["direction"]), ("allowed", "artist_credit", "symmetric"))
        self.assertEqual(credits[0]["rendered_credit"], "Tony Bennett & Lady Gaga")

    def test_recording_producer_relation_becomes_rejected_evidence(self) -> None:
        pilot = collector.load_pilot_config(PILOT_PATH)
        release = {"id": "00000000-0000-0000-0000-000000000020", "media": [{"tracks": [{"artist-credit": [{"name": "Arctic Monkeys", "artist": {"id": "ada7a83c-e3e1-40f1-93f9-3e73dbc9298a"}}], "recording": {"id": "00000000-0000-0000-0000-000000000021", "relations": [{"target-type": "artist", "type": "producer", "type-id": "00000000-0000-0000-0000-000000000022", "artist": {"id": "dbecd515-aac9-4e7b-808e-bca22809c316"}}]}}]}]}
        evidence, _ = collector.extract_recording_evidence(release, pilot)
        self.assertEqual(len(evidence), 1)
        self.assertEqual((evidence[0]["classification"], evidence[0]["evidence_kind"], evidence[0]["reason"]), ("rejected", "rejected", "producer"))
        self.assertEqual(collector.derive_edges(evidence), [])

    def test_performed_work_composer_becomes_rejected_evidence(self) -> None:
        pilot = collector.load_pilot_config(PILOT_PATH)
        release = {"id": "00000000-0000-0000-0000-000000000030", "media": [{"tracks": [{"artist-credit": [{"name": "Sinéad O’Connor", "artist": {"id": "c78a77fa-507c-4c07-947a-0355029453bd"}}], "recording": {"id": "00000000-0000-0000-0000-000000000031", "relations": [{"target-type": "work", "type": "performance", "work": {"id": "00000000-0000-0000-0000-000000000032", "relations": [{"target-type": "artist", "type": "composer", "type-id": "00000000-0000-0000-0000-000000000033", "artist": {"id": "070d193a-845c-479f-980e-bef15710653e"}}]}}]}}]}]}
        evidence, _ = collector.extract_recording_evidence(release, pilot)
        self.assertEqual(len(evidence), 1)
        self.assertEqual((evidence[0]["classification"], evidence[0]["evidence_kind"], evidence[0]["reason"]), ("rejected", "rejected", "composition"))
        self.assertEqual(collector.derive_edges(evidence), [])


class IdentityAndMatchingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pilot = collector.load_pilot_config(PILOT_PATH)
        self.prince = self.pilot.by_mbid["070d193a-845c-479f-980e-bef15710653e"]

    def test_alias_is_same_identity_and_not_artist_node(self) -> None:
        payload = {"aliases": [{"name": "Sinead O'Connor", "locale": "en", "type": "Artist name", "primary": True}]}
        aliases = collector.extract_aliases("c78a77fa-507c-4c07-947a-0355029453bd", payload)
        self.assertEqual(aliases[0]["artist_mbid"], "c78a77fa-507c-4c07-947a-0355029453bd")
        self.assertNotIn("mbid", aliases[0])

    def test_jay_z_variants_require_musicbrainz_alias_proof(self) -> None:
        artist = self.pilot.by_mbid["f82bcf78-5b69-4622-a5ef-73800768d9ac"]
        self.assertEqual(collector.match_product_artist("Jay-Z", artist, []), "NO_MATCH")
        aliases = collector.extract_aliases(artist.mbid, {"aliases": [{"name": "Jay-Z", "type": "Artist name"}]})
        self.assertEqual(collector.match_product_artist("Jay-Z", artist, aliases), "HIGH_CONFIDENCE")

    def test_no_substring_product_matching_for_prince_false_positives(self) -> None:
        for value in ("Bonnie “Prince” Billy", "Prince Far I", "King Princess", "Pantha du Prince"):
            with self.subTest(value=value):
                self.assertEqual(collector.match_product_artist(value, self.prince, []), "NO_MATCH")

    def test_group_names_do_not_match_people(self) -> None:
        brant = self.pilot.by_mbid["9c17713b-14f7-47ee-b0ba-87802d186aab"]
        coltrane = self.pilot.by_mbid["b625448e-bf4a-41c3-a421-72ad46cdb831"]
        self.assertEqual(collector.match_product_artist("Brant Bjork Trio", brant, []), "NO_MATCH")
        self.assertEqual(collector.match_product_artist("John Coltrane Quartet", coltrane, []), "NO_MATCH")

    def test_exact_canonical_and_proven_alias_match(self) -> None:
        self.assertEqual(collector.match_product_artist("  PRINCE  ", self.prince, []), "HIGH_CONFIDENCE")
        aliases = collector.extract_aliases(self.prince.mbid, {"aliases": [{"name": "The Artist", "type": "Artist name"}]})
        self.assertEqual(collector.match_product_artist("the artist", self.prince, aliases), "HIGH_CONFIDENCE")

    def test_search_hint_is_not_automatically_a_safe_product_alias(self) -> None:
        aliases = collector.extract_aliases(self.prince.mbid, {"aliases": [{"name": "Symbol", "type": "Search hint"}]})
        self.assertEqual(collector.match_product_artist("Symbol", self.prince, aliases), "NO_MATCH")

    def test_ended_artist_alias_is_not_safe_for_automatic_product_match(self) -> None:
        qotsa = self.pilot.by_mbid["7dc8f5bd-9d0b-4087-9f73-dc164950bbd8"]
        aliases = collector.extract_aliases(qotsa.mbid, {"aliases": [{"name": "Gamma Ray", "type": "Artist name", "begin": "1996", "end": "1997"}]})
        self.assertEqual(collector.match_product_artist("Gamma Ray", qotsa, aliases), "NO_MATCH")

    def test_unsafe_exact_alias_is_reported_ambiguous_not_silently_dropped(self) -> None:
        sinead = self.pilot.by_mbid["c78a77fa-507c-4c07-947a-0355029453bd"]
        aliases = collector.extract_aliases(sinead.mbid, {"aliases": [{"name": "Sinead O'Connor", "type": "Search hint"}]})
        exact, ambiguous = collector.plan_product_matches([{"id": "product", "artist": "Sinead O'Connor"}], self.pilot, aliases, [])
        self.assertEqual(exact, [])
        self.assertEqual(ambiguous[0]["reason"], "unsafe_musicbrainz_alias")
        self.assertEqual(ambiguous[0]["candidate_mbids"], [sinead.mbid])

    def test_unproven_multi_artist_string_is_ambiguous(self) -> None:
        status, mbids = collector.classify_multi_artist("Miles Davis & John Coltrane", [])
        self.assertEqual((status, mbids), ("AMBIGUOUS", []))

    def test_only_pilot_relevant_multi_artist_strings_are_audited(self) -> None:
        self.assertFalse(collector.is_pilot_relevant_multi_artist("Seba & Paradox", self.pilot, []))
        self.assertTrue(collector.is_pilot_relevant_multi_artist("Miles Davis & John Coltrane", self.pilot, []))

    def test_product_match_route_distinguishes_canonical_and_alias(self) -> None:
        jay_z = self.pilot.by_mbid["f82bcf78-5b69-4622-a5ef-73800768d9ac"]
        aliases = collector.extract_aliases(jay_z.mbid, {"aliases": [{"name": "JAY Z", "type": "Artist name"}]})
        self.assertEqual(collector.match_product_artist_route("JAŸ-Z", jay_z, aliases), "vinylofy_exact")
        self.assertEqual(collector.match_product_artist_route("JAY Z", jay_z, aliases), "musicbrainz_alias")


class SimilarityAndPlanningTests(unittest.TestCase):
    def setUp(self) -> None:
        self.pilot = collector.load_pilot_config(PILOT_PATH)

    def test_similarity_is_directional(self) -> None:
        qotsa = self.pilot.by_mbid["7dc8f5bd-9d0b-4087-9f73-dc164950bbd8"]
        kyuss = self.pilot.by_mbid["ea0f2a37-7007-4217-a812-396227f5013a"]
        forward = collector.resolve_similarity(qotsa, {"name": "Kyuss", "mbid": kyuss.mbid, "match": "0.509380"}, 4, self.pilot)
        backward = collector.resolve_similarity(kyuss, {"name": "Queens of the Stone Age", "mbid": qotsa.mbid, "match": "0.897874"}, 4, self.pilot)
        self.assertNotEqual(forward["source_mbid"], backward["source_mbid"])
        self.assertNotEqual(forward["match_score"], backward["match_score"])

    def test_lastfm_composite_without_mbid_is_unresolved(self) -> None:
        tony = self.pilot.by_mbid["8be0594f-8c13-46bb-ab06-f93ffba5c776"]
        row = collector.resolve_similarity(tony, {"name": "Tony Bennett & Lady Gaga", "mbid": None, "match": "1"}, 1, self.pilot)
        self.assertEqual(row["resolution_status"], "unresolved")
        self.assertIsNone(row["target_mbid"])

    def test_external_target_is_classified_without_recursion(self) -> None:
        self.assertEqual(collector.classify_external("7a2e6b55-f149-4e74-be6a-30a1b1a387bb", set(self.pilot.by_mbid)), "EXTERNAL_RELATED_NODE")
        self.assertEqual(collector.classify_external(None, set(self.pilot.by_mbid)), "UNRESOLVED")

    def test_planning_deduplicates_on_schema_keys(self) -> None:
        row = {"product_id": "p", "artist_mbid": "a", "credited_name": "A"}
        self.assertEqual(len(collector.dedupe([row, dict(row)], collector.UPSERT_KEYS["product_artists"])), 1)
        edge = {"source_mbid": "a", "target_mbid": "b", "classification": "allowed"}
        self.assertEqual(len(collector.derive_edges([edge, dict(edge)])), 1)

    def test_persistence_is_impossible_in_step_two(self) -> None:
        with self.assertRaises(collector.PersistenceUnavailable):
            collector.PersistencePlan.persist({})
        self.assertNotIn("--write", collector.build_parser()._option_string_actions)

    def test_rollback_contract_is_proven_after_additive_provenance(self) -> None:
        assessment = collector.rollback_assessment()
        self.assertEqual(assessment["status"], "PROVEN_CONTRACT")
        self.assertIn("ROLLBACK-BY-RUN = PROVEN", assessment["message"])

    def test_persistence_actions_create_seen_again_and_conflict(self) -> None:
        incoming = {
            "musicbrainz_artist_mbid": "mbid",
            "display_name": "Artist",
            "entity_type": "person",
            "musicbrainz_type_id": "type",
            "wikidata_qid": "Q1",
        }
        self.assertEqual(
            collector.plan_persistence_action("artists", incoming, None)["action"],
            "CREATE",
        )
        seen_again = collector.plan_persistence_action("artists", incoming, dict(incoming))
        self.assertEqual(seen_again["action"], "SEEN_AGAIN")
        self.assertEqual(
            set(seen_again["allowed_updates"]),
            collector.NON_DESTRUCTIVE_SEEN_AGAIN_UPDATES,
        )
        conflict = collector.plan_persistence_action(
            "artists",
            incoming,
            {**incoming, "display_name": "Other artist"},
        )
        self.assertEqual(conflict["action"], "CONFLICT")
        self.assertIn("display_name", conflict["conflicts"])

    def test_seen_again_row_is_not_owned_by_current_run(self) -> None:
        row = {
            "product_id": "product",
            "artist_mbid": "artist",
            "credited_name": "Artist",
            "credit_position": 1,
            "source_system": "vinylofy_exact",
        }
        action = collector.plan_persistence_action("product_artists", row, dict(row))
        self.assertEqual(action["action"], "SEEN_AGAIN")
        self.assertNotIn("created_by_run_id", action["allowed_updates"])

    def test_destructive_identity_and_endpoint_changes_are_conflicts(self) -> None:
        edge = {"artist_low_mbid": "a", "artist_high_mbid": "b"}
        changed_edge = {"artist_low_mbid": "a", "artist_high_mbid": "c"}
        self.assertEqual(
            collector.plan_persistence_action("edges", changed_edge, edge)["action"],
            "CONFLICT",
        )

    def test_rollback_distinguishes_owned_preexisting_and_shared_rows(self) -> None:
        run_id = "run-r"
        self.assertEqual(
            collector.plan_rollback_row(
                created_by_run_id=run_id,
                last_seen_run_id=run_id,
                rollback_run_id=run_id,
                dependency_count=0,
            ),
            "DELETE_CANDIDATE",
        )
        self.assertEqual(
            collector.plan_rollback_row(
                created_by_run_id="older-run",
                last_seen_run_id=run_id,
                rollback_run_id=run_id,
                dependency_count=0,
            ),
            "RETAIN_PREEXISTING_CLEAR_LAST_SEEN",
        )
        self.assertEqual(
            collector.plan_rollback_row(
                created_by_run_id=run_id,
                last_seen_run_id=run_id,
                rollback_run_id=run_id,
                dependency_count=1,
            ),
            "RETAIN_SHARED",
        )
        self.assertEqual(
            collector.plan_rollback_row(
                created_by_run_id=run_id,
                last_seen_run_id="newer-run",
                rollback_run_id=run_id,
                dependency_count=0,
            ),
            "RETAIN_SHARED",
        )


if __name__ == "__main__":
    unittest.main()
