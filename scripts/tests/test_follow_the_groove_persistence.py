from __future__ import annotations

import unittest
import inspect
import re

from scripts.follow_the_groove import generic_collector as generic, persistence


def artist(**changes):
    row={"id":"a1","musicbrainz_artist_mbid":"00000000-0000-0000-0000-000000000001","display_name":"Artist","entity_type":"person","musicbrainz_type_id":"type","wikidata_qid":None,"last_seen_run_id":"old"}
    row.update(changes); return row


class PlannerTests(unittest.TestCase):
    def test_create_seen_again_conflict(self):
        incoming=artist(id=None,last_seen_run_id=None)
        created=persistence.plan_rows("artists",[incoming],[])[0]
        self.assertEqual(created["action"],"CREATE")
        self.assertTrue(created["planned_id"])
        self.assertEqual(persistence.plan_rows("artists",[incoming],[artist()])[0]["action"],"SEEN_AGAIN")
        self.assertEqual(persistence.plan_rows("artists",[{**incoming,"display_name":"Other"}],[artist()])[0]["action"],"CONFLICT")

    def test_seen_again_captures_old_last_seen(self):
        plan=persistence.plan_rows("artists",[artist(id=None,last_seen_run_id=None)],[artist()])[0]
        self.assertEqual(plan["preimage"],{"id":"a1","last_seen_run_id":"old"})

    def test_seen_again_captures_all_present_freshness_fields(self):
        old=artist(last_verified_at="verified",last_seen_at="seen",updated_at="updated",checked_at="checked")
        plan=persistence.plan_rows("artists",[artist(id=None,last_seen_run_id=None)],[old])[0]
        self.assertEqual(plan["preimage"],{
            "id":"a1","last_seen_run_id":"old","last_verified_at":"verified",
            "last_seen_at":"seen","updated_at":"updated","checked_at":"checked",
        })

    def test_duplicate_incoming_is_skipped(self):
        rows=persistence.plan_rows("artists",[artist(),artist()],[])
        self.assertEqual([row["action"] for row in rows],["CREATE","SKIP"])

    def test_self_and_noncanonical_edges_rejected(self):
        with self.assertRaises(ValueError): persistence.validate_edge({"artist_low_mbid":"a","artist_high_mbid":"a"})
        with self.assertRaises(ValueError): persistence.validate_edge({"artist_low_mbid":"z","artist_high_mbid":"a"})

    def test_rejected_evidence_uses_decision_key(self):
        self.assertEqual(persistence.evidence_contract({"evidence_kind":"rejected"}),"decision")

    def test_generated_pairs_are_not_part_of_contract(self):
        fields=set(persistence.CONTRACTS["membership"].key+persistence.CONTRACTS["membership"].immutable)
        self.assertNotIn("pair_low_id",fields); self.assertNotIn("pair_high_id",fields)

    def test_product_link_dedupes_on_product_artist(self):
        row={"product_id":"p","artist_mbid":"a","credited_name":"A","credit_position":1,"source_system":"musicbrainz_artist_credit"}
        self.assertEqual([x["action"] for x in persistence.plan_rows("product_artists",[row,row],[])],["CREATE","SKIP"])

    def test_similarity_resolution_preimage_is_complete(self):
        old={"id":"s","source_system":"lastfm","source_mbid":"a","returned_target_name_normalized":"b","returned_mbid":"m","position":1,"match_score":"1","resolution_status":"unresolved","target_artist_id":None,"last_seen_run_id":"r1","updated_at":"time"}
        incoming={**old,"resolution_status":"resolved","target_mbid":"m"}
        plan=persistence.plan_similarity_resolution(incoming,old,identity_proven=True)
        self.assertEqual(plan["action"],"RESOLVE_EXISTING_UNRESOLVED")
        self.assertEqual(set(plan["preimage"]),{"id","target_artist_id","resolution_status","last_seen_run_id","updated_at"})

    def test_unproven_similarity_resolution_conflicts(self):
        old={"resolution_status":"unresolved","target_artist_id":None}
        self.assertEqual(persistence.plan_similarity_resolution({},old,identity_proven=False)["action"],"CONFLICT")

    def test_rollback_manifest_order_and_protection(self):
        manifest=persistence.rollback_manifest({"artists":[{"action":"CREATE","musicbrainz_artist_mbid":"a"}],"aliases":[]})
        self.assertEqual(manifest["status"],"PROVEN")
        self.assertEqual(manifest["dependency_delete_order"][-1],"collection_run")
        self.assertTrue(manifest["preexisting_artists_protected"])

    def test_conflict_blocks_rollback_proof(self):
        self.assertEqual(persistence.rollback_manifest({"artists":[{"action":"CONFLICT"}]})["status"],"NOT_PROVEN")

    def test_writer_is_hard_disabled(self):
        with self.assertRaises(persistence.PersistenceDisabled): persistence.DisabledWriter.persist({})


class WriterContractTests(unittest.TestCase):
    def test_edge_endpoints_are_canonicalized_by_database_id_not_mbid(self):
        artist_ids={"mbid-a":"f0000000-0000-0000-0000-000000000000",
                    "mbid-b":"10000000-0000-0000-0000-000000000000"}
        self.assertEqual(
            persistence.canonical_artist_id_pair("mbid-a","mbid-b",artist_ids),
            ("10000000-0000-0000-0000-000000000000","f0000000-0000-0000-0000-000000000000"),
        )

    def test_edge_endpoints_keep_database_id_order_when_already_canonical(self):
        artist_ids={"z-mbid":"10000000-0000-0000-0000-000000000000",
                    "a-mbid":"f0000000-0000-0000-0000-000000000000"}
        self.assertEqual(
            persistence.canonical_artist_id_pair("z-mbid","a-mbid",artist_ids),
            ("10000000-0000-0000-0000-000000000000","f0000000-0000-0000-0000-000000000000"),
        )

    def test_edge_canonicalization_rejects_same_database_artist(self):
        with self.assertRaisesRegex(persistence.PersistenceConflict,"self edge"):
            persistence.canonical_artist_id_pair("a","b",{"a":"10000000-0000-0000-0000-000000000000","b":"10000000-0000-0000-0000-000000000000"})

    def test_edge_insert_and_allowed_evidence_share_database_pair_lookup(self):
        source=inspect.getsource(persistence.apply_source_plan)
        self.assertIn('edge_ids[database_pair]=row_id',source)
        self.assertIn('edge_id=edge_ids.get(database_pair)',source)
        self.assertIn('artist_ids[row["source_mbid"]],artist_ids[row["target_mbid"]]',source)
        self.assertIn('if row["classification"]=="allowed" else None',source)

    def test_existing_similarity_target_outside_artist_plan_is_resolved_by_mbid(self):
        source_mbid="00000000-0000-0000-0000-000000000001"
        target_mbid="2fddb92d-24b2-46a5-bf28-3aed46f4684c"

        class Result:
            def fetchall(self):
                return [("kylie-id",target_mbid,"Kylie Minogue","person","type-person")]

        class Connection:
            def execute(self, query, params):
                self.query=query; self.params=params
                return Result()

        plans={
            "artists":[{**artist(musicbrainz_artist_mbid=source_mbid),"action":"SEEN_AGAIN","existing_id":"madonna-id"}],
            "aliases":[], "edges":[], "evidence":[], "product_artists":[],
            "similarities":[{"action":"CREATE","source_mbid":source_mbid,"target_mbid":target_mbid,"resolution_status":"resolved"}],
        }
        connection=Connection()
        artist_ids=persistence.resolve_referenced_artist_ids(connection,plans)
        self.assertEqual(artist_ids[source_mbid],"madonna-id")
        self.assertEqual(artist_ids[target_mbid],"kylie-id")
        self.assertIn("musicbrainz_artist_mbid",connection.query)
        self.assertNotIn("display_name =",connection.query.lower())

    def test_missing_referenced_artist_mbid_is_a_hard_conflict(self):
        class Result:
            def fetchall(self): return []
        class Connection:
            def execute(self, _query, _params): return Result()
        plans={"artists":[],"aliases":[],"edges":[],"evidence":[],"product_artists":[],
               "similarities":[{"action":"CREATE","source_mbid":"00000000-0000-0000-0000-000000000001",
                                "target_mbid":"00000000-0000-0000-0000-000000000002","resolution_status":"resolved"}]}
        with self.assertRaisesRegex(persistence.PersistenceConflict,"missing referenced artist MBID"):
            persistence.resolve_referenced_artist_ids(Connection(),plans)

    def test_all_writer_families_contribute_to_central_artist_reference_set(self):
        plans={
            "artists":[{"action":"CREATE","musicbrainz_artist_mbid":"artist"}],
            "aliases":[{"action":"CREATE","artist_mbid":"alias"}],
            "edges":[{"action":"CREATE","artist_low_mbid":"edge-low","artist_high_mbid":"edge-high"}],
            "evidence":[{"action":"CREATE","source_mbid":"evidence-source","target_mbid":"evidence-target"}],
            "similarities":[{"action":"CREATE","source_mbid":"similarity-source","target_mbid":"similarity-target"}],
            "product_artists":[{"action":"CREATE","artist_mbid":"product"}],
        }
        self.assertEqual(persistence.referenced_artist_mbids(plans),{
            "artist","alias","edge-low","edge-high","evidence-source","evidence-target",
            "similarity-source","similarity-target","product",
        })

    def test_writer_statement_scope_is_ftg_only(self):
        source=inspect.getsource(persistence.apply_source_plan).lower()
        written=set(re.findall(r'(?:insert into|update)\s+([a-z_]+)',source))
        written.discard("failed")  # exception message, not SQL
        self.assertTrue(written)
        self.assertLessEqual(written,persistence.ALLOWED_WRITE_TABLES)
        self.assertNotIn("update products",source)
        self.assertNotIn("delete from",source)

    def test_generated_evidence_columns_are_never_inserted(self):
        source=inspect.getsource(persistence.apply_source_plan)
        self.assertNotIn("pair_low_id",source)
        self.assertNotIn("pair_high_id",source)

    def test_run_is_registered_before_source_transaction(self):
        source=inspect.getsource(generic.execute_writes)
        self.assertLess(source.index("register_source_run"),source.index("begin isolation level serializable"))
        self.assertIn("mark_source_run_failed",source)

    def test_source_transaction_is_serializable_and_atomic(self):
        source=inspect.getsource(generic.execute_writes).lower()
        self.assertIn("begin isolation level serializable",source)
        self.assertIn("conn.rollback()",source)
        self.assertIn("conn.commit()",source)

    def test_writer_consumes_existing_plans_without_relation_derivation(self):
        source=inspect.getsource(persistence.apply_source_plan)
        self.assertNotIn("derive_edges",source)
        self.assertIn('plans["edges"]',source)

    def test_writer_captures_actual_created_ids_and_preimages(self):
        source=inspect.getsource(persistence.apply_source_plan)
        self.assertIn("created_ids",source)
        self.assertIn("preimages",source)
        self.assertIn("rollback_manifest",source)


if __name__ == "__main__": unittest.main()
