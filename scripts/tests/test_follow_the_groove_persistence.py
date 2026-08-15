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

    def test_membership_reverse_fetch_is_seen_again_without_losing_provenance(self):
        base={"id":"e1","source_system":"musicbrainz","relation_type_id":"member",
              "source_mbid":"person","target_mbid":"group","begin_date":"1968","end_date":"1980",
              "attribute_ids":["vocal"],"ended":True,"direction":"source_to_target",
              "classification":"allowed","evidence_kind":"membership","last_seen_run_id":"run-a",
              "provenance":{"fetched_for_mbid":"group","source_direction":"backward"}}
        incoming={**base,"id":None,"provenance":{"fetched_for_mbid":"person","source_direction":"forward"}}
        plan=persistence.plan_rows("membership",[incoming],[base])[0]
        self.assertEqual(plan["action"],"SEEN_AGAIN")
        self.assertEqual(plan["preserved_provenance"],base["provenance"])
        self.assertEqual(plan["observed_provenance"],incoming["provenance"])

    def test_membership_real_immutable_difference_still_conflicts(self):
        base={"id":"e1","source_system":"musicbrainz","relation_type_id":"member",
              "source_mbid":"person","target_mbid":"group","begin_date":None,"end_date":None,
              "attribute_ids":[],"ended":False,"direction":"source_to_target",
              "classification":"allowed","evidence_kind":"membership"}
        self.assertEqual(persistence.plan_rows("membership",[{**base,"ended":True}],[base])[0]["action"],"CONFLICT")
        self.assertEqual(persistence.plan_rows("membership",[{**base,"relation_type_id":"founder"}],[base])[0]["action"],"CREATE")

    def test_product_link_dedupes_on_product_artist(self):
        row={"product_id":"p","artist_mbid":"a","credited_name":"A","credit_position":1,"source_system":"musicbrainz_artist_credit"}
        self.assertEqual([x["action"] for x in persistence.plan_rows("product_artists",[row,row],[])],["CREATE","SKIP"])

    def test_product_link_safe_musicbrainz_credit_enrichment(self):
        old={"id":"pa","product_id":"p","artist_mbid":"a","credited_name":"John Coltrane",
             "credit_position":None,"source_system":"vinylofy_exact","last_seen_run_id":"old",
             "last_verified_at":"before"}
        incoming={"product_id":"p","artist_mbid":"a","credited_name":"Coltrane",
                  "credit_position":1,"source_system":"musicbrainz_artist_credit"}
        plan=persistence.plan_rows("product_artists",[incoming],[old])[0]
        self.assertEqual(plan["action"],"ENRICH_SAFE")
        self.assertEqual(plan["preimage"],{"id":"pa","last_seen_run_id":"old","last_verified_at":"before","credit_position":None})
        self.assertEqual(plan["preserved_source_system"],"vinylofy_exact")
        self.assertEqual(plan["preserved_credited_name"],"John Coltrane")

    def test_product_link_identical_is_seen_again(self):
        row={"id":"pa","product_id":"p","artist_mbid":"a","credited_name":"A",
             "credit_position":1,"source_system":"musicbrainz_artist_credit"}
        self.assertEqual(persistence.plan_rows("product_artists",[row],[row])[0]["action"],"SEEN_AGAIN")

    def test_product_link_conflicting_non_null_position_fails_closed(self):
        old={"id":"pa","product_id":"p","artist_mbid":"a","credited_name":"A",
             "credit_position":2,"source_system":"vinylofy_exact"}
        incoming={**old,"credit_position":1,"source_system":"musicbrainz_artist_credit"}
        self.assertEqual(persistence.plan_rows("product_artists",[incoming],[old])[0]["action"],"CONFLICT")

    def test_product_link_different_artist_is_a_distinct_create(self):
        old={"id":"pa","product_id":"p","artist_mbid":"a","credited_name":"A",
             "credit_position":1,"source_system":"musicbrainz_artist_credit"}
        incoming={**old,"id":None,"artist_mbid":"b"}
        self.assertEqual(persistence.plan_rows("product_artists",[incoming],[old])[0]["action"],"CREATE")

    def test_unverified_product_position_enrichment_fails_closed(self):
        old={"id":"pa","product_id":"p","artist_mbid":"a","credited_name":"A",
             "credit_position":None,"source_system":"vinylofy_exact"}
        incoming={**old,"credit_position":1,"source_system":"other"}
        self.assertEqual(persistence.plan_rows("product_artists",[incoming],[old])[0]["action"],"CONFLICT")

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
        self.assertTrue(manifest["late_revalidation_required"])

    def test_conflict_blocks_rollback_proof(self):
        self.assertEqual(persistence.rollback_manifest({"artists":[{"action":"CONFLICT"}]})["status"],"NOT_PROVEN")

    def test_writer_is_hard_disabled(self):
        with self.assertRaises(persistence.PersistenceDisabled): persistence.DisabledWriter.persist({})


class DependencyAwareRollbackTests(unittest.TestCase):
    RUN_A="00000000-0000-0000-0000-00000000000a"
    RUN_B="00000000-0000-0000-0000-00000000000b"
    RUN_C="00000000-0000-0000-0000-00000000000c"

    def classify_created(self, *, last_seen=None, dependencies=()):
        return persistence.classify_created_rollback_row(
            run_id=self.RUN_A,last_seen_run_id=last_seen or self.RUN_A,
            external_dependencies=dependencies,
        )

    def test_nas_britney_shared_artist_preserves_later_edge_and_evidence(self):
        dependencies=[{"family":"edges","created_by_run_id":self.RUN_B},{"family":"evidence","created_by_run_id":self.RUN_B}]
        self.assertEqual(self.classify_created(dependencies=dependencies),"PRESERVE_SHARED")

    def test_reverse_order_allows_b_then_a_when_dependencies_are_gone(self):
        self.assertEqual(persistence.classify_created_rollback_row(run_id=self.RUN_B,last_seen_run_id=self.RUN_B,external_dependencies=()),"DELETE_SAFE")
        self.assertEqual(self.classify_created(dependencies=()),"DELETE_SAFE")

    def test_multiple_dependencies_preserve_without_opportunistic_cleanup(self):
        self.assertEqual(self.classify_created(dependencies=[{"created_by_run_id":self.RUN_B},{"created_by_run_id":self.RUN_C}]),"PRESERVE_SHARED")
        self.assertEqual(self.classify_created(dependencies=[{"created_by_run_id":self.RUN_C}]),"PRESERVE_SHARED")
        self.assertEqual(self.classify_created(dependencies=()),"DELETE_SAFE")
        self.assertIn("opportunistic_cleanup",inspect.getsource(persistence.revalidate_rollback_plan))

    def test_edge_with_external_evidence_is_preserved(self):
        self.assertEqual(self.classify_created(dependencies=[{"family":"evidence","created_by_run_id":self.RUN_B}]),"PRESERVE_SHARED")

    def test_similarity_seen_by_later_run_is_preserved(self):
        self.assertEqual(self.classify_created(last_seen=self.RUN_B),"PRESERVE_SHARED")

    def test_product_artist_seen_by_later_run_is_preserved_and_products_are_never_deleted(self):
        self.assertEqual(self.classify_created(last_seen=self.RUN_B),"PRESERVE_SHARED")
        self.assertNotIn("products",persistence.DELETE_ORDER)

    def test_preimage_restore_never_clobbers_later_last_seen(self):
        self.assertEqual(persistence.classify_preimage_rollback_row(run_id=self.RUN_A,row_exists=True,last_seen_run_id=self.RUN_A),"RESTORE_PREIMAGE")
        self.assertEqual(persistence.classify_preimage_rollback_row(run_id=self.RUN_A,row_exists=True,last_seen_run_id=self.RUN_B),"PRESERVE_SHARED")
        self.assertEqual(persistence.classify_preimage_rollback_row(run_id=self.RUN_A,row_exists=False,last_seen_run_id=None),"BLOCKED_UNSAFE")

    def test_created_by_is_immutable_and_late_plan_is_read_only(self):
        source=inspect.getsource(persistence.revalidate_rollback_plan).lower()
        self.assertNotIn("update ",source)
        self.assertNotIn("delete ",source)
        self.assertIn("created_by_run_id",source)


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

    def test_product_enrichment_preserves_creator_provenance_and_has_atomic_precondition(self):
        source=inspect.getsource(persistence.apply_source_plan)
        statement=next(line for line in source.splitlines() if 'update product_artists set credit_position=' in line)
        self.assertNotIn("source_system",statement)
        self.assertNotIn("credited_name",statement)
        self.assertIn("credit_position is null",statement)
        self.assertIn('action=="ENRICH_SAFE"',source)


if __name__ == "__main__": unittest.main()
