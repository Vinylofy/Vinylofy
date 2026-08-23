from __future__ import annotations

import argparse
import contextlib
import io
import inspect
import unittest

from scripts.follow_the_groove import collector, generic_collector as generic, local_resolution, persistence


SOURCE=generic.Source("id","00000000-0000-0000-0000-000000000001","Source","person",collector.PERSON_TYPE_ID,3,"resolved_frontier")


class ConfigTests(unittest.TestCase):
    EXECUTION_ID="00000000-0000-0000-0000-000000000099"
    def test_safe_defaults(self):
        cfg=generic.BoundedConfig(); self.assertEqual((cfg.max_sources,cfg.max_direct_targets,cfg.lastfm_limit,cfg.graph_depth),(25,25,5,1)); self.assertEqual(cfg.recording_release_seeds,())

    def test_invalid_limits_and_depth_rejected(self):
        for kwargs in ({"max_sources":0},{"max_direct_targets":0},{"lastfm_limit":0},{"lastfm_limit":26},{"graph_depth":2}):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError): generic.BoundedConfig(**kwargs)

    def test_write_mode_impossible(self):
        with self.assertRaises(Exception): generic.BoundedConfig(dry_run=False)
        args=argparse.Namespace(write=True,dry_run=False,max_sources=3,max_direct_targets=25,lastfm_limit=5,graph_depth=1,source_mbid=[],recording_release_seed=[],output=None)
        with self.assertRaises(Exception): generic.run(args)

    def test_frontier_write_is_explicit_and_hard_capped(self):
        generic.validate_write_scope(argparse.Namespace(write=True,frontier=True,refresh=False,max_sources=3,source_mbid=[],execution_id=self.EXECUTION_ID))
        generic.validate_write_scope(argparse.Namespace(write=True,frontier=True,refresh=False,max_sources=10,source_mbid=[],execution_id=self.EXECUTION_ID))
        generic.validate_write_scope(argparse.Namespace(write=True,frontier=True,refresh=False,max_sources=25,source_mbid=[],execution_id=self.EXECUTION_ID))
        with self.assertRaises(persistence.PersistenceDisabled):
            generic.validate_write_scope(argparse.Namespace(write=True,frontier=True,refresh=False,max_sources=26,source_mbid=[],execution_id=self.EXECUTION_ID))
        with self.assertRaises(persistence.PersistenceDisabled):
            generic.validate_write_scope(argparse.Namespace(write=True,frontier=False,refresh=False,max_sources=1,source_mbid=[],execution_id=self.EXECUTION_ID))

    def test_refresh_write_requires_exact_explicit_scope(self):
        generic.validate_write_scope(argparse.Namespace(write=True,frontier=False,refresh=True,max_sources=1,source_mbid=[SOURCE.mbid],execution_id=self.EXECUTION_ID))
        with self.assertRaises(persistence.PersistenceDisabled):
            generic.validate_write_scope(argparse.Namespace(write=True,frontier=False,refresh=True,max_sources=1,source_mbid=[],execution_id=self.EXECUTION_ID))
        with self.assertRaises(persistence.PersistenceDisabled):
            generic.validate_write_scope(argparse.Namespace(write=True,frontier=False,refresh=True,max_sources=26,source_mbid=[SOURCE.mbid]*26,execution_id=self.EXECUTION_ID))

    def test_recording_seed_must_be_uuid(self):
        with self.assertRaises(ValueError): generic.BoundedConfig(recording_release_seeds=("all",))

    def test_write_requires_durable_execution_id(self):
        args=argparse.Namespace(write=True,frontier=True,refresh=False,max_sources=1,source_mbid=[])
        with self.assertRaises(persistence.PersistenceDisabled): generic.validate_write_scope(args)
        args.execution_id=self.EXECUTION_ID
        generic.validate_write_scope(args)

    def test_execution_safety_is_database_global_and_durable(self):
        source=inspect.getsource(generic)
        self.assertIn("pg_try_advisory_xact_lock",source)
        self.assertIn("WRITE_BLOCKED_ALREADY_RUNNING",source)
        self.assertIn("BATCH_COLLECTOR",source)
        self.assertIn("existing-execution",source)

    def test_output_is_written_after_durable_success_transition(self):
        source=inspect.getsource(generic.run)
        self.assertLess(source.rfind("update_execution_state(os.environ[\"DATABASE_URL\"], execution_id, status=\"succeeded\""), source.rfind("args.output.write_text"))


class SelectorTests(unittest.TestCase):
    class Conn:
        def __init__(self, rows): self.rows=rows; self.sql=""; self.params={}
        def execute(self, sql, params): self.sql=sql; self.params=params; return self
        def fetchall(self): return self.rows

    def test_deterministic_frontier_contract_and_limit(self):
        row=("id",SOURCE.mbid,"Source","person",collector.PERSON_TYPE_ID,3,"resolved_frontier",False)
        conn=self.Conn([row])
        result=generic.select_sources(conn,1)
        self.assertEqual(result,[SOURCE])
        self.assertEqual(conn.params["limit"],1)
        self.assertIn("order by",conn.sql.lower())
        self.assertIn("status='succeeded'",conn.sql)
        self.assertIn("select distinct counters->>'source_artist_mbid'",conn.sql)

    def test_explicit_source_filter_is_mbid_only(self):
        conn=self.Conn([]); generic.select_sources(conn,2,(SOURCE.mbid,))
        self.assertEqual(conn.params["source_mbids"],[SOURCE.mbid])
        self.assertIn("array_position",conn.sql)

    def test_successful_source_requires_explicit_refresh_override(self):
        conn=self.Conn([])
        generic.select_sources(conn,1,(SOURCE.mbid,),include_successful=True)
        self.assertTrue(conn.params["include_successful"])
        self.assertIn("p.mbid is null or %(include_successful)s",conn.sql)
        with self.assertRaisesRegex(ValueError,"explicit source MBIDs"):
            generic.select_sources(self.Conn([]),1,include_successful=True)

    def test_invalid_source_mbid_rejected(self):
        with self.assertRaises(ValueError): generic.select_sources(self.Conn([]),1,("name-only",))


class IdentityMembershipTests(unittest.TestCase):
    def test_identity_is_mbid_name_type_strict(self):
        ok={"id":SOURCE.mbid,"name":"Source","type":"Person","type-id":collector.PERSON_TYPE_ID}
        self.assertEqual(generic.validate_source(SOURCE,ok)["status"],"resolved")
        self.assertEqual(generic.validate_source(SOURCE,{**ok,"name":"Other"})["status"],"conflict")

    def test_membership_is_deterministically_capped_and_not_recursive(self):
        rels=[]
        for suffix in ("003","002","004"):
            mbid=f"00000000-0000-0000-0000-000000000{suffix}"
            rels.append({"target-type":"artist","type":"member of band","type-id":collector.MEMBER_OF_BAND_TYPE_ID,"artist":{"id":mbid,"name":suffix,"type":"Group","type-id":collector.GROUP_TYPE_ID},"attribute-ids":{}})
        targets,evidence,counts=generic.extract_direct_memberships(SOURCE,{"relations":rels},2)
        self.assertEqual([row["musicbrainz_artist_mbid"] for row in targets],sorted([rels[0]["artist"]["id"],rels[1]["artist"]["id"]]))
        self.assertEqual(counts,{"received":3,"selected":2,"truncated":1}); self.assertEqual(len(evidence),2)
        self.assertTrue(all(row["node_role"]=="DISCOVERED_TARGET" for row in targets))

    def test_invalid_target_type_is_not_promoted(self):
        relation={"target-type":"artist","type":"member of band","artist":{"id":"00000000-0000-0000-0000-000000000002","name":"X"}}
        targets,evidence,_=generic.extract_direct_memberships(SOURCE,{"relations":[relation]},25)
        self.assertEqual((targets,evidence),([],[]))


class LocalResolutionTests(unittest.TestCase):
    def test_category_a_validator_accepts_complete_local_target(self):
        row={"mbid":"00000000-0000-0000-0000-000000000002","canonical_name":"Target","entity_type":"Group","musicbrainz_type_id":collector.GROUP_TYPE_ID,"local_product_count":1}
        self.assertEqual(local_resolution.validate_target(row),[])

    def test_category_b_name_only_stays_unproven(self):
        self.assertEqual(local_resolution.classify_category({"classification":"EXISTING_CATALOG_NAME_BUT_IDENTITY_NOT_PROVEN"}),"SKIP")

    def test_composite_conflict_and_no_match_are_protected(self):
        for category in ("COMPOSITE_TARGET","MBID_CONFLICT","NO_VINYLOFY_CATALOG_MATCH"):
            self.assertEqual(local_resolution.classify_category({"classification":category}),"SKIP")


class OrchestrationTests(unittest.TestCase):
    def test_edge_snapshot_recanonicalizes_mbids_independent_of_database_id_order(self):
        self.assertEqual(
            generic.snapshot_edge_mbids("f0000000-0000-0000-0000-000000000000",
                                        "10000000-0000-0000-0000-000000000000"),
            ("10000000-0000-0000-0000-000000000000",
             "f0000000-0000-0000-0000-000000000000"),
        )

    def test_edge_planner_matches_reversed_snapshot_to_same_logical_edge(self):
        incoming={"artist_low_mbid":"10000000-0000-0000-0000-000000000000",
                  "artist_high_mbid":"f0000000-0000-0000-0000-000000000000"}
        existing={"id":"edge-id","artist_low_mbid":"f0000000-0000-0000-0000-000000000000",
                  "artist_high_mbid":"10000000-0000-0000-0000-000000000000","last_seen_run_id":"run"}
        low,high=generic.snapshot_edge_mbids(existing["artist_low_mbid"],existing["artist_high_mbid"])
        existing["artist_low_mbid"],existing["artist_high_mbid"]=low,high
        plan=persistence.plan_rows("edges",[incoming],[existing])
        self.assertEqual(plan[0]["action"],"SEEN_AGAIN")

    def test_edge_planner_new_and_duplicate_incoming_contract(self):
        row={"artist_low_mbid":"10000000-0000-0000-0000-000000000000",
             "artist_high_mbid":"f0000000-0000-0000-0000-000000000000"}
        self.assertEqual([item["action"] for item in persistence.plan_rows("edges",[row,row],[])],["CREATE","SKIP"])

    def test_optional_wikidata_observation_never_overwrites_existing_artist(self):
        incoming={"musicbrainz_artist_mbid":"m","display_name":"A","entity_type":"person","musicbrainz_type_id":"t","wikidata_qid":"Q1"}
        existing={**incoming,"id":"id","wikidata_qid":None,"last_seen_run_id":"r"}
        empty={"artists":[existing],"aliases":[],"edges":[],"evidence":[],"similarities":[],"product_artists":[]}
        collected={"artists":[incoming],"aliases":[],"edges":[],"evidence":[],"similarities":[],"product_artists":[]}
        plan=generic.build_plans(collected,empty)["artists"][0]
        self.assertEqual(plan["action"],"SEEN_AGAIN")
        self.assertIsNone(plan["wikidata_qid"])
        self.assertEqual(plan["observed_wikidata_qid"],"Q1")

    def test_source_failure_does_not_stop_later_sources(self):
        sources=[SOURCE,generic.Source("b","00000000-0000-0000-0000-000000000002","B","group",collector.GROUP_TYPE_ID,2,"frontier"),generic.Source("c","00000000-0000-0000-0000-000000000003","C","group",collector.GROUP_TYPE_ID,1,"frontier")]
        def collect(source):
            if source.display_name=="B": raise RuntimeError("source-local")
            return generic.SourceResult(source)
        result=generic.orchestrate(sources,collect)
        self.assertEqual([row.status for row in result],["succeeded","failed","succeeded"])

    def test_parser_requires_dry_run(self):
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit): generic.build_parser().parse_args([])

    def test_write_parser_requires_explicit_bounded_sources(self):
        parsed=generic.build_parser().parse_args(["--write","--refresh","--max-sources","1","--source-mbid",SOURCE.mbid])
        self.assertTrue(parsed.write); self.assertTrue(parsed.refresh); self.assertEqual(parsed.source_mbid,[SOURCE.mbid])

    def test_failure_taxonomy_contains_required_contract(self):
        expected={"SOURCE_IDENTITY_CONFLICT","MUSICBRAINZ_TRANSIENT","MUSICBRAINZ_PERMANENT","LASTFM_TRANSIENT","LASTFM_PERMANENT","LOCAL_RESOLUTION_CONFLICT","PERSISTENCE_CONFLICT","GLOBAL_CONFIGURATION_ERROR"}
        self.assertEqual(generic.FAILURES,expected)


if __name__ == "__main__": unittest.main()
