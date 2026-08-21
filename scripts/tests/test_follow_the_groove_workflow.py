from __future__ import annotations

import argparse
from pathlib import Path
import unittest

from scripts.follow_the_groove import generic_collector as generic, persistence


WORKFLOW=Path(".github/workflows/follow-the-groove-collector.yml")
MBID="79239441-bfd5-4981-a70c-55c3f15c1287"
EXECUTION_ID="00000000-0000-4000-8000-000000000001"


class WorkflowSafetyTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.text=WORKFLOW.read_text(encoding="utf-8")

    def test_scheduled_and_manual_bounded_workflow(self):
        self.assertIn("workflow_dispatch:",self.text)
        self.assertIn("schedule:",self.text)
        self.assertEqual(self.text.count('cron: "0 6,10,14,18,22 * * *"'),1)
        self.assertIn('          - "1"',self.text)
        self.assertIn('          - "2"',self.text)
        self.assertIn('          - "3"',self.text)
        self.assertIn('          - "10"',self.text)
        self.assertIn('^(1|2|3|10)$',self.text)

    def test_schedule_is_write_frontier_ten_and_manual_inputs_remain_available(self):
        self.assertIn("github.event_name == 'schedule' && 'frontier' || inputs.mode",self.text)
        self.assertIn("github.event_name == 'schedule' && '10' || inputs.max_sources",self.text)
        self.assertIn("github.event_name == 'schedule' && 'true' || inputs.write",self.text)
        self.assertIn('args+=(--write --execution-id "$execution_id")',self.text)

    def test_concurrency_timeout_and_artifact_contract(self):
        self.assertIn("group: follow-the-groove-collector",self.text)
        self.assertIn("cancel-in-progress: false",self.text)
        self.assertIn("timeout-minutes: 15",self.text)
        self.assertIn("actions/upload-artifact@v5",self.text)

    def test_recording_and_category_b_are_not_activated(self):
        self.assertNotIn("--recording-release-seed",self.text)
        self.assertNotIn("category-b",self.text.lower())
        self.assertIn("--graph-depth 1",self.text)

    def test_only_environment_secret_names_are_referenced(self):
        self.assertIn("secrets.DATABASE_URL",self.text)
        self.assertIn("secrets.LASTFM_API_KEY",self.text)

    def test_local_command_contracts_parse_without_execution(self):
        parser=generic.build_parser()
        cases=(
            ["--dry-run","--frontier","--max-sources","1"],
            ["--write","--frontier","--max-sources","1","--execution-id",EXECUTION_ID],
            ["--dry-run","--refresh","--max-sources","1","--source-mbid",MBID],
            ["--write","--refresh","--max-sources","1","--source-mbid",MBID,"--execution-id",EXECUTION_ID],
        )
        for values in cases:
            with self.subTest(values=values):
                args=parser.parse_args(values)
                generic.validate_write_scope(args)

    def test_unsafe_write_contracts_fail_closed(self):
        parser=generic.build_parser()
        for values in (
            ["--write","--frontier","--max-sources","11"],
            ["--write","--refresh","--max-sources","11","--source-mbid",MBID,"--source-mbid",MBID,"--source-mbid",MBID,"--source-mbid",MBID,"--source-mbid",MBID,"--source-mbid",MBID,"--source-mbid",MBID,"--source-mbid",MBID,"--source-mbid",MBID,"--source-mbid",MBID,"--source-mbid",MBID],
            ["--write","--refresh","--max-sources","1"],
            ["--write","--max-sources","1"],
        ):
            with self.subTest(values=values):
                with self.assertRaises(persistence.PersistenceDisabled):
                    generic.validate_write_scope(parser.parse_args(values))


if __name__ == "__main__":
    unittest.main()
