from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
MIGRATION = (
    REPO_ROOT
    / "supabase"
    / "migrations"
    / "20260908163000_optimize_usf_staging_queue.sql"
)
ROLLBACK = (
    REPO_ROOT
    / "supabase"
    / "rollbacks"
    / "20260908163000_optimize_usf_staging_queue.rollback.sql"
)
STAGING = REPO_ROOT / "scripts" / "scrapers" / "usf" / "core" / "staging.py"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "usf-dgmoutlet.yml"


class DgmOutletPipelineContractTests(unittest.TestCase):
    def test_staging_query_has_matching_latest_snapshot_and_antijoin_indexes(self):
        migration = MIGRATION.read_text(encoding="utf-8").lower()
        staging = STAGING.read_text(encoding="utf-8").lower()

        self.assertIn("idx_raw_shop_scrapes_shop_url_latest", migration)
        self.assertIn(
            "shop_id,\n    source_url,\n    scraped_at desc nulls last,\n    id desc",
            migration,
        )
        self.assertIn("idx_staged_offers_raw_scrape_id", migration)
        self.assertIn("on public.staged_offers (raw_scrape_id)", migration)
        self.assertIn("distinct on (r.shop_id, r.source_url)", staging)
        self.assertIn("with latest_raw as materialized", staging)
        self.assertIn("on s.raw_scrape_id = r.id", staging)

    def test_index_migration_has_exact_rollback(self):
        rollback = ROLLBACK.read_text(encoding="utf-8").lower()

        self.assertIn(
            "drop index if exists public.idx_staged_offers_raw_scrape_id",
            rollback,
        )
        self.assertIn(
            "drop index if exists public.idx_raw_shop_scrapes_shop_url_latest",
            rollback,
        )

    def test_dgm_workflow_executes_contract_test(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")

        self.assertIn("python -m unittest", workflow)
        self.assertIn(
            "scripts.scrapers.usf.tests.test_dgmoutlet_pipeline_contract",
            workflow,
        )


if __name__ == "__main__":
    unittest.main()
