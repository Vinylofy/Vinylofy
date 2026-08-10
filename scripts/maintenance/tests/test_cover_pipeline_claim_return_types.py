from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]

MIGRATION = (
    ROOT
    / "supabase"
    / "migrations"
    / "20260810102000_fix_cover_claim_return_types.sql"
)


class CoverClaimReturnTypeTests(unittest.TestCase):
    def test_claim_next_cover_job_casts_text_outputs(self) -> None:
        source = MIGRATION.read_text(encoding="utf-8")

        expected_final_select = """  select
    q.id,
    q.product_id,
    p.ean::text,
    p.artist::text,
    p.title::text,
    p.format_label::text,
    q.source_reason::text,
    q.priority,
    q.attempt_count
  from queue_update q
  join product_update p
    on p.id = q.product_id;"""

        self.assertIn(
            "create or replace function "
            "public.claim_next_cover_job(_worker_id text)",
            source,
        )

        self.assertIn("format_label text,", source)
        self.assertIn("trigger_source text,", source)
        self.assertIn(expected_final_select, source)

        self.assertNotIn(
            """  select
    q.id,
    q.product_id,
    p.ean,
    p.artist,
    p.title,
    p.format_label,
    q.source_reason,
    q.priority,
    q.attempt_count
  from queue_update q
  join product_update p
    on p.id = q.product_id;""",
            source,
        )


if __name__ == "__main__":
    unittest.main()
