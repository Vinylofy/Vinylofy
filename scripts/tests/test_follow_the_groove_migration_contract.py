from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FORWARD = ROOT / "supabase/migrations/20260814120000_add_follow_the_groove_v1.sql"
ROLLBACK = ROOT / "supabase/rollbacks/20260814120000_add_follow_the_groove_v1.rollback.sql"

TABLES = {
    "follow_the_groove_collection_runs",
    "artists",
    "artist_aliases",
    "artist_edges",
    "artist_relation_evidence",
    "artist_similarity",
    "product_artists",
}


class FollowTheGrooveMigrationContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.forward = FORWARD.read_text(encoding="utf-8").lower()
        cls.rollback = ROLLBACK.read_text(encoding="utf-8").lower()

    def test_expected_objects_are_created_and_rolled_back(self) -> None:
        for table in TABLES:
            self.assertIn(f"create table public.{table}", self.forward)
            self.assertIn(f"drop table if exists public.{table}", self.rollback)

    def test_existing_products_is_reference_only(self) -> None:
        forbidden = re.compile(
            r"\b(?:alter|drop|delete\s+from|update|insert\s+into|truncate)\s+"
            r"(?:table\s+)?public\.products\b"
        )
        self.assertIsNone(forbidden.search(self.forward))
        self.assertIsNone(forbidden.search(self.rollback))
        self.assertIn("references public.products(id) on delete cascade", self.forward)

    def test_security_and_relation_separation_are_explicit(self) -> None:
        for table in TABLES:
            self.assertIn(f"alter table public.{table} enable row level security", self.forward)

        self.assertIn("revoke all on table", self.forward)
        self.assertIn("artist_relation_evidence_service_role_all", self.forward)
        self.assertNotIn("artist_relation_evidence_public_read", self.forward)
        self.assertIn("artist_similarity_resolved_public_read", self.forward)
        self.assertIn("resolution_status = 'resolved'", self.forward)
        self.assertIn("unique (artist_low_id, artist_high_id)", self.forward)
        self.assertIn(
            "foreign key (edge_id, pair_low_id, pair_high_id)",
            self.forward,
        )
        self.assertIn("nulls not distinct", self.forward)


if __name__ == "__main__":
    unittest.main()
