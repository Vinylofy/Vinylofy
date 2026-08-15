from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
FORWARD = ROOT / "supabase/migrations/20260814120000_add_follow_the_groove_v1.sql"
ROLLBACK = ROOT / "supabase/rollbacks/20260814120000_add_follow_the_groove_v1.rollback.sql"
PROVENANCE_FORWARD = ROOT / "supabase/migrations/20260815090000_add_ftg_last_seen_run_provenance.sql"
PROVENANCE_ROLLBACK = ROOT / "supabase/rollbacks/20260815090000_add_ftg_last_seen_run_provenance.rollback.sql"

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
        cls.provenance_forward = PROVENANCE_FORWARD.read_text(encoding="utf-8").lower()
        cls.provenance_rollback = PROVENANCE_ROLLBACK.read_text(encoding="utf-8").lower()

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

    def test_all_persistent_rows_have_created_and_last_seen_run_provenance(self) -> None:
        combined = self.forward + "\n" + self.provenance_forward
        for table in {
            "artists",
            "artist_aliases",
            "artist_edges",
            "artist_relation_evidence",
            "artist_similarity",
            "product_artists",
        }:
            table_start = combined.index(f"create table public.{table}")
            next_table = combined.find("create table public.", table_start + 1)
            table_definition = combined[table_start : next_table if next_table >= 0 else len(combined)]
            if table in {"artists", "artist_aliases", "artist_edges", "product_artists"}:
                table_definition += self.provenance_forward
            self.assertIn("created_by_run_id", table_definition, table)
            self.assertIn("last_seen_run_id", table_definition, table)

    def test_additive_provenance_columns_use_restricting_run_foreign_keys(self) -> None:
        for table in {"artists", "artist_aliases", "artist_edges", "product_artists"}:
            pattern = re.compile(
                rf"alter table public\.{table}\s+"
                rf"add column last_seen_run_id uuid,\s+"
                rf"add constraint {table}_last_seen_run_id_fkey\s+"
                r"foreign key \(last_seen_run_id\)\s+"
                r"references public\.follow_the_groove_collection_runs\(id\)\s+"
                r"on delete restrict;"
            )
            self.assertRegex(self.provenance_forward, pattern)

    def test_run_query_indexes_and_narrow_rollback_are_explicit(self) -> None:
        for table in {"artists", "artist_aliases", "artist_edges", "product_artists"}:
            self.assertIn(f"create index {table}_created_by_run_idx", self.provenance_forward)
            self.assertIn(f"create index {table}_last_seen_run_idx", self.provenance_forward)
            self.assertIn(f"alter table public.{table} drop column if exists last_seen_run_id", self.provenance_rollback)
        self.assertNotRegex(
            self.provenance_forward,
            re.compile(r"\b(?:insert|update|delete\s+from|truncate)\b"),
        )
        self.assertNotIn("drop table", self.provenance_rollback)
        self.assertIn("rollback geweigerd", self.provenance_rollback)


if __name__ == "__main__":
    unittest.main()
