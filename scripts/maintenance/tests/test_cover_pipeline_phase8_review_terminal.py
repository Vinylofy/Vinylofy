from __future__ import annotations

from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[3]

WORKER = (
    ROOT
    / "scripts"
    / "maintenance"
    / "cover_worker.py"
)

MIGRATION = (
    ROOT
    / "supabase"
    / "migrations"
    / "20260810231500_stop_cover_review_reclaim.sql"
)


class Phase8ReviewTerminalContractTests(unittest.TestCase):
    def test_worker_does_not_treat_review_as_queueable(self) -> None:
        text = WORKER.read_text(encoding="utf-8")

        self.assertIn(
            'QUEUEABLE_STATUSES = ("pending", "retry_later")',
            text,
        )

        self.assertNotIn(
            'QUEUEABLE_STATUSES = ("pending", "retry_later", "review")',
            text,
        )

    def test_database_claim_rpc_does_not_claim_review(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")

        self.assertIn(
            "q.status in ('pending', 'retry_later')",
            text,
        )

        self.assertNotIn(
            "q.status in ('pending', 'retry_later', 'review')",
            text,
        )

    def test_claim_contract_keeps_exact_text_casts(self) -> None:
        text = MIGRATION.read_text(encoding="utf-8")

        for token in (
            "p.ean::text",
            "p.artist::text",
            "p.title::text",
            "p.format_label::text",
            "q.source_reason::text",
        ):
            self.assertIn(token, text)

    def test_review_status_still_exists_in_worker_logic(self) -> None:
        text = WORKER.read_text(encoding="utf-8")

        self.assertIn(
            'status="review"',
            text,
        )

        self.assertIn(
            '"no_candidates"',
            text,
        )


if __name__ == "__main__":
    unittest.main()
