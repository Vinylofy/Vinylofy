from __future__ import annotations

import hashlib
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[3]
MAINTENANCE = ROOT / "scripts" / "maintenance"
if str(MAINTENANCE) not in sys.path:
    sys.path.insert(0, str(MAINTENANCE))

import cover_candidate_refresh as refresh
import cover_common as common

BAD_URL = "https://www.platomania.nl/fbmania.png"
BAD_SHA = "162468189d7fa6d6481f1c80ef1861bb45af5cb5ae6e5cd317e0a6c9aa5e4e18"
GOOD_URL = "https://www.platomania.nl/images/articles/123/456/album.webp"


def candidate(url: str) -> common.CandidateRecord:
    return common.CandidateRecord(
        product_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        ean="8712345678901",
        shop_id=None,
        shop_domain="platomania.nl",
        shop_name="Platomania",
        product_url="https://www.platomania.nl/article/123/example",
        image_url=url,
        source_type="og",
        source_rank=100,
    )


class CaptureCursor:
    def __init__(self) -> None:
        self.sql = ""
        self.params = []

    def execute(self, sql, params):
        self.sql = sql
        self.params = list(params)


class BlockedCoverAssetTests(unittest.TestCase):
    def test_exact_bad_url_is_blocked_but_normal_platomania_is_allowed(self):
        self.assertTrue(common.is_blocked_cover_url(BAD_URL))
        self.assertFalse(common.is_blocked_cover_url(GOOD_URL))
        self.assertFalse(common.is_blocked_cover_url(BAD_URL + "?unproven=1"))

    def test_bad_sha_is_blocked_but_other_sha_is_allowed(self):
        self.assertTrue(common.is_blocked_cover_sha256(BAD_SHA))
        self.assertFalse(common.is_blocked_cover_sha256("0" * 64))
        with self.assertRaises(common.BlockedCoverAssetError):
            common.reject_blocked_cover_sha256(BAD_SHA)

    def test_bad_insert_payload_is_terminal_rejected_and_unselected(self):
        payload = refresh.build_candidate_insert_payload(
            candidate(BAD_URL),
            {
                "product_id", "image_url", "candidate_status",
                "is_selected", "last_error_code", "last_error_message",
            },
        )
        self.assertEqual(payload["candidate_status"], "rejected")
        self.assertFalse(payload["is_selected"])
        self.assertEqual(
            payload["last_error_code"],
            common.BLOCKED_COVER_ERROR_CODE,
        )

    def test_normal_platomania_insert_remains_pending(self):
        payload = refresh.build_candidate_insert_payload(
            candidate(GOOD_URL), {"image_url", "candidate_status"}
        )
        self.assertEqual(payload["candidate_status"], "pending")

    def test_refresh_cannot_resurrect_existing_rejected_candidate(self):
        cursor = CaptureCursor()
        refresh.update_candidate_row(
            cursor,
            row_id="candidate-id",
            candidate=candidate(GOOD_URL),
            existing={"candidate_status": "rejected"},
            columns={"candidate_status"},
        )
        self.assertEqual(cursor.params[0], "rejected")

    def test_known_bad_sha_cannot_pass_content_guard_under_other_url(self):
        # The worker invokes this shared guard for downloaded bytes and for
        # the prepared WebP before any Storage upload/publication.
        self.assertNotEqual(hashlib.sha256(b"normal cover").hexdigest(), BAD_SHA)
        with self.assertRaises(common.BlockedCoverAssetError):
            common.reject_blocked_cover_sha256(BAD_SHA)

    def test_migration_has_cleanup_queue_and_database_guards(self):
        migration = (ROOT / "supabase/migrations/20260820120000_block_bad_cover_assets.sql").read_text()
        for token in (
            BAD_URL,
            BAD_SHA,
            "product_cover_candidates_blocklist_guard",
            "old.candidate_status = 'rejected'",
            "new.is_selected := false",
            "cover_storage_path = null",
            "cover_needs_refresh = true",
            "on conflict (product_id)",
            "status = case",
            "public.product_cover_queue.status = 'processing'",
        ):
            self.assertIn(token, migration)

        worker = (MAINTENANCE / "cover_worker.py").read_text()
        self.assertLess(
            worker.index("reject_blocked_cover_sha256(prepared.sha256)"),
            worker.index("upsert_bytes_to_storage(", worker.index("for selected in choices")),
        )


if __name__ == "__main__":
    unittest.main()
