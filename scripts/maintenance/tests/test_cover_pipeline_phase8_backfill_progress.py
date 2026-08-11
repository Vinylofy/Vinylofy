from __future__ import annotations

import argparse
import ast
from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[3]

MAINTENANCE = (
    ROOT
    / "scripts"
    / "maintenance"
)

if str(MAINTENANCE) not in sys.path:
    sys.path.insert(
        0,
        str(MAINTENANCE),
    )

import cover_backfill  # noqa: E402


DRIVER = (
    ROOT
    / "scripts"
    / "maintenance"
    / "cover_backfill.py"
)

WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "cover-backfill.yml"
)

WORKER = (
    ROOT
    / "scripts"
    / "maintenance"
    / "cover_worker.py"
)


class Phase8BackfillProgressTests(
    unittest.TestCase
):
    def make_args(self) -> argparse.Namespace:
        return argparse.Namespace(
            refresh_limit=100,
            publish_limit=100,
            max_batches=3,
            checkpoint="",
            include_covered=False,
            dry_run=False,
            output_dir=(
                "output/cover_pipeline/backfill"
            ),
            output_json=(
                "output/cover_pipeline/"
                "backfill_progress.json"
            ),
        )

    def test_driver_parses(self) -> None:
        ast.parse(
            DRIVER.read_text(
                encoding="utf-8"
            ),
            filename=str(DRIVER),
        )

    def test_candidate_command_forwards_checkpoint(
        self,
    ) -> None:
        args = self.make_args()

        checkpoint = (
            "00564cdc-9369-487b-ae8d-"
            "84240b22f591"
        )

        command = (
            cover_backfill
            .build_candidate_command(
                args,
                checkpoint=checkpoint,
                output_path=(
                    ROOT
                    / "output"
                    / "cover_pipeline"
                    / "backfill"
                    / "test.json"
                ),
            )
        )

        self.assertIn(
            "--checkpoint",
            command,
        )

        index = command.index(
            "--checkpoint"
        )

        self.assertEqual(
            command[index + 1],
            checkpoint,
        )

    def test_dry_run_reaches_both_children(
        self,
    ) -> None:
        args = self.make_args()
        args.dry_run = True

        refresh = (
            cover_backfill
            .build_candidate_command(
                args,
                checkpoint="",
                output_path=(
                    ROOT
                    / "output"
                    / "cover_pipeline"
                    / "backfill"
                    / "refresh.json"
                ),
            )
        )

        worker = (
            cover_backfill
            .build_worker_command(
                args,
                output_path=(
                    ROOT
                    / "output"
                    / "cover_pipeline"
                    / "backfill"
                    / "worker.json"
                ),
            )
        )

        self.assertIn(
            "--dry-run",
            refresh,
        )

        self.assertIn(
            "--dry-run",
            worker,
        )

    def test_worker_command_forces_missing_only(
        self,
    ) -> None:
        args = self.make_args()

        worker = (
            cover_backfill
            .build_worker_command(
                args,
                output_path=(
                    ROOT
                    / "output"
                    / "cover_pipeline"
                    / "backfill"
                    / "worker.json"
                ),
            )
        )

        self.assertIn(
            "--missing-only",
            worker,
        )

    def test_missing_only_skips_repair_paths(
        self,
    ) -> None:
        text = WORKER.read_text(
            encoding="utf-8"
        )

        self.assertIn(
            '"--missing-only"',
            text,
        )

        self.assertIn(
            "if not args.missing_only:\n"
            "                reconciliation = "
            "reconcile_local_products(",
            text,
        )

        self.assertIn(
            "if not args.missing_only:\n"
            "                    job = "
            "claim_one_local_repair(",
            text,
        )

    def test_driver_has_no_offset_pagination(
        self,
    ) -> None:
        text = DRIVER.read_text(
            encoding="utf-8"
        ).lower()

        self.assertNotIn(
            " offset ",
            text,
        )

    def test_checkpoint_is_persisted_before_publish(
        self,
    ) -> None:
        text = DRIVER.read_text(
            encoding="utf-8"
        )

        comment = (
            "persist the new keyset "
            "checkpoint BEFORE publish"
        )

        publish_call = (
            "build_worker_command("
        )

        self.assertIn(
            comment,
            text,
        )

        self.assertLess(
            text.index(comment),
            text.index(
                publish_call,
                text.index(comment),
            ),
        )

    def test_workflow_exposes_resume_controls(
        self,
    ) -> None:
        text = WORKFLOW.read_text(
            encoding="utf-8"
        )

        for token in (
            "max_batches:",
            "checkpoint:",
            "dry_run:",
            "--max-batches",
            "--checkpoint",
            "cover_backfill.py",
            "backfill_progress.json",
            "group: vinylofy-cover-pipeline",
            "cancel-in-progress: false",
        ):
            self.assertIn(
                token,
                text,
            )

    def test_backfill_has_temporary_bulk_schedule(
        self,
    ) -> None:
        text = WORKFLOW.read_text(
            encoding="utf-8"
        )

        self.assertIn(
            "workflow_dispatch:",
            text,
        )
        self.assertIn(
            "schedule:",
            text,
        )
        self.assertEqual(
            text.count("cron:"),
            10,
        )

        for token in (
            '"2026-08-12"',
            '"2026-08-21"',
            "scheduled-bulk",
            "gh run download",
            "resume_checkpoint",
            "refresh_limit=100",
            "publish_limit=100",
            "max_batches=10",
            "include_covered=false",
            "dry_run=false",
            "queue: max",
        ):
            self.assertIn(
                token,
                text,
            )


if __name__ == "__main__":
    unittest.main()
