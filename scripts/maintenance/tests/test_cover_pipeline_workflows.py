from __future__ import annotations

import ast
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS = ROOT / ".github" / "workflows"

REFRESH = WORKFLOWS / "cover-candidate-refresh.yml"
PUBLISH = WORKFLOWS / "cover-publish.yml"
BACKFILL = WORKFLOWS / "cover-backfill.yml"
SUMMARY_HELPER = (
    ROOT
    / "scripts"
    / "maintenance"
    / "cover_workflow_summary.py"
)


def source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


class CoverPipelineWorkflowTests(unittest.TestCase):
    def test_summary_helper_parses(self) -> None:
        self.assertTrue(SUMMARY_HELPER.is_file())
        ast.parse(
            source(SUMMARY_HELPER),
            filename=str(SUMMARY_HELPER),
        )

    def test_schedules_are_preserved(self) -> None:
        self.assertIn(
            'cron: "15 */6 * * *"',
            source(REFRESH),
        )
        self.assertIn(
            'cron: "45 */6 * * *"',
            source(PUBLISH),
        )

        backfill = source(BACKFILL)

        expected_bulk_crons = (
            'cron: "7 0 12-21 8 *"',
            'cron: "31 2 12-21 8 *"',
            'cron: "55 4 12-21 8 *"',
            'cron: "19 7 12-21 8 *"',
            'cron: "43 9 12-21 8 *"',
            'cron: "7 12 12-21 8 *"',
            'cron: "31 14 12-21 8 *"',
            'cron: "55 16 12-21 8 *"',
            'cron: "19 19 12-21 8 *"',
            'cron: "43 21 12-21 8 *"',
        )

        self.assertEqual(
            backfill.count("cron:"),
            len(expected_bulk_crons),
        )

        for cron in expected_bulk_crons:
            self.assertIn(
                cron,
                backfill,
            )

        self.assertIn(
            '"2026-08-12"',
            backfill,
        )
        self.assertIn(
            '"2026-08-21"',
            backfill,
        )

    def test_shared_concurrency(self) -> None:
        for path in (REFRESH, PUBLISH, BACKFILL):
            text = source(path)
            self.assertEqual(
                text.count(
                    "group: vinylofy-cover-pipeline"
                ),
                1,
                str(path),
            )
            self.assertIn(
                "cancel-in-progress: false",
                text,
                str(path),
            )
            self.assertIn(
                "queue: max",
                text,
                str(path),
            )

    def test_shared_requirements_are_preserved(self) -> None:
        accepted_tokens = (
            (
                "pip install --requirement "
                "requirements-cover.txt"
            ),
            (
                "python -m pip install -r "
                "requirements-cover.txt"
            ),
            (
                "python -m pip install --requirement "
                "requirements-cover.txt"
            ),
        )

        for path in (REFRESH, PUBLISH, BACKFILL):
            text = source(path)
            self.assertTrue(
                any(
                    token in text
                    for token in accepted_tokens
                ),
                str(path),
            )

    def test_entrypoints_are_preserved(self) -> None:
        self.assertIn(
            "scripts/maintenance/"
            "cover_candidate_refresh.py",
            source(REFRESH),
        )
        self.assertIn(
            "scripts/maintenance/cover_worker.py",
            source(PUBLISH),
        )

        backfill = source(BACKFILL)
        self.assertIn(
            "scripts/maintenance/"
            "cover_candidate_refresh.py",
            backfill,
        )
        self.assertIn(
            "scripts/maintenance/cover_worker.py",
            backfill,
        )

    def test_artifacts_and_summaries_exist(self) -> None:
        for path in (REFRESH, PUBLISH, BACKFILL):
            text = source(path)
            self.assertIn(
                "actions/upload-artifact@v5",
                text,
            )
            self.assertEqual(
                text.count("- name: Write run summary"),
                1,
                str(path),
            )
            self.assertIn(
                "GITHUB_STEP_SUMMARY",
                text,
                str(path),
            )

    def test_summary_redirect_is_attached_to_python_command(
        self,
    ) -> None:
        for path in (REFRESH, PUBLISH, BACKFILL):
            lines = source(path).splitlines()
            redirect_indexes = [
                index
                for index, line in enumerate(lines)
                if '>> "$GITHUB_STEP_SUMMARY"' in line
            ]

            self.assertEqual(
                len(redirect_indexes),
                1,
                str(path),
            )

            redirect_index = redirect_indexes[0]
            previous_line = lines[redirect_index - 1].rstrip()

            self.assertTrue(
                previous_line.endswith("\\"),
                (
                    f"{path}: GITHUB_STEP_SUMMARY-redirect "
                    "is niet gekoppeld aan het Python-commando"
                ),
            )


if __name__ == "__main__":
    unittest.main()
