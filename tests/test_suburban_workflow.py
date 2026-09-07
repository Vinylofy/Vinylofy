from pathlib import Path
import unittest


WORKFLOW = Path(".github/workflows/usf-suburban.yml")


class SuburbanWorkflowTest(unittest.TestCase):
    def test_schedule_runs_the_database_write_path(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('if [ "${GITHUB_EVENT_NAME}" = "schedule" ]; then', workflow)
        self.assertIn('            WRITE="true"', workflow)
        self.assertIn("python -m scripts.importers.import_suburban ", workflow)
        self.assertIn("              /tmp/suburban-output/suburban_master.csv", workflow)

    def test_manual_run_keeps_explicit_write_input(self) -> None:
        workflow = WORKFLOW.read_text(encoding="utf-8")
        self.assertIn('            WRITE="${{ inputs.write }}"', workflow)
        self.assertIn("              /tmp/suburban-output/suburban_master.csv ", workflow)
        self.assertIn("              --dry-run", workflow)


if __name__ == "__main__":
    unittest.main()
