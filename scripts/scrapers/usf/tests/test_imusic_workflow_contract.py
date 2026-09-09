from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
MAIN_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "usf-imusic.yml"
EXPOSURE_WORKFLOW = (
    REPO_ROOT / ".github" / "workflows" / "usf-imusic-exposures.yml"
)


class ImusicWorkflowContractTests(unittest.TestCase):
    def test_imusic_writers_share_one_concurrency_group(self):
        main = MAIN_WORKFLOW.read_text(encoding="utf-8")
        exposures = EXPOSURE_WORKFLOW.read_text(encoding="utf-8")

        concurrency_group = "group: usf-imusic-production"
        self.assertIn(concurrency_group, main)
        self.assertIn(concurrency_group, exposures)
        self.assertIn("cancel-in-progress: false", main)
        self.assertIn("cancel-in-progress: false", exposures)


if __name__ == "__main__":
    unittest.main()
