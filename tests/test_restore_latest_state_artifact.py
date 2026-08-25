from __future__ import annotations

import unittest
from unittest.mock import Mock

from scripts.automation.restore_latest_state_artifact import (
    find_workflow_id,
    iter_successful_runs,
)


class RestoreLatestStateArtifactTest(unittest.TestCase):
    def test_workflow_runs_are_resolved_and_paginated_before_filtering(self):
        session = Mock()
        responses = {
            "https://api.github.com/repos/acme/shop/actions/workflows?per_page=2&page=1": {
                "workflows": [{"id": 17, "name": "Shop - Get Back Music"}],
            },
            "https://api.github.com/repos/acme/shop/actions/workflows/17/runs?status=completed&per_page=2&page=1": {
                "workflow_runs": [
                    {"id": 1, "conclusion": "failure"},
                    {"id": 2, "conclusion": "success"},
                ],
            },
            "https://api.github.com/repos/acme/shop/actions/workflows/17/runs?status=completed&per_page=2&page=2": {
                "workflow_runs": [{"id": 3, "conclusion": "success"}],
            },
        }

        def get(url, timeout=30):
            response = Mock()
            response.json.return_value = responses[url]
            response.raise_for_status.return_value = None
            return response

        session.get.side_effect = get

        self.assertEqual(find_workflow_id(session, "acme/shop", "Shop - Get Back Music", per_page=2), 17)
        runs = list(iter_successful_runs(session, "acme/shop", "Shop - Get Back Music", per_page=2))
        self.assertEqual([run["id"] for run in runs], [2, 3])


if __name__ == "__main__":
    unittest.main()
