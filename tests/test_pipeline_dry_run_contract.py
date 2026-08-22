from __future__ import annotations

import argparse
import unittest
from unittest.mock import MagicMock, patch

from scripts.automation import run_vinylofy_pipeline as pipeline


def args(*, dry_run_import: bool) -> argparse.Namespace:
    return argparse.Namespace(
        shop="atthemovies",
        skip_scrape=True,
        skip_import=False,
        skip_upload=True,
        dry_run_import=dry_run_import,
        summary_out="/tmp/pipeline-contract-summary.json",
    )


class PipelineDryRunContractTest(unittest.TestCase):
    def test_dry_run_does_not_open_monitoring_database_connection(self):
        with (
            patch.object(pipeline, "parse_args", return_value=args(dry_run_import=True)),
            patch.object(pipeline, "maybe_open_logging_connection") as open_monitoring,
            patch.object(pipeline, "resolve_shops", return_value=[]),
            patch.object(pipeline, "write_summary"),
        ):
            self.assertEqual(pipeline.main(), 0)
        open_monitoring.assert_not_called()

    def test_non_dry_run_keeps_monitoring_enabled(self):
        connection = MagicMock()
        with (
            patch.object(pipeline, "parse_args", return_value=args(dry_run_import=False)),
            patch.object(pipeline, "maybe_open_logging_connection", return_value=connection) as open_monitoring,
            patch.object(pipeline, "resolve_shops", return_value=[]),
            patch.object(pipeline, "write_summary"),
        ):
            self.assertEqual(pipeline.main(), 0)
        open_monitoring.assert_called_once_with()
        connection.close.assert_called_once_with()

    def test_atthemovies_is_explicit_only_and_does_not_change_all_runs(self):
        all_keys = {shop.key for shop in pipeline.resolve_shops("all")}
        self.assertNotIn("atthemovies", all_keys)
        self.assertEqual(pipeline.resolve_shops("atthemovies")[0].key, "atthemovies")


if __name__ == "__main__":
    unittest.main()
