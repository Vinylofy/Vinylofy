from __future__ import annotations

import unittest
from dataclasses import dataclass, field
from unittest.mock import patch

from scripts.release_discovery import (
    discover_bobsvinyl_releases as legacy,
)
from scripts.release_discovery.jobs import (
    discover_bobsvinyl as job,
)


@dataclass
class FakeResponse:
    status_code: int
    text: str = ""
    reason: str = ""
    headers: dict[str, str] = field(default_factory=dict)


MODULES = (legacy, job)


class ReleaseDiscoveryHttpTests(unittest.TestCase):
    def test_retryable_statuses_retry_then_succeed(self) -> None:
        for module in MODULES:
            for status_code in (429, 502, 503, 504):
                with self.subTest(
                    module=module.__name__,
                    status_code=status_code,
                ):
                    responses = [
                        FakeResponse(
                            status_code=status_code,
                            reason="temporary",
                            headers={"Retry-After": "2"},
                        ),
                        FakeResponse(
                            status_code=200,
                            text="<html>ok</html>",
                            reason="OK",
                        ),
                    ]

                    with (
                        patch.object(
                            module.requests,
                            "get",
                            side_effect=responses,
                        ) as get_mock,
                        patch.object(
                            module.time,
                            "sleep",
                        ) as sleep_mock,
                    ):
                        result = module.fetch(
                            "https://example.test/item",
                            0,
                        )

                    self.assertEqual(
                        result,
                        "<html>ok</html>",
                    )
                    self.assertEqual(
                        get_mock.call_count,
                        2,
                    )
                    sleep_mock.assert_called_once_with(2.0)

    def test_permanent_404_is_not_retried(self) -> None:
        for module in MODULES:
            with self.subTest(module=module.__name__):
                with (
                    patch.object(
                        module.requests,
                        "get",
                        return_value=FakeResponse(
                            status_code=404,
                            reason="Not Found",
                        ),
                    ) as get_mock,
                    patch.object(
                        module.time,
                        "sleep",
                    ) as sleep_mock,
                ):
                    result = module.fetch(
                        "https://example.test/missing",
                        0.5,
                    )

                self.assertIsNone(result)
                self.assertEqual(
                    get_mock.call_count,
                    1,
                )
                sleep_mock.assert_not_called()

    def test_429_exhausts_after_three_attempts(self) -> None:
        for module in MODULES:
            with self.subTest(module=module.__name__):
                responses = [
                    FakeResponse(
                        status_code=429,
                        reason="Too Many Requests",
                    ),
                    FakeResponse(
                        status_code=429,
                        reason="Too Many Requests",
                    ),
                    FakeResponse(
                        status_code=429,
                        reason="Too Many Requests",
                    ),
                ]

                with (
                    patch.object(
                        module.requests,
                        "get",
                        side_effect=responses,
                    ) as get_mock,
                    patch.object(
                        module.time,
                        "sleep",
                    ) as sleep_mock,
                ):
                    result = module.fetch(
                        "https://example.test/limited",
                        0.5,
                    )

                self.assertIsNone(result)
                self.assertEqual(
                    get_mock.call_count,
                    3,
                )
                self.assertEqual(
                    [call.args[0] for call in sleep_mock.call_args_list],
                    [1.0, 2.0],
                )


if __name__ == "__main__":
    unittest.main()
