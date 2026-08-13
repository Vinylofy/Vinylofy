from __future__ import annotations

import ast
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[3]
WORKER = ROOT / "scripts/maintenance/cover_worker.py"
RECOVERY = ROOT / ".github/workflows/cover-recovery.yml"


class CoverRecoveryContractTests(unittest.TestCase):
    def test_worker_has_candidate_pin(self) -> None:
        text = WORKER.read_text(encoding="utf-8")

        self.assertIn('"--candidate-id"', text)
        self.assertIn(
            '"--candidate-id vereist --product-id."',
            text,
        )
        self.assertIn(
            "def require_pinned_candidate(",
            text,
        )
        self.assertIn(
            "candidate_id=pinned_candidate_id",
            text,
        )
        self.assertIn(
            "target_candidate_id",
            text,
        )
        self.assertIn(
            "image_url_contains_exact_ean",
            text,
        )
        self.assertIn(
            "Pinned covercandidate mist exact EAN-bewijs",
            text,
        )

    def test_exact_ean_url_helper(self) -> None:
        tree = ast.parse(
            WORKER.read_text(encoding="utf-8")
        )

        helper = next(
            node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
            and node.name == "image_url_contains_exact_ean"
        )

        module = ast.Module(
            body=[helper],
            type_ignores=[],
        )
        ast.fix_missing_locations(module)

        namespace = {
            "re": re,
            "normalize_text": lambda value: str(value or ""),
            "normalize_ean": lambda value: (
                "".join(
                    ch
                    for ch in str(value or "")
                    if ch.isdigit()
                )
                or None
            ),
        }

        exec(
            compile(
                module,
                filename=str(WORKER),
                mode="exec",
            ),
            namespace,
        )

        fn = namespace["image_url_contains_exact_ean"]

        self.assertTrue(
            fn(
                "https://shop.test/0198028071413.jpg",
                "0198028071413",
            )
        )
        self.assertTrue(
            fn(
                "https://shop.test/x-0198028071413-cover.webp",
                "0198028071413",
            )
        )
        self.assertFalse(
            fn(
                "https://shop.test/999019802807141399.jpg",
                "0198028071413",
            )
        )
        self.assertFalse(
            fn(
                "https://shop.test/0602458870725.jpg",
                "0198028071413",
            )
        )

    def test_recovery_workflow_is_manual_only(self) -> None:
        text = RECOVERY.read_text(encoding="utf-8")

        self.assertIn("workflow_dispatch:", text)
        self.assertNotIn("schedule:", text)
        self.assertIn(
            "group: vinylofy-cover-recovery",
            text,
        )
        self.assertNotIn(
            "group: vinylofy-cover-pipeline",
            text,
        )

    def test_recovery_workflow_pins_both_ids(self) -> None:
        text = RECOVERY.read_text(encoding="utf-8")

        self.assertIn("product_id:", text)
        self.assertIn("candidate_id:", text)
        self.assertIn(
            '--product-id "$PRODUCT_ID"',
            text,
        )
        self.assertIn(
            '--candidate-id "$CANDIDATE_ID"',
            text,
        )
        self.assertIn(
            "--limit 1",
            text,
        )


if __name__ == "__main__":
    unittest.main()
