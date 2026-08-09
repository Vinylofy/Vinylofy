from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
WORKER = ROOT / "scripts" / "maintenance" / "cover_worker.py"


class CoverWorkerSqlPlaceholderTests(unittest.TestCase):
    def test_parameterized_sql_has_no_unescaped_literal_percent(self) -> None:
        source = WORKER.read_text(encoding="utf-8")
        tree = ast.parse(source)

        escaped_storage_literal = (
            "'%%/storage/v1/object/public/product-covers/'"
        )
        unescaped_storage_literal = (
            "'%/storage/v1/object/public/product-covers/'"
        )

        self.assertEqual(source.count(escaped_storage_literal), 2)
        self.assertNotIn(unescaped_storage_literal, source)

        findings: list[tuple[int, int, str, str]] = []

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue

            func = node.func

            if not (
                isinstance(func, ast.Attribute)
                and func.attr == "execute"
            ):
                continue

            has_parameters = (
                len(node.args) >= 2
                or any(
                    keyword.arg in {"params", "parameters"}
                    for keyword in node.keywords
                )
            )

            if not has_parameters or not node.args:
                continue

            sql_node = node.args[0]

            if not (
                isinstance(sql_node, ast.Constant)
                and isinstance(sql_node.value, str)
            ):
                continue

            sql = sql_node.value
            i = 0

            while i < len(sql):
                if sql[i] != "%":
                    i += 1
                    continue

                next_char = (
                    sql[i + 1]
                    if i + 1 < len(sql)
                    else ""
                )

                if next_char in {"s", "b", "t", "%"}:
                    i += 2
                    continue

                relative_line = sql[:i].count("\n") + 1
                sql_text = sql.splitlines()[relative_line - 1].strip()

                findings.append(
                    (
                        node.lineno,
                        relative_line,
                        next_char,
                        sql_text,
                    )
                )

                i += 1

        self.assertEqual(
            findings,
            [],
            msg=(
                "Parameterized psycopg SQL contains unescaped "
                f"literal percent characters: {findings!r}"
            ),
        )


if __name__ == "__main__":
    unittest.main()
