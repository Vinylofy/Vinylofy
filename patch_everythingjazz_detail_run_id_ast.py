#!/usr/bin/env python3
from __future__ import annotations

import ast
import shutil
import sys
from datetime import datetime
from pathlib import Path


TARGET = Path("scripts/scrapers/usf/jobs/detail_everythingjazz.py")


def fail(message: str) -> None:
    print(f"[ERROR] {message}")
    raise SystemExit(1)


def absolute_byte_offset(lines: list[bytes], lineno: int, col_offset: int) -> int:
    # AST line numbers are 1-based; col offsets are UTF-8 byte offsets.
    return sum(len(line) for line in lines[: lineno - 1]) + col_offset


def call_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def main() -> int:
    if not TARGET.exists():
        fail(f"bestand ontbreekt: {TARGET}")

    original_bytes = TARGET.read_bytes()

    try:
        original_text = original_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        fail(f"bestand is geen geldige UTF-8: {exc}")

    try:
        tree = ast.parse(original_text, filename=str(TARGET))
    except SyntaxError as exc:
        fail(f"huidig bestand bevat al een syntaxfout: {exc}")

    matching_calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and call_name(node.func) == "insert_raw_shop_scrape":
            matching_calls.append(node)

    if len(matching_calls) != 1:
        fail(
            "verwacht exact één insert_raw_shop_scrape-call; "
            f"gevonden: {len(matching_calls)}"
        )

    call = matching_calls[0]
    run_id_keywords = [
        keyword
        for keyword in call.keywords
        if keyword.arg == "run_id"
    ]

    if len(run_id_keywords) != 1:
        fail(
            "verwacht exact één run_id-keyword in insert_raw_shop_scrape; "
            f"gevonden: {len(run_id_keywords)}"
        )

    value = run_id_keywords[0].value

    if isinstance(value, ast.Constant) and value.value is None:
        print("[INFO] run_id staat al op None; niets aangepast.")
        return 0

    required_positions = (
        value.lineno,
        value.col_offset,
        value.end_lineno,
        value.end_col_offset,
    )
    if any(position is None for position in required_positions):
        fail("AST bevat geen volledige bronposities voor run_id")

    lines = original_bytes.splitlines(keepends=True)
    start = absolute_byte_offset(lines, value.lineno, value.col_offset)
    end = absolute_byte_offset(lines, value.end_lineno, value.end_col_offset)

    old_fragment = original_bytes[start:end].decode("utf-8")
    patched_bytes = original_bytes[:start] + b"None" + original_bytes[end:]

    backup = Path(
        "/tmp/detail_everythingjazz.py.before-run-id-fix-"
        + datetime.now().strftime("%Y%m%d-%H%M%S")
    )
    shutil.copy2(TARGET, backup)

    TARGET.write_bytes(patched_bytes)

    try:
        py_compile.compile(str(TARGET), doraise=True)
    except Exception as exc:
        shutil.copy2(backup, TARGET)
        fail(f"compilecheck faalde; backup hersteld: {exc}")

    patched_text = TARGET.read_text(encoding="utf-8")
    patched_tree = ast.parse(patched_text, filename=str(TARGET))

    patched_values: list[ast.AST] = []
    for node in ast.walk(patched_tree):
        if isinstance(node, ast.Call) and call_name(node.func) == "insert_raw_shop_scrape":
            patched_values.extend(
                keyword.value
                for keyword in node.keywords
                if keyword.arg == "run_id"
            )

    if not (
        len(patched_values) == 1
        and isinstance(patched_values[0], ast.Constant)
        and patched_values[0].value is None
    ):
        shutil.copy2(backup, TARGET)
        fail("nacontrole faalde; backup hersteld")

    print(
        "[OK] run_id in insert_raw_shop_scrape gewijzigd",
        {
            "old_value": old_fragment,
            "new_value": "None",
            "backup": str(backup),
        },
    )
    print("[OK] Python compilecheck en AST-nacontrole geslaagd")
    print("[INFO] Geen databasewrite, commit of push uitgevoerd")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
