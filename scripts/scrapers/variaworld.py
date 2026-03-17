#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve().parent
if str(CURRENT) not in sys.path:
    sys.path.insert(0, str(CURRENT))

from _runner import ensure_dir, move_if_exists, run_legacy

DEFAULT_OUTPUT_DIR = "data/raw/variaworld"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Automation wrapper voor Variaworld scraper")
    p.add_argument("--mode", choices=["listing", "ean", "both"], default="both")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--interactive", action="store_true")
    return p


def main() -> int:
    if len(sys.argv) == 1:
        return run_legacy("variaworld_legacy.py")

    args = build_parser().parse_args()
    if args.interactive:
        return run_legacy("variaworld_legacy.py")

    out_dir = ensure_dir(args.output_dir)
    choice = {"listing": "1", "ean": "2", "both": "3"}[args.mode]
    rc = run_legacy("variaworld_legacy.py", cwd=out_dir, stdin_data=f"{choice}\n4\n")
    move_if_exists(out_dir / "output" / "variaworld_products.csv", out_dir / "variaworld_products.csv")
    move_if_exists(out_dir / "output" / "variaworld_errors.csv", out_dir / "variaworld_errors.csv")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
