#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve().parent
if str(CURRENT) not in sys.path:
    sys.path.insert(0, str(CURRENT))

from _runner import ensure_dir, run_legacy

DEFAULT_OUTPUT_DIR = "data/raw/soundsvenlo"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Automation wrapper voor Sounds Venlo scraper")
    p.add_argument("--mode", choices=["step1", "step2", "both"], default="both")
    p.add_argument("--limit-detail", type=int, default=None, help="Max aantal detailpagina's in step2")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--interactive", action="store_true")
    return p


def main() -> int:
    if len(sys.argv) == 1:
        return run_legacy("soundsvenlo_legacy.py")

    args = build_parser().parse_args()
    if args.interactive:
        return run_legacy("soundsvenlo_legacy.py")

    out_dir = ensure_dir(args.output_dir)
    choice = {"step1": "1", "step2": "2", "both": "3"}[args.mode]
    limit_value = "" if args.limit_detail is None else str(max(1, args.limit_detail))

    if args.mode == "step1":
        stdin_data = f"{choice}\n"
    else:
        stdin_data = f"{choice}\n{limit_value}\n"

    return run_legacy("soundsvenlo_legacy.py", cwd=out_dir, stdin_data=stdin_data)


if __name__ == "__main__":
    raise SystemExit(main())
