#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve().parent
if str(CURRENT) not in sys.path:
    sys.path.insert(0, str(CURRENT))

from _runner import ensure_dir, run_legacy

DEFAULT_OUTPUT_DIR = "data/raw/platenzaak"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Automation wrapper voor Platenzaak scraper")
    p.add_argument("--mode", choices=["listing", "enrich", "both"], default="both")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--max-pages", type=int, default=None)
    p.add_argument("--limit-enrich", type=int, default=None, help="Max aantal productdetailpagina's in enrichment")
    p.add_argument("--force-all", action="store_true", help="Alleen relevant voor --mode enrich")
    p.add_argument("--interactive", action="store_true")
    return p


def main() -> int:
    if len(sys.argv) == 1:
        return run_legacy("platenzaak_legacy.py")

    args = build_parser().parse_args()
    if args.interactive:
        return run_legacy("platenzaak_legacy.py")

    out_dir = ensure_dir(args.output_dir)
    if args.mode == "listing":
        stdin_data = f"1\n{args.max_pages if args.max_pages else ''}\n"
    elif args.mode == "enrich":
        limit_value = "" if args.limit_enrich is None else str(max(1, args.limit_enrich))
        stdin_data = f"2\n{'j' if args.force_all else 'n'}\n{limit_value}\n"
    else:
        stdin_data = f"3\n{args.max_pages if args.max_pages else ''}\n"
    return run_legacy("platenzaak_legacy.py", cwd=out_dir, stdin_data=stdin_data)


if __name__ == "__main__":
    raise SystemExit(main())
