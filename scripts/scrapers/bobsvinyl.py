#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve().parent
if str(CURRENT) not in sys.path:
    sys.path.insert(0, str(CURRENT))

from _runner import ensure_dir, run_legacy
from bobsvinyl_refresh_known import run_refresh_known

DEFAULT_OUTPUT_DIR = "data/raw/bobsvinyl"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Automation wrapper voor Bob's Vinyl scraper")
    p.add_argument("--mode", choices=["step1", "step2", "both", "refresh-known"], default="both")
    p.add_argument("--workers", type=int, default=5)
    p.add_argument("--limit-detail", type=int, default=None, help="Max aantal detailpagina's in step2")
    p.add_argument("--stale-hours", type=float, default=20.0, help="Alleen known URLs verversen ouder dan dit aantal uur")
    p.add_argument("--limit-urls", type=int, default=None, help="Max aantal known URLs in refresh-known")
    p.add_argument("--max-pages", type=int, default=0, help="Max aantal /collections/all pagina's in refresh-known (0 = alles)")
    p.add_argument("--delay-seconds", type=float, default=0.25, help="Pauze tussen listingpagina's in refresh-known")
    p.add_argument("--state-file", default=None, help="Pad naar rotatiestate voor refresh-known (default: <output-dir>/bobsvinyl_refresh_state.json)")
    p.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    p.add_argument("--interactive", action="store_true")
    return p


def main() -> int:
    if len(sys.argv) == 1:
        return run_legacy("bobsvinyl_legacy.py")

    args = build_parser().parse_args()
    if args.interactive:
        return run_legacy("bobsvinyl_legacy.py")

    out_dir = ensure_dir(args.output_dir)

    if args.mode == "refresh-known":
        return run_refresh_known(
            output_dir=out_dir,
            workers=max(1, args.workers),
            stale_hours=args.stale_hours,
            limit_urls=args.limit_urls,
            max_pages=None if args.max_pages == 0 else max(1, args.max_pages),
            delay_seconds=max(0.0, args.delay_seconds),
            state_file=args.state_file,
        )

    choice = {"step1": "1", "step2": "2", "both": "3"}[args.mode]

    if args.mode == "step1":
        stdin_data = f"{choice}\n{max(1, args.workers)}\n"
    elif args.mode == "step2":
        limit_value = "" if args.limit_detail is None else str(max(1, args.limit_detail))
        stdin_data = f"{choice}\n{max(1, args.workers)}\n{limit_value}\n"
    else:
        stdin_data = f"{choice}\n{max(1, args.workers)}\n"

    return run_legacy("bobsvinyl_legacy.py", cwd=out_dir, stdin_data=stdin_data)


if __name__ == "__main__":
    raise SystemExit(main())
