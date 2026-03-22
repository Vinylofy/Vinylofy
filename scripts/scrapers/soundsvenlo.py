
#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve().parent
LEGACY_DIR = CURRENT / "legacy"

for path in (CURRENT, LEGACY_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import soundsvenlo_legacy  # noqa: E402

DEFAULT_OUTPUT_DIR = "data/raw/soundsvenlo"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Automation wrapper voor Sounds Venlo scraper")
    parser.add_argument("--mode", choices=["step1", "step2", "both"], default="both")
    parser.add_argument("--limit-detail", type=int, default=None, help="Max aantal detailpagina's in step2")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--interactive", action="store_true")
    parser.add_argument("--headful", action="store_true", help="Run browser headed instead of headless")
    return parser


def main() -> int:
    if len(sys.argv) == 1:
        return soundsvenlo_legacy.main([])

    args = build_parser().parse_args()

    argv: list[str] = [
        "--mode",
        args.mode,
        "--output-dir",
        args.output_dir,
    ]
    if args.limit_detail is not None:
        argv += ["--limit-detail", str(args.limit_detail)]
    if args.headful:
        argv.append("--headful")

    return soundsvenlo_legacy.main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
