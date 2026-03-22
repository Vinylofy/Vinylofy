#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
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
    p.add_argument("--limit-ean", type=int, default=None, help="Max aantal EAN-detailpagina's")
    p.add_argument("--interactive", action="store_true")
    return p


def _stage_existing_master_csv(out_dir: Path) -> None:
    '''
    Legacy scraper verwacht altijd output/variaworld_products.csv relatief aan cwd.
    Onze wrapper verplaatst dat bestand na afloop juist naar <out_dir>/variaworld_products.csv.
    Daarom moeten we vóór een ean/both run het bestand terug instagen naar output/.
    '''
    output_dir = out_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    canonical_csv = out_dir / "variaworld_products.csv"
    legacy_csv = output_dir / "variaworld_products.csv"

    if canonical_csv.exists() and not legacy_csv.exists():
        shutil.copy2(canonical_csv, legacy_csv)

    canonical_errors = out_dir / "variaworld_errors.csv"
    legacy_errors = output_dir / "variaworld_errors.csv"
    if canonical_errors.exists() and not legacy_errors.exists():
        shutil.copy2(canonical_errors, legacy_errors)


def main() -> int:
    if len(sys.argv) == 1:
        return run_legacy("variaworld_legacy.py")

    args = build_parser().parse_args()

    if args.interactive:
        return run_legacy("variaworld_legacy.py")

    out_dir = ensure_dir(args.output_dir)

    # Zorg dat stap 2 altijd hetzelfde bronbestand terugvindt
    if args.mode in {"ean", "both"}:
        _stage_existing_master_csv(out_dir)

    choice = {"listing": "1", "ean": "2", "both": "3"}[args.mode]

    if args.mode == "ean":
        limit_value = "" if args.limit_ean is None else str(max(1, args.limit_ean))
        stdin_data = f"{choice}\n{limit_value}\n4\n"
    else:
        stdin_data = f"{choice}\n4\n"

    rc = run_legacy("variaworld_legacy.py", cwd=out_dir, stdin_data=stdin_data)

    move_if_exists(out_dir / "output" / "variaworld_products.csv", out_dir / "variaworld_products.csv")
    move_if_exists(out_dir / "output" / "variaworld_errors.csv", out_dir / "variaworld_errors.csv")

    return rc


if __name__ == "__main__":
    raise SystemExit(main())
