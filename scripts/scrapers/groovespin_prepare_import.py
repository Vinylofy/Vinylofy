#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path

FINAL_PATTERN = re.compile(r"^groovespin_albums_\d{8}T\d{6}Z\.csv$")
DEFAULT_INPUT_DIR = "data/raw/groovespin"
DEFAULT_OUTPUT = "data/raw/groovespin/groovespin_products.csv"
CANONICAL_COLUMNS = [
    "timestamp",
    "master_id",
    "url",
    "artist",
    "title",
    "year",
    "price_raw",
    "price_eur",
    "ean",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Prepare latest Groovespin final CSV for importer")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser



def find_latest_final_csv(input_dir: Path) -> Path:
    candidates = [
        path
        for path in input_dir.glob("groovespin_albums_*.csv")
        if FINAL_PATTERN.match(path.name)
    ]
    if not candidates:
        raise SystemExit(f"Geen Groovespin final CSV gevonden in: {input_dir}")
    return max(candidates, key=lambda path: path.stat().st_mtime)



def convert_to_canonical(latest_csv: Path, output_csv: Path) -> int:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0
    with latest_csv.open("r", encoding="utf-8", newline="") as source_handle, output_csv.open(
        "w", encoding="utf-8", newline=""
    ) as target_handle:
        reader = csv.DictReader(source_handle, delimiter="~")
        writer = csv.DictWriter(target_handle, fieldnames=CANONICAL_COLUMNS)
        writer.writeheader()
        for row in reader:
            writer.writerow({column: row.get(column, "") for column in CANONICAL_COLUMNS})
            row_count += 1
    return row_count



def main() -> int:
    args = build_parser().parse_args()
    input_dir = Path(args.input_dir)
    output_csv = Path(args.output)
    latest_csv = find_latest_final_csv(input_dir)
    rows = convert_to_canonical(latest_csv, output_csv)
    print(f"[groovespin-prepare] source={latest_csv}")
    print(f"[groovespin-prepare] output={output_csv}")
    print(f"[groovespin-prepare] rows={rows}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
