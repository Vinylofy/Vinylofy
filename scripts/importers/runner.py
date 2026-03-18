#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.importers.common import run_import  # noqa: E402
from scripts.importers.contracts import ShopImporterDefinition, SourceValidationResult  # noqa: E402


def read_csv_headers(csv_path: Path) -> tuple[str, ...]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        try:
            first_row = next(reader)
        except StopIteration as exc:
            raise SystemExit(f"CSV is leeg: {csv_path}") from exc
    return tuple(str(item).strip() for item in first_row if str(item).strip())


def validate_source_file(
    definition: ShopImporterDefinition,
    csv_path: Path,
) -> SourceValidationResult:
    headers = read_csv_headers(csv_path)
    header_set = {header.strip() for header in headers}
    missing = tuple(column for column in definition.required_columns if column not in header_set)
    return SourceValidationResult(
        csv_path=str(csv_path),
        headers=headers,
        missing_required_columns=missing,
    )


def build_parser(definition: ShopImporterDefinition) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            definition.description
            or f"Import {definition.shop_name} CSV into Supabase/Postgres"
        )
    )
    parser.add_argument(
        "csv_path",
        nargs="?",
        default=str(PROJECT_ROOT / definition.files.csv_output_path),
        help=f"Path to source CSV (default: {definition.files.csv_output_path})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and summarize without writing to the database",
    )
    parser.add_argument(
        "--rejects",
        default=str(PROJECT_ROOT / definition.files.rejects_path),
        help="Path to write rejected rows CSV",
    )
    parser.add_argument(
        "--summary",
        default=str(PROJECT_ROOT / definition.files.summary_path),
        help="Path to write summary JSON",
    )
    parser.add_argument(
        "--skip-header-validation",
        action="store_true",
        help="Skip pre-flight validation of required source columns",
    )
    return parser


def run_registered_importer(definition: ShopImporterDefinition) -> None:
    parser = build_parser(definition)
    args = parser.parse_args()

    csv_path = Path(args.csv_path)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    if definition.before_run is not None:
        definition.before_run(str(csv_path))

    if not args.skip_header_validation and definition.required_columns:
        validation = validate_source_file(definition, csv_path)
        if not validation.ok:
            available = ", ".join(validation.headers) if validation.headers else "<geen headers>"
            missing = ", ".join(validation.missing_required_columns)
            raise SystemExit(
                f"Bronbestand mist verplichte kolommen voor {definition.key}: {missing}. "
                f"Aanwezige headers: {available}"
            )

    run_import(
        config=definition.config,
        csv_path=str(csv_path),
        row_mapper=definition.row_mapper,
        dry_run=args.dry_run,
        rejects_path=args.rejects,
        summary_path=args.summary,
    )
