#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.fast_listing_price_sync import bulk_update_prices_from_link_registry


def load_env() -> None:
    for env_file in (".env.local", ".env"):
        path = Path(env_file)
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


def run_step(label: str, command: list[str]) -> None:
    print(f"\n[PIPELINE] START {label}", flush=True)
    print("[PIPELINE] CMD", " ".join(command), flush=True)
    subprocess.run(command, check=True)
    print(f"[PIPELINE] DONE {label}", flush=True)



def run_existing_listing_price_sync(*, write: bool) -> None:
    print("\n[PIPELINE] START fast_listing_price_sync_existing_registry", flush=True)
    with db_connection() as conn:
        stats = bulk_update_prices_from_link_registry(
            conn,
            shop_registry_id="soundsvenlo",
            shop_domain="sounds-venlo.nl",
            write=write,
            currency="EUR",
        )
    print("[PIPELINE] fast_listing_price_sync_existing_registry", vars(stats), flush=True)
    print("[PIPELINE] DONE fast_listing_price_sync_existing_registry", flush=True)


def build_listing_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "scripts.scrapers.usf.jobs.refresh_soundsvenlo_listing_prices",
        "--start-page",
        str(args.listing_start_page),
        "--max-pages",
        str(args.listing_max_pages),
        "--sleep",
        str(args.listing_sleep),
        "--max-page-failures",
        str(args.max_page_failures),
    ]

    if args.fast_price_sync:
        command.append("--fast-price-sync")
    if args.debug_listing:
        command.append("--debug")
    if args.write:
        command.append("--write")

    return command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Sounds Venlo USF pipeline.")
    parser.add_argument("--listing-start-page", type=int, default=1)
    parser.add_argument("--listing-max-pages", type=int, default=1)
    parser.add_argument("--listing-sleep", type=float, default=0.35)
    parser.add_argument("--max-page-failures", type=int, default=3)
    parser.add_argument("--debug-listing", action="store_true")
    parser.add_argument("--skip-listing", action="store_true")
    parser.add_argument("--fast-price-sync", action="store_true")
    parser.add_argument("--run-detail", action="store_true")
    parser.add_argument("--run-stage-promote", action="store_true")
    parser.add_argument("--detail-limit", type=int, default=500)
    parser.add_argument("--detail-sleep", type=float, default=0.50)
    parser.add_argument("--detail-retry-days", type=int, default=30)
    parser.add_argument("--stage-limit", type=int, default=500)
    parser.add_argument("--promote-limit", type=int, default=500)
    parser.add_argument("--quarantine-limit", type=int, default=100)
    parser.add_argument("--skip-stage", action="store_true")
    parser.add_argument("--skip-promote", action="store_true")
    parser.add_argument("--skip-quarantine", action="store_true")
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_env()

    if args.write and not os.environ.get("DATABASE_URL"):
        raise SystemExit(
            "[ERROR] DATABASE_URL ontbreekt. Zet DATABASE_URL in .env, .env.local of environment."
        )
    if args.listing_start_page < 1:
        raise SystemExit("[ERROR] --listing-start-page moet minimaal 1 zijn.")
    if args.listing_max_pages < 0:
        raise SystemExit("[ERROR] --listing-max-pages mag niet negatief zijn.")
    if args.listing_sleep < 0:
        raise SystemExit("[ERROR] --listing-sleep mag niet negatief zijn.")
    if args.detail_limit < 1:
        raise SystemExit("[ERROR] --detail-limit moet minimaal 1 zijn.")
    if args.detail_sleep < 0:
        raise SystemExit("[ERROR] --detail-sleep mag niet negatief zijn.")
    if args.detail_retry_days < 1:
        raise SystemExit("[ERROR] --detail-retry-days moet minimaal 1 zijn.")
    if args.stage_limit < 1 or args.promote_limit < 1 or args.quarantine_limit < 1:
        raise SystemExit("[ERROR] stage/promote/quarantine limits moeten minimaal 1 zijn.")

    print(
        "[PIPELINE] config",
        {
            "shop": "soundsvenlo",
            "listing_start_page": args.listing_start_page,
            "listing_max_pages": args.listing_max_pages,
            "fast_price_sync": args.fast_price_sync,
            "skip_listing": args.skip_listing,
            "run_detail": args.run_detail,
            "run_stage_promote": args.run_stage_promote,
            "detail_limit": args.detail_limit,
            "stage_limit": args.stage_limit,
            "promote_limit": args.promote_limit,
            "quarantine_limit": args.quarantine_limit,
            "write": args.write,
        },
        flush=True,
    )

    if args.skip_listing:
        print(
            "[PIPELINE] SKIP refresh_soundsvenlo_listing_prices",
            {"reason": "skip_listing=true"},
            flush=True,
        )
        if args.fast_price_sync:
            run_existing_listing_price_sync(write=args.write)
    else:
        run_step("refresh_soundsvenlo_listing_prices", build_listing_command(args))

    if args.run_detail:
        detail_command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.detail_soundsvenlo",
            "--limit",
            str(args.detail_limit),
            "--sleep",
            str(args.detail_sleep),
            "--retry-days",
            str(args.detail_retry_days),
        ]
        if args.write:
            detail_command.append("--write")
        run_step("detail_soundsvenlo", detail_command)

    if args.run_detail or args.run_stage_promote:
        if not args.skip_stage:
            stage_command = [
                sys.executable,
                "-m",
                "scripts.scrapers.usf.jobs.stage_soundsvenlo",
                "--limit",
                str(args.stage_limit),
            ]
            if args.write:
                stage_command.append("--write")
            run_step("stage_soundsvenlo", stage_command)

        if not args.skip_promote:
            promote_command = [
                sys.executable,
                "-m",
                "scripts.scrapers.usf.jobs.promote_soundsvenlo",
                "--limit",
                str(args.promote_limit),
            ]
            if args.write:
                promote_command.append("--write")
            run_step("promote_soundsvenlo", promote_command)

    if args.run_detail and not args.skip_quarantine:
        quarantine_command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.quarantine_soundsvenlo",
            "--limit",
            str(args.quarantine_limit),
        ]
        if args.write:
            quarantine_command.append("--write")
        run_step("quarantine_soundsvenlo", quarantine_command)

    print("\n[PIPELINE] COMPLETE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
