from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


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


def main() -> int:
    parser = argparse.ArgumentParser()

    parser.add_argument("--listing-start-page", type=int, default=1)
    parser.add_argument("--listing-max-pages", type=int, default=5)
    parser.add_argument("--listing-sleep", type=float, default=1.5)
    parser.add_argument("--skip-listing-refresh", action="store_true")
    parser.add_argument(
        "--fast-price-sync",
        action="store_true",
        help="Gebruik snelle bulk price sync voor ROV listing refresh.",
    )
    parser.add_argument("--detail-limit", type=int, default=25)
    parser.add_argument("--stage-limit", type=int, default=25)
    parser.add_argument("--promote-limit", type=int, default=25)
    parser.add_argument("--quarantine-limit", type=int, default=100)

    parser.add_argument("--skip-detail", action="store_true")
    parser.add_argument("--skip-stage", action="store_true")
    parser.add_argument("--skip-promote", action="store_true")
    parser.add_argument("--skip-quarantine", action="store_true")

    parser.add_argument(
        "--write",
        action="store_true",
        help=(
            "Voer echte writes uit voor promotie en quarantine. "
            "Zonder --write blijven beide stappen dry-run."
        ),
    )

    args = parser.parse_args()

    load_env()

    if not os.environ.get("DATABASE_URL"):
        raise SystemExit(
            "[ERROR] DATABASE_URL ontbreekt. "
            "Zet DATABASE_URL in .env, .env.local of environment."
        )

    print("[PIPELINE] Sounds USF pipeline")
    print(f"[PIPELINE] listing_max_pages={args.listing_max_pages}")
    print(f"[PIPELINE] detail_limit={args.detail_limit}")
    print(f"[PIPELINE] stage_limit={args.stage_limit}")
    print(f"[PIPELINE] promote_limit={args.promote_limit}")
    print(f"[PIPELINE] quarantine_limit={args.quarantine_limit}")
    print(f"[PIPELINE] write={args.write}")
    print(f"[PIPELINE] fast_price_sync={args.fast_price_sync}")

    
    if not args.skip_listing_refresh:
        listing_command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.refresh_sounds_listing_prices",
            "--start-page",
            str(args.listing_start_page),
            "--max-pages",
            str(args.listing_max_pages),
            "--sleep",
            str(args.listing_sleep),
        ]
        if args.fast_price_sync:
            listing_command.append("--fast-price-sync")
        if args.write:
            listing_command.append("--write")
        run_step("refresh_sounds_listing_prices", listing_command)

    if not args.skip_detail:
        run_step(
            "detail_sounds",
            [
                sys.executable,
                "-m",
                "scripts.scrapers.usf.jobs.detail_sounds",
                "--limit",
                str(args.detail_limit),
            ],
        )

    if not args.skip_stage:
        run_step(
            "stage_sounds",
            [
                sys.executable,
                "-m",
                "scripts.scrapers.usf.jobs.stage_sounds",
                "--limit",
                str(args.stage_limit),
            ],
        )

    if not args.skip_promote:
        promote_command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.promote_sounds",
            "--limit",
            str(args.promote_limit),
        ]

        if args.write:
            promote_command.append("--write")

        run_step("promote_sounds", promote_command)

    if not args.skip_quarantine:
        quarantine_command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.quarantine_sounds",
            "--limit",
            str(args.quarantine_limit),
        ]

        if args.write:
            quarantine_command.append("--write")

        run_step("quarantine_sounds", quarantine_command)

    print("\n[PIPELINE] COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
