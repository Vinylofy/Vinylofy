#!/usr/bin/env python3
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
    subprocess.run(command, check=True)
    print(f"[PIPELINE] DONE {label}", flush=True)


def add_write_flag(
    command: list[str],
    write: bool,
) -> list[str]:
    if write:
        command.append("--write")

    return command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run de Bob's Vinyl USF-pipeline: discovery, "
            "EAN-detailverrijking, staging en promotion."
        )
    )

    parser.add_argument(
        "--discovery-start-page",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--discovery-max-pages",
        type=int,
        default=2,
        help=(
            "Veilige standaard is 2 pagina's. "
            "Gebruik 0 voor de volledige catalogus."
        ),
    )
    parser.add_argument(
        "--discovery-delay-seconds",
        type=float,
        default=0.20,
    )

    parser.add_argument(
        "--detail-limit",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--detail-sleep",
        type=float,
        default=0.50,
    )
    parser.add_argument(
        "--stage-limit",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--promote-limit",
        type=int,
        default=5,
    )

    parser.add_argument(
        "--skip-discovery",
        action="store_true",
    )
    parser.add_argument(
        "--skip-detail",
        action="store_true",
    )
    parser.add_argument(
        "--skip-stage",
        action="store_true",
    )
    parser.add_argument(
        "--skip-promote",
        action="store_true",
    )

    parser.add_argument(
        "--write",
        action="store_true",
        help=(
            "Voer echte registry-, raw-, staging- en publieke writes uit. "
            "Zonder --write blijven alle stappen in dry-runmodus."
        ),
    )

    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.discovery_start_page < 1:
        raise SystemExit(
            "[ERROR] --discovery-start-page moet minimaal 1 zijn."
        )

    if args.discovery_max_pages < 0:
        raise SystemExit(
            "[ERROR] --discovery-max-pages mag niet negatief zijn."
        )

    if args.discovery_delay_seconds < 0:
        raise SystemExit(
            "[ERROR] --discovery-delay-seconds mag niet negatief zijn."
        )

    if args.detail_sleep < 0:
        raise SystemExit(
            "[ERROR] --detail-sleep mag niet negatief zijn."
        )

    for name in (
        "detail_limit",
        "stage_limit",
        "promote_limit",
    ):
        value = getattr(args, name)

        if value < 1:
            option = name.replace("_", "-")
            raise SystemExit(
                f"[ERROR] --{option} moet minimaal 1 zijn."
            )


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)
    load_env()

    if not os.environ.get("DATABASE_URL"):
        raise SystemExit(
            "[ERROR] DATABASE_URL ontbreekt. "
            "Zet DATABASE_URL in .env.local, .env of environment."
        )

    print(
        "[PIPELINE]",
        {
            "shop": "bobsvinyl",
            "discovery_start_page": args.discovery_start_page,
            "discovery_max_pages": args.discovery_max_pages,
            "detail_limit": args.detail_limit,
            "stage_limit": args.stage_limit,
            "promote_limit": args.promote_limit,
            "write": args.write,
        },
        flush=True,
    )

    if not args.skip_discovery:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.refresh_bobsvinyl_listing_prices",
            "--start-page",
            str(args.discovery_start_page),
            "--max-pages",
            str(args.discovery_max_pages),
            "--delay-seconds",
            str(args.discovery_delay_seconds),
        ]

        run_step(
            "refresh_bobsvinyl_listing_prices",
            add_write_flag(command, args.write),
        )

    if not args.skip_detail:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.detail_bobsvinyl",
            "--limit",
            str(args.detail_limit),
            "--sleep",
            str(args.detail_sleep),
        ]

        run_step(
            "detail_bobsvinyl",
            add_write_flag(command, args.write),
        )

    if not args.skip_stage:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.stage_bobsvinyl",
            "--limit",
            str(args.stage_limit),
        ]

        run_step(
            "stage_bobsvinyl",
            add_write_flag(command, args.write),
        )

    if not args.skip_promote:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.promote_bobsvinyl",
            "--limit",
            str(args.promote_limit),
        ]

        run_step(
            "promote_bobsvinyl",
            add_write_flag(command, args.write),
        )

    print("\n[PIPELINE] COMPLETE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
