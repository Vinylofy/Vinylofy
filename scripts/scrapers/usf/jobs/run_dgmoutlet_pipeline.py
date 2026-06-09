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
    print("[PIPELINE] CMD", " ".join(command), flush=True)

    subprocess.run(command, check=True)

    print(f"[PIPELINE] DONE {label}", flush=True)


def add_write_flag(command: list[str], write: bool) -> list[str]:
    if write:
        command.append("--write")

    return command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run de DGM Outlet USF-pipeline: discovery, "
            "listingmaterialisatie, staging en promotion."
        )
    )

    parser.add_argument("--discovery-start-page", type=int, default=1)
    parser.add_argument(
        "--discovery-max-pages",
        type=int,
        default=0,
        help=(
            "Aantal discoverypagina's. Gebruik 0 voor de volledige "
            "listing tot de eerste lege pagina."
        ),
    )
    parser.add_argument(
        "--discovery-delay",
        type=float,
        default=0.2,
    )

    parser.add_argument("--materialize-limit", type=int, default=100)
    parser.add_argument("--stage-limit", type=int, default=100)
    parser.add_argument("--promote-limit", type=int, default=100)

    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument("--skip-materialize", action="store_true")
    parser.add_argument("--skip-stage", action="store_true")
    parser.add_argument("--skip-promote", action="store_true")

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

    for name in (
        "materialize_limit",
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
            "Zet DATABASE_URL in .env, .env.local of environment."
        )

    print("[PIPELINE] DGM Outlet USF pipeline")
    print(
        f"[PIPELINE] discovery_start_page="
        f"{args.discovery_start_page}"
    )
    print(
        f"[PIPELINE] discovery_max_pages="
        f"{args.discovery_max_pages}"
    )
    print(
        f"[PIPELINE] materialize_limit="
        f"{args.materialize_limit}"
    )
    print(f"[PIPELINE] stage_limit={args.stage_limit}")
    print(f"[PIPELINE] promote_limit={args.promote_limit}")
    print(f"[PIPELINE] write={args.write}")

    if not args.skip_discovery:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.discover_dgmoutlet",
            "--start-page",
            str(args.discovery_start_page),
            "--max-pages",
            str(args.discovery_max_pages),
            "--delay",
            str(args.discovery_delay),
        ]

        run_step(
            "discover_dgmoutlet",
            add_write_flag(command, args.write),
        )

    if not args.skip_materialize:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs."
            "materialize_dgmoutlet_listing",
            "--limit",
            str(args.materialize_limit),
        ]

        run_step(
            "materialize_dgmoutlet_listing",
            add_write_flag(command, args.write),
        )

    if not args.skip_stage:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.stage_dgmoutlet",
            "--limit",
            str(args.stage_limit),
        ]

        run_step(
            "stage_dgmoutlet",
            add_write_flag(command, args.write),
        )

    if not args.skip_promote:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.promote_dgmoutlet",
            "--limit",
            str(args.promote_limit),
        ]

        run_step(
            "promote_dgmoutlet",
            add_write_flag(command, args.write),
        )

    print("\n[PIPELINE] COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
