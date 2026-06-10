#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys


PYTHON = sys.executable


def run_step(label: str, command: list[str]) -> None:
    print()
    print(f"[PIPELINE] START {label}", flush=True)
    print("[PIPELINE] command=", " ".join(command), flush=True)
    subprocess.run(command, check=True)
    print(f"[PIPELINE] DONE {label}", flush=True)


def add_write_flag(command: list[str], write: bool) -> list[str]:
    if write:
        command.append("--write")
    return command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Platenzaak USF pipeline: discovery, detail, staging en promotion."
    )

    parser.add_argument("--discovery-max-pages", type=int, default=5)
    parser.add_argument("--discovery-delay-seconds", type=float, default=0.25)
    parser.add_argument("--detail-limit", type=int, default=5)
    parser.add_argument("--stage-limit", type=int, default=5)
    parser.add_argument("--promote-limit", type=int, default=5)

    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument("--skip-detail", action="store_true")
    parser.add_argument("--skip-stage", action="store_true")
    parser.add_argument("--skip-promote", action="store_true")

    parser.add_argument(
        "--write",
        action="store_true",
        help="Voer echte registry-, raw-, staging- en publieke writes uit.",
    )

    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.discovery_max_pages < 0:
        raise SystemExit("[ERROR] --discovery-max-pages mag niet negatief zijn.")
    if args.discovery_delay_seconds < 0:
        raise SystemExit("[ERROR] --discovery-delay-seconds mag niet negatief zijn.")

    for option in ("detail_limit", "stage_limit", "promote_limit"):
        if getattr(args, option) < 1:
            raise SystemExit(f"[ERROR] --{option.replace('_', '-')} moet minimaal 1 zijn.")


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)

    print(
        "[PIPELINE] config",
        {
            "shop": "platenzaak",
            "discovery_max_pages": args.discovery_max_pages,
            "discovery_delay_seconds": args.discovery_delay_seconds,
            "detail_limit": args.detail_limit,
            "stage_limit": args.stage_limit,
            "promote_limit": args.promote_limit,
            "write": args.write,
            "skip_discovery": args.skip_discovery,
            "skip_detail": args.skip_detail,
            "skip_stage": args.skip_stage,
            "skip_promote": args.skip_promote,
        },
        flush=True,
    )

    if not args.skip_discovery:
        run_step(
            "discover_platenzaak",
            add_write_flag(
                [
                    PYTHON,
                    "-m",
                    "scripts.scrapers.usf.jobs.discover_platenzaak",
                    "--max-pages",
                    str(args.discovery_max_pages),
                    "--delay-seconds",
                    str(args.discovery_delay_seconds),
                ],
                args.write,
            ),
        )

    if not args.skip_detail:
        run_step(
            "detail_platenzaak",
            add_write_flag(
                [
                    PYTHON,
                    "-m",
                    "scripts.scrapers.usf.jobs.detail_platenzaak",
                    "--limit",
                    str(args.detail_limit),
                ],
                args.write,
            ),
        )

    if not args.skip_stage:
        run_step(
            "stage_platenzaak",
            add_write_flag(
                [
                    PYTHON,
                    "-m",
                    "scripts.scrapers.usf.jobs.stage_platenzaak",
                    "--limit",
                    str(args.stage_limit),
                ],
                args.write,
            ),
        )

    if not args.skip_promote:
        run_step(
            "promote_platenzaak",
            add_write_flag(
                [
                    PYTHON,
                    "-m",
                    "scripts.scrapers.usf.jobs.promote_platenzaak",
                    "--limit",
                    str(args.promote_limit),
                ],
                args.write,
            ),
        )

    print()
    print("[PIPELINE] complete", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
