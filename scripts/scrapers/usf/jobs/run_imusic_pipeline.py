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
            "Run de iMusic USF-pipeline: EAN seed, stale requeue, "
            "detail lookup, staging, promotion en quarantine."
        )
    )

    parser.add_argument("--seed-limit", type=int, default=100)
    parser.add_argument("--requeue-stale-hours", type=float, default=24.0)
    parser.add_argument("--requeue-limit", type=int, default=500)
    parser.add_argument("--requeue-target-queue", type=int, default=500)
    parser.add_argument("--detail-limit", type=int, default=100)
    parser.add_argument("--detail-timeout", type=float, default=20.0)
    parser.add_argument("--detail-sleep", type=float, default=0.25)
    parser.add_argument("--stage-limit", type=int, default=100)
    parser.add_argument("--promote-limit", type=int, default=100)
    parser.add_argument("--quarantine-limit", type=int, default=100)

    parser.add_argument("--skip-seed", action="store_true")
    parser.add_argument("--skip-requeue", action="store_true")
    parser.add_argument("--skip-detail", action="store_true")
    parser.add_argument("--skip-stage", action="store_true")
    parser.add_argument("--skip-promote", action="store_true")
    parser.add_argument("--skip-quarantine", action="store_true")

    parser.add_argument(
        "--write",
        action="store_true",
        help="Voer echte registry-, raw-, staging- en publieke writes uit.",
    )

    return parser


def validate_args(args: argparse.Namespace) -> None:
    for name in (
        "seed_limit",
        "requeue_limit",
        "requeue_target_queue",
        "detail_limit",
        "stage_limit",
        "promote_limit",
        "quarantine_limit",
    ):
        value = getattr(args, name)
        if value < 1:
            option = name.replace("_", "-")
            raise SystemExit(f"[ERROR] --{option} moet minimaal 1 zijn.")

    if args.requeue_stale_hours < 0:
        raise SystemExit("[ERROR] --requeue-stale-hours mag niet negatief zijn.")
    if args.detail_timeout <= 0:
        raise SystemExit("[ERROR] --detail-timeout moet positief zijn.")
    if args.detail_sleep < 0:
        raise SystemExit("[ERROR] --detail-sleep mag niet negatief zijn.")


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)
    load_env()

    if not os.environ.get("DATABASE_URL"):
        raise SystemExit(
            "[ERROR] DATABASE_URL ontbreekt. "
            "Zet DATABASE_URL in .env, .env.local of environment."
        )

    print("[PIPELINE] iMusic USF pipeline")
    print(f"[PIPELINE] seed_limit={args.seed_limit}")
    print(f"[PIPELINE] requeue_stale_hours={args.requeue_stale_hours}")
    print(f"[PIPELINE] requeue_limit={args.requeue_limit}")
    print(f"[PIPELINE] requeue_target_queue={args.requeue_target_queue}")
    print(f"[PIPELINE] detail_limit={args.detail_limit}")
    print(f"[PIPELINE] stage_limit={args.stage_limit}")
    print(f"[PIPELINE] promote_limit={args.promote_limit}")
    print(f"[PIPELINE] quarantine_limit={args.quarantine_limit}")
    print(f"[PIPELINE] write={args.write}")

    if not args.skip_seed:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.seed_imusic_ean_links",
            "--limit",
            str(args.seed_limit),
        ]
        run_step("seed_imusic_ean_links", add_write_flag(command, args.write))

    if not args.skip_requeue:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.requeue_stale_links",
            "--shop-id",
            "imusic",
            "--stale-hours",
            str(args.requeue_stale_hours),
            "--limit",
            str(args.requeue_limit),
            "--target-queue",
            str(args.requeue_target_queue),
        ]
        run_step("requeue_stale_links", add_write_flag(command, args.write))

    if not args.skip_detail:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.detail_imusic",
            "--limit",
            str(args.detail_limit),
            "--timeout",
            str(args.detail_timeout),
            "--sleep",
            str(args.detail_sleep),
        ]
        run_step("detail_imusic", add_write_flag(command, args.write))

    if not args.skip_stage:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.stage_imusic",
            "--limit",
            str(args.stage_limit),
        ]
        run_step("stage_imusic", add_write_flag(command, args.write))

    if not args.skip_promote:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.promote_imusic",
            "--limit",
            str(args.promote_limit),
        ]
        run_step("promote_imusic", add_write_flag(command, args.write))

    if not args.skip_quarantine:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.quarantine_imusic",
            "--limit",
            str(args.quarantine_limit),
        ]
        run_step("quarantine_imusic", add_write_flag(command, args.write))

    print("\n[PIPELINE] COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
