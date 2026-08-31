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
            "Run de JPC USF-pipeline: vinyl discovery, stale requeue, "
            "batchgewijze EAN-detailverrijking, staging, promotion en quarantine."
        )
    )

    parser.add_argument("--routes", default="default")
    parser.add_argument("--include-route-index", action="store_true")
    parser.add_argument("--max-pages-per-route", type=int, default=1)
    parser.add_argument(
        "--pagination-fallback",
        choices=("none", "ff", "page", "pn"),
        default="ff",
    )
    parser.add_argument("--discovery-timeout", type=float, default=25.0)
    parser.add_argument("--discovery-sleep", type=float, default=4.0)
    parser.add_argument("--requeue-stale-hours", type=float, default=72.0)
    parser.add_argument("--requeue-limit", type=int, default=250)
    parser.add_argument("--requeue-target-queue", type=int, default=500)
    parser.add_argument("--detail-limit", type=int, default=100)
    parser.add_argument("--detail-timeout", type=float, default=25.0)
    parser.add_argument("--detail-sleep", type=float, default=4.0)
    parser.add_argument("--stage-limit", type=int, default=100)
    parser.add_argument("--promote-limit", type=int, default=100)
    parser.add_argument("--quarantine-limit", type=int, default=100)

    parser.add_argument("--skip-discovery", action="store_true")
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
    parser.add_argument(
        "--discovery-write",
        action="store_true",
        help="Schrijf alleen JPC discovered links naar shop_product_links.",
    )
    parser.add_argument(
        "--requeue-write",
        action="store_true",
        help="Voer alleen JPC stale-link requeue writes uit.",
    )
    parser.add_argument(
        "--detail-write",
        action="store_true",
        help="Schrijf alleen JPC detailresultaten naar raw_shop_scrapes.",
    )
    parser.add_argument(
        "--stage-write",
        action="store_true",
        help="Schrijf alleen JPC staged_offers.",
    )
    parser.add_argument(
        "--promote-write",
        action="store_true",
        help="Schrijf alleen JPC public promotion writes.",
    )
    parser.add_argument(
        "--quarantine-write",
        action="store_true",
        help="Schrijf alleen JPC quarantine rows.",
    )

    return parser


def validate_args(args: argparse.Namespace) -> None:
    for name in (
        "max_pages_per_route",
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

    for name in ("discovery_timeout", "detail_timeout"):
        if getattr(args, name) <= 0:
            option = name.replace("_", "-")
            raise SystemExit(f"[ERROR] --{option} moet positief zijn.")

    for name in ("discovery_sleep", "detail_sleep", "requeue_stale_hours"):
        if getattr(args, name) < 0:
            option = name.replace("_", "-")
            raise SystemExit(f"[ERROR] --{option} mag niet negatief zijn.")


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)
    load_env()

    if not os.environ.get("DATABASE_URL"):
        raise SystemExit(
            "[ERROR] DATABASE_URL ontbreekt. "
            "Zet DATABASE_URL in .env, .env.local of environment."
        )

    print("[PIPELINE] JPC USF pipeline")
    print(f"[PIPELINE] routes={args.routes}")
    print(f"[PIPELINE] include_route_index={args.include_route_index}")
    print(f"[PIPELINE] max_pages_per_route={args.max_pages_per_route}")
    print(f"[PIPELINE] pagination_fallback={args.pagination_fallback}")
    print(f"[PIPELINE] detail_limit={args.detail_limit}")
    print(f"[PIPELINE] stage_limit={args.stage_limit}")
    print(f"[PIPELINE] promote_limit={args.promote_limit}")
    effective_writes = {
        "discovery": args.write or args.discovery_write,
        "requeue": args.write or args.requeue_write,
        "detail": args.write or args.detail_write,
        "stage": args.write or args.stage_write,
        "promote": args.write or args.promote_write,
        "quarantine": args.write or args.quarantine_write,
    }
    print(f"[PIPELINE] write_all={args.write}")
    print(f"[PIPELINE] effective_writes={effective_writes}")

    if not args.skip_discovery:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.discover_jpc_vinyl",
            "--routes",
            args.routes,
            "--max-pages-per-route",
            str(args.max_pages_per_route),
            "--pagination-fallback",
            args.pagination_fallback,
            "--timeout",
            str(args.discovery_timeout),
            "--delay",
            str(args.discovery_sleep),
        ]
        if args.include_route_index:
            command.append("--include-route-index")
        run_step(
            "discover_jpc_vinyl",
            add_write_flag(command, effective_writes["discovery"]),
        )

    if not args.skip_requeue:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.requeue_stale_links",
            "--shop-id",
            "jpc",
            "--stale-hours",
            str(args.requeue_stale_hours),
            "--limit",
            str(args.requeue_limit),
            "--target-queue",
            str(args.requeue_target_queue),
        ]
        run_step(
            "requeue_stale_links",
            add_write_flag(command, effective_writes["requeue"]),
        )

    if not args.skip_detail:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.detail_jpc",
            "--limit",
            str(args.detail_limit),
            "--timeout",
            str(args.detail_timeout),
            "--sleep",
            str(args.detail_sleep),
        ]
        run_step(
            "detail_jpc",
            add_write_flag(command, effective_writes["detail"]),
        )

    if not args.skip_stage:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.stage_jpc",
            "--limit",
            str(args.stage_limit),
        ]
        run_step(
            "stage_jpc",
            add_write_flag(command, effective_writes["stage"]),
        )

    if not args.skip_promote:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.promote_jpc",
            "--limit",
            str(args.promote_limit),
        ]
        run_step(
            "promote_jpc",
            add_write_flag(command, effective_writes["promote"]),
        )

    if not args.skip_quarantine:
        command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.quarantine_jpc",
            "--limit",
            str(args.quarantine_limit),
        ]
        run_step(
            "quarantine_jpc",
            add_write_flag(command, effective_writes["quarantine"]),
        )

    print("\n[PIPELINE] COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
