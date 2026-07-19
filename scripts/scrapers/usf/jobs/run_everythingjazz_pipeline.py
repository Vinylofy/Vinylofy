#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
MODULE_PREFIX = "scripts.scrapers.usf.jobs"


def run_step(label: str, command: list[str]) -> None:
    print("[EVERYTHINGJAZZ-PIPELINE]", {"step": label, "command": command}, flush=True)
    subprocess.run(command, cwd=ROOT, check=True)


def with_write(command: list[str], write: bool) -> list[str]:
    return [*command, "--write"] if write else command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Everything Jazz USF orchestration.")
    parser.add_argument(
        "--mode",
        choices=("listing", "detail", "full"),
        default="full",
    )
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("--listing-start-page", type=int, default=1)
    parser.add_argument("--listing-max-pages", type=int, default=0)
    parser.add_argument("--listing-page-size", type=int, default=50)
    parser.add_argument("--listing-sleep", type=float, default=0.25)
    parser.add_argument("--listing-max-products", type=int, default=2500)
    parser.add_argument(
        "--listing-max-runtime-seconds", type=int, default=900
    )
    parser.add_argument("--detail-limit", type=int, default=25)
    parser.add_argument("--detail-retry-days", type=int, default=30)
    parser.add_argument("--detail-refresh-days", type=int, default=180)
    parser.add_argument("--detail-sleep", type=float, default=0.35)
    parser.add_argument("--stage-limit", type=int, default=25)
    parser.add_argument("--promote-limit", type=int, default=25)
    parser.add_argument("--quarantine-limit", type=int, default=100)
    parser.add_argument("--skip-stage", action="store_true")
    parser.add_argument("--skip-promote", action="store_true")
    parser.add_argument("--skip-quarantine", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    py = sys.executable

    if args.mode in {"listing", "full"}:
        command = [
            py,
            "-m",
            f"{MODULE_PREFIX}.refresh_everythingjazz_listing_prices",
            "--start-page",
            str(args.listing_start_page),
            "--max-pages",
            str(args.listing_max_pages),
            "--page-size",
            str(args.listing_page_size),
            "--sleep",
            str(args.listing_sleep),
            "--max-products",
            str(args.listing_max_products),
            "--max-runtime-seconds",
            str(args.listing_max_runtime_seconds),
        ]
        if args.debug:
            command.append("--debug")
        run_step("listing", with_write(command, args.write))

    if args.mode in {"detail", "full"}:
        command = [
            py,
            "-m",
            f"{MODULE_PREFIX}.detail_everythingjazz",
            "--limit",
            str(args.detail_limit),
            "--retry-days",
            str(args.detail_retry_days),
            "--refresh-days",
            str(args.detail_refresh_days),
            "--sleep",
            str(args.detail_sleep),
        ]
        if args.debug:
            command.append("--debug")
        run_step("detail", with_write(command, args.write))

        if not args.skip_stage:
            run_step(
                "stage",
                with_write(
                    [
                        py,
                        "-m",
                        f"{MODULE_PREFIX}.stage_everythingjazz",
                        "--limit",
                        str(args.stage_limit),
                    ],
                    args.write,
                ),
            )
        if not args.skip_promote:
            run_step(
                "promote",
                with_write(
                    [
                        py,
                        "-m",
                        f"{MODULE_PREFIX}.promote_everythingjazz",
                        "--limit",
                        str(args.promote_limit),
                    ],
                    args.write,
                ),
            )
        if not args.skip_quarantine:
            run_step(
                "quarantine",
                with_write(
                    [
                        py,
                        "-m",
                        f"{MODULE_PREFIX}.quarantine_everythingjazz",
                        "--limit",
                        str(args.quarantine_limit),
                    ],
                    args.write,
                ),
            )

    print(
        "[EVERYTHINGJAZZ-PIPELINE]",
        {"mode": args.mode, "write": args.write, "status": "complete"},
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
