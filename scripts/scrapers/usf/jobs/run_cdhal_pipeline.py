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

        for line in path.read_text(
            encoding="utf-8"
        ).splitlines():
            line = line.strip()

            if (
                not line
                or line.startswith("#")
                or "=" not in line
            ):
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            if key and key not in os.environ:
                os.environ[key] = value


def run_step(
    label: str,
    command: list[str],
) -> None:
    print(
        f"\n[CDHAL-PIPELINE] START {label}",
        flush=True,
    )
    print(
        "[CDHAL-PIPELINE] CMD",
        " ".join(command),
        flush=True,
    )

    subprocess.run(
        command,
        check=True,
    )

    print(
        f"[CDHAL-PIPELINE] DONE {label}",
        flush=True,
    )


def add_write(
    command: list[str],
    *,
    write: bool,
) -> list[str]:
    if write:
        command.append("--write")

    return command


def positive_int(
    parser: argparse.ArgumentParser,
    option: str,
    value: int,
) -> None:
    if value < 1:
        parser.error(
            f"{option} moet minimaal 1 zijn."
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run de zelfstandige CD Hal Ruinen USF-pipeline."
    )

    parser.add_argument(
        "--listing-start-page",
        type=int,
        default=1,
    )
    parser.add_argument(
        "--listing-max-pages",
        type=int,
        default=1,
        help="0 = doorlopen tot lege of dubbele pagina.",
    )
    parser.add_argument(
        "--listing-sleep",
        type=float,
        default=0.35,
    )
    parser.add_argument(
        "--max-page-failures",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--debug-listing",
        action="store_true",
    )
    parser.add_argument(
        "--skip-listing",
        action="store_true",
    )
    parser.add_argument(
        "--run-detail",
        action="store_true",
    )
    parser.add_argument(
        "--run-stage-promote",
        action="store_true",
    )
    parser.add_argument(
        "--detail-limit",
        type=int,
        default=500,
    )
    parser.add_argument(
        "--detail-sleep",
        type=float,
        default=0.50,
    )
    parser.add_argument(
        "--detail-retry-days",
        type=int,
        default=30,
    )
    parser.add_argument(
        "--stage-limit",
        type=int,
        default=500,
    )
    parser.add_argument(
        "--promote-limit",
        type=int,
        default=500,
    )
    parser.add_argument(
        "--quarantine-limit",
        type=int,
        default=100,
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
        "--skip-quarantine",
        action="store_true",
    )
    parser.add_argument(
        "--write",
        action="store_true",
    )

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    load_env()

    if args.write and not os.environ.get("DATABASE_URL"):
        raise SystemExit(
            "[ERROR] DATABASE_URL ontbreekt."
        )

    if args.listing_start_page < 1:
        parser.error(
            "--listing-start-page moet minimaal 1 zijn."
        )

    if args.listing_max_pages < 0:
        parser.error(
            "--listing-max-pages mag niet negatief zijn."
        )

    if args.listing_sleep < 0:
        parser.error(
            "--listing-sleep mag niet negatief zijn."
        )

    if args.max_page_failures < 1:
        parser.error(
            "--max-page-failures moet minimaal 1 zijn."
        )

    if args.detail_sleep < 0:
        parser.error(
            "--detail-sleep mag niet negatief zijn."
        )

    positive_int(
        parser,
        "--detail-retry-days",
        args.detail_retry_days,
    )
    positive_int(
        parser,
        "--detail-limit",
        args.detail_limit,
    )
    positive_int(
        parser,
        "--stage-limit",
        args.stage_limit,
    )
    positive_int(
        parser,
        "--promote-limit",
        args.promote_limit,
    )
    positive_int(
        parser,
        "--quarantine-limit",
        args.quarantine_limit,
    )

    print(
        "[CDHAL-PIPELINE] config",
        {
            "shop": "cdhal",
            "listing_start_page": args.listing_start_page,
            "listing_max_pages": args.listing_max_pages,
            "skip_listing": args.skip_listing,
            "run_detail": args.run_detail,
            "run_stage_promote": args.run_stage_promote,
            "write": args.write,
        },
        flush=True,
    )

    if not args.skip_listing:
        listing_command = [
            sys.executable,
            "-m",
            (
                "scripts.scrapers.usf.jobs."
                "refresh_cdhal_listing_prices"
            ),
            "--start-page",
            str(args.listing_start_page),
            "--max-pages",
            str(args.listing_max_pages),
            "--sleep",
            str(args.listing_sleep),
            "--max-page-failures",
            str(args.max_page_failures),
        ]

        if args.debug_listing:
            listing_command.append("--debug")

        run_step(
            "refresh_cdhal_listing_prices",
            add_write(
                listing_command,
                write=args.write,
            ),
        )
    else:
        print(
            "[CDHAL-PIPELINE] SKIP listing",
            flush=True,
        )

    if args.run_detail and not args.write:
        print(
            "[CDHAL-PIPELINE] SKIP detail/stage/promote/quarantine: "
            "listing-dry-run schrijft geen registryrecords; downstream zou "
            "daarom altijd een lege queue zien.",
            flush=True,
        )

    if args.run_detail and args.write:
        run_step(
            "detail_cdhal",
            add_write(
                [
                    sys.executable,
                    "-m",
                    (
                        "scripts.scrapers.usf.jobs."
                        "detail_cdhal"
                    ),
                    "--limit",
                    str(args.detail_limit),
                    "--sleep",
                    str(args.detail_sleep),
                    "--retry-days",
                    str(args.detail_retry_days),
                ],
                write=args.write,
            ),
        )

    if (
        (args.run_detail and args.write)
        or args.run_stage_promote
    ):
        if not args.skip_stage:
            run_step(
                "stage_cdhal",
                add_write(
                    [
                        sys.executable,
                        "-m",
                        (
                            "scripts.scrapers.usf.jobs."
                            "stage_cdhal"
                        ),
                        "--limit",
                        str(args.stage_limit),
                    ],
                    write=args.write,
                ),
            )

        if not args.skip_promote:
            run_step(
                "promote_cdhal",
                add_write(
                    [
                        sys.executable,
                        "-m",
                        (
                            "scripts.scrapers.usf.jobs."
                            "promote_cdhal"
                        ),
                        "--limit",
                        str(args.promote_limit),
                    ],
                    write=args.write,
                ),
            )

        if (
            args.run_detail
            and not args.skip_quarantine
        ):
            run_step(
                "quarantine_cdhal",
                add_write(
                    [
                        sys.executable,
                        "-m",
                        (
                            "scripts.scrapers.usf.jobs."
                            "quarantine_cdhal"
                        ),
                        "--limit",
                        str(args.quarantine_limit),
                    ],
                    write=args.write,
                ),
            )

    print(
        "\n[CDHAL-PIPELINE] COMPLETE",
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
