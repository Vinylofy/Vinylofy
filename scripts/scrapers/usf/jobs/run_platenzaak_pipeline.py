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


def add_write_flag(
    command: list[str],
    write: bool,
) -> list[str]:
    if write:
        command.append("--write")

    return command


def listing_refresh_command(
    args: argparse.Namespace,
) -> list[str]:
    return [
        PYTHON,
        "-m",
        "scripts.scrapers.usf.jobs."
        "refresh_platenzaak_listing_prices",
        "--max-pages",
        str(args.discovery_max_pages),
        "--delay-seconds",
        str(args.discovery_delay_seconds),
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run Platenzaak USF pipeline listing-first: "
            "listing price refresh, newest-title priority, "
            "detail EAN enrichment, listing price refresh."
        )
    )

    parser.add_argument(
        "--discovery-max-pages",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--discovery-delay-seconds",
        type=float,
        default=0.25,
    )
    parser.add_argument(
        "--detail-limit",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--latest-priority-pages",
        type=int,
        default=5,
    )

    # Legacy args blijven bestaan zodat bestaande workflow-inputs
    # niet breken.
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

    # Legacy flags blijven beschikbaar voor een bewuste recovery.
    parser.add_argument(
        "--skip-stage",
        action="store_true",
    )
    parser.add_argument(
        "--skip-promote",
        action="store_true",
    )
    parser.add_argument(
        "--allow-legacy-stage-promote",
        action="store_true",
        help=(
            "Legacy/noodoptie. Normaal niet gebruiken: "
            "Platenzaak current pricing moet via listing refresh lopen."
        ),
    )

    parser.add_argument(
        "--write",
        action="store_true",
        help=(
            "Voer echte registry-, raw- en publieke "
            "listing-price writes uit."
        ),
    )

    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.discovery_max_pages < 0:
        raise SystemExit(
            "[ERROR] --discovery-max-pages mag niet negatief zijn."
        )

    if args.discovery_delay_seconds < 0:
        raise SystemExit(
            "[ERROR] --discovery-delay-seconds mag niet negatief zijn."
        )

    if args.detail_limit < 1:
        raise SystemExit(
            "[ERROR] --detail-limit moet minimaal 1 zijn."
        )

    if args.latest_priority_pages < 1:
        raise SystemExit(
            "[ERROR] --latest-priority-pages moet minimaal 1 zijn."
        )

    if args.stage_limit < 1:
        raise SystemExit(
            "[ERROR] --stage-limit moet minimaal 1 zijn."
        )

    if args.promote_limit < 1:
        raise SystemExit(
            "[ERROR] --promote-limit moet minimaal 1 zijn."
        )


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)

    print(
        "[PIPELINE] config",
        {
            "shop": "platenzaak",
            "policy": "listing_first_current_pricing",
            "discovery_max_pages": args.discovery_max_pages,
            "discovery_delay_seconds": (
                args.discovery_delay_seconds
            ),
            "detail_limit": args.detail_limit,
            "latest_priority_pages": (
                args.latest_priority_pages
            ),
            "stage_limit": args.stage_limit,
            "promote_limit": args.promote_limit,
            "write": args.write,
            "skip_discovery": args.skip_discovery,
            "skip_detail": args.skip_detail,
            "allow_legacy_stage_promote": (
                args.allow_legacy_stage_promote
            ),
        },
        flush=True,
    )

    if not args.skip_discovery:
        run_step(
            "refresh_platenzaak_listing_prices",
            add_write_flag(
                listing_refresh_command(args),
                args.write,
            ),
        )

    if not args.skip_detail:
        run_step(
            "prioritize_latest_platenzaak",
            add_write_flag(
                [
                    PYTHON,
                    "-m",
                    "scripts.scrapers.usf.jobs."
                    "prioritize_latest_platenzaak",
                    "--max-pages",
                    str(args.latest_priority_pages),
                    "--delay-seconds",
                    str(args.discovery_delay_seconds),
                ],
                args.write,
            ),
        )

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

        if not args.skip_discovery:
            run_step(
                "refresh_platenzaak_listing_prices_after_detail",
                add_write_flag(
                    listing_refresh_command(args),
                    args.write,
                ),
            )

    if args.allow_legacy_stage_promote:
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
                        "scripts.scrapers.usf.jobs."
                        "promote_platenzaak",
                        "--limit",
                        str(args.promote_limit),
                    ],
                    args.write,
                ),
            )
    else:
        print()
        print(
            "[PIPELINE] SKIP legacy stage/promote: "
            "Platenzaak current pricing is listing-first. "
            "Use --allow-legacy-stage-promote only for "
            "a deliberate recovery run.",
            flush=True,
        )

    print()
    print("[PIPELINE] complete", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
