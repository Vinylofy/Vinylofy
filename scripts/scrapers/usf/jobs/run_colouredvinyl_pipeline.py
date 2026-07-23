#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


MODULES = {
    "listing": (
        "scripts.scrapers.usf.jobs."
        "refresh_colouredvinyl_listing_prices"
    ),
    "detail": (
        "scripts.scrapers.usf.jobs."
        "detail_colouredvinyl"
    ),
    "stage": (
        "scripts.scrapers.usf.jobs."
        "stage_colouredvinyl"
    ),
    "promote": (
        "scripts.scrapers.usf.jobs."
        "promote_colouredvinyl"
    ),
}

VALID_STEPS = tuple(MODULES)
OUTPUT_DIR = Path("output/usf-colouredvinyl")


def timestamp() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def module_help(module: str) -> str:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            module,
            "--help",
        ],
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env={
            **os.environ,
            "PYTHONUNBUFFERED": "1",
        },
    )

    if result.returncode != 0:
        raise RuntimeError(
            f"Help voor {module} faalde:\n"
            f"{result.stdout}"
        )

    return result.stdout


def supports_flag(
    help_text: str,
    flag: str,
) -> bool:
    pattern = (
        rf"(?<![\w-])"
        rf"{re.escape(flag)}"
        rf"(?=[\s,=])"
    )

    return bool(
        re.search(pattern, help_text)
    )


def require_flag(
    help_text: str,
    *candidates: str,
) -> str:
    for flag in candidates:
        if supports_flag(
            help_text,
            flag,
        ):
            return flag

    raise RuntimeError(
        "Geen ondersteunde CLI-optie gevonden: "
        + ", ".join(candidates)
    )


def execute(
    *,
    step: str,
    args: Sequence[str],
) -> dict[str, object]:
    module = MODULES[step]

    command = [
        sys.executable,
        "-u",
        "-m",
        module,
        *args,
    ]

    print(
        "[COLOUREDVINYL-PIPELINE-STEP]",
        {
            "step": step,
            "command": command,
        },
        flush=True,
    )

    started_at = timestamp()

    result = subprocess.run(
        command,
        check=False,
        env={
            **os.environ,
            "PYTHONUNBUFFERED": "1",
        },
    )

    finished_at = timestamp()

    return {
        "step": step,
        "module": module,
        "command": command,
        "returncode": result.returncode,
        "started_at": started_at,
        "finished_at": finished_at,
    }


def parse_steps(raw: str) -> list[str]:
    values = [
        value.strip()
        for value in raw.split(",")
        if value.strip()
    ]

    if not values:
        raise ValueError(
            "Er is geen pipelinestap geselecteerd."
        )

    unknown = [
        value
        for value in values
        if value not in VALID_STEPS
    ]

    if unknown:
        raise ValueError(
            "Onbekende pipelinestappen: "
            + ", ".join(unknown)
        )

    result: list[str] = []

    for value in values:
        if value not in result:
            result.append(value)

    return result


def build_step_arguments(
    *,
    step: str,
    mode: str,
    detail_limit: int,
    detail_retry_days: int,
    detail_sleep_seconds: float,
    stage_limit: int,
    promote_limit: int,
) -> list[str]:
    module = MODULES[step]
    help_text = module_help(module)

    args: list[str] = []

    if step == "detail":
        limit_flag = require_flag(
            help_text,
            "--limit",
        )

        retry_flag = require_flag(
            help_text,
            "--retry-days",
            "--retry_days",
        )

        sleep_flag = require_flag(
            help_text,
            "--sleep",
            "--sleep-seconds",
            "--sleep_seconds",
        )

        args.extend([
            limit_flag,
            str(detail_limit),
            retry_flag,
            str(detail_retry_days),
            sleep_flag,
            str(detail_sleep_seconds),
        ])

    elif step == "stage":
        limit_flag = require_flag(
            help_text,
            "--limit",
        )

        args.extend([
            limit_flag,
            str(stage_limit),
        ])

    elif step == "promote":
        limit_flag = require_flag(
            help_text,
            "--limit",
        )

        args.extend([
            limit_flag,
            str(promote_limit),
        ])

    if mode == "write":
        write_flag = require_flag(
            help_text,
            "--write",
        )

        args.append(write_flag)

    return args


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Voer de Coloured Vinyl USF-keten "
            "uit vanuit één Actions-workflow."
        )
    )

    parser.add_argument(
        "--mode",
        choices=(
            "dry-run",
            "write",
        ),
        default="dry-run",
    )

    parser.add_argument(
        "--steps",
        required=True,
        help=(
            "Kommalijst met listing,detail,"
            "stage,promote."
        ),
    )

    parser.add_argument(
        "--detail-limit",
        type=int,
        default=25,
    )

    parser.add_argument(
        "--detail-retry-days",
        type=int,
        default=30,
    )

    parser.add_argument(
        "--detail-sleep-seconds",
        type=float,
        default=3.0,
    )

    parser.add_argument(
        "--stage-limit",
        type=int,
        default=100,
    )

    parser.add_argument(
        "--promote-limit",
        type=int,
        default=25,
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()

    steps = parse_steps(args.steps)

    if args.detail_limit < 1:
        raise SystemExit(
            "--detail-limit moet minimaal 1 zijn."
        )

    if args.detail_retry_days < 0:
        raise SystemExit(
            "--detail-retry-days mag niet negatief zijn."
        )

    if args.detail_sleep_seconds < 0:
        raise SystemExit(
            "--detail-sleep-seconds mag niet negatief zijn."
        )

    if args.stage_limit < 1:
        raise SystemExit(
            "--stage-limit moet minimaal 1 zijn."
        )

    if args.promote_limit < 1:
        raise SystemExit(
            "--promote-limit moet minimaal 1 zijn."
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    summary: dict[str, object] = {
        "shop": "colouredvinyl",
        "mode": args.mode,
        "steps_requested": steps,
        "started_at": timestamp(),
        "finished_at": None,
        "success": False,
        "steps": [],
    }

    exit_code = 0

    try:
        for step in steps:
            step_args = build_step_arguments(
                step=step,
                mode=args.mode,
                detail_limit=args.detail_limit,
                detail_retry_days=(
                    args.detail_retry_days
                ),
                detail_sleep_seconds=(
                    args.detail_sleep_seconds
                ),
                stage_limit=args.stage_limit,
                promote_limit=(
                    args.promote_limit
                ),
            )

            result = execute(
                step=step,
                args=step_args,
            )

            cast_steps = summary["steps"]

            if not isinstance(
                cast_steps,
                list,
            ):
                raise RuntimeError(
                    "Interne samenvattingsfout."
                )

            cast_steps.append(result)

            if int(result["returncode"]) != 0:
                exit_code = int(
                    result["returncode"]
                )

                break

        summary["success"] = (
            exit_code == 0
        )

    except Exception as exc:
        exit_code = 1

        summary["error"] = (
            f"{type(exc).__name__}: {exc}"
        )

        print(
            "[COLOUREDVINYL-PIPELINE-ERROR]",
            summary["error"],
            flush=True,
        )

    finally:
        summary["finished_at"] = (
            timestamp()
        )

        summary_path = (
            OUTPUT_DIR
            / "pipeline-summary.json"
        )

        summary_path.write_text(
            json.dumps(
                summary,
                indent=2,
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        print(
            "[COLOUREDVINYL-PIPELINE-SUMMARY]",
            json.dumps(
                summary,
                ensure_ascii=False,
            ),
            flush=True,
        )

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
