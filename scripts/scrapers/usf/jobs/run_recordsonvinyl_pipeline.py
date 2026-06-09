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

    print("[PIPELINE] RecordsonVinyl USF pipeline")
    print(f"[PIPELINE] detail_limit={args.detail_limit}")
    print(f"[PIPELINE] stage_limit={args.stage_limit}")
    print(f"[PIPELINE] promote_limit={args.promote_limit}")
    print(f"[PIPELINE] quarantine_limit={args.quarantine_limit}")
    print(f"[PIPELINE] write={args.write}")

    if not args.skip_detail:
        run_step(
            "detail_recordsonvinyl",
            [
                sys.executable,
                "-m",
                "scripts.scrapers.usf.jobs.detail_recordsonvinyl",
                "--limit",
                str(args.detail_limit),
            ],
        )

    if not args.skip_stage:
        run_step(
            "stage_recordsonvinyl",
            [
                sys.executable,
                "-m",
                "scripts.scrapers.usf.jobs.stage_recordsonvinyl",
                "--limit",
                str(args.stage_limit),
            ],
        )

    if not args.skip_promote:
        promote_command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.promote_staged_offers",
            "--limit",
            str(args.promote_limit),
        ]

        if args.write:
            promote_command.append("--write")

        run_step("promote_staged_offers", promote_command)

    if not args.skip_quarantine:
        quarantine_command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.quarantine_recordsonvinyl",
            "--limit",
            str(args.quarantine_limit),
        ]

        if args.write:
            quarantine_command.append("--write")

        run_step("quarantine_recordsonvinyl", quarantine_command)

    print("\n[PIPELINE] COMPLETE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
