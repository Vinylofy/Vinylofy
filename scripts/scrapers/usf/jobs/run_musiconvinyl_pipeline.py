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


def add_write(command: list[str], write: bool) -> list[str]:
    if write:
        command.append("--write")
    return command


def run_step(label: str, command: list[str]) -> None:
    print(f"\n[PIPELINE] START {label}", flush=True)
    print("[PIPELINE] CMD", " ".join(command), flush=True)
    subprocess.run(command, check=True)
    print(f"[PIPELINE] DONE {label}", flush=True)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Music On Vinyl USF pipeline.")
    parser.add_argument("--collections", default="all-products,new-releases,back-in-stock")
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--listing-limit", type=int, default=250)
    parser.add_argument("--materialize-limit", type=int, default=500)
    parser.add_argument("--stage-limit", type=int, default=500)
    parser.add_argument("--promote-limit", type=int, default=500)
    parser.add_argument("--skip-discovery", action="store_true")
    parser.add_argument("--skip-materialize", action="store_true")
    parser.add_argument("--skip-stage", action="store_true")
    parser.add_argument("--skip-promote", action="store_true")
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_env()
    if args.write and not os.environ.get("DATABASE_URL"):
        raise SystemExit("[ERROR] DATABASE_URL ontbreekt voor --write.")

    if not args.skip_discovery:
        run_step(
            "discover_musiconvinyl",
            add_write(
                [
                    sys.executable,
                    "-m",
                    "scripts.scrapers.usf.jobs.discover_musiconvinyl",
                    "--collections",
                    args.collections,
                    "--max-pages",
                    str(args.max_pages),
                    "--limit",
                    str(args.listing_limit),
                ],
                args.write,
            ),
        )

    if not args.skip_materialize:
        run_step(
            "materialize_musiconvinyl_listing",
            add_write(
                [
                    sys.executable,
                    "-m",
                    "scripts.scrapers.usf.jobs.materialize_musiconvinyl_listing",
                    "--limit",
                    str(args.materialize_limit),
                ],
                args.write,
            ),
        )

    if not args.skip_stage:
        run_step(
            "stage_musiconvinyl",
            add_write(
                [
                    sys.executable,
                    "-m",
                    "scripts.scrapers.usf.jobs.stage_musiconvinyl",
                    "--limit",
                    str(args.stage_limit),
                ],
                args.write,
            ),
        )

    if not args.skip_promote:
        run_step(
            "promote_musiconvinyl",
            add_write(
                [
                    sys.executable,
                    "-m",
                    "scripts.scrapers.usf.jobs.promote_musiconvinyl",
                    "--limit",
                    str(args.promote_limit),
                ],
                args.write,
            ),
        )

    print("\n[PIPELINE] COMPLETE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
