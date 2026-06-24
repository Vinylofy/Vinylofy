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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Platomania USF pipeline.")

    parser.add_argument("--seed-limit", type=int, default=0)
    parser.add_argument("--max-pages-per-seed", type=int, default=0)
    parser.add_argument("--delay-seconds", type=float, default=0.35)
    parser.add_argument("--fast-price-sync", action="store_true")
    parser.add_argument("--write", action="store_true")

    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.seed_limit < 0:
        raise SystemExit("[ERROR] --seed-limit mag niet negatief zijn.")
    if args.max_pages_per_seed < 0:
        raise SystemExit("[ERROR] --max-pages-per-seed mag niet negatief zijn.")
    if args.delay_seconds < 0:
        raise SystemExit("[ERROR] --delay-seconds mag niet negatief zijn.")


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)
    load_env()

    if args.write and not os.environ.get("DATABASE_URL"):
        raise SystemExit("[ERROR] DATABASE_URL ontbreekt. Zet DATABASE_URL in .env, .env.local of environment.")

    print("[PIPELINE] Platomania USF listing-price pipeline", flush=True)
    print(
        "[PIPELINE] config",
        {
            "seed_limit": args.seed_limit,
            "max_pages_per_seed": args.max_pages_per_seed,
            "fast_price_sync": args.fast_price_sync,
            "write": args.write,
        },
        flush=True,
    )

    command = [
        sys.executable,
        "-m",
        "scripts.scrapers.usf.jobs.refresh_platomania_listing_prices",
        "--seed-limit",
        str(args.seed_limit),
        "--max-pages-per-seed",
        str(args.max_pages_per_seed),
        "--delay-seconds",
        str(args.delay_seconds),
    ]

    if args.fast_price_sync:
        command.append("--fast-price-sync")
    if args.write:
        command.append("--write")

    run_step("refresh_platomania_listing_prices", command)

    print("\n[PIPELINE] COMPLETE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
