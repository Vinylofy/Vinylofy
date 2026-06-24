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


def build_listing_command(args: argparse.Namespace, *, fast_price_sync: bool) -> list[str]:
    command = [
        sys.executable,
        "-m",
        "scripts.scrapers.usf.jobs.refresh_soundshaarlem_listing_prices",
        "--seed-limit",
        str(args.seed_limit),
        "--max-pages-per-seed",
        str(args.max_pages_per_seed),
        "--delay-seconds",
        str(args.delay_seconds),
    ]

    if fast_price_sync:
        command.append("--fast-price-sync")
    if args.write:
        command.append("--write")

    return command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Sounds Haarlem Shopify USF pipeline.")

    parser.add_argument("--seed-limit", type=int, default=0)
    parser.add_argument("--max-pages-per-seed", type=int, default=1)
    parser.add_argument("--delay-seconds", type=float, default=0.35)

    parser.add_argument("--run-detail", action="store_true")
    parser.add_argument("--detail-limit", type=int, default=500)
    parser.add_argument("--detail-delay-seconds", type=float, default=0.5)
    parser.add_argument("--detail-rescrape-days", type=int, default=14)

    parser.add_argument("--fast-price-sync", action="store_true")
    parser.add_argument("--write", action="store_true")

    return parser


def main() -> int:
    args = build_parser().parse_args()
    load_env()

    if args.write and not os.environ.get("DATABASE_URL"):
        raise SystemExit("[ERROR] DATABASE_URL ontbreekt. Zet DATABASE_URL in .env, .env.local of environment.")

    if args.seed_limit < 0:
        raise SystemExit("[ERROR] --seed-limit mag niet negatief zijn.")
    if args.max_pages_per_seed < 0:
        raise SystemExit("[ERROR] --max-pages-per-seed mag niet negatief zijn.")
    if args.delay_seconds < 0:
        raise SystemExit("[ERROR] --delay-seconds mag niet negatief zijn.")
    if args.detail_limit < 1:
        raise SystemExit("[ERROR] --detail-limit moet minimaal 1 zijn.")
    if args.detail_rescrape_days < 1:
        raise SystemExit("[ERROR] --detail-rescrape-days moet minimaal 1 zijn.")

    print(
        "[PIPELINE] config",
        {
            "shop": "soundshaarlem",
            "seed_limit": args.seed_limit,
            "max_pages_per_seed": args.max_pages_per_seed,
            "fast_price_sync": args.fast_price_sync,
            "run_detail": args.run_detail,
            "detail_limit": args.detail_limit,
            "detail_rescrape_days": args.detail_rescrape_days,
            "write": args.write,
        },
        flush=True,
    )

    run_step("listing_refresh_initial", build_listing_command(args, fast_price_sync=args.fast_price_sync))

    if args.run_detail:
        detail_command = [
            sys.executable,
            "-m",
            "scripts.scrapers.usf.jobs.detail_soundshaarlem",
            "--limit",
            str(args.detail_limit),
            "--rescrape-days",
            str(args.detail_rescrape_days),
            "--delay-seconds",
            str(args.detail_delay_seconds),
        ]

        if args.write:
            detail_command.append("--write")

        run_step("detail_soundshaarlem", detail_command)

        # After detail enrichment, do one normal listing sync so EAN-enriched links can create/update offers safely.
        run_step("listing_refresh_after_detail", build_listing_command(args, fast_price_sync=False))

    print("\n[PIPELINE] COMPLETE", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
