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


def run_step(name: str, command: list[str]) -> None:
    print("[NORTHEND-PIPELINE] step_start", {"step": name, "command": " ".join(command)}, flush=True)
    completed = subprocess.run(command, check=False)
    if completed.returncode != 0:
        raise SystemExit(f"[NORTHEND-PIPELINE][ERROR] step={name} exit_code={completed.returncode}")
    print("[NORTHEND-PIPELINE] step_done", {"step": name}, flush=True)


def build_listing_command(args: argparse.Namespace) -> list[str]:
    command = [
        sys.executable,
        "-u",
        "-m",
        "scripts.scrapers.usf.jobs.refresh_northendhaarlem_listing_prices",
        "--category-limit",
        str(args.category_limit),
        "--max-pages-per-category",
        str(args.max_pages_per_category),
        "--delay-seconds",
        str(args.delay_seconds),
    ]
    if args.fast_price_sync:
        command.append("--fast-price-sync")
    if args.write:
        command.append("--write")
    return command


def main() -> int:
    parser = argparse.ArgumentParser(description="Run North End Haarlem USF listing/detail/stage/promote pipeline.")
    parser.add_argument("--category-limit", type=int, default=0, help="Aantal categorie-seeds; 0 = alle seeds.")
    parser.add_argument("--max-pages-per-category", type=int, default=1, help="Aantal pagina's per categorie; 0 = tot stopconditie.")
    parser.add_argument("--delay-seconds", type=float, default=0.5)
    parser.add_argument("--run-detail", action="store_true")
    parser.add_argument("--detail-limit", type=int, default=100)
    parser.add_argument("--detail-delay-seconds", type=float, default=0.75)
    parser.add_argument("--detail-rescrape-days", type=int, default=14)
    parser.add_argument("--stage-limit", type=int, default=500)
    parser.add_argument("--promote-limit", type=int, default=500)
    parser.add_argument("--fast-price-sync", action="store_true")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    load_env()
    print(
        "[NORTHEND-PIPELINE] config",
        {
            "shop": "northendhaarlem",
            "category_limit": args.category_limit,
            "max_pages_per_category": args.max_pages_per_category,
            "run_detail": args.run_detail,
            "detail_limit": args.detail_limit,
            "detail_rescrape_days": args.detail_rescrape_days,
            "stage_limit": args.stage_limit,
            "promote_limit": args.promote_limit,
            "fast_price_sync": args.fast_price_sync,
            "write": args.write,
        },
        flush=True,
    )

    run_step("listing_refresh", build_listing_command(args))

    if args.run_detail:
        detail_command = [
            sys.executable,
            "-u",
            "-m",
            "scripts.scrapers.usf.jobs.detail_northendhaarlem",
            "--limit",
            str(args.detail_limit),
            "--delay-seconds",
            str(args.detail_delay_seconds),
            "--rescrape-days",
            str(args.detail_rescrape_days),
        ]
        if args.write:
            detail_command.append("--write")
        run_step("detail_enrichment", detail_command)

        stage_command = [
            sys.executable,
            "-u",
            "-m",
            "scripts.scrapers.usf.jobs.stage_northendhaarlem",
            "--limit",
            str(args.stage_limit),
        ]
        if args.write:
            stage_command.append("--write")
        run_step("stage", stage_command)

        promote_command = [
            sys.executable,
            "-u",
            "-m",
            "scripts.scrapers.usf.jobs.promote_northendhaarlem",
            "--limit",
            str(args.promote_limit),
        ]
        if args.write:
            promote_command.append("--write")
        run_step("promote", promote_command)
    else:
        print("[NORTHEND-PIPELINE] detail_stage_promote_skipped", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
