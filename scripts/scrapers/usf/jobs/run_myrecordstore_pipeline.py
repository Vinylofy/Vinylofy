from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

def load_env() -> None:
    for env_file in (".env.local", ".env"):
        path = Path(env_file)
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

def run_step(label: str, command: list[str]) -> None:
    print(f"\n[MYRECORDSTORE-PIPELINE] START {label}", flush=True)
    print("[MYRECORDSTORE-PIPELINE] CMD", " ".join(command), flush=True)
    subprocess.run(command, check=True)
    print(f"[MYRECORDSTORE-PIPELINE] DONE {label}", flush=True)

def main() -> int:
    parser = argparse.ArgumentParser(description="Run My Record Store USF pipeline.")
    parser.add_argument("--mode", choices=("price-only", "detail", "full"), default="price-only")
    parser.add_argument("--listing-start-page", type=int, default=1)
    parser.add_argument("--listing-max-pages", type=int, default=0)
    parser.add_argument("--listing-sleep", type=float, default=1.0)
    parser.add_argument("--detail-limit", type=int, default=25)
    parser.add_argument("--stage-limit", type=int, default=25)
    parser.add_argument("--promote-limit", type=int, default=25)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    load_env()

    if not os.environ.get("DATABASE_URL"):
        raise SystemExit("[ERROR] DATABASE_URL ontbreekt.")

    if args.mode in {"price-only", "full"}:
        cmd = [
            sys.executable, "-m", "scripts.scrapers.usf.jobs.refresh_myrecordstore_listing_prices",
            "--start-page", str(args.listing_start_page),
            "--max-pages", str(args.listing_max_pages),
            "--sleep", str(args.listing_sleep),
            "--fast-price-sync",
            "--delist-missing",
        ]
        if args.write:
            cmd.append("--write")
        run_step("refresh_myrecordstore_listing_prices", cmd)

    if args.mode in {"detail", "full"}:
        detail_cmd = [sys.executable, "-m", "scripts.scrapers.usf.jobs.detail_myrecordstore", "--limit", str(args.detail_limit)]
        stage_cmd = [sys.executable, "-m", "scripts.scrapers.usf.jobs.stage_myrecordstore", "--limit", str(args.stage_limit)]
        promote_cmd = [sys.executable, "-m", "scripts.scrapers.usf.jobs.promote_myrecordstore", "--limit", str(args.promote_limit)]
        if args.write:
            detail_cmd.append("--write")
            stage_cmd.append("--write")
            promote_cmd.append("--write")
        run_step("detail_myrecordstore", detail_cmd)
        run_step("stage_myrecordstore", stage_cmd)
        run_step("promote_myrecordstore", promote_cmd)

    print("\n[MYRECORDSTORE-PIPELINE] COMPLETE", flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
