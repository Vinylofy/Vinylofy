#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

CURRENT = Path(__file__).resolve().parent
TARGET = CURRENT / "groovespin.py"


def ensure_dir(path: str | Path) -> Path:
    out = Path(path)
    out.mkdir(parents=True, exist_ok=True)
    return out


def find_latest_stage1(out_dir: Path) -> Path:
    candidates = sorted(out_dir.glob("groovespin_albums_*.stage1.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"Geen stage1 CSV gevonden in {out_dir}")
    return candidates[0]


def find_latest_final(out_dir: Path) -> Path:
    candidates = [
        p for p in out_dir.glob("groovespin_albums_*.csv")
        if not p.name.endswith(".stage1.csv")
    ]
    candidates = sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"Geen final CSV gevonden in {out_dir}")
    return candidates[0]


def run_command(args: list[str]) -> None:
    proc = subprocess.run(args, text=True)
    if proc.returncode != 0:
        raise SystemExit(proc.returncode)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Automation wrapper voor Groovespin scraper")
    p.add_argument("--mode", choices=["listing", "full", "ean"], default="full")
    p.add_argument("--output-dir", default="data/raw/groovespin")
    p.add_argument("--max-pages", type=int, default=30)
    p.add_argument("--limit-ean", type=int, default=250)
    p.add_argument("--stage1-file", default="")
    p.add_argument("--headless", default="true")
    return p


def main() -> int:
    args = build_parser().parse_args()
    out_dir = ensure_dir(args.output_dir)

    canonical_stage1 = out_dir / "groovespin_stage1.csv"
    canonical_final = out_dir / "groovespin_products.csv"

    if args.mode == "listing":
        cmd = [
            sys.executable, "-u", str(TARGET),
            "--mode", "listing",
            "--headless", str(args.headless),
            "--out-dir", str(out_dir),
            "--max-pages", str(max(1, args.max_pages)),
        ]
        run_command(cmd)
        shutil.copy2(find_latest_stage1(out_dir), canonical_stage1)
        latest_final = find_latest_final(out_dir)
        shutil.copy2(latest_final, canonical_final)
        print(f"[CANONICAL] stage1 -> {canonical_stage1}")
        print(f"[CANONICAL] final -> {canonical_final}")
        return 0

    if args.mode == "ean":
        stage1_source = Path(args.stage1_file) if args.stage1_file else canonical_stage1
        if not stage1_source.exists():
            raise FileNotFoundError(f"Stage1 bestand ontbreekt: {stage1_source}")
        cmd = [
            sys.executable, "-u", str(TARGET),
            "--mode", "ean-only",
            "--headless", str(args.headless),
            "--out-dir", str(out_dir),
            "--stage1-file", str(stage1_source),
            "--limit-ean", str(max(1, args.limit_ean)),
        ]
        run_command(cmd)
        shutil.copy2(find_latest_final(out_dir), canonical_final)
        print(f"[CANONICAL] final -> {canonical_final}")
        return 0

    cmd = [
        sys.executable, "-u", str(TARGET),
        "--mode", "full",
        "--headless", str(args.headless),
        "--out-dir", str(out_dir),
        "--max-pages", str(max(1, args.max_pages)),
        "--limit-ean", str(max(1, args.limit_ean)),
    ]
    run_command(cmd)
    shutil.copy2(find_latest_stage1(out_dir), canonical_stage1)
    shutil.copy2(find_latest_final(out_dir), canonical_final)
    print(f"[CANONICAL] stage1 -> {canonical_stage1}")
    print(f"[CANONICAL] final -> {canonical_final}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
