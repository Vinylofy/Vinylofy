#!/usr/bin/env python3
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parent
LEGACY_DIR = ROOT / 'legacy'

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p

def legacy_path(filename: str) -> Path:
    return LEGACY_DIR / filename

def run_legacy(filename: str, args: Iterable[str] | None = None, cwd: str | Path | None = None, stdin_data: str | None = None) -> int:
    cmd = [sys.executable, str(legacy_path(filename))]
    if args:
        cmd.extend(list(args))
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd is not None else None,
        input=stdin_data,
        text=True,
    )
    return int(result.returncode)

def move_if_exists(src: str | Path, dst: str | Path) -> None:
    src_p = Path(src)
    dst_p = Path(dst)
    if src_p.exists():
        dst_p.parent.mkdir(parents=True, exist_ok=True)
        if dst_p.exists():
            if dst_p.is_file():
                dst_p.unlink()
        shutil.move(str(src_p), str(dst_p))
