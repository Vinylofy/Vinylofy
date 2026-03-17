#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import shlex
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(CURRENT_FILE.parent) not in sys.path:
    sys.path.insert(0, str(CURRENT_FILE.parent))

from pipeline_config import SHOPS, ShopPipelineConfig, get_shop_config  # noqa: E402


@dataclass
class UploadResult:
    local_path: str
    remote_path: str
    ok: bool
    error: str | None = None


@dataclass
class ShopRunResult:
    shop: str
    started_at: str
    finished_at: str | None = None
    scraper_command: str | None = None
    scraper_ran: bool = False
    scraper_ok: bool = False
    importer_ok: bool = False
    upload_ok: bool = False
    csv_path: str | None = None
    rejects_path: str | None = None
    summary_path: str | None = None
    importer_summary: dict[str, Any] | None = None
    uploads: list[UploadResult] = field(default_factory=list)
    error: str | None = None


class PipelineError(RuntimeError):
    pass


def utc_now() -> datetime:
    return datetime.now(UTC)


def log(message: str) -> None:
    timestamp = utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{timestamp}] {message}", flush=True)


def run_command(command: str | list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    if isinstance(command, str):
        return subprocess.run(command, cwd=cwd, shell=True, text=True, capture_output=True, check=False)
    return subprocess.run(command, cwd=cwd, text=True, capture_output=True, check=False)


def emit_process_output(label: str, proc: subprocess.CompletedProcess[str]) -> None:
    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n", flush=True)
    if proc.stderr:
        log(f"[{label}] stderr:")
        print(proc.stderr, end="" if proc.stderr.endswith("\n") else "\n", file=sys.stderr, flush=True)


def ensure_file_exists(path: Path, description: str) -> None:
    if not path.exists():
        raise PipelineError(f"{description} not found: {path}")


def maybe_load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def build_storage_path(prefix: str, category: str, local_path: Path, timestamp: datetime) -> str:
    date_part = timestamp.strftime("%Y/%m/%d")
    ts = timestamp.strftime("%Y%m%dT%H%M%SZ")
    return f"{prefix}/{category}/{date_part}/{ts}_{local_path.name}"


def upload_files_to_supabase(
    files: list[tuple[str, Path]],
    shop_config: ShopPipelineConfig,
    timestamp: datetime,
) -> list[UploadResult]:
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SECRET_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    bucket = os.getenv("SUPABASE_STORAGE_BUCKET")

    if not url or not key or not bucket:
        raise PipelineError(
            "Supabase upload requested but SUPABASE_URL, SUPABASE_SECRET_KEY (or SUPABASE_SERVICE_ROLE_KEY), "
            "and SUPABASE_STORAGE_BUCKET are not all set."
        )

    try:
        from supabase import create_client
    except ImportError as exc:
        raise PipelineError(
            "Package 'supabase' is not installed. Install it before using uploads."
        ) from exc

    client = create_client(url, key)
    results: list[UploadResult] = []

    for category, local_path in files:
        if not local_path.exists():
            results.append(
                UploadResult(
                    local_path=str(local_path),
                    remote_path="",
                    ok=False,
                    error="file_missing",
                )
            )
            continue

        remote_path = build_storage_path(shop_config.storage_prefix, category, local_path, timestamp)
        content_type = mimetypes.guess_type(local_path.name)[0] or "application/octet-stream"

        with local_path.open("rb") as handle:
            try:
                client.storage.from_(bucket).upload(
                    path=remote_path,
                    file=handle,
                    file_options={
                        "content-type": content_type,
                        "upsert": "true",
                    },
                )
                results.append(
                    UploadResult(
                        local_path=str(local_path),
                        remote_path=remote_path,
                        ok=True,
                    )
                )
            except Exception as exc:
                results.append(
                    UploadResult(
                        local_path=str(local_path),
                        remote_path=remote_path,
                        ok=False,
                        error=str(exc),
                    )
                )

    return results


def resolve_shops(selection: str) -> list[ShopPipelineConfig]:
    if selection == "all":
        return [SHOPS[key] for key in sorted(SHOPS)]
    return [get_shop_config(selection)]


def run_single_shop(
    shop_config: ShopPipelineConfig,
    *,
    skip_scrape: bool,
    skip_import: bool,
    skip_upload: bool,
    dry_run_import: bool,
) -> ShopRunResult:
    started = utc_now()
    result = ShopRunResult(shop=shop_config.key, started_at=started.isoformat())

    csv_path = PROJECT_ROOT / shop_config.csv_output_path
    rejects_path = PROJECT_ROOT / shop_config.rejects_path
    summary_path = PROJECT_ROOT / shop_config.summary_path

    result.csv_path = str(csv_path)
    result.rejects_path = str(rejects_path)
    result.summary_path = str(summary_path)

    if not skip_scrape:
        scraper_command = os.getenv(shop_config.scraper_command_env, "").strip()
        result.scraper_command = scraper_command or None
        if not scraper_command:
            raise PipelineError(
                f"Environment variable {shop_config.scraper_command_env} is empty. "
                "Set it in your shell or GitHub Actions variables."
            )

        log(f"[{shop_config.key}] Starting scraper: {scraper_command}")
        scraper_proc = run_command(scraper_command, cwd=PROJECT_ROOT)
        emit_process_output(f"{shop_config.key}:scraper", scraper_proc)
        result.scraper_ran = True
        result.scraper_ok = scraper_proc.returncode == 0
        if scraper_proc.returncode != 0:
            raise PipelineError(f"Scraper failed for {shop_config.key} with exit code {scraper_proc.returncode}")
    else:
        log(f"[{shop_config.key}] Scrape skipped")
        result.scraper_ok = True

    ensure_file_exists(csv_path, f"CSV output for {shop_config.key}")

    if not skip_import:
        importer_command = list(shop_config.importer_command)
        if dry_run_import:
            importer_command.append("--dry-run")

        log(f"[{shop_config.key}] Starting importer")
        importer_proc = run_command(importer_command, cwd=PROJECT_ROOT)
        emit_process_output(f"{shop_config.key}:importer", importer_proc)
        result.importer_ok = importer_proc.returncode == 0
        if importer_proc.returncode != 0:
            raise PipelineError(f"Importer failed for {shop_config.key} with exit code {importer_proc.returncode}")

        result.importer_summary = maybe_load_json(summary_path)
    else:
        log(f"[{shop_config.key}] Import skipped")
        result.importer_ok = True

    upload_candidates = [
        ("raw", csv_path),
        ("summary", summary_path),
        ("rejects", rejects_path),
    ]

    if not skip_upload:
        log(f"[{shop_config.key}] Uploading artifacts to Supabase Storage")
        uploads = upload_files_to_supabase(upload_candidates, shop_config, started)
        result.uploads = uploads
        result.upload_ok = all(item.ok for item in uploads if item.remote_path or item.local_path)
    else:
        log(f"[{shop_config.key}] Upload skipped")
        result.upload_ok = True

    result.finished_at = utc_now().isoformat()
    return result


def write_pipeline_summary(results: list[ShopRunResult]) -> Path:
    summary_dir = PROJECT_ROOT / "output" / "pipeline_runs"
    summary_dir.mkdir(parents=True, exist_ok=True)

    timestamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    summary_path = summary_dir / f"pipeline_run_{timestamp}.json"

    payload = {
        "generated_at": utc_now().isoformat(),
        "results": [asdict(item) for item in results],
    }
    summary_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return summary_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Vinylofy scraper/import pipeline")
    parser.add_argument("--shop", default="all", help="all | bobsvinyl | dgmoutlet | platomania")
    parser.add_argument("--skip-scrape", action="store_true")
    parser.add_argument("--skip-import", action="store_true")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--dry-run-import", action="store_true")
    args = parser.parse_args()

    selected_shops = resolve_shops(args.shop)
    results: list[ShopRunResult] = []
    exit_code = 0

    for shop_config in selected_shops:
        try:
            result = run_single_shop(
                shop_config,
                skip_scrape=args.skip_scrape,
                skip_import=args.skip_import,
                skip_upload=args.skip_upload,
                dry_run_import=args.dry_run_import,
            )
        except Exception as exc:
            exit_code = 1
            result = ShopRunResult(
                shop=shop_config.key,
                started_at=utc_now().isoformat(),
                finished_at=utc_now().isoformat(),
                error=str(exc),
            )
            log(f"[{shop_config.key}] FAILED: {exc}")

        results.append(result)

    summary_path = write_pipeline_summary(results)
    log(f"Pipeline summary written to {summary_path}")

    print(json.dumps({"results": [asdict(item) for item in results]}, indent=2, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())