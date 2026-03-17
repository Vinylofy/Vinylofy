#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parents[2]

if str(CURRENT_FILE.parent) not in sys.path:
    sys.path.insert(0, str(CURRENT_FILE.parent))

from pipeline_config import SHOPS, ShopPipelineConfig, get_shop_config  # noqa: E402

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None


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
    rows_raw: int = 0
    rows_accepted: int = 0
    rows_rejected: int = 0
    new_products: int = 0
    new_price_rows: int = 0
    price_updates: int = 0
    unchanged_prices: int = 0
    upserted_products: int = 0
    upserted_prices: int = 0
    inserted_history_rows: int = 0
    uploaded_files: int = 0
    upload_failures: int = 0
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
    except ImportError as exc:  # pragma: no cover
        raise PipelineError("Package 'supabase' is not installed. Install it before using uploads.") from exc

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
            except Exception as exc:  # pragma: no cover
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


def create_initial_result(shop_config: ShopPipelineConfig) -> ShopRunResult:
    started = utc_now().isoformat()
    return ShopRunResult(
        shop=shop_config.key,
        started_at=started,
        csv_path=str(PROJECT_ROOT / shop_config.csv_output_path),
        rejects_path=str(PROJECT_ROOT / shop_config.rejects_path),
        summary_path=str(PROJECT_ROOT / shop_config.summary_path),
    )


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def populate_result_metrics_from_summary(result: ShopRunResult) -> None:
    summary = result.importer_summary or {}
    result.rows_raw = safe_int(summary.get("rows_raw"))
    result.rows_accepted = safe_int(summary.get("accepted_records"))
    result.rows_rejected = safe_int(summary.get("rejected_records"))
    if result.rows_raw == 0:
        result.rows_raw = result.rows_accepted + result.rows_rejected
    result.new_products = safe_int(summary.get("new_products"))
    result.new_price_rows = safe_int(summary.get("new_price_rows"))
    result.price_updates = safe_int(summary.get("price_updates"))
    result.unchanged_prices = safe_int(summary.get("unchanged_prices"))
    result.upserted_products = safe_int(summary.get("upserted_products"))
    result.upserted_prices = safe_int(summary.get("upserted_prices"))
    result.inserted_history_rows = safe_int(summary.get("inserted_history_rows"))


def summarize_uploads(result: ShopRunResult) -> None:
    result.uploaded_files = sum(1 for item in result.uploads if item.ok)
    result.upload_failures = sum(1 for item in result.uploads if not item.ok)


def iso_to_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def duration_seconds(started_at: str, finished_at: str | None) -> float | None:
    started = iso_to_datetime(started_at)
    finished = iso_to_datetime(finished_at)
    if started is None or finished is None:
        return None
    return round((finished - started).total_seconds(), 3)


def derive_status(result: ShopRunResult) -> str:
    if result.error:
        return "failed"
    if result.scraper_ok and result.importer_ok and result.upload_ok:
        return "success"
    if result.scraper_ok or result.importer_ok or result.upload_ok:
        return "partial"
    return "started"


def maybe_open_logging_connection():
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        log("[monitoring] DATABASE_URL ontbreekt; scraper_runs logging wordt overgeslagen")
        return None
    if psycopg is None:
        log("[monitoring] psycopg ontbreekt; scraper_runs logging wordt overgeslagen")
        return None

    try:
        conn = psycopg.connect(db_url)
        conn.autocommit = True
        return conn
    except Exception as exc:  # pragma: no cover
        log(f"[monitoring] Kon geen DB-verbinding openen voor scraper_runs logging: {exc}")
        return None


def resolve_shop_id(cur, shop_domain: str):
    cur.execute("select id from public.shops where domain = %s limit 1", (shop_domain,))
    row = cur.fetchone()
    return row[0] if row else None


def insert_run_log(conn, pipeline_run_id: str, shop_config: ShopPipelineConfig, result: ShopRunResult):
    if conn is None:
        return None

    try:
        with conn.cursor() as cur:
            shop_id = resolve_shop_id(cur, shop_config.shop_domain)
    except Exception:
        shop_id = None

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into public.scraper_runs (
                  pipeline_run_id,
                  shop_id,
                  shop_key,
                  shop_name,
                  shop_domain,
                  run_type,
                  status,
                  started_at,
                  scraper_command,
                  scraper_ran,
                  scraper_ok,
                  importer_ok,
                  upload_ok,
                  csv_path,
                  rejects_path,
                  summary_path,
                  rows_raw,
                  rows_accepted,
                  rows_rejected,
                  new_products,
                  new_price_rows,
                  price_updates,
                  unchanged_prices,
                  upserted_products,
                  upserted_prices,
                  inserted_history_rows,
                  uploaded_files,
                  upload_failures,
                  error_message,
                  importer_summary,
                  uploads,
                  created_at,
                  updated_at
                )
                values (
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                  now(), now()
                )
                returning id
                """,
                (
                    pipeline_run_id,
                    shop_id,
                    shop_config.key,
                    shop_config.shop_name,
                    shop_config.shop_domain,
                    "pipeline",
                    "started",
                    iso_to_datetime(result.started_at),
                    result.scraper_command,
                    result.scraper_ran,
                    result.scraper_ok,
                    result.importer_ok,
                    result.upload_ok,
                    result.csv_path,
                    result.rejects_path,
                    result.summary_path,
                    result.rows_raw,
                    result.rows_accepted,
                    result.rows_rejected,
                    result.new_products,
                    result.new_price_rows,
                    result.price_updates,
                    result.unchanged_prices,
                    result.upserted_products,
                    result.upserted_prices,
                    result.inserted_history_rows,
                    result.uploaded_files,
                    result.upload_failures,
                    result.error,
                    json.dumps(result.importer_summary or {}, ensure_ascii=False),
                    json.dumps([asdict(item) for item in result.uploads], ensure_ascii=False),
                ),
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as exc:  # pragma: no cover
        log(f"[monitoring] Kon scraper_runs startrecord niet schrijven voor {shop_config.key}: {exc}")
        return None


def update_run_log(conn, run_id, pipeline_run_id: str, shop_config: ShopPipelineConfig, result: ShopRunResult) -> None:
    if conn is None or run_id is None:
        return

    summarize_uploads(result)

    try:
        with conn.cursor() as cur:
            shop_id = resolve_shop_id(cur, shop_config.shop_domain)
            cur.execute(
                """
                update public.scraper_runs
                set pipeline_run_id = %s,
                    shop_id = %s,
                    shop_key = %s,
                    shop_name = %s,
                    shop_domain = %s,
                    run_type = %s,
                    status = %s,
                    started_at = %s,
                    finished_at = %s,
                    duration_seconds = %s,
                    scraper_command = %s,
                    scraper_ran = %s,
                    scraper_ok = %s,
                    importer_ok = %s,
                    upload_ok = %s,
                    csv_path = %s,
                    rejects_path = %s,
                    summary_path = %s,
                    rows_raw = %s,
                    rows_accepted = %s,
                    rows_rejected = %s,
                    new_products = %s,
                    new_price_rows = %s,
                    price_updates = %s,
                    unchanged_prices = %s,
                    upserted_products = %s,
                    upserted_prices = %s,
                    inserted_history_rows = %s,
                    uploaded_files = %s,
                    upload_failures = %s,
                    error_message = %s,
                    importer_summary = %s,
                    uploads = %s,
                    updated_at = now()
                where id = %s
                """,
                (
                    pipeline_run_id,
                    shop_id,
                    shop_config.key,
                    shop_config.shop_name,
                    shop_config.shop_domain,
                    "pipeline",
                    derive_status(result),
                    iso_to_datetime(result.started_at),
                    iso_to_datetime(result.finished_at),
                    duration_seconds(result.started_at, result.finished_at),
                    result.scraper_command,
                    result.scraper_ran,
                    result.scraper_ok,
                    result.importer_ok,
                    result.upload_ok,
                    result.csv_path,
                    result.rejects_path,
                    result.summary_path,
                    result.rows_raw,
                    result.rows_accepted,
                    result.rows_rejected,
                    result.new_products,
                    result.new_price_rows,
                    result.price_updates,
                    result.unchanged_prices,
                    result.upserted_products,
                    result.upserted_prices,
                    result.inserted_history_rows,
                    result.uploaded_files,
                    result.upload_failures,
                    result.error,
                    json.dumps(result.importer_summary or {}, ensure_ascii=False),
                    json.dumps([asdict(item) for item in result.uploads], ensure_ascii=False),
                    run_id,
                ),
            )
    except Exception as exc:  # pragma: no cover
        log(f"[monitoring] Kon scraper_runs eindrecord niet updaten voor {shop_config.key}: {exc}")


def run_single_shop(
    result: ShopRunResult,
    shop_config: ShopPipelineConfig,
    *,
    skip_scrape: bool,
    skip_import: bool,
    skip_upload: bool,
    dry_run_import: bool,
) -> None:
    started = utc_now()
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
        populate_result_metrics_from_summary(result)
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
        summarize_uploads(result)
        result.upload_ok = result.upload_failures == 0
        if not result.upload_ok:
            raise PipelineError(f"Upload failed for {shop_config.key}: {result.upload_failures} file(s)")
    else:
        log(f"[{shop_config.key}] Upload skipped")
        result.upload_ok = True

    result.finished_at = utc_now().isoformat()


def write_pipeline_summary(results: list[ShopRunResult], pipeline_run_id: str) -> Path:
    summary_dir = PROJECT_ROOT / "output" / "pipeline_runs"
    summary_dir.mkdir(parents=True, exist_ok=True)

    timestamp = utc_now().strftime("%Y%m%dT%H%M%SZ")
    summary_path = summary_dir / f"pipeline_run_{timestamp}.json"

    payload = {
        "generated_at": utc_now().isoformat(),
        "pipeline_run_id": pipeline_run_id,
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
    pipeline_run_id = f"{utc_now().strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    logging_conn = maybe_open_logging_connection()

    for shop_config in selected_shops:
        result = create_initial_result(shop_config)
        run_id = insert_run_log(logging_conn, pipeline_run_id, shop_config, result)

        try:
            run_single_shop(
                result,
                shop_config,
                skip_scrape=args.skip_scrape,
                skip_import=args.skip_import,
                skip_upload=args.skip_upload,
                dry_run_import=args.dry_run_import,
            )
        except Exception as exc:
            exit_code = 1
            result.error = str(exc)
            result.finished_at = result.finished_at or utc_now().isoformat()
            log(f"[{shop_config.key}] FAILED: {exc}")
        finally:
            if result.finished_at is None:
                result.finished_at = utc_now().isoformat()
            if result.importer_summary is None and result.summary_path:
                result.importer_summary = maybe_load_json(Path(result.summary_path))
                if result.importer_summary:
                    populate_result_metrics_from_summary(result)
            update_run_log(logging_conn, run_id, pipeline_run_id, shop_config, result)
            results.append(result)

    if logging_conn is not None:
        logging_conn.close()

    summary_path = write_pipeline_summary(results, pipeline_run_id)
    log(f"Pipeline summary written to {summary_path}")

    print(json.dumps({"pipeline_run_id": pipeline_run_id, "results": [asdict(item) for item in results]}, indent=2, ensure_ascii=False))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())