#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
from typing import Any
from uuid import UUID


ROOT = Path(__file__).resolve().parents[2]

CANDIDATE_REFRESH = (
    ROOT
    / "scripts"
    / "maintenance"
    / "cover_candidate_refresh.py"
)

COVER_WORKER = (
    ROOT
    / "scripts"
    / "maintenance"
    / "cover_worker.py"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run bounded, resumable batches through the existing "
            "central Vinylofy cover pipeline."
        )
    )
    parser.add_argument(
        "--refresh-limit",
        type=int,
        default=100,
    )
    parser.add_argument(
        "--publish-limit",
        type=int,
        default=100,
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=3,
    )
    parser.add_argument(
        "--checkpoint",
        default="",
        help=(
            "products.id checkpoint from the previous batch/run. "
            "Empty starts at the beginning of the eligible catalog."
        ),
    )
    parser.add_argument(
        "--include-covered",
        action="store_true",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
    )
    parser.add_argument(
        "--output-dir",
        default="output/cover_pipeline/backfill",
    )
    parser.add_argument(
        "--output-json",
        default="output/cover_pipeline/backfill_progress.json",
    )

    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    for name in (
        "refresh_limit",
        "publish_limit",
        "max_batches",
    ):
        value = int(getattr(args, name))
        if value <= 0:
            raise ValueError(
                f"--{name.replace('_', '-')} must be greater than zero"
            )

    if args.max_batches > 20:
        raise ValueError(
            "--max-batches may not exceed 20"
        )

    if args.checkpoint:
        UUID(args.checkpoint)


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(
        path.read_text(encoding="utf-8")
    )
    if not isinstance(payload, dict):
        raise ValueError(
            f"Expected a JSON object in {path}"
        )
    return payload


def write_json(
    path: Path,
    payload: dict[str, Any],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )
    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def metric(
    payload: dict[str, Any],
    key: str,
) -> int:
    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        return 0

    try:
        return int(metrics.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def build_candidate_command(
    args: argparse.Namespace,
    *,
    checkpoint: str,
    output_path: Path,
) -> list[str]:
    command = [
        sys.executable,
        "-u",
        str(CANDIDATE_REFRESH.relative_to(ROOT)),
        "--limit",
        str(args.refresh_limit),
        "--max-offers-per-product",
        "3",
        "--output-json",
        str(output_path.relative_to(ROOT)),
    ]

    if checkpoint:
        command.extend(
            [
                "--checkpoint",
                checkpoint,
            ]
        )

    if args.include_covered:
        command.append(
            "--include-covered"
        )

    if args.dry_run:
        command.append(
            "--dry-run"
        )

    return command


def build_worker_command(
    args: argparse.Namespace,
    *,
    output_path: Path,
) -> list[str]:
    command = [
        sys.executable,
        "-u",
        str(COVER_WORKER.relative_to(ROOT)),
        "--mode",
        "publish",
        "--limit",
        str(args.publish_limit),
        "--output-json",
        str(output_path.relative_to(ROOT)),
    ]

    if args.dry_run:
        command.append(
            "--dry-run"
        )

    return command


def run_child(command: list[str]) -> None:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
    )

    if completed.returncode != 0:
        raise RuntimeError(
            "Child command failed with return code "
            f"{completed.returncode}: "
            + " ".join(command)
        )


def add_metrics(
    totals: dict[str, int],
    payload: dict[str, Any],
    keys: tuple[str, ...],
) -> None:
    for key in keys:
        totals[key] += metric(
            payload,
            key,
        )


def initial_progress(
    args: argparse.Namespace,
) -> dict[str, Any]:
    start = args.checkpoint or None

    return {
        "schema_version": 1,
        "dry_run": bool(args.dry_run),
        "include_covered": bool(
            args.include_covered
        ),
        "refresh_limit": args.refresh_limit,
        "publish_limit": args.publish_limit,
        "max_batches": args.max_batches,
        "checkpoint_start": start,
        "checkpoint_end": start,
        "resume_checkpoint": start,
        "has_more": True,
        "catalog_pass_complete": False,
        "batches_completed": 0,
        "failed_at_batch": None,
        "fatal_error": None,
        "metrics": {
            "products_selected": 0,
            "products_with_candidates": 0,
            "products_without_candidates": 0,
            "candidates_discovered": 0,
            "candidate_rows_inserted": 0,
            "candidate_rows_updated": 0,
            "queue_rows_touched": 0,
            "refresh_errors": 0,
            "claims": 0,
            "published": 0,
            "repaired_local_cover": 0,
            "reused_local_cover": 0,
            "retry_later": 0,
            "review": 0,
            "failed": 0,
            "downloads": 0,
            "uploads": 0,
        },
        "batches": [],
    }


def run_backfill(
    args: argparse.Namespace,
) -> dict[str, Any]:
    validate_args(args)

    output_dir = (
        ROOT / args.output_dir
    ).resolve()

    output_path = (
        ROOT / args.output_json
    ).resolve()

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    progress = initial_progress(args)
    write_json(
        output_path,
        progress,
    )

    checkpoint = args.checkpoint

    for batch_number in range(
        1,
        args.max_batches + 1,
    ):
        refresh_path = (
            output_dir
            / (
                "candidate_refresh_batch_"
                f"{batch_number:03d}.json"
            )
        )

        publish_path = (
            output_dir
            / (
                "publish_batch_"
                f"{batch_number:03d}.json"
            )
        )

        batch: dict[str, Any] = {
            "batch": batch_number,
            "checkpoint_start": (
                checkpoint or None
            ),
            "checkpoint_end": None,
            "has_more": None,
            "candidate_refresh_summary": str(
                refresh_path.relative_to(ROOT)
            ),
            "publish_summary": None,
            "refresh_metrics": {},
            "publish_metrics": {},
        }

        progress["batches"].append(batch)

        try:
            run_child(
                build_candidate_command(
                    args,
                    checkpoint=checkpoint,
                    output_path=refresh_path,
                )
            )

            refresh = read_json(
                refresh_path
            )

            selected = metric(
                refresh,
                "products_selected",
            )

            refresh_products = refresh.get(
                "products"
            )

            products_with_candidates = (
                len(refresh_products)
                if isinstance(
                    refresh_products,
                    list,
                )
                else 0
            )

            products_without_candidates = max(
                0,
                selected
                - products_with_candidates,
            )

            checkpoint_end = str(
                refresh.get("checkpoint_end")
                or checkpoint
                or ""
            )

            has_more = bool(
                refresh.get("has_more")
            )

            batch["checkpoint_end"] = (
                checkpoint_end or None
            )
            batch["has_more"] = has_more
            batch["refresh_metrics"] = (
                refresh.get("metrics", {})
            )

            progress["checkpoint_end"] = (
                checkpoint_end or None
            )

            progress["resume_checkpoint"] = (
                checkpoint_end or None
            )

            progress["has_more"] = has_more

            progress["batches_completed"] = (
                batch_number
            )

            add_metrics(
                progress["metrics"],
                refresh,
                (
                    "products_selected",
                    "candidates_discovered",
                    "candidate_rows_inserted",
                    "candidate_rows_updated",
                    "queue_rows_touched",
                ),
            )

            progress["metrics"][
                "products_with_candidates"
            ] += products_with_candidates

            progress["metrics"][
                "products_without_candidates"
            ] += products_without_candidates

            errors = refresh.get("errors")
            if isinstance(errors, list):
                progress["metrics"][
                    "refresh_errors"
                ] += len(errors)

            # Critical resumability contract:
            # persist the new keyset checkpoint BEFORE publish.
            #
            # Candidate refresh has already completed at this point.
            # If publish fails afterwards, a future run can continue
            # after this exact catalog position while the queued work
            # remains available to the central worker.
            write_json(
                output_path,
                progress,
            )

            if selected == 0:
                progress[
                    "catalog_pass_complete"
                ] = True
                progress["has_more"] = False
                progress[
                    "resume_checkpoint"
                ] = None

                write_json(
                    output_path,
                    progress,
                )
                break

            run_child(
                build_worker_command(
                    args,
                    output_path=publish_path,
                )
            )

            publish = read_json(
                publish_path
            )

            batch["publish_summary"] = str(
                publish_path.relative_to(ROOT)
            )

            batch["publish_metrics"] = (
                publish.get("metrics", {})
            )

            add_metrics(
                progress["metrics"],
                publish,
                (
                    "claims",
                    "published",
                    "repaired_local_cover",
                    "reused_local_cover",
                    "retry_later",
                    "review",
                    "failed",
                    "downloads",
                    "uploads",
                ),
            )

            checkpoint = checkpoint_end

            if not has_more:
                progress[
                    "catalog_pass_complete"
                ] = True

                progress[
                    "resume_checkpoint"
                ] = None

                write_json(
                    output_path,
                    progress,
                )
                break

            write_json(
                output_path,
                progress,
            )

        except Exception as exc:
            progress["failed_at_batch"] = (
                batch_number
            )

            progress["fatal_error"] = (
                f"{type(exc).__name__}: {exc}"
            )

            write_json(
                output_path,
                progress,
            )

            raise

    write_json(
        output_path,
        progress,
    )

    return progress


def main() -> None:
    args = parse_args()

    progress = run_backfill(args)

    print(
        "[DONE] cover backfill | "
        f"dry_run={progress['dry_run']} | "
        f"batches={progress['batches_completed']} | "
        f"checkpoint={progress['checkpoint_end']} | "
        f"has_more={progress['has_more']} | "
        "catalog_pass_complete="
        f"{progress['catalog_pass_complete']} | "
        f"published="
        f"{progress['metrics']['published']} | "
        f"downloads="
        f"{progress['metrics']['downloads']} | "
        f"uploads="
        f"{progress['metrics']['uploads']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
