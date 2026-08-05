#!/usr/bin/env python3
"""Exporteer een exact, read-only manifest van de Vinylofy-coverbucket.

Het manifest:
- inventariseert alle bestaande objectpaden recursief;
- gebruikt gepagineerde, deterministisch gesorteerde Storage-listcalls;
- schrijft geen objecten en verwijdert niets;
- schrijft atomair naar een directory buiten de repository;
- bindt het resultaat aan de exacte forward-migration-SHA.

Binaire SHA-256-hashes zijn optioneel. Zonder --hash-binaries worden geen
objectbinaries gedownload.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import traceback
from typing import Any, Iterable

try:
    from dotenv import load_dotenv
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "python-dotenv ontbreekt. Installeer de repositorydependencies."
    ) from exc

try:
    from supabase import create_client
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "supabase-py ontbreekt. Installeer de repositorydependencies."
    ) from exc


WORKTREE = Path("/workspaces/Vinylofy-cover-localization-20260804-101907")
FORWARD_RELATIVE_PATH = Path(
    "supabase/migrations/20260805123000_finalize_central_cover_pipeline.sql"
)
FORWARD_SHA256 = "872aed286523d3b5cb32b11db2c0d901a365a81d60ccb9437097f7d854d9d1be"
DEFAULT_BUCKET = "product-covers"
DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 1000
MAX_DIRECTORIES = 1_000_000


class ManifestError(RuntimeError):
    """Duidelijke fout voor onveilige of onverwachte manifesttoestanden."""


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def load_environment() -> None:
    load_dotenv(WORKTREE / ".env.local", override=True)
    load_dotenv(WORKTREE / ".env", override=True)
    load_dotenv(override=True)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_args() -> argparse.Namespace:
    load_environment()

    parser = argparse.ArgumentParser(
        description="Exporteer het exacte pre-migration Storage-objectmanifest."
    )
    parser.add_argument(
        "--supabase-url",
        default=os.getenv("SUPABASE_URL", ""),
        help="Supabase-project-URL. Standaard: SUPABASE_URL.",
    )
    parser.add_argument(
        "--service-key",
        default=(
            os.getenv("SUPABASE_SERVICE_ROLE_KEY")
            or os.getenv("SUPABASE_SECRET_KEY")
            or ""
        ),
        help=(
            "Service-role/secret key. Standaard uit "
            "SUPABASE_SERVICE_ROLE_KEY of SUPABASE_SECRET_KEY."
        ),
    )
    parser.add_argument(
        "--bucket",
        default=(
            os.getenv("VINYLOFY_COVER_STORAGE_BUCKET")
            or DEFAULT_BUCKET
        ),
        help="Coverbucket. Standaard: product-covers.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Aantal listresultaten per pagina, maximaal 1000.",
    )
    parser.add_argument(
        "--hash-binaries",
        action="store_true",
        help=(
            "Download ieder object read-only en leg SHA-256 en werkelijke "
            "bytegrootte vast."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp") / f"vinylofy-cover-storage-manifest-{utc_stamp()}",
        help="Nieuwe outputdirectory buiten de repository.",
    )
    return parser.parse_args()


def validate_runtime_args(args: argparse.Namespace) -> None:
    if not str(args.supabase_url).strip():
        raise ManifestError(
            "SUPABASE_URL ontbreekt. Geef --supabase-url mee of zet de env var."
        )
    if not str(args.service_key).strip():
        raise ManifestError(
            "Supabase service-role/secret key ontbreekt."
        )
    if not str(args.bucket).strip():
        raise ManifestError("Bucketnaam mag niet leeg zijn.")
    if not 1 <= int(args.page_size) <= MAX_PAGE_SIZE:
        raise ManifestError(
            f"--page-size moet tussen 1 en {MAX_PAGE_SIZE} liggen."
        )

    resolved = args.output_dir.expanduser().resolve()
    worktree = WORKTREE.resolve()
    if resolved == worktree or worktree in resolved.parents:
        raise ManifestError(
            "Het runtime-manifest mag niet binnen de repository worden geschreven."
        )
    if resolved.exists():
        raise ManifestError(f"Outputdirectory bestaat al: {resolved}")


def validate_forward_migration() -> Path:
    migration = WORKTREE / FORWARD_RELATIVE_PATH
    if not migration.is_file():
        raise ManifestError(f"Forward migration ontbreekt: {migration}")

    actual = sha256_file(migration)
    if actual != FORWARD_SHA256:
        raise ManifestError(
            "Forward-migration-SHA wijkt af: "
            f"verwacht {FORWARD_SHA256}, vond {actual}"
        )
    return migration


def normalize_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump()
        if isinstance(dumped, dict):
            return dict(dumped)

    as_dict = getattr(value, "dict", None)
    if callable(as_dict):
        dumped = as_dict()
        if isinstance(dumped, dict):
            return dict(dumped)

    attributes = getattr(value, "__dict__", None)
    if isinstance(attributes, dict):
        return {
            key: item
            for key, item in attributes.items()
            if not key.startswith("_")
        }

    raise ManifestError(
        f"Onverwacht Storage-listitemtype: {type(value).__name__}"
    )


def normalize_list_response(response: Any) -> list[dict[str, Any]]:
    if response is None:
        return []

    if isinstance(response, list):
        raw_items = response
    elif isinstance(response, dict) and isinstance(response.get("data"), list):
        raw_items = response["data"]
    else:
        data = getattr(response, "data", None)
        if isinstance(data, list):
            raw_items = data
        else:
            raise ManifestError(
                f"Onverwacht Storage-listresponsetype: {type(response).__name__}"
            )

    return [normalize_mapping(item) for item in raw_items]


def clean_prefix(prefix: str) -> str:
    return "/".join(part for part in prefix.strip("/").split("/") if part)


def join_storage_path(prefix: str, name: str) -> str:
    clean_name = str(name).strip("/")
    if not clean_name:
        raise ManifestError("Storage-listitem zonder geldige naam aangetroffen.")
    return f"{prefix}/{clean_name}" if prefix else clean_name


def normalize_metadata(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    return {"raw_value": value}


def is_folder_item(item: dict[str, Any]) -> bool:
    metadata = item.get("metadata")
    object_id = item.get("id")
    return not object_id and metadata in (None, {})


def walk_storage(
    bucket_api: Any,
    page_size: int,
) -> tuple[list[dict[str, Any]], list[str], int]:
    pending_directories = [""]
    visited_directories: set[str] = set()
    seen_objects: set[str] = set()
    objects: list[dict[str, Any]] = []
    list_calls = 0

    while pending_directories:
        prefix = clean_prefix(pending_directories.pop(0))
        if prefix in visited_directories:
            continue

        visited_directories.add(prefix)
        if len(visited_directories) > MAX_DIRECTORIES:
            raise ManifestError(
                "Folderlimiet overschreden; mogelijke cyclische listrespons."
            )

        offset = 0
        while True:
            response = bucket_api.list(
                prefix,
                {
                    "limit": page_size,
                    "offset": offset,
                    "sortBy": {"column": "name", "order": "asc"},
                },
            )
            list_calls += 1
            page = normalize_list_response(response)

            if not page:
                break

            for item in page:
                name = item.get("name")
                path = join_storage_path(prefix, str(name or ""))

                if is_folder_item(item):
                    if path not in visited_directories:
                        pending_directories.append(path)
                    continue

                if path in seen_objects:
                    raise ManifestError(
                        f"Dubbel Storage-objectpad aangetroffen: {path}"
                    )
                seen_objects.add(path)

                metadata = normalize_metadata(item.get("metadata"))
                raw_record = dict(item)
                raw_record["path"] = path
                raw_record["metadata"] = metadata

                size_value = metadata.get("size")
                try:
                    metadata_size = (
                        int(size_value) if size_value is not None else None
                    )
                except (TypeError, ValueError):
                    metadata_size = None

                objects.append(
                    {
                        "path": path,
                        "id": item.get("id"),
                        "created_at": item.get("created_at"),
                        "updated_at": item.get("updated_at"),
                        "last_accessed_at": item.get("last_accessed_at"),
                        "metadata_size": metadata_size,
                        "metadata_mimetype": (
                            metadata.get("mimetype")
                            or metadata.get("contentType")
                        ),
                        "metadata_cache_control": (
                            metadata.get("cacheControl")
                            or metadata.get("cache-control")
                        ),
                        "metadata_etag": (
                            metadata.get("eTag")
                            or metadata.get("etag")
                        ),
                        "user_metadata": item.get("user_metadata"),
                        "raw": raw_record,
                    }
                )

            offset += len(page)
            if len(page) < page_size:
                break

    objects.sort(key=lambda item: str(item["path"]))
    directories = sorted(visited_directories)
    return objects, directories, list_calls


def hash_object_binaries(
    bucket_api: Any,
    objects: list[dict[str, Any]],
) -> None:
    for index, item in enumerate(objects, start=1):
        path = str(item["path"])
        binary = bucket_api.download(path)
        if not isinstance(binary, (bytes, bytearray)):
            raise ManifestError(
                f"Onverwacht downloadtype voor {path}: {type(binary).__name__}"
            )

        content = bytes(binary)
        item["binary_sha256"] = hashlib.sha256(content).hexdigest()
        item["binary_byte_size"] = len(content)
        print(
            f"[HASH] {index}/{len(objects)} path={path} bytes={len(content)}",
            flush=True,
        )


def json_default(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=json_default,
        )
        + "\n",
        encoding="utf-8",
    )


def write_objects_csv(
    path: Path,
    bucket: str,
    objects: Iterable[dict[str, Any]],
) -> int:
    fieldnames = (
        "bucket",
        "path",
        "id",
        "created_at",
        "updated_at",
        "last_accessed_at",
        "metadata_size",
        "metadata_mimetype",
        "metadata_cache_control",
        "metadata_etag",
        "binary_byte_size",
        "binary_sha256",
        "user_metadata_json",
        "raw_json",
    )

    rows = list(objects)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()

        for item in rows:
            writer.writerow(
                {
                    "bucket": bucket,
                    "path": item.get("path"),
                    "id": item.get("id"),
                    "created_at": item.get("created_at"),
                    "updated_at": item.get("updated_at"),
                    "last_accessed_at": item.get("last_accessed_at"),
                    "metadata_size": item.get("metadata_size"),
                    "metadata_mimetype": item.get("metadata_mimetype"),
                    "metadata_cache_control": item.get(
                        "metadata_cache_control"
                    ),
                    "metadata_etag": item.get("metadata_etag"),
                    "binary_byte_size": item.get("binary_byte_size"),
                    "binary_sha256": item.get("binary_sha256"),
                    "user_metadata_json": json.dumps(
                        item.get("user_metadata"),
                        ensure_ascii=False,
                        sort_keys=True,
                        default=json_default,
                    ),
                    "raw_json": json.dumps(
                        item.get("raw"),
                        ensure_ascii=False,
                        sort_keys=True,
                        default=json_default,
                    ),
                }
            )

    return len(rows)


def write_checksums(root: Path) -> dict[str, str]:
    checksums: dict[str, str] = {}
    files = sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.name != "checksums.sha256"
    )

    for path in files:
        relative = path.relative_to(root).as_posix()
        checksums[relative] = sha256_file(path)

    (root / "checksums.sha256").write_text(
        "".join(
            f"{digest}  {relative}\n"
            for relative, digest in sorted(checksums.items())
        ),
        encoding="utf-8",
    )
    return checksums


def main() -> int:
    args = parse_args()
    validate_runtime_args(args)
    validate_forward_migration()

    output_dir = args.output_dir.expanduser().resolve()
    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temp_root = Path(
        tempfile.mkdtemp(
            prefix=output_dir.name + ".",
            suffix=".tmp",
            dir=output_dir.parent,
        )
    )

    try:
        client = create_client(
            str(args.supabase_url).rstrip("/"),
            str(args.service_key),
        )
        bucket_api = client.storage.from_(str(args.bucket))

        objects, directories, list_calls = walk_storage(
            bucket_api,
            int(args.page_size),
        )

        if args.hash_binaries:
            hash_object_binaries(bucket_api, objects)

        write_json(temp_root / "objects.json", objects)
        object_count = write_objects_csv(
            temp_root / "objects.csv",
            str(args.bucket),
            objects,
        )
        (temp_root / "object-paths.txt").write_text(
            "".join(f"{item['path']}\n" for item in objects),
            encoding="utf-8",
        )
        (temp_root / "directories.txt").write_text(
            "".join(
                f"{directory or '<root>'}\n"
                for directory in directories
            ),
            encoding="utf-8",
        )

        metadata_bytes = sum(
            int(item["metadata_size"])
            for item in objects
            if item.get("metadata_size") is not None
        )
        binary_bytes = sum(
            int(item["binary_byte_size"])
            for item in objects
            if item.get("binary_byte_size") is not None
        )

        manifest = {
            "artifact_type": "vinylofy_cover_storage_object_manifest",
            "format_version": 1,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "forward_migration": FORWARD_RELATIVE_PATH.as_posix(),
            "forward_migration_sha256": FORWARD_SHA256,
            "supabase_url": str(args.supabase_url).rstrip("/"),
            "bucket": str(args.bucket),
            "object_count": object_count,
            "directory_count_including_root": len(directories),
            "list_call_count": list_calls,
            "page_size": int(args.page_size),
            "metadata_total_bytes": metadata_bytes,
            "binary_hashes_included": bool(args.hash_binaries),
            "binary_total_bytes_when_hashed": (
                binary_bytes if args.hash_binaries else None
            ),
            "storage_list_calls_only": not bool(args.hash_binaries),
            "storage_writes_performed": False,
            "storage_deletes_performed": False,
            "secrets_included": False,
        }
        write_json(temp_root / "manifest.json", manifest)

        readme = (
            "Vinylofy centrale coverpipeline — Storage-objectmanifest\n"
            "\n"
            f"Bucket: {args.bucket}\n"
            f"Forward migration: {FORWARD_RELATIVE_PATH.as_posix()}\n"
            f"Forward SHA256: {FORWARD_SHA256}\n"
            "\n"
            "object-paths.txt is de exacte allowlist van objecten die vóór de\n"
            "migration bestonden. Rollback-/cleanupcode mag nooit op prefix,\n"
            "glob, EAN of extensie verwijderen; uitsluitend op exact berekende\n"
            "verschillen ten opzichte van dit manifest.\n"
        )
        (temp_root / "README.txt").write_text(readme, encoding="utf-8")

        checksums = write_checksums(temp_root)
        os.replace(temp_root, output_dir)

        print("== STORAGE-MANIFEST ==")
        print(f"output_dir={output_dir}")
        print(f"bucket={args.bucket}")
        print(f"forward_sha256={FORWARD_SHA256}")
        print(f"objects={object_count}")
        print(f"directories_including_root={len(directories)}")
        print(f"list_calls={list_calls}")
        print(f"metadata_total_bytes={metadata_bytes}")
        print(f"binary_hashes_included={str(bool(args.hash_binaries)).lower()}")
        print("storage_writes=0")
        print("storage_deletes=0")
        print("secrets_afgedrukt=nee")
        print(f"manifest_files={len(checksums) + 1}")
        return 0
    except Exception:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        print(f"[ERROR] {type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
        raise SystemExit(1)
