#!/usr/bin/env python3
"""Maak een exacte, read-only snapshot vóór de centrale covermigration.

Dit script:
- wijzigt de database niet;
- exporteert uitsluitend covergerelateerde databasevelden en catalogusmetadata;
- controleert dat de database nog de pre-migration schemavorm heeft;
- bindt de snapshot aan de exacte forward-migration-SHA;
- schrijft atomair naar een directory buiten de repository.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import traceback
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

try:
    import psycopg
    from psycopg import sql
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "psycopg ontbreekt. Installeer de repositorydependencies voordat je exporteert."
    ) from exc


WORKTREE = Path("/workspaces/Vinylofy-cover-localization-20260804-101907")
FORWARD_RELATIVE_PATH = Path(
    "supabase/migrations/20260805123000_finalize_central_cover_pipeline.sql"
)
FORWARD_SHA256 = "872aed286523d3b5cb32b11db2c0d901a365a81d60ccb9437097f7d854d9d1be"

PRODUCT_COLUMNS = (
    "id",
    "ean",
    "cover_url",
    "cover_storage_path",
    "cover_source",
    "cover_source_url",
    "cover_status",
    "cover_confidence",
    "cover_priority",
    "cover_mbid",
    "cover_last_attempt_at",
    "cover_last_success_at",
    "cover_fail_count",
    "cover_needs_refresh",
    "cover_locked_at",
    "cover_locked_by",
    "cover_error_code",
    "cover_error_message",
    "cover_source_shop_id",
    "cover_width",
    "cover_height",
    "updated_at",
)

FULL_TABLE_EXPORTS = (
    ("public", "product_cover_candidates", ("id",)),
    ("public", "product_cover_queue", ("id",)),
    ("public", "release_calendar", ("id",)),
    ("public", "cover_preload_stage", ("id",)),
)

CATALOG_TABLES = (
    "products",
    "product_cover_candidates",
    "product_cover_queue",
    "release_calendar",
    "cover_preload_stage",
)

CATALOG_FUNCTIONS = (
    "normalize_cover_ean",
    "queue_cover_for_products",
    "queue_cover_for_eans",
    "apply_cover_preload_batch",
    "claim_next_cover_job",
)

CATALOG_VIEWS = (
    "cover_management_status_v1",
    "cover_candidates_missing_v1",
    "cover_candidates_failed_review_v1",
    "cover_priority_candidates_v1",
)


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_default(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Exporteer de exacte pre-migration coverdatastaat."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
        help="Postgres-URL. Standaard: DATABASE_URL uit de environment.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("/tmp") / f"vinylofy-cover-pre-migration-{utc_stamp()}",
        help="Nieuwe outputdirectory buiten de repository.",
    )
    return parser.parse_args()


def validate_output_path(output_dir: Path) -> None:
    resolved = output_dir.expanduser().resolve()
    worktree = WORKTREE.resolve()

    if resolved == worktree or worktree in resolved.parents:
        raise RuntimeError(
            "De database-export mag niet binnen de repository worden geschreven."
        )
    if resolved.exists():
        raise RuntimeError(f"Outputdirectory bestaat al: {resolved}")


def validate_forward_migration() -> Path:
    migration = WORKTREE / FORWARD_RELATIVE_PATH
    if not migration.is_file():
        raise RuntimeError(f"Forward migration ontbreekt: {migration}")

    actual = sha256_file(migration)
    if actual != FORWARD_SHA256:
        raise RuntimeError(
            "Forward-migration-SHA wijkt af: "
            f"verwacht {FORWARD_SHA256}, vond {actual}"
        )
    return migration


def fetch_rows(
    conn: psycopg.Connection[Any],
    query: str,
    params: Sequence[Any] = (),
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [description.name for description in cur.description or ()]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def table_columns(
    conn: psycopg.Connection[Any],
    schema_name: str,
    table_name: str,
) -> list[str]:
    rows = fetch_rows(
        conn,
        """
        select column_name
        from information_schema.columns
        where table_schema = %s
          and table_name = %s
        order by ordinal_position
        """,
        (schema_name, table_name),
    )
    return [str(row["column_name"]) for row in rows]


def validate_pre_migration_schema(conn: psycopg.Connection[Any]) -> dict[str, list[str]]:
    columns_by_table = {
        table: table_columns(conn, "public", table)
        for table in CATALOG_TABLES
    }

    missing_tables = [
        table for table, columns in columns_by_table.items() if not columns
    ]
    if missing_tables:
        raise RuntimeError(
            "Vereiste tabellen ontbreken: " + ", ".join(sorted(missing_tables))
        )

    product_columns = set(columns_by_table["products"])
    candidate_columns = set(columns_by_table["product_cover_candidates"])
    queue_columns = set(columns_by_table["product_cover_queue"])
    release_columns = set(columns_by_table["release_calendar"])

    missing_product_columns = sorted(set(PRODUCT_COLUMNS) - product_columns)
    if missing_product_columns:
        raise RuntimeError(
            "Vereiste pre-migration productkolommen ontbreken: "
            + ", ".join(missing_product_columns)
        )

    forbidden_post_migration = {
        "products.cover_sha256": "cover_sha256" in product_columns,
        "release_calendar.image_source_url": "image_source_url" in release_columns,
    }
    present_forbidden = [
        name for name, present in forbidden_post_migration.items() if present
    ]
    if present_forbidden:
        raise RuntimeError(
            "Database lijkt al gemigreerd; post-migration kolommen aangetroffen: "
            + ", ".join(present_forbidden)
        )

    required_legacy_queue = {
        "state",
        "requested_priority",
        "available_at",
        "attempts",
        "locked_at",
        "locked_by",
        "last_requested_at",
    }
    required_canonical_queue = {
        "status",
        "priority",
        "candidate_count",
        "attempt_count",
        "claimed_at",
        "next_attempt_at",
    }

    missing_legacy = sorted(required_legacy_queue - queue_columns)
    missing_canonical = sorted(required_canonical_queue - queue_columns)
    if missing_legacy or missing_canonical:
        details = []
        if missing_legacy:
            details.append("legacy ontbreekt: " + ", ".join(missing_legacy))
        if missing_canonical:
            details.append("canoniek ontbreekt: " + ", ".join(missing_canonical))
        raise RuntimeError(
            "Queue heeft niet de verwachte dubbele pre-migration vorm; "
            + "; ".join(details)
        )

    return columns_by_table


def export_query_csv(
    conn: psycopg.Connection[Any],
    destination: Path,
    query: sql.Composed,
) -> int:
    destination.parent.mkdir(parents=True, exist_ok=True)

    count_query = sql.SQL("select count(*) from ({}) as exported_rows").format(query)
    with conn.cursor() as cur:
        cur.execute(count_query)
        row_count = int(cur.fetchone()[0])

    copy_query = sql.SQL(
        "copy ({}) to stdout with (format csv, header true, encoding 'UTF8')"
    ).format(query)

    with destination.open("wb") as handle:
        with conn.cursor() as cur:
            with cur.copy(copy_query) as copy:
                for data in copy:
                    handle.write(data)

    return row_count


def export_products(conn: psycopg.Connection[Any], destination: Path) -> int:
    query = sql.SQL("select {} from public.products order by id").format(
        sql.SQL(", ").join(sql.Identifier(column) for column in PRODUCT_COLUMNS)
    )
    return export_query_csv(conn, destination, query)


def export_full_table(
    conn: psycopg.Connection[Any],
    schema_name: str,
    table_name: str,
    order_columns: Iterable[str],
    destination: Path,
) -> int:
    order_sql = sql.SQL(", ").join(
        sql.Identifier(column) for column in order_columns
    )
    query = sql.SQL("select * from {}.{} order by {}").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name),
        order_sql,
    )
    return export_query_csv(conn, destination, query)


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


def export_catalog(
    conn: psycopg.Connection[Any],
    root: Path,
    columns_by_table: dict[str, list[str]],
) -> None:
    write_json(root / "catalog" / "columns.json", columns_by_table)

    constraints = fetch_rows(
        conn,
        """
        select
          n.nspname as schema_name,
          c.relname as table_name,
          con.conname as constraint_name,
          con.contype as constraint_type,
          pg_get_constraintdef(con.oid, true) as definition
        from pg_constraint con
        join pg_class c on c.oid = con.conrelid
        join pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'public'
          and c.relname = any(%s)
        order by c.relname, con.conname
        """,
        (list(CATALOG_TABLES),),
    )
    write_json(root / "catalog" / "constraints.json", constraints)

    indexes = fetch_rows(
        conn,
        """
        select
          schemaname as schema_name,
          tablename as table_name,
          indexname as index_name,
          indexdef as definition
        from pg_indexes
        where schemaname = 'public'
          and tablename = any(%s)
        order by tablename, indexname
        """,
        (list(CATALOG_TABLES),),
    )
    write_json(root / "catalog" / "indexes.json", indexes)

    functions = fetch_rows(
        conn,
        """
        select
          n.nspname as schema_name,
          p.proname as function_name,
          pg_get_function_identity_arguments(p.oid) as identity_arguments,
          pg_get_functiondef(p.oid) as definition
        from pg_proc p
        join pg_namespace n on n.oid = p.pronamespace
        where n.nspname = 'public'
          and p.proname = any(%s)
        order by p.proname, pg_get_function_identity_arguments(p.oid)
        """,
        (list(CATALOG_FUNCTIONS),),
    )
    write_json(root / "catalog" / "functions.json", functions)

    views = fetch_rows(
        conn,
        """
        select
          schemaname as schema_name,
          viewname as view_name,
          definition
        from pg_views
        where schemaname = 'public'
          and viewname = any(%s)
        order by viewname
        """,
        (list(CATALOG_VIEWS),),
    )
    write_json(root / "catalog" / "views.json", views)


def write_checksums(root: Path) -> dict[str, str]:
    checksums: dict[str, str] = {}
    files = sorted(
        path for path in root.rglob("*")
        if path.is_file() and path.name != "checksums.sha256"
    )

    for path in files:
        relative = path.relative_to(root).as_posix()
        checksums[relative] = sha256_file(path)

    checksum_path = root / "checksums.sha256"
    checksum_path.write_text(
        "".join(
            f"{digest}  {relative}\n"
            for relative, digest in sorted(checksums.items())
        ),
        encoding="utf-8",
    )
    return checksums


def main() -> int:
    args = parse_args()
    database_url = args.database_url.strip()
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL ontbreekt. Zet die in de environment of geef "
            "--database-url mee."
        )

    validate_forward_migration()
    output_dir = args.output_dir.expanduser().resolve()
    validate_output_path(output_dir)

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    temp_root = Path(
        tempfile.mkdtemp(
            prefix=output_dir.name + ".",
            suffix=".tmp",
            dir=output_dir.parent,
        )
    )

    try:
        with psycopg.connect(
            database_url,
            options="-c default_transaction_read_only=on",
        ) as conn:
            conn.execute(
                "set transaction isolation level repeatable read, read only, deferrable"
            )

            database_info = fetch_rows(
                conn,
                """
                select
                  current_database() as database_name,
                  current_schema() as current_schema,
                  current_user as database_user,
                  current_setting('transaction_read_only') as transaction_read_only,
                  current_setting('server_version') as server_version,
                  now() as snapshot_time
                """,
            )[0]

            if database_info["transaction_read_only"] != "on":
                raise RuntimeError("Databaseverbinding is niet read-only.")

            columns_by_table = validate_pre_migration_schema(conn)

            row_counts: dict[str, int] = {}
            row_counts["products_cover_state"] = export_products(
                conn,
                temp_root / "tables" / "products_cover_state.csv",
            )

            for schema_name, table_name, order_columns in FULL_TABLE_EXPORTS:
                row_counts[table_name] = export_full_table(
                    conn,
                    schema_name,
                    table_name,
                    order_columns,
                    temp_root / "tables" / f"{table_name}.csv",
                )

            export_catalog(conn, temp_root, columns_by_table)
            conn.rollback()

        readme = (
            "Vinylofy centrale coverpipeline — pre-migration database-export\n"
            "\n"
            "Deze snapshot is read-only gemaakt vóór toepassing van:\n"
            f"{FORWARD_RELATIVE_PATH.as_posix()}\n"
            f"SHA256: {FORWARD_SHA256}\n"
            "\n"
            "Bevat geen Storage-objecten. Gebruik daarvoor het afzonderlijke\n"
            "Storage-manifest. Commit deze runtime-export niet naar Git.\n"
        )
        (temp_root / "README.txt").write_text(readme, encoding="utf-8")

        manifest = {
            "artifact_type": "vinylofy_cover_pre_migration_database_export",
            "format_version": 1,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "forward_migration": FORWARD_RELATIVE_PATH.as_posix(),
            "forward_migration_sha256": FORWARD_SHA256,
            "database": database_info,
            "row_counts": row_counts,
            "catalog_tables": list(CATALOG_TABLES),
            "catalog_functions": list(CATALOG_FUNCTIONS),
            "catalog_views": list(CATALOG_VIEWS),
            "storage_objects_included": False,
            "database_writes_performed": False,
        }
        write_json(temp_root / "manifest.json", manifest)
        checksums = write_checksums(temp_root)

        os.replace(temp_root, output_dir)

        print("== PRE-MIGRATION EXPORT ==")
        print(f"output_dir={output_dir}")
        print(f"forward_sha256={FORWARD_SHA256}")
        print("transaction_read_only=on")
        print("database_writes=0")
        print(f"exported_files={len(checksums) + 1}")
        for name, count in sorted(row_counts.items()):
            print(f"rows_{name}={count}")
        print("secrets_afgedrukt=nee")
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
