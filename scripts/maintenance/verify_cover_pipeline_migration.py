#!/usr/bin/env python3
"""Verifieer de definitieve centrale Vinylofy-covermigration read-only.

Dit script controleert na toepassing van de migration:
- schema, kolommen, constraints en indexes;
- RPC's, SKIP LOCKED en stale-claimherstel;
- operationele views;
- product-, kandidaat-, queue- en release-invarianten;
- de configuratie van de product-covers Storage-bucket;
- optioneel de registratie in supabase_migrations.

Er worden geen database- of Storage-mutaties uitgevoerd.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
import traceback
from typing import Any, Callable, Sequence

try:
    import psycopg
except ImportError as exc:  # pragma: no cover - runtime dependency guard
    raise SystemExit(
        "psycopg ontbreekt. Installeer de repositorydependencies."
    ) from exc


WORKTREE = Path("/workspaces/Vinylofy-cover-localization-20260804-101907")
FORWARD_RELATIVE_PATH = Path(
    "supabase/migrations/20260805123000_finalize_central_cover_pipeline.sql"
)
FORWARD_SHA256 = "872aed286523d3b5cb32b11db2c0d901a365a81d60ccb9437097f7d854d9d1be"
MIGRATION_VERSION = "20260805123000"
EXPECTED_BUCKET = "product-covers"
EXPECTED_BUCKET_LIMIT = 5 * 1024 * 1024
EXPECTED_MIME_TYPES = {
    "image/jpeg",
    "image/png",
    "image/webp",
}

EXPECTED_COLUMNS = {
    "products": {
        "cover_sha256",
        "cover_mime_type",
        "cover_byte_size",
        "cover_storage_path",
        "cover_source_url",
        "cover_status",
        "cover_confidence",
    },
    "product_cover_candidates": {
        "product_id",
        "candidate_status",
        "is_selected",
        "byte_size",
    },
    "product_cover_queue": {
        "id",
        "product_id",
        "priority",
        "candidate_count",
        "source_reason",
        "status",
        "attempt_count",
        "claimed_by",
        "claimed_at",
        "next_attempt_at",
        "last_completed_at",
        "last_error_code",
        "last_error_message",
        "created_at",
        "updated_at",
    },
    "release_calendar": {
        "id",
        "ean",
        "product_id",
        "image_url",
        "image_source_url",
        "image_storage_path",
        "image_status",
        "image_sha256",
        "image_mime_type",
        "image_byte_size",
        "image_last_attempt_at",
        "image_error_code",
        "image_error_message",
    },
}

FORBIDDEN_LEGACY_QUEUE_COLUMNS = {
    "ean",
    "trigger_source",
    "requested_priority",
    "requested_by",
    "request_count",
    "state",
    "available_at",
    "attempts",
    "locked_at",
    "locked_by",
    "last_requested_at",
}

EXPECTED_CONSTRAINTS = {
    "products": {
        "products_cover_status_chk",
        "products_cover_sha256_chk",
        "products_cover_mime_type_chk",
        "products_cover_byte_size_chk",
        "products_cover_ready_storage_chk",
        "products_cover_confidence_chk",
    },
    "product_cover_candidates": {
        "product_cover_candidates_byte_size_chk",
        "product_cover_candidates_selected_status_chk",
    },
    "product_cover_queue": {
        "product_cover_queue_status_chk",
        "product_cover_queue_priority_chk",
        "product_cover_queue_candidate_count_chk",
        "product_cover_queue_attempt_count_chk",
    },
    "release_calendar": {
        "release_calendar_product_id_fkey",
        "release_calendar_image_status_chk",
        "release_calendar_image_sha256_chk",
        "release_calendar_image_mime_type_chk",
        "release_calendar_image_byte_size_chk",
        "release_calendar_ready_local_image_chk",
    },
}

EXPECTED_INDEXES = {
    "products_cover_storage_path_idx",
    "products_cover_localization_idx",
    "product_cover_candidates_one_selected_idx",
    "product_cover_queue_claim_v2_idx",
    "product_cover_queue_stale_claim_idx",
    "release_calendar_product_id_idx",
    "release_calendar_image_localization_idx",
}

EXPECTED_FUNCTION_TOKENS = {
    "queue_cover_for_products": (
        "product_cover_queue",
        "cover_storage_path",
        "cover_status",
        "blocked",
    ),
    "queue_cover_for_eans": (
        "normalize_cover_ean",
        "queue_cover_for_products",
    ),
    "apply_cover_preload_batch": (
        "cover_preload_stage",
        "queue_cover_for_products",
    ),
    "recover_stale_cover_claims": (
        "retry_later",
        "stale_claim_recovered",
    ),
    "claim_next_cover_job": (
        "for update of q skip locked",
        "recover_stale_cover_claims",
        "processing",
    ),
}

EXPECTED_VIEWS = {
    "cover_management_status_v1",
    "cover_candidates_missing_v1",
    "cover_candidates_failed_review_v1",
    "cover_priority_candidates_v1",
}


class VerificationError(RuntimeError):
    """Fout in de verificatie zelf, niet een mislukte databasecheck."""


class ResultCollector:
    def __init__(self) -> None:
        self.checks: list[dict[str, Any]] = []

    def add(
        self,
        name: str,
        passed: bool,
        *,
        actual: Any = None,
        expected: Any = None,
        severity: str = "error",
        detail: str = "",
    ) -> None:
        if severity not in {"error", "warning", "info"}:
            raise VerificationError(f"Onbekende severity: {severity}")

        self.checks.append(
            {
                "name": name,
                "passed": bool(passed),
                "severity": severity,
                "actual": actual,
                "expected": expected,
                "detail": detail,
            }
        )

    @property
    def hard_failures(self) -> list[dict[str, Any]]:
        return [
            item
            for item in self.checks
            if item["severity"] == "error" and not item["passed"]
        ]

    @property
    def warnings(self) -> list[dict[str, Any]]:
        return [
            item
            for item in self.checks
            if item["severity"] == "warning" and not item["passed"]
        ]


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verifieer de centrale covermigration read-only."
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", ""),
        help="Postgres-URL. Standaard: DATABASE_URL.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("/tmp") / f"vinylofy-cover-verify-{utc_stamp()}.json",
        help="Pad voor het JSON-verificatierapport.",
    )
    parser.add_argument(
        "--require-migration-history",
        action="store_true",
        help=(
            "Maak ontbrekende registratie in supabase_migrations tot een "
            "harde fout in plaats van een waarschuwing."
        ),
    )
    return parser.parse_args()


def validate_forward_migration() -> Path:
    migration = WORKTREE / FORWARD_RELATIVE_PATH
    if not migration.is_file():
        raise VerificationError(f"Forward migration ontbreekt: {migration}")

    actual = sha256_file(migration)
    if actual != FORWARD_SHA256:
        raise VerificationError(
            "Forward-migration-SHA wijkt af: "
            f"verwacht {FORWARD_SHA256}, vond {actual}"
        )
    return migration


def fetch_all(
    conn: psycopg.Connection[Any],
    query: str,
    params: Sequence[Any] = (),
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(query, params)
        columns = [description.name for description in cur.description or ()]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_scalar(
    conn: psycopg.Connection[Any],
    query: str,
    params: Sequence[Any] = (),
) -> Any:
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        if row is None:
            raise VerificationError("Scalarquery gaf geen rij terug.")
        return row[0]


def table_columns(
    conn: psycopg.Connection[Any],
    table_name: str,
) -> dict[str, str]:
    rows = fetch_all(
        conn,
        """
        select column_name, data_type
        from information_schema.columns
        where table_schema = 'public'
          and table_name = %s
        order by ordinal_position
        """,
        (table_name,),
    )
    return {
        str(row["column_name"]): str(row["data_type"])
        for row in rows
    }


def check_columns(
    conn: psycopg.Connection[Any],
    results: ResultCollector,
) -> None:
    for table_name, expected_columns in EXPECTED_COLUMNS.items():
        columns = table_columns(conn, table_name)
        actual_names = set(columns)

        results.add(
            f"table_exists:{table_name}",
            bool(columns),
            actual=sorted(actual_names),
            expected="table with columns",
        )
        results.add(
            f"required_columns:{table_name}",
            expected_columns <= actual_names,
            actual=sorted(actual_names),
            expected=sorted(expected_columns),
            detail=(
                "Ontbrekend: "
                + ", ".join(sorted(expected_columns - actual_names))
                if expected_columns - actual_names
                else ""
            ),
        )

    queue_columns = set(table_columns(conn, "product_cover_queue"))
    present_legacy = sorted(FORBIDDEN_LEGACY_QUEUE_COLUMNS & queue_columns)
    results.add(
        "legacy_queue_columns_absent",
        not present_legacy,
        actual=present_legacy,
        expected=[],
    )

    product_types = table_columns(conn, "products")
    results.add(
        "products_cover_confidence_integer",
        product_types.get("cover_confidence") == "integer",
        actual=product_types.get("cover_confidence"),
        expected="integer",
    )

    shop_id_nullable = fetch_scalar(
        conn,
        """
        select is_nullable
        from information_schema.columns
        where table_schema = 'public'
          and table_name = 'product_cover_candidates'
          and column_name = 'shop_id'
        """,
    )
    results.add(
        "column:product_cover_candidates.shop_id_nullable",
        str(shop_id_nullable).upper() == "YES",
        actual=shop_id_nullable,
        expected="YES",
        detail=(
            "Externe covermetadata mag zonder shop_id worden opgeslagen; "
            "shopgebonden candidates behouden de bestaande foreign key."
        ),
    )



def check_constraints(
    conn: psycopg.Connection[Any],
    results: ResultCollector,
) -> None:
    rows = fetch_all(
        conn,
        """
        select
          c.relname as table_name,
          con.conname as constraint_name,
          pg_get_constraintdef(con.oid, true) as definition
        from pg_constraint con
        join pg_class c
          on c.oid = con.conrelid
        join pg_namespace n
          on n.oid = c.relnamespace
        where n.nspname = 'public'
          and c.relname = any(%s)
        order by c.relname, con.conname
        """,
        (list(EXPECTED_CONSTRAINTS),),
    )

    actual_by_table: dict[str, set[str]] = {
        table_name: set()
        for table_name in EXPECTED_CONSTRAINTS
    }
    for row in rows:
        table_name = str(row["table_name"])
        actual_by_table.setdefault(table_name, set()).add(
            str(row["constraint_name"])
        )

    for table_name, expected in EXPECTED_CONSTRAINTS.items():
        actual = actual_by_table.get(table_name, set())
        results.add(
            f"constraints:{table_name}",
            expected <= actual,
            actual=sorted(actual),
            expected=sorted(expected),
            detail=(
                "Ontbrekend: " + ", ".join(sorted(expected - actual))
                if expected - actual
                else ""
            ),
        )


def check_indexes(
    conn: psycopg.Connection[Any],
    results: ResultCollector,
) -> None:
    rows = fetch_all(
        conn,
        """
        select indexname, indexdef
        from pg_indexes
        where schemaname = 'public'
        order by indexname
        """,
    )
    actual = {
        str(row["indexname"]): str(row["indexdef"])
        for row in rows
    }

    results.add(
        "expected_indexes",
        EXPECTED_INDEXES <= set(actual),
        actual=sorted(set(actual) & EXPECTED_INDEXES),
        expected=sorted(EXPECTED_INDEXES),
        detail=(
            "Ontbrekend: "
            + ", ".join(sorted(EXPECTED_INDEXES - set(actual)))
            if EXPECTED_INDEXES - set(actual)
            else ""
        ),
    )

    selected_definition = actual.get(
        "product_cover_candidates_one_selected_idx",
        "",
    ).lower()
    results.add(
        "one_selected_candidate_partial_unique_index",
        (
            "unique index" in selected_definition
            and "is_selected" in selected_definition
            and "product_id" in selected_definition
        ),
        actual=selected_definition,
        expected="unique partial index on product_id where is_selected",
    )


def check_functions(
    conn: psycopg.Connection[Any],
    results: ResultCollector,
) -> None:
    rows = fetch_all(
        conn,
        """
        select
          p.proname as function_name,
          pg_get_function_identity_arguments(p.oid) as identity_arguments,
          pg_get_functiondef(p.oid) as definition
        from pg_proc p
        join pg_namespace n
          on n.oid = p.pronamespace
        where n.nspname = 'public'
          and p.proname = any(%s)
        order by p.proname, pg_get_function_identity_arguments(p.oid)
        """,
        (list(EXPECTED_FUNCTION_TOKENS),),
    )

    by_name: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_name.setdefault(str(row["function_name"]), []).append(row)

    for function_name, tokens in EXPECTED_FUNCTION_TOKENS.items():
        definitions = [
            str(item["definition"]).lower()
            for item in by_name.get(function_name, [])
        ]
        combined = "\n".join(definitions)
        missing = [
            token for token in tokens
            if token.lower() not in combined
        ]

        results.add(
            f"function:{function_name}",
            len(definitions) >= 1 and not missing,
            actual={
                "overloads": len(definitions),
                "identity_arguments": [
                    item["identity_arguments"]
                    for item in by_name.get(function_name, [])
                ],
                "missing_tokens": missing,
            },
            expected={"minimum_overloads": 1, "tokens": list(tokens)},
        )

    stale_defaults = fetch_all(
        conn,
        """
        select
          pg_get_function_arguments(p.oid) as full_arguments,
          pg_get_expr(p.proargdefaults, 0) as default_expression,
          (
            extract(
              epoch from (
                (
                  regexp_match(
                    pg_get_expr(p.proargdefaults, 0),
                    $re$^'([^']+)'::interval$re$
                  )
                )[1]::interval
              )
            )
          )::bigint as default_seconds
        from pg_proc p
        join pg_namespace n
          on n.oid = p.pronamespace
        where n.nspname = 'public'
          and p.proname = 'recover_stale_cover_claims'
        """,
    )

    stale_default = stale_defaults[0] if len(stale_defaults) == 1 else {}
    stale_default_seconds = stale_default.get("default_seconds")

    results.add(
        "function:recover_stale_cover_claims_default",
        len(stale_defaults) == 1 and stale_default_seconds == 5400,
        actual={
            "overloads": len(stale_defaults),
            "full_arguments": stale_default.get("full_arguments"),
            "default_expression": stale_default.get("default_expression"),
            "default_seconds": stale_default_seconds,
        },
        expected={
            "overloads": 1,
            "default_seconds": 5400,
            "equivalent_interval": "90 minutes",
        },
        detail=(
            "Controleert de semantische intervalwaarde in plaats van de "
            "tekstuele PostgreSQL-weergave."
        ),
    )


def check_views(
    conn: psycopg.Connection[Any],
    results: ResultCollector,
) -> None:
    rows = fetch_all(
        conn,
        """
        select viewname, definition
        from pg_views
        where schemaname = 'public'
          and viewname = any(%s)
        order by viewname
        """,
        (list(EXPECTED_VIEWS),),
    )
    actual = {str(row["viewname"]) for row in rows}

    results.add(
        "expected_views",
        EXPECTED_VIEWS <= actual,
        actual=sorted(actual),
        expected=sorted(EXPECTED_VIEWS),
        detail=(
            "Ontbrekend: " + ", ".join(sorted(EXPECTED_VIEWS - actual))
            if EXPECTED_VIEWS - actual
            else ""
        ),
    )


def check_data_invariants(
    conn: psycopg.Connection[Any],
    results: ResultCollector,
) -> dict[str, int]:
    queries: dict[str, tuple[str, int, str]] = {
        "products_external_cover_url": (
            """
            select count(*)
            from public.products
            where nullif(btrim(cover_url), '') ~* '^https?://'
            """,
            0,
            "error",
        ),
        "products_ready_without_storage_path": (
            """
            select count(*)
            from public.products
            where cover_status = 'ready'
              and nullif(btrim(cover_storage_path), '') is null
            """,
            0,
            "error",
        ),
        "products_invalid_sha256": (
            """
            select count(*)
            from public.products
            where cover_sha256 is not null
              and cover_sha256 !~ '^[0-9a-f]{64}$'
            """,
            0,
            "error",
        ),
        "selected_candidate_duplicates": (
            """
            select count(*)
            from (
              select product_id
              from public.product_cover_candidates
              where product_id is not null
                and is_selected is true
              group by product_id
              having count(*) > 1
            ) duplicate_products
            """,
            0,
            "error",
        ),
        "selected_candidate_without_published_product": (
            """
            select count(*)
            from public.product_cover_candidates
            where is_selected is true
              and (
                product_id is null
                or candidate_status <> 'published'
              )
            """,
            0,
            "error",
        ),
        "queue_invalid_status": (
            """
            select count(*)
            from public.product_cover_queue
            where status not in (
              'pending',
              'processing',
              'published',
              'failed',
              'review',
              'retry_later'
            )
            """,
            0,
            "error",
        ),
        "release_external_image_url": (
            """
            select count(*)
            from public.release_calendar
            where nullif(btrim(image_url), '') ~* '^https?://'
            """,
            0,
            "error",
        ),
        "release_ready_without_local_fields": (
            """
            select count(*)
            from public.release_calendar
            where image_status = 'ready'
              and (
                nullif(btrim(image_url), '') is null
                or nullif(btrim(image_storage_path), '') is null
              )
            """,
            0,
            "error",
        ),
        "release_product_ean_mismatch": (
            """
            select count(*)
            from public.release_calendar r
            join public.products p
              on p.id = r.product_id
            where public.normalize_cover_ean(r.ean) is distinct from
                  public.normalize_cover_ean(p.ean)
            """,
            0,
            "error",
        ),
        "processing_claim_without_claimed_at": (
            """
            select count(*)
            from public.product_cover_queue
            where status = 'processing'
              and claimed_at is null
            """,
            0,
            "error",
        ),
        "stale_processing_claims_over_90_minutes": (
            """
            select count(*)
            from public.product_cover_queue
            where status = 'processing'
              and claimed_at < now() - interval '90 minutes'
            """,
            0,
            "warning",
        ),
        "blocked_products_with_claimable_queue_status": (
            """
            select count(*)
            from public.product_cover_queue q
            join public.products p
              on p.id = q.product_id
            where p.cover_status = 'blocked'
              and q.status in ('pending', 'retry_later', 'review')
            """,
            0,
            "warning",
        ),
    }

    counts: dict[str, int] = {}
    for name, (query, expected, severity) in queries.items():
        actual = int(fetch_scalar(conn, query))
        counts[name] = actual
        results.add(
            f"data:{name}",
            actual == expected,
            actual=actual,
            expected=expected,
            severity=severity,
        )

    summary_queries = {
        "products_total": "select count(*) from public.products",
        "products_with_local_path": """
            select count(*)
            from public.products
            where nullif(btrim(cover_storage_path), '') is not null
        """,
        "selected_candidates_total": """
            select count(*)
            from public.product_cover_candidates
            where is_selected is true
        """,
        "queue_total": "select count(*) from public.product_cover_queue",
        "release_rows_total": "select count(*) from public.release_calendar",
        "release_rows_linked_to_product": """
            select count(*)
            from public.release_calendar
            where product_id is not null
        """,
    }
    for name, query in summary_queries.items():
        counts[name] = int(fetch_scalar(conn, query))

    return counts


def check_storage_bucket(
    conn: psycopg.Connection[Any],
    results: ResultCollector,
) -> dict[str, Any] | None:
    storage_schema_exists = bool(
        fetch_scalar(
            conn,
            """
            select exists (
              select 1
              from information_schema.schemata
              where schema_name = 'storage'
            )
            """,
        )
    )
    results.add(
        "storage_schema_exists",
        storage_schema_exists,
        actual=storage_schema_exists,
        expected=True,
    )
    if not storage_schema_exists:
        return None

    rows = fetch_all(
        conn,
        """
        select
          id,
          name,
          public,
          file_size_limit,
          allowed_mime_types
        from storage.buckets
        where id = %s
        """,
        (EXPECTED_BUCKET,),
    )
    if len(rows) != 1:
        results.add(
            "storage_bucket_exists",
            False,
            actual=len(rows),
            expected=1,
        )
        return None

    bucket = rows[0]
    results.add(
        "storage_bucket_exists",
        True,
        actual=1,
        expected=1,
    )
    results.add(
        "storage_bucket_public",
        bucket.get("public") is True,
        actual=bucket.get("public"),
        expected=True,
    )
    results.add(
        "storage_bucket_size_limit",
        int(bucket.get("file_size_limit") or 0) == EXPECTED_BUCKET_LIMIT,
        actual=bucket.get("file_size_limit"),
        expected=EXPECTED_BUCKET_LIMIT,
    )

    actual_mime_types = set(bucket.get("allowed_mime_types") or [])
    results.add(
        "storage_bucket_mime_types",
        actual_mime_types == EXPECTED_MIME_TYPES,
        actual=sorted(actual_mime_types),
        expected=sorted(EXPECTED_MIME_TYPES),
    )
    return bucket


def check_migration_history(
    conn: psycopg.Connection[Any],
    results: ResultCollector,
    *,
    required: bool,
) -> None:
    table_exists = bool(
        fetch_scalar(
            conn,
            """
            select to_regclass(
              'supabase_migrations.schema_migrations'
            ) is not null
            """,
        )
    )
    if not table_exists:
        results.add(
            "migration_history_table_exists",
            False,
            actual=False,
            expected=True,
            severity="error" if required else "warning",
        )
        return

    present = bool(
        fetch_scalar(
            conn,
            """
            select exists (
              select 1
              from supabase_migrations.schema_migrations
              where version = %s
            )
            """,
            (MIGRATION_VERSION,),
        )
    )
    results.add(
        "migration_registered",
        present,
        actual=present,
        expected=True,
        severity="error" if required else "warning",
    )


def print_report(
    results: ResultCollector,
    counts: dict[str, int],
    output_json: Path,
) -> None:
    for item in results.checks:
        if item["passed"]:
            marker = "OK"
        elif item["severity"] == "warning":
            marker = "WARN"
        else:
            marker = "FAIL"

        print(
            f"[{marker}] {item['name']} "
            f"actual={json.dumps(item['actual'], default=str, ensure_ascii=False)}"
        )

    print()
    print("== DATA-SAMENVATTING ==")
    for name, value in sorted(counts.items()):
        print(f"{name}={value}")

    print()
    print("== EINDSTATUS ==")
    print(f"checks_total={len(results.checks)}")
    print(f"hard_failures={len(results.hard_failures)}")
    print(f"warnings={len(results.warnings)}")
    print("database_writes=0")
    print("storage_writes=0")
    print("storage_deletes=0")
    print(f"report={output_json}")


def main() -> int:
    args = parse_args()
    database_url = str(args.database_url).strip()
    if not database_url:
        raise VerificationError(
            "DATABASE_URL ontbreekt. Zet deze in de environment of geef "
            "--database-url mee."
        )

    validate_forward_migration()

    output_json = args.output_json.expanduser().resolve()
    output_json.parent.mkdir(parents=True, exist_ok=True)
    if output_json.exists():
        raise VerificationError(
            f"JSON-rapport bestaat al: {output_json}"
        )

    results = ResultCollector()
    counts: dict[str, int] = {}

    with psycopg.connect(
        database_url,
        options="-c default_transaction_read_only=on",
    ) as conn:
        conn.execute(
            "set transaction isolation level repeatable read, read only, deferrable"
        )

        database_info = fetch_all(
            conn,
            """
            select
              current_database() as database_name,
              current_user as database_user,
              current_setting('transaction_read_only') as transaction_read_only,
              current_setting('server_version') as server_version,
              now() as snapshot_time
            """,
        )[0]
        results.add(
            "transaction_read_only",
            database_info["transaction_read_only"] == "on",
            actual=database_info["transaction_read_only"],
            expected="on",
        )

        check_columns(conn, results)
        check_constraints(conn, results)
        check_indexes(conn, results)
        check_functions(conn, results)
        check_views(conn, results)
        counts = check_data_invariants(conn, results)
        bucket = check_storage_bucket(conn, results)
        check_migration_history(
            conn,
            results,
            required=bool(args.require_migration_history),
        )
        conn.rollback()

    report = {
        "artifact_type": "vinylofy_cover_post_migration_verification",
        "format_version": 1,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "forward_migration": FORWARD_RELATIVE_PATH.as_posix(),
        "forward_migration_sha256": FORWARD_SHA256,
        "migration_version": MIGRATION_VERSION,
        "database": database_info,
        "storage_bucket": bucket,
        "data_counts": counts,
        "checks": results.checks,
        "hard_failure_count": len(results.hard_failures),
        "warning_count": len(results.warnings),
        "database_writes_performed": False,
        "storage_writes_performed": False,
        "storage_deletes_performed": False,
    }
    output_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )

    print_report(results, counts, output_json)
    return 1 if results.hard_failures else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        print(f"[ERROR] {type(exc).__name__}: {exc}", file=sys.stderr)
        traceback.print_exc()
        raise SystemExit(2)
