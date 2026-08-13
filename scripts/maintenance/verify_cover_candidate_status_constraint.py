#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

import psycopg


EXPECTED_STATUSES = {
    "failed",
    "new",
    "pending",
    "published",
    "rejected",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Verify the live product_cover_candidates "
            "status constraint."
        )
    )
    parser.add_argument(
        "--output-json",
        default="",
    )
    return parser.parse_args()


def normalize_sql(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip().lower()


def main() -> None:
    args = parse_args()

    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("DATABASE_URL ontbreekt.")

    checks: list[dict[str, object]] = []

    with psycopg.connect(database_url) as conn:
        conn.execute("set transaction read only")

        with conn.cursor() as cur:
            cur.execute(
                """
                select pg_get_constraintdef(c.oid, true)
                from pg_constraint c
                join pg_class t
                  on t.oid = c.conrelid
                join pg_namespace n
                  on n.oid = t.relnamespace
                where n.nspname = 'public'
                  and t.relname = 'product_cover_candidates'
                  and c.conname =
                      'product_cover_candidates_status_chk'
                """
            )
            rows = cur.fetchall()

            if len(rows) != 1:
                raise SystemExit(
                    "Expected exactly one "
                    "product_cover_candidates_status_chk; "
                    f"found {len(rows)}."
                )

            definition = str(rows[0][0])
            normalized = normalize_sql(definition)

            for status in sorted(EXPECTED_STATUSES):
                ok = f"'{status}'::text" in normalized
                checks.append(
                    {
                        "check": f"status_allowed:{status}",
                        "ok": ok,
                    }
                )
                if not ok:
                    raise SystemExit(
                        f"Live constraint mist status {status!r}."
                    )

            null_allowed = "candidate_status is null" in normalized
            checks.append(
                {
                    "check": "candidate_status_null_allowed",
                    "ok": null_allowed,
                }
            )
            if not null_allowed:
                raise SystemExit(
                    "Live constraint staat NULL niet toe."
                )

            cur.execute(
                """
                select
                    candidate_status,
                    count(*)
                from public.product_cover_candidates
                group by candidate_status
                order by candidate_status
                """
            )
            distribution = {
                status: int(count)
                for status, count in cur.fetchall()
            }

            invalid = {
                status: count
                for status, count in distribution.items()
                if status is not None
                and status not in EXPECTED_STATUSES
            }

            if invalid:
                raise SystemExit(
                    "Ongeldige live candidate-statussen: "
                    + repr(invalid)
                )

            cur.execute(
                """
                select count(*)
                from public.product_cover_candidates
                where is_selected is true
                  and candidate_status
                      is distinct from 'published'
                """
            )
            selected_mismatch = int(cur.fetchone()[0])

            if selected_mismatch != 0:
                raise SystemExit(
                    "Selected candidates zonder published status: "
                    f"{selected_mismatch}"
                )

    result = {
        "constraint_definition": definition,
        "status_distribution": distribution,
        "selected_status_mismatches": selected_mismatch,
        "checks": checks,
        "hard_failures": 0,
    }

    if args.output_json:
        output = Path(args.output_json)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(
                result,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    print(f"constraint_definition={definition}")

    for status, count in distribution.items():
        print(f"status={status} count={count}")

    print(
        "selected_status_mismatches="
        f"{selected_mismatch}"
    )
    print(f"checks={len(checks)}")
    print("hard_failures=0")
    print("candidate_status_constraint=GREEN")


if __name__ == "__main__":
    main()
