#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg


STOCK_MODULE = "scripts.scrapers.usf.stock_shop3345"

LOCK_NAME = "vinylofy:3345:stock-batch-v1"

NO_MATCH_MARKER = (
    "geen enkele gescrapete target url "
    "koppelde aan public.prices"
)

HAS_NEXT_FALSE_PATTERN = re.compile(
    r"has_next\s*=\s*false",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class PageResult:
    page: int
    status: str
    has_next: bool
    returncode: int


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description=(
            "Draai de bestaande 3345-voorraadscraper "
            "één pagina per proces met een databasecursor."
        )
    )

    result.add_argument(
        "--pages",
        type=int,
        default=50,
        help="Maximumaantal pagina's in deze batch.",
    )
    result.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="Startpagina voor een dry-run.",
    )
    result.add_argument(
        "--page-sleep",
        type=float,
        default=10.0,
        help="Pauze tussen twee pagina's.",
    )
    result.add_argument(
        "--wait-ms",
        type=int,
        default=5000,
        help="Wachttijd van de bestaande browserscraper.",
    )
    result.add_argument(
        "--retries",
        type=int,
        default=6,
    )
    result.add_argument(
        "--reset-cycle",
        action="store_true",
    )
    result.add_argument(
        "--write",
        action="store_true",
    )
    result.add_argument(
        "--output-dir",
        default="output/usf-shop3345-stock",
    )

    return result


def get_database_url() -> str:
    value = os.environ.get("DATABASE_URL", "").strip()

    if not value:
        raise RuntimeError(
            "DATABASE_URL ontbreekt voor een write-run."
        )

    return value


def prepare_state(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists
                public.shop3345_stock_batch_state (
                    id smallint primary key
                        check (id = 1),

                    next_page integer not null
                        default 1
                        check (next_page >= 1),

                    cycle_number integer not null
                        default 1
                        check (cycle_number >= 1),

                    last_successful_page integer,

                    updated_at timestamptz not null
                        default now()
                )
            """
        )

        cur.execute(
            """
            insert into public.shop3345_stock_batch_state (
                id,
                next_page,
                cycle_number,
                last_successful_page,
                updated_at
            )
            values (
                1,
                1,
                1,
                null,
                now()
            )
            on conflict (id) do nothing
            """
        )

    conn.commit()


def acquire_lock(conn: psycopg.Connection) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            select pg_try_advisory_lock(
                hashtext(%s)
            )
            """,
            (LOCK_NAME,),
        )
        row = cur.fetchone()

    conn.commit()

    return bool(row and row[0])


def load_state(
    conn: psycopg.Connection,
) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                next_page,
                cycle_number
            from public.shop3345_stock_batch_state
            where id = 1
            """
        )
        row = cur.fetchone()

    conn.commit()

    if row is None:
        raise RuntimeError(
            "De 3345-voorraadcursor ontbreekt."
        )

    return int(row[0]), int(row[1])


def reset_state(
    conn: psycopg.Connection,
    *,
    increment_cycle: bool,
) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.shop3345_stock_batch_state
            set
                next_page = 1,
                cycle_number =
                    cycle_number + %s,
                last_successful_page = null,
                updated_at = now()
            where id = 1
            returning
                next_page,
                cycle_number
            """,
            (1 if increment_cycle else 0,),
        )
        row = cur.fetchone()

    conn.commit()

    if row is None:
        raise RuntimeError(
            "De voorraadcursor kon niet worden gereset."
        )

    return int(row[0]), int(row[1])


def checkpoint(
    conn: psycopg.Connection,
    *,
    page: int,
    cycle_number: int,
    cycle_complete: bool,
) -> tuple[int, int]:
    if cycle_complete:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.shop3345_stock_batch_state
                set
                    next_page = 1,
                    cycle_number =
                        cycle_number + 1,
                    last_successful_page = %s,
                    updated_at = now()
                where id = 1
                  and next_page = %s
                  and cycle_number = %s
                returning
                    next_page,
                    cycle_number
                """,
                (
                    page,
                    page,
                    cycle_number,
                ),
            )
            row = cur.fetchone()
    else:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.shop3345_stock_batch_state
                set
                    next_page = %s,
                    last_successful_page = %s,
                    updated_at = now()
                where id = 1
                  and next_page = %s
                  and cycle_number = %s
                returning
                    next_page,
                    cycle_number
                """,
                (
                    page + 1,
                    page,
                    page,
                    cycle_number,
                ),
            )
            row = cur.fetchone()

    conn.commit()

    if row is None:
        raise RuntimeError(
            "De cursor veranderde tijdens de run; "
            "checkpoint is geweigerd."
        )

    return int(row[0]), int(row[1])


def run_page(
    *,
    page: int,
    cycle_number: int,
    wait_ms: int,
    retries: int,
    write: bool,
    output_dir: Path,
) -> PageResult:
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    prefix = (
        f"cycle-{cycle_number:04d}"
        f"-page-{page:04d}"
    )

    csv_path = output_dir / f"{prefix}.csv"
    report_path = output_dir / f"{prefix}-report.csv"
    log_path = output_dir / f"{prefix}.log"

    command = [
        sys.executable,
        "-u",
        "-m",
        STOCK_MODULE,
        "--start-page",
        str(page),
        "--max-pages",
        "1",
        "--concurrency",
        "1",
        "--wait-ms",
        str(wait_ms),
        "--retries",
        str(retries),
        "--output",
        str(csv_path),
        "--report",
        str(report_path),
    ]

    if write:
        command.append("--write")

    print(
        "[3345-STOCK-BATCH][COMMAND]",
        " ".join(command),
        flush=True,
    )

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    lines: list[str] = []

    assert process.stdout is not None

    with log_path.open(
        "w",
        encoding="utf-8",
    ) as log_handle:
        for line in process.stdout:
            print(
                line,
                end="",
                flush=True,
            )
            log_handle.write(line)
            log_handle.flush()
            lines.append(line)

    returncode = process.wait()
    output = "".join(lines)
    lowered = output.lower()

    has_next = not bool(
        HAS_NEXT_FALSE_PATTERN.search(output)
    )

    if returncode == 0:
        status = "written" if write else "dry_run"

        return PageResult(
            page=page,
            status=status,
            has_next=has_next,
            returncode=returncode,
        )

    if (
        write
        and NO_MATCH_MARKER in lowered
    ):
        print(
            "[3345-STOCK-BATCH][NOOP]",
            {
                "page": page,
                "reason": (
                    "geen gekoppelde public.prices-records "
                    "op deze begrensde pagina"
                ),
                "checkpoint": True,
            },
            flush=True,
        )

        return PageResult(
            page=page,
            status="no_linked_prices",
            has_next=has_next,
            returncode=returncode,
        )

    tail = "\n".join(
        output.splitlines()[-40:]
    )

    raise RuntimeError(
        f"Voorraadpagina {page} is mislukt "
        f"met exitcode {returncode}.\n\n"
        f"Laatste logregels:\n{tail}"
    )


def main() -> int:
    args = parser().parse_args()

    if args.pages < 1:
        raise SystemExit(
            "--pages moet minimaal 1 zijn."
        )

    if args.start_page < 1:
        raise SystemExit(
            "--start-page moet minimaal 1 zijn."
        )

    if args.page_sleep < 0:
        raise SystemExit(
            "--page-sleep mag niet negatief zijn."
        )

    if args.wait_ms < 0:
        raise SystemExit(
            "--wait-ms mag niet negatief zijn."
        )

    if args.retries < 1:
        raise SystemExit(
            "--retries moet minimaal 1 zijn."
        )

    output_dir = Path(args.output_dir)

    if not args.write:
        page = args.start_page

        for index in range(args.pages):
            result = run_page(
                page=page,
                cycle_number=0,
                wait_ms=args.wait_ms,
                retries=args.retries,
                write=False,
                output_dir=output_dir,
            )

            print(
                "[3345-STOCK-BATCH][PAGE]",
                result,
                flush=True,
            )

            if not result.has_next:
                print(
                    "[3345-STOCK-BATCH] "
                    "Einde collectie bereikt.",
                    flush=True,
                )
                return 0

            page += 1

            if index + 1 < args.pages:
                time.sleep(args.page_sleep)

        return 0

    with psycopg.connect(
        get_database_url()
    ) as conn:
        prepare_state(conn)

        if not acquire_lock(conn):
            print(
                "[3345-STOCK-BATCH] "
                "Een andere 3345-run is actief; "
                "deze run stopt veilig.",
                flush=True,
            )
            return 0

        if args.reset_cycle:
            page, cycle_number = reset_state(
                conn,
                increment_cycle=False,
            )
        else:
            page, cycle_number = load_state(conn)

        print(
            "[3345-STOCK-BATCH][START]",
            {
                "cycle": cycle_number,
                "start_page": page,
                "maximum_pages": args.pages,
            },
            flush=True,
        )

        processed = 0

        while processed < args.pages:
            result = run_page(
                page=page,
                cycle_number=cycle_number,
                wait_ms=args.wait_ms,
                retries=args.retries,
                write=True,
                output_dir=output_dir,
            )

            cycle_complete = not result.has_next

            next_page, next_cycle = checkpoint(
                conn,
                page=page,
                cycle_number=cycle_number,
                cycle_complete=cycle_complete,
            )

            processed += 1

            print(
                "[3345-STOCK-BATCH][CHECKPOINT]",
                {
                    "page": page,
                    "result": result.status,
                    "processed": processed,
                    "next_page": next_page,
                    "cycle": next_cycle,
                },
                flush=True,
            )

            if cycle_complete:
                print(
                    "[3345-STOCK-BATCH][CYCLE-COMPLETE]",
                    {
                        "completed_cycle": cycle_number,
                        "last_page": page,
                        "next_cycle": next_cycle,
                        "next_page": 1,
                    },
                    flush=True,
                )
                return 0

            page = next_page

            if processed < args.pages:
                time.sleep(args.page_sleep)

        print(
            "[3345-STOCK-BATCH][DONE]",
            {
                "cycle": cycle_number,
                "pages_processed": processed,
                "next_page": page,
            },
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
