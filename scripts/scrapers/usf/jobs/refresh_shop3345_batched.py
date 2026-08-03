#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass

import psycopg


BASE_MODULE = (
    "scripts.scrapers.usf.jobs."
    "refresh_shop3345_listing_prices"
)

LOCK_NAME = "vinylofy:shop3345:listing-batch-v2"

END_MARKERS = (
    "lege pagina",
    "geen productlinks",
    "geen product links",
    "no product links",
    "no products found",
    "scan leverde geen productlinks op",
)


@dataclass(frozen=True)
class PageResult:
    status: str
    output: str


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Draai de bewezen 3345-listingscraper pagina voor pagina. "
            "De databasecursor gaat pas vooruit na een geslaagde pagina."
        )
    )

    parser.add_argument(
        "--pages",
        type=int,
        default=50,
        help="Maximumaantal succesvolle pagina's in deze run.",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=1,
        help="Startpagina voor een dry-run.",
    )
    parser.add_argument(
        "--page-sleep",
        type=float,
        default=5.0,
        help="Pauze tussen succesvolle pagina's.",
    )
    parser.add_argument(
        "--max-page-failures",
        type=int,
        default=6,
        help="Retrylimiet van de bestaande scraper.",
    )
    parser.add_argument(
        "--reset-cycle",
        action="store_true",
        help="Zet de persistente cursor terug naar pagina 1.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
    )
    parser.add_argument(
        "--write",
        action="store_true",
    )

    return parser


def database_url() -> str:
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
            create table if not exists public.shop3345_batch_state (
                id smallint primary key
                    check (id = 1),
                next_page integer not null
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
            insert into public.shop3345_batch_state (
                id,
                next_page,
                cycle_number,
                last_successful_page,
                updated_at
            )
            values (1, 1, 1, null, now())
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


def read_state(
    conn: psycopg.Connection,
) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                next_page,
                cycle_number
            from public.shop3345_batch_state
            where id = 1
            """
        )
        row = cur.fetchone()

    conn.commit()

    if row is None:
        raise RuntimeError(
            "De 3345-batchcursor ontbreekt."
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
            update public.shop3345_batch_state
            set
                next_page = 1,
                cycle_number = cycle_number + %s,
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
            "De 3345-batchcursor kon niet worden gereset."
        )

    return int(row[0]), int(row[1])


def checkpoint_page(
    conn: psycopg.Connection,
    *,
    page: int,
    cycle_number: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.shop3345_batch_state
            set
                next_page = %s,
                last_successful_page = %s,
                updated_at = now()
            where id = 1
              and next_page = %s
              and cycle_number = %s
            """,
            (
                page + 1,
                page,
                page,
                cycle_number,
            ),
        )

        if cur.rowcount != 1:
            raise RuntimeError(
                "De cursor is tijdens de run gewijzigd. "
                "Checkpoint geweigerd."
            )

    conn.commit()


def page_command(
    *,
    page: int,
    max_page_failures: int,
    debug: bool,
    write: bool,
) -> list[str]:
    command = [
        sys.executable,
        "-u",
        "-m",
        BASE_MODULE,
        "--start-page",
        str(page),
        "--max-pages",
        "1",
        "--sleep",
        "1.0",
        "--max-page-failures",
        str(max_page_failures),
    ]

    if debug:
        command.append("--debug")

    if write:
        command.append("--write")

    return command


def run_page(
    *,
    page: int,
    max_page_failures: int,
    debug: bool,
    write: bool,
) -> PageResult:
    command = page_command(
        page=page,
        max_page_failures=max_page_failures,
        debug=debug,
        write=write,
    )

    print(
        "[3345-BATCH][COMMAND]",
        " ".join(command),
        flush=True,
    )

    process = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )

    output = process.stdout or ""

    print(output, end="" if output.endswith("\n") else "\n")

    if process.returncode == 0:
        return PageResult(
            status="success",
            output=output,
        )

    lowered = output.lower()

    if any(marker in lowered for marker in END_MARKERS):
        return PageResult(
            status="end",
            output=output,
        )

    tail = "\n".join(output.splitlines()[-30:])

    raise RuntimeError(
        f"3345-pagina {page} is mislukt met "
        f"exitcode {process.returncode}.\n\n"
        f"Laatste logregels:\n{tail}"
    )


def confirmed_end(
    *,
    page: int,
    max_page_failures: int,
    debug: bool,
    write: bool,
) -> bool:
    print(
        "[3345-BATCH] Mogelijk einde van de collectie. "
        "Dezelfde pagina wordt nog één keer gecontroleerd.",
        {"page": page},
        flush=True,
    )

    time.sleep(10)

    second_result = run_page(
        page=page,
        max_page_failures=max_page_failures,
        debug=debug,
        write=write,
    )

    return second_result.status == "end"


def main() -> int:
    args = build_parser().parse_args()

    if args.pages < 1:
        raise SystemExit(
            "[ERROR] --pages moet minimaal 1 zijn."
        )

    if args.start_page < 1:
        raise SystemExit(
            "[ERROR] --start-page moet minimaal 1 zijn."
        )

    if args.page_sleep < 0:
        raise SystemExit(
            "[ERROR] --page-sleep mag niet negatief zijn."
        )

    if args.max_page_failures < 1:
        raise SystemExit(
            "[ERROR] --max-page-failures moet minimaal 1 zijn."
        )

    if not args.write:
        page = args.start_page

        print(
            "[3345-BATCH][DRY-RUN]",
            {
                "start_page": page,
                "pages": args.pages,
            },
            flush=True,
        )

        for index in range(args.pages):
            result = run_page(
                page=page,
                max_page_failures=args.max_page_failures,
                debug=args.debug,
                write=False,
            )

            if result.status == "end":
                print(
                    "[3345-BATCH] Einde collectie bereikt.",
                    {"page": page},
                    flush=True,
                )
                return 0

            page += 1

            if index + 1 < args.pages:
                time.sleep(args.page_sleep)

        print(
            "[3345-BATCH][DRY-RUN-DONE]",
            {"next_page": page},
            flush=True,
        )

        return 0

    with psycopg.connect(database_url()) as conn:
        prepare_state(conn)

        if not acquire_lock(conn):
            print(
                "[3345-BATCH] Een andere 3345-batch is actief. "
                "Deze run stopt veilig.",
                flush=True,
            )
            return 0

        if args.reset_cycle:
            page, cycle_number = reset_state(
                conn,
                increment_cycle=False,
            )
        else:
            page, cycle_number = read_state(conn)

        print(
            "[3345-BATCH][START]",
            {
                "cycle": cycle_number,
                "start_page": page,
                "maximum_pages": args.pages,
            },
            flush=True,
        )

        pages_committed = 0

        while pages_committed < args.pages:
            result = run_page(
                page=page,
                max_page_failures=args.max_page_failures,
                debug=args.debug,
                write=True,
            )

            if result.status == "end":
                if not confirmed_end(
                    page=page,
                    max_page_failures=args.max_page_failures,
                    debug=args.debug,
                    write=True,
                ):
                    print(
                        "[3345-BATCH] De tweede controle slaagde. "
                        "De pagina wordt alsnog gecheckpoint.",
                        {"page": page},
                        flush=True,
                    )
                else:
                    next_page, next_cycle = reset_state(
                        conn,
                        increment_cycle=True,
                    )

                    print(
                        "[3345-BATCH][CYCLE-COMPLETE]",
                        {
                            "completed_cycle": cycle_number,
                            "end_page": page,
                            "next_cycle": next_cycle,
                            "next_page": next_page,
                        },
                        flush=True,
                    )

                    return 0

            checkpoint_page(
                conn,
                page=page,
                cycle_number=cycle_number,
            )

            pages_committed += 1

            print(
                "[3345-BATCH][CHECKPOINT]",
                {
                    "cycle": cycle_number,
                    "page": page,
                    "pages_committed": pages_committed,
                    "next_page": page + 1,
                },
                flush=True,
            )

            page += 1

            if pages_committed < args.pages:
                time.sleep(args.page_sleep)

        print(
            "[3345-BATCH][DONE]",
            {
                "cycle": cycle_number,
                "pages_committed": pages_committed,
                "next_page": page,
            },
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
