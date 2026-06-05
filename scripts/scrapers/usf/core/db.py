from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator

from dotenv import load_dotenv
import psycopg


def load_env() -> None:
    load_dotenv(".env.local", override=True)
    load_dotenv(override=True)


def get_database_url() -> str:
    load_env()
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL ontbreekt. Zet DATABASE_URL in .env.local of je environment.")
    return database_url


@contextmanager
def db_connection() -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(get_database_url())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
