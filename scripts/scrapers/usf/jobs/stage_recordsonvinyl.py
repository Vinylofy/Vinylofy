from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from html import unescape
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row


SHOP_ID = "recordsonvinyl"


def load_env() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return

    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")

        if key and key not in os.environ:
            os.environ[key] = value


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_table_columns(conn: psycopg.Connection, table_name: str) -> dict[str, dict[str, Any]]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            select
                column_name,
                data_type,
                is_nullable,
                column_default
            from information_schema.columns
            where table_schema = 'public'
              and table_name = %s
            order by ordinal_position
            """,
            (table_name,),
        )
        rows = cur.fetchall()

    return {row["column_name"]: dict(row) for row in rows}


def normalize_price(value: Any) -> Decimal | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = (
        text.replace("€", "")
        .replace("EUR", "")
        .replace("\xa0", " ")
        .strip()
    )

    match = re.search(r"([0-9]+(?:[.,][0-9]{1,2})?)", text)
    if not match:
        return None

    number = match.group(1).replace(",", ".")

    try:
        return Decimal(number)
    except InvalidOperation:
        return None


def normalize_ean(value: Any) -> str | None:
    if value is None:
        return None

    digits = re.sub(r"\D", "", str(value))
    if 8 <= len(digits) <= 14:
        return digits

    return None


def find_html(payload: Any) -> str | None:
    if payload is None:
        return None

    if isinstance(payload, str):
        stripped = payload.strip()

        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return find_html(json.loads(stripped))
            except Exception:
                pass

        if "<html" in stripped.lower() or "<body" in stripped.lower() or "application/ld+json" in stripped.lower():
            return stripped

        return None

    if isinstance(payload, dict):
        candidates: list[str] = []

        for key in (
            "html",
            "body",
            "text",
            "response_text",
            "page_html",
            "raw_html",
            "content",
        ):
            value = payload.get(key)
            if isinstance(value, str):
                candidates.append(value)

        for value in payload.values():
            found = find_html(value)
            if found:
                candidates.append(found)

        if candidates:
            return max(candidates, key=len)

    if isinstance(payload, list):
        candidates = [found for item in payload if (found := find_html(item))]
        if candidates:
            return max(candidates, key=len)

    return None


def clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = unescape(str(value))
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


def normalize_title(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None

    text = re.sub(r"\s*(?:&ndash;|–|-|\|)\s*Records on Vinyl\s*$", "", text, flags=re.IGNORECASE)
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


def title_from_slug(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    text = re.sub(r"[-_]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


def extract_title_from_html(html: str | None) -> str | None:
    if not html:
        return None

    patterns = [
        r"<h1[^>]*>(.*?)</h1>",
        r"<title[^>]*>(.*?)</title>",
        r'"name"\s*:\s*"([^"]+)"',
    ]

    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            title = clean_text(match.group(1))
            if title:
                title = re.sub(r"\s*[|–-]\s*Records on Vinyl\s*$", "", title, flags=re.IGNORECASE)
                return title.strip()

    return None


def extract_ean_from_html(html: str | None) -> str | None:
    if not html:
        return None

    patterns = [
        r'"barcode"\s*:\s*"(\d{8,14})"',
        r'"gtin(?:8|12|13|14)?"\s*:\s*"(\d{8,14})"',
        r'\b(?:EAN|GTIN|Barcode|Streepjescode)\D{0,80}(\d{8,14})\b',
    ]

    for pattern in patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            ean = normalize_ean(match.group(1))
            if ean:
                return ean

    return None


def extract_price_from_html(html: str | None) -> Decimal | None:
    if not html:
        return None

    money_patterns = [
        r'property=["\']product:price:amount["\'][^>]*content=["\']([0-9]+(?:[.,][0-9]{1,2})?)["\']',
        r'content=["\']([0-9]+(?:[.,][0-9]{1,2})?)["\'][^>]*property=["\']product:price:amount["\']',
        r'"price"\s*:\s*"(?P<price>[0-9]+(?:[.,][0-9]{1,2})?)"',
        r'€\s*([0-9]+(?:[.,][0-9]{1,2})?)',
    ]

    for pattern in money_patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            value = match.groupdict().get("price") if match.groupdict() else match.group(1)
            price = normalize_price(value)
            if price is not None:
                return price

    # Shopify product JSON gebruikt vaak centen: "price":2499 = €24.99
    cent_patterns = [
        r'"price"\s*:\s*(\d{3,7})',
        r'"price_min"\s*:\s*(\d{3,7})',
    ]

    for pattern in cent_patterns:
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            cents = Decimal(match.group(1))
            if cents > 100:
                return cents / Decimal("100")

    return None


def extract_availability_from_html(html: str | None) -> str | None:
    if not html:
        return None

    lower = html.lower()

    # Explicit Shopify variant/product JSON wins over generic page text.
    if re.search(r'"available"\s*:\s*true\b', html, flags=re.IGNORECASE):
        return "in_stock"

    if re.search(r'"available"\s*:\s*false\b', html, flags=re.IGNORECASE):
        return "out_of_stock"

    # Positive purchase signals win over hidden generic "sold out" snippets.
    if (
        "instock" in lower
        or "add to cart" in lower
        or "in winkelwagen" in lower
        or "in winkelmandje" in lower
        or "binnen 48 uur" in lower
    ):
        return "in_stock"

    if "outofstock" in lower or "sold out" in lower or "uitverkocht" in lower:
        return "out_of_stock"

    return None


def get_payload(row: dict[str, Any]) -> Any:
    for key in ("payload_raw", "raw_payload", "payload", "metadata", "data"):
        if key in row and row[key] is not None:
            return row[key]
    return None


def fetch_unstaged_raw_rows(
    conn: psycopg.Connection,
    raw_columns: dict[str, dict[str, Any]],
    staged_columns: dict[str, dict[str, Any]],
    limit: int,
) -> list[dict[str, Any]]:
    raw_id_col = "id" if "id" in raw_columns else None
    if not raw_id_col:
        raise RuntimeError("raw_shop_scrapes heeft geen id-kolom; staging kan raw records niet veilig dedupliceren.")

    staged_raw_ref_col = next(
        (
            col
            for col in ("raw_scrape_id", "raw_shop_scrape_id", "raw_id")
            if col in staged_columns
        ),
        None,
    )

    with conn.cursor(row_factory=dict_row) as cur:
        if staged_raw_ref_col:
            cur.execute(
                f"""
                select r.*
                from public.raw_shop_scrapes r
                left join public.staged_offers s
                  on s.{q(staged_raw_ref_col)} = r.{q(raw_id_col)}
                where r.shop_id = %s
                  and r.ean_raw is not null
                  and r.price_raw is not null
                  and s.{q(staged_raw_ref_col)} is null
                order by r.scraped_at desc nulls last, r.{q(raw_id_col)} asc
                limit %s
                """,
                (SHOP_ID, limit),
            )
        elif "source_url" in staged_columns:
            cur.execute(
                """
                select r.*
                from public.raw_shop_scrapes r
                where r.shop_id = %s
                  and r.ean_raw is not null
                  and r.price_raw is not null
                  and not exists (
                    select 1
                    from public.staged_offers s
                    where s.source_url = r.source_url
                  )
                order by r.scraped_at desc nulls last, r.id asc
                limit %s
                """,
                (SHOP_ID, limit),
            )
        else:
            raise RuntimeError(
                "staged_offers heeft geen raw_scrape_id/raw_shop_scrape_id/raw_id en geen source_url; "
                "kan niet veilig dedupliceren."
            )

        return [dict(row) for row in cur.fetchall()]


def build_staged_values(
    row: dict[str, Any],
    raw_columns: dict[str, dict[str, Any]],
    staged_columns: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    payload = get_payload(row)
    html = find_html(payload)

    raw_id = row.get("id")
    source_url = row.get("source_url")
    source_product_id = row.get("source_product_id")
    shop_id = row.get("shop_id") or SHOP_ID

    title_raw = row.get("title_raw") or extract_title_from_html(html) or title_from_slug(source_product_id)
    title = clean_text(title_raw) or extract_title_from_html(html) or title_from_slug(source_product_id)
    title_normalized = normalize_title(title)

    ean_raw = row.get("ean_raw") or extract_ean_from_html(html)
    ean_normalized = normalize_ean(ean_raw)

    price_raw = row.get("price_raw")
    price = normalize_price(price_raw) or extract_price_from_html(html)

    availability_raw = row.get("availability_raw")
    html_availability = extract_availability_from_html(html)
    raw_availability = clean_text(availability_raw)
    availability = html_availability or raw_availability

    image_url_raw = row.get("image_url_raw")
    image_url = clean_text(image_url_raw)

    now = datetime.now(timezone.utc)

    missing_reasons = []
    if not ean_normalized:
        missing_reasons.append("missing_ean")
    if price is None:
        missing_reasons.append("missing_price")
    if not title_normalized:
        missing_reasons.append("missing_title")

    stage_reason = ";".join(missing_reasons) if missing_reasons else None

    values_by_possible_column = {
        "raw_scrape_id": raw_id,
        "raw_shop_scrape_id": raw_id,
        "raw_id": raw_id,
        "shop_id": shop_id,
        "source_url": source_url,
        "url": source_url,
        "product_url": source_url,
        "source_product_id": source_product_id,
        "shop_product_id": source_product_id,
        "title_raw": title_raw,
        "title": title,
        "title_normalized": title_normalized,
        "product_title": title,
        "name": title,
        "ean_raw": ean_raw,
        "ean": ean_normalized,
        "ean_normalized": ean_normalized,
        "ean_match_key": ean_normalized,
        "gtin": ean_normalized,
        "gtin_normalized": ean_normalized,
        "price_raw": price_raw,
        "price": price,
        "price_amount": price,
        "amount": price,
        "currency": "EUR" if price is not None else None,
        "availability_raw": availability_raw,
        "availability": availability,
        "availability_normalized": availability,
        "image_url_raw": image_url_raw,
        "image_url": image_url,
        "image": image_url,
        "in_stock": availability == "in_stock" if availability else None,
        "status": "staged",
        "stage_status": "staged",
        "stage_reason": stage_reason,
        "staged_at": now,
        "created_at": now,
        "updated_at": now,
    }

    insert_values: dict[str, Any] = {}

    for col in staged_columns:
        if col in values_by_possible_column:
            value = values_by_possible_column[col]
            if value is not None:
                insert_values[col] = value

    required_missing = []
    for col, meta in staged_columns.items():
        if col in insert_values:
            continue
        if meta["is_nullable"] == "NO" and meta["column_default"] is None:
            if col not in ("id",):
                required_missing.append(col)

    if required_missing:
        raise RuntimeError(
            "Kan staged_offers niet vullen; verplichte kolommen zonder waarde/default: "
            + ", ".join(required_missing)
        )

    return insert_values


def insert_staged_offer(
    conn: psycopg.Connection,
    values: dict[str, Any],
    dry_run: bool,
) -> None:
    if dry_run:
        print("[DRY-RUN] staged values:", values)
        return

    cols = list(values.keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_sql = ", ".join(q(col) for col in cols)

    sql = f"""
        insert into public.staged_offers ({col_sql})
        values ({placeholders})
    """

    with conn.cursor() as cur:
        cur.execute(sql, [values[col] for col in cols])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=25)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    load_env()

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("[ERROR] DATABASE_URL ontbreekt. Zet DATABASE_URL in je environment of .env.")

    with psycopg.connect(database_url) as conn:
        raw_columns = get_table_columns(conn, "raw_shop_scrapes")
        staged_columns = get_table_columns(conn, "staged_offers")

        if not raw_columns:
            raise SystemExit("[ERROR] Tabel public.raw_shop_scrapes niet gevonden of heeft geen kolommen.")

        if not staged_columns:
            raise SystemExit("[ERROR] Tabel public.staged_offers niet gevonden of heeft geen kolommen.")

        rows = fetch_unstaged_raw_rows(conn, raw_columns, staged_columns, args.limit)

        print(f"[STAGE] shop={SHOP_ID} queued={len(rows)} dry_run={args.dry_run}")

        inserted = 0
        skipped = 0

        for idx, row in enumerate(rows, start=1):
            raw_id = row.get("id")
            source_url = row.get("source_url")
            print(f"[STAGE] {idx}/{len(rows)} raw_id={raw_id} url={source_url}")

            try:
                values = build_staged_values(row, raw_columns, staged_columns)
                insert_staged_offer(conn, values, args.dry_run)
                if not args.dry_run:
                    conn.commit()
                inserted += 1
            except Exception as exc:
                conn.rollback()
                skipped += 1
                print(f"[STAGE][WARN] skipped raw_id={raw_id} error={exc}")

        print(f"[STAGE] done inserted={inserted} skipped={skipped}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
