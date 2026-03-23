#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import psycopg


def normalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    scheme = (parts.scheme or "https").lower()
    netloc = parts.netloc.lower().strip()
    path = re.sub(r"/+", "/", parts.path or "")
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


def normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def root_from_domain(domain: str) -> str:
    d = (domain or "").strip().lower()
    d = d.split(":")[0]
    d = d.split("/")[0]
    if d.startswith("www."):
        d = d[4:]
    first = d.split(".")[0] if d else ""
    return normalize_key(first)


def expand_aliases(name: str, domain: str) -> set[str]:
    aliases: set[str] = set()

    name_key = normalize_key(name)
    domain_key = root_from_domain(domain)

    for key in [name_key, domain_key]:
        if not key:
            continue
        aliases.add(key)

        if key.isdigit():
            aliases.add(f"shop{key}")

        if key.startswith("shop") and key[4:].isdigit():
            aliases.add(key[4:])

    return aliases


def read_clean_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append({k: (v or "").strip() for k, v in row.items()})
        return rows


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def fetch_shop_map(
    conn: psycopg.Connection,
    shops_table: str,
    shop_id_column: str,
    shop_name_column: str,
    shop_domain_column: str,
) -> dict[str, str]:
    sql = f"SELECT {shop_id_column}::text, {shop_name_column}, {shop_domain_column} FROM {shops_table}"
    out: dict[str, str] = {}
    with conn.cursor() as cur:
        cur.execute(sql)
        for shop_id, name, domain in cur.fetchall():
            for alias in expand_aliases(name or "", domain or ""):
                out[alias] = str(shop_id)
    return out


def fetch_price_rows_for_shop(
    conn: psycopg.Connection,
    prices_table: str,
    products_table: str,
    shop_id_column: str,
    product_id_column: str,
    product_url_column: str,
    product_pk_column: str,
    product_ean_column: str,
    shop_id: str,
) -> list[tuple[int, str, str | None]]:
    sql = f"""
        SELECT
            p.{product_pk_column}::text AS product_id,
            pr.{product_url_column}::text AS product_url,
            p.{product_ean_column}::text AS current_ean
        FROM {prices_table} pr
        JOIN {products_table} p
          ON p.{product_pk_column} = pr.{product_id_column}
        WHERE pr.{shop_id_column} = %s
          AND pr.{product_url_column} IS NOT NULL
    """
    with conn.cursor() as cur:
        cur.execute(sql, (shop_id,))
        return [(str(pid), url or "", current_ean) for pid, url, current_ean in cur.fetchall()]


def main() -> None:
    parser = argparse.ArgumentParser(description="Dry-run EAN backfill against existing prices/products.")
    parser.add_argument("clean_csv", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("output/ean_backfill"))
    parser.add_argument("--database-url", default=os.getenv("DATABASE_URL", ""))
    parser.add_argument("--shops-table", default="public.shops")
    parser.add_argument("--prices-table", default="public.prices")
    parser.add_argument("--products-table", default="public.products")
    parser.add_argument("--shop-id-column", default="id")
    parser.add_argument("--shop-name-column", default="name")
    parser.add_argument("--shop-domain-column", default="domain")
    parser.add_argument("--product-id-column", default="product_id")
    parser.add_argument("--product-pk-column", default="id")
    parser.add_argument("--product-url-column", default="product_url")
    parser.add_argument("--product-ean-column", default="ean")
    args = parser.parse_args()

    if not args.database_url:
        raise SystemExit("DATABASE_URL ontbreekt. Zet die in je environment of geef --database-url mee.")

    clean_rows = read_clean_rows(args.clean_csv)
    stats = Counter()
    stats["clean_input_rows"] = len(clean_rows)

    apply_rows: list[dict] = []
    unmatched_rows: list[dict] = []
    conflict_rows: list[dict] = []
    skipped_rows: list[dict] = []

    with psycopg.connect(args.database_url) as conn:
        shop_map = fetch_shop_map(
            conn,
            args.shops_table,
            args.shop_id_column,
            args.shop_name_column,
            args.shop_domain_column,
        )

        by_shop: dict[str, list[dict]] = defaultdict(list)
        for row in clean_rows:
            by_shop[normalize_key(row["shop_key"])].append(row)

        for shop_key, rows in by_shop.items():
            shop_id = shop_map.get(shop_key)
            if shop_id is None:
                for row in rows:
                    unmatched_rows.append({**row, "reason": "shop_not_found_in_db"})
                    stats["unmatched_shop_not_found_in_db"] += 1
                continue

            db_rows = fetch_price_rows_for_shop(
                conn,
                args.prices_table,
                args.products_table,
                args.shop_id_column,
                args.product_id_column,
                args.product_url_column,
                args.product_pk_column,
                args.product_ean_column,
                shop_id,
            )

            by_norm_url: dict[str, list[tuple[int, str, str | None]]] = defaultdict(list)
            for product_id, product_url, current_ean in db_rows:
                norm = normalize_url(product_url)
                if norm:
                    by_norm_url[norm].append((product_id, product_url, current_ean))

            for row in rows:
                stats["candidate_rows_examined"] += 1
                matches = by_norm_url.get(row["normalized_product_url"], [])

                if not matches:
                    unmatched_rows.append({**row, "shop_id": shop_id, "reason": "url_not_found_in_prices"})
                    stats["unmatched_url_not_found_in_prices"] += 1
                    continue

                if len(matches) > 1:
                    distinct_product_ids = sorted({m[0] for m in matches})
                    conflict_rows.append({
                        **row,
                        "shop_id": shop_id,
                        "conflict_type": "multiple_price_matches_for_same_url",
                        "matched_product_ids": "|".join(map(str, distinct_product_ids)),
                    })
                    stats["conflict_multiple_price_matches_for_same_url"] += 1
                    continue

                product_id, matched_product_url, current_ean = matches[0]

                if current_ean:
                    if current_ean == row["ean_clean"]:
                        skipped_rows.append({
                            **row,
                            "shop_id": shop_id,
                            "product_id": product_id,
                            "matched_product_url": matched_product_url,
                            "ean_old": current_ean,
                            "skip_reason": "ean_already_same",
                        })
                        stats["skipped_ean_already_same"] += 1
                    else:
                        conflict_rows.append({
                            **row,
                            "shop_id": shop_id,
                            "product_id": product_id,
                            "matched_product_url": matched_product_url,
                            "ean_old": current_ean,
                            "conflict_type": "existing_ean_differs",
                            "ean_new": row["ean_clean"],
                        })
                        stats["conflict_existing_ean_differs"] += 1
                    continue

                apply_rows.append({
                    **row,
                    "shop_id": shop_id,
                    "product_id": product_id,
                    "matched_product_url": matched_product_url,
                    "ean_old": current_ean or "",
                    "ean_new": row["ean_clean"],
                })
                stats["apply_rows"] += 1

    outdir = args.output_dir
    write_csv(
        outdir / "ean_url_backfill_apply.csv",
        apply_rows,
        [
            "source_row",
            "shop_raw",
            "shop_key",
            "shop_id",
            "product_id",
            "original_product_url",
            "normalized_product_url",
            "matched_product_url",
            "ean_original",
            "ean_clean",
            "ean_old",
            "ean_new",
        ],
    )
    write_csv(
        outdir / "ean_url_backfill_unmatched.csv",
        unmatched_rows,
        sorted({k for row in unmatched_rows for k in row.keys()}),
    )
    write_csv(
        outdir / "ean_url_backfill_conflicts_db.csv",
        conflict_rows,
        sorted({k for row in conflict_rows for k in row.keys()}),
    )
    write_csv(
        outdir / "ean_url_backfill_skipped.csv",
        skipped_rows,
        sorted({k for row in skipped_rows for k in row.keys()}),
    )

    payload = {
        "clean_csv": str(args.clean_csv),
        "output_dir": str(outdir),
        "stats": dict(stats),
        "tables": {
            "shops": args.shops_table,
            "prices": args.prices_table,
            "products": args.products_table,
        },
    }
    (outdir / "ean_url_backfill_db_match_stats.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
