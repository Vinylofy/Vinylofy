#!/usr/bin/env python3
"""Audit and merge UPC/EAN product duplicates by strict GTIN identity.

The default mode is read-only.  ``--apply`` performs transactional batches
for the narrowly defined case currently present in Vinylofy: exactly two
products share a valid GTIN, exactly one has the canonical ``gtin_normalized``
value, and the other is the UPC/EAN display variant with a missing normalized
value.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
import psycopg
from psycopg.errors import UniqueViolation

# Support both ``python -m scripts.maintenance...`` and the repository's
# common direct-file invocation style.
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.importers.common import strict_normalize_gtin


@dataclass(frozen=True)
class Product:
    product_id: str
    ean: str
    gtin_normalized: str | None
    artist: str
    title: str
    format_label: str | None
    created_at: str


@dataclass(frozen=True)
class DuplicatePair:
    gtin: str
    canonical: Product
    duplicate: Product


def clean(value: object) -> str:
    return "" if value is None else str(value).strip()


def read_products(cur) -> list[Product]:
    cur.execute(
        """
        select id::text, coalesce(ean, ''), gtin_normalized, artist, title,
               format_label, created_at::text
        from public.products
        order by created_at, id
        """
    )
    return [
        Product(
            product_id=str(row[0]),
            ean=clean(row[1]),
            gtin_normalized=clean(row[2]) or None,
            artist=clean(row[3]),
            title=clean(row[4]),
            format_label=clean(row[5]) or None,
            created_at=clean(row[6]),
        )
        for row in cur.fetchall()
    ]


def find_pairs(products: Iterable[Product]) -> tuple[list[DuplicatePair], list[str]]:
    by_gtin: dict[str, list[Product]] = {}
    for product in products:
        gtin = strict_normalize_gtin(product.ean)
        if gtin:
            by_gtin.setdefault(gtin, []).append(product)

    pairs: list[DuplicatePair] = []
    unsupported: list[str] = []
    for gtin, rows in sorted(by_gtin.items()):
        if len(rows) == 1:
            continue

        canonical = [row for row in rows if row.gtin_normalized == gtin]
        missing = [row for row in rows if row.gtin_normalized is None]
        if len(rows) == 2 and len(canonical) == 1 and len(missing) == 1:
            pairs.append(DuplicatePair(gtin, canonical[0], missing[0]))
        else:
            unsupported.append(
                f"gtin={gtin} rows={len(rows)} canonical={len(canonical)} missing={len(missing)}"
            )

    return pairs, unsupported


def write_audit(path: Path, pairs: Iterable[DuplicatePair]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "gtin_normalized",
                "canonical_product_id",
                "canonical_ean",
                "canonical_artist",
                "canonical_title",
                "canonical_format",
                "duplicate_product_id",
                "duplicate_ean",
                "duplicate_artist",
                "duplicate_title",
                "duplicate_format",
            ],
        )
        writer.writeheader()
        for pair in pairs:
            writer.writerow(
                {
                    "gtin_normalized": pair.gtin,
                    "canonical_product_id": pair.canonical.product_id,
                    "canonical_ean": pair.canonical.ean,
                    "canonical_artist": pair.canonical.artist,
                    "canonical_title": pair.canonical.title,
                    "canonical_format": pair.canonical.format_label or "",
                    "duplicate_product_id": pair.duplicate.product_id,
                    "duplicate_ean": pair.duplicate.ean,
                    "duplicate_artist": pair.duplicate.artist,
                    "duplicate_title": pair.duplicate.title,
                    "duplicate_format": pair.duplicate.format_label or "",
                }
            )


def write_snapshot(cur, path: Path, pairs: Iterable[DuplicatePair]) -> None:
    """Persist affected rows locally before any destructive merge step."""
    product_ids = [
        product_id
        for pair in pairs
        for product_id in (pair.canonical.product_id, pair.duplicate.product_id)
    ]
    tables = (
        "products",
        "prices",
        "price_history",
        "product_artists",
        "product_cover_candidates",
        "product_cover_queue",
        "cover_lookup_queue",
        "product_masterdata_queue",
        "product_musicbrainz_enrichment",
        "release_calendar",
        "top_deals_snapshot",
        "groovespin_ean_probe_status",
    )
    snapshot: dict[str, list[dict[str, object]]] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "product_ids": product_ids,
        "tables": {},
    }
    for table in tables:
        if table == "products":
            cur.execute("select * from public.products where id = any(%s::uuid[])", (product_ids,))
        else:
            cur.execute(
                f"select * from public.{table} where product_id = any(%s::uuid[])",
                (product_ids,),
            )
        columns = [description.name for description in cur.description]
        snapshot["tables"][table] = [dict(zip(columns, row)) for row in cur.fetchall()]

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=2, default=str), encoding="utf-8")


def count_rows(cur, table: str, product_id: str) -> int:
    cur.execute(f"select count(*) from public.{table} where product_id = %s", (product_id,))
    return int(cur.fetchone()[0])


def merge_prices(cur, canonical_id: str, duplicate_id: str) -> int:
    """Move prices and merge same-shop rows while retaining the freshest data."""
    cur.execute(
        """
        select id, shop_id, price, currency, product_url, availability,
               first_seen_at, last_seen_at, is_active, created_at, updated_at,
               ean_raw, gtin_normalized
        from public.prices
        where product_id = %s
        order by id
        """,
        (duplicate_id,),
    )
    duplicate_rows = cur.fetchall()
    merged = 0
    for row in duplicate_rows:
        price_id, shop_id = row[0], row[1]
        cur.execute(
            "select id, price, currency, product_url, availability, first_seen_at, last_seen_at, is_active, updated_at, ean_raw from public.prices where product_id=%s and shop_id=%s for update",
            (canonical_id, shop_id),
        )
        existing = cur.fetchone()
        if existing is None:
            cur.execute("update public.prices set product_id=%s where id=%s", (canonical_id, price_id))
            continue

        existing_id = existing[0]
        duplicate_last_seen = row[7]
        existing_last_seen = existing[6]
        availability_rank = {"in_stock": 3, "preorder": 2, "unknown": 1, "out_of_stock": 0}
        duplicate_is_newer = (
            duplicate_last_seen > existing_last_seen
            or (
                duplicate_last_seen == existing_last_seen
                and availability_rank.get(row[5], -1) > availability_rank.get(existing[4], -1)
            )
        )
        if duplicate_is_newer:
            cur.execute(
                """
                update public.prices
                set price=%s, currency=%s, product_url=%s, availability=%s,
                    is_active=%s, updated_at=%s, ean_raw=coalesce(%s, ean_raw),
                    first_seen_at=least(first_seen_at, %s),
                    last_seen_at=greatest(last_seen_at, %s)
                where id=%s
                """,
                (
                    row[2], row[3], row[4], row[5], row[8], row[10], row[11],
                    row[6], row[7], existing_id,
                ),
            )
        else:
            cur.execute(
                """
                update public.prices
                set first_seen_at=least(first_seen_at, %s),
                    last_seen_at=greatest(last_seen_at, %s)
                where id=%s
                """,
                (row[6], row[7], existing_id),
            )
        cur.execute("delete from public.prices where id=%s", (price_id,))
        merged += 1

    return merged


def reparent_or_drop_conflict(cur, table: str, primary_key: str, canonical_id: str, duplicate_id: str) -> int:
    """Reparent rows; drop only rows blocked by a target-side unique key."""
    cur.execute(f"select {primary_key} from public.{table} where product_id=%s", (duplicate_id,))
    row_ids = [row[0] for row in cur.fetchall()]
    dropped = 0
    for row_id in row_ids:
        savepoint = f"sp_{table}_{str(row_id).replace('-', '_')}"[:60]
        cur.execute(f"savepoint {savepoint}")
        try:
            cur.execute(
                f"update public.{table} set product_id=%s where {primary_key}=%s",
                (canonical_id, row_id),
            )
        except UniqueViolation:
            cur.execute(f"rollback to savepoint {savepoint}")
            cur.execute(f"delete from public.{table} where {primary_key}=%s", (row_id,))
            dropped += 1
        finally:
            cur.execute(f"release savepoint {savepoint}")
    return dropped


def merge_cover_candidates(cur, canonical_id: str, duplicate_id: str) -> int:
    """Move candidates; collapse only exact source duplicates."""
    cur.execute(
        "select id, shop_id, source_url, image_url, is_selected, candidate_status, candidate_score, last_seen_at from public.product_cover_candidates where product_id=%s order by id",
        (duplicate_id,),
    )
    rows = cur.fetchall()
    dropped = 0
    for row in rows:
        candidate_id, shop_id, source_url, image_url, is_selected, status, score, last_seen = row
        conflict_conditions: list[str] = []
        conflict_params: list[object] = [canonical_id]
        if image_url is not None:
            conflict_conditions.append("image_url=%s")
            conflict_params.append(image_url)
        if shop_id is not None and source_url is not None:
            conflict_conditions.append("(shop_id=%s and source_url=%s)")
            conflict_params.extend((shop_id, source_url))

        if conflict_conditions:
            cur.execute(
                f"""
                select id, is_selected, candidate_status, candidate_score, last_seen_at
                from public.product_cover_candidates
                where product_id=%s and ({' or '.join(conflict_conditions)})
                for update
                """,
                conflict_params,
            )
            existing = cur.fetchone()
        else:
            existing = None

        if existing is None:
            cur.execute(
                "select id from public.product_cover_candidates where product_id=%s and is_selected=true for update",
                (canonical_id,),
            )
            canonical_selected = cur.fetchone()
            if is_selected and canonical_selected is not None:
                # The canonical product already has a selected candidate. Keep
                # that selection and retain the duplicate only as an audit
                # snapshot, not as a second selected candidate.
                cur.execute("delete from public.product_cover_candidates where id=%s", (candidate_id,))
                dropped += 1
            else:
                cur.execute("update public.product_cover_candidates set product_id=%s where id=%s", (canonical_id, candidate_id))
            continue

        existing_id, existing_selected, existing_status, existing_score, existing_seen = existing
        if is_selected and not existing_selected:
            # The duplicate's selection is retained only when it is the only
            # selected copy of this exact source candidate.
            cur.execute(
                "select 1 from public.product_cover_candidates where product_id=%s and is_selected=true limit 1",
                (canonical_id,),
            )
            if cur.fetchone() is None:
                cur.execute(
                    "update public.product_cover_candidates set is_selected=true, selected_at=coalesce(selected_at, now()), candidate_status='published' where id=%s",
                    (existing_id,),
                )
        cur.execute(
            """
            update public.product_cover_candidates
            set last_seen_at=greatest(last_seen_at, %s),
                candidate_score=greatest(coalesce(candidate_score, 0), coalesce(%s, 0)),
                updated_at=greatest(updated_at, now())
            where id=%s
            """,
            (last_seen, score, existing_id),
        )
        cur.execute("delete from public.product_cover_candidates where id=%s", (candidate_id,))
        dropped += 1
    return dropped


def merge_top_deals(cur, canonical_id: str, duplicate_id: str) -> int:
    """Reparent derived snapshot rows, dropping only composite-key conflicts."""
    cur.execute(
        "select snapshot_key, rank from public.top_deals_snapshot where product_id=%s",
        (duplicate_id,),
    )
    rows = cur.fetchall()
    dropped = 0
    for index, (snapshot_key, rank) in enumerate(rows):
        savepoint = f"sp_top_deal_{index}"
        cur.execute(f"savepoint {savepoint}")
        try:
            cur.execute(
                "update public.top_deals_snapshot set product_id=%s where snapshot_key=%s and rank=%s",
                (canonical_id, snapshot_key, rank),
            )
        except UniqueViolation:
            cur.execute(f"rollback to savepoint {savepoint}")
            cur.execute(
                "delete from public.top_deals_snapshot where snapshot_key=%s and rank=%s",
                (snapshot_key, rank),
            )
            dropped += 1
        finally:
            cur.execute(f"release savepoint {savepoint}")
    return dropped


def merge_pair(cur, pair: DuplicatePair) -> dict[str, int]:
    canonical_id = pair.canonical.product_id
    duplicate_id = pair.duplicate.product_id
    cur.execute(
        "select id from public.products where id in (%s, %s) for update",
        (canonical_id, duplicate_id),
    )

    stats: dict[str, int] = {"prices_merged": merge_prices(cur, canonical_id, duplicate_id)}
    cur.execute("update public.price_history set product_id=%s where product_id=%s", (canonical_id, duplicate_id))
    stats["price_history_reparented"] = cur.rowcount

    stats["cover_candidates_dropped"] = merge_cover_candidates(cur, canonical_id, duplicate_id)

    # These tables either have no product-level uniqueness or represent a
    # queue/enrichment state where the canonical row is authoritative.
    for table in ("cover_lookup_queue", "product_cover_queue", "product_musicbrainz_enrichment"):
        stats[f"{table}_dropped"] = reparent_or_drop_conflict(cur, table, "id" if table != "product_musicbrainz_enrichment" else "product_id", canonical_id, duplicate_id)
    stats["product_artists_dropped"] = reparent_or_drop_conflict(
        cur, "product_artists", "id", canonical_id, duplicate_id
    )
    stats["top_deals_dropped"] = merge_top_deals(cur, canonical_id, duplicate_id)

    for table in ("product_masterdata_queue", "release_calendar", "groovespin_ean_probe_status"):
        cur.execute(f"update public.{table} set product_id=%s where product_id=%s", (canonical_id, duplicate_id))
        stats[f"{table}_reparented"] = cur.rowcount

    # top_deals_snapshot has a composite primary key; the generic helper above
    # uses rank as a per-snapshot identifier and may leave a conflict. Remove
    # any remaining duplicate references because this is a derived snapshot.
    cur.execute("delete from public.top_deals_snapshot where product_id=%s", (duplicate_id,))
    stats["top_deals_deleted_remaining"] = cur.rowcount

    cur.execute("select count(*) from public.product_cover_candidates where product_id=%s", (duplicate_id,))
    if cur.fetchone()[0]:
        raise RuntimeError(f"cover candidates still reference duplicate {duplicate_id}")

    cur.execute("delete from public.products where id=%s", (duplicate_id,))
    if cur.rowcount != 1:
        raise RuntimeError(f"Expected to delete one duplicate product, got {cur.rowcount}: {duplicate_id}")
    stats["products_deleted"] = 1
    return stats


def install_merge_map(cur, pairs: list[DuplicatePair]) -> None:
    cur.execute(
        "create temporary table _gtin_merge_map (gtin_normalized text primary key, canonical_id uuid not null, duplicate_id uuid not null) on commit drop"
    )
    with cur.copy(
        "copy _gtin_merge_map (gtin_normalized, canonical_id, duplicate_id) from stdin"
    ) as copy:
        for pair in pairs:
            copy.write_row((pair.gtin, pair.canonical.product_id, pair.duplicate.product_id))


def apply_set_based(cur, pairs: list[DuplicatePair]) -> dict[str, int]:
    """Apply the same merge policy with set-based SQL and one short transaction."""
    install_merge_map(cur, pairs)
    cur.execute(
        """
        select count(*)
        from _gtin_merge_map m
        where not exists (select 1 from public.products p where p.id=m.canonical_id)
           or not exists (select 1 from public.products p where p.id=m.duplicate_id)
        """
    )
    if cur.fetchone()[0]:
        raise RuntimeError("Merge map contains a product that no longer exists")

    totals: dict[str, int] = {}

    cur.execute(
        """
        with latest as (
          select distinct on (m.canonical_id, d.shop_id)
                 c.id as canonical_price_id,
                 d.price, d.currency, d.product_url, d.availability,
                 d.is_active, d.updated_at, d.ean_raw,
                 d.first_seen_at, d.last_seen_at
          from _gtin_merge_map m
          join public.prices d on d.product_id=m.duplicate_id
          join public.prices c on c.product_id=m.canonical_id and c.shop_id=d.shop_id
          order by m.canonical_id, d.shop_id, d.last_seen_at desc,
            case d.availability when 'in_stock' then 3 when 'preorder' then 2 when 'unknown' then 1 else 0 end desc,
            d.updated_at desc, d.id
        )
        update public.prices c
        set price=l.price, currency=l.currency, product_url=l.product_url,
            availability=l.availability, is_active=l.is_active,
            updated_at=l.updated_at, ean_raw=coalesce(l.ean_raw, c.ean_raw),
            first_seen_at=least(c.first_seen_at, l.first_seen_at),
            last_seen_at=greatest(c.last_seen_at, l.last_seen_at)
        from latest l
        where c.id=l.canonical_price_id
        """
    )
    totals["prices_merged"] = cur.rowcount
    cur.execute(
        """
        update public.prices d
        set product_id=m.canonical_id
        from _gtin_merge_map m
        where d.product_id=m.duplicate_id
          and not exists (
            select 1 from public.prices c
            where c.product_id=m.canonical_id and c.shop_id=d.shop_id
          )
        """
    )
    totals["prices_reparented"] = cur.rowcount
    cur.execute(
        "delete from public.prices d using _gtin_merge_map m where d.product_id=m.duplicate_id"
    )
    totals["prices_deleted"] = cur.rowcount

    for table in ("price_history", "product_masterdata_queue", "release_calendar", "groovespin_ean_probe_status"):
        cur.execute(
            f"update public.{table} d set product_id=m.canonical_id from _gtin_merge_map m where d.product_id=m.duplicate_id"
        )
        totals[f"{table}_reparented"] = cur.rowcount

    cur.execute(
        """
        delete from public.product_artists d
        using _gtin_merge_map m
        where d.product_id=m.duplicate_id
          and exists (
            select 1 from public.product_artists c
            where c.product_id=m.canonical_id and c.artist_id=d.artist_id
          )
        """
    )
    totals["product_artists_deleted"] = cur.rowcount
    cur.execute(
        "update public.product_artists d set product_id=m.canonical_id from _gtin_merge_map m where d.product_id=m.duplicate_id"
    )
    totals["product_artists_reparented"] = cur.rowcount

    # Preserve a selected duplicate candidate where the canonical product has
    # no selected candidate yet. Exact source/image conflicts are collapsed.
    cur.execute(
        """
        with selected_target as (
          select distinct on (m.canonical_id, t.id) t.id as target_id
          from _gtin_merge_map m
          join public.product_cover_candidates d on d.product_id=m.duplicate_id and d.is_selected=true
          join public.product_cover_candidates t on t.product_id=m.canonical_id
            and ((d.image_url is not null and t.image_url=d.image_url)
              or (d.shop_id is not null and d.source_url is not null and t.shop_id=d.shop_id and t.source_url=d.source_url))
          where not exists (
            select 1 from public.product_cover_candidates selected
            where selected.product_id=m.canonical_id and selected.is_selected=true
          )
        )
        update public.product_cover_candidates t
        set is_selected=true, selected_at=coalesce(t.selected_at, now()), candidate_status='published'
        from selected_target s
        where t.id=s.target_id
        """
    )
    totals["cover_candidates_selected_transferred"] = cur.rowcount
    cur.execute(
        """
        update public.product_cover_candidates d
        set product_id=m.canonical_id
        from _gtin_merge_map m
        where d.product_id=m.duplicate_id
          and not exists (
            select 1 from public.product_cover_candidates t
            where t.product_id=m.canonical_id
              and (
                (d.image_url is not null and t.image_url=d.image_url)
                or (d.shop_id is not null and d.source_url is not null and t.shop_id=d.shop_id and t.source_url=d.source_url)
                or (d.is_selected=true and t.is_selected=true)
              )
          )
        """
    )
    totals["cover_candidates_reparented"] = cur.rowcount
    cur.execute(
        "delete from public.product_cover_candidates d using _gtin_merge_map m where d.product_id=m.duplicate_id"
    )
    totals["cover_candidates_deleted"] = cur.rowcount

    for table in ("cover_lookup_queue", "product_cover_queue", "product_musicbrainz_enrichment"):
        cur.execute(
            f"""
            delete from public.{table} d
            using _gtin_merge_map m
            where d.product_id=m.duplicate_id
              and exists (select 1 from public.{table} c where c.product_id=m.canonical_id)
            """
        )
        totals[f"{table}_deleted"] = cur.rowcount
        cur.execute(
            f"update public.{table} d set product_id=m.canonical_id from _gtin_merge_map m where d.product_id=m.duplicate_id"
        )
        totals[f"{table}_reparented"] = cur.rowcount

    cur.execute(
        """
        delete from public.top_deals_snapshot d
        using _gtin_merge_map m
        where d.product_id=m.duplicate_id
          and exists (
            select 1 from public.top_deals_snapshot c
            where c.snapshot_key=d.snapshot_key and c.rank=d.rank
              and c.product_id=m.canonical_id
          )
        """
    )
    totals["top_deals_deleted"] = cur.rowcount
    cur.execute(
        "update public.top_deals_snapshot d set product_id=m.canonical_id from _gtin_merge_map m where d.product_id=m.duplicate_id"
    )
    totals["top_deals_reparented"] = cur.rowcount

    child_tables = (
        "prices",
        "price_history",
        "product_artists",
        "product_cover_candidates",
        "product_cover_queue",
        "cover_lookup_queue",
        "product_masterdata_queue",
        "product_musicbrainz_enrichment",
        "release_calendar",
        "top_deals_snapshot",
        "groovespin_ean_probe_status",
    )
    for table in child_tables:
        cur.execute(
            f"select count(*) from public.{table} d join _gtin_merge_map m on d.product_id=m.duplicate_id"
        )
        remaining = int(cur.fetchone()[0])
        if remaining:
            raise RuntimeError(f"{table} still references duplicate products: {remaining}")

    cur.execute("delete from public.products p using _gtin_merge_map m where p.id=m.duplicate_id")
    totals["products_deleted"] = cur.rowcount
    if totals["products_deleted"] != len(pairs):
        raise RuntimeError(
            f"Expected to delete {len(pairs)} duplicate products, got {totals['products_deleted']}"
        )
    return totals


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-output", type=Path, default=Path("output/product_gtin_duplicate_audit.csv"))
    parser.add_argument("--snapshot-output", type=Path, default=Path("output/product_gtin_duplicate_snapshot.json"))
    parser.add_argument("--max-groups", type=int, default=0, help="Limit groups for a bounded dry-run/apply; 0 means all supported groups.")
    parser.add_argument("--batch-size", type=int, default=25, help="Commit this many pairs per transaction when applying.")
    parser.add_argument("--set-based", action="store_true", help="Use the fast set-based merge implementation.")
    parser.add_argument("--apply", action="store_true", help="Apply the supported merges in one transaction.")
    args = parser.parse_args()

    load_dotenv(".env.local", override=True)
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        raise SystemExit("DATABASE_URL ontbreekt")

    with psycopg.connect(database_url, connect_timeout=30) as conn:
        with conn.cursor() as cur:
            products = read_products(cur)
            pairs, unsupported = find_pairs(products)
            if args.max_groups > 0:
                pairs = pairs[: args.max_groups]
            write_audit(args.audit_output, pairs)
            write_snapshot(cur, args.snapshot_output, pairs)
            print(f"[AUDIT] products={len(products)} supported_pairs={len(pairs)} unsupported_groups={len(unsupported)}")
            print(f"[AUDIT] file={args.audit_output}")
            print(f"[SNAPSHOT] file={args.snapshot_output}")
            if unsupported:
                print(f"[STOP] unsupported groups present; first={unsupported[0]}")
                return 2
            if not args.apply:
                print("[DRY RUN] Geen database writes uitgevoerd")
                return 0

            if args.batch_size < 1:
                raise SystemExit("--batch-size moet minimaal 1 zijn")

            if args.set_based:
                stats = apply_set_based(cur, pairs)
                conn.commit()
                print(f"[APPLY SET-BASED] committed={len(pairs)}/{len(pairs)}", flush=True)
                print(f"[APPLY DONE] {stats}", flush=True)
                print(f"[APPLY TIME] {datetime.now(timezone.utc).isoformat()}", flush=True)
                return 0

            totals: dict[str, int] = {}
            for batch_start in range(0, len(pairs), args.batch_size):
                batch = pairs[batch_start : batch_start + args.batch_size]
                try:
                    for pair in batch:
                        stats = merge_pair(cur, pair)
                        for key, value in stats.items():
                            totals[key] = totals.get(key, 0) + value
                    conn.commit()
                except BaseException:
                    conn.rollback()
                    raise
                committed = batch_start + len(batch)
                print(f"[APPLY BATCH] committed={committed}/{len(pairs)}", flush=True)

            print(f"[APPLY DONE] {totals}", flush=True)
            print(f"[APPLY TIME] {datetime.now(timezone.utc).isoformat()}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
