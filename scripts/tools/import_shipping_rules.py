#!/usr/bin/env python3
import argparse
import csv
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import psycopg


REQUIRED = [
    "shop_slug",
    "country_code",
    "currency",
    "shipping_cost_cents",
    "free_shipping_threshold_cents",
    "shipping_logic",
    "confidence",
    "active",
]


def clean(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value else None


def to_int(value):
    value = clean(value)
    return int(value) if value is not None else None


def to_bool(value):
    return str(value).strip().lower() in ("true", "1", "yes", "ja")


class ShopResolutionError(ValueError):
    """A shipping row cannot be mapped to exactly one existing shop."""


def source_domain(source_url):
    parsed = urlparse(clean(source_url) or "")
    hostname = parsed.hostname
    return hostname.lower().rstrip(".") if hostname else None


def domain_candidates(domain):
    """Return safe candidates for a shop's www/non-www hostname variant."""
    canonical = domain[4:] if domain.startswith("www.") else domain
    return sorted({domain, canonical, f"www.{canonical}"})


def resolve_shop_id(cur, row):
    domain = source_domain(row.get("source_url"))
    if not domain:
        raise ShopResolutionError(
            f"shippingregel heeft geen bruikbaar source_url: "
            f"shop_slug={row.get('shop_slug')!r}"
        )

    cur.execute(
        """
        select id
        from public.shops
        where lower(domain) = any(%s)
        """,
        (domain_candidates(domain),),
    )
    matches = cur.fetchall()
    if len(matches) != 1:
        state = "ontbreekt" if not matches else "ambigu"
        raise ShopResolutionError(
            f"geen unieke bestaande shop voor shippingregel: "
            f"shop_slug={row.get('shop_slug')!r}, domain={domain!r}, "
            f"status={state}, matches={len(matches)}"
        )
    return matches[0][0]


def build_payload(cur, rows):
    payload = []
    for row in rows:
        payload.append({
            "shop_id": resolve_shop_id(cur, row),
            "shop_slug": clean(row.get("shop_slug")),
            "shop_name": clean(row.get("shop_name")),
            "country_code": clean(row.get("country_code")) or "NL",
            "currency": clean(row.get("currency")) or "EUR",
            "shipping_cost_cents": to_int(row.get("shipping_cost_cents")),
            "free_shipping_threshold_cents": to_int(row.get("free_shipping_threshold_cents")),
            "shipping_logic": clean(row.get("shipping_logic")) or "threshold",
            "shipping_note": clean(row.get("shipping_note")),
            "confidence": clean(row.get("confidence")) or "verified",
            "source_url": clean(row.get("source_url")),
            "source_url_2": clean(row.get("source_url_2")),
            "verified_at": clean(row.get("verified_at")),
            "active": to_bool(row.get("active", "true")),
        })
    return payload


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()

    csv_path = Path(args.input)
    if not csv_path.exists():
        print(f"[ERROR] file not found: {csv_path}", file=sys.stderr)
        return 1

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("[ERROR] DATABASE_URL env var is required", file=sys.stderr)
        return 1

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("[ERROR] CSV is empty", file=sys.stderr)
        return 1

    malformed_rows = [
        line_number
        for line_number, row in enumerate(rows, start=2)
        if None in row
    ]
    if malformed_rows:
        print(
            f"[ERROR] CSV bevat extra velden op regels: {malformed_rows}",
            file=sys.stderr,
        )
        return 1

    missing = [c for c in REQUIRED if c not in rows[0]]
    if missing:
        print(f"[ERROR] missing columns: {missing}", file=sys.stderr)
        return 1

    sql = """
      insert into public.shop_shipping_rules (
      shop_id,
      shop_slug,
      shop_name,
      country_code,
      currency,
      shipping_cost_cents,
      free_shipping_threshold_cents,
      shipping_logic,
      shipping_note,
      confidence,
      source_url,
      source_url_2,
      verified_at,
      active
    )
    values (
      %(shop_id)s,
      %(shop_slug)s,
      %(shop_name)s,
      %(country_code)s,
      %(currency)s,
      %(shipping_cost_cents)s,
      %(free_shipping_threshold_cents)s,
      %(shipping_logic)s,
      %(shipping_note)s,
      %(confidence)s,
      %(source_url)s,
      %(source_url_2)s,
      %(verified_at)s,
      %(active)s
    )
    on conflict (shop_slug, country_code)
    do update set
      shop_id = excluded.shop_id,
      shop_name = excluded.shop_name,
      currency = excluded.currency,
      shipping_cost_cents = excluded.shipping_cost_cents,
      free_shipping_threshold_cents = excluded.free_shipping_threshold_cents,
      shipping_logic = excluded.shipping_logic,
      shipping_note = excluded.shipping_note,
      confidence = excluded.confidence,
      source_url = excluded.source_url,
      source_url_2 = excluded.source_url_2,
      verified_at = excluded.verified_at,
      active = excluded.active,
      rules_version = public.shop_shipping_rules.rules_version + 1,
      updated_at = now()
    """

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            payload = build_payload(cur, rows)
            cur.executemany(sql, payload)
        conn.commit()

    print(f"[IMPORT] upserted={len(payload)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
