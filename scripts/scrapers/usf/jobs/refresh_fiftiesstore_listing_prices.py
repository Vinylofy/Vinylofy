from __future__ import annotations

import argparse
import os
import json
import re
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from urllib.parse import urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

import psycopg
from psycopg.rows import dict_row

from scripts.scrapers.usf.core.db import load_env
from scripts.scrapers.usf.core.link_registry import insert_raw_shop_scrape, upsert_discovered_links
from scripts.scrapers.usf.core.delist_missing_links import mark_missing_links_out_of_stock
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "fiftiesstore"
START_URL = "https://www.fiftiesstore.nl/platenzaak-bennies-fifties/vinyl-albums.html"
CLERK_URL = "https://api.clerk.io/v2/recommendations/category/popular"
CLERK_KEY = "KSYUwL4HkXqXANG9JpdJVMCJzvklz8NT"
CATEGORY_ID = 1530

EAN13_RE = re.compile(r"^\d{13}$")


def clean(v):
    return re.sub(r"\s+", " ", str(v or "")).strip()


def normalize_url(url):
    if not url:
        return None
    p = urlparse(url)
    return urlunparse(("https", p.netloc.lower(), p.path, "", "", ""))


def price(v):
    if v is None or v is False:
        return None
    return str(Decimal(str(v)).quantize(Decimal("0.01")))


def ean_from_sku(sku):
    s = clean(sku)
    return s if EAN13_RE.match(s) else None


def source_product_id(item):
    return str(item.get("id") or item.get("sku") or "")



def ensure_scrape_run(run_id, total_expected=None):
    db = os.environ.get("DATABASE_URL")
    if not db:
        return

    with psycopg.connect(db, prepare_threshold=None) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                select column_name, is_nullable, column_default
                from information_schema.columns
                where table_schema = 'public'
                  and table_name = 'scrape_runs'
            """)
            cols = {r["column_name"]: r for r in cur.fetchall()}

            values = {}

            for c in ["id", "run_id"]:
                if c in cols:
                    values[c] = run_id

            for c in ["shop_id", "shop"]:
                if c in cols:
                    values[c] = SHOP_ID

            for c in ["source", "scraper", "scraper_name", "job_name"]:
                if c in cols:
                    values[c] = "refresh_fiftiesstore_listing_prices"

            if "job_type" in cols:
                values["job_type"] = "listing_refresh"

            for c in ["status", "run_status", "state"]:
                if c in cols:
                    values[c] = "running"

            now_cols = ["started_at", "created_at", "run_started_at"]
            for c in now_cols:
                if c in cols:
                    values[c] = datetime.now(timezone.utc)

            for c in ["total_expected", "expected_total", "total_items", "discovered_total"]:
                if c in cols and total_expected is not None:
                    values[c] = int(total_expected)

            missing_required = [
                c for c, meta in cols.items()
                if meta["is_nullable"] == "NO"
                and meta["column_default"] is None
                and c not in values
            ]

            if missing_required:
                raise RuntimeError(f"scrape_runs required columns not handled: {missing_required}")

            columns = list(values)
            placeholders = ["%s"] * len(columns)
            sql = f"""
                insert into public.scrape_runs ({",".join(columns)})
                values ({",".join(placeholders)})
                on conflict do nothing
            """
            cur.execute(sql, [values[c] for c in columns])
        conn.commit()


def finish_scrape_run(run_id, status="completed"):
    db = os.environ.get("DATABASE_URL")
    if not db:
        return

    with psycopg.connect(db, prepare_threshold=None) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("""
                select column_name
                from information_schema.columns
                where table_schema = 'public'
                  and table_name = 'scrape_runs'
            """)
            cols = {r["column_name"] for r in cur.fetchall()}

            sets = []
            vals = []

            for c in ["status", "run_status", "state"]:
                if c in cols:
                    sets.append(f"{c} = %s")
                    vals.append(status)

            for c in ["finished_at", "completed_at", "run_finished_at", "updated_at"]:
                if c in cols:
                    sets.append(f"{c} = %s")
                    vals.append(datetime.now(timezone.utc))

            id_col = "id" if "id" in cols else "run_id" if "run_id" in cols else None
            if sets and id_col:
                vals.append(run_id)
                cur.execute(f"update public.scrape_runs set {', '.join(sets)} where {id_col} = %s", vals)
        conn.commit()



def fetch_batch(offset, limit):
    payload = {
        "offset": offset,
        "category": CATEGORY_ID,
        "limit": limit,
        "key": CLERK_KEY,
        "visitor": "auto",
        "language": "dutch",
        "attributes": [
            "id", "name", "url", "image", "price", "list_price",
            "brand", "manufacturer", "categories", "sku"
        ],
    }
    qs = urlencode({
        "payload": json.dumps(payload, separators=(",", ":")),
        "callback": "cb",
    })
    req = Request(
        CLERK_URL + "?" + qs,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": START_URL,
        },
    )
    text = urlopen(req, timeout=45).read().decode("utf-8", "replace")
    if text.startswith("cb("):
        text = text[3:].rstrip(");")
    data = json.loads(text)
    return data


def main():
    ap = argparse.ArgumentParser(description="Refresh FiftiesStore/Bennies via Clerk API.")
    ap.add_argument("--batch-size", type=int, default=250)
    ap.add_argument("--max-products", type=int, default=0, help="0 = all")
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    load_env()

    run_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    offset = 0
    total = None
    raw_rows = []
    links = []

    while True:
        data = fetch_batch(offset, args.batch_size)
        total = data.get("count", total)
        products = data.get("product_data") or []

        if not products:
            break

        for pos, item in enumerate(products, offset + 1):
            url = normalize_url(item.get("url"))
            if not url:
                continue

            sku = clean(item.get("sku"))
            current_price = price(item.get("price"))
            list_price = price(item.get("list_price"))
            ean = ean_from_sku(sku)

            payload = {
                "shop_id": SHOP_ID,
                "source_url": url,
                "source_product_id": source_product_id(item),
                "discovery_source": "clerk_category_api",
                "discovery_url": START_URL,
                "listing_position": pos,
                "title_raw": clean(item.get("name")),
                "artist_raw": clean(item.get("brand")),
                "title_clean": clean(item.get("name")),
                "sku": sku,
                "ean": ean,
                "price": current_price,
                "original_price": list_price if list_price != current_price else None,
                "compare_at_price": list_price if list_price != current_price else None,
                "is_sale": bool(list_price and current_price and Decimal(list_price) > Decimal(current_price)),
                "availability": "in_stock",
                "image_url": item.get("image"),
                "categories": item.get("categories"),
                "listing_seen_at": now,
                "price_source": "clerk_api",
            }

            raw_rows.append(payload)
            links.append(
                DiscoveredLink(
                    shop_id=SHOP_ID,
                    source_url=url,
                    source_product_id=payload["source_product_id"],
                    payload=payload,
                )
            )

            if args.max_products and len(raw_rows) >= args.max_products:
                break

        print("[FIFTIES-API] batch", {
            "offset": offset,
            "got": len(products),
            "collected": len(raw_rows),
            "total": total,
        }, flush=True)

        if args.max_products and len(raw_rows) >= args.max_products:
            break

        offset += len(products)

        if total is not None and offset >= int(total):
            break

        time.sleep(args.sleep)

    stats = {
        "total_api_count": total,
        "collected": len(raw_rows),
        "with_ean_from_sku": sum(1 for r in raw_rows if r.get("ean")),
        "with_price": sum(1 for r in raw_rows if r.get("price")),
        "sale": sum(1 for r in raw_rows if r.get("is_sale")),
        "write": args.write,
    }

    print("[FIFTIES-API] stats", stats, flush=True)
    print("[FIFTIES-API] sample", json.dumps(raw_rows[:5], ensure_ascii=False, indent=2), flush=True)

    if not args.write:
        return 0

    ensure_scrape_run(run_id, total_expected=len(raw_rows))

    reg = upsert_discovered_links(links)
    print("[FIFTIES-API] registry", vars(reg), flush=True)

    inserted = 0
    for r in raw_rows:
        insert_raw_shop_scrape(
            run_id=run_id,
            shop_id=SHOP_ID,
            source_url=r["source_url"],
            source_product_id=r.get("source_product_id"),
            title_raw=r.get("title_raw"),
            ean_raw=r.get("ean"),
            price_raw=r.get("price"),
            availability_raw=r.get("availability"),
            image_url_raw=r.get("image_url"),
            payload=r,
        )
        inserted += 1

    print("[FIFTIES-API] raw", {"inserted": inserted, "run_id": run_id}, flush=True)

    delist_stats = mark_missing_links_out_of_stock(
        shop_id=SHOP_ID,
        seen_source_urls=[r["source_url"] for r in raw_rows],
        run_started_at=datetime.now(timezone.utc),
        write=True,
    )
    print("[FIFTIES-API] delist_missing", delist_stats, flush=True)

    finish_scrape_run(run_id, "completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
