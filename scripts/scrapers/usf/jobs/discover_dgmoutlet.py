from __future__ import annotations

import argparse
import csv
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from scripts.scrapers.dgmoutlet import run_default
from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "dgmoutlet"


def clean(value: object) -> str:
    return str(value or "").strip()


def extract_source_product_id(source_url: str) -> str | None:
    path = urlparse(source_url).path.rstrip("/")
    if not path:
        return None
    value = path.split("/")[-1].strip()
    return value or None


def build_payload(row: dict[str, str]) -> dict[str, object]:
    payload: dict[str, object] = {
        "discovery_source": "dgmoutlet_lp_listing",
    }

    page = clean(row.get("page"))
    if page:
        try:
            payload["page"] = int(page)
        except ValueError:
            payload["page"] = page

    field_mapping = {
        "ean": "ean",
        "price_current": "price_current",
        "price_original": "price_original",
        "artist": "artist",
        "title": "title",
        "format": "format",
        "raw_name": "raw_name",
        "description_snippet": "description_snippet",
        "image_url": "image_url",
        "image_source_page_url": "image_source_page_url",
        "image_source_type": "image_source_type",
        "availability": "availability",
        "availability_source": "availability_source",
        "scraped_at": "scraped_at",
    }

    for source_field, payload_field in field_mapping.items():
        value = clean(row.get(source_field))
        if value:
            payload[payload_field] = value

    return payload


def read_discovered_links(csv_path: Path) -> list[DiscoveredLink]:
    links_by_url: dict[str, DiscoveredLink] = {}

    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            source_url = clean(row.get("url"))
            if not source_url:
                continue

            links_by_url[source_url] = DiscoveredLink(
                shop_id=SHOP_ID,
                source_url=source_url,
                source_product_id=extract_source_product_id(source_url),
                payload=build_payload(row),
            )

    return list(links_by_url.values())


def mark_public_prices_oos_for_links(cur, *, link_filter_sql: str, params: tuple[object, ...]) -> int:
    cur.execute(
        """
        select id
        from public.shops
        where lower(domain) in ('dgmoutlet.nl', 'www.dgmoutlet.nl')
           or lower(name) in ('dgm outlet', 'dgmoutlet')
        order by created_at asc nulls last
        limit 1
        """
    )
    row = cur.fetchone()
    if row is None:
        print("[DGM-BULK-DELIST-SKIP] reason=public_shop_not_found", flush=True)
        return 0

    public_shop_id = row[0]

    cur.execute(
        f"""
        update public.prices p
        set availability = 'out_of_stock',
            is_active = false,
            updated_at = now()
        where p.shop_id = %s
          and p.product_url in (
            select source_url
            from public.shop_product_links
            where shop_id = %s
              and {link_filter_sql}
          )
          and (
            p.is_active is distinct from false
            or p.availability is distinct from 'out_of_stock'
          )
        """,
        (public_shop_id, SHOP_ID, *params),
    )
    return int(cur.rowcount or 0)


def cleanup_dgm_availability_after_discovery(
    *,
    started_at: datetime,
    links: list[DiscoveredLink],
    full_discovery: bool,
    max_unseen_ratio: float = 0.60,
    min_links_for_bulk_cleanup: int = 25,
) -> None:
    if not links:
        print("[DGM-BULK-DELIST-SKIP] reason=no_links", flush=True)
        return

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update public.shop_product_links
                set status = 'inactive'
                where shop_id = %s
                  and last_seen_at >= %s
                  and payload->>'availability' = 'out_of_stock'
                """,
                (SHOP_ID, started_at),
            )
            explicit_oos_links = int(cur.rowcount or 0)

            explicit_oos_prices = mark_public_prices_oos_for_links(
                cur,
                link_filter_sql="payload->>'availability' = 'out_of_stock'",
                params=(),
            )

            if explicit_oos_links or explicit_oos_prices:
                print(
                    "[DGM-SEEN-OOS] "
                    f"explicit_oos_links={explicit_oos_links} "
                    f"prices_marked_oos={explicit_oos_prices}",
                    flush=True,
                )

            if not full_discovery:
                print("[DGM-BULK-DELIST-SKIP] reason=partial_discovery unseen_cleanup=false", flush=True)
                return

            if len(links) < min_links_for_bulk_cleanup:
                print(
                    "[DGM-BULK-DELIST-SKIP] "
                    f"reason=too_few_links links={len(links)} min_links={min_links_for_bulk_cleanup}",
                    flush=True,
                )
                return

            cur.execute(
                """
                select count(*)
                from public.shop_product_links
                where shop_id = %s
                  and status = 'active'
                """,
                (SHOP_ID,),
            )
            active_count = int(cur.fetchone()[0] or 0)

            cur.execute(
                """
                select count(*)
                from public.shop_product_links
                where shop_id = %s
                  and status = 'active'
                  and last_seen_at < %s
                """,
                (SHOP_ID, started_at),
            )
            unseen_count = int(cur.fetchone()[0] or 0)

            unseen_ratio = (unseen_count / active_count) if active_count else 0.0
            if active_count and unseen_ratio > max_unseen_ratio:
                print(
                    "[DGM-BULK-DELIST-SKIP] "
                    f"reason=extreme_drop active_count={active_count} "
                    f"unseen_count={unseen_count} unseen_ratio={unseen_ratio:.2f} "
                    f"max_unseen_ratio={max_unseen_ratio:.2f}",
                    flush=True,
                )
                return

            cur.execute(
                """
                update public.shop_product_links
                set status = 'inactive'
                where shop_id = %s
                  and status = 'active'
                  and last_seen_at < %s
                """,
                (SHOP_ID, started_at),
            )
            links_deactivated = int(cur.rowcount or 0)

            prices_deactivated = mark_public_prices_oos_for_links(
                cur,
                link_filter_sql="status = 'inactive'",
                params=(),
            )

            print(
                "[DGM-BULK-DELIST-DONE] "
                f"links_seen={len(links)} active_count_before_cleanup={active_count} "
                f"unseen_count={unseen_count} links_deactivated={links_deactivated} "
                f"prices_marked_oos={prices_deactivated}",
                flush=True,
            )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Discover DGM Outlet listing products and optionally register "
            "their links in the USF shop_product_links registry."
        )
    )
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Aantal listingpagina's. Gebruik 0 om door te lopen tot de eerste lege pagina.",
    )
    parser.add_argument("--delay", type=float, default=0.0)
    parser.add_argument(
        "--write",
        action="store_true",
        help="Schrijf links naar shop_product_links. Zonder deze vlag is dit een dry-run.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.start_page < 1:
        raise SystemExit("[ERROR] --start-page moet minimaal 1 zijn.")
    if args.max_pages < 0:
        raise SystemExit("[ERROR] --max-pages mag niet negatief zijn.")

    max_pages = None if args.max_pages == 0 else args.max_pages
    discovery_started_at = datetime.now(timezone.utc)

    with tempfile.TemporaryDirectory(prefix="vinylofy-dgm-discovery-") as temp_dir:
        csv_path = Path(temp_dir) / "dgmoutlet_discovery.csv"

        run_default(
            output_path=csv_path,
            start_page=args.start_page,
            max_pages=max_pages,
            delay=args.delay,
        )

        links = read_discovered_links(csv_path)

        print(
            f"[DISCOVER] shop={SHOP_ID} links={len(links)} "
            f"start_page={args.start_page} max_pages={args.max_pages} write={args.write}",
            flush=True,
        )

        for link in links[:5]:
            print(
                "[SAMPLE]",
                {
                    "source_url": link.source_url,
                    "source_product_id": link.source_product_id,
                    "ean": link.payload.get("ean"),
                    "price_current": link.payload.get("price_current"),
                    "availability": link.payload.get("availability"),
                    "availability_source": link.payload.get("availability_source"),
                    "page": link.payload.get("page"),
                },
                flush=True,
            )

        if not links:
            raise SystemExit("[ERROR] DGM discovery leverde geen productlinks op.")

        if not args.write:
            print("[DISCOVER] dry-run complete; geen databasewrites.", flush=True)
            return 0

        result = upsert_discovered_links(links)

        print(
            f"[DISCOVER] registered inserted={result.inserted} updated={result.updated} total={result.total}",
            flush=True,
        )

        cleanup_dgm_availability_after_discovery(
            started_at=discovery_started_at,
            links=links,
            full_discovery=max_pages is None,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
