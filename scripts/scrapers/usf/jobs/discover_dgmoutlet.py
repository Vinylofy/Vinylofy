from __future__ import annotations

import argparse
import csv
import tempfile
from pathlib import Path
from urllib.parse import urlparse

from scripts.scrapers.dgmoutlet import run_default
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
        f"start_page={args.start_page} max_pages={args.max_pages} "
        f"write={args.write}",
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
        f"[DISCOVER] registered inserted={result.inserted} "
        f"updated={result.updated} total={result.total}",
        flush=True,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
