#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from urllib.parse import urljoin

import requests

from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink


SHOP_ID = "recordsonvinyl"
BASE_URL = "https://recordsonvinyl.nl"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Discover RecordsonVinyl product links into Supabase")
    p.add_argument("--start-page", type=int, default=1)
    p.add_argument("--max-pages", type=int, default=5)
    p.add_argument("--sleep", type=float, default=1.5)
    return p


def extract_product_links(html: str) -> list[str]:
    hrefs = re.findall(r'href=["\']([^"\']*/products/[^"\']+)["\']', html)
    urls: list[str] = []

    for href in hrefs:
        url = urljoin(BASE_URL, href.split("?")[0])
        if url not in urls:
            urls.append(url)

    return urls


def main() -> int:
    args = build_parser().parse_args()

    session = requests.Session()
    all_links: list[DiscoveredLink] = []

    for page in range(args.start_page, args.start_page + args.max_pages):
        url = f"{BASE_URL}/collections/all?page={page}"
        print(f"[DISCOVER] page={page} url={url}", flush=True)

        response = session.get(url, timeout=30)
        if response.status_code == 429:
            print("[WARN] HTTP 429, stopping discovery safely.", flush=True)
            break

        response.raise_for_status()

        urls = extract_product_links(response.text)
        print(f"[DISCOVER] page={page} links={len(urls)}", flush=True)

        if not urls:
            print(f"[DISCOVER] page={page} no links, stopping.", flush=True)
            break

        for product_url in urls:
            handle = product_url.rstrip("/").split("/")[-1]
            all_links.append(
                DiscoveredLink(
                    shop_id=SHOP_ID,
                    source_url=product_url,
                    source_product_id=handle,
                    payload={"discovery_url": url, "page": page},
                )
            )

        time.sleep(args.sleep)

    result = upsert_discovered_links(all_links)
    print(f"[DISCOVER] registered inserted={result.inserted} updated={result.updated} total={result.total}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
