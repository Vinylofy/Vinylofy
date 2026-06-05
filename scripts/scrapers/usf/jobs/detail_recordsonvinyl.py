#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time

import requests

from scripts.scrapers.usf.core.link_registry import (
    get_links_for_detail_scrape,
    insert_raw_shop_scrape,
    mark_detail_scraped,
)


SHOP_ID = "recordsonvinyl"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Fetch RecordsonVinyl detail pages into raw_shop_scrapes")
    p.add_argument("--limit", type=int, default=5)
    p.add_argument("--sleep", type=float, default=1.5)
    return p


def extract_title(html: str) -> str | None:
    m = re.search(r"<title>(.*?)</title>", html, flags=re.I | re.S)
    if not m:
        return None
    return re.sub(r"\s+", " ", m.group(1)).strip()


def main() -> int:
    args = build_parser().parse_args()
    links = get_links_for_detail_scrape(SHOP_ID, limit=args.limit)

    print(f"[DETAIL] queued={len(links)}", flush=True)

    session = requests.Session()

    for idx, link in enumerate(links, start=1):
        url = link["source_url"]
        print(f"[DETAIL] {idx}/{len(links)} {url}", flush=True)

        response = session.get(url, timeout=30)

        if response.status_code == 429:
            print("[WARN] HTTP 429, stopping safely.", flush=True)
            break

        if response.status_code in (404, 410):
            print(f"[DETAIL][SKIP] dead link status={response.status_code} url={url}")
            mark_detail_scraped(link["id"])
            continue

        try:
            response.raise_for_status()
        except requests.RequestException as exc:
            print(f"[DETAIL][WARN] HTTP error url={url} error={exc}")
            continue

        html = response.text
        title = extract_title(html)

        raw_id = insert_raw_shop_scrape(
            run_id=None,
            shop_id=SHOP_ID,
            source_url=url,
            source_product_id=link["source_product_id"],
            title_raw=title,
            ean_raw=None,
            price_raw=None,
            availability_raw=None,
            image_url_raw=None,
            payload={
                "html_length": len(html),
                "status_code": response.status_code,
                "source": "detail_recordsonvinyl",
            },
        )

        mark_detail_scraped(link["id"])
        print(f"[DETAIL] stored raw_id={raw_id}", flush=True)

        time.sleep(args.sleep)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
