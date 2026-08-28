from __future__ import annotations

import argparse
import json
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from typing import Callable, Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

from scripts.importers.common import normalize_text
from scripts.scrapers.usf.core.db import db_connection

SOURCE_SHOP = "upcomingvinyl"
BASE_URL = "https://upcomingvinyl.com"
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_MAX_PAGES = 50
DEFAULT_MAX_ATTEMPTS = 3
RETRYABLE_STATUSES = {429, 502, 503, 504}
RECORD_PATH_RE = re.compile(r"^https://upcomingvinyl\.com/record/[a-z0-9][a-z0-9\-]*$", re.I)
FORMAT_SUFFIX_RE = re.compile(r"\s*\[([^\]]+)\]\s*$")

MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


@dataclass(frozen=True)
class ReleaseItem:
    artist: str
    title: str
    release_date: date
    source_url: str
    format: str | None = None
    source_payload: dict[str, object] | None = None


@dataclass
class PageStats:
    url: str
    raw_records: int = 0
    parsed_records: int = 0
    new_records: int = 0
    duplicate_records: int = 0
    skips: Counter[str] = field(default_factory=Counter)
    min_release_date: date | None = None
    max_release_date: date | None = None


@dataclass(frozen=True)
class ParsedPage:
    items: list[ReleaseItem]
    stats: PageStats
    next_url: str | None
    record_urls: frozenset[str]


@dataclass
class DiscoveryResult:
    items: list[ReleaseItem]
    page_stats: list[PageStats]
    stop_reason: str


def clean(value: object) -> str:
    return normalize_text(value) or ""


def listing_url(page: int) -> str:
    if page <= 1:
        return BASE_URL
    return f"{BASE_URL}/releases?page={page}"


def parse_release_date(value: object) -> date | None:
    text = clean(value)
    parts = re.match(r"^([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})$", text)
    if not parts:
        return None

    month = MONTHS.get(parts.group(1).lower())
    if month is None:
        return None

    try:
        return date(int(parts.group(3)), month, int(parts.group(2)))
    except ValueError:
        return None


def title_and_format(value: object) -> tuple[str | None, str | None]:
    title = clean(value)
    if not title:
        return None, None

    format_label = None
    match = FORMAT_SUFFIX_RE.search(title)
    if match:
        format_label = clean(match.group(1)) or None
        title = title[: match.start()].strip()

    return title or None, format_label


def artist_from_heading(heading: Tag) -> str | None:
    parts: list[str] = []
    for child in heading.children:
        if isinstance(child, NavigableString):
            parts.append(str(child))
        elif isinstance(child, Tag) and child.name != "span":
            parts.append(child.get_text(" ", strip=True))
    return clean(" ".join(parts)) or None


def item_from_card(card: Tag, release_date: date, *, listing_page_url: str) -> ReleaseItem | None:
    anchor = card.find("a", href=True)
    if not isinstance(anchor, Tag):
        return None

    source_url = urljoin(BASE_URL, clean(anchor.get("href")).split("#", 1)[0])
    if not RECORD_PATH_RE.match(source_url):
        return None

    heading = anchor.find("h2")
    if not isinstance(heading, Tag):
        return None

    artist = artist_from_heading(heading)
    title_node = heading.find("span")
    title, format_label = title_and_format(title_node.get_text(" ", strip=True) if title_node else "")

    if not artist or not title:
        return None

    return ReleaseItem(
        artist=artist,
        title=title,
        release_date=release_date,
        source_url=source_url,
        format=format_label,
        source_payload={
            "source": "upcomingvinyl_release_listing",
            "listing_page_url": listing_page_url,
            "ean_status": "not_provided",
            "calendar_only": True,
        },
    )


def parse_listing_page(
    html: str,
    *,
    listing_page_url: str,
    min_date: date | None,
    global_seen_urls: set[str],
) -> ParsedPage:
    soup = BeautifulSoup(html, "html.parser")
    stats = PageStats(url=listing_page_url)
    items: list[ReleaseItem] = []
    page_record_urls: set[str] = set()
    current_date: date | None = None

    main = soup.find("main") or soup
    for node in main.children:
        if not isinstance(node, Tag):
            continue

        if "page-heading" in node.get("class", []):
            date_node = node.select_one("h1 span")
            current_date = parse_release_date(date_node.get_text(" ", strip=True) if date_node else "")
            continue

        if node.name != "ul" or "record-grid" not in node.get("class", []):
            continue

        if current_date is None:
            continue
        if min_date is not None and current_date < min_date:
            continue

        for card in node.find_all("li", recursive=False):
            stats.raw_records += 1
            item = item_from_card(card, current_date, listing_page_url=listing_page_url)
            if item is None:
                stats.skips["parse_failed"] += 1
                continue

            page_record_urls.add(item.source_url)
            if item.source_url in global_seen_urls:
                stats.duplicate_records += 1
                continue

            global_seen_urls.add(item.source_url)
            items.append(item)
            stats.parsed_records += 1
            stats.new_records += 1
            stats.min_release_date = (
                item.release_date
                if stats.min_release_date is None
                else min(stats.min_release_date, item.release_date)
            )
            stats.max_release_date = (
                item.release_date
                if stats.max_release_date is None
                else max(stats.max_release_date, item.release_date)
            )

    load_more = soup.select_one("a#load-more[href]")
    next_url = (
        urljoin(BASE_URL, clean(load_more.get("href")).split("#", 1)[0])
        if isinstance(load_more, Tag)
        else None
    )

    return ParsedPage(
        items=items,
        stats=stats,
        next_url=next_url,
        record_urls=frozenset(page_record_urls),
    )


def fetch(session: requests.Session, url: str, *, timeout: float) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; Vinylofy-UpcomingVinyl-ReleaseDiscovery/1.0; "
            "+https://vinylofy.com)"
        )
    }

    for attempt in range(1, DEFAULT_MAX_ATTEMPTS + 1):
        response = session.get(url, timeout=timeout, allow_redirects=True, headers=headers)
        if response.status_code == 200:
            return response.text

        if response.status_code not in RETRYABLE_STATUSES or attempt == DEFAULT_MAX_ATTEMPTS:
            response.raise_for_status()

        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            delay = min(float(retry_after), 30.0)
        else:
            delay = min(2.0 * attempt, 10.0)
        time.sleep(delay)

    raise RuntimeError(f"UpcomingVinyl fetch failed unexpectedly: {url}")


def discover_release_items(
    *,
    max_pages: int,
    delay_seconds: float,
    timeout_seconds: float,
    min_date: date | None = None,
    fetch_page: Callable[[str], str] | None = None,
) -> DiscoveryResult:
    items: list[ReleaseItem] = []
    page_stats: list[PageStats] = []
    global_seen_urls: set[str] = set()
    previous_page_urls: frozenset[str] | None = None
    next_url: str | None = listing_url(1)
    stop_reason = "max_pages"

    with requests.Session() as session:
        for page in range(1, max_pages + 1):
            if not next_url:
                stop_reason = "no_next_page"
                break

            html = fetch_page(next_url) if fetch_page else fetch(session, next_url, timeout=timeout_seconds)
            parsed = parse_listing_page(
                html,
                listing_page_url=next_url,
                min_date=min_date,
                global_seen_urls=global_seen_urls,
            )

            page_stats.append(parsed.stats)
            items.extend(parsed.items)
            print("[RELEASE-UPCOMINGVINYL-PAGE]", {
                "page": page,
                "url": next_url,
                "raw_records": parsed.stats.raw_records,
                "parsed_records": parsed.stats.parsed_records,
                "new_records": parsed.stats.new_records,
                "duplicate_records": parsed.stats.duplicate_records,
                "skips": dict(parsed.stats.skips),
                "min_release_date": parsed.stats.min_release_date.isoformat() if parsed.stats.min_release_date else None,
                "max_release_date": parsed.stats.max_release_date.isoformat() if parsed.stats.max_release_date else None,
                "next_url": parsed.next_url,
            }, flush=True)

            if not parsed.record_urls:
                stop_reason = "no_records"
                break
            if previous_page_urls is not None and parsed.record_urls.issubset(previous_page_urls):
                stop_reason = "duplicate_page"
                break
            if not parsed.next_url:
                stop_reason = "no_next_page"
                break

            previous_page_urls = parsed.record_urls
            next_url = parsed.next_url
            if delay_seconds > 0:
                time.sleep(delay_seconds)

    return DiscoveryResult(items=items, page_stats=page_stats, stop_reason=stop_reason)


def source_key(item: ReleaseItem) -> str:
    return "::".join(
        [
            item.release_date.isoformat(),
            item.artist.casefold(),
            item.title.casefold(),
        ]
    )


def upsert_release(cur, item: ReleaseItem) -> bool:
    payload = dict(item.source_payload or {})
    payload["source_key"] = source_key(item)

    params = {
        "ean": None,
        "artist": item.artist,
        "title": item.title,
        "release_date": item.release_date.isoformat(),
        "source_shop": SOURCE_SHOP,
        "source_url": item.source_url,
        "image_source_url": None,
        "format": item.format,
        "label": None,
        "product_id": None,
        "source_payload": json.dumps(payload, ensure_ascii=False),
    }

    cur.execute(
        """
        select id
        from public.release_calendar
        where source_url = %(source_url)s
           or (
                source_shop = %(source_shop)s
                and ean is null
                and release_date = %(release_date)s::date
                and lower(artist) = lower(%(artist)s)
                and lower(title) = lower(%(title)s)
           )
        order by id
        for update
        """,
        params,
    )
    existing_rows = cur.fetchall()
    if len(existing_rows) > 1:
        raise RuntimeError(
            "UpcomingVinyl release-identiteitsconflict: source_url en "
            "(source_shop, release_date, artist, title) verwijzen naar "
            "verschillende release_calendar-rijen."
        )

    if existing_rows:
        params["existing_id"] = existing_rows[0][0]
        cur.execute(
            """
            update public.release_calendar
            set
                artist = %(artist)s,
                title = %(title)s,
                release_date = %(release_date)s::date,
                format = coalesce(%(format)s, public.release_calendar.format),
                label = public.release_calendar.label,
                product_id = public.release_calendar.product_id,
                status = 'active',
                confidence = greatest(public.release_calendar.confidence, 80),
                source_payload = (
                    public.release_calendar.source_payload || %(source_payload)s::jsonb
                ),
                last_seen_at = now(),
                updated_at = now()
            where id = %(existing_id)s
            """,
            params,
        )
        return False

    cur.execute(
        """
        insert into public.release_calendar (
            ean,
            artist,
            title,
            release_date,
            source_shop,
            source_url,
            image_source_url,
            format,
            label,
            product_id,
            status,
            confidence,
            source_payload,
            first_seen_at,
            last_seen_at,
            created_at,
            updated_at
        )
        values (
            %(ean)s,
            %(artist)s,
            %(title)s,
            %(release_date)s::date,
            %(source_shop)s,
            %(source_url)s,
            %(image_source_url)s,
            %(format)s,
            %(label)s,
            %(product_id)s,
            'active',
            80,
            %(source_payload)s::jsonb,
            now(),
            now(),
            now(),
            now()
        )
        """,
        params,
    )
    return True


def write_releases(items: Iterable[ReleaseItem], *, write: bool) -> dict[str, int]:
    stats = {
        "items": 0,
        "inserted": 0,
        "updated": 0,
        "databasewrites": int(write),
    }

    if not write:
        for _ in items:
            stats["items"] += 1
        return stats

    with db_connection() as conn:
        with conn.cursor() as cur:
            for item in items:
                stats["items"] += 1
                inserted = upsert_release(cur, item)
                if inserted:
                    stats["inserted"] += 1
                else:
                    stats["updated"] += 1
        conn.commit()

    return stats


def run(args: argparse.Namespace) -> int:
    min_date = None if args.include_past else date.today()
    result = discover_release_items(
        max_pages=args.max_pages,
        delay_seconds=args.sleep,
        timeout_seconds=args.timeout,
        min_date=min_date,
    )
    write_stats = write_releases(result.items, write=args.write)
    summary = {
        "source": SOURCE_SHOP,
        "items": len(result.items),
        "pages": len(result.page_stats),
        "stop_reason": result.stop_reason,
        "write": args.write,
        "write_stats": write_stats,
        "page_stats": [
            {
                "url": stats.url,
                "raw_records": stats.raw_records,
                "parsed_records": stats.parsed_records,
                "new_records": stats.new_records,
                "duplicate_records": stats.duplicate_records,
                "skips": dict(stats.skips),
                "min_release_date": stats.min_release_date.isoformat() if stats.min_release_date else None,
                "max_release_date": stats.max_release_date.isoformat() if stats.max_release_date else None,
            }
            for stats in result.page_stats
        ],
    }
    print("[RELEASE-UPCOMINGVINYL] summary", json.dumps(summary, ensure_ascii=False), flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--sleep", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--include-past", action="store_true")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
