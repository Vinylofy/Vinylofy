from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from html import unescape
from typing import Iterable
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup

from scripts.scrapers.usf.core.db import db_connection


SOURCE_SHOP = "bobsvinyl"
BASE_URL = "https://bobsvinyl.nl"
EAN_RE = re.compile(r"\b(?:\d{8}|\d{12,14})\b")


@dataclass
class ReleaseItem:
    ean: str | None
    artist: str
    title: str
    release_date: date
    source_url: str
    image_source_url: str | None = None
    format: str | None = None
    label: str | None = None
    source_payload: dict | None = None


def upcoming_fridays(past_weeks: int, future_weeks: int) -> list[date]:
    today = date.today()
    this_friday = today + timedelta(days=(4 - today.weekday()) % 7)
    return [
        this_friday + timedelta(weeks=offset)
        for offset in range(-past_weeks, future_weeks + 1)
    ]


def bob_date_query(d: date) -> str:
    return f"{d.day}-{d.month}-{d.year}"


def fetch(url: str, sleep: float) -> str | None:
    max_attempts = 3
    retryable_statuses = {429, 502, 503, 504}

    print("[RELEASE-BOB] fetch", {"url": url}, flush=True)

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(
                url,
                timeout=30,
                headers={
                    "User-Agent": "VinylofyReleaseDiscovery/0.1",
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
        except requests.RequestException as exc:
            if attempt >= max_attempts:
                print("[RELEASE-BOB] fetch_error", {
                    "url": url,
                    "attempt": attempt,
                    "error": str(exc),
                }, flush=True)
                return None

            delay = min(
                max(sleep, 1.0) * (2 ** (attempt - 1)),
                30.0,
            )

            print("[RELEASE-BOB] fetch_retry", {
                "url": url,
                "attempt": attempt,
                "reason": "request_exception",
                "delay_seconds": delay,
            }, flush=True)

            time.sleep(delay)
            continue

        if response.status_code < 400:
            if sleep:
                time.sleep(sleep)
            return response.text

        if (
            response.status_code not in retryable_statuses
            or attempt >= max_attempts
        ):
            print("[RELEASE-BOB] fetch_skip", {
                "url": url,
                "status_code": response.status_code,
                "reason": response.reason,
                "attempt": attempt,
            }, flush=True)
            return None

        retry_after = response.headers.get("Retry-After")
        delay = None

        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = None

        if delay is None or delay < 0:
            delay = max(sleep, 1.0) * (2 ** (attempt - 1))

        delay = min(delay, 30.0)

        print("[RELEASE-BOB] fetch_retry", {
            "url": url,
            "status_code": response.status_code,
            "reason": response.reason,
            "attempt": attempt,
            "delay_seconds": delay,
        }, flush=True)

        time.sleep(delay)

    return None



def extract_links(search_html: str) -> list[str]:
    soup = BeautifulSoup(search_html, "html.parser")
    links: list[str] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        href = str(a["href"])
        if "/products/" not in href:
            continue
        if "cadeaubon" in href.lower() or "gift" in href.lower():
            continue
        url = urljoin(BASE_URL, href.split("?")[0])
        if url in seen:
            continue
        seen.add(url)
        links.append(url)

    return links


def clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", unescape(value or "")).strip()


def split_artist_title(raw_title: str) -> tuple[str, str]:
    title = clean_text(raw_title)
    for sep in [" - ", " – ", " — "]:
        if sep in title:
            artist, release_title = title.split(sep, 1)
            return clean_text(artist), clean_text(release_title)
    return "", title


def meta_content(soup: BeautifulSoup, selector: str) -> str | None:
    tag = soup.select_one(selector)
    if not tag:
        return None
    value = tag.get("content")
    return clean_text(str(value)) if value else None


def find_ean(text: str) -> str | None:
    matches = EAN_RE.findall(text or "")
    if not matches:
        return None
    # Prefer normal retail barcode lengths.
    for m in matches:
        if len(m) in (12, 13):
            return m
    return matches[0]


def parse_detail(html: str, source_url: str, release_date: date) -> ReleaseItem | None:
    soup = BeautifulSoup(html, "html.parser")

    raw_title = (
        meta_content(soup, 'meta[property="og:title"]')
        or clean_text(soup.title.get_text(" ")) if soup.title else ""
    )
    raw_title = raw_title.replace(" – Bob's Vinyl", "").replace(" - Bob's Vinyl", "")
    artist, title = split_artist_title(raw_title)

    body_text = clean_text(soup.get_text(" "))
    ean = find_ean(body_text)

    image_source_url = meta_content(soup, 'meta[property="og:image"]')
    if image_source_url:
        image_source_url = urljoin(BASE_URL, image_source_url)

    if not title:
        print("[RELEASE-BOB] skip_no_title", {"url": source_url}, flush=True)
        return None

    payload = {
        "raw_title": raw_title,
        "parsed_artist": artist,
        "parsed_title": title,
        "ean_found": bool(ean),
    }

    return ReleaseItem(
        ean=ean,
        artist=artist or "Onbekende artiest",
        title=title,
        release_date=release_date,
        source_url=source_url,
        image_source_url=image_source_url,
        source_payload=payload,
    )


def upsert_release(item: ReleaseItem) -> None:
    params = {
        "ean": item.ean,
        "artist": item.artist,
        "title": item.title,
        "release_date": item.release_date.isoformat(),
        "source_shop": SOURCE_SHOP,
        "source_url": item.source_url,
        "image_source_url": item.image_source_url,
        "format": item.format,
        "label": item.label,
        "source_payload": json.dumps(item.source_payload or {}),
    }

    select_sql = """
        with source_match as (
            select id, ean
            from public.release_calendar
            where source_url = %(source_url)s
        ),
        effective_identity as (
            select coalesce(
                (select ean from source_match),
                %(ean)s
            ) as ean
        )
        select id
        from public.release_calendar
        where source_url = %(source_url)s
           or (
                (select ean from effective_identity) is not null
                and ean = (select ean from effective_identity)
                and source_shop = %(source_shop)s
                and release_date = %(release_date)s::date
           )
        order by id
        for update
    """

    update_sql = """
        update public.release_calendar
        set
            ean = coalesce(public.release_calendar.ean, %(ean)s),
            artist = case
                when public.release_calendar.artist = ''
                    then %(artist)s
                else public.release_calendar.artist
            end,
            title = case
                when public.release_calendar.title = ''
                    then %(title)s
                else public.release_calendar.title
            end,
            release_date = %(release_date)s::date,
            image_source_url = coalesce(
                public.release_calendar.image_source_url,
                %(image_source_url)s
            ),
            format = coalesce(
                public.release_calendar.format,
                %(format)s
            ),
            label = coalesce(
                public.release_calendar.label,
                %(label)s
            ),
            status = 'active',
            source_payload = (
                public.release_calendar.source_payload
                || %(source_payload)s::jsonb
            ),
            last_seen_at = now(),
            updated_at = now()
        where id = %(existing_id)s
    """

    insert_sql = """
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
            status,
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
            'active',
            %(source_payload)s::jsonb,
            now(),
            now(),
            now(),
            now()
        )
    """

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(select_sql, params)
            existing_rows = cur.fetchall()

            if len(existing_rows) > 1:
                raise RuntimeError(
                    "Release-identiteitsconflict: source_url en "
                    "(ean, source_shop, release_date) verwijzen naar "
                    "verschillende release_calendar-rijen."
                )

            if existing_rows:
                update_params = dict(params)
                update_params["existing_id"] = existing_rows[0][0]
                cur.execute(update_sql, update_params)
            else:
                cur.execute(insert_sql, params)

        conn.commit()



def run(args: argparse.Namespace) -> int:
    dates = upcoming_fridays(args.past_weeks, args.future_weeks)
    print("[RELEASE-BOB] start", {
        "dates": [d.isoformat() for d in dates],
        "write": args.write,
        "max_details_per_date": args.max_details_per_date,
    }, flush=True)

    total_links = 0
    total_items = 0
    total_written = 0

    for release_date in dates:
        q = bob_date_query(release_date)
        search_url = f"{BASE_URL}/search?q={quote(json.dumps(q))}&options%5Bprefix%5D=last"
        html = fetch(search_url, args.sleep)
        if not html:
            print("[RELEASE-BOB] search_skip", {
                "release_date": release_date.isoformat(),
                "query": q,
                "url": search_url,
            }, flush=True)
            continue
        links = extract_links(html)[: args.max_details_per_date]
        total_links += len(links)

        print("[RELEASE-BOB] search_result", {
            "release_date": release_date.isoformat(),
            "query": q,
            "links": len(links),
        }, flush=True)

        for url in links:
            detail_html = fetch(url, args.sleep)
            if not detail_html:
                continue
            item = parse_detail(detail_html, url, release_date)
            if not item:
                continue

            total_items += 1
            print("[RELEASE-BOB] parsed", {
                "release_date": item.release_date.isoformat(),
                "ean": item.ean,
                "artist": item.artist,
                "title": item.title,
                "url": item.source_url,
            }, flush=True)

            if args.write:
                upsert_release(item)
                total_written += 1

    print("[RELEASE-BOB] done", {
        "links": total_links,
        "items": total_items,
        "written": total_written,
        "dry_run": not args.write,
    }, flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--past-weeks", type=int, default=1)
    parser.add_argument("--future-weeks", type=int, default=4)
    parser.add_argument("--max-details-per-date", type=int, default=80)
    parser.add_argument("--sleep", type=float, default=0.5)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
