from __future__ import annotations

import argparse
import importlib.util
import json
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

from scripts.importers.common import (
    identifier_candidates,
    normalize_ean,
    normalize_text,
    normalize_gtin14,
    parse_price,
)
from scripts.scrapers.usf.core.db import db_connection

SOURCE_SHOP = "imusic"
BASE_URL = "https://imusic.co"
EXPOSURE_ID = "3146"
EXPOSURE_SLUG = "new-lps-and-upcoming-vinyl-releases"
DEFAULT_TIMEOUT_SECONDS = 20.0
DEFAULT_DELAY_SECONDS = 1.0
DEFAULT_MAX_PAGES = 30
DEFAULT_MAX_ATTEMPTS = 3
RETRYABLE_STATUSES = {429, 502, 503, 504}
PRODUCT_PATH_RE = re.compile(r"^/music/([0-9][0-9\-\s]{7,24})(?:/|$)")
RELEASE_DATE_RE = re.compile(r"\bRelease\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})\b", re.I)
TOTAL_COUNT_RE = re.compile(r"\bout of\s+([0-9][0-9,\.]*)", re.I)
OFFSET_RE = re.compile(r"[?&]offset=(\d+)")

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

VINYL_MARKERS = (
    re.compile(r"\bvinyl\b", re.I),
    re.compile(r"(?<![a-z0-9])lp(?:s)?(?![a-z0-9])", re.I),
    re.compile(r"(?<![a-z0-9])\d+\s*lp(?:s)?(?![a-z0-9])", re.I),
    re.compile(r"\b(?:12|10|7)\s*(?:\"|inch|inches)\b", re.I),
)
IN_STOCK_MARKERS = ("in stock", "op voorraad")
PREORDER_MARKERS = ("pre-order", "preorder", "voorbestelling")
OUT_OF_STOCK_MARKERS = ("sold out", "out of stock", "not available", "uitverkocht", "niet leverbaar")


def load_gtin_validator() -> Callable[[object], bool]:
    helper_path = Path(__file__).resolve().parents[2] / "importers" / "common" / "gtin.py"
    spec = importlib.util.spec_from_file_location("vinylofy_importers_common_gtin", helper_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Kan GTIN-helper niet laden: {helper_path}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.is_valid_gtin


is_valid_gtin = load_gtin_validator()


@dataclass(frozen=True)
class ReleaseItem:
    ean: str
    gtin_normalized: str
    artist: str
    title: str
    release_date: date
    source_url: str
    image_source_url: str | None = None
    format: str | None = None
    label: str | None = None
    price_raw: str | None = None
    availability: str | None = None
    source_payload: dict | None = None


@dataclass
class PageStats:
    offset: int
    cards: int = 0
    product_links: int = 0
    valid_eans: int = 0
    vinyl_products: int = 0
    valid_release_dates: int = 0
    new_eans: int = 0
    duplicate_eans: int = 0
    skips: Counter[str] = field(default_factory=Counter)
    min_release_date: date | None = None
    max_release_date: date | None = None


@dataclass(frozen=True)
class ParsedPage:
    items: list[ReleaseItem]
    stats: PageStats
    total_count: int | None
    offset_step: int | None
    eans: frozenset[str]


@dataclass
class DiscoveryResult:
    items: list[ReleaseItem]
    page_stats: list[PageStats]
    total_count: int | None
    stop_reason: str


def clean(value: object) -> str:
    return normalize_text(value) or ""


def exposure_url(offset: int = 0) -> str:
    url = f"{BASE_URL}/exposure/{EXPOSURE_ID}/{EXPOSURE_SLUG}"
    if offset > 0:
        return f"{url}?offset={offset}#tbl"
    return url


def product_url_from_href(href: object) -> str:
    href_text = clean(href)
    if not href_text:
        return ""
    return urljoin(BASE_URL, href_text.split("#", 1)[0])


def ean_from_product_url(url: object) -> str | None:
    href_text = clean(url)
    if not href_text:
        return None

    parsed = urlparse(href_text)
    path = parsed.path or href_text
    match = PRODUCT_PATH_RE.search(path)
    if not match:
        return None

    ean = normalize_ean(match.group(1))
    if not ean or not is_valid_gtin(ean):
        return None
    return ean


def parse_release_date(value: object) -> date | None:
    text = clean(value)
    if not text:
        return None

    match = RELEASE_DATE_RE.search(text)
    if match:
        text = match.group(1)

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


def parse_title_attribute(value: object) -> tuple[str | None, str | None, str | None, str | None]:
    text = clean(value)
    if not text:
        return None, None, None, None

    if " · " not in text:
        return None, None, None, None

    artist, rest = text.split(" · ", 1)
    rest = re.sub(r"\s*\(\d{4}\)\s*$", "", rest).strip()

    label = None
    label_match = re.search(r"\s*\[([^\]]+)\]\s*$", rest)
    if label_match:
        label = clean(label_match.group(1))
        rest = rest[: label_match.start()].strip()

    format_label = None
    format_match = re.search(r"\s*\(([^()]*)\)\s*$", rest)
    if format_match:
        format_label = clean(format_match.group(1))
        rest = rest[: format_match.start()].strip()

    title = clean(rest)
    return clean(artist) or None, title or None, format_label or None, label or None


def direct_text(node: Tag | None) -> str:
    if node is None:
        return ""
    return clean(node.get_text(" ", strip=True))


def item_format_and_label(card: Tag, title_format: str | None, title_label: str | None) -> tuple[str | None, str | None]:
    acronym = card.select_one(".type acronym")
    format_label = direct_text(acronym) or title_format

    label_node = card.select_one(".type .label-blank")
    label = direct_text(label_node) or title_label

    return format_label or None, label or None


def is_vinyl(format_label: str | None, label: str | None, title_attr: str | None) -> bool:
    combined = " ".join(clean(value).lower() for value in (format_label, label, title_attr))
    if not combined:
        return False

    return any(marker.search(combined) for marker in VINYL_MARKERS)


def extract_release_date(card: Tag) -> date | None:
    for node in card.select(".label-warning"):
        parsed = parse_release_date(node.get_text(" ", strip=True))
        if parsed:
            return parsed

    return parse_release_date(card.get_text(" ", strip=True))


def extract_price_raw(card: Tag) -> str | None:
    node = card.select_one("button.price")
    if not node:
        return None

    text = direct_text(node)
    if parse_price(text) is None:
        return None
    return text


def extract_availability(card: Tag) -> str | None:
    text = clean(card.get_text(" ", strip=True)).lower()
    button_titles = " ".join(clean(node.get("title")).lower() for node in card.select("button[title]"))
    combined = f"{text} {button_titles}"

    if any(marker in combined for marker in PREORDER_MARKERS):
        return "preorder"
    if any(marker in combined for marker in OUT_OF_STOCK_MARKERS):
        return "out_of_stock"
    if any(marker in combined for marker in IN_STOCK_MARKERS):
        return "in_stock"
    return None


def extract_image_source_url(card: Tag) -> str | None:
    image = card.select_one("img.item-cover")
    src = clean(image.get("src")) if image else ""
    if not src:
        return None
    return urljoin(BASE_URL, src)


def parse_total_count(soup: BeautifulSoup) -> int | None:
    text = clean(soup.get_text(" ", strip=True))
    match = TOTAL_COUNT_RE.search(text)
    if not match:
        return None
    raw = re.sub(r"\D", "", match.group(1))
    return int(raw) if raw else None


def parse_offset_step(soup: BeautifulSoup) -> int | None:
    offsets = sorted(
        {
            int(match.group(1))
            for option in soup.select("option[value*='offset=']")
            if (match := OFFSET_RE.search(clean(option.get("value"))))
        }
    )
    positive = [offset for offset in offsets if offset > 0]
    return positive[0] if positive else None


def parse_listing_page(html: str, *, listing_url: str, offset: int, global_seen_eans: set[str]) -> ParsedPage:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".list-container .list-item")
    stats = PageStats(offset=offset, cards=len(cards))
    items: list[ReleaseItem] = []
    page_seen_eans: set[str] = set()

    for position, card in enumerate(cards, start=1):
        anchor = card.select_one("a[href*='/music/']")
        if not anchor:
            stats.skips["missing_product_link"] += 1
            continue

        stats.product_links += 1
        source_url = product_url_from_href(anchor.get("href"))
        ean = ean_from_product_url(source_url)
        if not ean:
            stats.skips["invalid_ean"] += 1
            continue

        stats.valid_eans += 1
        if ean not in global_seen_eans:
            stats.new_eans += 1
        if ean in page_seen_eans:
            stats.duplicate_eans += 1
            stats.skips["duplicate_ean_on_page"] += 1
            continue
        page_seen_eans.add(ean)

        title_attr = clean(anchor.get("title"))
        attr_artist, attr_title, attr_format, attr_label = parse_title_attribute(title_attr)

        title_node = card.select_one(".item-text .title")
        artist_node = card.select_one(".item-text .artist")
        title = clean(title_node.get("title") if title_node else "") or direct_text(title_node) or attr_title
        artist = clean(artist_node.get("title") if artist_node else "") or direct_text(artist_node) or attr_artist

        if not title or not artist:
            stats.skips["missing_artist_or_title"] += 1
            continue

        format_label, label = item_format_and_label(card, attr_format, attr_label)
        if not is_vinyl(format_label, label, title_attr):
            stats.skips["non_vinyl"] += 1
            continue

        stats.vinyl_products += 1
        release_date = extract_release_date(card)
        if not release_date:
            stats.skips["missing_release_date"] += 1
            continue

        stats.valid_release_dates += 1
        stats.min_release_date = release_date if stats.min_release_date is None else min(stats.min_release_date, release_date)
        stats.max_release_date = release_date if stats.max_release_date is None else max(stats.max_release_date, release_date)

        gtin_normalized = normalize_gtin14(ean)
        if not gtin_normalized:
            stats.skips["invalid_gtin_normalization"] += 1
            continue

        items.append(
            ReleaseItem(
                ean=ean,
                gtin_normalized=gtin_normalized,
                artist=artist,
                title=title,
                release_date=release_date,
                source_url=source_url,
                image_source_url=extract_image_source_url(card),
                format=format_label,
                label=label,
                price_raw=extract_price_raw(card),
                availability=extract_availability(card),
                source_payload={
                    "source": "imusic_release_listing",
                    "listing_url": listing_url,
                    "listing_offset": offset,
                    "listing_position": position,
                    "title_attribute": title_attr,
                    "price_raw": extract_price_raw(card),
                    "availability": extract_availability(card),
                },
            )
        )

    return ParsedPage(
        items=items,
        stats=stats,
        total_count=parse_total_count(soup),
        offset_step=parse_offset_step(soup),
        eans=frozenset(page_seen_eans),
    )


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; Vinylofy-iMusic-ReleaseDiscovery/1.0; "
                "+https://vinylofy.nl)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,nl;q=0.6",
        }
    )
    return session


def fetch(session: requests.Session, url: str, *, timeout: float) -> str:
    for attempt in range(1, DEFAULT_MAX_ATTEMPTS + 1):
        try:
            response = session.get(url, timeout=timeout, allow_redirects=True)
        except requests.RequestException:
            if attempt >= DEFAULT_MAX_ATTEMPTS:
                raise

            time.sleep(min(2 ** (attempt - 1), 30.0))
            continue

        if response.status_code < 400:
            return response.text

        if response.status_code not in RETRYABLE_STATUSES or attempt >= DEFAULT_MAX_ATTEMPTS:
            response.raise_for_status()

        retry_after = response.headers.get("Retry-After")
        try:
            delay = float(retry_after) if retry_after else None
        except ValueError:
            delay = None
        if delay is None or delay < 0:
            delay = 2 ** (attempt - 1)

        time.sleep(min(delay, 30.0))

    raise RuntimeError(f"iMusic fetch failed unexpectedly: {url}")


def discover_release_items(
    *,
    max_pages: int,
    offset_step: int | None,
    delay_seconds: float,
    timeout_seconds: float,
    fetch_page: Callable[[str], str] | None = None,
) -> DiscoveryResult:
    if max_pages < 1:
        raise ValueError("max_pages moet minimaal 1 zijn")
    if offset_step is not None and offset_step < 1:
        raise ValueError("offset_step moet minimaal 1 zijn")

    session = build_session()
    items_by_ean: dict[str, ReleaseItem] = {}
    page_stats: list[PageStats] = []
    seen_page_ean_sets: set[frozenset[str]] = set()
    total_count: int | None = None
    inferred_offset_step = offset_step
    offset = 0
    stop_reason = "max_pages"

    for page_number in range(1, max_pages + 1):
        url = exposure_url(offset)
        html = fetch_page(url) if fetch_page else fetch(session, url, timeout=timeout_seconds)
        parsed_page = parse_listing_page(
            html,
            listing_url=url,
            offset=offset,
            global_seen_eans=set(items_by_ean),
        )
        page_items = parsed_page.items
        stats = parsed_page.stats

        if total_count is None and parsed_page.total_count is not None:
            total_count = parsed_page.total_count

        if inferred_offset_step is None and parsed_page.offset_step is not None:
            inferred_offset_step = parsed_page.offset_step

        if stats.cards == 0 or stats.product_links == 0 or stats.valid_eans == 0:
            stop_reason = "empty_page"
            page_stats.append(stats)
            log_page(stats, page_number=page_number, url=url)
            break
        if parsed_page.eans in seen_page_ean_sets:
            stop_reason = "duplicate_page"
            page_stats.append(stats)
            log_page(stats, page_number=page_number, url=url)
            break

        seen_page_ean_sets.add(parsed_page.eans)
        new_items = 0
        for item in page_items:
            if item.ean not in items_by_ean:
                new_items += 1
            items_by_ean[item.ean] = item

        if stats.new_eans == 0:
            stop_reason = "no_new_product_links"
            page_stats.append(stats)
            log_page(stats, page_number=page_number, url=url)
            break

        page_stats.append(stats)
        log_page(stats, page_number=page_number, url=url)

        step = inferred_offset_step or 100
        next_offset = offset + step
        if total_count is not None and next_offset >= total_count:
            stop_reason = "last_page"
            break

        offset = next_offset

        if delay_seconds > 0 and page_number < max_pages:
            time.sleep(delay_seconds)

    return DiscoveryResult(
        items=list(items_by_ean.values()),
        page_stats=page_stats,
        total_count=total_count,
        stop_reason=stop_reason,
    )


def log_page(stats: PageStats, *, page_number: int, url: str) -> None:
    print(
        "[RELEASE-IMUSIC-PAGE]",
        {
            "page": page_number,
            "offset": stats.offset,
            "cards": stats.cards,
            "product_links": stats.product_links,
            "valid_eans": stats.valid_eans,
            "vinyl_products": stats.vinyl_products,
            "valid_release_dates": stats.valid_release_dates,
            "new_eans": stats.new_eans,
            "skips": dict(stats.skips),
            "min_release_date": stats.min_release_date.isoformat() if stats.min_release_date else None,
            "max_release_date": stats.max_release_date.isoformat() if stats.max_release_date else None,
            "url": url,
        },
        flush=True,
    )


def release_window(anchor: date | None = None) -> tuple[date, date]:
    today = anchor or date.today()
    return (
        today - timedelta(days=14),
        today + timedelta(days=14),
    )


def find_unique_product_id(cur, item: ReleaseItem) -> str | None:
    candidates = identifier_candidates(item.ean, item.gtin_normalized)
    if not candidates:
        return None

    cur.execute(
        """
        select id
        from public.products
        where ean = any(%s)
           or gtin_normalized = any(%s)
        order by id
        """,
        (candidates, candidates),
    )
    product_ids = [str(row[0]) for row in cur.fetchall()]
    unique_product_ids = sorted(set(product_ids))
    if len(unique_product_ids) == 1:
        return unique_product_ids[0]
    return None


def upsert_release(cur, item: ReleaseItem, *, product_id: str | None) -> bool:
    payload = dict(item.source_payload or {})
    payload["gtin_normalized"] = item.gtin_normalized
    payload["product_match"] = "unique" if product_id else "missing_or_ambiguous"

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
        "product_id": product_id,
        "source_payload": json.dumps(payload, ensure_ascii=False),
    }

    cur.execute(
        """
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
        """,
        params,
    )
    existing_rows = cur.fetchall()
    if len(existing_rows) > 1:
        raise RuntimeError(
            "Release-identiteitsconflict: source_url en "
            "(ean, source_shop, release_date) verwijzen naar verschillende "
            "release_calendar-rijen."
        )

    if existing_rows:
        params["existing_id"] = existing_rows[0][0]
        cur.execute(
            """
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
                format = coalesce(public.release_calendar.format, %(format)s),
                label = coalesce(public.release_calendar.label, %(label)s),
                product_id = coalesce(public.release_calendar.product_id, %(product_id)s),
                status = 'active',
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
        "matched_products": 0,
        "inserted": 0,
        "updated": 0,
        "databasewrites": int(write),
    }

    with db_connection() as conn:
        with conn.cursor() as cur:
            for item in items:
                stats["items"] += 1
                product_id = find_unique_product_id(cur, item)
                if product_id:
                    stats["matched_products"] += 1

                if not write:
                    continue

                inserted = upsert_release(cur, item, product_id=product_id)
                if inserted:
                    stats["inserted"] += 1
                else:
                    stats["updated"] += 1

        if write:
            conn.commit()
        else:
            conn.rollback()

    return stats


def inspect_db_overlap(items: Iterable[ReleaseItem]) -> dict[str, int]:
    items_list = list(items)
    stats = {
        "items": len(items_list),
        "existing_product_overlap": 0,
        "bobsvinyl_release_overlap": 0,
        "existing_imusic_release_rows": 0,
        "new_release_candidates": 0,
    }
    if not items_list:
        return stats

    candidate_values: list[str] = []
    for item in items_list:
        for candidate in identifier_candidates(item.ean, item.gtin_normalized):
            if candidate not in candidate_values:
                candidate_values.append(candidate)

    def item_matches_values(item: ReleaseItem, values: set[str]) -> bool:
        return any(
            normalize_ean(candidate) in values
            for candidate in identifier_candidates(item.ean, item.gtin_normalized)
        )

    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select ean, gtin_normalized
                from public.products
                where ean = any(%s)
                   or gtin_normalized = any(%s)
                """,
                (candidate_values, candidate_values),
            )
            product_eans: set[str] = set()
            for product_ean, product_gtin in cur.fetchall():
                for candidate in identifier_candidates(product_ean, product_gtin):
                    normalized = normalize_ean(candidate)
                    if normalized:
                        product_eans.add(normalized)
            stats["existing_product_overlap"] = sum(
                1 for item in items_list if item_matches_values(item, product_eans)
            )

            cur.execute(
                """
                select distinct ean
                from public.release_calendar
                where source_shop = 'bobsvinyl'
                  and ean = any(%s)
                """,
                (candidate_values,),
            )
            bob_eans: set[str] = set()
            for (release_ean,) in cur.fetchall():
                for candidate in identifier_candidates(release_ean):
                    normalized = normalize_ean(candidate)
                    if normalized:
                        bob_eans.add(normalized)
            stats["bobsvinyl_release_overlap"] = sum(
                1 for item in items_list if item_matches_values(item, bob_eans)
            )

            cur.execute(
                """
                select distinct ean
                from public.release_calendar
                where source_shop = %s
                  and ean = any(%s)
                """,
                (SOURCE_SHOP, candidate_values),
            )
            imusic_eans: set[str] = set()
            for (release_ean,) in cur.fetchall():
                for candidate in identifier_candidates(release_ean):
                    normalized = normalize_ean(candidate)
                    if normalized:
                        imusic_eans.add(normalized)
            stats["existing_imusic_release_rows"] = sum(
                1 for item in items_list if item_matches_values(item, imusic_eans)
            )

        conn.rollback()

    stats["new_release_candidates"] = (
        stats["items"] - stats["existing_imusic_release_rows"]
    )
    return stats


def detail_ean_from_html(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    text = clean(soup.get_text(" ", strip=True))
    match = re.search(r"EAN/UPC\s+([0-9][0-9\-\s]{7,24})", text, re.I)
    if not match:
        return None
    return ean_from_product_url(f"/music/{match.group(1)}")


def verify_detail_eans(items: list[ReleaseItem], *, limit: int, timeout_seconds: float, delay_seconds: float) -> dict[str, int]:
    stats = {"checked": 0, "matched": 0, "mismatched": 0, "missing": 0, "errors": 0}
    if limit < 1:
        return stats

    session = build_session()
    for item in items[:limit]:
        try:
            html = fetch(session, item.source_url, timeout=timeout_seconds)
        except requests.RequestException as exc:
            stats["errors"] += 1
            print(
                "[RELEASE-IMUSIC-DETAIL-CHECK]",
                {"ean": item.ean, "url": item.source_url, "error": str(exc)},
                flush=True,
            )
            continue

        stats["checked"] += 1
        detail_ean = detail_ean_from_html(html)
        if detail_ean is None:
            stats["missing"] += 1
        elif detail_ean == item.ean:
            stats["matched"] += 1
        else:
            stats["mismatched"] += 1

        print(
            "[RELEASE-IMUSIC-DETAIL-CHECK]",
            {
                "url_ean": item.ean,
                "detail_ean": detail_ean,
                "url": item.source_url,
            },
            flush=True,
        )
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    return stats


def summarize_window(items: Iterable[ReleaseItem], *, anchor: date | None = None) -> dict[str, int]:
    min_date, max_date = release_window(anchor)
    items_list = list(items)
    return {
        "items": len(items_list),
        "within_existing_new_releases_window": sum(
            1 for item in items_list if min_date <= item.release_date <= max_date
        ),
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Discover iMusic release-calendar items from the new LP exposure listing."
    )
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES)
    parser.add_argument("--offset-step", type=int, default=None)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--detail-check-limit", type=int, default=0)
    parser.add_argument("--write", action="store_true")
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.max_pages < 1:
        raise SystemExit("[ERROR] --max-pages moet minimaal 1 zijn.")
    if args.offset_step is not None and args.offset_step < 1:
        raise SystemExit("[ERROR] --offset-step moet minimaal 1 zijn.")
    if args.delay < 0:
        raise SystemExit("[ERROR] --delay mag niet negatief zijn.")
    if args.timeout <= 0:
        raise SystemExit("[ERROR] --timeout moet positief zijn.")
    if args.detail_check_limit < 0:
        raise SystemExit("[ERROR] --detail-check-limit mag niet negatief zijn.")


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)

    print(
        "[RELEASE-IMUSIC] start",
        {
            "url": exposure_url(0),
            "max_pages": args.max_pages,
            "offset_step": args.offset_step,
            "delay": args.delay,
            "detail_check_limit": args.detail_check_limit,
            "write": args.write,
        },
        flush=True,
    )

    result = discover_release_items(
        max_pages=args.max_pages,
        offset_step=args.offset_step,
        delay_seconds=args.delay,
        timeout_seconds=args.timeout,
    )

    window_summary = summarize_window(result.items)
    duplicate_count = sum(stats.duplicate_eans for stats in result.page_stats)

    print(
        "[RELEASE-IMUSIC] collected",
        {
            "items": len(result.items),
            "pages": len(result.page_stats),
            "total_count": result.total_count,
            "stop_reason": result.stop_reason,
            "duplicates_within_imusic": duplicate_count,
            **window_summary,
        },
        flush=True,
    )

    for item in result.items[:10]:
        print(
            "[RELEASE-IMUSIC-SAMPLE]",
            {
                "artist": item.artist,
                "title": item.title,
                "ean": item.ean,
                "release_date": item.release_date.isoformat(),
                "format": item.format,
                "label": item.label,
                "price_raw": item.price_raw,
                "availability": item.availability,
                "url": item.source_url,
            },
            flush=True,
        )

    detail_stats = verify_detail_eans(
        result.items,
        limit=args.detail_check_limit,
        timeout_seconds=args.timeout,
        delay_seconds=args.delay,
    )
    if detail_stats["mismatched"]:
        raise SystemExit("[ERROR] iMusic URL-EAN/detail-EAN mismatch gevonden.")

    db_stats = write_releases(result.items, write=args.write)
    overlap_stats = inspect_db_overlap(result.items)
    print("[RELEASE-IMUSIC] db", db_stats, flush=True)
    print("[RELEASE-IMUSIC] overlap", overlap_stats, flush=True)
    print("[RELEASE-IMUSIC] detail_check", detail_stats, flush=True)
    if not args.write:
        print("[RELEASE-IMUSIC] dry-run complete; geen databasewrites.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
