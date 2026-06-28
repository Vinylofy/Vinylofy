#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote_plus, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from psycopg.rows import dict_row

from scripts.importers.common import normalize_ean, normalize_gtin14, normalize_text
from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.listing_price_sync import (
    ListingOffer,
    ListingSyncStats,
    ensure_shop_for_listing_offer,
    find_product_by_ean,
    insert_history_if_needed,
    normalize_availability,
    parse_price,
    upsert_listing_price,
)


SHOP_NAME = "Groovespin"
SHOP_DOMAIN = "groovespin.nl"
SHOP_COUNTRY = "NL"
BASE_URL = "https://www.groovespin.nl"

DEFAULT_SEARCH_URL_TEMPLATES = [
    # Dit zijn alleen probe-kandidaten. Er wordt pas geschreven na exacte EAN-verificatie.
    "https://www.groovespin.nl/search?q={ean}",
    "https://www.groovespin.nl/search?search={ean}",
    "https://www.groovespin.nl/zoeken?q={ean}",
    "https://www.groovespin.nl/zoeken?search={ean}",
    "https://www.groovespin.nl/albums?q={ean}",
    "https://www.groovespin.nl/albums?search={ean}",
]

UA = (
    "Mozilla/5.0 (compatible; Vinylofy-Groovespin-EANProbe/1.0; "
    "+https://vinylofy.nl)"
)


@dataclass(frozen=True)
class ProbeTarget:
    product_id: str
    ean: str
    artist: str | None
    title: str | None
    active_shop_count: int
    min_price: Decimal | None
    max_price: Decimal | None
    price_spread: Decimal | None
    latest_seen_at: datetime | None
    groovespin_last_seen_at: datetime | None
    groovespin_is_active: bool | None
    popularity_rank: int
    popularity_score: Decimal


@dataclass(frozen=True)
class FormSearchTemplate:
    action_url: str
    field_name: str
    params: dict[str, str]


@dataclass(frozen=True)
class FetchResult:
    requested_url: str
    final_url: str
    status_code: int | None
    html: str | None
    error: str | None


@dataclass(frozen=True)
class ProbeResult:
    target: ProbeTarget
    offer: ListingOffer | None
    reason: str | None
    search_url: str | None
    detail_url: str | None
    found_ean: str | None
    price_raw: str | None


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def log(label: str, payload: dict[str, Any] | str) -> None:
    print(f"[{label}] {payload}", flush=True)


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def normalize_url_keep_release_id(value: str, base_url: str = BASE_URL) -> str:
    raw = clean(value)
    if not raw:
        return ""

    abs_url = urljoin(base_url, raw)
    parsed = urlparse(abs_url)

    keep_params: list[tuple[str, str]] = []
    for key, val in parse_qsl(parsed.query, keep_blank_values=True):
        if key.lower() == "releaseid":
            keep_params.append((key, val))

    return urlunparse(
        parsed._replace(
            scheme="https",
            netloc=parsed.netloc.lower(),
            query=urlencode(keep_params, doseq=True),
            fragment="",
        )
    ).rstrip("/")


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )
    return session


def ean_variants(value: str | None) -> set[str]:
    base = normalize_ean(value)
    if not base:
        return set()

    variants = {base}
    gtin14 = normalize_gtin14(base)
    if gtin14:
        variants.add(gtin14)

    if len(base) == 14 and base.startswith("0"):
        shorter = base[1:]
        variants.add(shorter)
        shorter_gtin = normalize_gtin14(shorter)
        if shorter_gtin:
            variants.add(shorter_gtin)

    if len(base) == 13:
        variants.add("0" + base)

    if len(base) == 12:
        variants.add("0" + base)
        variants.add("00" + base)

    return {v for v in variants if normalize_ean(v)}


def same_ean(left: str | None, right: str | None) -> bool:
    return bool(ean_variants(left) & ean_variants(right))


def extract_money(text: str | None) -> str | None:
    value = clean(text)
    if not value:
        return None

    patterns = [
        r"€\s*([0-9]+(?:[.,][0-9]{1,2})?)",
        r"([0-9]+(?:[.,][0-9]{1,2})?)\s*€",
    ]

    for pattern in patterns:
        matches = re.findall(pattern, value)
        if matches:
            return matches[0].replace(",", ".")

    return None


def availability_from_text(text: str | None) -> str:
    low = clean(text).lower()
    if not low:
        return "unknown"

    if any(token in low for token in ("uitverkocht", "niet leverbaar", "sold out", "out of stock")):
        return "out_of_stock"

    if any(token in low for token in ("preorder", "pre-order", "voorbestelling", "voorverkoop")):
        return "preorder"

    if any(token in low for token in ("op voorraad", "in stock", "naar winkelmandje")):
        return "in_stock"

    if any(token in low for token in ("bij leverancier", "om te bestellen", "onderweg", "binnen een week", "binnen een maand")):
        return "unknown"

    return "unknown"


def extract_labeled_eans(html: str | None) -> list[str]:
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    candidates: list[str] = []

    label_pattern = re.compile(
        r"\b(?:EAN|GTIN|Barcode|Bar\s*code|Streepjescode)\b[^0-9]{0,80}([0-9][0-9\-\s]{7,24})\b",
        flags=re.I,
    )

    for source in (text, html):
        for match in label_pattern.finditer(source):
            ean = normalize_ean(match.group(1))
            if ean and ean not in candidates:
                candidates.append(ean)

    return candidates


def find_ean_window(html: str, target_ean: str, radius_before: int = 1200, radius_after: int = 1800) -> str:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    low_text = text.lower()

    for variant in sorted(ean_variants(target_ean), key=len, reverse=True):
        idx = low_text.find(variant.lower())
        if idx >= 0:
            start = max(0, idx - radius_before)
            end = min(len(text), idx + len(variant) + radius_after)
            return text[start:end]

    return ""


def extract_price_for_verified_ean(html: str, target_ean: str) -> str | None:
    window = find_ean_window(html, target_ean)
    price = extract_money(window)
    if price:
        return price

    labeled_eans = extract_labeled_eans(html)
    exact_eans = [ean for ean in labeled_eans if normalize_ean(ean)]

    # Alleen generieke prijs gebruiken wanneer er maar één EAN op de pagina staat.
    # Bij meerdere releases op één album-pagina is generieke prijs te riskant.
    if len(set(exact_eans)) <= 1:
        soup = BeautifulSoup(html, "html.parser")
        return extract_money(soup.get_text(" ", strip=True))

    return None


def extract_title_raw(html: str | None) -> str | None:
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", attrs={"property": "og:title"}) or soup.find(
        "meta", attrs={"name": "twitter:title"}
    )
    if meta and meta.get("content"):
        return clean(meta.get("content")) or None

    h1 = soup.find("h1")
    if h1:
        return clean(h1.get_text(" ", strip=True)) or None

    if soup.title and soup.title.string:
        return clean(soup.title.string) or None

    return None


def fetch_html(session: requests.Session, url: str, *, timeout: float, sleep: float) -> FetchResult:
    try:
        response = session.get(url, timeout=timeout, allow_redirects=True)
    except requests.RequestException as exc:
        time.sleep(sleep)
        return FetchResult(
            requested_url=url,
            final_url=url,
            status_code=None,
            html=None,
            error=f"{type(exc).__name__}: {exc}",
        )

    time.sleep(sleep)

    if response.status_code in (429, 500, 502, 503, 504):
        return FetchResult(
            requested_url=url,
            final_url=response.url,
            status_code=response.status_code,
            html=None,
            error=f"http_status:{response.status_code}",
        )

    if response.status_code >= 400:
        return FetchResult(
            requested_url=url,
            final_url=response.url,
            status_code=response.status_code,
            html=None,
            error=f"http_status:{response.status_code}",
        )

    response.encoding = response.apparent_encoding or response.encoding or "utf-8"

    return FetchResult(
        requested_url=url,
        final_url=response.url,
        status_code=response.status_code,
        html=response.text or "",
        error=None,
    )


def is_album_url(url: str) -> bool:
    try:
        path = urlparse(url).path.lower()
    except Exception:
        return False

    return "/album/" in path


def extract_candidate_album_links(html: str | None, base_url: str) -> list[str]:
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []

    for node in soup.select("a[href]"):
        href = clean(node.get("href"))
        if not href:
            continue

        url = normalize_url_keep_release_id(href, base_url)
        if not url or not is_album_url(url):
            continue

        if url not in out:
            out.append(url)

    return out


def discover_form_search_templates(session: requests.Session, *, timeout: float, sleep: float) -> list[FormSearchTemplate]:
    templates: list[FormSearchTemplate] = []

    seed_urls = [
        BASE_URL,
        f"{BASE_URL}/albums",
    ]

    for seed_url in seed_urls:
        result = fetch_html(session, seed_url, timeout=timeout, sleep=sleep)
        if not result.html:
            log("GROOVESPIN-SEARCH-DISCOVERY-SKIP", {"url": seed_url, "error": result.error})
            continue

        soup = BeautifulSoup(result.html, "html.parser")

        for form in soup.find_all("form"):
            method = clean(form.get("method")).lower() or "get"
            if method and method != "get":
                continue

            action = clean(form.get("action")) or seed_url
            action_url = urljoin(result.final_url, action)

            inputs = form.find_all("input")
            params: dict[str, str] = {}
            query_fields: list[str] = []

            for inp in inputs:
                name = clean(inp.get("name"))
                if not name:
                    continue

                input_type = clean(inp.get("type")).lower()
                placeholder = clean(inp.get("placeholder")).lower()
                aria = clean(inp.get("aria-label")).lower()
                value = clean(inp.get("value"))

                looks_search = (
                    input_type == "search"
                    or name.lower() in {"q", "query", "search", "s", "keyword", "term", "phrase"}
                    or "zoek" in placeholder
                    or "search" in placeholder
                    or "zoek" in aria
                    or "search" in aria
                )

                if looks_search:
                    query_fields.append(name)
                    continue

                if input_type in {"hidden", "submit"} and value:
                    params[name] = value

            for field_name in query_fields:
                template = FormSearchTemplate(action_url=action_url, field_name=field_name, params=params.copy())
                if template not in templates:
                    templates.append(template)

    log(
        "GROOVESPIN-SEARCH-DISCOVERY",
        {
            "form_templates": [
                {"action_url": t.action_url, "field_name": t.field_name, "params": t.params}
                for t in templates
            ]
        },
    )

    return templates


def build_search_urls(
    ean: str,
    *,
    form_templates: list[FormSearchTemplate],
    explicit_templates: list[str],
) -> list[str]:
    urls: list[str] = []

    for template in form_templates:
        parsed = urlparse(template.action_url)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query.update(template.params)
        query[template.field_name] = ean
        url = urlunparse(parsed._replace(query=urlencode(query, doseq=True), fragment=""))
        if url not in urls:
            urls.append(url)

    for template in explicit_templates:
        url = template.format(ean=quote_plus(ean))
        if url not in urls:
            urls.append(url)

    return urls


def select_probe_targets(
    *,
    limit: int,
    cohort_count: int,
    cohort: int,
    min_active_shops: int,
) -> list[ProbeTarget]:
    query = """
    with groovespin_shop as (
      select id
      from public.shops
      where domain = 'groovespin.nl'
         or lower(name) in ('groovespin', 'groove spin')
      order by updated_at desc nulls last
      limit 1
    ),

    base as (
      select
        p.id::text as product_id,
        coalesce(nullif(p.ean, ''), nullif(p.gtin_normalized, '')) as ean,
        nullif(p.artist, '') as artist,
        nullif(p.title, '') as title,
        nullif(p.cover_url, '') as cover_url,

        count(distinct pr.shop_id) as active_shop_count,
        min(pr.price) as min_price,
        max(pr.price) as max_price,
        max(pr.last_seen_at) as latest_seen_at,
        max(pr.updated_at) as latest_price_updated_at,

        gp.last_seen_at as groovespin_last_seen_at,
        gp.is_active as groovespin_is_active

      from public.products p
      left join groovespin_shop gs on true

      join public.prices pr
        on pr.product_id = p.id
       and pr.is_active = true
       and pr.price is not null
       and (gs.id is null or pr.shop_id <> gs.id)

      left join public.prices gp
        on gs.id is not null
       and gp.product_id = p.id
       and gp.shop_id = gs.id

      where coalesce(nullif(p.ean, ''), nullif(p.gtin_normalized, '')) is not null

      group by
        p.id,
        p.ean,
        p.gtin_normalized,
        p.artist,
        p.title,
        p.cover_url,
        gp.last_seen_at,
        gp.is_active
    ),

    scored as (
      select
        *,
        greatest(coalesce(max_price, 0) - coalesce(min_price, 0), 0) as price_spread,

        (
          least(active_shop_count, 8) * 50
          +
          least(greatest(coalesce(max_price, 0) - coalesce(min_price, 0), 0), 30) * 3
          +
          case
            when latest_seen_at > now() - interval '14 days' then 20
            when latest_seen_at > now() - interval '45 days' then 10
            else 0
          end
          +
          case when artist is not null and title is not null then 15 else 0 end
          +
          case when cover_url is not null then 5 else 0 end
          +
          case
            when groovespin_last_seen_at is null then 10
            when groovespin_last_seen_at < now() - interval '60 hours' then 8
            else 0
          end
        )::numeric as popularity_score

      from base
      where active_shop_count >= %s
    ),

    ranked as (
      select
        *,
        row_number() over (
          order by
            popularity_score desc,
            active_shop_count desc,
            price_spread desc,
            latest_seen_at desc nulls last,
            product_id
        ) as popularity_rank
      from scored
    )

    select
      product_id,
      ean,
      artist,
      title,
      active_shop_count,
      min_price,
      max_price,
      price_spread,
      latest_seen_at,
      groovespin_last_seen_at,
      groovespin_is_active,
      popularity_rank,
      popularity_score
    from ranked
    where (((popularity_rank - 1) / %s)::int %% %s) = %s
    order by popularity_rank asc
    limit %s
    """

    with db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, (min_active_shops, limit, cohort_count, cohort, limit))
            rows = cur.fetchall()

    targets: list[ProbeTarget] = []

    for row in rows:
        ean = normalize_ean(row["ean"])
        if not ean:
            continue

        targets.append(
            ProbeTarget(
                product_id=str(row["product_id"]),
                ean=ean,
                artist=row["artist"],
                title=row["title"],
                active_shop_count=int(row["active_shop_count"] or 0),
                min_price=row["min_price"],
                max_price=row["max_price"],
                price_spread=row["price_spread"],
                latest_seen_at=row["latest_seen_at"],
                groovespin_last_seen_at=row["groovespin_last_seen_at"],
                groovespin_is_active=row["groovespin_is_active"],
                popularity_rank=int(row["popularity_rank"]),
                popularity_score=row["popularity_score"],
            )
        )

    return targets


def write_selection_csv(path: Path, targets: list[ProbeTarget]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "product_id",
                "ean",
                "artist",
                "title",
                "active_shop_count",
                "min_price",
                "max_price",
                "price_spread",
                "latest_seen_at",
                "groovespin_last_seen_at",
                "groovespin_is_active",
                "popularity_rank",
                "popularity_score",
            ],
        )
        writer.writeheader()

        for target in targets:
            writer.writerow(
                {
                    "product_id": target.product_id,
                    "ean": target.ean,
                    "artist": target.artist,
                    "title": target.title,
                    "active_shop_count": target.active_shop_count,
                    "min_price": target.min_price,
                    "max_price": target.max_price,
                    "price_spread": target.price_spread,
                    "latest_seen_at": target.latest_seen_at.isoformat() if target.latest_seen_at else "",
                    "groovespin_last_seen_at": target.groovespin_last_seen_at.isoformat()
                    if target.groovespin_last_seen_at
                    else "",
                    "groovespin_is_active": target.groovespin_is_active,
                    "popularity_rank": target.popularity_rank,
                    "popularity_score": target.popularity_score,
                }
            )


def probe_one_target(
    session: requests.Session,
    target: ProbeTarget,
    *,
    form_templates: list[FormSearchTemplate],
    explicit_templates: list[str],
    timeout: float,
    sleep: float,
    max_candidate_links: int,
) -> ProbeResult:
    search_urls = build_search_urls(
        target.ean,
        form_templates=form_templates,
        explicit_templates=explicit_templates,
    )

    seen_detail_urls: set[str] = set()

    for search_url in search_urls:
        search_result = fetch_html(session, search_url, timeout=timeout, sleep=sleep)
        if not search_result.html:
            log(
                "GROOVESPIN-SEARCH-SKIP",
                {
                    "ean": target.ean,
                    "search_url": search_url,
                    "status_code": search_result.status_code,
                    "error": search_result.error,
                },
            )
            continue

        candidate_urls = extract_candidate_album_links(search_result.html, search_result.final_url)

        if is_album_url(search_result.final_url):
            direct_url = normalize_url_keep_release_id(search_result.final_url)
            if direct_url and direct_url not in candidate_urls:
                candidate_urls.insert(0, direct_url)

        candidate_urls = candidate_urls[:max_candidate_links]

        log(
            "GROOVESPIN-SEARCH",
            {
                "ean": target.ean,
                "rank": target.popularity_rank,
                "search_url": search_url,
                "final_url": search_result.final_url,
                "candidate_links": len(candidate_urls),
            },
        )

        for detail_url in candidate_urls:
            if detail_url in seen_detail_urls:
                continue

            seen_detail_urls.add(detail_url)

            detail_result = fetch_html(session, detail_url, timeout=timeout, sleep=sleep)
            if not detail_result.html:
                log(
                    "GROOVESPIN-DETAIL-SKIP",
                    {
                        "ean": target.ean,
                        "detail_url": detail_url,
                        "status_code": detail_result.status_code,
                        "error": detail_result.error,
                    },
                )
                continue

            found_eans = extract_labeled_eans(detail_result.html)
            matching_ean = next((ean for ean in found_eans if same_ean(ean, target.ean)), None)

            if not matching_ean:
                continue

            price_raw = extract_price_for_verified_ean(detail_result.html, target.ean)
            availability = availability_from_text(BeautifulSoup(detail_result.html, "html.parser").get_text(" ", strip=True))
            title_raw = extract_title_raw(detail_result.html)

            if not price_raw:
                return ProbeResult(
                    target=target,
                    offer=None,
                    reason="exact_ean_found_but_no_safe_price",
                    search_url=search_url,
                    detail_url=detail_result.final_url,
                    found_ean=matching_ean,
                    price_raw=None,
                )

            offer = ListingOffer(
                shop_name=SHOP_NAME,
                shop_domain=SHOP_DOMAIN,
                shop_country=SHOP_COUNTRY,
                source_url=normalize_url_keep_release_id(detail_result.final_url),
                price=price_raw,
                availability=availability,
                currency="EUR",
                ean=normalize_ean(matching_ean),
                seen_at=now_utc(),
                raw={
                    "source": "groovespin_selected_ean_probe",
                    "selection_source": "vinylofy_priority_eans",
                    "vinylofy_product_id": target.product_id,
                    "vinylofy_artist": target.artist,
                    "vinylofy_title": target.title,
                    "popularity_rank": target.popularity_rank,
                    "popularity_score": str(target.popularity_score),
                    "search_url": search_url,
                    "detail_url": detail_result.final_url,
                    "title_raw": title_raw,
                    "found_ean": matching_ean,
                    "price_source": "detail_exact_ean_window_or_single_ean_page",
                },
            )

            return ProbeResult(
                target=target,
                offer=offer,
                reason=None,
                search_url=search_url,
                detail_url=detail_result.final_url,
                found_ean=matching_ean,
                price_raw=price_raw,
            )

    return ProbeResult(
        target=target,
        offer=None,
        reason="no_exact_ean_match_found",
        search_url=search_urls[0] if search_urls else None,
        detail_url=None,
        found_ean=None,
        price_raw=None,
    )


def sync_ean_exact_offers(
    conn,
    offers: list[ListingOffer],
    *,
    write: bool,
) -> ListingSyncStats:
    stats = ListingSyncStats(total=len(offers))

    with conn.cursor(row_factory=dict_row) as cur:
        shop_ids_by_domain: dict[str, str] = {}

        for offer in offers:
            price = parse_price(offer.price)

            if offer.price is None:
                stats.skipped_no_price += 1
                continue

            if price is None:
                stats.skipped_bad_price += 1
                continue

            stats.with_price += 1

            if offer.shop_domain not in shop_ids_by_domain:
                shop_ids_by_domain[offer.shop_domain] = ensure_shop_for_listing_offer(
                    cur,
                    shop_name=offer.shop_name,
                    shop_domain=offer.shop_domain,
                    shop_country=offer.shop_country,
                )

            shop_id = shop_ids_by_domain[offer.shop_domain]

            # Bewust EAN-first: Groovespin albumpagina's kunnen meerdere releases hebben.
            product_id = find_product_by_ean(cur, ean=offer.ean)

            if product_id is None:
                stats.unmatched += 1
                continue

            stats.matched_offer_ean += 1

            availability = normalize_availability(offer.availability)
            seen_at = offer.seen_at or now_utc()
            if seen_at.tzinfo is None:
                seen_at = seen_at.replace(tzinfo=timezone.utc)

            if not write:
                stats.refreshed_prices += 1
                stats.changed_prices += 1
                continue

            inserted, changed = upsert_listing_price(
                cur,
                product_id=product_id,
                shop_id=shop_id,
                source_url=offer.source_url,
                price=price,
                currency=offer.currency,
                availability=availability,
                seen_at=seen_at,
            )

            if changed:
                if insert_history_if_needed(
                    cur,
                    product_id=product_id,
                    shop_id=shop_id,
                    price=price,
                    currency=offer.currency,
                    availability=availability,
                    captured_at=seen_at,
                ):
                    stats.history_rows += 1

            stats.inserted_prices += int(inserted)
            stats.changed_prices += int(changed)
            stats.refreshed_prices += 1

    if write:
        conn.commit()

    return stats


def deactivate_stale_groovespin_offers(*, hours: float, write: bool) -> int:
    with db_connection() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                select id
                from public.shops
                where domain = %s
                limit 1
                """,
                (SHOP_DOMAIN,),
            )
            shop = cur.fetchone()

            if not shop:
                return 0

            shop_id = shop["id"]

            cur.execute(
                """
                select count(*) as n
                from public.prices
                where shop_id = %s
                  and is_active = true
                  and last_seen_at < now() - (%s * interval '1 hour')
                """,
                (shop_id, hours),
            )
            count = int(cur.fetchone()["n"])

            if write and count:
                cur.execute(
                    """
                    update public.prices
                    set is_active = false,
                        availability = 'out_of_stock',
                        updated_at = now()
                    where shop_id = %s
                      and is_active = true
                      and last_seen_at < now() - (%s * interval '1 hour')
                    """,
                    (shop_id, hours),
                )

        if write:
            conn.commit()

    return count


def resolve_cohort(cohort: int, cohort_count: int) -> int:
    if cohort >= 0:
        return cohort % cohort_count

    return now_utc().toordinal() % cohort_count


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Selecteer dagelijks prioritaire Vinylofy-EANs en probe Groovespin exact op die EANs."
        )
    )

    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--cohort-count", type=int, default=2)
    parser.add_argument("--cohort", type=int, default=-1, help="-1 = automatisch roteren per dag")
    parser.add_argument("--min-active-shops", type=int, default=1)

    parser.add_argument(
        "--search-url-template",
        action="append",
        default=[],
        help="Extra Groovespin zoek-URL-template met {ean}, mag meerdere keren.",
    )

    parser.add_argument("--max-candidate-links", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=25.0)
    parser.add_argument("--sleep", type=float, default=1.5)

    parser.add_argument("--selection-csv", default="output/usf-groovespin/selected_eans.csv")
    parser.add_argument("--deactivate-stale-after-hours", type=float, default=60.0)
    parser.add_argument("--skip-deactivate", action="store_true")
    parser.add_argument("--min-offers-to-write", type=int, default=1)
    parser.add_argument("--write", action="store_true")

    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")
    if args.cohort_count < 1:
        raise SystemExit("[ERROR] --cohort-count moet minimaal 1 zijn.")
    if args.min_active_shops < 1:
        raise SystemExit("[ERROR] --min-active-shops moet minimaal 1 zijn.")
    if args.max_candidate_links < 1:
        raise SystemExit("[ERROR] --max-candidate-links moet minimaal 1 zijn.")
    if args.timeout <= 0:
        raise SystemExit("[ERROR] --timeout moet positief zijn.")
    if args.sleep < 0:
        raise SystemExit("[ERROR] --sleep mag niet negatief zijn.")
    if args.deactivate_stale_after_hours <= 0:
        raise SystemExit("[ERROR] --deactivate-stale-after-hours moet positief zijn.")
    if args.min_offers_to_write < 0:
        raise SystemExit("[ERROR] --min-offers-to-write mag niet negatief zijn.")


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)

    cohort = resolve_cohort(args.cohort, args.cohort_count)

    log(
        "GROOVESPIN-EAN-PROBE-START",
        {
            "route": "vinylofy_priority_eans_first",
            "limit": args.limit,
            "cohort": cohort,
            "cohort_count": args.cohort_count,
            "min_active_shops": args.min_active_shops,
            "write": args.write,
        },
    )

    targets = select_probe_targets(
        limit=args.limit,
        cohort_count=args.cohort_count,
        cohort=cohort,
        min_active_shops=args.min_active_shops,
    )

    log(
        "GROOVESPIN-SELECTION-DONE",
        {
            "targets": len(targets),
            "first_rank": targets[0].popularity_rank if targets else None,
            "last_rank": targets[-1].popularity_rank if targets else None,
        },
    )

    if not targets:
        raise SystemExit("[ERROR] Geen Vinylofy EANs geselecteerd voor Groovespin probe.")

    selection_path = Path(args.selection_csv)
    write_selection_csv(selection_path, targets)
    log("GROOVESPIN-SELECTION-CSV", str(selection_path))

    session = build_session()

    form_templates = discover_form_search_templates(
        session,
        timeout=args.timeout,
        sleep=args.sleep,
    )

    explicit_templates = list(args.search_url_template or []) + DEFAULT_SEARCH_URL_TEMPLATES

    offers: list[ListingOffer] = []
    miss_reasons: dict[str, int] = {}

    for idx, target in enumerate(targets, start=1):
        result = probe_one_target(
            session,
            target,
            form_templates=form_templates,
            explicit_templates=explicit_templates,
            timeout=args.timeout,
            sleep=args.sleep,
            max_candidate_links=args.max_candidate_links,
        )

        if result.offer:
            offers.append(result.offer)
            log(
                "GROOVESPIN-OFFER",
                {
                    "idx": idx,
                    "rank": target.popularity_rank,
                    "ean": target.ean,
                    "artist": target.artist,
                    "title": target.title,
                    "price": result.price_raw,
                    "detail_url": result.detail_url,
                    "offers": len(offers),
                },
            )
        else:
            reason = result.reason or "unknown"
            miss_reasons[reason] = miss_reasons.get(reason, 0) + 1
            log(
                "GROOVESPIN-MISS",
                {
                    "idx": idx,
                    "rank": target.popularity_rank,
                    "ean": target.ean,
                    "artist": target.artist,
                    "title": target.title,
                    "reason": reason,
                    "search_url": result.search_url,
                    "detail_url": result.detail_url,
                    "found_ean": result.found_ean,
                },
            )

    log(
        "GROOVESPIN-PROBE-DONE",
        {
            "targets": len(targets),
            "offers": len(offers),
            "miss_reasons": miss_reasons,
            "write": args.write,
        },
    )

    if args.write and len(offers) < args.min_offers_to_write:
        raise SystemExit(
            f"[ERROR] Slechts {len(offers)} offers gevonden; onder min-offers-to-write={args.min_offers_to_write}. "
            "Geen writes/deactivatie uitgevoerd."
        )

    with db_connection() as conn:
        stats = sync_ean_exact_offers(conn, offers, write=args.write)

    log("GROOVESPIN-SYNC-DONE", vars(stats))

    if args.write and not args.skip_deactivate:
        stale_count = deactivate_stale_groovespin_offers(
            hours=args.deactivate_stale_after_hours,
            write=True,
        )
        log(
            "GROOVESPIN-STALE-DEACTIVATE",
            {
                "eligible": stale_count,
                "hours": args.deactivate_stale_after_hours,
                "write": True,
            },
        )
    elif not args.write:
        log("GROOVESPIN-DRY-RUN", "Geen databasewrites en geen stale-deactivatie uitgevoerd.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
