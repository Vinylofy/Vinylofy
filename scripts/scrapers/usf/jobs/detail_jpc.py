#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from scripts.importers.common import normalize_ean, normalize_text, parse_price
from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.link_registry import (
    insert_raw_shop_scrape,
    mark_detail_ean_found,
    mark_detail_scraped,
)

SHOP_ID = "jpc"
BASE_URL = "https://www.jpc.de"
DEFAULT_DELAY_SECONDS = 4.0
DEFAULT_TIMEOUT_SECONDS = 25.0

EURO_PRICE_RE = re.compile(r"(?:EUR|€)\s*([0-9]+(?:[.,][0-9]{2}))", flags=re.I)
HNUM_RE = re.compile(r"/hnum/([0-9]+)(?:[/?#]|$)", flags=re.I)


@dataclass(frozen=True)
class ParsedJpcOffer:
    title: str | None
    artist: str | None
    title_raw: str | None
    ean: str | None
    price_raw: str | None
    availability: str
    availability_text: str | None
    image_url: str | None
    format_label: str | None
    article_number: str | None
    canonical_url: str
    final_url: str
    http_status: int


def clean(value: object) -> str:
    return normalize_text(value) or ""


def text_lines(soup: BeautifulSoup) -> list[str]:
    return [clean(line) for line in soup.get_text("\n").splitlines() if clean(line)]


def meta_content(soup: BeautifulSoup, *keys: str) -> str | None:
    for key in keys:
        node = soup.find("meta", attrs={"property": key}) or soup.find(
            "meta", attrs={"name": key}
        )
        if node and node.get("content"):
            return clean(node.get("content"))
    return None


def extract_canonical_url(soup: BeautifulSoup, fallback_url: str) -> str:
    node = soup.find(
        "link",
        attrs={"rel": lambda value: value and "canonical" in str(value).lower()},
    )
    href = clean(node.get("href")) if node else ""
    return urljoin(fallback_url, href) if href else fallback_url


def extract_image_url(soup: BeautifulSoup, base_url: str) -> str | None:
    image = meta_content(soup, "og:image", "twitter:image")
    if image:
        return urljoin(base_url, image)

    for node in soup.find_all("img"):
        src = clean(node.get("src"))
        alt = clean(node.get("alt")).lower()
        if src and ("cover" in alt or "image" in alt or "bild" in alt):
            return urljoin(base_url, src)

    return None


def extract_hnum_from_url(url: str) -> str | None:
    match = HNUM_RE.search(urlparse(url).path)
    return match.group(1) if match else None


def extract_article_number(lines: list[str], final_url: str) -> str | None:
    hnum = extract_hnum_from_url(final_url)
    if hnum:
        return hnum

    for index, line in enumerate(lines):
        if "artikelnummer" not in line.lower():
            continue

        inline = re.search(r"\b([0-9]{5,})\b", line)
        if inline:
            return inline.group(1)

        for candidate in lines[index + 1 : index + 4]:
            found = re.search(r"\b([0-9]{5,})\b", candidate)
            if found:
                return found.group(1)

    return None


def extract_ean(soup: BeautifulSoup, lines: list[str]) -> str | None:
    text = soup.get_text(" ", strip=True)
    for pattern in (
        r"(?:UPC/EAN|EAN/UPC|EAN)\s*:?\s*([0-9][0-9\-\s]{7,24})",
        r"\bGTIN\s*:?\s*([0-9][0-9\-\s]{7,24})",
    ):
        match = re.search(pattern, text, flags=re.I)
        if match:
            return normalize_ean(match.group(1))

    for index, line in enumerate(lines):
        if not any(marker in line.upper() for marker in ("UPC/EAN", "EAN/UPC", "EAN")):
            continue

        inline = re.search(r"([0-9][0-9\-\s]{7,24})", line)
        if inline:
            return normalize_ean(inline.group(1))

        for candidate in lines[index + 1 : index + 4]:
            found = re.search(r"([0-9][0-9\-\s]{7,24})", candidate)
            if found:
                return normalize_ean(found.group(1))

    return None


def extract_price(lines: list[str]) -> tuple[str | None, int | None]:
    for index, line in enumerate(lines):
        if "aktueller preis" not in line.lower():
            continue
        matches = EURO_PRICE_RE.findall(line)
        valid = [match for match in matches if parse_price(match) is not None]
        if valid:
            return valid[-1], index

        for candidate in lines[max(0, index - 3) : index + 3]:
            matches = EURO_PRICE_RE.findall(candidate)
            valid = [match for match in matches if parse_price(match) is not None]
            if valid:
                return valid[-1], index

    all_valid: list[tuple[str, int]] = []
    for index, line in enumerate(lines[:120]):
        if "vorheriger preis" in line.lower():
            continue
        for match in EURO_PRICE_RE.findall(line):
            if parse_price(match) is not None:
                all_valid.append((match, index))

    if all_valid:
        return all_valid[0]

    return None, None


def extract_availability(lines: list[str], price_index: int | None) -> tuple[str, str | None]:
    search_lines = lines[:80]
    if price_index is not None:
        search_lines = lines[max(0, price_index - 12) : price_index + 12]

    raw = " | ".join(search_lines)
    low = raw.lower()

    if "noch nicht erschienen" in low or "lieferbar ab" in low:
        return "preorder", raw[:500]
    if (
        "benachrichtigung anfordern" in low
        or "nicht erhältlich" in low
        or "nicht erhaltlich" in low
        or "nicht lieferbar" in low
    ):
        return "out_of_stock", raw[:500]
    if (
        "artikel am lager" in low
        or "innerhalb 24 stunden" in low
        or "innerhalb von 24 stunden" in low
        or "innerhalb 3 tagen" in low
        or "innerhalb von 3 tagen" in low
    ):
        return "in_stock", raw[:500]
    if "lieferbar" in low or "innerhalb" in low:
        return "out_of_stock", raw[:500]

    return "unknown", raw[:500] if raw else None


def split_artist_title(value: str | None) -> tuple[str | None, str | None]:
    text = clean(value)
    if not text:
        return None, None

    text = re.sub(r"\s*(?:-|–)\s*jpc\.de\s*$", "", text, flags=re.I)
    text = re.sub(r"\s+online kaufen\s*$", "", text, flags=re.I)

    if ":" in text:
        artist, title = text.split(":", 1)
        return clean(artist) or None, clean(title) or None

    for sep in (" – ", " - "):
        if sep in text:
            artist, title = text.split(sep, 1)
            return clean(artist) or None, clean(title) or None

    return None, text


def extract_title_artist(soup: BeautifulSoup, lines: list[str]) -> tuple[str | None, str | None]:
    for value in (
        meta_content(soup, "og:title", "twitter:title"),
        soup.title.string if soup.title and soup.title.string else None,
    ):
        artist, title = split_artist_title(value)
        if title:
            return title, artist

    heading = soup.find("h1")
    if heading:
        artist, title = split_artist_title(heading.get_text(" ", strip=True))
        if title:
            return title, artist

    for line in lines[:30]:
        artist, title = split_artist_title(line)
        if title and artist:
            return title, artist

    return (lines[0] if lines else None), None


def extract_format(lines: list[str], title: str | None) -> str | None:
    combined = " ".join(lines[:100])
    title_text = clean(title)

    for text in (title_text, combined):
        for pattern, label in (
            (r"\b4\s*LPs?\b", "4LP"),
            (r"\b3\s*LPs?\b", "3LP"),
            (r"\b2\s*LPs?\b", "2LP"),
            (r"\bLPs?\b", "LP"),
            (r"\bSingle\s*12\"", "12 inch"),
            (r"\bSingle\s*10\"", "10 inch"),
            (r"\bSingle\s*7\"", "7 inch"),
            (r"\bVinyl\b", "LP"),
        ):
            if re.search(pattern, text, flags=re.I):
                return label

    return None


def parse_offer(html: str, *, final_url: str, status_code: int) -> ParsedJpcOffer:
    soup = BeautifulSoup(html, "html.parser")
    lines = text_lines(soup)

    canonical_url = extract_canonical_url(soup, final_url)
    image_url = extract_image_url(soup, canonical_url)
    ean = extract_ean(soup, lines)
    price_raw, price_index = extract_price(lines)
    availability, availability_text = extract_availability(lines, price_index)
    title, artist = extract_title_artist(soup, lines)
    format_label = extract_format(lines, title)
    article_number = extract_article_number(lines, final_url)

    title_raw = None
    if artist and title:
        title_raw = f"{artist} - {title}"
    elif title:
        title_raw = title

    return ParsedJpcOffer(
        title=title,
        artist=artist,
        title_raw=title_raw,
        ean=ean,
        price_raw=price_raw,
        availability=availability,
        availability_text=availability_text,
        image_url=image_url,
        format_label=format_label,
        article_number=article_number,
        canonical_url=canonical_url,
        final_url=final_url,
        http_status=status_code,
    )


def registry_payload_from_link(link: dict[str, Any]) -> dict[str, Any]:
    payload = link.get("payload") or {}
    return payload if isinstance(payload, dict) else {}


def listing_price_from_payload(payload: dict[str, Any]) -> str | None:
    value = clean(payload.get("listing_price_raw") or payload.get("price_raw"))
    if value and parse_price(value) is not None:
        return value
    return None


def listing_availability_from_payload(payload: dict[str, Any]) -> tuple[str | None, str | None]:
    availability = clean(payload.get("listing_availability"))
    raw = clean(payload.get("listing_availability_raw"))
    if availability in {"in_stock", "out_of_stock", "preorder", "unknown"}:
        return availability, raw or None
    return None, raw or None


def get_missing_ean_links_for_detail_scrape(limit: int) -> list[dict[str, Any]]:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                r"""
                select l.id, l.shop_id, l.source_url, l.source_product_id, l.payload
                from public.shop_product_links l
                where l.shop_id = %s
                  and l.status = 'active'
                  and l.last_detail_scraped_at is null
                  and nullif(l.payload->>'last_successful_ean', '') is null
                  and not exists (
                      select 1
                      from public.raw_shop_scrapes r
                      where r.shop_id = l.shop_id
                        and r.source_product_id = l.source_product_id
                        and regexp_replace(coalesce(r.ean_raw, ''), '\D', '', 'g')
                            ~ '^(\d{8}|\d{12}|\d{13}|\d{14})$'
                  )
                order by
                    case
                        when l.payload->>'detail_priority' = 'high' then 0
                        else 1
                    end,
                    l.last_seen_at desc nulls last,
                    l.first_seen_at asc
                limit %s
                """,
                (SHOP_ID, limit),
            )
            rows = cur.fetchall()

    return [
        {
            "id": str(row[0]),
            "shop_id": row[1],
            "source_url": row[2],
            "source_product_id": row[3],
            "payload": row[4],
        }
        for row in rows
    ]


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; Vinylofy-JPC-EANBatch/1.0; "
                "+https://vinylofy.nl)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,de;q=0.8,en;q=0.6",
        }
    )
    return session


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verrijk JPC productlinks batchgewijs met EANs vanaf detailpagina's."
    )
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--sleep", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.limit < 1:
        raise SystemExit("[ERROR] --limit moet minimaal 1 zijn.")
    if args.timeout <= 0:
        raise SystemExit("[ERROR] --timeout moet positief zijn.")
    if args.sleep < 0:
        raise SystemExit("[ERROR] --sleep mag niet negatief zijn.")

    links = get_missing_ean_links_for_detail_scrape(limit=args.limit)
    session = build_session()

    stats = {
        "queued": len(links),
        "fetched": 0,
        "offers": 0,
        "skipped": 0,
        "errors": 0,
        "write": args.write,
    }

    for link in links:
        source_url = link["source_url"]
        expected_hnum = clean(link.get("source_product_id")) or None

        try:
            response = session.get(
                source_url,
                timeout=args.timeout,
                allow_redirects=True,
            )
            stats["fetched"] += 1
        except requests.RequestException as exc:
            stats["errors"] += 1
            print(
                "[JPC-DETAIL-ERROR]",
                {"source_url": source_url, "hnum": expected_hnum, "error": str(exc)},
                flush=True,
            )
            time.sleep(args.sleep)
            continue

        if response.status_code == 429:
            stats["errors"] += 1
            print(
                "[JPC-DETAIL-RATE-LIMIT]",
                {"source_url": source_url, "hnum": expected_hnum},
                flush=True,
            )
            break

        if response.status_code >= 400:
            stats["skipped"] += 1
            print(
                "[JPC-DETAIL-SKIP]",
                {
                    "source_url": source_url,
                    "final_url": response.url,
                    "hnum": expected_hnum,
                    "status_code": response.status_code,
                    "reason": "http_status",
                },
                flush=True,
            )
            if args.write:
                mark_detail_scraped(link["id"])
            time.sleep(args.sleep)
            continue

        parsed = parse_offer(
            response.text,
            final_url=response.url,
            status_code=response.status_code,
        )

        if expected_hnum and parsed.article_number and parsed.article_number != expected_hnum:
            stats["skipped"] += 1
            print(
                "[JPC-DETAIL-SKIP]",
                {
                    "source_url": source_url,
                    "final_url": response.url,
                    "expected_hnum": expected_hnum,
                    "found_hnum": parsed.article_number,
                    "reason": "hnum_mismatch",
                },
                flush=True,
            )
            if args.write:
                mark_detail_scraped(link["id"])
            time.sleep(args.sleep)
            continue

        if not parsed.ean:
            stats["skipped"] += 1
            print(
                "[JPC-DETAIL-SKIP]",
                {
                    "source_url": source_url,
                    "final_url": response.url,
                    "hnum": expected_hnum,
                    "reason": "missing_ean",
                },
                flush=True,
            )
            if args.write:
                mark_detail_scraped(link["id"])
            time.sleep(args.sleep)
            continue

        registry_payload = registry_payload_from_link(link)
        listing_price_raw = listing_price_from_payload(registry_payload)
        listing_availability, listing_availability_text = listing_availability_from_payload(
            registry_payload
        )

        final_price_raw = listing_price_raw or parsed.price_raw
        final_price_source = "jpc_listing" if listing_price_raw else "detail_page"

        final_availability = listing_availability or parsed.availability
        final_availability_text = listing_availability_text or parsed.availability_text
        final_availability_source = (
            "jpc_listing" if listing_availability else "detail_page"
        )

        if not final_price_raw:
            stats["skipped"] += 1
            print(
                "[JPC-DETAIL-SKIP]",
                {
                    "source_url": source_url,
                    "final_url": response.url,
                    "ean": parsed.ean,
                    "reason": "missing_price",
                },
                flush=True,
            )
            if args.write:
                mark_detail_scraped(link["id"])
            time.sleep(args.sleep)
            continue

        raw_payload = {
            "source": "jpc_ean_detail",
            "shop_product_link_id": link["id"],
            "registry_source_url": source_url,
            "final_url": response.url,
            "canonical_url": parsed.canonical_url,
            "http_status": parsed.http_status,
            "hnum": expected_hnum or parsed.article_number,
            "article_number": parsed.article_number,
            "found_ean": parsed.ean,
            "artist": parsed.artist,
            "title": parsed.title,
            "format": parsed.format_label or registry_payload.get("format"),
            "price_source": final_price_source,
            "listing_price_raw": listing_price_raw,
            "detail_price_raw": parsed.price_raw,
            "availability_source": final_availability_source,
            "availability_text": final_availability_text,
            "detail_availability": parsed.availability,
            "detail_availability_text": parsed.availability_text,
            "listing_payload": registry_payload,
            "listing_price_and_availability_are_authoritative": bool(
                listing_price_raw or listing_availability
            ),
        }

        raw_id = None
        if args.write:
            raw_id = insert_raw_shop_scrape(
                run_id=None,
                shop_id=SHOP_ID,
                source_url=parsed.canonical_url or response.url,
                source_product_id=expected_hnum or parsed.article_number,
                title_raw=parsed.title_raw,
                ean_raw=parsed.ean,
                price_raw=final_price_raw,
                availability_raw=final_availability,
                image_url_raw=parsed.image_url,
                payload=raw_payload,
            )
            mark_detail_ean_found(link["id"], parsed.ean)

        stats["offers"] += 1
        print(
            "[JPC-DETAIL-OFFER]",
            {
                "source_url": source_url,
                "final_url": response.url,
                "raw_id": raw_id,
                "hnum": expected_hnum or parsed.article_number,
                "ean": parsed.ean,
                "title_raw": parsed.title_raw,
                "price_raw": final_price_raw,
                "price_source": final_price_source,
                "availability": final_availability,
                "availability_source": final_availability_source,
                "format": raw_payload["format"],
                "write": args.write,
            },
            flush=True,
        )

        time.sleep(args.sleep)

    print("[JPC-DETAIL-SUMMARY]", stats, flush=True)
    if not args.write:
        print("[JPC-DETAIL] dry-run complete; geen databasewrites.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
