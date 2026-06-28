#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urljoin, urlparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

from scripts.importers.common import normalize_ean, normalize_text, parse_price
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "imusic"
BASE_URL = "https://imusic.nl"
SEARCH_PATH = "/page/search"
DEFAULT_OFFSET_STEP = 30
DEFAULT_DELAY_SECONDS = 0.50
DEFAULT_TIMEOUT_SECONDS = 20.0

PRODUCT_HREF_RE = re.compile(r"/music/([0-9][0-9\-\s]{7,20})(?:/|$|\?|#)")
EURO_PRICE_RE = re.compile(r"€\s*([0-9]+(?:[.,][0-9]{2}))")

AVAILABILITY_MARKERS = (
    "op voorraad",
    "weinig op voorraad",
    "besteld in een afgelegen magazijn",
    "verwachte levering",
    "koop",
    "pre-order",
    "preorder",
    "voorbestelling",
    "uitverkocht",
    "niet leverbaar",
)


@dataclass(frozen=True)
class GenreSpec:
    genre_id: str
    name: str | None = None


def clean(value: object) -> str:
    return normalize_text(value) or ""


def parse_genre_specs(value: str) -> list[GenreSpec]:
    specs: list[GenreSpec] = []

    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue

        if ":" in part:
            genre_id, name = part.split(":", 1)
            genre_id = genre_id.strip()
            name = name.strip() or None
        else:
            genre_id = part.strip()
            name = None

        if not genre_id.isdigit():
            raise SystemExit(
                f"[ERROR] Ongeldige genreId '{genre_id}'. Gebruik bv. 16 of 16637:Indie."
            )

        specs.append(GenreSpec(genre_id=genre_id, name=name))

    if not specs:
        raise SystemExit("[ERROR] Geef minimaal één genreId via --genre-ids.")

    return specs


def build_search_url(
    *,
    genre_id: str,
    offset: int,
    only_local_stock: bool,
    sort: str,
    media_group_id: str,
) -> str:
    params = {
        "_form": "searchForm",
        "advanced": "1",
        "combined": "",
        "artist": "",
        "title": "",
        "tracks": "",
        "composer": "",
        "released": "",
        "releaseDate": "",
        "label": "",
        "releaseCountryId": "",
        "releaseCode": "",
        "genreId": genre_id,
        "mediaGroupId": media_group_id,
        "mediaId": "",
        "languageId": "",
        "subtitleId": "",
        "priceRange": "",
        "price": "",
        "discountPercent": "",
        "onlyLocalStock": "1" if only_local_stock else "",
        "sort": sort,
        "search": "",
    }

    if offset > 0:
        params["offset"] = str(offset)

    return f"{BASE_URL}{SEARCH_PATH}?{urlencode(params)}#tbl"


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; Vinylofy-iMusic-GenreDiscovery/1.0; "
                "+https://vinylofy.nl)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.6",
        }
    )
    return session


def ean_from_href(href: object) -> str | None:
    href_text = clean(href)
    if not href_text:
        return None

    parsed_href = urlparse(href_text)
    href_path = parsed_href.path or href_text

    match = PRODUCT_HREF_RE.search(href_path)
    if not match:
        match = PRODUCT_HREF_RE.search(href_text)

    if not match:
        return None

    return normalize_ean(match.group(1))


def context_text_until_next_product(anchor: Tag, current_ean: str) -> str:
    """
    Pak de listingtekst vanaf deze productlink tot aan de volgende ANDERE productlink.

    Dit is belangrijk bij iMusic: één zoekresultaat bevat titel/voorraad/prijs in
    documentvolgorde. Omhoog klimmen naar parent nodes pakt te snel de hele pagina,
    waardoor één prijs abusievelijk op alle producten terechtkomt.
    """
    parts: list[str] = []

    anchor_text = clean(anchor.get_text(" ", strip=True))
    if anchor_text:
        parts.append(anchor_text)

    for node in anchor.next_elements:
        if node is anchor:
            continue

        if isinstance(node, Tag):
            if node.name == "a" and node.has_attr("href"):
                next_ean = ean_from_href(node.get("href"))
                if next_ean and next_ean != current_ean:
                    break

            # Sla script/style/nav-achtige ruis over.
            if node.name in {"script", "style", "noscript", "svg"}:
                continue

        if isinstance(node, NavigableString):
            value = clean(str(node))
            if value:
                parts.append(value)

        joined = " | ".join(parts)

        # Niet stoppen op alleen "Op voorraad" + prijs:
        # bij sale-producten staat vaak eerst "Normale prijs € X"
        # en pas daarna de actuele prijs + "Koop".
        low = joined.lower()
        if "€" in joined and (
            "koop" in low
            or "pre-order" in low
            or "preorder" in low
            or "voorbestelling" in low
        ):
            if len(joined) > 80:
                break

        # Hard stop tegen per ongeluk doorlopen naar footer/paginering.
        # Ruimer dan eerst, zodat saleprijs na "Normale prijs" mee kan komen.
        if len(joined) > 2200:
            break

    return " | ".join(parts)


def extract_price_hint(text: str) -> str | None:
    matches = EURO_PRICE_RE.findall(text)
    valid: list[str] = []

    for match in matches:
        if parse_price(match) is not None:
            valid.append(match)

    if not valid:
        return None

    # iMusic listings kunnen "Normale prijs € X" + actuele lagere prijs tonen.
    # De actuele verkoopprijs staat doorgaans later in het productblok.
    return valid[-1]


def extract_availability_hint(text: str) -> str | None:
    low = text.lower()
    if not any(marker in low for marker in AVAILABILITY_MARKERS):
        return None

    parts = [clean(part) for part in re.split(r"\s*\|\s*|\s{2,}", text)]
    snippets = [
        part
        for part in parts
        if part and any(marker in part.lower() for marker in AVAILABILITY_MARKERS)
    ]

    if snippets:
        return " | ".join(snippets[:4])

    found = [marker for marker in AVAILABILITY_MARKERS if marker in low]
    return " | ".join(found[:4]) if found else None


def extract_title_hint(anchor: Tag, context_text: str) -> str | None:
    link_text = clean(anchor.get_text(" ", strip=True))
    if link_text and len(link_text) > 3:
        return link_text[:300]

    title_attr = clean(anchor.get("title"))
    if title_attr:
        return title_attr[:300]

    aria = clean(anchor.get("aria-label"))
    if aria:
        return aria[:300]

    if context_text:
        return context_text[:300]

    return None


def parse_listing_links(
    html: str,
    *,
    listing_url: str,
    genre: GenreSpec,
    offset: int,
) -> list[DiscoveredLink]:
    soup = BeautifulSoup(html, "html.parser")
    links_by_ean: dict[str, DiscoveredLink] = {}

    for position, anchor in enumerate(soup.find_all("a", href=True), start=1):
        href = clean(anchor.get("href"))
        if not href:
            continue

        ean = ean_from_href(href)
        if not ean:
            continue

        listing_product_url = urljoin(BASE_URL, href)
        context_text = context_text_until_next_product(anchor, ean)

        price_hint = extract_price_hint(context_text)
        availability_hint = extract_availability_hint(context_text)
        title_hint = extract_title_hint(anchor, context_text)

        payload: dict[str, Any] = {
            "source": "imusic_genre_listing",
            "genre_id": genre.genre_id,
            "genre_name": genre.name,
            "offset": offset,
            "listing_url": listing_url,
            "listing_product_url": listing_product_url,
            "listing_position": position,
            "ean": ean,
        }

        if title_hint:
            payload["listing_title_hint"] = title_hint
        if price_hint:
            payload["listing_price_raw"] = price_hint
            payload["listing_currency"] = "EUR"
            payload["listing_price_source"] = "genre_listing"
        if availability_hint:
            payload["listing_availability_hint"] = availability_hint
        if context_text:
            payload["listing_context_sample"] = context_text[:1000]

        # Canonical queue-url gelijk aan seed_imusic_ean_links:
        # voorkomt dubbele links voor /music/<EAN> en /music/<EAN>/<slug>.
        queue_url = f"{BASE_URL}/music/{ean}"

        links_by_ean[ean] = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=queue_url,
            source_product_id=ean,
            payload=payload,
        )

    return list(links_by_ean.values())


def discover_genre_links(
    *,
    genre_specs: list[GenreSpec],
    max_pages_per_genre: int | None,
    offset_step: int,
    only_local_stock: bool,
    sort: str,
    media_group_id: str,
    delay_seconds: float,
    timeout_seconds: float,
) -> list[DiscoveredLink]:
    session = build_session()
    all_links_by_ean: dict[str, DiscoveredLink] = {}

    for genre in genre_specs:
        pages_processed = 0
        offset = 0
        seen_offsets: set[int] = set()

        while max_pages_per_genre is None or pages_processed < max_pages_per_genre:
            if offset in seen_offsets:
                print(
                    "[IMUSIC-GENRE-DISCOVER] stop duplicate offset",
                    {"genre_id": genre.genre_id, "genre_name": genre.name, "offset": offset},
                    flush=True,
                )
                break

            seen_offsets.add(offset)
            listing_url = build_search_url(
                genre_id=genre.genre_id,
                offset=offset,
                only_local_stock=only_local_stock,
                sort=sort,
                media_group_id=media_group_id,
            )

            try:
                response = session.get(
                    listing_url,
                    timeout=timeout_seconds,
                    allow_redirects=True,
                )
            except requests.RequestException as exc:
                print(
                    "[IMUSIC-GENRE-ERROR]",
                    {
                        "genre_id": genre.genre_id,
                        "genre_name": genre.name,
                        "offset": offset,
                        "url": listing_url,
                        "error": str(exc),
                    },
                    flush=True,
                )
                break

            if response.status_code >= 400:
                print(
                    "[IMUSIC-GENRE-HTTP-SKIP]",
                    {
                        "genre_id": genre.genre_id,
                        "genre_name": genre.name,
                        "offset": offset,
                        "url": listing_url,
                        "status_code": response.status_code,
                    },
                    flush=True,
                )
                break

            links = parse_listing_links(
                response.text,
                listing_url=response.url,
                genre=genre,
                offset=offset,
            )

            new_count = 0
            for link in links:
                ean = link.source_product_id or ""
                if ean not in all_links_by_ean:
                    new_count += 1
                all_links_by_ean[ean] = link

            print(
                "[IMUSIC-GENRE-PAGE]",
                {
                    "genre_id": genre.genre_id,
                    "genre_name": genre.name,
                    "offset": offset,
                    "status_code": response.status_code,
                    "page_links": len(links),
                    "new_links_added": new_count,
                    "unique_total": len(all_links_by_ean),
                    "url": response.url,
                },
                flush=True,
            )

            pages_processed += 1

            if not links:
                print(
                    "[IMUSIC-GENRE-DISCOVER] stop empty page",
                    {
                        "genre_id": genre.genre_id,
                        "genre_name": genre.name,
                        "offset": offset,
                    },
                    flush=True,
                )
                break

            offset += offset_step

            if delay_seconds > 0:
                time.sleep(delay_seconds)

    return list(all_links_by_ean.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Discover iMusic vinyl productlinks via genrepagina's. "
            "Schrijft alleen shop_product_links; detail_imusic bevestigt later EAN/UPC."
        )
    )
    parser.add_argument(
        "--genre-ids",
        required=True,
        help='Comma-separated genreIds, bv. "16:Blues,16637:Indie" of "16,16637".',
    )
    parser.add_argument(
        "--max-pages-per-genre",
        type=int,
        default=1,
        help="Aantal offsetpagina's per genre; 1 = alleen offset 0.",
    )
    parser.add_argument(
        "--offset-step",
        type=int,
        default=DEFAULT_OFFSET_STEP,
        help="iMusic pagineert momenteel in stappen van 30.",
    )
    parser.add_argument(
        "--sort",
        default="relevance",
        help="iMusic sort-parameter, standaard relevance.",
    )
    parser.add_argument(
        "--media-group-id",
        default="6",
        help="iMusic mediaGroupId; 6 is vinyl/LP volgens de huidige URL's.",
    )
    parser.add_argument(
        "--include-non-local-stock",
        action="store_true",
        help="Laat onlyLocalStock leeg in plaats van onlyLocalStock=1.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DEFAULT_DELAY_SECONDS,
        help="Pauze tussen listingpagina's.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout per listingpagina.",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Werkelijk naar shop_product_links schrijven.",
    )
    return parser


def validate_args(args: argparse.Namespace) -> None:
    if args.max_pages_per_genre < 1:
        raise SystemExit("[ERROR] --max-pages-per-genre moet minimaal 1 zijn.")
    if args.offset_step < 1:
        raise SystemExit("[ERROR] --offset-step moet minimaal 1 zijn.")
    if args.delay < 0:
        raise SystemExit("[ERROR] --delay mag niet negatief zijn.")
    if args.timeout <= 0:
        raise SystemExit("[ERROR] --timeout moet positief zijn.")
    if not args.media_group_id.isdigit():
        raise SystemExit("[ERROR] --media-group-id moet numeriek zijn.")


def main() -> int:
    args = build_parser().parse_args()
    validate_args(args)

    genre_specs = parse_genre_specs(args.genre_ids)

    print(
        "[IMUSIC-GENRE-DISCOVER] start",
        {
            "genre_ids": [genre.genre_id for genre in genre_specs],
            "genre_names": {genre.genre_id: genre.name for genre in genre_specs if genre.name},
            "max_pages_per_genre": args.max_pages_per_genre,
            "offset_step": args.offset_step,
            "only_local_stock": not args.include_non_local_stock,
            "sort": args.sort,
            "media_group_id": args.media_group_id,
            "write": args.write,
        },
        flush=True,
    )

    links = discover_genre_links(
        genre_specs=genre_specs,
        max_pages_per_genre=args.max_pages_per_genre,
        offset_step=args.offset_step,
        only_local_stock=not args.include_non_local_stock,
        sort=args.sort,
        media_group_id=args.media_group_id,
        delay_seconds=args.delay,
        timeout_seconds=args.timeout,
    )

    print(
        "[IMUSIC-GENRE-DISCOVER] collected",
        {
            "unique_links": len(links),
            "write": args.write,
        },
        flush=True,
    )

    for link in links[:20]:
        payload = link.payload or {}
        print(
            "[IMUSIC-GENRE-SAMPLE]",
            {
                "source_url": link.source_url,
                "source_product_id": link.source_product_id,
                "genre_id": payload.get("genre_id"),
                "genre_name": payload.get("genre_name"),
                "listing_price_raw": payload.get("listing_price_raw"),
                "listing_currency": payload.get("listing_currency"),
                "listing_price_source": payload.get("listing_price_source"),
                "listing_availability_hint": payload.get("listing_availability_hint"),
                "listing_title_hint": payload.get("listing_title_hint"),
                "listing_product_url": payload.get("listing_product_url"),
            },
            flush=True,
        )

    if not args.write:
        print("[IMUSIC-GENRE-DISCOVER] dry-run complete; geen databasewrites.", flush=True)
        return 0

    result = upsert_discovered_links(links)
    print("[IMUSIC-GENRE-DISCOVER] registry", vars(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
