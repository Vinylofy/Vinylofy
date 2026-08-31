#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

from scripts.importers.common import normalize_text, parse_price
from scripts.scrapers.usf.core.link_registry import upsert_discovered_links
from scripts.scrapers.usf.core.models import DiscoveredLink

SHOP_ID = "jpc"
BASE_URL = "https://www.jpc.de"
VINYL_HOME_URL = f"{BASE_URL}/jpcng/vinyl/home"
DEFAULT_DELAY_SECONDS = 4.0
DEFAULT_TIMEOUT_SECONDS = 25.0

PRODUCT_HREF_RE = re.compile(
    r"/jpcng/[^?#]+/detail/-/art/[^?#]+/hnum/([0-9]+)(?:[/?#]|$)",
    flags=re.I,
)
VINYL_TAXONOMY_CID_RE = re.compile(r"^/(?:s|ff)/1238692_[0-9]+$", flags=re.I)
EURO_PRICE_RE = re.compile(r"(?:EUR|€)\s*([0-9]+(?:[.,][0-9]{2}))", flags=re.I)
VINYL_MEDIA_RE = re.compile(
    r"\b(?:[0-9]+\s*)?LPs?\b|\bSingle\s*(?:7|10|12)\"|\bVinyl\b",
    flags=re.I,
)
NON_VINYL_MEDIA_RE = re.compile(
    r"\b(?:CD|DVD|Blu-ray|SACD|Super Audio CD|MC)\b",
    flags=re.I,
)

DEFAULT_ROUTE_SPECS = (
    "rock=https://www.jpc.de/s/1238692_66733?searchtype=cid",
    "pop=https://www.jpc.de/s/1238692_66726?searchtype=cid",
    "metal=https://www.jpc.de/s/1238692_66718?searchtype=cid",
    "soul-funk=https://www.jpc.de/s/1238692_66740?searchtype=cid",
)

EXCLUDED_ROUTE_LABEL_MARKERS = {
    "zubehor",
    "zubehoer",
    "zeitschriften",
    "pflege",
    "aufbewahrung",
    "test records",
    "schweberahmen",
    "geschenkgutscheine",
}


@dataclass(frozen=True)
class RouteSpec:
    name: str
    url: str
    source: str = "configured"


def clean(value: object) -> str:
    return normalize_text(value) or ""


def normalize_route_label(value: str) -> str:
    text = clean(value).lower()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue")
    text = text.replace("ß", "ss")
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return text.strip("-") or "route"


def parse_route_specs(value: str | None) -> list[RouteSpec]:
    if not value or value.strip().lower() == "default":
        return [
            RouteSpec(name=part.split("=", 1)[0], url=part.split("=", 1)[1])
            for part in DEFAULT_ROUTE_SPECS
        ]

    specs: list[RouteSpec] = []
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue

        if "=" in part:
            name, url = part.split("=", 1)
        else:
            url = part
            name = urlparse(url).path.rsplit("/", 1)[-1] or "route"

        url = urljoin(BASE_URL, clean(url))
        if not url.startswith(BASE_URL):
            raise SystemExit(f"[ERROR] JPC route buiten domein: {url}")

        specs.append(RouteSpec(name=normalize_route_label(name), url=url))

    if not specs:
        raise SystemExit("[ERROR] Geef minimaal een JPC route of gebruik default.")

    return specs


def canonical_taxonomy_route_url(href: str) -> str | None:
    parsed = urlparse(href)
    if parsed.netloc and parsed.netloc.lower() not in {"www.jpc.de", "jpc.de"}:
        return None
    if not VINYL_TAXONOMY_CID_RE.match(parsed.path):
        return None

    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if params.get("searchtype") != "cid":
        return None

    return urlunparse(
        (
            "https",
            "www.jpc.de",
            parsed.path.replace("/ff/", "/s/", 1),
            "",
            urlencode({"searchtype": "cid"}),
            "",
        )
    )


def ff_page_url(route_url: str, *, page_number: int) -> str | None:
    parsed = urlparse(route_url)
    match = re.match(r"^/s/(1238692_[0-9]+)$", parsed.path, flags=re.I)
    if not match:
        return None

    return urlunparse(
        (
            "https",
            "www.jpc.de",
            f"/ff/{match.group(1)}",
            "",
            urlencode({"page": str(page_number), "searchtype": "cid"}),
            "",
        )
    )


def is_listing_page_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc and parsed.netloc.lower() not in {"www.jpc.de", "jpc.de"}:
        return False
    if "/detail/" in parsed.path or parsed.fragment:
        return False
    if parsed.path.startswith("/jpcng/vinyl/static/"):
        return False
    if parsed.path == "/jpcng/vinyl/home":
        return False
    if canonical_taxonomy_route_url(url):
        return True
    return False


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (compatible; Vinylofy-JPC-VinylDiscovery/1.0; "
                "+https://vinylofy.nl)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,de;q=0.8,en;q=0.6",
        }
    )
    return session


def hnum_from_href(href: object) -> str | None:
    href_text = clean(href)
    if not href_text:
        return None

    match = PRODUCT_HREF_RE.search(href_text)
    if match:
        return match.group(1)

    parsed = urlparse(href_text)
    match = PRODUCT_HREF_RE.search(parsed.path)
    if match:
        return match.group(1)

    return None


def context_text_until_next_product(anchor: Tag, current_hnum: str) -> str:
    parts: list[str] = []

    anchor_text = clean(anchor.get_text(" ", strip=True))
    if anchor_text:
        parts.append(anchor_text)

    for node in anchor.next_elements:
        if node is anchor:
            continue

        if isinstance(node, Tag):
            if node.name == "a" and node.has_attr("href"):
                next_hnum = hnum_from_href(node.get("href"))
                if next_hnum and next_hnum != current_hnum:
                    break

            if node.name in {"script", "style", "noscript", "svg"}:
                continue

        if isinstance(node, NavigableString):
            value = clean(str(node))
            if value:
                parts.append(value)

        joined = " | ".join(parts)
        low = joined.lower()
        if "aktueller preis" in low and (
            "warenkorb" in low or "artikel merken" in low or len(joined) > 250
        ):
            break
        if len(joined) > 3000:
            break

    return " | ".join(parts)


def extract_title_hint(anchor: Tag, context_text: str) -> str | None:
    for value in (
        anchor.get_text(" ", strip=True),
        anchor.get("title"),
        anchor.get("aria-label"),
    ):
        text = clean(value)
        if text:
            return text[:350]

    return context_text[:350] if context_text else None


def extract_format_hint(text: str) -> str | None:
    matches = VINYL_MEDIA_RE.findall(text)
    if not matches:
        return None

    for match in matches:
        value = clean(match)
        if value:
            return value

    return "Vinyl"


def is_vinyl_context(text: str) -> bool:
    if VINYL_MEDIA_RE.search(text):
        return True

    titleish = text[:350]
    if "vinyl" in titleish.lower():
        return True

    return False


def extract_price_hint(text: str) -> str | None:
    current = re.findall(
        r"(?:EUR|€)\s*([0-9]+(?:[.,][0-9]{2}))\*?\s*Aktueller Preis",
        text,
        flags=re.I,
    )
    candidates = current or EURO_PRICE_RE.findall(text)
    valid = [candidate for candidate in candidates if parse_price(candidate) is not None]
    if not valid:
        return None

    return valid[-1]


def extract_availability_hint(text: str) -> tuple[str | None, str | None]:
    low = text.lower()

    snippets = []
    for marker in (
        "artikel am lager",
        "lieferbar innerhalb",
        "lieferbar in mind",
        "lieferbar ab",
        "benachrichtigung anfordern",
        "nicht erhaltlich",
        "nicht erhältlich",
        "nicht lieferbar",
    ):
        if marker in low:
            snippets.append(marker)

    raw = " | ".join(snippets) if snippets else None

    if "lieferbar ab" in low:
        return "preorder", raw or "lieferbar ab"
    if (
        "benachrichtigung anfordern" in low
        or "nicht erhältlich" in low
        or "nicht erhaltlich" in low
        or "nicht lieferbar" in low
    ):
        return "out_of_stock", raw
    if "artikel am lager" in low or "lieferbar" in low:
        return "in_stock", raw

    return None, raw


def parse_listing_links(
    html: str,
    *,
    listing_url: str,
    route: RouteSpec,
    page_number: int,
) -> list[DiscoveredLink]:
    soup = BeautifulSoup(html, "html.parser")
    links_by_hnum: dict[str, DiscoveredLink] = {}

    for position, anchor in enumerate(soup.find_all("a", href=True), start=1):
        href = clean(anchor.get("href"))
        hnum = hnum_from_href(href)
        if not hnum:
            continue

        product_url = urljoin(BASE_URL, href)
        context_text = context_text_until_next_product(anchor, hnum)
        if not is_vinyl_context(context_text):
            continue

        format_hint = extract_format_hint(context_text)
        if not format_hint and NON_VINYL_MEDIA_RE.search(context_text):
            continue

        availability, availability_raw = extract_availability_hint(context_text)
        payload: dict[str, Any] = {
            "source": "jpc_vinyl_listing",
            "route_name": route.name,
            "route_source": route.source,
            "route_url": route.url,
            "listing_url": listing_url,
            "listing_page_number": page_number,
            "listing_position": position,
            "hnum": hnum,
        }

        title_hint = extract_title_hint(anchor, context_text)
        price_hint = extract_price_hint(context_text)

        if title_hint:
            payload["listing_title_hint"] = title_hint
        if format_hint:
            payload["format"] = format_hint
        if price_hint:
            payload["listing_price_raw"] = price_hint
            payload["listing_currency"] = "EUR"
            payload["listing_price_source"] = "jpc_listing"
        if availability:
            payload["listing_availability"] = availability
        if availability_raw:
            payload["listing_availability_raw"] = availability_raw
        if context_text:
            payload["listing_context_sample"] = context_text[:1200]

        links_by_hnum[hnum] = DiscoveredLink(
            shop_id=SHOP_ID,
            source_url=product_url,
            source_product_id=hnum,
            payload=payload,
        )

    return list(links_by_hnum.values())


def route_is_probably_vinyl(label: str, href: str) -> bool:
    text = f"{label} {href}".lower()
    if any(marker in text for marker in EXCLUDED_ROUTE_LABEL_MARKERS):
        return False

    return canonical_taxonomy_route_url(href) is not None


def discover_route_index(
    *,
    session: requests.Session,
    timeout_seconds: float,
) -> list[RouteSpec]:
    response = session.get(VINYL_HOME_URL, timeout=timeout_seconds)
    if response.status_code >= 400:
        print(
            "[JPC-ROUTE-INDEX-SKIP]",
            {"url": VINYL_HOME_URL, "status_code": response.status_code},
            flush=True,
        )
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    routes_by_url: dict[str, RouteSpec] = {}

    for anchor in soup.find_all("a", href=True):
        label = clean(anchor.get_text(" ", strip=True))
        href = urljoin(BASE_URL, clean(anchor.get("href")))
        if not href.startswith(BASE_URL):
            continue
        if not route_is_probably_vinyl(label, href):
            continue

        href = canonical_taxonomy_route_url(href) or href
        name = normalize_route_label(label or urlparse(href).path)
        routes_by_url[href] = RouteSpec(name=name, url=href, source="vinyl_home")

    return list(routes_by_url.values())


def extract_next_url(html: str, *, current_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")

    rel_next = soup.find("a", attrs={"rel": lambda value: value and "next" in str(value).lower()})
    if rel_next and rel_next.get("href"):
        candidate = urljoin(current_url, clean(rel_next.get("href")))
        if is_listing_page_url(candidate):
            return candidate

    for anchor in soup.find_all("a", href=True):
        text = clean(anchor.get_text(" ", strip=True)).lower()
        aria = clean(anchor.get("aria-label")).lower()
        title = clean(anchor.get("title")).lower()
        combined = " ".join(value for value in (text, aria, title) if value)
        if combined in {">", "›", "»"} or any(
            marker in combined
            for marker in ("weiter", "nachste", "nächste", "next", "folgende")
        ):
            candidate = urljoin(current_url, clean(anchor.get("href")))
            if is_listing_page_url(candidate):
                return candidate

    return None


def add_page_fallback(url: str, *, page_number: int, mode: str) -> str | None:
    if mode == "none":
        return None
    if mode == "ff":
        return ff_page_url(url, page_number=page_number)

    parsed = urlparse(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))

    if mode == "page":
        params["page"] = str(page_number)
    elif mode == "pn":
        params["pn"] = str(page_number)
    else:
        raise ValueError(f"Unsupported pagination fallback: {mode}")

    return urlunparse(parsed._replace(query=urlencode(params)))


def merge_routes(*route_lists: list[RouteSpec]) -> list[RouteSpec]:
    merged: dict[str, RouteSpec] = {}
    for routes in route_lists:
        for route in routes:
            merged.setdefault(route.url, route)
    return list(merged.values())


def discover_links(
    *,
    routes: list[RouteSpec],
    include_route_index: bool,
    max_pages_per_route: int,
    pagination_fallback: str,
    delay_seconds: float,
    timeout_seconds: float,
) -> list[DiscoveredLink]:
    session = build_session()
    indexed_routes = (
        discover_route_index(session=session, timeout_seconds=timeout_seconds)
        if include_route_index
        else []
    )
    all_routes = merge_routes(routes, indexed_routes)
    all_links_by_hnum: dict[str, DiscoveredLink] = {}

    print(
        "[JPC-DISCOVER-ROUTES]",
        {
            "configured_routes": len(routes),
            "indexed_routes": len(indexed_routes),
            "total_routes": len(all_routes),
        },
        flush=True,
    )

    for route in all_routes:
        current_url: str | None = route.url
        seen_page_signatures: set[tuple[str, ...]] = set()

        for page_number in range(1, max_pages_per_route + 1):
            if not current_url:
                break

            try:
                response = session.get(
                    current_url,
                    timeout=timeout_seconds,
                    allow_redirects=True,
                )
            except requests.RequestException as exc:
                print(
                    "[JPC-DISCOVER-ERROR]",
                    {
                        "route": route.name,
                        "page": page_number,
                        "url": current_url,
                        "error": str(exc),
                    },
                    flush=True,
                )
                break

            if response.status_code == 429:
                print(
                    "[JPC-DISCOVER-RATE-LIMIT]",
                    {"route": route.name, "page": page_number, "url": current_url},
                    flush=True,
                )
                break

            if response.status_code >= 400:
                print(
                    "[JPC-DISCOVER-HTTP-SKIP]",
                    {
                        "route": route.name,
                        "page": page_number,
                        "url": current_url,
                        "status_code": response.status_code,
                    },
                    flush=True,
                )
                break

            links = parse_listing_links(
                response.text,
                listing_url=response.url,
                route=route,
                page_number=page_number,
            )
            signature = tuple(sorted(link.source_product_id or "" for link in links))
            if signature and signature in seen_page_signatures:
                print(
                    "[JPC-DISCOVER-STOP]",
                    {
                        "route": route.name,
                        "page": page_number,
                        "reason": "duplicate_page_signature",
                    },
                    flush=True,
                )
                break
            if signature:
                seen_page_signatures.add(signature)

            new_count = 0
            for link in links:
                hnum = link.source_product_id or ""
                if hnum not in all_links_by_hnum:
                    new_count += 1
                all_links_by_hnum[hnum] = link

            print(
                "[JPC-DISCOVER-PAGE]",
                {
                    "route": route.name,
                    "page": page_number,
                    "status_code": response.status_code,
                    "page_links": len(links),
                    "new_links_added": new_count,
                    "unique_total": len(all_links_by_hnum),
                    "url": response.url,
                },
                flush=True,
            )

            if not links:
                break

            next_url = extract_next_url(response.text, current_url=response.url)
            if not next_url:
                next_url = add_page_fallback(
                    route.url,
                    page_number=page_number + 1,
                    mode=pagination_fallback,
                )

            current_url = next_url
            time.sleep(delay_seconds)

    return list(all_links_by_hnum.values())


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ontdek JPC vinyl listinglinks voor batchgewijze EAN-verrijking."
    )
    parser.add_argument(
        "--routes",
        default="default",
        help=(
            "Comma-separated route specs: name=url,name=url. "
            "Gebruik default voor Rock/Pop/Metal/Soul-Funk; "
            "--include-route-index volgt overige vinyl-taxonomie-CID's."
        ),
    )
    parser.add_argument(
        "--include-route-index",
        action="store_true",
        help="Lees ook de JPC vinyl-home uit als route-index.",
    )
    parser.add_argument("--max-pages-per-route", type=int, default=1)
    parser.add_argument(
        "--pagination-fallback",
        choices=("none", "ff", "page", "pn"),
        default="ff",
        help=(
            "Fallback voor vervolgpagina's. JPC vinyl-taxonomie gebruikt "
            "/ff/<cid>?page=N&searchtype=cid."
        ),
    )
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY_SECONDS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument("--write", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()

    if args.max_pages_per_route < 1:
        raise SystemExit("[ERROR] --max-pages-per-route moet minimaal 1 zijn.")
    if args.delay < 0:
        raise SystemExit("[ERROR] --delay mag niet negatief zijn.")
    if args.timeout <= 0:
        raise SystemExit("[ERROR] --timeout moet positief zijn.")

    routes = parse_route_specs(args.routes)
    links = discover_links(
        routes=routes,
        include_route_index=args.include_route_index,
        max_pages_per_route=args.max_pages_per_route,
        pagination_fallback=args.pagination_fallback,
        delay_seconds=args.delay,
        timeout_seconds=args.timeout,
    )

    print(
        "[JPC-DISCOVER]",
        {"links": len(links), "write": args.write},
        flush=True,
    )

    for link in links[:20]:
        print(
            "[JPC-DISCOVER-SAMPLE]",
            {
                "source_url": link.source_url,
                "source_product_id": link.source_product_id,
                "payload": link.payload,
            },
            flush=True,
        )

    if args.write and links:
        result = upsert_discovered_links(
            links,
            preserve_payload_keys=("last_successful_ean",),
        )
        print(
            "[JPC-DISCOVER-WRITE]",
            {
                "inserted": result.inserted,
                "updated": result.updated,
                "total": result.total,
            },
            flush=True,
        )
    elif not args.write:
        print("[JPC-DISCOVER] dry-run complete; geen databasewrites.", flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
