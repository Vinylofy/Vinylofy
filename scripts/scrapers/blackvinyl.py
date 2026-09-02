#!/usr/bin/env python3
"""Full Blackvinyl LP Nieuw discovery and detail enrichment.

Blackvinyl exposes a public WooCommerce Store API.  The API is the listing
source: it supplies pagination metadata, current/sale prices and stock.  The
product pages are used only for detail/EAN enrichment and never to replace a
newer listing price or availability.
"""
from __future__ import annotations

import argparse
import csv
import html
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Keep the script entry point (``python scripts/scrapers/blackvinyl.py``)
# usable as well as the module entry point used by the pipeline.  Python puts
# the script directory, rather than the repository root, on sys.path when a
# file is executed directly.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.importers.common import strict_normalize_gtin


SHOP_ID = "blackvinyl"
BASE_URL = "https://www.blackvinyl.nl"
STORE_API_URL = f"{BASE_URL}/wp-json/wc/store/v1/products"
STORE_CATEGORIES_URL = f"{BASE_URL}/wp-json/wc/store/v1/products/categories"
DEFAULT_CATEGORY_ID = 15
DEFAULT_CATEGORY_SLUG = "vinyl-nieuw"
DEFAULT_OUTPUT_PATH = Path("data/raw/blackvinyl/blackvinyl_products.csv")
DEFAULT_DETAIL_CACHE_PATH = Path("data/raw/blackvinyl/blackvinyl_detail_cache.json")
DEFAULT_PER_PAGE = 100
DEFAULT_DETAIL_DELAY = 0.35
DEFAULT_TIMEOUT = 30.0
MAX_API_PAGES = 1000

CSV_COLUMNS = (
    "scraped_at",
    "product_url",
    "product_id",
    "artist",
    "title",
    "price",
    "standard_price",
    "sale_price",
    "currency",
    "availability",
    "stock_text",
    "sku",
    "format",
    "category_id",
    "category_slug",
    "image_url",
    "ean",
    "gtin_normalized",
    "ean_source",
    "detail_ean",
    "detail_status",
    "detail_checked_at",
    "source_page",
    "source_api_url",
    "shipping_profile",
)

EAN_LABEL_RE = re.compile(r"\b(?:ean|gtin(?:-?(?:8|12|13|14))?)\s*[:=]\s*([0-9]{8,14})\b", re.I)
DIGIT_RE = re.compile(r"(?<![0-9])([0-9]{8,14})(?![0-9])")


def clean(value: object) -> str:
    return re.sub(r"\s+", " ", html.unescape(str(value or ""))).strip()


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_product_url(value: object) -> str:
    raw = clean(value)
    absolute = urljoin(BASE_URL, raw)
    parts = urlsplit(absolute)
    if parts.netloc.lower() not in {"blackvinyl.nl", "www.blackvinyl.nl"}:
        raise ValueError(f"Product URL buiten blackvinyl.nl: {value!r}")
    path = parts.path.rstrip("/") or "/"
    return urlunsplit(("https", "www.blackvinyl.nl", path, "", ""))


def split_artist_title(value: object) -> tuple[str, str]:
    text = clean(value)
    for separator in (" – ", " — ", " - ", "–", "—"):
        if separator in text:
            artist, title = text.split(separator, 1)
            artist, title = clean(artist), clean(title)
            if artist and title:
                return artist, title
    return "", text


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Vinylofy-Blackvinyl/1.0 (+https://vinylofy.nl)",
            "Accept": "application/json,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.7",
        }
    )
    return session


def money_from_minor(value: object, minor_unit: object = 2) -> str:
    if value in (None, ""):
        return ""
    try:
        amount = int(str(value))
        unit = int(minor_unit or 2)
    except (TypeError, ValueError):
        return ""
    if unit < 0 or unit > 6:
        return ""
    return f"{amount / (10 ** unit):.{unit}f}"


def prices_from_api(product: dict[str, Any]) -> tuple[str, str, str, str]:
    prices = product.get("prices") or {}
    minor_unit = prices.get("currency_minor_unit", 2)
    current = money_from_minor(prices.get("price"), minor_unit)
    regular = money_from_minor(prices.get("regular_price"), minor_unit)
    sale = money_from_minor(prices.get("sale_price"), minor_unit)
    currency = clean(prices.get("currency_code")) or "EUR"
    return current, regular, sale, currency


def extract_valid_gtin(*values: object) -> tuple[str, str] | None:
    """Return the first checkdigit-valid source token and its canonical GTIN."""
    explicit: list[str] = []
    general: list[str] = []
    for value in values:
        text = clean(value)
        if not text:
            continue
        explicit.extend(match.group(1) for match in EAN_LABEL_RE.finditer(text))
        general.extend(match.group(1) for match in DIGIT_RE.finditer(text))

    for token in [*explicit, *general]:
        normalized = strict_normalize_gtin(token)
        if normalized:
            return token, normalized
    return None


def resolve_category_id(
    session: requests.Session,
    slug: str,
    fallback: int,
    timeout: float,
) -> int:
    # Blackvinyl has duplicate/ambiguous category slugs in the Store API.
    # The category ID supplied by the configured source URL is authoritative;
    # never replace it with the first slug match (which currently resolves to
    # an 11-item accessories category instead of LP Nieuw).
    if fallback > 0:
        return fallback

    try:
        response = session.get(
            STORE_CATEGORIES_URL,
            params={"slug": slug, "per_page": 100},
            timeout=timeout,
        )
        if response.ok:
            payload = response.json()
            if isinstance(payload, list) and payload:
                category_id = int(payload[0].get("id"))
                if category_id > 0:
                    return category_id
    except (requests.RequestException, ValueError, TypeError):
        pass
    return fallback


def fetch_listing_page(
    session: requests.Session,
    *,
    category_id: int,
    page: int,
    per_page: int,
    timeout: float,
) -> tuple[list[dict[str, Any]], int | None, int | None, str]:
    params = {"category": category_id, "per_page": per_page, "page": page}
    response = session.get(STORE_API_URL, params=params, timeout=timeout)
    if response.status_code == 429:
        raise RuntimeError("Blackvinyl Store API rate limited (HTTP 429)")
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise RuntimeError("Blackvinyl Store API gaf geen productlijst terug")

    def header_int(name: str) -> int | None:
        value = response.headers.get(name)
        try:
            return int(value) if value else None
        except ValueError:
            return None

    return payload, header_int("X-WP-Total"), header_int("X-WP-TotalPages"), response.url


def listing_row(
    product: dict[str, Any],
    *,
    page: int,
    category_id: int,
    category_slug: str,
    scraped_at: str,
    source_api_url: str,
) -> dict[str, str]:
    product_id = str(product.get("id") or "").strip()
    product_url = canonical_product_url(product.get("permalink"))
    artist, title = split_artist_title(product.get("name"))
    current, regular, sale, currency = prices_from_api(product)

    description = clean(BeautifulSoup(str(product.get("description") or ""), "html.parser").get_text(" ", strip=True))
    sku = clean(product.get("sku"))
    ean_result = extract_valid_gtin(description, sku)
    ean, gtin = (ean_result if ean_result else ("", ""))
    ean_source = "description_or_sku" if ean else ""

    categories = product.get("categories") or []
    format_label = clean(categories[0].get("name")) if categories and isinstance(categories[0], dict) else "Vinyl"
    image_url = ""
    images = product.get("images") or []
    if images and isinstance(images[0], dict):
        image_url = clean(images[0].get("src"))

    stock = product.get("stock_availability") or {}
    stock_text = clean(stock.get("text")) if isinstance(stock, dict) else ""
    if isinstance(product.get("is_in_stock"), bool):
        availability = "in_stock" if product["is_in_stock"] else "out_of_stock"
    elif isinstance(stock, dict) and clean(stock.get("class")).casefold() == "in-stock":
        availability = "in_stock"
    elif isinstance(stock, dict) and clean(stock.get("class")).casefold() == "out-of-stock":
        availability = "out_of_stock"
    else:
        availability = "unknown"

    return {
        "scraped_at": scraped_at,
        "product_url": product_url,
        "product_id": product_id,
        "artist": artist,
        "title": title,
        "price": current,
        "standard_price": regular,
        "sale_price": sale,
        "currency": currency,
        "availability": availability,
        "stock_text": stock_text,
        "sku": sku,
        "format": format_label or "Vinyl",
        "category_id": str(category_id),
        "category_slug": category_slug,
        "image_url": image_url,
        "ean": ean,
        "gtin_normalized": gtin,
        "ean_source": ean_source,
        "detail_ean": "",
        "detail_status": "api",
        "detail_checked_at": "",
        "source_page": str(page),
        "source_api_url": source_api_url,
        "shipping_profile": "blackvinyl_12inch_nl",
    }


def discover_all(
    session: requests.Session,
    *,
    category_id: int,
    category_slug: str,
    per_page: int,
    timeout: float,
) -> tuple[list[dict[str, str]], int | None, int | None]:
    if per_page < 1 or per_page > 100:
        raise ValueError("per_page moet tussen 1 en 100 liggen")

    rows_by_id: dict[str, dict[str, str]] = {}
    page = 1
    reported_total = None
    reported_pages = None

    while True:
        products, total, total_pages, request_url = fetch_listing_page(
            session,
            category_id=category_id,
            page=page,
            per_page=per_page,
            timeout=timeout,
        )
        reported_total = total if total is not None else reported_total
        reported_pages = total_pages if total_pages is not None else reported_pages

        print(
            "[BLACKVINYL-DISCOVER-PAGE]",
            {"page": page, "products": len(products), "reported_total": reported_total, "reported_pages": reported_pages},
            flush=True,
        )

        if not products:
            if reported_pages is not None and page <= reported_pages:
                raise RuntimeError(
                    f"Blackvinyl API-pagina {page} was leeg terwijl X-WP-TotalPages={reported_pages}"
                )
            break

        for product in products:
            if not isinstance(product, dict):
                raise RuntimeError("Blackvinyl API bevatte een niet-object product")
            product_id = str(product.get("id") or "").strip()
            if not product_id:
                raise RuntimeError("Blackvinyl product zonder ID")
            row = listing_row(
                product,
                page=page,
                category_id=category_id,
                category_slug=category_slug,
                scraped_at=now_iso(),
                source_api_url=request_url,
            )
            previous = rows_by_id.get(product_id)
            if previous is not None and previous["product_url"] != row["product_url"]:
                raise RuntimeError(
                    f"Product-ID {product_id} verwees naar meerdere URLs: "
                    f"{previous['product_url']} / {row['product_url']}"
                )
            rows_by_id[product_id] = row

        if reported_pages is not None:
            if reported_pages < 1 or reported_pages > MAX_API_PAGES:
                raise RuntimeError(f"Ongeldige Blackvinyl pagination: {reported_pages}")
            if page >= reported_pages:
                break
        elif len(products) < per_page:
            break

        page += 1
        if page > MAX_API_PAGES:
            raise RuntimeError("Blackvinyl API pagination overschreed veilige bovengrens")

    rows = sorted(rows_by_id.values(), key=lambda row: (row["artist"].casefold(), row["title"].casefold(), row["product_id"]))
    if reported_total is not None and len(rows) != reported_total:
        raise RuntimeError(
            f"Blackvinyl discovery telde {len(rows)} unieke producten, API rapporteerde {reported_total}"
        )
    return rows, reported_total, reported_pages


def detail_ean_from_html(content: str) -> tuple[str, str] | None:
    soup = BeautifulSoup(content, "html.parser")
    visible_text = soup.get_text(" ", strip=True)
    candidates: list[object] = [visible_text]
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.string or script.get_text())
        except (TypeError, json.JSONDecodeError):
            continue
        candidates.append(json.dumps(data, ensure_ascii=False))
    sku = soup.select_one(".sku")
    if sku:
        candidates.append(sku.get_text(" ", strip=True))
    return extract_valid_gtin(*candidates)


def load_detail_cache(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_detail_cache(path: Path, cache: dict[str, dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(cache, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def enrich_details(
    session: requests.Session,
    rows: list[dict[str, str]],
    *,
    mode: str,
    cache_path: Path,
    detail_delay: float,
    timeout: float,
    refresh_cache: bool,
) -> dict[str, int]:
    cache = {} if refresh_cache else load_detail_cache(cache_path)
    stats = {"attempted": 0, "cached": 0, "ok": 0, "not_found": 0, "technical_error": 0}

    for index, row in enumerate(rows, start=1):
        if mode == "none" or (mode == "missing" and row.get("ean")):
            continue

        url = row["product_url"]
        cached = cache.get(url)
        if cached:
            stats["cached"] += 1
            result = cached
        else:
            stats["attempted"] += 1
            try:
                response = session.get(url, timeout=timeout)
                if response.status_code == 429:
                    raise RuntimeError("HTTP 429")
                if response.status_code in {404, 410}:
                    result = {
                        "status": "not_found",
                        "ean": "",
                        "gtin_normalized": "",
                        "checked_at": now_iso(),
                    }
                    cache[url] = result
                    save_detail_cache(cache_path, cache)
                    status = "not_found"
                    stats[status] += 1
                    row["detail_status"] = status
                    row["detail_checked_at"] = clean(result.get("checked_at"))
                    row["detail_ean"] = ""
                    print(
                        "[BLACKVINYL-DETAIL]",
                        {"index": index, "total": len(rows), "url": url, "status": status, "ean": bool(row.get("ean"))},
                        flush=True,
                    )
                    continue
                response.raise_for_status()
                found = detail_ean_from_html(response.text)
                result = {
                    "status": "ok" if found else "not_found",
                    "ean": found[0] if found else "",
                    "gtin_normalized": found[1] if found else "",
                    "checked_at": now_iso(),
                }
            except (requests.RequestException, RuntimeError) as exc:
                result = {
                    "status": "technical_error",
                    "ean": "",
                    "gtin_normalized": "",
                    "checked_at": now_iso(),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            cache[url] = result
            save_detail_cache(cache_path, cache)
            if detail_delay > 0 and index < len(rows):
                time.sleep(detail_delay)

        status = clean(result.get("status")) or "not_found"
        stats[status] = stats.get(status, 0) + 1
        row["detail_status"] = status
        row["detail_checked_at"] = clean(result.get("checked_at"))
        row["detail_ean"] = clean(result.get("ean"))

        # Listing remains authoritative.  Detail fills only a missing EAN.
        if not row.get("ean") and result.get("ean") and result.get("gtin_normalized"):
            row["ean"] = clean(result.get("ean"))
            row["gtin_normalized"] = clean(result.get("gtin_normalized"))
            row["ean_source"] = "detail"

        print(
            "[BLACKVINYL-DETAIL]",
            {"index": index, "total": len(rows), "url": url, "status": status, "ean": bool(row.get("ean"))},
            flush=True,
        )

    return stats


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            writer.writerows({column: row.get(column, "") for column in CSV_COLUMNS} for row in rows)
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Volledige Blackvinyl LP Nieuw Store API scraper")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--detail-cache", type=Path, default=DEFAULT_DETAIL_CACHE_PATH)
    parser.add_argument("--category-id", type=int, default=DEFAULT_CATEGORY_ID)
    parser.add_argument("--category-slug", default=DEFAULT_CATEGORY_SLUG)
    parser.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("--detail-delay", type=float, default=DEFAULT_DETAIL_DELAY)
    parser.add_argument("--detail-mode", choices=("all", "missing", "none"), default="all")
    parser.add_argument("--refresh-detail-cache", action="store_true")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.category_id < 1:
        raise SystemExit("[ERROR] --category-id moet positief zijn")
    if args.timeout <= 0:
        raise SystemExit("[ERROR] --timeout moet positief zijn")
    if args.detail_delay < 0:
        raise SystemExit("[ERROR] --detail-delay mag niet negatief zijn")

    session = make_session()
    category_id = resolve_category_id(session, args.category_slug, args.category_id, args.timeout)
    rows, reported_total, reported_pages = discover_all(
        session,
        category_id=category_id,
        category_slug=args.category_slug,
        per_page=args.per_page,
        timeout=args.timeout,
    )
    detail_stats = enrich_details(
        session,
        rows,
        mode=args.detail_mode,
        cache_path=args.detail_cache,
        detail_delay=args.detail_delay,
        timeout=args.timeout,
        refresh_cache=args.refresh_detail_cache,
    )
    write_csv(args.output, rows)

    with_ean = sum(1 for row in rows if row.get("ean"))
    print(
        "[BLACKVINYL] complete",
        {
            "category_id": category_id,
            "category_slug": args.category_slug,
            "rows": len(rows),
            "api_total": reported_total,
            "api_pages": reported_pages,
            "with_ean": with_ean,
            "without_ean": len(rows) - with_ean,
            "detail": detail_stats,
            "output": str(args.output),
        },
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
