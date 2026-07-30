#!/usr/bin/env python3
"""
3345 voorraadstatus-scraper met een echte Chromium-browser.

Waarom Playwright?
------------------
Een requests-scraper ontvangt alleen de server-HTML. Wanneer 3345 productkaarten
of voorraadknoppen later met JavaScript hydrateert, helpt een langere requests-
timeout niet. Deze scraper opent iedere verzamelpagina in Chromium, wacht tot
de DOM stabiel is en leest daarna de zichtbare productkaarten uit.

Output:
    target_url,stock_status

Statusmapping:
    data-purchase-type="instant"  + Add to cart -> in_stock
    data-purchase-type="preorder" + Pre-order   -> preorder
    alles anders / ontbrekend                   -> out_of_stock

Installeren:
    python -m pip install playwright
    python -m playwright install chromium

Offline parsertest:
    python scrape_3345_stock_browser.py --self-test

Korte browsertest:
    python scrape_3345_stock_browser.py \
      --max-pages 10 \
      --concurrency 5 \
      --wait-ms 5000 \
      --headed \
      --output 3345_stock_test.csv \
      --debug-dir debug_3345

Volledige run:
    python scrape_3345_stock_browser.py \
      --max-pages 0 \
      --concurrency 5 \
      --wait-ms 4000 \
      --output 3345_stock.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Sequence
from urllib.parse import urljoin, urlsplit, urlunsplit

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

from playwright.async_api import (
    Browser,
    BrowserContext,
    Locator,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)


BASE_URL = "https://3345.nl"
COLLECTION_URL = f"{BASE_URL}/collections/all"

STATUS_IN_STOCK = "in_stock"
STATUS_PREORDER = "preorder"
STATUS_OUT_OF_STOCK = "out_of_stock"


class RateLimitError(RuntimeError):
    def __init__(self, message: str, retry_after_seconds: int) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


PRODUCT_PATH_RE = re.compile(r"^/products/[^/?#]+/?$")
LP_RE = re.compile(
    r"(?<![A-Z0-9])(?:\d+\s*[X×]\s*)?LP(?:S)?(?![A-Z0-9])",
    re.IGNORECASE,
)

# Geldige XPath-expressies voor waarschijnlijke productkaart-ancestors.
# CSS-selectors mogen niet rechtstreeks achter "ancestor::" worden geplaatst.
CARD_ANCESTOR_XPATHS = (
    "ancestor::li[contains(concat(' ', normalize-space(@class), ' '), ' grid__item ')][1]",
    "ancestor::*[contains(concat(' ', normalize-space(@class), ' '), ' product-grid-item ')][1]",
    "ancestor::*[contains(concat(' ', normalize-space(@class), ' '), ' product-card ')][1]",
    "ancestor::*[contains(concat(' ', normalize-space(@class), ' '), ' card-wrapper ')][1]",
    "ancestor::*[@data-product-card][1]",
    "ancestor::article[1]",
)

CTA_SELECTOR = (
    'cart-add-button, '
    'button.plp-cta-btn, '
    'button[disabled], '
    '[data-purchase-type]'
)

PRODUCT_LINK_SELECTOR = 'a[href*="/products/"]'


@dataclass(frozen=True)
class StockRow:
    target_url: str
    stock_status: str


@dataclass(frozen=True)
class ScrapeResult:
    rows: list[StockRow]
    processed_pages: int
    highest_found_page: int
    completed: bool


@dataclass(frozen=True)
class DatabaseStats:
    linked_target_urls: int
    unlinked_target_urls: int
    secondhand: int
    changed_records: int
    no_ean: int
    delisted_not_seen: int


def normalize_product_url(href: str) -> str | None:
    if not href:
        return None

    absolute = urljoin(BASE_URL, href.strip())
    parts = urlsplit(absolute)
    if parts.netloc.lower() not in {"3345.nl", "www.3345.nl"}:
        return None

    path = re.sub(r"/+", "/", parts.path).rstrip("/")

    # 3345 gebruikt zowel /products/... als /nl/products/... voor dezelfde
    # productpagina. De bestaande prijspipeline bewaart deze canoniek zonder
    # locale-prefix. Gebruik exact dezelfde normalisatie voor browser- en
    # database-URL's, zodat target URL de linking key blijft.
    if path.startswith("/nl/products/"):
        path = path[3:]

    if not PRODUCT_PATH_RE.match(path):
        return None

    return urlunsplit(("https", "3345.nl", path, "", ""))


def normalize_text(value: str | None) -> str:
    return " ".join((value or "").split()).strip()


def classify_status(
    *,
    tag_name: str,
    purchase_type: str | None,
    label: str | None,
    disabled: bool,
) -> str:
    """Fail-closed mapping volgens de afgesproken 3345-regels."""
    purchase_type_n = normalize_text(purchase_type).lower()
    label_n = normalize_text(label).lower()

    if (
        tag_name.lower() == "cart-add-button"
        and purchase_type_n == "instant"
        and label_n == "add to cart"
        and not disabled
    ):
        return STATUS_IN_STOCK

    if (
        tag_name.lower() == "cart-add-button"
        and purchase_type_n == "preorder"
        and label_n in {"pre-order", "preorder"}
        and not disabled
    ):
        return STATUS_PREORDER

    return STATUS_OUT_OF_STOCK


def collection_url(page_number: int) -> str:
    if page_number < 1:
        raise ValueError("page_number moet minimaal 1 zijn")
    return f"{COLLECTION_URL}?page={page_number}"


async def wait_for_rendered_products(
    page: Page,
    *,
    selector_timeout_ms: int,
    wait_ms: int,
) -> None:
    """
    Wacht op productlinks, CTA-hydratatie en daarna een extra rustige periode.

    networkidle alleen is bij moderne storefronts niet altijd voldoende:
    analytics of live scripts kunnen actief blijven. Daarom combineren we:
      1. domcontentloaded;
      2. minimaal één productlink;
      3. minimaal één CTA, indien aanwezig;
      4. optionele vaste hydratatiewacht.
    """
    await page.wait_for_load_state("domcontentloaded")

    await page.locator(PRODUCT_LINK_SELECTOR).first.wait_for(
        state="attached",
        timeout=selector_timeout_ms,
    )

    try:
        await page.locator(CTA_SELECTOR).first.wait_for(
            state="attached",
            timeout=selector_timeout_ms,
        )
    except PlaywrightTimeoutError as exc:
        raise RuntimeError(
            f"Geen CTA binnen {selector_timeout_ms}ms; "
            "pagina wordt opnieuw geprobeerd."
        ) from exc

    try:
        await page.wait_for_load_state("networkidle", timeout=selector_timeout_ms)
    except PlaywrightTimeoutError:
        logging.debug("networkidle niet bereikt; vaste wachttijd wordt gebruikt.")

    if wait_ms:
        await page.wait_for_timeout(wait_ms)


async def find_card_for_link(link: Locator) -> Locator | None:
    """
    Zoek de kleinste ancestor rond een productlink die:
      - exact één unieke producthandle bevat;
      - én een CTA of LP-tekst bevat.

    We vermijden afhankelijkheid van één theme-specifieke CSS-class.
    """
    handle = await link.get_attribute("href")
    normalized = normalize_product_url(handle or "")
    if not normalized:
        return None

    for xpath_expression in CARD_ANCESTOR_XPATHS:
        candidate = link.locator(f"xpath={xpath_expression}")
        if await candidate.count() == 0:
            continue

        links = candidate.locator(PRODUCT_LINK_SELECTOR)
        hrefs = await links.evaluate_all(
            """nodes => [...new Set(
                nodes.map(n => n.getAttribute('href')).filter(Boolean)
            )]"""
        )
        normalized_links = {
            value
            for href in hrefs
            if (value := normalize_product_url(str(href)))
        }

        if normalized_links == {normalized}:
            return candidate

    # Generieke DOM-wandeling als theme-selectors niet matchen.
    candidate = link
    for _ in range(12):
        candidate = candidate.locator("xpath=..")
        if await candidate.count() == 0:
            return None

        hrefs = await candidate.locator(PRODUCT_LINK_SELECTOR).evaluate_all(
            """nodes => [...new Set(
                nodes.map(n => n.getAttribute('href')).filter(Boolean)
            )]"""
        )
        normalized_links = {
            value
            for href in hrefs
            if (value := normalize_product_url(str(href)))
        }

        if len(normalized_links) > 1:
            return None

        if normalized_links == {normalized}:
            has_cta = await candidate.locator(CTA_SELECTOR).count() > 0
            text = normalize_text(await candidate.inner_text())
            if has_cta or LP_RE.search(text):
                return candidate

    return None


async def read_card_status(card: Locator) -> tuple[str, dict[str, object]]:
    ctas = card.locator(CTA_SELECTOR)
    count = await ctas.count()

    debug_ctas: list[dict[str, object]] = []

    for index in range(count):
        cta = ctas.nth(index)
        tag_name = await cta.evaluate("el => el.tagName.toLowerCase()")
        purchase_type = await cta.get_attribute("data-purchase-type")
        label = normalize_text(await cta.inner_text())
        disabled = await cta.evaluate(
            """el => Boolean(
                el.disabled ||
                el.hasAttribute('disabled') ||
                el.getAttribute('aria-disabled') === 'true'
            )"""
        )

        status = classify_status(
            tag_name=tag_name,
            purchase_type=purchase_type,
            label=label,
            disabled=disabled,
        )

        debug_ctas.append(
            {
                "tag_name": tag_name,
                "purchase_type": purchase_type,
                "label": label,
                "disabled": disabled,
                "classified_as": status,
            }
        )

        if status in {STATUS_IN_STOCK, STATUS_PREORDER}:
            return status, {"ctas": debug_ctas}

    return STATUS_OUT_OF_STOCK, {"ctas": debug_ctas}


async def detect_pagination(page: Page, current_page: int) -> dict[str, object]:
    """
    Bepaal of de collectie nog een volgende pagina heeft.

    De scraper stopt nadrukkelijk NIET meer op nul gevonden/publiceerbare LP's.
    We kijken naar:
      1. rel="next";
      2. zichtbare/bruikbare Next/Volgende-link of knop;
      3. paginalinks met een paginanummer groter dan de huidige pagina.

    Geeft tevens het hoogste zichtbare paginanummer terug als diagnostiek.
    """
    return await page.evaluate(
        """({ currentPage }) => {
            const isDisabled = (el) => {
                if (!el) return true;
                const cls = String(el.className || '').toLowerCase();
                return Boolean(
                    el.disabled ||
                    el.hasAttribute('disabled') ||
                    el.getAttribute('aria-disabled') === 'true' ||
                    cls.includes('disabled')
                );
            };

            const hrefPage = (href) => {
                if (!href) return null;
                try {
                    const url = new URL(href, location.href);
                    const value = Number(url.searchParams.get('page'));
                    return Number.isInteger(value) && value > 0 ? value : null;
                } catch {
                    return null;
                }
            };

            const relNext = document.querySelector('a[rel="next"]');
            if (relNext && !isDisabled(relNext)) {
                return {
                    has_next: true,
                    next_url: relNext.href,
                    highest_visible_page: hrefPage(relNext.href),
                    reason: 'rel_next'
                };
            }

            const controls = [...document.querySelectorAll(
                'a, button, [role="button"]'
            )];

            const nextControl = controls.find((el) => {
                const text = String(
                    el.getAttribute('aria-label') ||
                    el.getAttribute('title') ||
                    el.textContent ||
                    ''
                ).replace(/\\s+/g, ' ').trim().toLowerCase();

                return (
                    !isDisabled(el) &&
                    (
                        text === 'next' ||
                        text === 'volgende' ||
                        text.includes('next page') ||
                        text.includes('volgende pagina')
                    )
                );
            });

            if (nextControl) {
                return {
                    has_next: true,
                    next_url: nextControl.href || null,
                    highest_visible_page: hrefPage(nextControl.href),
                    reason: 'next_control'
                };
            }

            const pageNumbers = [...document.querySelectorAll('a[href*="page="]')]
                .map((a) => hrefPage(a.getAttribute('href')))
                .filter((value) => Number.isInteger(value));

            const highest = pageNumbers.length ? Math.max(...pageNumbers) : null;
            return {
                has_next: highest !== null && highest > currentPage,
                next_url: null,
                highest_visible_page: highest,
                reason: highest !== null ? 'numbered_links' : 'no_next_found'
            };
        }""",
        {"currentPage": current_page},
    )


async def parse_rendered_page(page: Page) -> tuple[list[StockRow], list[dict[str, object]]]:
    links = page.locator(PRODUCT_LINK_SELECTOR)
    link_count = await links.count()

    rows: dict[str, StockRow] = {}
    diagnostics: list[dict[str, object]] = []
    processed_urls: set[str] = set()

    for index in range(link_count):
        link = links.nth(index)
        href = await link.get_attribute("href")
        target_url = normalize_product_url(href or "")
        if not target_url or target_url in processed_urls:
            continue

        card = await find_card_for_link(link)
        if card is None:
            continue

        card_text = normalize_text(await card.inner_text())
        if not LP_RE.search(card_text):
            continue

        processed_urls.add(target_url)
        status, status_debug = await read_card_status(card)

        rows[target_url] = StockRow(
            target_url=target_url,
            stock_status=status,
        )

        diagnostics.append(
            {
                "target_url": target_url,
                "stock_status": status,
                "card_text": card_text[:500],
                **status_debug,
            }
        )

    return list(rows.values()), diagnostics


async def save_debug(
    page: Page,
    *,
    debug_dir: Path,
    page_number: int,
    diagnostics: list[dict[str, object]],
) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)

    html_path = debug_dir / f"page_{page_number:04d}.html"
    screenshot_path = debug_dir / f"page_{page_number:04d}.png"
    json_path = debug_dir / f"page_{page_number:04d}.json"

    html_path.write_text(await page.content(), encoding="utf-8")
    await page.screenshot(path=str(screenshot_path), full_page=True)
    json_path.write_text(
        json.dumps(diagnostics, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


async def scrape(
    *,
    start_page: int,
    max_pages: int,
    concurrency: int,
    wait_ms: int,
    selector_timeout_ms: int,
    navigation_timeout_ms: int,
    headed: bool,
    slow_mo_ms: int,
    debug_dir: Path | None,
    retries: int,
) -> ScrapeResult:
    if start_page < 1:
        raise ValueError("--start-page moet minimaal 1 zijn")
    if max_pages < 0:
        raise ValueError("--max-pages mag niet negatief zijn")
    if wait_ms < 0:
        raise ValueError("--wait-ms mag niet negatief zijn")
    if concurrency < 1:
        raise ValueError("--concurrency moet minimaal 1 zijn")
    if retries < 1:
        raise ValueError("--retries moet minimaal 1 zijn")

    all_rows: dict[str, StockRow] = {}
    processed_pages = 0
    page_number = start_page
    highest_found_page = 0
    completed = False

    async with async_playwright() as playwright:
        browser: Browser = await playwright.chromium.launch(
            headless=not headed,
            slow_mo=slow_mo_ms,
        )
        context: BrowserContext = await browser.new_context(
            locale="nl-NL",
            timezone_id="Europe/Amsterdam",
            viewport={"width": 1440, "height": 1200},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/150.0 Safari/537.36"
            ),
        )
        async def scrape_one(page_number_to_scrape: int):
            last_error: Exception | None = None

            for attempt in range(1, retries + 1):
                page = await context.new_page()
                page.set_default_navigation_timeout(navigation_timeout_ms)
                page.set_default_timeout(selector_timeout_ms)

                try:
                    url = collection_url(page_number_to_scrape)
                    logging.info(
                        "Browser opent pagina %s (poging %s/%s): %s",
                        page_number_to_scrape,
                        attempt,
                        retries,
                        url,
                    )

                    response = await page.goto(
                        url,
                        wait_until="domcontentloaded",
                        timeout=navigation_timeout_ms,
                    )
                    if response is None:
                        raise RuntimeError(f"Geen HTTP-response ontvangen voor {url}")
                    if response.status == 429:
                        retry_after_raw = response.headers.get("retry-after")
                        try:
                            retry_after_seconds = int(retry_after_raw or "")
                        except (TypeError, ValueError):
                            retry_after_seconds = 0

                        if retry_after_seconds < 1:
                            retry_after_seconds = min(
                                300,
                                30 * (2 ** (attempt - 1)),
                            )

                        raise RateLimitError(
                            f"3345 gaf HTTP 429 voor {url}; "
                            f"nieuwe poging over {retry_after_seconds}s",
                            retry_after_seconds,
                        )

                    if response.status != 200:
                        raise RuntimeError(
                            f"3345 gaf HTTP {response.status} voor {url}"
                        )

                    await wait_for_rendered_products(
                        page,
                        selector_timeout_ms=selector_timeout_ms,
                        wait_ms=wait_ms,
                    )

                    page_rows, diagnostics = await parse_rendered_page(page)
                    pagination = await detect_pagination(
                        page,
                        page_number_to_scrape,
                    )

                    if debug_dir:
                        diagnostics.append({"_pagination": pagination})
                        await save_debug(
                            page,
                            debug_dir=debug_dir,
                            page_number=page_number_to_scrape,
                            diagnostics=diagnostics,
                        )

                    return page_number_to_scrape, page_rows, pagination
                except (
                    PlaywrightTimeoutError,
                    RuntimeError,
                    OSError,
                ) as exc:
                    last_error = exc
                    logging.warning(
                        "Pagina %s poging %s/%s mislukt: %s",
                        page_number_to_scrape,
                        attempt,
                        retries,
                        exc,
                    )
                    if attempt < retries:
                        retry_delay = (
                            exc.retry_after_seconds
                            if isinstance(exc, RateLimitError)
                            else min(10, attempt * 2)
                        )
                        logging.info(
                            "Pagina %s: retry over %ss",
                            page_number_to_scrape,
                            retry_delay,
                        )
                        await asyncio.sleep(retry_delay)
                finally:
                    await page.close()

            raise RuntimeError(
                f"Pagina {page_number_to_scrape} mislukt na {retries} pogingen: "
                f"{last_error}"
            )

        try:
            stop_requested = False

            while not stop_requested:
                if max_pages and processed_pages >= max_pages:
                    logging.info("Testlimiet van %s pagina's bereikt.", max_pages)
                    break

                remaining = (
                    max_pages - processed_pages
                    if max_pages
                    else concurrency
                )
                batch_size = min(concurrency, remaining) if max_pages else concurrency
                batch_pages = list(range(page_number, page_number + batch_size))

                results = await asyncio.gather(
                    *(scrape_one(number) for number in batch_pages)
                )

                for current_page, page_rows, pagination in sorted(
                    results,
                    key=lambda item: item[0],
                ):
                    new_rows = [
                        row for row in page_rows
                        if row.target_url not in all_rows
                    ]

                    for row in page_rows:
                        all_rows[row.target_url] = row

                    counts = {
                        STATUS_IN_STOCK: 0,
                        STATUS_PREORDER: 0,
                        STATUS_OUT_OF_STOCK: 0,
                    }
                    for row in page_rows:
                        counts[row.stock_status] += 1

                    logging.info(
                        "Pagina %s: %s LP's, %s nieuw | "
                        "in_stock=%s preorder=%s out_of_stock=%s",
                        current_page,
                        len(page_rows),
                        len(new_rows),
                        counts[STATUS_IN_STOCK],
                        counts[STATUS_PREORDER],
                        counts[STATUS_OUT_OF_STOCK],
                    )

                    processed_pages += 1

                    visible_highest = pagination.get("highest_visible_page")
                    if isinstance(visible_highest, int):
                        highest_found_page = max(
                            highest_found_page,
                            visible_highest,
                        )
                    highest_found_page = max(highest_found_page, current_page)

                    logging.info(
                        "Paginering pagina %s: has_next=%s, reden=%s, "
                        "hoogste_zichtbare_pagina=%s",
                        current_page,
                        pagination.get("has_next"),
                        pagination.get("reason"),
                        pagination.get("highest_visible_page"),
                    )

                    # Nul publiceerbare LP's is géén stopconditie: een pagina kan
                    # uitsluitend tweedehands producten bevatten.
                    if not pagination.get("has_next", False):
                        completed = start_page == 1
                        logging.info(
                            "Stop: paginering op pagina %s heeft geen volgende pagina.",
                            current_page,
                        )
                        stop_requested = True
                        break

                    if max_pages and processed_pages >= max_pages:
                        stop_requested = True
                        break

                page_number += batch_size

        finally:
            await context.close()
            await browser.close()

    return ScrapeResult(
        rows=list(all_rows.values()),
        processed_pages=processed_pages,
        highest_found_page=highest_found_page,
        completed=completed,
    )


def write_output(rows: Sequence[StockRow], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.suffix.lower() == ".csv":
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["target_url", "stock_status"],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(asdict(row))
        return

    if path.suffix.lower() == ".json":
        path.write_text(
            json.dumps(
                [asdict(row) for row in rows],
                ensure_ascii=False,
                indent=2,
            ) + "\n",
            encoding="utf-8",
        )
        return

    raise ValueError("--output moet eindigen op .csv of .json")



def truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return normalize_text(str(value or "")).lower() in {
        "1",
        "true",
        "yes",
        "ja",
        "y",
    }


def normalize_ean(value: object) -> str | None:
    text = normalize_text(str(value or ""))
    return text or None


def canonical_gtin(value: object) -> str | None:
    """
    Vergelijk GTIN-8, UPC-A/GTIN-12, EAN-13 en GTIN-14 canoniek.

    Een UPC-A van 12 cijfers en dezelfde EAN-13 met een voorloopnul
    vertegenwoordigen hetzelfde artikel. Alleen de vergelijking wordt
    genormaliseerd; de opgeslagen EAN wordt niet gewijzigd.
    """
    ean = normalize_ean(value)
    if not ean:
        return None

    if not ean.isdigit() or len(ean) not in {8, 12, 13, 14}:
        return ean

    return ean.zfill(14)


def write_report(rows: list[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "target_url",
        "product_id",
        "ean",
        "old_stock_status",
        "scraped_stock_status",
        "new_stock_status",
        "secondhand",
        "linked",
        "changed",
        "reason",
        "price_before",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def apply_database_statuses(
    *,
    scrape_result: ScrapeResult,
    write: bool,
    report_path: Path,
) -> DatabaseStats:
    load_dotenv()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL ontbreekt")

    scraped_by_url = {row.target_url: row for row in scrape_result.rows}
    report_rows: list[dict[str, object]] = []
    registry_updates: dict[str, dict[str, object]] = {}
    linked = 0
    unlinked = 0
    secondhand_count = 0
    no_ean_count = 0
    changed_records = 0
    delisted_not_seen = 0

    with psycopg.connect(
        database_url,
        row_factory=dict_row,
        prepare_threshold=None,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select id::text as id, domain
                from public.shops
                where lower(regexp_replace(domain, '^www\\.', '')) = '3345.nl'
                order by id
                """
            )
            shops = [dict(row) for row in cur.fetchall()]
            if len(shops) != 1:
                raise RuntimeError(
                    "Verwacht exact één public.shops-record voor 3345.nl, "
                    f"gevonden: {len(shops)}"
                )
            price_shop_id = str(shops[0]["id"])
            registry_shop_ids = sorted({price_shop_id, "shop3345"})

            cur.execute(
                """
                select
                    p.product_id::text as product_id,
                    p.product_url,
                    p.availability,
                    p.price,
                    p.updated_at,
                    products.ean
                from public.prices p
                join public.products products
                  on products.id = p.product_id
                where p.shop_id::text = %s
                  and p.product_url is not null
                order by p.product_url, p.product_id
                """,
                (price_shop_id,),
            )
            price_rows = [dict(row) for row in cur.fetchall()]

            prices_by_url: dict[str, list[dict[str, object]]] = {}
            for row in price_rows:
                target_url = normalize_product_url(str(row.get("product_url") or ""))
                if not target_url:
                    continue
                prices_by_url.setdefault(target_url, []).append(row)

            duplicate_urls = {
                target_url: rows
                for target_url, rows in prices_by_url.items()
                if len(rows) > 1
            }
            conflicting_eans: dict[str, list[str]] = {}
            equivalent_ean_variants: dict[str, list[str]] = {}
            for target_url, rows in duplicate_urls.items():
                raw_eans = sorted(
                    {
                        ean
                        for row in rows
                        if (ean := normalize_ean(row.get("ean")))
                    }
                )
                canonical_eans = {
                    canonical
                    for ean in raw_eans
                    if (canonical := canonical_gtin(ean))
                }

                if len(canonical_eans) > 1:
                    conflicting_eans[target_url] = raw_eans
                elif len(raw_eans) > 1:
                    equivalent_ean_variants[target_url] = raw_eans

            if conflicting_eans:
                details = "; ".join(
                    f"{target_url} -> {','.join(eans)}"
                    for target_url, eans in sorted(conflicting_eans.items())[:20]
                )
                raise RuntimeError(
                    "Target URL koppelt aan verschillende EAN's binnen 3345 "
                    f"public.prices; veilige voorraadwrite geweigerd: {details}"
                )

            if equivalent_ean_variants:
                logging.warning(
                    "%s 3345 target-URL's gebruiken equivalente UPC/EAN-varianten; "
                    "dezelfde voorraadstatus wordt op alle bijbehorende records toegepast: %s",
                    len(equivalent_ean_variants),
                    "; ".join(
                        f"{target_url} -> {','.join(eans)}"
                        for target_url, eans in sorted(
                            equivalent_ean_variants.items()
                        )[:20]
                    ),
                )

            if duplicate_urls:
                logging.warning(
                    "%s 3345 target-URL's hebben meerdere public.prices-records; "
                    "alle niet-conflicterende records worden per target-URL bijgewerkt: %s",
                    len(duplicate_urls),
                    ", ".join(sorted(duplicate_urls)[:20]),
                )

            cur.execute(
                """
                select source_url, payload
                from public.shop_product_links
                where shop_id::text = any(%s)
                  and source_url is not null
                order by source_url
                """,
                (registry_shop_ids,),
            )
            registry_rows = [dict(row) for row in cur.fetchall()]

            registry_by_url: dict[str, list[dict[str, object]]] = {}
            secondhand_by_url: dict[str, bool] = {}
            for row in registry_rows:
                target_url = normalize_product_url(str(row.get("source_url") or ""))
                if not target_url:
                    continue
                registry_by_url.setdefault(target_url, []).append(row)
                payload = row.get("payload")
                is_secondhand = (
                    truthy(payload.get("is_secondhand"))
                    if isinstance(payload, dict)
                    else False
                )
                secondhand_by_url[target_url] = (
                    secondhand_by_url.get(target_url, False) or is_secondhand
                )

            update_plan: list[dict[str, object]] = []
            for target_url, scraped in sorted(scraped_by_url.items()):
                price_matches = prices_by_url.get(target_url, [])
                is_secondhand = secondhand_by_url.get(target_url, False)
                if is_secondhand:
                    secondhand_count += 1

                if not price_matches:
                    unlinked += 1
                    registry_updates[target_url] = {
                        "target_url": target_url,
                        "old_stock_status": None,
                        "scraped_stock_status": scraped.stock_status,
                        "new_stock_status": STATUS_OUT_OF_STOCK,
                    }
                    report_rows.append(
                        {
                            "target_url": target_url,
                            "product_id": None,
                            "ean": None,
                            "old_stock_status": None,
                            "scraped_stock_status": scraped.stock_status,
                            "new_stock_status": STATUS_OUT_OF_STOCK,
                            "secondhand": is_secondhand,
                            "linked": False,
                            "changed": False,
                            "reason": "geen_bestaande_price_target_url_fail_closed",
                            "price_before": None,
                        }
                    )
                    continue

                linked += 1
                linked_eans = sorted(
                    {
                        ean
                        for price_row in price_matches
                        if (ean := normalize_ean(price_row.get("ean")))
                    }
                )
                registry_status = scraped.stock_status
                if is_secondhand or not linked_eans:
                    registry_status = STATUS_OUT_OF_STOCK

                old_statuses = sorted(
                    {
                        normalize_text(str(price_row.get("availability") or "")).lower()
                        for price_row in price_matches
                    }
                )
                old_status_summary = (
                    old_statuses[0]
                    if len(old_statuses) == 1
                    else "mixed:" + ",".join(old_statuses)
                )
                registry_updates[target_url] = {
                    "target_url": target_url,
                    "old_stock_status": old_status_summary,
                    "scraped_stock_status": scraped.stock_status,
                    "new_stock_status": registry_status,
                }

                for price_row in price_matches:
                    ean = normalize_ean(price_row.get("ean"))
                    if not ean:
                        no_ean_count += 1

                    new_status = scraped.stock_status
                    reason = "browser_status"
                    if is_secondhand:
                        new_status = STATUS_OUT_OF_STOCK
                        reason = "tweedehands_fail_closed"
                    elif not ean:
                        new_status = STATUS_OUT_OF_STOCK
                        reason = "geen_ean_fail_closed"

                    old_status = normalize_text(
                        str(price_row.get("availability") or "")
                    ).lower()
                    changed = old_status != new_status
                    if changed:
                        changed_records += 1

                    plan_row = {
                        "target_url": target_url,
                        "product_id": str(price_row["product_id"]),
                        "ean": ean,
                        "old_stock_status": old_status,
                        "scraped_stock_status": scraped.stock_status,
                        "new_stock_status": new_status,
                        "secondhand": is_secondhand,
                        "linked": True,
                        "changed": changed,
                        "reason": reason,
                        "price_before": price_row.get("price"),
                    }
                    update_plan.append(plan_row)
                    report_rows.append(plan_row)

            if scrape_result.completed:
                for target_url, price_matches in sorted(prices_by_url.items()):
                    if target_url in scraped_by_url:
                        continue
                    delisted_not_seen += 1
                    is_secondhand = secondhand_by_url.get(target_url, False)
                    if is_secondhand:
                        secondhand_count += 1

                    old_statuses = sorted(
                        {
                            normalize_text(str(price_row.get("availability") or "")).lower()
                            for price_row in price_matches
                        }
                    )
                    old_status_summary = (
                        old_statuses[0]
                        if len(old_statuses) == 1
                        else "mixed:" + ",".join(old_statuses)
                    )
                    registry_updates[target_url] = {
                        "target_url": target_url,
                        "old_stock_status": old_status_summary,
                        "scraped_stock_status": None,
                        "new_stock_status": STATUS_OUT_OF_STOCK,
                    }

                    for price_row in price_matches:
                        old_status = normalize_text(
                            str(price_row.get("availability") or "")
                        ).lower()
                        new_status = STATUS_OUT_OF_STOCK
                        changed = old_status != new_status
                        if changed:
                            changed_records += 1
                        ean = normalize_ean(price_row.get("ean"))
                        if not ean:
                            no_ean_count += 1
                        plan_row = {
                            "target_url": target_url,
                            "product_id": str(price_row["product_id"]),
                            "ean": ean,
                            "old_stock_status": old_status,
                            "scraped_stock_status": None,
                            "new_stock_status": new_status,
                            "secondhand": is_secondhand,
                            "linked": True,
                            "changed": changed,
                            "reason": "niet_gezien_in_volledige_scan",
                            "price_before": price_row.get("price"),
                        }
                        update_plan.append(plan_row)
                        report_rows.append(plan_row)
            else:
                logging.info(
                    "Geen missing-target delist: run is begrensd of paginering "
                    "is niet aantoonbaar volledig."
                )

            if linked == 0:
                raise RuntimeError(
                    "Geen enkele gescrapete target URL koppelde aan public.prices; "
                    "databasewrites worden geweigerd."
                )

            if write:
                for item in update_plan:
                    if not item["changed"]:
                        continue
                    cur.execute(
                        """
                        update public.prices
                        set availability = %s,
                            updated_at = now()
                        where product_id::text = %s
                          and shop_id::text = %s
                        """,
                        (
                            item["new_stock_status"],
                            item["product_id"],
                            price_shop_id,
                        ),
                    )
                    if cur.rowcount != 1:
                        raise RuntimeError(
                            "Onverwacht aantal gewijzigde price-records voor "
                            f"{item['target_url']} / {item['product_id']}: "
                            f"{cur.rowcount}"
                        )

                # De registry is de bestaande URL-laag. Eén URL kan meerdere
                # price-records hebben, maar krijgt slechts één voorraadpayload.
                for target_url, item in sorted(registry_updates.items()):
                    registry_matches = registry_by_url.get(target_url, [])
                    for registry_row in registry_matches:
                        source_url = str(registry_row["source_url"])
                        publish_eligible = (
                            bool(item.get("linked"))
                            and item["new_stock_status"]
                            in {STATUS_IN_STOCK, STATUS_PREORDER}
                            and bool(item.get("ean"))
                            and not truthy(item.get("secondhand"))
                        )
                        cur.execute(
                            """
                            update public.shop_product_links
                            set payload = coalesce(payload, '{}'::jsonb)
                                || jsonb_build_object(
                                    'availability', (%s)::text,
                                    'publish_eligible', (%s)::boolean,
                                    'stock_previous_availability', (%s)::text,
                                    'stock_scraped_availability', (%s)::text,
                                    'stock_authority', 'stock_shop3345',
                                    'stock_updated_at', (%s)::text
                                )
                            where shop_id::text = any(%s)
                              and source_url = %s
                            """,
                            (
                                item["new_stock_status"],
                                publish_eligible,
                                item.get("old_stock_status"),
                                item.get("scraped_stock_status"),
                                datetime.now(timezone.utc).isoformat(),
                                registry_shop_ids,
                                source_url,
                            ),
                        )

                # Structurele prijscontrole in dezelfde transactie.
                changed_items = [item for item in update_plan if item["changed"]]
                for item in changed_items:
                    cur.execute(
                        """
                        select price, availability
                        from public.prices
                        where product_id::text = %s
                          and shop_id::text = %s
                        """,
                        (item["product_id"], price_shop_id),
                    )
                    after = cur.fetchone()
                    if after is None:
                        raise RuntimeError(
                            f"Price-record verdwenen voor {item['target_url']}"
                        )
                    before_price = Decimal(str(item["price_before"]))
                    after_price = Decimal(str(after["price"]))
                    if before_price != after_price:
                        raise RuntimeError(
                            "Prijs is gewijzigd tijdens voorraadupdate voor "
                            f"{item['target_url']}: {before_price} -> {after_price}"
                        )
                    if str(after["availability"]) != item["new_stock_status"]:
                        raise RuntimeError(
                            "Voorraadwrite niet teruggelezen voor "
                            f"{item['target_url']} / {item['product_id']}"
                        )
                conn.commit()
            else:
                conn.rollback()

    write_report(report_rows, report_path)
    return DatabaseStats(
        linked_target_urls=linked,
        unlinked_target_urls=unlinked,
        secondhand=secondhand_count,
        changed_records=changed_records,
        no_ean=no_ean_count,
        delisted_not_seen=delisted_not_seen,
    )




def run_self_test() -> None:
    cases = [
        (
            {
                "tag_name": "cart-add-button",
                "purchase_type": "instant",
                "label": "Add to cart",
                "disabled": False,
            },
            STATUS_IN_STOCK,
        ),
        (
            {
                "tag_name": "cart-add-button",
                "purchase_type": "preorder",
                "label": "Pre-order",
                "disabled": False,
            },
            STATUS_PREORDER,
        ),
        (
            {
                "tag_name": "button",
                "purchase_type": None,
                "label": "Sold out",
                "disabled": True,
            },
            STATUS_OUT_OF_STOCK,
        ),
        (
            {
                "tag_name": "cart-add-button",
                "purchase_type": "backorder",
                "label": "Backorder",
                "disabled": False,
            },
            STATUS_OUT_OF_STOCK,
        ),
        (
            {
                "tag_name": "cart-add-button",
                "purchase_type": None,
                "label": "Add to cart",
                "disabled": False,
            },
            STATUS_OUT_OF_STOCK,
        ),
    ]

    for arguments, expected in cases:
        actual = classify_status(**arguments)
        if actual != expected:
            raise AssertionError(
                f"Verwacht {expected}, kreeg {actual}: {arguments}"
            )

    url_cases = {
        "https://3345.nl/products/test-lp": (
            "https://3345.nl/products/test-lp"
        ),
        "https://3345.nl/nl/products/test-lp?variant=123": (
            "https://3345.nl/products/test-lp"
        ),
        "https://www.3345.nl/nl/products/test-lp/": (
            "https://3345.nl/products/test-lp"
        ),
    }
    for source_url, expected_url in url_cases.items():
        actual_url = normalize_product_url(source_url)
        if actual_url != expected_url:
            raise AssertionError(
                f"URL-normalisatie verwacht {expected_url}, "
                f"kreeg {actual_url}: {source_url}"
            )

    print(
        "SELF-TEST OK: voorraadmapping en canonieke "
        "3345 target-URL-normalisatie werken."
    )


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Scrape gerenderde 3345 LP-voorraadstatussen met Chromium."
    )
    result.add_argument("--start-page", type=int, default=1)
    result.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="0 = doorgaan tot 3345-paginering geen volgende pagina toont; N = testlimiet.",
    )
    result.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Aantal verzamelpagina's parallel, standaard 5.",
    )
    result.add_argument(
        "--wait-ms",
        type=int,
        default=4000,
        help="Extra wachttijd na DOM/CTA-hydratatie, standaard 4000 ms.",
    )
    result.add_argument(
        "--selector-timeout-ms",
        type=int,
        default=20000,
    )
    result.add_argument(
        "--navigation-timeout-ms",
        type=int,
        default=60000,
    )
    result.add_argument(
        "--headed",
        action="store_true",
        help="Toon Chromium tijdens de test.",
    )
    result.add_argument(
        "--slow-mo-ms",
        type=int,
        default=0,
        help="Vertraag browseracties voor visuele debugging.",
    )
    result.add_argument(
        "--debug-dir",
        type=Path,
        help="Bewaar per pagina gerenderde HTML, screenshot en CTA-diagnostiek.",
    )
    result.add_argument(
        "--output",
        type=Path,
        default=Path("3345_stock.csv"),
    )
    result.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Aantal browserpogingen per pagina, standaard 3.",
    )
    result.add_argument(
        "--report",
        type=Path,
        default=Path("output/usf-shop3345/stock-report.csv"),
        help="CSV met DB-koppeling, oud/nieuw en prijscontrole.",
    )
    result.add_argument(
        "--write",
        action="store_true",
        help="Schrijf uitsluitend voorraadstatus naar bestaande records.",
    )
    result.add_argument("--self-test", action="store_true")
    result.add_argument("--verbose", action="store_true")
    return result


def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    if args.self_test:
        run_self_test()
        return 0

    started = time.monotonic()
    try:
        scrape_result = asyncio.run(
            scrape(
                start_page=args.start_page,
                max_pages=args.max_pages,
                concurrency=args.concurrency,
                wait_ms=args.wait_ms,
                selector_timeout_ms=args.selector_timeout_ms,
                navigation_timeout_ms=args.navigation_timeout_ms,
                headed=args.headed,
                slow_mo_ms=args.slow_mo_ms,
                debug_dir=args.debug_dir,
                retries=args.retries,
            )
        )
        write_output(scrape_result.rows, args.output)
        db_stats = apply_database_statuses(
            scrape_result=scrape_result,
            write=args.write,
            report_path=args.report,
        )
    except (
        ValueError,
        RuntimeError,
        PlaywrightTimeoutError,
        OSError,
        psycopg.Error,
    ) as exc:
        logging.error("%s", exc)
        return 1

    totals = Counter(row.stock_status for row in scrape_result.rows)
    runtime = time.monotonic() - started
    print(
        f"Klaar: {len(scrape_result.rows)} unieke LP-doelpagina's "
        f"-> {args.output}\n"
        f"  pagina's:              {scrape_result.processed_pages}\n"
        f"  hoogste gevonden pagina:{scrape_result.highest_found_page}\n"
        f"  volledige scan:        {scrape_result.completed}\n"
        f"  in_stock:              {totals[STATUS_IN_STOCK]}\n"
        f"  preorder:              {totals[STATUS_PREORDER]}\n"
        f"  out_of_stock:          {totals[STATUS_OUT_OF_STOCK]}\n"
        f"  tweedehands:           {db_stats.secondhand}\n"
        f"  gekoppelde URLs:       {db_stats.linked_target_urls}\n"
        f"  niet gekoppelde URLs:  {db_stats.unlinked_target_urls}\n"
        f"  zonder EAN:            {db_stats.no_ean}\n"
        f"  niet gezien/delist:    {db_stats.delisted_not_seen}\n"
        f"  gewijzigde records:    {db_stats.changed_records}\n"
        f"  write:                  {args.write}\n"
        f"  report:                 {args.report}\n"
        f"  runtime_sec:            {runtime:.1f}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
