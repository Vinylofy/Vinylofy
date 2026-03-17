#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BASE_URL = "https://www.dgmoutlet.nl/muziek-films-games/muziek/lp/?order=name-asc&p={page}"
SHOP_NAME = "dgmoutlet"
DEFAULT_OUTPUT = "dgmoutlet_lp_listing.csv"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "nl-NL,nl;q=0.9,en;q=0.8",
}


@dataclass
class ProductRow:
    source_shop: str
    page: int
    artist: str
    title: str
    format: str
    url: str
    ean: str
    price_current: str
    price_original: str
    raw_name: str
    description_snippet: str
    scraped_at: str


class DGMOutletLPScraper:
    def __init__(self, output_path: str, start_page: int = 1, max_pages: Optional[int] = None, delay: float = 0.0):
        self.output_path = Path(output_path)
        self.start_page = max(1, start_page)
        self.max_pages = max_pages if (max_pages is None or max_pages > 0) else None
        self.delay = max(0.0, delay)
        self.session = self._build_session()
        self.seen_keys: set[str] = set()
        self.total_rows_written = 0
        self.output_initialized = False

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=5,
            connect=5,
            read=5,
            status=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD"),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        session.headers.update(HEADERS)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def run(self) -> None:
        self._prepare_output()
        page = self.start_page
        processed_pages = 0

        while True:
            if self.max_pages is not None and processed_pages >= self.max_pages:
                print(f"[STOP] max_pages bereikt ({self.max_pages}).")
                break

            url = BASE_URL.format(page=page)
            html = self._fetch(url)
            if html is None:
                print(f"[PAGINA {page}] ophalen mislukt, stop om halflege output te voorkomen.")
                break

            rows = self._parse_listing_page(html, page=page, page_url=url)
            if not rows:
                print(f"[PAGINA {page}] 0 product-cards gevonden. Stop.")
                break

            new_rows = []
            duplicates = 0
            for row in rows:
                key = row.ean or row.url
                if key in self.seen_keys:
                    duplicates += 1
                    continue
                self.seen_keys.add(key)
                new_rows.append(row)

            self._append_rows(new_rows)
            self.total_rows_written += len(new_rows)
            processed_pages += 1

            print(
                f"[PAGINA {page}] gevonden={len(rows)} | nieuw={len(new_rows)} | "
                f"dubbels={duplicates} | totaal={self.total_rows_written}"
            )

            page += 1
            if self.delay:
                time.sleep(self.delay)

        print(f"[KLAAR] rows geschreven: {self.total_rows_written}")
        print(f"[BESTAND] {self.output_path.resolve()}")

    def _fetch(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=30)
        except requests.RequestException as exc:
            print(f"[FOUT] request mislukt voor {url}: {exc}")
            return None

        if response.status_code >= 400:
            print(f"[FOUT] HTTP {response.status_code} voor {url}")
            return None
        return response.text

    def _parse_listing_page(self, html: str, page: int, page_url: str) -> list[ProductRow]:
        soup = BeautifulSoup(html, "html.parser")
        cards = soup.select("div.card.product-box.box-standard")
        rows: list[ProductRow] = []
        scraped_at = datetime.now(timezone.utc).isoformat()

        for card in cards:
            row = self._parse_card(card, page=page, page_url=page_url, scraped_at=scraped_at)
            if row is not None:
                rows.append(row)
        return rows

    def _parse_card(self, card: Tag, page: int, page_url: str, scraped_at: str) -> Optional[ProductRow]:
        name_link = card.select_one("a.product-name") or card.select_one("a.product-image-link")
        if not name_link:
            return None

        raw_name = self._clean_text(name_link.get_text(" ", strip=True))
        raw_url = name_link.get("href", "").strip()
        url = urljoin(page_url, raw_url)
        description = self._clean_text_from_html(card.select_one("div.product-description"))

        ean = self._extract_ean(card, url)
        price_current = self._extract_current_price(card)
        price_original = self._extract_original_price(card)
        artist, title, fmt = self._parse_name(raw_name, description)

        return ProductRow(
            source_shop=SHOP_NAME,
            page=page,
            artist=artist,
            title=title,
            format=fmt,
            url=url,
            ean=ean,
            price_current=price_current,
            price_original=price_original,
            raw_name=raw_name,
            description_snippet=description,
            scraped_at=scraped_at,
        )

    def _extract_current_price(self, card: Tag) -> str:
        price_node = card.select_one("span.product-price")
        if not price_node:
            return ""
        clone = BeautifulSoup(str(price_node), "html.parser")
        for nested in clone.select("span.list-price, span.list-price-price"):
            nested.decompose()
        cleaned = self._clean_text(clone.get_text(" ", strip=True))
        return self._normalize_price(cleaned)

    def _extract_original_price(self, card: Tag) -> str:
        node = card.select_one("span.list-price-price")
        if not node:
            return ""
        return self._normalize_price(self._clean_text(node.get_text(" ", strip=True)))

    def _extract_ean(self, card: Tag, url: str) -> str:
        sku_input = card.select_one('input[name="sku"]')
        if sku_input:
            sku_value = (sku_input.get("value") or "").strip()
            candidate = self._extract_ean_candidate(sku_value)
            if candidate:
                return candidate

        last_path = urlparse(url).path.rstrip("/").split("/")[-1]
        candidate = self._extract_ean_candidate(last_path)
        if candidate:
            return candidate
        return ""

    @staticmethod
    def _extract_ean_candidate(text: str) -> str:
        text = (text or "").strip()
        matches = re.findall(r"\d{8,14}", text)
        if not matches:
            return ""
        matches.sort(key=len, reverse=True)
        return matches[0]

    def _parse_name(self, raw_name: str, description: str) -> tuple[str, str, str]:
        name = self._clean_text(raw_name)
        if " - " in name:
            artist, remainder = name.split(" - ", 1)
        else:
            artist, remainder = "", name
        groups = [self._clean_text(g) for g in re.findall(r"\(([^()]*)\)", remainder) if self._clean_text(g)]
        title = self._clean_text(re.sub(r"\s*\([^)]*\)", "", remainder))
        fmt = self._build_format(groups=groups, description=description)
        return artist.strip(), title.strip(), fmt.strip()

    def _build_format(self, groups: list[str], description: str) -> str:
        format_parts: list[str] = []
        seen: set[str] = set()

        def add(value: str) -> None:
            cleaned = self._clean_format_part(value)
            if not cleaned:
                return
            key = cleaned.casefold()
            if key not in seen:
                seen.add(key)
                format_parts.append(cleaned)

        for part in groups:
            add(part)

        if description:
            desc_prefix = description.split("Productbeschrijving", 1)[0]
            tokens = [self._clean_text(x) for x in re.split(r"[\n\r\t]+|\xa0", desc_prefix)]
            for token in tokens:
                low = token.casefold()
                if low in {
                    "lp", "vinyl", "coloured vinyl", "colored vinyl", "limited edition", "picture disc",
                    "180 gram", "180g", "standard edition", "2 lp", "3 lp", "4 lp",
                }:
                    add(token)

        if not format_parts:
            desc_join = description or " ".join(groups)
            match = re.search(r"\b\d*\s*lp\b", desc_join, re.I)
            if match:
                add(match.group(0))

        return " - ".join(format_parts)

    @staticmethod
    def _clean_format_part(value: str) -> str:
        value = DGMOutletLPScraper._clean_text(value)
        if not value:
            return ""
        replacements = {
            "colored vinyl": "Coloured Vinyl",
            "colour vinyl": "Coloured Vinyl",
            "vinyl lp": "LP",
            "lp vinyl": "LP",
            "vinyl": "Vinyl",
            "lp": "LP",
        }
        lower = value.casefold()
        if lower in replacements:
            return replacements[lower]
        value = re.sub(r"\bLp\b", "LP", value)
        value = re.sub(r"\bEp\b", "EP", value)
        value = re.sub(r"\bCd\b", "CD", value)
        value = re.sub(r"\b2 Lp\b", "2 LP", value)
        value = re.sub(r"\b3 Lp\b", "3 LP", value)
        value = re.sub(r"\b4 Lp\b", "4 LP", value)
        return value

    @staticmethod
    def _clean_text(text: str) -> str:
        text = text.replace("\xa0", " ")
        text = re.sub(r"\s+", " ", text)
        return text.strip()

    @classmethod
    def _clean_text_from_html(cls, node: Optional[Tag]) -> str:
        if node is None:
            return ""
        return cls._clean_text(node.get_text(" ", strip=True))

    @staticmethod
    def _normalize_price(text: str) -> str:
        text = (text or "").strip()
        if not text:
            return ""
        cleaned = re.sub(r"[^\d,\.]", "", text)
        if not cleaned:
            return ""
        if "," in cleaned and "." in cleaned:
            if cleaned.rfind(",") > cleaned.rfind("."):
                cleaned = cleaned.replace(".", "").replace(",", ".")
        elif "," in cleaned:
            cleaned = cleaned.replace(",", ".")
        try:
            value = float(cleaned)
        except ValueError:
            return ""
        return f"{value:.2f}"

    def _prepare_output(self) -> None:
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        if self.output_path.exists():
            self.output_path.unlink()
        with self.output_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(self._empty_row()).keys()))
            writer.writeheader()
        self.output_initialized = True

    def _append_rows(self, rows: Iterable[ProductRow]) -> None:
        if not self.output_initialized:
            self._prepare_output()
        rows = list(rows)
        if not rows:
            return
        with self.output_path.open("a", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(self._empty_row()).keys()))
            for row in rows:
                writer.writerow(asdict(row))

    @staticmethod
    def _empty_row() -> ProductRow:
        return ProductRow(
            source_shop="",
            page=0,
            artist="",
            title="",
            format="",
            url="",
            ean="",
            price_current="",
            price_original="",
            raw_name="",
            description_snippet="",
            scraped_at="",
        )


def run_default(output_path: str | Path, start_page: int = 1, max_pages: Optional[int] = None, delay: float = 0.0) -> Path:
    output_path = Path(output_path)
    scraper = DGMOutletLPScraper(
        output_path=str(output_path),
        start_page=start_page,
        max_pages=max_pages,
        delay=delay,
    )
    scraper.run()
    return output_path


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scrape DGM Outlet LP listing pages naar CSV.")
    parser.add_argument("--start-page", type=int, default=1, help="Startpagina, standaard 1.")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="Optioneel maximum aantal pagina's. Laat leeg om door te lopen tot eerste lege pagina.",
    )
    parser.add_argument("--delay", type=float, default=0.0, help="Optionele vertraging tussen pagina's in seconden.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help=f"Output CSV-bestand. Standaard: {DEFAULT_OUTPUT}")
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()
    run_default(args.output, start_page=args.start_page, max_pages=args.max_pages, delay=args.delay)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
