from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path

import requests

from scripts.release_discovery.jobs import discover_imusic as imusic

ROOT = Path(__file__).resolve().parents[2]


def source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def item_html(
    *,
    ean: str = "0199584532912",
    title: str = "Timeless",
    artist: str = "Prince",
    format_label: str = "LP",
    label: str = "",
    release_label: str = "Release August 28, 2026",
    price: str = "€ 23.49",
) -> str:
    return f"""
    <div class="list-item">
      <a href="/music/{ean}/prince-2026-timeless-lp"
         title="{artist} · {title} ({format_label}){' [' + label + ']' if label else ''} (2026)">
        <div class="type">
          <span class="label"><acronym title="Vinyl">{format_label}</acronym></span>
          {'<div class="label label-blank breakable-label">' + label + '</div>' if label else ''}
        </div>
        <div class="item-text">
          <span class="title" title="{title}">{title}</span>
          <div class="artist" title="{artist}">{artist}</div>
        </div>
        {'<span class="label label-warning">' + release_label + '</span>' if release_label else ''}
      </a>
      <form><button class="btn btn-success price">{price}</button><button title="Expected to ship">Pre-order</button></form>
    </div>
    """


def page_html(items: list[str], *, pagination: str = "") -> str:
    return f"""
    <html>
      <body>
        <div class="list-container list-items">{''.join(items)}</div>
        {pagination}
      </body>
    </html>
    """


class FakeResponse:
    def __init__(self, status_code: int, text: str = "", headers: dict[str, str] | None = None):
        self.status_code = status_code
        self.text = text
        self.headers = headers or {}

    def raise_for_status(self) -> None:
        raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self.responses = responses
        self.urls: list[str] = []

    def get(self, url: str, *, timeout: float, allow_redirects: bool) -> FakeResponse:
        self.urls.append(url)
        if not self.responses:
            raise AssertionError("no fake responses left")
        return self.responses.pop(0)


class IMusicReleaseDiscoveryTests(unittest.TestCase):
    def parse_items(self, html: str):
        parsed_page = imusic.parse_listing_page(
            html,
            listing_url=imusic.exposure_url(0),
            offset=0,
            global_seen_eans=set(),
        )
        return (
            parsed_page.items,
            parsed_page.stats,
            parsed_page.total_count,
            parsed_page.offset_step,
        )

    def test_valid_ean_from_imusic_product_url(self) -> None:
        self.assertEqual(
            imusic.ean_from_product_url("/music/0199584532912/prince-2026-timeless-lp"),
            "0199584532912",
        )

    def test_invalid_checkdigit_is_rejected(self) -> None:
        self.assertIsNone(
            imusic.ean_from_product_url("/music/0199584532913/prince-2026-timeless-lp")
        )

    def test_missing_numeric_url_segment_is_rejected(self) -> None:
        self.assertIsNone(imusic.ean_from_product_url("/music/prince-2026-timeless-lp"))

    def test_valid_release_date(self) -> None:
        self.assertEqual(
            imusic.parse_release_date("Release August 28, 2026"),
            date(2026, 8, 28),
        )

    def test_missing_or_invalid_release_date_skips_item(self) -> None:
        items, stats, _, _ = self.parse_items(
            page_html([item_html(release_label="Release Foo 99, 2026")])
        )
        self.assertEqual(items, [])
        self.assertEqual(stats.skips["missing_release_date"], 1)

    def test_non_vinyl_is_excluded(self) -> None:
        items, stats, _, _ = self.parse_items(
            page_html([item_html(format_label="CD", label="Compact Disc")])
        )
        self.assertEqual(items, [])
        self.assertEqual(stats.skips["non_vinyl"], 1)

    def test_missing_vinyl_marker_is_excluded(self) -> None:
        items, stats, _, _ = self.parse_items(
            page_html([item_html(format_label="", label="", title="Timeless")])
        )
        self.assertEqual(items, [])
        self.assertEqual(stats.skips["non_vinyl"], 1)

    def test_incidental_lp_substring_is_not_vinyl(self) -> None:
        items, stats, _, _ = self.parse_items(
            page_html([item_html(format_label="", label="Helpful edition", title="Timeless")])
        )
        self.assertEqual(items, [])
        self.assertEqual(stats.skips["non_vinyl"], 1)

    def test_vinyl_combination_release_is_allowed(self) -> None:
        items, stats, _, _ = self.parse_items(
            page_html([item_html(format_label="LP/CD× 4", label="Box Set edition")])
        )
        self.assertEqual([item.ean for item in items], ["0199584532912"])
        self.assertEqual(stats.skips["non_vinyl"], 0)

    def test_two_colour_variants_with_different_eans_remain_separate(self) -> None:
        items, _, _, _ = self.parse_items(
            page_html(
                [
                    item_html(ean="0888072790742", artist="Mastodon", title="Marrow Deep", label="Coke Bottle Clear Vinyl edition"),
                    item_html(ean="0888072774186", artist="Mastodon", title="Marrow Deep", label="Black Vinyl edition"),
                ]
            )
        )
        self.assertEqual([item.ean for item in items], ["0888072790742", "0888072774186"])

    def test_bob_and_imusic_same_product_identity_is_frontend_deduped_by_product_id(self) -> None:
        data_source = source("lib/vinylofy-data.ts")
        self.assertIn("const seenProductIds = new Set<string>();", data_source)
        self.assertIn("if (seenProductIds.has(row.product_id)) return false;", data_source)
        self.assertIn("seenProductIds.add(row.product_id);", data_source)

    def test_listing_release_discovery_does_not_update_public_prices_or_availability(self) -> None:
        job_source = source("scripts/release_discovery/jobs/discover_imusic.py")
        self.assertNotIn("update public.prices", job_source)
        self.assertNotIn("insert into public.prices", job_source)
        self.assertNotIn("insert into public.price_history", job_source)

    def test_duplicate_page_stop(self) -> None:
        html = page_html([item_html()])
        calls: list[str] = []

        def fetch_page(url: str) -> str:
            calls.append(url)
            return html

        result = imusic.discover_release_items(
            max_pages=3,
            offset_step=100,
            delay_seconds=0,
            timeout_seconds=1,
            fetch_page=fetch_page,
        )

        self.assertEqual(result.stop_reason, "duplicate_page")
        self.assertEqual(len(calls), 2)

    def test_last_page_stop_from_total_count(self) -> None:
        pagination = """
        <select>
          <option value="/exposure/3146/new-lps-and-upcoming-vinyl-releases?offset=0#tbl">1-100</option>
          <option value="/exposure/3146/new-lps-and-upcoming-vinyl-releases?offset=100#tbl">101-150</option>
        </select>
        out of 150
        """
        pages = {
            imusic.exposure_url(0): page_html([item_html(ean="0199584532912")], pagination=pagination),
            imusic.exposure_url(100): page_html([item_html(ean="0888072790742", artist="Mastodon", title="Marrow Deep")], pagination=pagination),
        }

        result = imusic.discover_release_items(
            max_pages=5,
            offset_step=None,
            delay_seconds=0,
            timeout_seconds=1,
            fetch_page=lambda url: pages[url],
        )

        self.assertEqual(result.stop_reason, "last_page")
        self.assertEqual([item.ean for item in result.items], ["0199584532912", "0888072790742"])

    def test_fetch_retries_429_with_retry_after(self) -> None:
        session = FakeSession(
            [
                FakeResponse(429, headers={"Retry-After": "0"}),
                FakeResponse(200, text="ok"),
            ]
        )
        original_sleep = imusic.time.sleep
        imusic.time.sleep = lambda delay: None
        try:
            self.assertEqual(imusic.fetch(session, "https://imusic.co/test", timeout=1), "ok")
        finally:
            imusic.time.sleep = original_sleep

        self.assertEqual(session.urls, ["https://imusic.co/test", "https://imusic.co/test"])

    def test_existing_imusic_modules_still_expose_expected_entrypoints(self) -> None:
        from scripts.scrapers.usf.jobs import detail_imusic, discover_imusic_exposures, promote_imusic, stage_imusic

        self.assertTrue(callable(detail_imusic.build_parser))
        self.assertTrue(callable(discover_imusic_exposures.parse_exposure_links))
        self.assertTrue(callable(stage_imusic.build_parser))
        self.assertEqual(promote_imusic.CONFIG.shop_id, "imusic")

    def test_new_releases_query_keeps_existing_publication_window_and_filters(self) -> None:
        data_source = source("lib/vinylofy-data.ts")
        self.assertIn("minDateValue.setUTCDate(minDateValue.getUTCDate() - 14);", data_source)
        self.assertIn("maxDateValue.setUTCDate(maxDateValue.getUTCDate() + 14);", data_source)
        self.assertIn('.eq("status", "active")', data_source)
        self.assertIn("if (!row.product_id) return false;", data_source)
        self.assertIn("if (freshShopCount < 2) return false;", data_source)


if __name__ == "__main__":
    unittest.main()
