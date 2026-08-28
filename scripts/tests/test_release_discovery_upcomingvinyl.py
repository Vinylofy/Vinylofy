from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path

import requests

from scripts.release_discovery.jobs import discover_upcomingvinyl as upcomingvinyl

ROOT = Path(__file__).resolve().parents[2]


def source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def item_html(
    *,
    artist: str = "A Certain Ratio",
    title: str = "Force Majeure",
    href: str = "https://upcomingvinyl.com/record/a-certain-ratio-force-majeure",
) -> str:
    return f"""
    <li>
      <a href="{href}">
        <h2>
          {artist}
          <span>{title}</span>
        </h2>
      </a>
    </li>
    """


def page_html(*, release_date: str = "August 28, 2026", items: list[str] | None = None, next_url: str | None = None) -> str:
    load_more = f'<a href="{next_url}" id="load-more" class="load-more">Show more</a>' if next_url else ""
    return f"""
    <html>
      <body>
        <main>
          <div class="page-heading with-right-content secondary sticky">
            <h1><span>{release_date}</span> / Friday</h1>
          </div>
          <ul class="record-grid">
            {''.join(items or [item_html()])}
          </ul>
          {load_more}
        </main>
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

    def get(self, url: str, *, timeout: float, allow_redirects: bool, headers: dict[str, str]) -> FakeResponse:
        self.urls.append(url)
        if not self.responses:
            raise AssertionError("no fake responses left")
        return self.responses.pop(0)


class UpcomingVinylReleaseDiscoveryTests(unittest.TestCase):
    def parse_items(self, html: str, *, min_date: date | None = None):
        parsed = upcomingvinyl.parse_listing_page(
            html,
            listing_page_url=upcomingvinyl.listing_url(1),
            min_date=min_date,
            global_seen_urls=set(),
        )
        return parsed.items, parsed.stats, parsed.next_url

    def test_parses_artist_title_and_release_date_from_listing(self) -> None:
        items, stats, _ = self.parse_items(page_html())
        self.assertEqual(stats.raw_records, 1)
        self.assertEqual(stats.parsed_records, 1)
        self.assertEqual(items[0].artist, "A Certain Ratio")
        self.assertEqual(items[0].title, "Force Majeure")
        self.assertEqual(items[0].release_date, date(2026, 8, 28))
        self.assertEqual(items[0].source_url, "https://upcomingvinyl.com/record/a-certain-ratio-force-majeure")

    def test_extracts_format_suffix_without_polluting_title(self) -> None:
        items, _, _ = self.parse_items(
            page_html(items=[item_html(title="Fragments of Life [2xLP]")])
        )
        self.assertEqual(items[0].title, "Fragments of Life")
        self.assertEqual(items[0].format, "2xLP")

    def test_skips_records_before_min_date(self) -> None:
        items, stats, _ = self.parse_items(
            page_html(release_date="August 27, 2026"),
            min_date=date(2026, 8, 28),
        )
        self.assertEqual(items, [])
        self.assertEqual(stats.raw_records, 0)

    def test_dedupes_by_source_url_across_pages(self) -> None:
        first = page_html(next_url="https://upcomingvinyl.com/releases?page=2")
        second = page_html()
        pages = {
            upcomingvinyl.listing_url(1): first,
            "https://upcomingvinyl.com/releases?page=2": second,
        }
        result = upcomingvinyl.discover_release_items(
            max_pages=3,
            delay_seconds=0,
            timeout_seconds=1,
            min_date=date(2026, 8, 28),
            fetch_page=lambda url: pages[url],
        )
        self.assertEqual(len(result.items), 1)
        self.assertEqual(result.stop_reason, "duplicate_page")

    def test_fetch_retries_429_with_retry_after(self) -> None:
        session = FakeSession(
            [
                FakeResponse(429, headers={"Retry-After": "0"}),
                FakeResponse(200, text="ok"),
            ]
        )
        original_sleep = upcomingvinyl.time.sleep
        upcomingvinyl.time.sleep = lambda delay: None
        try:
            self.assertEqual(
                upcomingvinyl.fetch(session, "https://upcomingvinyl.com/test", timeout=1),
                "ok",
            )
        finally:
            upcomingvinyl.time.sleep = original_sleep

        self.assertEqual(session.urls, ["https://upcomingvinyl.com/test", "https://upcomingvinyl.com/test"])

    def test_job_writes_only_release_calendar(self) -> None:
        job_source = source("scripts/release_discovery/jobs/discover_upcomingvinyl.py")
        self.assertIn('SOURCE_SHOP = "upcomingvinyl"', job_source)
        self.assertIn("insert into public.release_calendar", job_source.lower())
        self.assertNotIn("insert into public.prices", job_source.lower())
        self.assertNotIn("update public.prices", job_source.lower())
        self.assertNotIn("insert into public.price_history", job_source.lower())

    def test_release_workflow_stops_imusic_release_discovery(self) -> None:
        workflow = source(".github/workflows/release-calendar-bobsvinyl.yml")
        self.assertNotIn("Discover iMusic releases", workflow)
        self.assertNotIn("scripts.release_discovery.jobs.discover_imusic", workflow)

    def test_upcomingvinyl_has_separate_weekly_workflow(self) -> None:
        workflow = source(".github/workflows/release-calendar-upcomingvinyl.yml")
        self.assertIn("Release Calendar - UpcomingVinyl", workflow)
        self.assertIn('cron: "35 6 * * 1"', workflow)
        self.assertIn("scripts.release_discovery.jobs.discover_upcomingvinyl", workflow)


if __name__ == "__main__":
    unittest.main()
