from __future__ import annotations

import sys
from unittest.mock import patch

from scripts.scrapers.usf.jobs import refresh_variaworld_listing_prices as scraper


class FakeResponse:
    def __init__(self, body: str, *, url: str = "https://example.test/listing") -> None:
        self.status_code = 200
        self.text = body
        self.content = body.encode()
        self.headers = {"Content-Type": "text/html; charset=utf-8"}
        self.url = url
        self.encoding = "utf-8"
        self.apparent_encoding = "utf-8"

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = iter(responses)
        self.calls = 0
        self.headers = {}

    def get(self, url: str, *, timeout: int) -> FakeResponse:
        self.calls += 1
        return next(self.responses)


VALID_LISTING = """
<html><title>Variaworld listing</title><body>
<div id="overzicht_container">
  <a class="overzichtbox_2" href="/artikel/detail.php?at=123">
    <span class="koptekst">Artist</span>
    <span class="tekst">Album</span>
    <span class="overzicht_prijs">€ 12,95</span>
  </a>
</div>
</body></html>
"""


def test_empty_listing_retries_are_bounded_and_later_listing_succeeds() -> None:
    empty = FakeResponse("<html><title>Access denied</title><body>verify you are human</body></html>")
    session = FakeSession([empty, empty, FakeResponse(VALID_LISTING)])

    with (
        patch.object(scraper, "SEEDS", {"lp_nieuw": "https://example.test/page={page}"}),
        patch.object(scraper.requests, "Session", return_value=session),
        patch.object(scraper, "time") as mocked_time,
        patch.object(sys, "argv", ["refresh_variaworld_listing_prices.py", "--max-pages", "1", "--sleep", "0"]),
    ):
        assert scraper.main() == 0

    assert session.calls == 3
    assert mocked_time.sleep.call_count == 2


def test_empty_listing_stops_after_configured_retry_limit() -> None:
    session = FakeSession([FakeResponse("<html><title>Challenge</title></html>")] * 4)

    with (
        patch.object(scraper, "SEEDS", {"lp_nieuw": "https://example.test/page={page}"}),
        patch.object(scraper.requests, "Session", return_value=session),
        patch.object(scraper, "time") as mocked_time,
        patch.object(sys, "argv", [
            "refresh_variaworld_listing_prices.py",
            "--max-pages",
            "1",
            "--sleep",
            "0",
            "--empty-page-retries",
            "2",
        ]),
    ):
        try:
            scraper.main()
        except SystemExit as exc:
            assert exc.code == "[ERROR] Variaworld listing refresh leverde geen links op."
        else:
            raise AssertionError("expected empty listing failure")

    assert session.calls == 3
    assert mocked_time.sleep.call_count == 2
