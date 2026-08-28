from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path

from scripts.release_discovery.core.candidates import ReleaseObservation, union_release_observations
from scripts.release_discovery.core.release_weeks import release_fridays
from scripts.release_discovery.jobs import backfill_musicbrainz as mb_backfill
from scripts.release_discovery.jobs import discover_bobsvinyl as bob
from scripts.release_discovery.jobs import discover_imusic as imusic


ROOT = Path(__file__).resolve().parents[2]


def source(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


class ReleaseCalendarContractTests(unittest.TestCase):
    def test_release_only_found_by_bobsvinyl_remains_candidate(self) -> None:
        candidates = union_release_observations(
            [ReleaseObservation("bobsvinyl", "0199584532912", date(2026, 8, 28))]
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].sources, {"bobsvinyl"})

    def test_release_only_found_by_imusic_remains_candidate(self) -> None:
        candidates = union_release_observations(
            [ReleaseObservation("imusic", "0199584532912", date(2026, 8, 28))]
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].sources, {"imusic"})

    def test_release_only_found_by_metadata_source_remains_candidate(self) -> None:
        candidates = union_release_observations(
            [ReleaseObservation("musicbrainz", "0199584532912", date(2026, 8, 28))]
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].sources, {"musicbrainz"})

    def test_missing_second_release_source_does_not_reject_candidate(self) -> None:
        candidates = union_release_observations(
            [ReleaseObservation("bobsvinyl", "0199584532912", date(2026, 8, 28))]
        )
        self.assertEqual([candidate.ean for candidate in candidates], ["0199584532912"])

    def test_same_ean_from_two_release_sources_becomes_one_candidate(self) -> None:
        candidates = union_release_observations(
            [
                ReleaseObservation("bobsvinyl", "0199584532912", date(2026, 8, 28)),
                ReleaseObservation("imusic", "0199584532912", date(2026, 8, 28)),
            ]
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].sources, {"bobsvinyl", "imusic"})

    def test_two_variants_with_different_eans_remain_separate(self) -> None:
        candidates = union_release_observations(
            [
                ReleaseObservation("imusic", "0888072790742", date(2026, 8, 28)),
                ReleaseObservation("imusic", "0888072774186", date(2026, 8, 28)),
            ]
        )
        self.assertEqual([candidate.ean for candidate in candidates], ["0888072774186", "0888072790742"])

    def test_release_date_conflict_does_not_exclude_candidate(self) -> None:
        candidates = union_release_observations(
            [
                ReleaseObservation("bobsvinyl", "0199584532912", date(2026, 8, 28)),
                ReleaseObservation("imusic", "0199584532912", date(2026, 8, 29)),
            ]
        )
        self.assertEqual(len(candidates), 1)
        self.assertEqual(len(candidates[0].date_conflicts), 1)
        self.assertEqual(candidates[0].release_date, date(2026, 8, 28))

    def test_source_absence_does_not_deactivate_existing_release_date(self) -> None:
        combined = (
            source("scripts/release_discovery/jobs/discover_bobsvinyl.py")
            + source("scripts/release_discovery/jobs/discover_imusic.py")
            + source("scripts/release_discovery/jobs/backfill_musicbrainz.py")
            + source("scripts/release_discovery/jobs/discover_upcomingvinyl.py")
        ).lower()
        self.assertNotIn("status = 'inactive'", combined)
        self.assertNotIn("release_date = null", combined)

    def test_historical_boundary_exactly_fourteen_days_ago_is_inclusive(self) -> None:
        min_date, max_date = mb_backfill.release_window(
            past_days=14,
            future_days=0,
            anchor=date(2026, 8, 28),
        )
        row = {
            "id": "product-1",
            "ean": "0199584532912",
            "artist": "Prince",
            "title": "Timeless",
            "format_label": "Vinyl",
            "musicbrainz_artist": "Prince",
            "musicbrainz_title": "Timeless",
            "musicbrainz_format": "12\" Vinyl",
            "musicbrainz_release_date": "2026-08-14",
            "musicbrainz_release_id": "00000000-0000-0000-0000-000000000001",
        }
        self.assertEqual(min_date, date(2026, 8, 14))
        self.assertIsNotNone(mb_backfill.candidate_from_product_row(row, min_date=min_date, max_date=max_date))

    def test_release_older_than_historical_window_is_excluded(self) -> None:
        min_date, max_date = mb_backfill.release_window(
            past_days=14,
            future_days=0,
            anchor=date(2026, 8, 28),
        )
        row = {
            "id": "product-1",
            "ean": "0199584532912",
            "artist": "Prince",
            "title": "Timeless",
            "format_label": "Vinyl",
            "musicbrainz_artist": "Prince",
            "musicbrainz_title": "Timeless",
            "musicbrainz_format": "12\" Vinyl",
            "musicbrainz_release_date": "2026-08-13",
            "musicbrainz_release_id": "00000000-0000-0000-0000-000000000001",
        }
        self.assertIsNone(mb_backfill.candidate_from_product_row(row, min_date=min_date, max_date=max_date))

    def test_one_shop_with_multiple_offer_rows_stays_one_shop(self) -> None:
        data_source = source("lib/vinylofy-data.ts")
        self.assertIn("new Map<string, Set<string>>()", data_source)
        self.assertIn("shopIds.add(row.shop_id);", data_source)
        self.assertIn("shopIds.size", data_source)

    def test_exactly_one_current_shop_satisfies_new_releases_rule(self) -> None:
        data_source = source("lib/vinylofy-data.ts")
        self.assertIn("if (freshShopCount < 1) return false;", data_source)

    def test_current_shop_rule_keeps_offer_filters(self) -> None:
        data_source = source("lib/vinylofy-data.ts")
        self.assertIn(".eq(\"availability\", \"in_stock\")", data_source)
        self.assertIn(".eq(\"is_active\", true)", data_source)
        self.assertIn(".gte(\"last_seen_at\", cutoff)", data_source)

    def test_metadata_release_source_does_not_count_as_shop(self) -> None:
        mb_source = source("scripts/release_discovery/jobs/backfill_musicbrainz.py")
        self.assertIn('SOURCE_SHOP = "musicbrainz"', mb_source)
        self.assertNotIn("insert into public.prices", mb_source.lower())
        self.assertNotIn("update public.prices", mb_source.lower())

    def test_forty_eight_hour_rule_is_only_applied_to_offers(self) -> None:
        data_source = source("lib/vinylofy-data.ts")
        function_start = data_source.index("async function getFreshInstockShopCountMap")
        function_end = data_source.index("function scoreProductMatch")
        offer_count_function = data_source[function_start:function_end]
        self.assertIn(".from(\"prices\")", offer_count_function)
        self.assertIn("48 * 60 * 60 * 1000", offer_count_function)
        self.assertNotIn(".from(\"release_calendar\")", offer_count_function)

    def test_existing_future_releases_still_use_existing_window(self) -> None:
        data_source = source("lib/vinylofy-data.ts")
        self.assertIn("export async function getUpcomingReleaseCalendarItems", data_source)
        self.assertIn("const minDate = isoDateDaysFromToday(1);", data_source)
        self.assertIn("const maxDate = isoDateDaysFromToday(14);", data_source)

    def test_current_release_cards_stop_at_today(self) -> None:
        data_source = source("lib/vinylofy-data.ts")
        current_function_start = data_source.index("export async function getReleaseCalendarItems")
        current_function_end = data_source.index("export async function getUpcomingReleaseCalendarItems")
        current_function = data_source[current_function_start:current_function_end]
        self.assertIn("const minDate = isoDateDaysFromToday(-14);", current_function)
        self.assertIn("const maxDate = isoDateDaysFromToday(0);", current_function)

    def test_future_calendar_does_not_require_current_shop(self) -> None:
        data_source = source("lib/vinylofy-data.ts")
        future_function_start = data_source.index("export async function getUpcomingReleaseCalendarItems")
        future_function = data_source[future_function_start:]
        self.assertIn(".gt(\"release_date\", isoDateDaysFromToday(0))", future_function)
        self.assertIn(".lte(\"release_date\", maxDate)", future_function)
        self.assertNotIn("freshShopCount", future_function)
        self.assertNotIn("getFreshInstockShopCountMap", future_function)

    def test_bobsvinyl_regression_validates_gtin_and_parses_detail(self) -> None:
        html = """
        <html>
          <head>
            <meta property="og:title" content="Prince - Timeless - Bob's Vinyl" />
          </head>
          <body>EAN 0199584532912</body>
        </html>
        """
        item = bob.parse_detail(html, "https://bobsvinyl.nl/products/timeless", date(2026, 8, 28))
        self.assertIsNotNone(item)
        self.assertEqual(item.ean, "0199584532912")
        self.assertIsNone(bob.find_ean("EAN 0199584532913"))

    def test_imusic_regression_keeps_valid_url_ean_and_window_summary(self) -> None:
        item = imusic.ReleaseItem(
            ean="0199584532912",
            gtin_normalized="00199584532912",
            artist="Prince",
            title="Timeless",
            release_date=date(2026, 8, 28),
            source_url="https://imusic.co/music/0199584532912/prince-2026-timeless-lp",
            format="LP",
            label=None,
        )
        self.assertEqual(imusic.ean_from_product_url(item.source_url), "0199584532912")
        self.assertEqual(
            imusic.summarize_window([item], anchor=date(2026, 8, 28)),
            {"items": 1, "within_existing_new_releases_window": 1},
        )

    def test_frontend_query_uses_union_window_and_no_source_intersection(self) -> None:
        data_source = source("lib/vinylofy-data.ts")
        self.assertIn(".from(\"release_calendar\")", data_source)
        self.assertIn(".gte(\"release_date\", minDate)", data_source)
        self.assertIn(".lte(\"release_date\", maxDate)", data_source)
        self.assertNotIn("source_count", data_source)
        self.assertNotIn(".eq(\"source_shop\",", data_source)

    def test_bobsvinyl_default_release_weeks_cover_fourteen_day_boundary(self) -> None:
        dates = release_fridays(anchor=date(2026, 8, 28))
        self.assertIn(date(2026, 8, 14), dates)
        self.assertIn(date(2026, 8, 28), dates)
        self.assertIn(date(2026, 9, 25), dates)


if __name__ == "__main__":
    unittest.main()
