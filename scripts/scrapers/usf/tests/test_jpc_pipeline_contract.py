from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from scripts.scrapers.usf.jobs.detail_jpc import parse_offer
from scripts.scrapers.usf.jobs.discover_jpc_vinyl import (
    RouteSpec,
    add_page_fallback,
    canonical_taxonomy_route_url,
    parse_listing_links,
    listing_page_numbers_for_shard,
    route_is_probably_vinyl,
    select_route_shard,
)


ROOT = Path(__file__).resolve().parents[1]


def run_tests() -> None:
    files = {
        "discovery": ROOT / "jobs" / "discover_jpc_vinyl.py",
        "detail": ROOT / "jobs" / "detail_jpc.py",
        "link_registry": ROOT / "core" / "link_registry.py",
        "requeue": ROOT / "core" / "requeue.py",
        "stage": ROOT / "jobs" / "stage_jpc.py",
        "promote": ROOT / "jobs" / "promote_jpc.py",
        "quarantine": ROOT / "jobs" / "quarantine_jpc.py",
        "price_sync": ROOT / "jobs" / "sync_jpc_listing_prices.py",
        "runner": ROOT / "jobs" / "run_jpc_pipeline.py",
    }

    for name, path in files.items():
        assert path.exists(), f"{name} ontbreekt: {path}"

    runner = files["runner"].read_text(encoding="utf-8")

    for module in (
        "discover_jpc_vinyl",
        "detail_jpc",
        "stage_jpc",
        "promote_jpc",
        "quarantine_jpc",
        "sync_jpc_listing_prices",
    ):
        assert module in runner, f"runner mist module {module}"

    detail = files["detail"].read_text(encoding="utf-8")
    discovery = files["discovery"].read_text(encoding="utf-8")
    link_registry = files["link_registry"].read_text(encoding="utf-8")
    requeue = files["requeue"].read_text(encoding="utf-8")
    promote = files["promote"].read_text(encoding="utf-8")
    price_sync = files["price_sync"].read_text(encoding="utf-8")
    workflow = (REPO_ROOT / ".github" / "workflows" / "usf-jpc.yml").read_text(
        encoding="utf-8"
    )

    assert "missing_ean" in detail
    assert "mark_detail_ean_found" in detail
    assert "last_successful_ean" in link_registry
    assert "raw_shop_scrapes r" in detail
    assert "last_successful_ean" in requeue
    assert "exclude_successful_ean" in requeue
    assert "listing_price_and_availability_are_authoritative" in detail
    assert "listing_price_raw or parsed.price_raw" in detail
    assert "include-route-index" in discovery
    assert "route-shard-index" in discovery
    assert "listing-page-shard-index" in discovery
    assert "max-pages-per-route" in discovery
    assert "--discovery-write" in runner
    assert "--detail-write" in runner
    assert "--price-sync-write" in runner
    assert "skip-requeue" in runner
    assert "--exclude-successful-ean" in runner
    assert "effective_writes" in runner
    assert 'shop_country="DE"' in promote
    assert 'shop_domain="jpc.de"' in promote
    assert "public.raw_shop_scrapes" in price_sync
    assert "public.prices pr" in price_sync
    assert "Nieuwe JPC offers worden niet" in price_sync
    assert 'cron: "17 4,16 * * *"' in workflow
    assert 'cron: "47 1,7,13,19 * * *"' in workflow
    assert 'DETAIL_BURST_START_DATE: "2026-08-31"' in workflow
    assert 'DETAIL_BURST_END_DATE_EXCLUSIVE: "2026-09-28"' in workflow
    assert "--detail-limit 500" in workflow
    assert "--skip-requeue" in workflow
    assert "--sync-listing-prices" in workflow
    assert "--listing-page-shard-count \"$SHARD_COUNT\"" in workflow
    assert "SHARD_COUNT=4" in workflow
    assert "/ 43200" in workflow

    listing_html = """
    <html><body>
      <h3><a href="/jpcng/poprock/detail/-/art/artist-title/hnum/12345678">Artist: Title</a></h3>
      <p>Artikel am Lager</p>
      <p>2 LPs</p>
      <p>EUR 36,99** Vorheriger Preis EUR 36,99, reduziert um 18%</p>
      <p>EUR 29,99* Aktueller Preis: EUR 29,99</p>
      <h3><a href="/jpcng/poprock/detail/-/art/cd-only/hnum/87654321">CD Artist: CD Title</a></h3>
      <p>Artikel am Lager</p>
      <p>CD</p>
      <p>EUR 12,99* Aktueller Preis: EUR 12,99</p>
    </body></html>
    """
    links = parse_listing_links(
        listing_html,
        listing_url="https://www.jpc.de/s/example?searchtype=cid",
        route=RouteSpec("test", "https://www.jpc.de/s/example?searchtype=cid"),
        page_number=1,
    )
    assert len(links) == 1
    assert links[0].source_product_id == "12345678"
    assert links[0].payload["listing_price_raw"] == "29,99"
    assert links[0].payload["listing_availability"] == "in_stock"

    detail_html = """
    <html>
      <head>
        <meta property="og:title" content="Artist: Title - jpc.de">
        <meta property="og:image" content="/image.jpg">
        <link rel="canonical" href="https://www.jpc.de/jpcng/poprock/detail/-/art/artist-title/hnum/12345678">
      </head>
      <body>
        <h1>Artist: Title</h1>
        <p>Artikelnummer: 12345678</p>
        <p>UPC/EAN: 0199584438818</p>
        <p>LP</p>
        <p>lieferbar innerhalb einer Woche</p>
        <p>EUR 34,99* Aktueller Preis: EUR 34,99</p>
      </body>
    </html>
    """
    parsed = parse_offer(
        detail_html,
        final_url="https://www.jpc.de/jpcng/poprock/detail/-/art/artist-title/hnum/12345678",
        status_code=200,
    )
    assert parsed.ean == "0199584438818"
    assert parsed.article_number == "12345678"
    assert parsed.price_raw == "34,99"
    assert parsed.availability == "in_stock"
    assert parsed.format_label == "LP"

    assert route_is_probably_vinyl(
        "Rock",
        "https://www.jpc.de/s/1238692_66733?searchtype=cid",
    )
    assert not route_is_probably_vinyl(
        "Switch to English",
        "https://www.jpc.de/jpcng/vinyl/home?lang=en",
    )
    assert not route_is_probably_vinyl(
        "Vinyl Lagerraeumung",
        "https://www.jpc.de/s/1238777_121878?searchtype=cid&vinyl_home_heroshot_lagerraumung",
    )
    assert not route_is_probably_vinyl(
        "Vinyl immer portofrei",
        "https://www.jpc.de/jpcng/vinyl/static/-/page/vinyl-immer-portofrei",
    )
    assert not route_is_probably_vinyl(
        "Review",
        "https://www.jpc.de/jpcng/poprock/detail/-/art/prince-timeless/hnum/12767124#reviews",
    )
    assert canonical_taxonomy_route_url(
        "https://www.jpc.de/ff/1238692_66697?page=40&searchtype=cid"
    ) == "https://www.jpc.de/s/1238692_66697?searchtype=cid"
    assert add_page_fallback(
        "https://www.jpc.de/s/1238692_66697?searchtype=cid",
        page_number=40,
        mode="ff",
    ) == "https://www.jpc.de/ff/1238692_66697?page=40&searchtype=cid"

    routes = [
        RouteSpec("b", "https://www.jpc.de/s/1238692_66740?searchtype=cid"),
        RouteSpec("a", "https://www.jpc.de/s/1238692_66718?searchtype=cid"),
        RouteSpec("c", "https://www.jpc.de/s/1238692_66733?searchtype=cid"),
    ]
    shard_0 = select_route_shard(routes, shard_index=0, shard_count=2)
    shard_1 = select_route_shard(routes, shard_index=1, shard_count=2)
    assert {route.url for route in shard_0}.isdisjoint(
        {route.url for route in shard_1}
    )
    assert {route.url for route in shard_0 + shard_1} == {
        route.url for route in routes
    }

    page_shards = [
        set(listing_page_numbers_for_shard(
            shard_index=index,
            shard_count=24,
            max_pages_per_route=73,
        ))
        for index in range(24)
    ]
    assert not any(
        left.intersection(right)
        for index, left in enumerate(page_shards)
        for right in page_shards[index + 1 :]
    )
    assert set().union(*page_shards) == set(range(1, 74))

    print("[TEST-OK] alle JPC-jobs aanwezig")
    print("[TEST-OK] runner gebruikt alle JPC-modules")
    print("[TEST-OK] detail vereist EAN voor raw offers")
    print("[TEST-OK] JPC detail bezoekt bekende EANs niet opnieuw")
    print("[TEST-OK] JPC stale requeue sluit succesvolle EAN-items uit")
    print("[TEST-OK] JPC detail blijft listing-first")
    print("[TEST-OK] JPC price sync gebruikt listingprijzen voor bestaande prices")
    print("[TEST-OK] JPC workflow plant 48-uurs listing-shards")
    print("[TEST-OK] JPC workflow plant vier weken detailburst zonder requeue")
    print("[TEST-OK] JPC promotie gebruikt land DE")
    print("[TEST-OK] JPC parsers halen listingprijs en detail-EAN uit HTML")
    print("[TEST-OK] JPC route-index volgt alleen vinyl-taxonomie-CID's")
    print("[TEST-OK] JPC route- en pagina-sharding dekt alle routes en pagina's")
    print("[TEST-OK] JPC runner ondersteunt losse write-flags")
    print("[TEST-OK] JPC pagination gebruikt bewezen /ff/<cid>?page=N route")


if __name__ == "__main__":
    run_tests()
