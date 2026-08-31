from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO_ROOT))

from scripts.scrapers.usf.jobs.detail_jpc import parse_offer
from scripts.scrapers.usf.jobs.discover_jpc_vinyl import (
    RouteSpec,
    parse_listing_links,
    route_is_probably_vinyl,
)


ROOT = Path(__file__).resolve().parents[1]


def run_tests() -> None:
    files = {
        "discovery": ROOT / "jobs" / "discover_jpc_vinyl.py",
        "detail": ROOT / "jobs" / "detail_jpc.py",
        "stage": ROOT / "jobs" / "stage_jpc.py",
        "promote": ROOT / "jobs" / "promote_jpc.py",
        "quarantine": ROOT / "jobs" / "quarantine_jpc.py",
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
    ):
        assert module in runner, f"runner mist module {module}"

    detail = files["detail"].read_text(encoding="utf-8")
    discovery = files["discovery"].read_text(encoding="utf-8")
    promote = files["promote"].read_text(encoding="utf-8")

    assert "missing_ean" in detail
    assert "listing_price_and_availability_are_authoritative" in detail
    assert "listing_price_raw or parsed.price_raw" in detail
    assert "include-route-index" in discovery
    assert "--discovery-write" in runner
    assert "--detail-write" in runner
    assert "effective_writes" in runner
    assert 'shop_country="DE"' in promote
    assert 'shop_domain="jpc.de"' in promote

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

    print("[TEST-OK] alle JPC-jobs aanwezig")
    print("[TEST-OK] runner gebruikt alle JPC-modules")
    print("[TEST-OK] detail vereist EAN voor raw offers")
    print("[TEST-OK] JPC detail blijft listing-first")
    print("[TEST-OK] JPC promotie gebruikt land DE")
    print("[TEST-OK] JPC parsers halen listingprijs en detail-EAN uit HTML")
    print("[TEST-OK] JPC route-index volgt alleen vinyl-taxonomie-CID's")
    print("[TEST-OK] JPC runner ondersteunt losse write-flags")


if __name__ == "__main__":
    run_tests()
