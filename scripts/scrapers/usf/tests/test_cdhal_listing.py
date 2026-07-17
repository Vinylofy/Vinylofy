from pathlib import Path

FIXTURE_PATH = Path(".local/cdhal-work/vinyl-page-1.html")


def run_tests(module) -> None:
    from datetime import datetime, timezone

    assert module.normalize_price("€\xa019,99") == "19.99"
    assert module.normalize_price("€ 1.234,56") == "1234.56"
    assert module.normalize_price("0.00") is None

    assert (
        module.canonical_product_url(
            "/album?utm_source=test#image"
        )
        == "https://www.cdhal.nl/album"
    )

    assert module.canonical_product_url(
        "/vinyl?p=2"
    ) == ""

    assert module.normalize_availability(
        "Direct leverbaar"
    ) == (
        "in_stock",
        "direct",
        True,
    )

    assert module.normalize_availability(
        "Binnenkort leverbaar"
    ) == (
        "preorder",
        "coming_soon",
        True,
    )

    assert module.normalize_availability(
        "Niet op voorraad: Levertijd 3-4 Werkdagen"
    ) == (
        "unknown",
        "backorder_with_lead_time",
        False,
    )

    assert module.normalize_availability(
        "Tijdelijk niet leverbaar"
    ) == (
        "out_of_stock",
        "temporary_unavailable",
        False,
    )

    normal_html = """
    <li class="product-item">
      <div data-product-id="23349">
        <a class="product-item-link" href="/normal-album">
          Artist - Normal Album
        </a>
        <div class="price-box">
          <span data-price-type="finalPrice"
                data-price-amount="19.99">
            <span class="price">€ 19,99</span>
          </span>
        </div>
        <div>Direct leverbaar</div>
      </div>
    </li>
    """

    links, offers, diagnostics = module.parse_listing_page(
        normal_html,
        page=1,
        source_listing_url="https://www.cdhal.nl/vinyl",
        seen_at=datetime(
            2026,
            7,
            17,
            tzinfo=timezone.utc,
        ),
    )

    assert len(links) == 1
    assert len(offers) == 1
    assert str(offers[0].price) == "19.99"
    assert offers[0].availability == "in_stock"
    assert diagnostics["prices"] == 1

    sale_html = """
    <li class="product-item">
      <div data-product-id="12221">
        <a class="product-item-link" href="/sale-album">
          Artist - Sale Album
        </a>
        <div class="price-box">
          <span class="special-price">
            <span data-price-type="finalPrice"
                  data-price-amount="13.99">
              <span class="price">€ 13,99</span>
            </span>
          </span>
          <span class="old-price">
            <span data-price-type="oldPrice"
                  data-price-amount="16.99">
              <span class="price">€ 16,99</span>
            </span>
          </span>
        </div>
        <div>Binnenkort leverbaar</div>
      </div>
    </li>
    """

    links, offers, diagnostics = module.parse_listing_page(
        sale_html,
        page=1,
        source_listing_url="https://www.cdhal.nl/vinyl",
        seen_at=datetime(
            2026,
            7,
            17,
            tzinfo=timezone.utc,
        ),
    )

    assert len(links) == 1
    assert len(offers) == 1
    assert str(offers[0].price) == "13.99"
    assert links[0].payload["old_price"] == "16.99"
    assert links[0].payload["sale"] is True
    assert offers[0].availability == "preorder"
    assert diagnostics["sales"] == 1

    zero_html = """
    <li class="product-item">
      <div>
        <a class="product-item-link" href="/zero-product">
          Geen product
        </a>
        <div class="price-box">
          <span data-price-type="finalPrice"
                data-price-amount="0.00">
            <span class="price">€ 0,00</span>
          </span>
        </div>
        <div>Direct leverbaar</div>
      </div>
    </li>
    """

    links, offers, diagnostics = module.parse_listing_page(
        zero_html,
        page=1,
        source_listing_url="https://www.cdhal.nl/vinyl",
        seen_at=datetime(
            2026,
            7,
            17,
            tzinfo=timezone.utc,
        ),
    )

    assert len(links) == 1
    assert offers == []
    assert diagnostics["zero_prices"] == 1

    real_html = FIXTURE_PATH.read_text(
        encoding="utf-8"
    )

    links, offers, diagnostics = module.parse_listing_page(
        real_html,
        page=1,
        source_listing_url="https://www.cdhal.nl/vinyl",
        seen_at=datetime(
            2026,
            7,
            17,
            tzinfo=timezone.utc,
        ),
    )

    assert diagnostics["cards"] >= 30
    assert diagnostics["valid_links"] >= 30
    assert diagnostics["prices"] >= 25
    assert len(links) >= 30
    assert len(offers) >= 25

    print("[TEST-OK] prijsnormalisatie")
    print("[TEST-OK] URL-normalisatie")
    print("[TEST-OK] beschikbaarheidsmapping")
    print("[TEST-OK] normale prijs")
    print("[TEST-OK] saleprijs en oude prijs")
    print("[TEST-OK] nulprijs uitgesloten")
    print("[TEST-OK] echte CDHAL-fixture")
    print(
        "[TEST-FIXTURE]",
        diagnostics,
    )
