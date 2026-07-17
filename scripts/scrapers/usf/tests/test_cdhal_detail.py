from __future__ import annotations

from pathlib import Path


def run_tests(module) -> None:
    normal = """
    <table>
      <tr>
        <th class="col label">EAN</th>
        <td class="col data" data-th="EAN">
          889397521097
        </td>
      </tr>
    </table>
    """

    assert (
        module.extract_ean(normal)
        == "889397521097"
    )

    whitespace = """
    <table>
      <tr>
        <td
          class="col data"
          data-th="ean"
        >
          0199957546454
        </td>
      </tr>
    </table>
    """

    assert (
        module.extract_ean(whitespace)
        == "0199957546454"
    )

    missing = """
    <table>
      <tr>
        <td class="col data" data-th="SKU">
          12345
        </td>
      </tr>
    </table>
    """

    assert module.extract_ean(missing) is None

    invalid = """
    <table>
      <tr>
        <td class="col data" data-th="EAN">
          ABC-123
        </td>
      </tr>
    </table>
    """

    assert module.extract_ean(invalid) is None

    duplicate_rows = """
    <table>
      <tr>
        <td class="col data" data-th="SKU">
          999999
        </td>
      </tr>
      <tr>
        <td class="col data" data-th="EAN">
          0199957546454
        </td>
      </tr>
    </table>
    """

    assert (
        module.extract_ean(duplicate_rows)
        == "0199957546454"
    )

    parsed = module.parse_detail_html(
        """
        <html>
          <h1 class="page-title">
            <span class="base">
              Test Artist - Test Album - LP
            </span>
          </h1>
          <table>
            <tr>
              <td class="col data" data-th="EAN">
                889397521097
              </td>
            </tr>
          </table>
        </html>
        """,
        source_url=(
            "https://www.cdhal.nl/test-product"
        ),
        payload={
            "price": "19.99",
            "availability": "in_stock",
        },
    )

    assert parsed["ean_raw"] == "889397521097"
    assert parsed["price_raw"] == "19.99"
    assert (
        parsed["availability_raw"]
        == "in_stock"
    )
    assert parsed["image_url_raw"] is None
    assert (
        parsed["payload"]["detail_price_policy"]
        == (
            "listing_price_and_availability_"
            "are_authoritative"
        )
    )

    print("[TEST-OK] EAN-selector")
    print("[TEST-OK] hoofdletterongevoelig EAN-label")
    print("[TEST-OK] ontbrekende EAN")
    print("[TEST-OK] ongeldige EAN")
    print("[TEST-OK] meerdere tabelrijen")
    print("[TEST-OK] listingprijs blijft leidend")
    print("[TEST-OK] geen shopafbeelding")
