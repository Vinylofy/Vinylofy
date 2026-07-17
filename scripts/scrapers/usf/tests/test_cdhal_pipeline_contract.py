from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[4]


def run_tests() -> None:
    files = {
        "listing": ROOT / "jobs" / "refresh_cdhal_listing_prices.py",
        "detail": ROOT / "jobs" / "detail_cdhal.py",
        "stage": ROOT / "jobs" / "stage_cdhal.py",
        "promote": ROOT / "jobs" / "promote_cdhal.py",
        "quarantine": ROOT / "jobs" / "quarantine_cdhal.py",
        "runner": ROOT / "jobs" / "run_cdhal_pipeline.py",
    }

    for name, path in files.items():
        assert path.exists(), f"{name} ontbreekt: {path}"

    runner = files["runner"].read_text(encoding="utf-8")

    for module in (
        "refresh_cdhal_listing_prices",
        "detail_cdhal",
        "stage_cdhal",
        "promote_cdhal",
        "quarantine_cdhal",
    ):
        assert module in runner, (
            f"runner mist module {module}"
        )

    for forbidden in (
        "shop3345",
        "detail_shop3345",
        "stage_shop3345",
        "promote_shop3345",
        "quarantine_shop3345",
        "run_shop3345",
    ):
        for name, path in files.items():
            text = path.read_text(encoding="utf-8")
            assert forbidden not in text, (
                f"{name} bevat oude referentie {forbidden}"
            )

    detail = files["detail"].read_text(encoding="utf-8")

    assert "image_url_raw=None" in detail
    assert (
        "listing_price_and_availability_are_authoritative"
        in detail
    )

    print("[TEST-OK] alle CDHAL-jobs aanwezig")
    print("[TEST-OK] runner gebruikt alle CDHAL-modules")
    print("[TEST-OK] geen oude 3345-modulereferenties")
    print("[TEST-OK] detail blijft listing-first")
    print("[TEST-OK] detail slaat geen CDHAL-afbeeldingen op")
