#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

TARGET_FILES = {
    "bobsvinyl": PROJECT_ROOT / "data/raw/bobsvinyl/bobsvinyl_step2_enriched.csv",
    "dgmoutlet": PROJECT_ROOT / "data/raw/dgmoutlet/dgmoutlet_products.csv",
    "platenzaak": PROJECT_ROOT / "data/raw/platenzaak/platenzaak_master.csv",
    "platomania": PROJECT_ROOT / "data/raw/platomania/platomania_step2_enriched.csv",
    "recordsonvinyl": PROJECT_ROOT / "data/raw/recordsonvinyl/recordsonvinyl_products.csv",
}

LIKELY_IMAGE_COLUMNS = (
    "image",
    "image_url",
    "image_src",
    "img",
    "img_url",
    "cover",
    "cover_url",
    "cover_image",
    "thumbnail",
    "thumbnail_url",
    "featured_image",
    "featured_image_url",
    "picture",
    "picture_url",
    "photo",
    "photo_url",
    "afbeelding",
    "afbeelding_url",
)


def normalize(value: str | None) -> str:
    return (value or "").strip()


def inspect_csv(path: Path) -> dict:
    result: dict = {
        "path": str(path),
        "exists": path.exists(),
        "headers": [],
        "matching_image_headers": [],
        "sample_values_for_matching_headers": {},
        "sample_row_subset": {},
        "row_count_probe": 0,
    }

    if not path.exists():
        return result

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        result["headers"] = headers

        matching_headers = []
        for header in headers:
            lowered = header.lower()
            if any(token in lowered for token in LIKELY_IMAGE_COLUMNS):
                matching_headers.append(header)

        result["matching_image_headers"] = matching_headers

        first_row = None
        for idx, row in enumerate(reader, start=1):
            if first_row is None:
                first_row = row
            result["row_count_probe"] = idx
            if idx >= 5:
                break

        if first_row:
            for header in matching_headers:
                result["sample_values_for_matching_headers"][header] = normalize(first_row.get(header))

            for header in headers:
                lowered = header.lower()
                if (
                    "url" in lowered
                    or "image" in lowered
                    or "cover" in lowered
                    or "thumbnail" in lowered
                    or header in {"ean", "ean13", "title", "artist", "album", "product_url"}
                ):
                    result["sample_row_subset"][header] = normalize(first_row.get(header))

    return result


def main() -> None:
    output = {}
    for shop_key, path in TARGET_FILES.items():
        output[shop_key] = inspect_csv(path)
    print(json.dumps(output, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()