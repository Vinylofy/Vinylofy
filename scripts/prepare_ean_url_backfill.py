#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit, urlunsplit

SHOP_MAP = {
    "3345": "shop3345",
    "bobsvinyl": "bobsvinyl",
    "dgmoutlet": "dgmoutlet",
    "groovespin": "groovespin",
    "platenzaak": "platenzaak",
    "platomania": "platomania",
    "recordsonvinyl": "recordsonvinyl",
    "soundsvenlo": "soundsvenlo",
    "variaworld": "variaworld",
}

ALLOWED_EAN_LENGTHS_STRICT = {8, 12, 13, 14}
ALLOWED_EAN_LENGTHS_PRAGMATIC = {8, 11, 12, 13, 14}


def sniff_delimiter(path: Path) -> str:
    sample = path.read_text(encoding="utf-8-sig", errors="replace")[:4096]
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
    except csv.Error:
        return ";" if sample.count(";") >= sample.count(",") else ","


def normalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""
    parts = urlsplit(raw)
    scheme = (parts.scheme or "https").lower()
    netloc = parts.netloc.lower().strip()
    path = re.sub(r"/+", "/", parts.path or "")
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit((scheme, netloc, path, "", ""))


def clean_ean(ean: str) -> tuple[str, str]:
    raw = (ean or "").strip()
    if not raw:
        return "", "missing_ean"
    if "e+" in raw.lower() or "e-" in raw.lower():
        return "", "scientific_notation"
    compact = raw.replace(" ", "").replace("-", "")
    if compact.startswith("="):
        compact = compact.lstrip("=").strip('"')
    if not compact.isdigit():
        digits = re.sub(r"\D", "", compact)
        if digits != compact and digits:
            compact = digits
        else:
            return "", "non_numeric_ean"
    return compact, ""


def iter_rows(path: Path) -> Iterable[dict[str, str]]:
    delim = sniff_delimiter(path)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        for row in reader:
            yield {str(k).strip(): (v or "").strip() for k, v in row.items() if k is not None}


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare safe EAN/URL backfill candidates.")
    parser.add_argument("input_csv", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("output/ean_backfill"))
    parser.add_argument("--allow-11-digit", action="store_true", help="Allow 11-digit codes in the clean output.")
    parser.add_argument("--exclude-shops", nargs="*", default=["variaworld"])
    args = parser.parse_args()

    allowed_lengths = ALLOWED_EAN_LENGTHS_PRAGMATIC if args.allow_11_digit else ALLOWED_EAN_LENGTHS_STRICT
    excluded = {s.strip().lower() for s in args.exclude_shops}

    raw_rows = list(iter_rows(args.input_csv))
    stats = Counter()
    safe_candidates: dict[tuple[str, str], dict] = {}
    duplicate_values: dict[tuple[str, str], set[str]] = defaultdict(set)
    quarantine_rows: list[dict] = []

    for idx, row in enumerate(raw_rows, start=2):
        stats["input_rows"] += 1
        shop_raw = row.get("shop", "").strip().lower()
        shop_key = SHOP_MAP.get(shop_raw, shop_raw)
        url_raw = row.get("shop_url", "") or row.get("product_url", "")
        url_norm = normalize_url(url_raw)
        ean_clean, reason = clean_ean(row.get("ean", ""))

        common = {
            "source_row": idx,
            "shop_raw": shop_raw,
            "shop_key": shop_key,
            "original_product_url": url_raw,
            "normalized_product_url": url_norm,
            "ean_original": row.get("ean", ""),
            "ean_clean": ean_clean,
        }

        if not shop_key:
            quarantine_rows.append({**common, "reason": "missing_shop"})
            stats["quarantine_missing_shop"] += 1
            continue
        if shop_key in excluded:
            quarantine_rows.append({**common, "reason": "excluded_shop"})
            stats[f"quarantine_excluded_{shop_key}"] += 1
            continue
        if not url_norm:
            quarantine_rows.append({**common, "reason": "missing_or_invalid_url"})
            stats["quarantine_bad_url"] += 1
            continue
        if reason:
            quarantine_rows.append({**common, "reason": reason})
            stats[f"quarantine_{reason}"] += 1
            continue
        if len(ean_clean) not in allowed_lengths:
            quarantine_rows.append({**common, "reason": f"invalid_ean_length_{len(ean_clean)}"})
            stats[f"quarantine_invalid_ean_length_{len(ean_clean)}"] += 1
            continue

        key = (shop_key, url_norm)
        duplicate_values[key].add(ean_clean)
        existing = safe_candidates.get(key)
        if existing is None:
            safe_candidates[key] = common
        elif existing["ean_clean"] != ean_clean:
            stats["candidate_conflicting_duplicates"] += 1

    clean_rows: list[dict] = []
    conflict_rows: list[dict] = []
    for key, row in safe_candidates.items():
        eans = duplicate_values[key]
        if len(eans) > 1:
            conflict_rows.append({
                **row,
                "conflict_type": "multiple_eans_for_same_shop_url",
                "conflicting_eans": "|".join(sorted(eans)),
            })
            stats["conflict_multiple_eans_for_same_shop_url"] += 1
            continue
        clean_rows.append(row)
        stats["clean_rows"] += 1

    stats["unique_shop_url_candidates"] = len(safe_candidates)
    stats["quarantine_rows"] = len(quarantine_rows)
    stats["conflict_rows"] = len(conflict_rows)

    outdir = args.output_dir
    write_csv(
        outdir / "ean_url_backfill_clean.csv",
        clean_rows,
        [
            "source_row",
            "shop_raw",
            "shop_key",
            "original_product_url",
            "normalized_product_url",
            "ean_original",
            "ean_clean",
        ],
    )
    write_csv(
        outdir / "ean_url_backfill_quarantine.csv",
        quarantine_rows,
        [
            "source_row",
            "shop_raw",
            "shop_key",
            "original_product_url",
            "normalized_product_url",
            "ean_original",
            "ean_clean",
            "reason",
        ],
    )
    write_csv(
        outdir / "ean_url_backfill_conflicts.csv",
        conflict_rows,
        [
            "source_row",
            "shop_raw",
            "shop_key",
            "original_product_url",
            "normalized_product_url",
            "ean_original",
            "ean_clean",
            "conflict_type",
            "conflicting_eans",
        ],
    )

    stats_payload = {
        "input_csv": str(args.input_csv),
        "allow_11_digit": args.allow_11_digit,
        "excluded_shops": sorted(excluded),
        "stats": dict(stats),
    }
    (outdir / "ean_url_backfill_stats.json").write_text(json.dumps(stats_payload, indent=2), encoding="utf-8")
    print(json.dumps(stats_payload, indent=2))


if __name__ == "__main__":
    main()
