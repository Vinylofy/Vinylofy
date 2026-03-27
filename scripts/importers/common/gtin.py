"""
GTIN / EAN / UPC normalization helpers for Vinylofy.

Goal:
- treat UPC-A (12 digits), EAN-13 (13 digits) and GTIN-14 (14 digits)
  as the same product when they represent the same underlying code
- use GTIN-14 as the internal canonical match key
- optionally expose a display EAN-13 for UI / debugging

Example:
- 602577427664   -> 00602577427664
- 0602577427664  -> 00602577427664
"""

from __future__ import annotations

import re
from typing import Optional


VALID_GTIN_LENGTHS = {8, 12, 13, 14}


def digits_only(value: object) -> str:
    if value is None:
        return ""
    return re.sub(r"\D", "", str(value).strip())


def compute_gtin_check_digit(body: str) -> str:
    """
    Compute the GS1 check digit for a GTIN body (all digits except the last one).

    Works for GTIN-8 / 12 / 13 / 14 bodies as long as you pass body-without-check-digit.
    Weighting is applied from right to left: 3,1,3,1,...
    """
    if not body or not body.isdigit():
        raise ValueError("GTIN body must contain digits only")

    total = 0
    multiplier = 3
    for ch in reversed(body):
        total += int(ch) * multiplier
        multiplier = 1 if multiplier == 3 else 3

    return str((10 - (total % 10)) % 10)


def is_valid_gtin(value: object) -> bool:
    digits = digits_only(value)
    if len(digits) not in VALID_GTIN_LENGTHS:
        return False
    return compute_gtin_check_digit(digits[:-1]) == digits[-1]


def normalize_gtin(value: object, *, validate: bool = True) -> Optional[str]:
    """
    Normalize EAN / UPC / GTIN to a canonical GTIN-14 key.

    Returns:
        14-digit string, or None when value is empty / invalid / unsupported.

    Rules:
    - strip non-digits
    - accept GTIN lengths 8, 12, 13, 14
    - optionally validate check digit
    - left-pad with zeros to 14 digits
    """
    digits = digits_only(value)
    if not digits:
        return None

    if len(digits) not in VALID_GTIN_LENGTHS:
        return None

    if validate and not is_valid_gtin(digits):
        return None

    return digits.zfill(14)


def gtin14_to_ean13(value: object) -> Optional[str]:
    """
    Convert canonical GTIN-14 to EAN-13 for display when possible.

    For a GTIN-14 that starts with '0', the display EAN-13 is the last 13 digits.
    Example:
        00602577427664 -> 0602577427664
    """
    normalized = normalize_gtin(value, validate=True)
    if not normalized:
        return None

    if normalized.startswith("0"):
        return normalized[1:]

    return normalized


def same_product_code(left: object, right: object, *, validate: bool = True) -> bool:
    left_norm = normalize_gtin(left, validate=validate)
    right_norm = normalize_gtin(right, validate=validate)
    return bool(left_norm and right_norm and left_norm == right_norm)


if __name__ == "__main__":
    examples = [
        "0602577427664",
        "602577427664",
        " 0602577427664 ",
        "EAN 0602577427664",
    ]

    for raw in examples:
        print(
            {
                "raw": raw,
                "digits_only": digits_only(raw),
                "is_valid": is_valid_gtin(raw),
                "gtin_normalized": normalize_gtin(raw),
                "display_ean13": gtin14_to_ean13(raw),
            }
        )

    assert same_product_code("0602577427664", "602577427664") is True
    print("OK: UPC/EAN match works")
