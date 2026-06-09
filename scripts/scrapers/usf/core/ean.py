from __future__ import annotations


VALID_EAN_LENGTHS = {8, 12, 13, 14}


def only_digits(value: str | None) -> str | None:
    if not value:
        return None

    digits = "".join(ch for ch in str(value) if ch.isdigit())
    return digits or None


def normalize_ean(value: str | None) -> str | None:
    digits = only_digits(value)

    if not digits:
        return None

    if len(digits) not in VALID_EAN_LENGTHS:
        return None

    return digits


def ean_match_key(value: str | None) -> str | None:
    normalized = normalize_ean(value)

    if not normalized:
        return None

    stripped = normalized.lstrip("0")
    return stripped or normalized
