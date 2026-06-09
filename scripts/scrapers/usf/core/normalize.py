from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None

    cleaned = re.sub(r"\s+", " ", str(value)).strip()
    return cleaned or None


def normalize_price(value: str | int | float | Decimal | None) -> Decimal | None:
    if value is None:
        return None

    if isinstance(value, Decimal):
        return value

    text = str(value).strip()
    if not text:
        return None

    text = text.replace("€", "").replace("EUR", "").strip()
    text = re.sub(r"[^0-9,.\-]", "", text)

    if not text:
        return None

    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")

    try:
        amount = Decimal(text)
    except InvalidOperation:
        return None

    if amount <= 0:
        return None

    return amount.quantize(Decimal("0.01"))


def normalize_availability(value: str | None) -> str | None:
    text = clean_text(value)
    if not text:
        return None

    lower = text.lower()

    if any(x in lower for x in ["op voorraad", "in stock", "available", "leverbaar"]):
        return "in_stock"

    if any(x in lower for x in ["uitverkocht", "out of stock", "sold out", "niet leverbaar"]):
        return "out_of_stock"

    if any(x in lower for x in ["pre-order", "preorder", "verwacht", "expected"]):
        return "preorder"

    return "unknown"
