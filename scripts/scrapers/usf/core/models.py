from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class DiscoveredLink:
    shop_id: str
    source_url: str
    source_product_id: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RawProductData:
    shop_id: str
    source_url: str
    source_product_id: str | None = None
    title_raw: str | None = None
    ean_raw: str | None = None
    price_raw: str | None = None
    availability_raw: str | None = None
    image_url_raw: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StagedOffer:
    shop_id: str
    source_url: str
    source_product_id: str | None
    title_normalized: str | None
    ean_normalized: str | None
    ean_match_key: str | None
    price: Decimal | None
    currency: str
    availability: str | None
    image_url: str | None
    stage_status: str
    stage_reason: str | None = None
