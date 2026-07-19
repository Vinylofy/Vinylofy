#!/usr/bin/env python3
from __future__ import annotations

import re

_WHITESPACE_PATTERN = re.compile(r"\s+")

# Eerst blokkeren we expliciete concurrerende geluidsdragers.
# Een gecombineerd producttype zoals "LP + CD" wordt daardoor conservatief
# niet gepubliceerd totdat daar een aparte, bewezen projectregel voor bestaat.
_EXPLICIT_NON_VINYL_PATTERN = re.compile(
    r"""
    (?:
        \bcd\b
        |\bsacd\b
        |\bblu[\s-]?ray\b
        |\bdvd\b
        |\bcassette\b
        |\btape\b
        |\bdigital\b
        |\bdownload\b
    )
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

# Bewezen Everything Jazz-voorbeelden:
# Vinyl, Vinyl LP, Vinyl 2LP, Acoustic Sounds Vinyl, Tone Poet Vinyl,
# 1LP, 2LP, 4LP-Box, Col. LP + signed Art Card en vergelijkbare LP-labels.
#
# De LP-match vereist een zelfstandig format-token. Daardoor matchen woorden
# waarin de letters "lp" toevallig voorkomen niet.
_VINYL_MARKER_PATTERN = re.compile(
    r"""
    (?:
        \bvinyl\b
        |
        (?<![a-z0-9])
        (?:\d+\s*)?
        lp
        (?:[\s-]*box)?
        (?![a-z0-9])
    )
    """,
    flags=re.IGNORECASE | re.VERBOSE,
)

_BOX_PATTERN = re.compile(r"\bbox(?:set)?\b", flags=re.IGNORECASE)


def normalize_product_type(value: object) -> str:
    return _WHITESPACE_PATTERN.sub(
        " ",
        str(value or "").replace("\xa0", " "),
    ).strip()


def is_everythingjazz_vinyl_type(value: object) -> bool:
    product_type = normalize_product_type(value)
    if not product_type:
        return False
    if _EXPLICIT_NON_VINYL_PATTERN.search(product_type):
        return False
    return bool(_VINYL_MARKER_PATTERN.search(product_type))


def canonical_vinyl_format(value: object) -> str:
    product_type = normalize_product_type(value)
    if not is_everythingjazz_vinyl_type(product_type):
        raise ValueError(f"geen ondersteund vinyl-producttype: {product_type!r}")
    if _BOX_PATTERN.search(product_type):
        return "Vinyl-Box"
    return "Vinyl"
