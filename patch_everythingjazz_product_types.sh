#!/usr/bin/env bash
set -Eeuo pipefail

fail() {
  printf '\n[ERROR] %s\n' "$*" >&2
  exit 1
}

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" \
  || fail "voer dit script binnen de Vinylofy-repository uit"
cd "$ROOT"

REMOTE="$(git remote get-url origin 2>/dev/null || true)"
case "$REMOTE" in
  https://github.com/Vinylofy/Vinylofy|https://github.com/Vinylofy/Vinylofy.git|git@github.com:Vinylofy/Vinylofy.git)
    ;;
  *)
    fail "onverwachte origin: ${REMOTE:-<geen>}; verwacht Vinylofy/Vinylofy"
    ;;
esac

LISTING="scripts/scrapers/usf/jobs/refresh_everythingjazz_listing_prices.py"
DETAIL="scripts/scrapers/usf/jobs/detail_everythingjazz.py"
HELPER="scripts/scrapers/usf/jobs/everythingjazz_product_type.py"

for path in "$LISTING" "$DETAIL"; do
  [[ -f "$path" ]] || fail "verwacht bestand ontbreekt: $path"
done

[[ ! -e "$HELPER" ]] \
  || fail "helper bestaat al; patch stopt om niets te overschrijven: $HELPER"

printf '== Repository ==\n'
printf 'root:   %s\n' "$ROOT"
printf 'origin: %s\n' "$REMOTE"
printf 'branch: %s\n' "$(git branch --show-current)"
printf '\n== Status vóór patch ==\n'
git status --short

BACKUP_DIR="/tmp/vinylofy-everythingjazz-product-types-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP_DIR"
cp -a "$LISTING" "$DETAIL" "$BACKUP_DIR/"
printf '\nBackup: %s\n' "$BACKUP_DIR"

cat > "$HELPER" <<'PY'
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
PY

python - "$LISTING" "$DETAIL" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

listing_path, detail_path = map(Path, sys.argv[1:])


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"[PATCH-ERROR] {label}: verwachtte exact 1 match, vond {count}"
        )
    return text.replace(old, new, 1)


listing = listing_path.read_text(encoding="utf-8")

listing = replace_once(
    listing,
    "from scripts.scrapers.usf.core.models import DiscoveredLink\n",
    "from scripts.scrapers.usf.core.models import DiscoveredLink\n"
    "from scripts.scrapers.usf.jobs.everythingjazz_product_type import (\n"
    "    canonical_vinyl_format,\n"
    "    is_everythingjazz_vinyl_type,\n"
    ")\n",
    label="listing-helper-import",
)

listing = replace_once(
    listing,
    'ALLOWED_PRODUCT_TYPES = {"vinyl", "vinyl-box"}\n',
    "",
    label="listing-oude-allowlist",
)

listing = replace_once(
    listing,
    "    if product_type.casefold() not in ALLOWED_PRODUCT_TYPES:\n"
    '        return None, None, "unsupported_product_type"\n',
    "    if not is_everythingjazz_vinyl_type(product_type):\n"
    '        return None, None, "unsupported_product_type"\n',
    label="listing-classificatie",
)

listing = replace_once(
    listing,
    '        "format": "Vinyl-Box" if product_type.casefold() == "vinyl-box" else "Vinyl",\n',
    '        "format": canonical_vinyl_format(product_type),\n',
    label="listing-canonical-format",
)

listing_path.write_text(listing, encoding="utf-8")


detail = detail_path.read_text(encoding="utf-8")

detail = replace_once(
    detail,
    "from scripts.scrapers.usf.core.link_registry import insert_raw_shop_scrape, mark_detail_scraped\n",
    "from scripts.scrapers.usf.core.link_registry import insert_raw_shop_scrape, mark_detail_scraped\n"
    "from scripts.scrapers.usf.jobs.everythingjazz_product_type import (\n"
    "    is_everythingjazz_vinyl_type,\n"
    ")\n",
    label="detail-helper-import",
)

detail = replace_once(
    detail,
    'ALLOWED_PRODUCT_TYPES = {"vinyl", "vinyl-box"}\n',
    "",
    label="detail-oude-allowlist",
)

detail = replace_once(
    detail,
    "    if product_type.casefold() not in ALLOWED_PRODUCT_TYPES:\n"
    '        detail_issue = "unsupported_product_type"\n',
    "    if not is_everythingjazz_vinyl_type(product_type):\n"
    '        detail_issue = "unsupported_product_type"\n',
    label="detail-classificatie",
)

detail_path.write_text(detail, encoding="utf-8")

print("[PATCH] gedeelde Everything Jazz-producttypeclassificatie toegevoegd")
print("[PATCH] listing en detail gebruiken nu dezelfde conservatieve regels")
PY

printf '\n== Classificatietests op de feitelijk aangetroffen typen ==\n'
python - <<'PY'
from scripts.scrapers.usf.jobs.everythingjazz_product_type import (
    canonical_vinyl_format,
    is_everythingjazz_vinyl_type,
)

accepted = [
    "1LP",
    "Col. LP + signed Art card",
    "2LP",
    "Exclusive 2LP + Signed Art Card",
    "Vinyl 2LP",
    "Vinyl",
    "Vinyl LP",
    "LP + signed ArtCard",
    "4LP-Box",
    "Col. LP + sign. Art Card + White Label",
    "Col. LP + sign. Art Card",
    "LP + sign. Art Card + White Label",
    "LP + sign. Art Card",
    "Acoustic Sounds Vinyl",
    "Tone Poet Vinyl",
    "Limited Contemporary Records LP",
]

rejected = [
    "CD + signed Art card",
    "CD + signed ArtCard",
    "Blu-ray Audio",
    "LP + CD",
    "",
    "Merchandise",
]

failures = []

for value in accepted:
    if not is_everythingjazz_vinyl_type(value):
        failures.append(f"had geaccepteerd moeten worden: {value!r}")

for value in rejected:
    if is_everythingjazz_vinyl_type(value):
        failures.append(f"had afgewezen moeten worden: {value!r}")

if canonical_vinyl_format("4LP-Box") != "Vinyl-Box":
    failures.append("4LP-Box kreeg niet canonical format Vinyl-Box")

if canonical_vinyl_format("Tone Poet Vinyl") != "Vinyl":
    failures.append("Tone Poet Vinyl kreeg niet canonical format Vinyl")

if failures:
    raise SystemExit("[TEST-ERROR]\n- " + "\n- ".join(failures))

print(
    {
        "accepted_tests": len(accepted),
        "rejected_tests": len(rejected),
        "box_format": canonical_vinyl_format("4LP-Box"),
        "regular_format": canonical_vinyl_format("Tone Poet Vinyl"),
    }
)
PY

printf '\n== Python compilechecks ==\n'
python -m py_compile \
  "$HELPER" \
  "$LISTING" \
  "$DETAIL"

printf '\n== Module- en CLI-checks ==\n'
python -m scripts.scrapers.usf.jobs.refresh_everythingjazz_listing_prices --help >/dev/null
printf '[OK] listing --help\n'
python -m scripts.scrapers.usf.jobs.detail_everythingjazz --help >/dev/null
printf '[OK] detail --help\n'

printf '\n== Diffcontrole ==\n'
git diff --check
git diff --stat -- "$HELPER" "$LISTING" "$DETAIL"
git diff -- "$HELPER" "$LISTING" "$DETAIL"

printf '\n== Eindstatus ==\n'
git status --short
printf '\nKLAAR — alleen classificatiecode aangepast; geen webrequest, databasewrite, commit of push uitgevoerd.\n'
