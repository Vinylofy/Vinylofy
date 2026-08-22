#!/usr/bin/env bash

# Dit bestand altijd uitvoeren met:
#   bash run_everythingjazz_first_write_safe.sh
# Niet sourcen. Bij fouten stopt alleen dit subprocess, nooit de terminal.
if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then
  echo "[ERROR] Source dit bestand niet. Gebruik: bash ${BASH_SOURCE[0]}"
  return 2
fi

set -u
set -o pipefail

fail() {
  printf '\n[ERROR] %s\n' "$*" >&2
  exit 1
}

ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" \
  || fail "voer dit script binnen de Vinylofy-repository uit"
cd "$ROOT" || fail "kan repository-root niet openen"

REMOTE="$(git remote get-url origin 2>/dev/null || true)"
case "$REMOTE" in
  https://github.com/Vinylofy/Vinylofy|https://github.com/Vinylofy/Vinylofy.git|git@github.com:Vinylofy/Vinylofy.git)
    ;;
  *)
    fail "onverwachte origin: ${REMOTE:-<geen>}; verwacht Vinylofy/Vinylofy"
    ;;
esac

LISTING_MODULE="scripts.scrapers.usf.jobs.refresh_everythingjazz_listing_prices"
LISTING_FILE="scripts/scrapers/usf/jobs/refresh_everythingjazz_listing_prices.py"
SHIPPING_SOURCE="data/shipping/vinylofy_shipping_rules_nl.csv"
SHIPPING_IMPORTER="scripts/tools/import_shipping_rules.py"

for path in "$LISTING_FILE" "$SHIPPING_SOURCE" "$SHIPPING_IMPORTER"; do
  [[ -f "$path" ]] || fail "vereist bestand ontbreekt: $path"
done

RUN_ID="$(date +%Y%m%d-%H%M%S)"
LISTING_LOG="/tmp/everythingjazz-first-write-${RUN_ID}.log"
SHIPPING_LOG="/tmp/everythingjazz-shipping-import-${RUN_ID}.log"
SUMMARY="/tmp/everythingjazz-first-write-summary-${RUN_ID}.json"
SHIPPING_TMP="/tmp/everythingjazz-shipping-only-${RUN_ID}.csv"

printf '== Repository ==\n'
printf 'root:   %s\n' "$ROOT"
printf 'origin: %s\n' "$REMOTE"
printf 'branch: %s\n' "$(git branch --show-current)"
printf '\n== Gitstatus vooraf ==\n'
git status --short

printf '\n== 1. Compile- en CLI-controle ==\n'
python -m py_compile "$LISTING_FILE" \
  || fail "listingmodule compileert niet"

python -m "$LISTING_MODULE" --help >/dev/null 2>&1 \
  || fail "listingmodule kan niet als python -m worden gestart"

printf '[OK] listingmodule\n'

printf '\n== 2. Read-only databasepreflight ==\n'
python - <<'PY'
from scripts.scrapers.usf.core.db import db_connection, get_database_url
from urllib.parse import urlparse

database_url = get_database_url()
parsed = urlparse(database_url)

with db_connection() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            select
              to_regclass('public.shop_product_links') is not null,
              to_regclass('public.shops') is not null,
              to_regclass('public.prices') is not null,
              to_regclass('public.shop_shipping_rules') is not null
            """
        )
        checks = cursor.fetchone()

if not all(checks):
    raise SystemExit(
        "[ERROR] een of meer vereiste Supabase-tabellen ontbreken: "
        f"{checks!r}"
    )

print(
    "[OK]",
    {
        "host": parsed.hostname,
        "database": parsed.path.lstrip("/"),
        "required_tables": True,
    },
)
PY
RC=$?
[[ "$RC" -eq 0 ]] || fail "databasepreflight faalde"

printf '\n== 3. Isoleer en valideer shippingregel ==\n'
python - "$SHIPPING_SOURCE" "$SHIPPING_TMP" <<'PY'
from __future__ import annotations

import csv
import sys
from pathlib import Path

source = Path(sys.argv[1])
target = Path(sys.argv[2])

with source.open(newline="", encoding="utf-8-sig") as handle:
    reader = csv.DictReader(handle)
    fieldnames = reader.fieldnames
    rows = [
        row
        for row in reader
        if (row.get("shop_slug") or "").strip() == "everythingjazz"
    ]

if not fieldnames:
    raise SystemExit("[ERROR] shipping-CSV heeft geen kolommen")

if len(rows) != 1:
    raise SystemExit(
        f"[ERROR] verwacht exact één Everything Jazz-regel, vond {len(rows)}"
    )

row = rows[0]
checks = {
    "country_code": (row.get("country_code") or "").strip() == "NL",
    "currency": (row.get("currency") or "").strip() == "EUR",
    "shipping_cost_cents": (
        row.get("shipping_cost_cents") or ""
    ).strip() == "995",
    "free_shipping_threshold_cents": not (
        row.get("free_shipping_threshold_cents") or ""
    ).strip(),
    "shipping_logic": (
        row.get("shipping_logic") or ""
    ).strip().lower() == "flat",
    "active": (
        row.get("active") or ""
    ).strip().lower() == "true",
}

if not all(checks.values()):
    raise SystemExit(
        f"[ERROR] shippingregel wijkt af: checks={checks}, row={row}"
    )

with target.open("w", newline="", encoding="utf-8") as handle:
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow(row)

print(
    "[OK]",
    {
        "shop_slug": row.get("shop_slug"),
        "shipping_cost_cents": row.get("shipping_cost_cents"),
        "free_shipping_threshold_cents": row.get(
            "free_shipping_threshold_cents"
        ),
        "temporary_file": str(target),
    },
)
PY
RC=$?
[[ "$RC" -eq 0 ]] || fail "shippingvalidatie faalde"

printf '\n== 4. Database-uitgangssituatie ==\n'
python - <<'PY'
from scripts.scrapers.usf.core.db import db_connection

with db_connection() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            select
              count(*) as total_links,
              count(*) filter (where status = 'active') as active_links,
              count(*) filter (
                where nullif(trim(payload->>'price'), '') is not null
              ) as links_with_listing_price
            from public.shop_product_links
            where shop_id = 'everythingjazz'
            """
        )
        links = cursor.fetchone()

        cursor.execute(
            """
            select count(*)
            from public.prices p
            join public.shops s on s.id = p.shop_id
            where s.domain = 'eustore.everythingjazz.com'
            """
        )
        prices = cursor.fetchone()[0]

        cursor.execute(
            """
            select count(*)
            from public.shop_shipping_rules
            where shop_slug = 'everythingjazz'
            """
        )
        shipping = cursor.fetchone()[0]

print(
    {
        "registry_total": int(links[0]),
        "registry_active": int(links[1]),
        "registry_with_listing_price": int(links[2]),
        "public_price_rows": int(prices),
        "shipping_rows": int(shipping),
    }
)
PY
RC=$?
[[ "$RC" -eq 0 ]] || fail "database-uitgangssituatie kon niet worden gelezen"

printf '\n== 5. Volledige Everything Jazz listingwrite ==\n'
printf 'Voortgang gaat naar: %s\n' "$LISTING_LOG"

python -m "$LISTING_MODULE" \
  --start-page 1 \
  --max-pages 40 \
  --page-size 50 \
  --max-products 2000 \
  --max-runtime-seconds 900 \
  --expected-min-links 1200 \
  --output "$SUMMARY" \
  --write \
  >"$LISTING_LOG" 2>&1
LISTING_RC=$?

printf '\n== Listingkernresultaten ==\n'
grep -E \
  "EVERYTHINGJAZZ-LISTING-SUMMARY|EVERYTHINGJAZZ-LISTING-REGISTRY|'sync':|EVERYTHINGJAZZ-LISTING-MISSING|missing-link-delisting|Traceback|ERROR|FOUT|GUARD" \
  "$LISTING_LOG" | tail -n 50 || true

if [[ "$LISTING_RC" -ne 0 ]]; then
  printf '\n[ERROR] listingwrite faalde met exitcode %s\n' "$LISTING_RC" >&2
  printf 'Laatste 40 logregels:\n' >&2
  tail -n 40 "$LISTING_LOG" >&2
  printf '\nShipping is niet geïmporteerd.\n' >&2
  exit "$LISTING_RC"
fi

printf '\n== 6. Valideer write-samenvatting ==\n'
python - "$SUMMARY" <<'PY'
from __future__ import annotations

import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
if not path.exists():
    raise SystemExit(f"[ERROR] summary ontbreekt: {path}")

summary = json.loads(path.read_text(encoding="utf-8"))

checks = {
    "write": summary.get("write") is True,
    "scan_completed_safely": (
        summary.get("scan_completed_safely") is True
    ),
    "full_scan": summary.get("full_scan") is True,
    "safe_stop_reason": summary.get("stop_reason") in {
        "empty_page",
        "partial_last_page",
    },
    "minimum_links": int(summary.get("links") or 0) >= 1200,
    "minimum_offers": int(summary.get("offers") or 0) >= 1200,
    "missing_price_zero": int(summary.get("missing_price") or 0) == 0,
    "price_coverage": float(summary.get("price_coverage") or 0) >= 0.99,
}

print({"summary": summary, "checks": checks})

if not all(checks.values()):
    raise SystemExit("[ERROR] write-samenvatting voldoet niet aan alle guards")
PY
RC=$?
if [[ "$RC" -ne 0 ]]; then
  printf '\n[ERROR] Listingprocess eindigde zonder geldige volledige-writebevestiging.\n' >&2
  printf 'Er kunnen al registrywrites zijn gedaan; shipping is niet geïmporteerd.\n' >&2
  exit "$RC"
fi

printf '\n== 7. Valideer geregistreerde listingdata ==\n'
python - <<'PY'
from scripts.scrapers.usf.core.db import db_connection

with db_connection() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            select
              count(*) as total_links,
              count(*) filter (where status = 'active') as active_links,
              count(*) filter (
                where nullif(trim(payload->>'price'), '') is not null
              ) as links_with_listing_price,
              count(*) filter (
                where payload->>'availability' = 'in_stock'
              ) as in_stock,
              count(*) filter (
                where payload->>'availability' = 'preorder'
              ) as preorder,
              count(*) filter (
                where payload->>'availability' = 'out_of_stock'
              ) as out_of_stock
            from public.shop_product_links
            where shop_id = 'everythingjazz'
            """
        )
        row = cursor.fetchone()

result = {
    "total_links": int(row[0]),
    "active_links": int(row[1]),
    "links_with_listing_price": int(row[2]),
    "in_stock": int(row[3]),
    "preorder": int(row[4]),
    "out_of_stock": int(row[5]),
}
print(result)

if result["total_links"] < 1200:
    raise SystemExit("[ERROR] te weinig Everything Jazz-links in registry")
if result["links_with_listing_price"] < 1200:
    raise SystemExit("[ERROR] te weinig listingprijzen in registrypayload")
PY
RC=$?
if [[ "$RC" -ne 0 ]]; then
  printf '\n[ERROR] Registryvalidatie faalde; shipping wordt niet geïmporteerd.\n' >&2
  exit "$RC"
fi

printf '\n== 8. Importeer uitsluitend Everything Jazz-shipping ==\n'

IMPORT_COMMAND=()
if python "$SHIPPING_IMPORTER" --help >/dev/null 2>&1; then
  IMPORT_COMMAND=(python "$SHIPPING_IMPORTER")
elif python -m scripts.tools.import_shipping_rules --help >/dev/null 2>&1; then
  IMPORT_COMMAND=(python -m scripts.tools.import_shipping_rules)
else
  fail "shippingimporter kan niet worden gestart"
fi

"${IMPORT_COMMAND[@]}" --input "$SHIPPING_TMP" \
  >"$SHIPPING_LOG" 2>&1
SHIPPING_RC=$?

cat "$SHIPPING_LOG"

if [[ "$SHIPPING_RC" -ne 0 ]]; then
  printf '\n[ERROR] Listingwrite is gelukt, maar shippingimport faalde.\n' >&2
  printf 'Shippinglog: %s\n' "$SHIPPING_LOG" >&2
  exit "$SHIPPING_RC"
fi

printf '\n== 9. Valideer shipping in Supabase ==\n'
python - <<'PY'
from scripts.scrapers.usf.core.db import db_connection

with db_connection() as connection:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            select
              count(*) as total_rows,
              count(*) filter (where active) as active_rows,
              count(*) filter (
                where active
                  and country_code = 'NL'
                  and currency = 'EUR'
                  and shipping_cost_cents = 995
                  and free_shipping_threshold_cents is null
                  and shipping_logic = 'flat'
              ) as exact_active_rows
            from public.shop_shipping_rules
            where shop_slug = 'everythingjazz'
            """
        )
        row = cursor.fetchone()

result = {
    "total_rows": int(row[0]),
    "active_rows": int(row[1]),
    "exact_active_rows": int(row[2]),
}
print(result)

if result["active_rows"] != 1 or result["exact_active_rows"] != 1:
    raise SystemExit(
        "[ERROR] Everything Jazz-shipping is niet exact één actieve "
        "NL flat-rate-regel van 995 cent zonder gratis grens"
    )
PY
RC=$?
[[ "$RC" -eq 0 ]] || fail "shippingvalidatie in Supabase faalde"

printf '\n== 10. Eindstatus ==\n'
git status --short

printf '\nKLAAR\n'
printf 'Listingregistry: geschreven en gecontroleerd.\n'
printf 'Shipping: € 9,95 per bestelling; geen gratis-verzenddrempel.\n'
printf 'Detail, stage en promote: nog niet uitgevoerd.\n'
printf 'Listinglog: %s\n' "$LISTING_LOG"
printf 'Shippinglog: %s\n' "$SHIPPING_LOG"
printf 'Summary: %s\n' "$SUMMARY"
printf '\nLet op: vóór de detail-EAN-flow kunnen publieke prijsregels nog unmatched zijn. Dat is volgens de EAN-gatekeeper.\n'
exit 0
