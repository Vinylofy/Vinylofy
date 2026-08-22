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
PIPELINE="scripts/scrapers/usf/jobs/run_everythingjazz_pipeline.py"
WORKFLOW=".github/workflows/usf-everythingjazz-listing.yml"

for path in "$LISTING" "$PIPELINE" "$WORKFLOW"; do
  [[ -f "$path" ]] || fail "verwacht bestand ontbreekt: $path"
done

printf '== Repository ==\n'
printf 'root:   %s\n' "$ROOT"
printf 'origin: %s\n' "$REMOTE"
printf 'branch: %s\n' "$(git branch --show-current)"
printf '\n== Status vóór patch ==\n'
git status --short

BACKUP_DIR="/tmp/vinylofy-everythingjazz-safe-listing-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP_DIR"
cp -a "$LISTING" "$PIPELINE" "$WORKFLOW" "$BACKUP_DIR/"
printf '\nBackup: %s\n' "$BACKUP_DIR"

python - "$LISTING" "$PIPELINE" "$WORKFLOW" <<'PY'
from __future__ import annotations

import sys
from pathlib import Path

listing_path, pipeline_path, workflow_path = map(Path, sys.argv[1:])


def replace_once(text: str, old: str, new: str, *, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"[PATCH-ERROR] {label}: verwachtte exact 1 match, vond {count}"
        )
    return text.replace(old, new, 1)


listing = listing_path.read_text(encoding="utf-8")

# Idempotency guard.
if "SAFE_DEFAULT_PAGE_SIZE = 50" in listing:
    raise SystemExit("[PATCH-ERROR] veilige listingpatch lijkt al toegepast")

listing = replace_once(
    listing,
    "import argparse\nimport json\n",
    "import argparse\nimport gc\nimport json\n",
    label="gc-import",
)

listing = replace_once(
    listing,
    'ALLOWED_PRODUCT_TYPES = {"vinyl", "vinyl-box"}\n',
    'ALLOWED_PRODUCT_TYPES = {"vinyl", "vinyl-box"}\n'
    "SAFE_DEFAULT_PAGE_SIZE = 50\n"
    "SAFE_MAX_PAGE_SIZE = 100\n"
    "DEFAULT_MAX_PRODUCTS = 2500\n"
    "DEFAULT_MAX_RUNTIME_SECONDS = 900\n",
    label="veiligheidsconstanten",
)

fetch_old = '''def fetch_page(
    session: requests.Session,
    *,
    page: int,
    limit: int,
    timeout: int,
) -> list[dict[str, Any]]:
    url = listing_url_for_page(page, limit)
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(f"collection endpoint gaf geen geldige JSON: {url}") from exc
    products = payload.get("products") if isinstance(payload, dict) else None
    if not isinstance(products, list):
        raise RuntimeError(f"collection endpoint mist products-lijst: {url}")
    return [item for item in products if isinstance(item, dict)]
'''

fetch_new = '''def compact_product(product: dict[str, Any]) -> dict[str, Any]:
    """Bewaar uitsluitend velden die de listingparser daadwerkelijk gebruikt."""
    variants: list[dict[str, Any]] = []
    raw_variants = product.get("variants")
    if isinstance(raw_variants, list):
        for variant in raw_variants:
            if not isinstance(variant, dict):
                continue
            variants.append(
                {
                    "price": variant.get("price"),
                    "compare_at_price": variant.get("compare_at_price"),
                    "available": variant.get("available") is True,
                }
            )

    return {
        "id": product.get("id"),
        "handle": product.get("handle"),
        "title": product.get("title"),
        "vendor": product.get("vendor"),
        "product_type": product.get("product_type"),
        "tags": product.get("tags"),
        "variants": variants,
    }


def fetch_page(
    session: requests.Session,
    *,
    page: int,
    limit: int,
    timeout: int,
) -> list[dict[str, Any]]:
    url = listing_url_for_page(page, limit)
    with session.get(url, timeout=timeout) as response:
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"collection endpoint gaf geen geldige JSON: {url}"
            ) from exc

    raw_products = payload.get("products") if isinstance(payload, dict) else None
    if not isinstance(raw_products, list):
        raise RuntimeError(f"collection endpoint mist products-lijst: {url}")

    products = [
        compact_product(item)
        for item in raw_products
        if isinstance(item, dict)
    ]
    del raw_products
    del payload
    gc.collect()
    return products
'''

listing = replace_once(
    listing,
    fetch_old,
    fetch_new,
    label="compacte fetch_page",
)

listing = replace_once(
    listing,
    '    parser.add_argument("--page-size", type=int, default=250)\n',
    '    parser.add_argument(\n'
    '        "--page-size",\n'
    '        type=int,\n'
    '        default=SAFE_DEFAULT_PAGE_SIZE,\n'
    '        help=(\n'
    '            "Shopify-producten per request; hard begrensd op "\n'
    '            f"{SAFE_MAX_PAGE_SIZE} om Codespaces en Actions te beschermen."\n'
    '        ),\n'
    '    )\n',
    label="veilige page-size default",
)

listing = replace_once(
    listing,
    '    parser.add_argument("--max-page-failures", type=int, default=3)\n',
    '    parser.add_argument("--max-page-failures", type=int, default=3)\n'
    '    parser.add_argument(\n'
    '        "--max-products",\n'
    '        type=int,\n'
    '        default=DEFAULT_MAX_PRODUCTS,\n'
    '        help="Harde guard op het totale aantal ruwe producten per run.",\n'
    '    )\n'
    '    parser.add_argument(\n'
    '        "--max-runtime-seconds",\n'
    '        type=int,\n'
    '        default=DEFAULT_MAX_RUNTIME_SECONDS,\n'
    '        help="Harde runtimeguard; bij overschrijding worden geen writes gedaan.",\n'
    '    )\n'
    '    parser.add_argument(\n'
    '        "--debug-products-per-page",\n'
    '        type=int,\n'
    '        default=3,\n'
    '        help="Maximaal aantal productregels per pagina bij --debug.",\n'
    '    )\n',
    label="scanlimiet-argumenten",
)

listing = replace_once(
    listing,
    '    if not 1 <= args.page_size <= 250:\n'
    '        raise SystemExit("[ERROR] --page-size moet tussen 1 en 250 liggen.")\n'
    '    if args.sleep < 0 or args.timeout < 1 or args.max_page_failures < 1:\n'
    '        raise SystemExit("[ERROR] ongeldige timing- of failure-instelling.")\n',
    '    if not 1 <= args.page_size <= SAFE_MAX_PAGE_SIZE:\n'
    '        raise SystemExit(\n'
    '            "[ERROR] --page-size moet tussen 1 en "\n'
    '            f"{SAFE_MAX_PAGE_SIZE} liggen."\n'
    '        )\n'
    '    if args.sleep < 0 or args.timeout < 1 or args.max_page_failures < 1:\n'
    '        raise SystemExit("[ERROR] ongeldige timing- of failure-instelling.")\n'
    '    if args.max_products < 1 or args.max_runtime_seconds < 1:\n'
    '        raise SystemExit("[ERROR] scan- en runtimeguards moeten positief zijn.")\n'
    '    if not 0 <= args.debug_products_per_page <= 20:\n'
    '        raise SystemExit(\n'
    '            "[ERROR] --debug-products-per-page moet tussen 0 en 20 liggen."\n'
    '        )\n',
    label="argumentvalidatie",
)

listing = replace_once(
    listing,
    '    missing_price = 0\n\n'
    '    while args.max_pages == 0 or pages_fetched < args.max_pages:\n',
    '    missing_price = 0\n'
    '    stop_reason = "not_started"\n\n'
    '    while args.max_pages == 0 or pages_fetched < args.max_pages:\n'
    '        elapsed_seconds = (\n'
    '            datetime.now(timezone.utc) - started_at\n'
    '        ).total_seconds()\n'
    '        if elapsed_seconds >= args.max_runtime_seconds:\n'
    '            stop_reason = "max_runtime_guard"\n'
    '            print(\n'
    '                "[EVERYTHINGJAZZ-LISTING][GUARD]",\n'
    '                {\n'
    '                    "reason": stop_reason,\n'
    '                    "elapsed_seconds": round(elapsed_seconds, 2),\n'
    '                    "max_runtime_seconds": args.max_runtime_seconds,\n'
    '                },\n'
    '                flush=True,\n'
    '            )\n'
    '            break\n',
    label="runtimeguard",
)

listing = replace_once(
    listing,
    '            if consecutive_failures >= args.max_page_failures:\n'
    '                break\n',
    '            if consecutive_failures >= args.max_page_failures:\n'
    '                stop_reason = "max_page_failures"\n'
    '                break\n',
    label="failure-stopreden",
)

listing = replace_once(
    listing,
    '        if not products:\n'
    '            scan_completed_safely = True\n'
    '            print(\n',
    '        if not products:\n'
    '            scan_completed_safely = True\n'
    '            stop_reason = "empty_page"\n'
    '            print(\n',
    label="empty-stopreden",
)

listing = replace_once(
    listing,
    '        if signature in signatures:\n'
    '            scan_completed_safely = True\n'
    '            print(\n'
    '                "[EVERYTHINGJAZZ-LISTING] herhaalde JSON-pagina; scan gestopt",\n',
    '        if signature in signatures:\n'
    '            scan_completed_safely = False\n'
    '            stop_reason = "repeated_page_guard"\n'
    '            print(\n'
    '                "[EVERYTHINGJAZZ-LISTING][GUARD] herhaalde JSON-pagina; scan gestopt",\n',
    label="repeatguard",
)

listing = replace_once(
    listing,
    '        signatures.add(signature)\n\n'
    '        page_links = 0\n',
    '        signatures.add(signature)\n\n'
    '        raw_page_count = len(products)\n'
    '        if raw_products_seen + raw_page_count > args.max_products:\n'
    '            stop_reason = "max_products_guard"\n'
    '            print(\n'
    '                "[EVERYTHINGJAZZ-LISTING][GUARD]",\n'
    '                {\n'
    '                    "reason": stop_reason,\n'
    '                    "raw_products_seen": raw_products_seen,\n'
    '                    "next_page_products": raw_page_count,\n'
    '                    "max_products": args.max_products,\n'
    '                },\n'
    '                flush=True,\n'
    '            )\n'
    '            del products\n'
    '            gc.collect()\n'
    '            break\n'
    '        final_partial_page = raw_page_count < args.page_size\n\n'
    '        page_links = 0\n',
    label="productguard",
)

listing = replace_once(
    listing,
    '            if args.debug and page_links <= 20:\n',
    '            if (\n'
    '                args.debug\n'
    '                and page_links <= args.debug_products_per_page\n'
    '            ):\n',
    label="compacte debuglimiet",
)

listing = replace_once(
    listing,
    '                "raw_products": len(products),\n',
    '                "raw_products": raw_page_count,\n',
    label="page-count zonder productsreferentie",
)

listing = replace_once(
    listing,
    '        page += 1\n'
    '        if args.max_pages and pages_fetched >= args.max_pages:\n'
    '            scan_completed_safely = False\n'
    '            break\n'
    '        time.sleep(args.sleep)\n\n'
    '    full_scan = args.max_pages == 0 and scan_completed_safely\n',
    '        del products\n'
    '        gc.collect()\n\n'
    '        if final_partial_page:\n'
    '            scan_completed_safely = True\n'
    '            stop_reason = "partial_last_page"\n'
    '            break\n\n'
    '        page += 1\n'
    '        if args.max_pages and pages_fetched >= args.max_pages:\n'
    '            scan_completed_safely = False\n'
    '            stop_reason = "max_pages_limit"\n'
    '            break\n'
    '        time.sleep(args.sleep)\n\n'
    '    session.close()\n'
    '    gc.collect()\n'
    '    full_scan = args.max_pages == 0 and scan_completed_safely\n',
    label="pagevrijgave en veilige eindpagina",
)

listing = replace_once(
    listing,
    '        "pages_fetched": pages_fetched,\n'
    '        "raw_products_seen": raw_products_seen,\n',
    '        "pages_fetched": pages_fetched,\n'
    '        "page_size": args.page_size,\n'
    '        "max_products": args.max_products,\n'
    '        "max_runtime_seconds": args.max_runtime_seconds,\n'
    '        "stop_reason": stop_reason,\n'
    '        "elapsed_seconds": round(\n'
    '            (datetime.now(timezone.utc) - started_at).total_seconds(), 2\n'
    '        ),\n'
    '        "raw_products_seen": raw_products_seen,\n',
    label="summaryguards",
)

listing_path.write_text(listing, encoding="utf-8")

pipeline = pipeline_path.read_text(encoding="utf-8")
if 'parser.add_argument("--listing-page-size", type=int, default=250)' not in pipeline:
    raise SystemExit(
        "[PATCH-ERROR] pipeline page-size default niet exact aangetroffen"
    )
pipeline = pipeline.replace(
    'parser.add_argument("--listing-page-size", type=int, default=250)',
    'parser.add_argument("--listing-page-size", type=int, default=50)',
    1,
)

pipeline = replace_once(
    pipeline,
    '    parser.add_argument("--listing-sleep", type=float, default=0.25)\n',
    '    parser.add_argument("--listing-sleep", type=float, default=0.25)\n'
    '    parser.add_argument("--listing-max-products", type=int, default=2500)\n'
    '    parser.add_argument(\n'
    '        "--listing-max-runtime-seconds", type=int, default=900\n'
    '    )\n',
    label="pipelineguard-argumenten",
)

pipeline = replace_once(
    pipeline,
    '            "--sleep",\n'
    '            str(args.listing_sleep),\n'
    '        ]\n',
    '            "--sleep",\n'
    '            str(args.listing_sleep),\n'
    '            "--max-products",\n'
    '            str(args.listing_max_products),\n'
    '            "--max-runtime-seconds",\n'
    '            str(args.listing_max_runtime_seconds),\n'
    '        ]\n',
    label="pipelineguard-doorvoer",
)

pipeline_path.write_text(pipeline, encoding="utf-8")

workflow = workflow_path.read_text(encoding="utf-8")
workflow = replace_once(
    workflow,
    '        default: "250"\n',
    '        default: "50"\n',
    label="workflow page-size default",
)
workflow = replace_once(
    workflow,
    '        description: "Shopify collection JSON page size"\n',
    '        description: "Shopify page size; maximaal 100"\n',
    label="workflow page-size omschrijving",
)
workflow = replace_once(
    workflow,
    '        default: true\n        type: boolean\n',
    '        default: false\n        type: boolean\n',
    label="workflow debug default",
)
workflow_path.write_text(workflow, encoding="utf-8")

print("[PATCH] listingresponses worden direct teruggebracht tot noodzakelijke velden")
print("[PATCH] standaard page-size 250 -> 50; harde bovengrens 100")
print("[PATCH] productguard 2500 en runtimeguard 900 seconden toegevoegd")
print("[PATCH] debugoutput begrensd op standaard 3 producten per pagina")
print("[PATCH] pipeline en listingworkflow gebruiken veilige defaults")
PY

printf '\n== Compilechecks ==\n'
python -m py_compile "$LISTING" "$PIPELINE"

printf '\n== CLI-contractchecks ==\n'
python -m scripts.scrapers.usf.jobs.refresh_everythingjazz_listing_prices --help >/dev/null
python -m scripts.scrapers.usf.jobs.run_everythingjazz_pipeline --help >/dev/null
printf '[OK] module-imports en --help\n'

printf '\n== Statische veiligheidscontrole ==\n'
python - <<'PY'
from pathlib import Path

listing = Path(
    "scripts/scrapers/usf/jobs/refresh_everythingjazz_listing_prices.py"
).read_text(encoding="utf-8")
pipeline = Path(
    "scripts/scrapers/usf/jobs/run_everythingjazz_pipeline.py"
).read_text(encoding="utf-8")
workflow = Path(
    ".github/workflows/usf-everythingjazz-listing.yml"
).read_text(encoding="utf-8")

checks = {
    "compact_product": "def compact_product(" in listing,
    "safe_default_50": "SAFE_DEFAULT_PAGE_SIZE = 50" in listing,
    "hard_max_100": "SAFE_MAX_PAGE_SIZE = 100" in listing,
    "max_products_guard": 'stop_reason = "max_products_guard"' in listing,
    "runtime_guard": 'stop_reason = "max_runtime_guard"' in listing,
    "gc_release": listing.count("gc.collect()") >= 3,
    "pipeline_default_50": (
        'parser.add_argument("--listing-page-size", type=int, default=50)'
        in pipeline
    ),
    "workflow_default_50": 'default: "50"' in workflow,
    "workflow_debug_false": "default: false" in workflow,
}

print(checks)
if not all(checks.values()):
    raise SystemExit("[ERROR] niet alle veiligheidschecks zijn geslaagd")
PY

printf '\n== Gerichte wijzigingscontrole ==\n'
git diff --check -- "$LISTING" "$PIPELINE" "$WORKFLOW"
for path in "$LISTING" "$PIPELINE" "$WORKFLOW"; do
  before="$BACKUP_DIR/$(basename "$path")"
  if cmp -s "$before" "$path"; then
    fail "bestand is onverwacht ongewijzigd: $path"
  fi
  printf '[GEWIJZIGD] %s (%s regels)\n' "$path" "$(wc -l < "$path")"
done

printf '\n== Status na patch ==\n'
git status --short

printf '\nKLAAR — alleen code gepatcht; geen live crawl, databasewrite, commit of push uitgevoerd.\n'
