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

TARGET="scripts/scrapers/usf/jobs/refresh_everythingjazz_listing_prices.py"
[[ -f "$TARGET" ]] || fail "bestand ontbreekt: $TARGET"

printf '== Repository ==\n'
printf 'root:   %s\n' "$ROOT"
printf 'branch: %s\n' "$(git branch --show-current)"
printf '\n== Status vóór patch ==\n'
git status --short

BACKUP="/tmp/refresh_everythingjazz_listing_prices.py.before-full-scan-fix-$(date +%Y%m%d-%H%M%S)"
cp -a "$TARGET" "$BACKUP"
printf '\nBackup: %s\n' "$BACKUP"

python - "$TARGET" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
text = path.read_text(encoding="utf-8")

old = "    full_scan = args.max_pages == 0 and scan_completed_safely\n"
new = (
    "    full_scan = (\n"
    "        args.start_page == 1\n"
    "        and scan_completed_safely\n"
    '        and stop_reason in {"empty_page", "partial_last_page"}\n'
    "    )\n"
)

count = text.count(old)
if count != 1:
    raise SystemExit(
        f"[PATCH-ERROR] verwachtte exact één oude full_scan-regel, vond {count}"
    )

text = text.replace(old, new, 1)
path.write_text(text, encoding="utf-8")
print("[PATCH] full_scan baseert zich nu op bereik en veilige eindconditie")
PY

printf '\n== Compilecheck ==\n'
python -m py_compile "$TARGET"

printf '\n== CLI-check ==\n'
python -m scripts.scrapers.usf.jobs.refresh_everythingjazz_listing_prices \
  --help >/dev/null
printf '[OK] module en CLI\n'

printf '\n== Logische contractcheck ==\n'
python - <<'PY'
def full_scan(start_page: int, safe: bool, stop_reason: str) -> bool:
    return (
        start_page == 1
        and safe
        and stop_reason in {"empty_page", "partial_last_page"}
    )

cases = {
    "volledig_met_cap": full_scan(1, True, "partial_last_page"),
    "volledig_met_lege_pagina": full_scan(1, True, "empty_page"),
    "begrensde_test": full_scan(1, False, "max_pages_limit"),
    "start_middenin_catalogus": full_scan(31, True, "partial_last_page"),
    "guardstop": full_scan(1, False, "max_products_guard"),
}

expected = {
    "volledig_met_cap": True,
    "volledig_met_lege_pagina": True,
    "begrensde_test": False,
    "start_middenin_catalogus": False,
    "guardstop": False,
}

print(cases)
if cases != expected:
    raise SystemExit(f"[ERROR] full_scan-contract wijkt af: {cases!r}")
PY

printf '\n== Diffcontrole ==\n'
git diff --check -- "$TARGET"
git diff -- "$TARGET"

printf '\n== Eindstatus ==\n'
git status --short
printf '\nKLAAR — alleen full_scan-logica aangepast; geen crawl, databasewrite, commit of push.\n'
