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

PIPELINE="scripts/scrapers/usf/jobs/run_everythingjazz_pipeline.py"
LISTING_WORKFLOW=".github/workflows/usf-everythingjazz-listing.yml"
DETAIL_WORKFLOW=".github/workflows/usf-everythingjazz-detail.yml"

for path in "$PIPELINE" "$LISTING_WORKFLOW" "$DETAIL_WORKFLOW"; do
  [[ -f "$path" ]] || fail "verwacht bestand ontbreekt: $path"
done

printf '== Repository ==\n'
printf 'root:   %s\n' "$ROOT"
printf 'origin: %s\n' "$REMOTE"
printf 'branch: %s\n' "$(git branch --show-current)"
printf '\n== Status vóór herstel ==\n'
git status --short

BACKUP_DIR="/tmp/vinylofy-everythingjazz-module-fix-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$BACKUP_DIR"
cp -a "$PIPELINE" "$LISTING_WORKFLOW" "$DETAIL_WORKFLOW" "$BACKUP_DIR/"

python - "$PIPELINE" "$LISTING_WORKFLOW" "$DETAIL_WORKFLOW" <<'PY'
from __future__ import annotations

import re
import sys
from pathlib import Path

pipeline_path, listing_workflow_path, detail_workflow_path = map(Path, sys.argv[1:])

pipeline = pipeline_path.read_text(encoding="utf-8")

old_root_block = (
    'ROOT = Path(__file__).resolve().parents[4]\n'
    'JOBS = ROOT / "scripts" / "scrapers" / "usf" / "jobs"\n'
)
new_root_block = (
    'ROOT = Path(__file__).resolve().parents[4]\n'
    'MODULE_PREFIX = "scripts.scrapers.usf.jobs"\n'
)

if pipeline.count(old_root_block) != 1:
    raise SystemExit(
        "[PATCH-ERROR] verwachtte exact één oude ROOT/JOBS-definitie in pipeline"
    )
pipeline = pipeline.replace(old_root_block, new_root_block, 1)

pattern = re.compile(
    r'(?m)^(?P<indent>[ \t]*)str\(JOBS / "(?P<name>'
    r'refresh_everythingjazz_listing_prices|detail_everythingjazz|'
    r'stage_everythingjazz|promote_everythingjazz|quarantine_everythingjazz'
    r')\.py"\),$'
)
matches = list(pattern.finditer(pipeline))
if len(matches) != 5:
    raise SystemExit(
        f"[PATCH-ERROR] verwachtte 5 directe child-scriptcalls, vond {len(matches)}"
    )

def module_call(match: re.Match[str]) -> str:
    indent = match.group("indent")
    name = match.group("name")
    return (
        f'{indent}"-m",\n'
        f'{indent}f"{{MODULE_PREFIX}}.{name}",'
    )

pipeline = pattern.sub(module_call, pipeline)

if "str(JOBS /" in pipeline or "\nJOBS =" in pipeline:
    raise SystemExit("[PATCH-ERROR] directe JOBS-padcall bleef achter")

pipeline_path.write_text(pipeline, encoding="utf-8")


def patch_workflow(path: Path, expected_replacements: int) -> None:
    text = path.read_text(encoding="utf-8")
    workflow_pattern = re.compile(
        r"python scripts/scrapers/usf/jobs/"
        r"(refresh_everythingjazz_listing_prices|detail_everythingjazz|"
        r"stage_everythingjazz|promote_everythingjazz|"
        r"quarantine_everythingjazz|run_everythingjazz_pipeline)\.py"
    )
    found = len(workflow_pattern.findall(text))
    if found != expected_replacements:
        raise SystemExit(
            f"[PATCH-ERROR] {path}: verwachtte {expected_replacements} "
            f"directe Python-calls, vond {found}"
        )
    text = workflow_pattern.sub(
        lambda match: (
            "python -m scripts.scrapers.usf.jobs." + match.group(1)
        ),
        text,
    )
    if "python scripts/scrapers/usf/jobs/" in text:
        raise SystemExit(f"[PATCH-ERROR] directe workflow-call bleef achter in {path}")
    path.write_text(text, encoding="utf-8")


patch_workflow(listing_workflow_path, expected_replacements=3)
patch_workflow(detail_workflow_path, expected_replacements=5)

print("[PATCH] pipeline-childcalls omgezet naar python -m")
print("[PATCH] listingworkflow omgezet naar python -m")
print("[PATCH] detailworkflow omgezet naar python -m")
PY

printf '\n== Python compilechecks ==\n'
python -m py_compile \
  scripts/scrapers/usf/jobs/refresh_everythingjazz_listing_prices.py \
  scripts/scrapers/usf/jobs/detail_everythingjazz.py \
  scripts/scrapers/usf/jobs/stage_everythingjazz.py \
  scripts/scrapers/usf/jobs/promote_everythingjazz.py \
  scripts/scrapers/usf/jobs/quarantine_everythingjazz.py \
  scripts/scrapers/usf/jobs/run_everythingjazz_pipeline.py

printf '\n== Module-import- en CLI-checks ==\n'
MODULES=(
  scripts.scrapers.usf.jobs.refresh_everythingjazz_listing_prices
  scripts.scrapers.usf.jobs.detail_everythingjazz
  scripts.scrapers.usf.jobs.stage_everythingjazz
  scripts.scrapers.usf.jobs.promote_everythingjazz
  scripts.scrapers.usf.jobs.quarantine_everythingjazz
  scripts.scrapers.usf.jobs.run_everythingjazz_pipeline
)
for module in "${MODULES[@]}"; do
  python -m "$module" --help >/dev/null
  printf '[OK] python -m %s --help\n' "$module"
done

printf '\n== Shippingregelcontrole ==\n'
python - <<'PY'
from __future__ import annotations

import csv
from pathlib import Path

path = Path("data/shipping/vinylofy_shipping_rules_nl.csv")
if not path.exists():
    raise SystemExit(f"[ERROR] shippingbestand ontbreekt: {path}")

with path.open(newline="", encoding="utf-8-sig") as handle:
    rows = [
        row
        for row in csv.DictReader(handle)
        if (row.get("shop_slug") or "").strip() == "everythingjazz"
    ]

if len(rows) != 1:
    raise SystemExit(
        f"[ERROR] verwacht exact één Everything Jazz-shippingregel, vond {len(rows)}"
    )

row = rows[0]
checks = {
    "shipping_cost_cents": row.get("shipping_cost_cents") == "995",
    "free_shipping_threshold_cents": not (
        row.get("free_shipping_threshold_cents") or ""
    ).strip(),
    "shipping_logic": (row.get("shipping_logic") or "").strip() == "flat",
    "active": (row.get("active") or "").strip().lower() == "true",
}
if not all(checks.values()):
    raise SystemExit(
        f"[ERROR] shippingregel wijkt af: checks={checks}, row={row}"
    )

print(
    {
        "shop_slug": row.get("shop_slug"),
        "shipping_cost_cents": row.get("shipping_cost_cents"),
        "free_shipping_threshold_cents": row.get(
            "free_shipping_threshold_cents"
        ),
        "shipping_logic": row.get("shipping_logic"),
        "active": row.get("active"),
    }
)
PY

printf '\n== Diffcheck vóór dry-run ==\n'
git diff --check
git diff --stat -- \
  "$PIPELINE" \
  "$LISTING_WORKFLOW" \
  "$DETAIL_WORKFLOW"

printf '\n== Everything Jazz listing: twee pagina’s, dry-run ==\n'
LOG="/tmp/everythingjazz-listing-dry-run-$(date +%Y%m%d-%H%M%S).log"
set +e
python -m scripts.scrapers.usf.jobs.run_everythingjazz_pipeline \
  --mode listing \
  --listing-max-pages 2 \
  --listing-page-size 250 \
  --debug 2>&1 | tee "$LOG"
DRY_RUN_RC=${PIPESTATUS[0]}
set -e

printf '\n== Eindcontrole ==\n'
git diff --check
git status --short
printf '\nBackup: %s\n' "$BACKUP_DIR"
printf 'Dry-runlog: %s\n' "$LOG"
printf 'Dry-run exitcode: %s\n' "$DRY_RUN_RC"

if [[ "$DRY_RUN_RC" -ne 0 ]]; then
  printf '\n[ERROR] De module-importfout is hersteld, maar de live dry-run vond een volgende fout.\n' >&2
  exit "$DRY_RUN_RC"
fi

printf '\nKLAAR — Everything Jazz modulecalls en workflows zijn hersteld; niets gecommit of gepusht.\n'
