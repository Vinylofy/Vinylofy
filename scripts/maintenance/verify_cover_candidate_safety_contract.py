#!/usr/bin/env python3
from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[2]

checks = []

def check(name, ok):
    checks.append((name, bool(ok)))

common = (
    ROOT / "scripts/maintenance/cover_common.py"
).read_text(encoding="utf-8")

refresh = (
    ROOT / "scripts/maintenance/cover_candidate_refresh.py"
).read_text(encoding="utf-8")

worker = (
    ROOT / "scripts/maintenance/cover_worker.py"
).read_text(encoding="utf-8")

mb_worker = (
    ROOT / "scripts/maintenance/cover_mb_worker.py"
).read_text(encoding="utf-8")

constraint_verifier = (
    ROOT
    / "scripts/maintenance/"
      "verify_cover_candidate_status_constraint.py"
).read_text(encoding="utf-8")

migration = (
    ROOT
    / "supabase/migrations/"
      "20260813100000_allow_rejected_cover_candidates.sql"
).read_text(encoding="utf-8")

for source in (
    common,
    refresh,
    worker,
    mb_worker,
    constraint_verifier,
):
    ast.parse(source)

check(
    "extractor_has_no_arbitrary_img_scan",
    'soup.find_all("img")' not in common,
)

check(
    "refresh_rejects_img_tag",
    'if source_type == "img_tag":' in refresh,
)

check(
    "worker_excludes_legacy_img_tag",
    "not in ('img_tag', 'img', 'image')" in worker,
)

check(
    "common_has_rejected",
    '"rejected"' in common,
)

check(
    "common_has_no_accepted_status",
    '"accepted"' not in common,
)

check(
    "worker_has_no_accepted_status",
    '"accepted"' not in worker,
)

check(
    "mb_worker_has_no_accepted_status",
    "'accepted'" not in mb_worker,
)

check(
    "mb_worker_preserves_rejected",
    "('published', 'rejected')" in mb_worker,
)

check(
    "refresh_preserves_rejected",
    '{"published", "rejected"}' in refresh,
)

check(
    "constraint_verifier_expects_rejected",
    '"rejected"' in constraint_verifier,
)

check(
    "migration_allows_rejected",
    "'rejected'::text" in migration,
)

failed = [
    name
    for name, ok in checks
    if not ok
]

for name, ok in checks:
    print(f"{name}={'PASS' if ok else 'FAIL'}")

print(f"checks={len(checks)}")
print(f"failures={len(failed)}")

if failed:
    print("SAFETY_CONTRACT=FAIL")
else:
    print("SAFETY_CONTRACT=GREEN")
