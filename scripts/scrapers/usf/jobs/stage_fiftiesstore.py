from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.db import load_env
from scripts.scrapers.usf.core.staging import stage_latest_raw_snapshots

SHOP_ID = "fiftiesstore"

def main() -> int:
    ap = argparse.ArgumentParser(description="Stage FiftiesStore/Bennies raw snapshots.")
    ap.add_argument("--limit", type=int, default=6000)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    load_env()
    result = stage_latest_raw_snapshots(
        shop_id=SHOP_ID,
        limit=args.limit,
        write=args.write,
    )
    print("[FIFTIES-STAGE]", vars(result), flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
