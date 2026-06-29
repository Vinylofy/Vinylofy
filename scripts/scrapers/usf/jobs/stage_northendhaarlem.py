from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.db import load_env
from scripts.scrapers.usf.core.staging import stage_latest_raw_snapshots

SHOP_ID = "northendhaarlem"


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage latest North End Haarlem raw USF snapshots.")
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    load_env()
    result = stage_latest_raw_snapshots(shop_id=SHOP_ID, limit=args.limit, write=args.write)
    print("[NORTHEND-STAGE]", vars(result), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
