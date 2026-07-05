from __future__ import annotations

import argparse
from scripts.scrapers.usf.core.db import db_connection
from scripts.scrapers.usf.core.fast_listing_price_sync import bulk_update_prices_from_link_registry

SHOP_ID = "myrecordstore"
SHOP_DOMAIN = "myrecordstore.nl"

def main() -> int:
    parser = argparse.ArgumentParser(description="Fast sync My Record Store listing prices to existing live prices.")
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    with db_connection() as conn:
        stats = bulk_update_prices_from_link_registry(
            conn,
            shop_registry_id=SHOP_ID,
            shop_domain=SHOP_DOMAIN,
            write=args.write,
            currency="EUR",
        )

    print("[MYRECORDSTORE-FAST-SYNC]", vars(stats), flush=True)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
