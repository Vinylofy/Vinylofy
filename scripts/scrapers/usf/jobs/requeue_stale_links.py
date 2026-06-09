#!/usr/bin/env python3
from __future__ import annotations

import argparse

from scripts.scrapers.usf.core.requeue import requeue_stale_links


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Plaats stale, reeds verwerkte shoplinks opnieuw in de "
            "USF-queue, zonder de ingestelde doelqueue te overschrijden."
        )
    )
    parser.add_argument("--shop-id", required=True)
    parser.add_argument(
        "--stale-hours",
        type=float,
        default=24.0,
        help="Minimale ouderdom van last_detail_scraped_at.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Maximaal aantal links dat per run wordt gerequeued.",
    )
    parser.add_argument(
        "--target-queue",
        type=int,
        default=500,
        help=(
            "Vul de actieve queue maximaal aan tot dit aantal. "
            "Bij een grotere bestaande backlog wordt niets gerequeued."
        ),
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Voer de requeue werkelijk uit. Standaard is dry-run.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()

    result = requeue_stale_links(
        shop_id=args.shop_id,
        stale_hours=args.stale_hours,
        limit=args.limit,
        target_queue=args.target_queue,
        write=args.write,
    )

    print(
        "[REQUEUE]",
        {
            "shop": result.shop_id,
            "current_queue": result.current_queue,
            "eligible": result.eligible,
            "target_queue": result.target_queue,
            "requested_limit": result.requested_limit,
            "planned": result.planned,
            "requeued": result.requeued,
            "write": args.write,
        },
        flush=True,
    )

    for link_id in result.link_ids[:10]:
        print("[REQUEUE-LINK]", link_id, flush=True)

    if not args.write:
        print(
            "[REQUEUE] dry-run complete; geen databasewrites.",
            flush=True,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
