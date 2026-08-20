#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from cover_common import (
    CandidateRecord,
    CoverPipelineError,
    DEFAULT_MAX_OFFERS_PER_PRODUCT,
    OfferSource,
    connect_db,
    ensure_runtime_directories,
    fetch_page_candidates,
    get_table_columns,
    log,
    make_session,
    normalize_candidate_url,
    normalize_ean,
    normalize_source_type,
    normalize_text,
    rank_candidate,
    require_table_columns,
    safe_parse_datetime,
    serialize_json,
    utc_now,
    BLOCKED_COVER_ERROR_CODE,
    BLOCKED_COVER_ERROR_MESSAGE,
    is_blocked_cover_url,
)


@dataclass(slots=True)
class ProductSelection:
    product_id: str
    ean: str
    cover_priority: int
    cover_status: str
    cover_storage_path: str
    cover_source_url: str
    cover_needs_refresh: bool
    offers: list[OfferSource] = field(default_factory=list)


@dataclass(slots=True)
class WriteMetrics:
    inserted: int = 0
    updated: int = 0
    queued: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh cover candidates from existing offers or a candidate CSV."
        )
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=75,
        help="Maximum aantal producten in deze keysetbatch.",
    )
    parser.add_argument(
        "--max-offers-per-product",
        type=int,
        default=DEFAULT_MAX_OFFERS_PER_PRODUCT,
        help="Maximum aantal unieke shop-URL's per product.",
    )
    parser.add_argument(
        "--include-covered",
        action="store_true",
        help=(
            "Neem uitsluitend lokale covers mee die expliciet "
            "cover_needs_refresh=true hebben."
        ),
    )
    parser.add_argument(
        "--checkpoint",
        type=str,
        default="",
        help=(
            "Laatste products.id uit de vorige batch. Selectie gebruikt "
            "uitsluitend p.id > checkpoint; OFFSET wordt niet gebruikt."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Selecteer en rapporteer alleen. Geen netwerkrequests, "
            "databasewrites, claims of queuewijzigingen."
        ),
    )
    parser.add_argument(
        "--csv",
        type=str,
        default="",
        help=(
            "Optioneel CSV-pad met ean/product_id, shop_domain, "
            "product_url, image_url en source_type."
        ),
    )
    parser.add_argument(
        "--output-json",
        type=str,
        default="output/cover_pipeline/candidate_refresh_summary.json",
        help="Pad voor de JSON-runsamenvatting.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.limit <= 0:
        raise CoverPipelineError("--limit moet groter zijn dan nul.")
    if args.max_offers_per_product <= 0:
        raise CoverPipelineError(
            "--max-offers-per-product moet groter zijn dan nul."
        )
    if args.checkpoint:
        try:
            UUID(args.checkpoint)
        except ValueError as exc:
            raise CoverPipelineError(
                "--checkpoint moet een geldige UUID zijn."
            ) from exc
    if args.csv and args.checkpoint:
        raise CoverPipelineError(
            "--checkpoint kan niet samen met --csv worden gebruikt."
        )


def normalize_http_url(
    base_url: str,
    candidate_url: str | None,
) -> str | None:
    normalized = normalize_candidate_url(base_url, candidate_url)
    if not normalized:
        return None
    parts = urlsplit(normalized)
    if parts.scheme.lower() not in {"http", "https"} or not parts.netloc:
        return None
    return normalized


def load_product_batch(
    conn,
    *,
    limit: int,
    include_covered: bool,
    max_offers_per_product: int,
    checkpoint: str,
) -> list[ProductSelection]:
    products_columns = require_table_columns(
        conn,
        "products",
        [
            "id",
            "ean",
            "cover_status",
            "cover_storage_path",
            "cover_source_url",
            "cover_url",
            "cover_priority",
            "cover_needs_refresh",
        ],
    )
    prices_columns = require_table_columns(
        conn,
        "prices",
        ["product_id", "shop_id", "product_url"],
    )
    require_table_columns(conn, "shops", ["id", "domain"])
    require_table_columns(
        conn,
        "product_cover_queue",
        ["product_id", "status"],
    )

    del products_columns
    last_seen_expression = (
        "pr.last_seen_at"
        if "last_seen_at" in prices_columns
        else "null"
    )
    is_active_condition = (
        "coalesce(pr.is_active, true) = true"
        if "is_active" in prices_columns
        else "true"
    )
    checkpoint_value = checkpoint or None

    with conn.cursor() as cur:
        cur.execute(
            """
            select
                p.id,
                p.ean,
                coalesce(p.cover_priority, 0) as cover_priority,
                p.cover_status,
                coalesce(p.cover_storage_path, '') as cover_storage_path,
                coalesce(
                    nullif(btrim(p.cover_source_url), ''),
                    case
                        when p.cover_url ~* '^https?://'
                            then p.cover_url
                        else null
                    end
                ) as cover_source_url,
                coalesce(p.cover_needs_refresh, false)
                    as cover_needs_refresh
            from public.products p
            left join public.product_cover_queue q
              on q.product_id = p.id
            where public.normalize_cover_ean(p.ean) is not null
              and (%s::uuid is null or p.id > %s::uuid)
              and p.cover_status <> 'blocked'
              and (
                    nullif(btrim(p.cover_storage_path), '') is null
                    or (
                        %s
                        and coalesce(p.cover_needs_refresh, false)
                    )
              )
              and (q.status is null or q.status <> 'processing')
            order by p.id
            limit %s
            """,
            (
                checkpoint_value,
                checkpoint_value,
                include_covered,
                limit,
            ),
        )
        product_rows = cur.fetchall()

        products = [
            ProductSelection(
                product_id=str(row[0]),
                ean=normalize_ean(row[1]) or "",
                cover_priority=int(row[2] or 0),
                cover_status=normalize_text(row[3]),
                cover_storage_path=normalize_text(row[4]),
                cover_source_url=normalize_text(row[5]),
                cover_needs_refresh=bool(row[6]),
            )
            for row in product_rows
        ]
        if not products:
            return []

        product_ids = [product.product_id for product in products]
        cur.execute(
            f"""
            with ranked_offers as (
                select
                    pr.product_id,
                    pr.shop_id,
                    s.domain,
                    s.name,
                    pr.product_url,
                    {last_seen_expression} as last_seen_at,
                    row_number() over (
                        partition by pr.product_id
                        order by
                            {last_seen_expression} desc nulls last,
                            s.domain,
                            pr.product_url
                    ) as offer_rank
                from public.prices pr
                join public.shops s
                  on s.id = pr.shop_id
                where pr.product_id = any(%s::uuid[])
                  and {is_active_condition}
                  and coalesce(nullif(pr.product_url, ''), '') <> ''
            )
            select
                product_id,
                shop_id,
                domain,
                name,
                product_url,
                last_seen_at,
                offer_rank
            from ranked_offers
            where offer_rank <= %s
            order by product_id, offer_rank
            """,
            (product_ids, max_offers_per_product),
        )
        offer_rows = cur.fetchall()

    products_by_id = {
        product.product_id: product
        for product in products
    }
    seen_offer_urls: dict[str, set[str]] = defaultdict(set)
    for row in offer_rows:
        product_id = str(row[0])
        product = products_by_id.get(product_id)
        if product is None:
            continue

        product_url = normalize_http_url(
            normalize_text(row[4]),
            normalize_text(row[4]),
        )
        if not product_url:
            continue
        if product_url in seen_offer_urls[product_id]:
            continue
        seen_offer_urls[product_id].add(product_url)

        product.offers.append(
            OfferSource(
                product_id=product.product_id,
                ean=product.ean,
                shop_id=str(row[1]) if row[1] is not None else None,
                shop_domain=normalize_text(row[2]),
                shop_name=normalize_text(row[3]) or None,
                product_url=product_url,
                cover_priority=product.cover_priority,
                offer_rank=len(product.offers) + 1,
                last_seen_at=safe_parse_datetime(row[5]),
            )
        )
    return products


def load_product_states(
    conn,
    product_ids: list[str],
) -> dict[str, ProductSelection]:
    if not product_ids:
        return {}

    require_table_columns(
        conn,
        "products",
        [
            "id",
            "ean",
            "cover_status",
            "cover_storage_path",
            "cover_source_url",
            "cover_url",
            "cover_priority",
            "cover_needs_refresh",
        ],
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                id,
                ean,
                coalesce(cover_priority, 0),
                cover_status,
                coalesce(cover_storage_path, ''),
                coalesce(
                    nullif(btrim(cover_source_url), ''),
                    case
                        when cover_url ~* '^https?://' then cover_url
                        else null
                    end
                ),
                coalesce(cover_needs_refresh, false)
            from public.products
            where id = any(%s::uuid[])
            """,
            (product_ids,),
        )
        rows = cur.fetchall()

    return {
        str(row[0]): ProductSelection(
            product_id=str(row[0]),
            ean=normalize_ean(row[1]) or "",
            cover_priority=int(row[2] or 0),
            cover_status=normalize_text(row[3]),
            cover_storage_path=normalize_text(row[4]),
            cover_source_url=normalize_text(row[5]),
            cover_needs_refresh=bool(row[6]),
        )
        for row in rows
    }


def product_is_eligible(
    product: ProductSelection,
    *,
    include_covered: bool,
) -> tuple[bool, str]:
    if product.cover_status == "blocked":
        return False, "blocked"
    if product.cover_storage_path:
        if include_covered and product.cover_needs_refresh:
            return True, "repair"
        return False, "local_cover"
    return True, "missing_local_cover"


def candidate_from_product_source(
    product: ProductSelection,
) -> CandidateRecord | None:
    image_url = normalize_http_url(
        product.cover_source_url,
        product.cover_source_url,
    )
    if not image_url:
        return None

    product_url = (
        product.offers[0].product_url
        if product.offers
        else image_url
    )
    candidate = CandidateRecord(
        product_id=product.product_id,
        ean=product.ean,
        shop_id=None,
        shop_domain=urlsplit(image_url).hostname or "",
        shop_name=None,
        product_url=product_url,
        image_url=image_url,
        source_type="meta",
        source_rank=0,
        is_primary=True,
        mime_type=None,
        width=None,
        height=None,
    )
    candidate.source_rank = rank_candidate(candidate)
    return candidate


def candidate_records_from_discovered(
    offer: OfferSource,
    discovered: list[dict[str, Any]],
) -> list[CandidateRecord]:
    candidates: list[CandidateRecord] = []
    seen_urls: set[str] = set()

    for item in discovered:
        image_url = normalize_http_url(
            offer.product_url,
            item.get("image_url"),
        )
        source_type = normalize_source_type(
            normalize_text(item.get("source_type"))
        )

        # Arbitrary page <img> elements are insufficient evidence
        # for automatic cover publication.
        if source_type == "img_tag":
            continue

        if not image_url or image_url in seen_urls:
            continue
        seen_urls.add(image_url)

        candidate = CandidateRecord(
            product_id=offer.product_id,
            ean=offer.ean,
            shop_id=offer.shop_id,
            shop_domain=offer.shop_domain,
            shop_name=offer.shop_name,
            product_url=offer.product_url,
            image_url=image_url,
            source_type=source_type,
            source_rank=0,
            is_primary=bool(item.get("is_primary")),
            mime_type=None,
            width=item.get("width"),
            height=item.get("height"),
        )
        candidate.source_rank = rank_candidate(
            candidate,
            recency_reference=offer.last_seen_at,
        )
        candidates.append(candidate)

    return candidates


def discover_candidates_for_offer(
    offer: OfferSource,
    session,
) -> tuple[list[CandidateRecord], int]:
    discovered, http_status, _ = fetch_page_candidates(
        session,
        offer.product_url,
    )
    return (
        candidate_records_from_discovered(offer, discovered),
        http_status,
    )


def deduplicate_candidates(
    candidates: list[CandidateRecord],
) -> tuple[list[CandidateRecord], int]:
    by_key: dict[tuple[str, str], CandidateRecord] = {}
    duplicate_count = 0

    for candidate in candidates:
        image_url = normalize_http_url(
            candidate.product_url or candidate.image_url,
            candidate.image_url,
        )
        if not image_url:
            continue
        candidate.image_url = image_url
        candidate.source_type = normalize_source_type(
            candidate.source_type
        )
        key = (candidate.product_id, image_url)
        existing = by_key.get(key)
        if existing is None:
            by_key[key] = candidate
            continue

        duplicate_count += 1
        if candidate.source_rank > existing.source_rank:
            candidate.is_primary = (
                candidate.is_primary or existing.is_primary
            )
            by_key[key] = candidate
        else:
            existing.is_primary = (
                existing.is_primary or candidate.is_primary
            )

    result = sorted(
        by_key.values(),
        key=lambda candidate: (
            candidate.product_id,
            -candidate.source_rank,
            candidate.image_url,
        ),
    )
    return result, duplicate_count


def get_candidate_table_profile(
    conn,
) -> tuple[set[str], list[str]]:
    columns = get_table_columns(conn, "product_cover_candidates")
    url_columns = [
        column
        for column in ("image_url", "source_url", "candidate_url")
        if column in columns
    ]
    if not url_columns:
        raise CoverPipelineError(
            "Tabel public.product_cover_candidates mist "
            "image_url/source_url/candidate_url."
        )
    return columns, url_columns


def build_candidate_insert_payload(
    candidate: CandidateRecord,
    columns: set[str],
) -> dict[str, Any]:
    now = utc_now()
    blocked = is_blocked_cover_url(candidate.image_url)
    values = {
        "product_id": candidate.product_id,
        "shop_id": candidate.shop_id,
        "ean": candidate.ean,
        "product_url": candidate.product_url,
        "image_url": candidate.image_url,
        "source_url": candidate.image_url,
        "candidate_url": candidate.image_url,
        "source_type": normalize_source_type(candidate.source_type),
        "source_rank": max(0, int(candidate.source_rank or 0)),
        "is_primary": candidate.is_primary,
        "mime_type": candidate.mime_type,
        "width": candidate.width,
        "height": candidate.height,
        "candidate_status": "rejected" if blocked else "pending",
        "is_selected": False,
        "last_error_code": (
            BLOCKED_COVER_ERROR_CODE if blocked else None
        ),
        "last_error_message": (
            BLOCKED_COVER_ERROR_MESSAGE if blocked else None
        ),
        "discovered_at": now,
        "first_seen_at": now,
        "last_seen_at": now,
        "created_at": now,
        "updated_at": now,
    }
    return {
        column: value
        for column, value in values.items()
        if column in columns
    }


def find_existing_candidate(
    cur,
    *,
    product_id: str,
    image_url: str,
    url_columns: list[str],
    columns: set[str],
) -> dict[str, Any] | None:
    select_columns = ["id"]
    for column in (
        "source_rank",
        "is_primary",
        "mime_type",
        "width",
        "height",
        "candidate_status",
    ):
        if column in columns:
            select_columns.append(column)

    predicates = " or ".join(
        f"{column} = %s"
        for column in url_columns
    )
    cur.execute(
        f"""
        select {", ".join(select_columns)}
        from public.product_cover_candidates
        where product_id = %s
          and ({predicates})
        order by
            updated_at desc nulls last,
            created_at desc nulls last,
            id
        limit 1
        """,
        [product_id, *([image_url] * len(url_columns))],
    )
    row = cur.fetchone()
    if row is None:
        return None
    return dict(zip(select_columns, row))


def insert_candidate_row(
    cur,
    payload: dict[str, Any],
) -> None:
    columns = list(payload)
    cur.execute(
        f"""
        insert into public.product_cover_candidates
            ({", ".join(columns)})
        values ({", ".join(["%s"] * len(columns))})
        """,
        [payload[column] for column in columns],
    )


def update_candidate_row(
    cur,
    *,
    row_id: Any,
    candidate: CandidateRecord,
    existing: dict[str, Any],
    columns: set[str],
) -> None:
    now = utc_now()
    payload: dict[str, Any] = {}

    for column, value in (
        ("shop_id", candidate.shop_id),
        ("ean", candidate.ean),
        ("product_url", candidate.product_url),
    ):
        if column in columns and value:
            payload[column] = value

    for column in ("image_url", "source_url", "candidate_url"):
        if column in columns:
            payload[column] = candidate.image_url

    if "source_type" in columns:
        payload["source_type"] = normalize_source_type(
            candidate.source_type
        )
    if "source_rank" in columns:
        payload["source_rank"] = max(
            int(existing.get("source_rank") or 0),
            int(candidate.source_rank or 0),
        )
    if "is_primary" in columns:
        payload["is_primary"] = (
            bool(existing.get("is_primary"))
            or bool(candidate.is_primary)
        )
    if (
        "mime_type" in columns
        and candidate.mime_type
        and not existing.get("mime_type")
    ):
        payload["mime_type"] = candidate.mime_type
    if "width" in columns:
        payload["width"] = (
            existing.get("width") or candidate.width
        )
    if "height" in columns:
        payload["height"] = (
            existing.get("height") or candidate.height
        )
    if "candidate_status" in columns:
        current_status = normalize_text(
            existing.get("candidate_status")
        )
        payload["candidate_status"] = (
            "rejected"
            if is_blocked_cover_url(candidate.image_url)
            else current_status
            if current_status in {"published", "rejected"}
            else "pending"
        )
    if is_blocked_cover_url(candidate.image_url):
        if "is_selected" in columns:
            payload["is_selected"] = False
        if "last_error_code" in columns:
            payload["last_error_code"] = BLOCKED_COVER_ERROR_CODE
        if "last_error_message" in columns:
            payload["last_error_message"] = BLOCKED_COVER_ERROR_MESSAGE
    if "last_seen_at" in columns:
        payload["last_seen_at"] = now
    if "updated_at" in columns:
        payload["updated_at"] = now

    if not payload:
        return
    assignments = ", ".join(
        f"{column} = %s"
        for column in payload
    )
    cur.execute(
        f"""
        update public.product_cover_candidates
        set {assignments}
        where id = %s
        """,
        [*payload.values(), row_id],
    )


def upsert_candidate_rows(
    conn,
    candidates: list[CandidateRecord],
) -> WriteMetrics:
    metrics = WriteMetrics()
    if not candidates:
        return metrics

    columns, url_columns = get_candidate_table_profile(conn)
    with conn.cursor() as cur:
        for candidate in candidates:
            existing = find_existing_candidate(
                cur,
                product_id=candidate.product_id,
                image_url=candidate.image_url,
                url_columns=url_columns,
                columns=columns,
            )
            if existing is None:
                insert_candidate_row(
                    cur,
                    build_candidate_insert_payload(
                        candidate,
                        columns,
                    ),
                )
                metrics.inserted += 1
            else:
                update_candidate_row(
                    cur,
                    row_id=existing["id"],
                    candidate=candidate,
                    existing=existing,
                    columns=columns,
                )
                metrics.updated += 1
    return metrics


def queue_products(
    conn,
    candidates_by_product: dict[str, list[CandidateRecord]],
    priorities: dict[str, int],
) -> int:
    product_ids = sorted(
        product_id
        for product_id, candidates in candidates_by_product.items()
        if candidates
    )
    if not product_ids:
        return 0

    groups: dict[int, list[str]] = defaultdict(list)
    for product_id in product_ids:
        groups[max(0, int(priorities.get(product_id, 0)))].append(
            product_id
        )

    queued = 0
    with conn.cursor() as cur:
        for priority_bump, grouped_ids in sorted(groups.items()):
            cur.execute(
                """
                select public.queue_cover_for_products(
                    %s::uuid[],
                    'candidate_refresh',
                    %s,
                    'cover_candidate_refresh'
                )
                """,
                (grouped_ids, priority_bump),
            )
            row = cur.fetchone()
            queued += int(row[0] or 0) if row else 0

        for product_id in product_ids:
            cur.execute(
                """
                update public.product_cover_queue
                set
                    candidate_count = %s,
                    updated_at = now()
                where product_id = %s
                """,
                (
                    len(candidates_by_product[product_id]),
                    product_id,
                ),
            )
    return queued


def load_candidates_from_csv(
    path: Path,
) -> tuple[
    dict[str, list[CandidateRecord]],
    dict[str, list[OfferSource]],
]:
    if not path.exists():
        raise CoverPipelineError(f"CSV niet gevonden: {path}")

    candidates_by_key: dict[str, list[CandidateRecord]] = defaultdict(list)
    offers_by_key: dict[str, list[OfferSource]] = defaultdict(list)
    seen_candidate_urls: dict[str, set[str]] = defaultdict(set)
    seen_offer_urls: dict[str, set[str]] = defaultdict(set)

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader, start=2):
            product_id = normalize_text(row.get("product_id"))
            ean = normalize_ean(row.get("ean")) or ""
            if not product_id and not ean:
                raise CoverPipelineError(
                    f"CSV regel {row_number} mist product_id en EAN."
                )

            key = product_id or ean
            product_url = normalize_http_url(
                normalize_text(row.get("product_url")),
                normalize_text(row.get("product_url")),
            )
            image_url = normalize_http_url(
                product_url or normalize_text(row.get("image_url")),
                normalize_text(row.get("image_url")),
            )
            if not product_url or not image_url:
                raise CoverPipelineError(
                    f"CSV regel {row_number} bevat geen geldige "
                    "HTTP(S) product_url en image_url."
                )
            if image_url in seen_candidate_urls[key]:
                continue
            seen_candidate_urls[key].add(image_url)

            candidate = CandidateRecord(
                product_id=product_id or key,
                ean=ean,
                shop_id=normalize_text(row.get("shop_id")) or None,
                shop_domain=normalize_text(row.get("shop_domain")),
                shop_name=normalize_text(row.get("shop_name")) or None,
                product_url=product_url,
                image_url=image_url,
                source_type=normalize_source_type(
                    normalize_text(row.get("source_type"))
                    or "listing"
                ),
                source_rank=int(row.get("source_rank") or 0),
                is_primary=(
                    str(row.get("is_primary") or "").lower()
                    in {"1", "true", "yes", "y"}
                ),
                mime_type=normalize_text(row.get("mime_type")) or None,
                width=(
                    int(row["width"])
                    if str(row.get("width") or "").isdigit()
                    else None
                ),
                height=(
                    int(row["height"])
                    if str(row.get("height") or "").isdigit()
                    else None
                ),
            )
            if candidate.source_rank <= 0:
                candidate.source_rank = rank_candidate(candidate)
            candidates_by_key[key].append(candidate)

            if product_url not in seen_offer_urls[key]:
                seen_offer_urls[key].add(product_url)
                offers_by_key[key].append(
                    OfferSource(
                        product_id=product_id or key,
                        ean=ean,
                        shop_id=candidate.shop_id,
                        shop_domain=candidate.shop_domain,
                        shop_name=candidate.shop_name,
                        product_url=product_url,
                        cover_priority=0,
                        offer_rank=len(offers_by_key[key]) + 1,
                        last_seen_at=None,
                    )
                )

    return candidates_by_key, offers_by_key


def resolve_missing_product_ids(
    conn,
    candidates_by_key: dict[str, list[CandidateRecord]],
    offers_by_key: dict[str, list[OfferSource]],
) -> tuple[
    dict[str, list[CandidateRecord]],
    dict[str, list[OfferSource]],
]:
    unresolved_eans = [
        key
        for key, candidates in candidates_by_key.items()
        if normalize_ean(key)
        and all(candidate.product_id == key for candidate in candidates)
    ]
    if not unresolved_eans:
        return candidates_by_key, offers_by_key

    with conn.cursor() as cur:
        cur.execute(
            """
            select id, public.normalize_cover_ean(ean)
            from public.products
            where public.normalize_cover_ean(ean) = any(%s)
            """,
            (unresolved_eans,),
        )
        mapping = {
            normalize_text(row[1]): str(row[0])
            for row in cur.fetchall()
        }

    missing = [
        ean
        for ean in unresolved_eans
        if ean not in mapping
    ]
    if missing:
        raise CoverPipelineError(
            "CSV bevat onbekende EANs: "
            + ", ".join(missing[:10])
        )

    resolved_candidates: dict[str, list[CandidateRecord]] = defaultdict(list)
    resolved_offers: dict[str, list[OfferSource]] = defaultdict(list)
    for key, candidates in candidates_by_key.items():
        product_id = mapping.get(key, key)
        for candidate in candidates:
            candidate.product_id = product_id
            resolved_candidates[product_id].append(candidate)
    for key, offers in offers_by_key.items():
        product_id = mapping.get(key, key)
        for offer in offers:
            offer.product_id = product_id
            resolved_offers[product_id].append(offer)
    return resolved_candidates, resolved_offers


def filter_csv_products(
    conn,
    candidates_by_product: dict[str, list[CandidateRecord]],
    offers_by_product: dict[str, list[OfferSource]],
    *,
    include_covered: bool,
) -> tuple[
    dict[str, list[CandidateRecord]],
    dict[str, list[OfferSource]],
    dict[str, int],
]:
    states = load_product_states(
        conn,
        sorted(candidates_by_product),
    )
    filtered_candidates: dict[str, list[CandidateRecord]] = {}
    filtered_offers: dict[str, list[OfferSource]] = {}
    skipped = {
        "unknown_product": 0,
        "blocked": 0,
        "local_cover": 0,
    }

    for product_id, candidates in candidates_by_product.items():
        product = states.get(product_id)
        if product is None:
            skipped["unknown_product"] += 1
            continue
        eligible, reason = product_is_eligible(
            product,
            include_covered=include_covered,
        )
        if not eligible:
            skipped[reason] += 1
            continue

        unique, _ = deduplicate_candidates(candidates)
        filtered_candidates[product_id] = unique
        filtered_offers[product_id] = offers_by_product.get(
            product_id,
            [],
        )
    return filtered_candidates, filtered_offers, skipped


def write_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        serialize_json(summary) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    args = parse_args()
    validate_args(args)
    output_path = Path(args.output_json)
    ensure_runtime_directories()

    summary: dict[str, Any] = {
        "started_at": utc_now().isoformat(),
        "mode": "csv" if args.csv else "db",
        "dry_run": bool(args.dry_run),
        "limit": args.limit,
        "max_offers_per_product": args.max_offers_per_product,
        "include_covered_repairs": bool(args.include_covered),
        "checkpoint_start": args.checkpoint or None,
        "checkpoint_end": args.checkpoint or None,
        "has_more": False,
        "metrics": {
            "products_selected": 0,
            "products_eligible": 0,
            "products_skipped_blocked": 0,
            "products_skipped_local_cover": 0,
            "products_skipped_unknown": 0,
            "source_metadata_candidates": 0,
            "offer_pages_planned": 0,
            "offer_pages_fetched": 0,
            "offer_urls_deduplicated": 0,
            "candidate_urls_deduplicated": 0,
            "candidates_discovered": 0,
            "candidate_rows_inserted": 0,
            "candidate_rows_updated": 0,
            "queue_rows_touched": 0,
        },
        "errors": [],
        "products": [],
    }

    conn = connect_db()
    conn.autocommit = False
    try:
        require_table_columns(
            conn,
            "product_cover_candidates",
            [
                "product_id",
                "source_type",
                "source_rank",
                "candidate_status",
            ],
        )
        require_table_columns(
            conn,
            "product_cover_queue",
            [
                "product_id",
                "status",
                "priority",
                "candidate_count",
            ],
        )

        candidates_by_product: dict[str, list[CandidateRecord]]
        offers_by_product: dict[str, list[OfferSource]]
        priorities: dict[str, int] = {}

        if args.csv:
            raw_candidates, raw_offers = load_candidates_from_csv(
                Path(args.csv)
            )
            raw_candidates, raw_offers = resolve_missing_product_ids(
                conn,
                raw_candidates,
                raw_offers,
            )
            (
                candidates_by_product,
                offers_by_product,
                skipped,
            ) = filter_csv_products(
                conn,
                raw_candidates,
                raw_offers,
                include_covered=args.include_covered,
            )
            summary["metrics"]["products_selected"] = len(
                raw_candidates
            )
            summary["metrics"]["products_eligible"] = len(
                candidates_by_product
            )
            summary["metrics"]["products_skipped_blocked"] = skipped[
                "blocked"
            ]
            summary["metrics"]["products_skipped_local_cover"] = skipped[
                "local_cover"
            ]
            summary["metrics"]["products_skipped_unknown"] = skipped[
                "unknown_product"
            ]
        else:
            products = load_product_batch(
                conn,
                limit=args.limit,
                include_covered=args.include_covered,
                max_offers_per_product=args.max_offers_per_product,
                checkpoint=args.checkpoint,
            )
            summary["metrics"]["products_selected"] = len(products)
            summary["metrics"]["products_eligible"] = len(products)
            summary["has_more"] = len(products) == args.limit
            if products:
                summary["checkpoint_end"] = products[-1].product_id

            candidates_by_product = defaultdict(list)
            offers_by_product = {
                product.product_id: product.offers
                for product in products
            }
            priorities = {
                product.product_id: product.cover_priority
                for product in products
            }

            for product in products:
                source_candidate = candidate_from_product_source(
                    product
                )
                if source_candidate:
                    candidates_by_product[
                        product.product_id
                    ].append(source_candidate)
                    summary["metrics"][
                        "source_metadata_candidates"
                    ] += 1

                summary["metrics"]["offer_pages_planned"] += len(
                    product.offers
                )

            if not args.dry_run:
                session = make_session()
                page_cache: dict[
                    str,
                    list[dict[str, Any]],
                ] = {}
                for product in products:
                    product_errors: list[str] = []
                    for offer in product.offers:
                        try:
                            raw_discovered = page_cache.get(
                                offer.product_url
                            )
                            if raw_discovered is None:
                                raw_discovered, _, _ = (
                                    fetch_page_candidates(
                                        session,
                                        offer.product_url,
                                    )
                                )
                                page_cache[
                                    offer.product_url
                                ] = raw_discovered
                                summary["metrics"][
                                    "offer_pages_fetched"
                                ] += 1
                            else:
                                summary["metrics"][
                                    "offer_urls_deduplicated"
                                ] += 1

                            discovered = (
                                candidate_records_from_discovered(
                                    offer,
                                    raw_discovered,
                                )
                            )
                            candidates_by_product[
                                product.product_id
                            ].extend(discovered)
                        except Exception as exc:
                            product_errors.append(
                                f"{offer.shop_domain}: {exc}"
                            )
                    if product_errors:
                        summary["errors"].append(
                            {
                                "product_id": product.product_id,
                                "errors": product_errors,
                            }
                        )

        deduplicated_by_product: dict[
            str,
            list[CandidateRecord],
        ] = {}
        for product_id, candidates in candidates_by_product.items():
            unique, duplicate_count = deduplicate_candidates(
                candidates
            )
            deduplicated_by_product[product_id] = unique
            summary["metrics"][
                "candidate_urls_deduplicated"
            ] += duplicate_count
            summary["metrics"]["candidates_discovered"] += len(
                unique
            )
            summary["products"].append(
                {
                    "product_id": product_id,
                    "candidate_count": len(unique),
                    "offer_count": len(
                        offers_by_product.get(product_id, [])
                    ),
                    "top_candidates": [
                        asdict(candidate)
                        for candidate in unique[:3]
                    ],
                }
            )

        if args.dry_run:
            conn.rollback()
        else:
            write_metrics = upsert_candidate_rows(
                conn,
                [
                    candidate
                    for candidates in deduplicated_by_product.values()
                    for candidate in candidates
                ],
            )
            summary["metrics"]["candidate_rows_inserted"] = (
                write_metrics.inserted
            )
            summary["metrics"]["candidate_rows_updated"] = (
                write_metrics.updated
            )
            summary["metrics"]["queue_rows_touched"] = queue_products(
                conn,
                deduplicated_by_product,
                priorities,
            )
            conn.commit()

    except Exception as exc:
        conn.rollback()
        summary["failed_at"] = utc_now().isoformat()
        summary["fatal_error"] = str(exc)
        write_summary(output_path, summary)
        raise
    finally:
        conn.close()

    summary["finished_at"] = utc_now().isoformat()
    write_summary(output_path, summary)
    log(
        "[DONE] candidate refresh | "
        f"dry_run={args.dry_run} | "
        f"producten={summary['metrics']['products_eligible']} | "
        f"candidates={summary['metrics']['candidates_discovered']} | "
        f"checkpoint={summary['checkpoint_end']}"
    )


if __name__ == "__main__":
    main()
