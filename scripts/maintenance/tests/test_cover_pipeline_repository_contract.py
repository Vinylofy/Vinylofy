from __future__ import annotations

import ast
from pathlib import Path
import re
import unittest

ROOT = Path(__file__).resolve().parents[3]
MAINTENANCE = ROOT / "scripts" / "maintenance"
MIGRATION = ROOT / (
    "supabase/migrations/"
    "20260805123000_finalize_central_cover_pipeline.sql"
)
ROLLBACK = ROOT / (
    "supabase/rollbacks/"
    "20260805123000_finalize_central_cover_pipeline.rollback.sql"
)
CORRECTIVE_MIGRATION = ROOT / (
    "supabase/migrations/"
    "20260809082100_allow_external_cover_candidates.sql"
)
CORRECTIVE_ROLLBACK = ROOT / (
    "supabase/rollbacks/"
    "20260809082100_allow_external_cover_candidates.rollback.sql"
)

COMMON = MAINTENANCE / "cover_common.py"
REFRESH = MAINTENANCE / "cover_candidate_refresh.py"
WORKER = MAINTENANCE / "cover_worker.py"
MB_WORKER = MAINTENANCE / "cover_mb_worker.py"
COVER_URL = ROOT / "lib" / "cover-url.ts"
COVER_IMAGE = ROOT / "components" / "cover-image.tsx"
COVER_IMAGE_CLIENT = ROOT / "components" / "cover-image-client.tsx"
COVER_ROUTE = ROOT / "app" / "covers" / "[...path]" / "route.ts"
VINYLOFY_DATA = ROOT / "lib" / "vinylofy-data.ts"
SEARCH_RESULT_CARD = ROOT / "components" / "search" / "product-result-card.tsx"
PRODUCT_SUMMARY_CARD = ROOT / "components" / "product" / "product-summary-card.tsx"
HOME_NEW_RELEASES_GRID = ROOT / "components" / "home" / "new-releases-grid.tsx"
TOP_DEAL_CARD = ROOT / "components" / "topdeals" / "top-deal-card.tsx"
NEW_RELEASES_PAGE = ROOT / "app" / "nieuwe-releases" / "page.tsx"


def source(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def parsed(path: Path) -> ast.Module:
    return ast.parse(source(path), filename=str(path))


def call_names(path: Path) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(parsed(path)):
        if not isinstance(node, ast.Call):
            continue
        function = node.func
        if isinstance(function, ast.Name):
            names.add(function.id)
        elif isinstance(function, ast.Attribute):
            names.add(function.attr)
    return names


def sql_strings(path: Path) -> list[str]:
    values: list[str] = []
    for node in ast.walk(parsed(path)):
        if not (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
        ):
            continue
        lowered = node.value.lower()
        if any(
            token in lowered
            for token in (
                "select ",
                "insert ",
                "update ",
                "delete ",
            )
        ):
            values.append(node.value)
    return values


class RepositoryArchitectureContractTests(unittest.TestCase):
    def test_required_files_exist_and_parse(self) -> None:
        for path in (
            COMMON,
            REFRESH,
            WORKER,
            MB_WORKER,
            COVER_URL,
            COVER_IMAGE,
            COVER_IMAGE_CLIENT,
            COVER_ROUTE,
            VINYLOFY_DATA,
            SEARCH_RESULT_CARD,
            PRODUCT_SUMMARY_CARD,
            HOME_NEW_RELEASES_GRID,
            TOP_DEAL_CARD,
            NEW_RELEASES_PAGE,
            MIGRATION,
            ROLLBACK,
            CORRECTIVE_MIGRATION,
            CORRECTIVE_ROLLBACK,
        ):
            self.assertTrue(path.is_file(), str(path))
        for path in (COMMON, REFRESH, WORKER, MB_WORKER):
            parsed(path)

    def test_storage_upsert_has_only_helper_and_worker_callers(self) -> None:
        callers: list[str] = []
        for path in sorted(MAINTENANCE.glob("*.py")):
            if "upsert_bytes_to_storage" in call_names(path):
                callers.append(path.name)
        self.assertEqual(
            callers,
            ["cover_common.py", "cover_worker.py"],
        )
        executable_callers = [
            name for name in callers if name != "cover_common.py"
        ]
        self.assertEqual(executable_callers, ["cover_worker.py"])

    def test_candidate_refresh_is_keyset_candidate_only(self) -> None:
        text = source(REFRESH)
        self.assertIn("--dry-run", text)
        self.assertIn("--checkpoint", text)
        self.assertIn("p.id > %s::uuid", text)
        self.assertIn("p.cover_status <> 'blocked'", text)
        self.assertIn(
            "nullif(btrim(p.cover_storage_path), '') is null",
            text,
        )
        self.assertIn("cover_source_url", text)
        self.assertIn("public.queue_cover_for_products", text)
        self.assertNotIn("download_binary(", text)
        self.assertNotIn("upsert_bytes_to_storage(", text)
        self.assertNotIn(".storage.", text)
        for statement in sql_strings(REFRESH):
            self.assertIsNone(
                re.search(r"\boffset\b", statement, re.IGNORECASE)
            )

    def test_central_worker_enforces_claim_and_publication_contract(self) -> None:
        text = source(WORKER)
        for token in (
            "from public.claim_next_cover_job(%s)",
            "public.recover_stale_cover_claims",
            "for update of p skip locked",
            "source_reason = 'local_repair'",
            "preflight_local_object",
            "download_binary(",
            "prepare_image_for_storage(",
            "upsert_bytes_to_storage(",
            "compensate_storage_upload(receipt)",
            "cover_status = 'ready'",
            "candidate_status = 'published'",
            "is_selected = true",
            "--dry-run",
        ):
            self.assertIn(token, text)
        self.assertNotIn("fetch_page_candidates", text)
        self.assertNotIn("upload_bytes_to_storage", text)
        for statement in sql_strings(WORKER):
            self.assertIsNone(
                re.search(r"\boffset\b", statement, re.IGNORECASE)
            )

    def test_musicbrainz_route_is_metadata_only(self) -> None:
        text = source(MB_WORKER)
        self.assertIn("product_cover_candidates", text)
        self.assertIn("public.queue_cover_for_products", text)
        self.assertIn("set transaction read only", text)
        self.assertIn("for update of q skip locked", text)
        self.assertIn('SOURCE_TYPE = "meta"', text)
        for forbidden in (
            "storage_upload",
            "download_image",
            "requests.post",
            "SUPABASE_SERVICE_ROLE_KEY",
            "update public.products",
            "upsert_bytes_to_storage",
        ):
            self.assertNotIn(forbidden, text)

    def test_shared_helper_contract(self) -> None:
        text = source(COMMON)
        for token in (
            '"listing_img_src": "listing"',
            "respect_retry_after_header=True",
            "DEFAULT_MAX_IMAGE_BYTES = 20 * 1024 * 1024",
            "ImageOps.exif_transpose(image)",
            'return f"ean/{ean_clean[:3]}/{ean_clean}.webp"',
            (
                'return f"covers/releases/'
                '{release_clean.lower()}.webp"'
            ),
            "bucket_api.remove([receipt.remote_path])",
        ):
            self.assertIn(token, text)

    def test_storage_credentials_prefer_secret_key(self) -> None:
        text = source(COMMON)
        self.assertIn(
            'os.getenv("SUPABASE_SECRET_KEY") '
            'or os.getenv("SUPABASE_SERVICE_ROLE_KEY")',
            text,
        )

    def test_storage_upload_uses_supported_bytes_and_worker_imports_log(self) -> None:
        common_text = source(COMMON)

        self.assertIn(
            "file=prepared_image.output_bytes",
            common_text,
        )
        self.assertNotIn(
            "payload = io.BytesIO(prepared_image.output_bytes)",
            common_text,
        )
        self.assertNotIn(
            "file=payload",
            common_text,
        )

        worker_tree = parsed(WORKER)

        cover_common_imports = [
            node
            for node in worker_tree.body
            if isinstance(node, ast.ImportFrom)
            and node.module == "cover_common"
        ]

        self.assertEqual(
            len(cover_common_imports),
            1,
        )

        imported_names = {
            alias.name
            for alias in cover_common_imports[0].names
        }

        self.assertIn(
            "log",
            imported_names,
        )

    def test_published_cover_candidate_status_corrective_migration(self) -> None:
        root = Path(__file__).resolve().parents[3]

        migration = (
            root / 'supabase/migrations/20260809163500_allow_published_cover_candidates.sql'
        ).read_text(encoding="utf-8")

        rollback = (
            root / 'supabase/rollbacks/20260809163500_allow_published_cover_candidates.rollback.sql'
        ).read_text(encoding="utf-8")

        verifier = (
            root / 'scripts/maintenance/verify_cover_candidate_status_constraint.py'
        ).read_text(encoding="utf-8")

        for status in (
            "failed",
            "new",
            "pending",
            "published",
        ):
            self.assertIn(
                f"'{status}'::text",
                migration,
            )

        self.assertIn(
            "candidate_status is null",
            migration,
        )
        self.assertIn(
            "drop constraint "
            "product_cover_candidates_status_chk",
            migration,
        )
        self.assertIn(
            "add constraint "
            "product_cover_candidates_status_chk",
            migration,
        )

        self.assertIn(
            "where candidate_status = 'published'",
            rollback,
        )
        self.assertIn(
            "Refusing rollback",
            rollback,
        )

        self.assertIn(
            '"published"',
            verifier,
        )
        self.assertIn(
            "selected_status_mismatches",
            verifier,
        )

    def test_targeted_worker_isolated_from_global_queue(self) -> None:
        text = source(WORKER)
        tree = parsed(WORKER)

        functions = {
            node.name: node
            for node in tree.body
            if isinstance(node, ast.FunctionDef)
        }

        self.assertIn(
            "claim_one_job_for_product",
            functions,
        )

        target_text = ast.get_source_segment(
            text,
            functions["claim_one_job_for_product"],
        ) or ""

        for token in (
            "q.product_id = %s",
            "for update of q skip locked",
            "status = 'processing'",
            "cover_status = 'resolving'",
        ):
            self.assertIn(token, target_text)

        for forbidden in (
            "recover_stale_claims",
            "reconcile_local_products",
            "claim_next_cover_job",
        ):
            self.assertNotIn(
                forbidden,
                target_text,
            )

        main_text = ast.get_source_segment(
            text,
            functions["main"],
        ) or ""

        for token in (
            "--product-id",
            "if args.product_id:",
            "claim_one_job_for_product(",
            "elif args.dry_run:",
            "recover_stale_claims(",
            "reconcile_local_products(",
            "claim_one_job(",
        ):
            self.assertIn(token, text if token == "--product-id" else main_text)

    def test_migration_enforces_single_selection_and_claim_rpcs(self) -> None:
        text = source(MIGRATION).lower()
        for token in (
            "create or replace function public.claim_next_cover_job",
            "for update of q skip locked",
            "create or replace function public.queue_cover_for_products",
            "create or replace function public.recover_stale_cover_claims",
            "stale_claim_recovered",
            "cover_storage_path",
            "cover_source_url",
            "cover_needs_refresh",
            "is_selected",
        ):
            self.assertIn(token, text)

        for token in (
            "unique",
            "product_cover_candidates",
            "product_id",
            "where",
            "is_selected",
        ):
            self.assertIn(token, text)

    def test_external_cover_candidates_allow_null_shop_id(self) -> None:
        refresh_text = source(REFRESH)
        migration_text = source(CORRECTIVE_MIGRATION).lower()
        rollback_text = source(CORRECTIVE_ROLLBACK).lower()
        verifier_text = source(
            MAINTENANCE / "verify_cover_pipeline_migration.py"
        )

        self.assertIn(
            "shop_id=None",
            refresh_text,
        )
        self.assertIn(
            "alter column shop_id drop not null",
            migration_text,
        )
        self.assertIn(
            "alter column shop_id set not null",
            rollback_text,
        )
        self.assertIn(
            "where shop_id is null",
            rollback_text,
        )
        self.assertIn(
            "column:product_cover_candidates.shop_id_nullable",
            verifier_text,
        )

    def test_new_releases_use_central_cover_contract(self) -> None:
        data_text = source(VINYLOFY_DATA)
        page_text = source(NEW_RELEASES_PAGE)

        self.assertIn(
            'export const dynamic = "force-dynamic";',
            page_text,
        )

        type_start = data_text.index(
            "export type ReleaseCalendarItem = {"
        )
        type_end = data_text.index("\n};", type_start)
        type_block = data_text[type_start:type_end]

        row_start = data_text.index(
            "type ReleaseCalendarRow = {",
            type_end,
        )
        row_end = data_text.index("\n};", row_start)
        row_block = data_text[row_start:row_end]

        function_start = data_text.index(
            "export async function getReleaseCalendarItems("
        )
        function_block = data_text[function_start:]

        self.assertIn(
            "imageStoragePath: string | null;",
            type_block,
        )

        for forbidden in (
            "image_url:",
            "image_storage_path:",
            "image_status:",
        ):
            self.assertNotIn(forbidden, row_block)

        for token in (
            (
                "id, ean, artist, title, release_date, "
                "source_shop, source_url, format, label, product_id"
            ),
            "const productMap = new Map<string, ProductRow>();",
            "const batchProducts = await getProductsByIds(batchProductIds);",
            "productMap.set(product.id, product);",
            "if (!row.product_id) return false;",
            "if (freshShopCount < 2) return false;",
            "imageUrl: product?.cover_url ?? null",
            "imageStoragePath: product?.cover_storage_path ?? null",
        ):
            self.assertIn(token, function_block)

        for forbidden in (
            "releaseImageReady",
            "row.image_url",
            "row.image_storage_path",
            "row.image_status",
        ):
            self.assertNotIn(forbidden, function_block)

        for token in (
            'import { CoverImage } from "@/components/cover-image";',
            "<CoverImage",
            "src={release.imageUrl}",
            "storagePath={release.imageStoragePath}",
            'alt={`${release.artist} - ${displayTitle}`}',
            (
                'className="h-full w-full object-cover '
                'data-[cover-fallback=true]:object-contain"'
            ),
            'loading="lazy"',
        ):
            self.assertIn(token, page_text)

        for forbidden in (
            "<img",
            "release.imageUrl ?",
            "Geen afbeelding beschikbaar",
            "image_source_url",
        ):
            self.assertNotIn(forbidden, page_text)

    def test_release_discovery_keeps_images_as_source_metadata(self) -> None:
        root = Path(__file__).resolve().parents[3]

        files = (
            (
                root
                / "scripts"
                / "release_discovery"
                / "discover_bobsvinyl_releases.py"
            ),
            (
                root
                / "scripts"
                / "release_discovery"
                / "jobs"
                / "discover_bobsvinyl.py"
            ),
        )

        for discovery_file in files:
            discovery_text = discovery_file.read_text(
                encoding="utf-8"
            )

            self.assertIn(
                "image_source_url",
                discovery_text,
            )
            self.assertIn(
                "public.release_calendar.image_source_url",
                discovery_text,
            )
            self.assertNotIn(
                "image_url",
                discovery_text,
            )

    def test_release_discovery_handles_both_unique_identities(self) -> None:
        root = Path(__file__).resolve().parents[3]

        files = (
            (
                root
                / "scripts"
                / "release_discovery"
                / "discover_bobsvinyl_releases.py"
            ),
            (
                root
                / "scripts"
                / "release_discovery"
                / "jobs"
                / "discover_bobsvinyl.py"
            ),
        )

        for discovery_file in files:
            discovery_text = discovery_file.read_text(
                encoding="utf-8"
            )

            for token in (
                "with source_match as (",
                "select id, ean",
                "where source_url = %(source_url)s",
                "effective_identity as (",
                "(select ean from source_match)",
                "%(ean)s",
                "(select ean from effective_identity) is not null",
                "and ean = (select ean from effective_identity)",
                "and source_shop = %(source_shop)s",
                "and release_date = %(release_date)s::date",
                "existing_rows = cur.fetchall()",
                "if len(existing_rows) > 1:",
                "Release-identiteitsconflict:",
                'update_params["existing_id"]',
            ):
                self.assertIn(token, discovery_text)

            self.assertNotIn(
                "on conflict (ean, source_shop, release_date)",
                discovery_text,
            )
            self.assertNotIn(
                "on conflict (source_url)",
                discovery_text,
            )

    def test_top_deals_use_central_storage_path(self) -> None:
        data_text = source(VINYLOFY_DATA)
        card_text = source(TOP_DEAL_CARD)

        type_start = data_text.index("export type TopDealItem = {")
        type_end = data_text.index("\n};", type_start)
        type_block = data_text[type_start:type_end]

        function_start = data_text.index(
            "export async function getTopDeals("
        )
        function_end = data_text.index(
            "\nexport type ReleaseCalendarItem = {",
            function_start,
        )
        function_block = data_text[function_start:function_end]

        self.assertIn(
            "coverStoragePath: string | null;",
            type_block,
        )
        for token in (
            "const productIds = Array.from(",
            "new Set(rows.map((row) => row.product_id)),",
            "const products = await getProductsByIds(productIds);",
            "const coverStoragePathMap = new Map(",
            "product.cover_storage_path,",
            (
                "coverStoragePath: "
                "coverStoragePathMap.get(row.product_id) ?? null,"
            ),
        ):
            self.assertIn(token, function_block)

        for token in (
            'import { CoverImage } from "@/components/cover-image";',
            "<CoverImage",
            "src={deal.coverUrl}",
            "storagePath={deal.coverStoragePath}",
            'alt={`${deal.artist} - ${deal.title}`}',
            'className="h-full w-full object-contain"',
            'loading={rank <= 6 ? "eager" : "lazy"}',
            'decoding="async"',
            'fetchPriority={rank <= 6 ? "high" : "low"}',
        ):
            self.assertIn(token, card_text)

        for forbidden in (
            "<img",
            'alt=""',
            "COVER_PLACEHOLDER",
            "coverSrc",
            "/placeholders/vinylofy-cover-placeholder-white2.png",
        ):
            self.assertNotIn(forbidden, card_text)

    def test_homepage_uses_central_storage_path(self) -> None:
        data_text = source(VINYLOFY_DATA)
        grid_text = source(HOME_NEW_RELEASES_GRID)

        home_type_start = data_text.index("export type HomeProduct = {")
        home_type_end = data_text.index("\n};", home_type_start)
        home_type = data_text[home_type_start:home_type_end]

        home_data_start = data_text.index(
            "export async function getHomePageData("
        )
        home_data_end = data_text.index(
            "\nasync function resolveProductRowByRouteKey(",
            home_data_start,
        )
        home_data = data_text[home_data_start:home_data_end]

        self.assertIn(
            "coverStoragePath: string | null;",
            home_type,
        )
        self.assertIn(
            (
                "format_label, cover_url, cover_storage_path, "
                "created_at"
            ),
            data_text,
        )
        self.assertEqual(
            home_data.count(
                "coverStoragePath: product.cover_storage_path,"
            ),
            2,
        )

        for token in (
            'import { CoverImage } from "@/components/cover-image";',
            "<CoverImage",
            "src={item.coverUrl}",
            "storagePath={item.coverStoragePath}",
            "data-[cover-fallback=true]:object-contain",
        ):
            self.assertIn(token, grid_text)

        for forbidden in (
            "<img",
            "item.coverUrl ?",
            '<div className="aspect-square rounded-xl bg-neutral-100" />',
        ):
            self.assertNotIn(forbidden, grid_text)

    def test_product_detail_uses_central_storage_path(self) -> None:
        data_text = source(VINYLOFY_DATA)
        card_text = source(PRODUCT_SUMMARY_CARD)

        for token in (
            "coverStoragePath: string | null;",
            (
                "format_label, cover_url, cover_storage_path, "
                "created_at"
            ),
            "coverStoragePath: product.cover_storage_path,",
        ):
            self.assertIn(token, data_text)

        for token in (
            'import { CoverImage } from "@/components/cover-image";',
            "<CoverImage",
            "src={product.coverUrl}",
            "storagePath={product.coverStoragePath}",
            "data-[cover-fallback=true]:h-[104px]",
            "data-[cover-fallback=true]:object-contain",
        ):
            self.assertIn(token, card_text)

        for forbidden in (
            "<img",
            "coverSrc",
            "hasRealCover",
            "/placeholders/vinylofy-cover-placeholder-white2.png",
        ):
            self.assertNotIn(forbidden, card_text)

    def test_search_results_use_central_storage_path(self) -> None:
        data_text = source(VINYLOFY_DATA)
        card_text = source(SEARCH_RESULT_CARD)

        for token in (
            "cover_storage_path: string | null;",
            "coverStoragePath: string | null;",
            (
                "format_label, cover_url, cover_storage_path, "
                "created_at"
            ),
            "coverStoragePath: product.cover_storage_path,",
        ):
            self.assertIn(token, data_text)

        for token in (
            'import { CoverImage } from "@/components/cover-image";',
            "<CoverImage",
            "src={item.coverUrl}",
            "storagePath={item.coverStoragePath}",
            "data-[cover-fallback=true]:object-contain",
        ):
            self.assertIn(token, card_text)

        for forbidden in (
            "<img",
            "coverSrc",
            "hasRealCover",
            "/placeholders/vinylofy-cover-placeholder-white2.png",
        ):
            self.assertNotIn(forbidden, card_text)

    def test_public_product_cover_urls_are_sanitized_before_react_tree(self) -> None:
        text = source(VINYLOFY_DATA)
        self.assertIn(
            'import { resolveCoverUrl } from "@/lib/cover-url";',
            text,
        )
        self.assertEqual(
            text.count("coverUrl: toPublicCoverUrl(product),"),
            4,
        )
        self.assertEqual(
            text.count("coverUrl: product.cover_url,"),
            1,
        )

        helper_start = text.index("function toPublicCoverUrl(")
        helper_end = text.index("\n}\n", helper_start) + 3
        helper_text = text[helper_start:helper_end]

        for token in (
            "return resolveCoverUrl({",
            "coverUrl: product.cover_url,",
            "storagePath: product.cover_storage_path,",
        ):
            self.assertIn(token, helper_text)

        self.assertNotIn(
            "coverUrl: toPublicCoverUrl(product),",
            helper_text,
        )

    def test_frontend_cover_url_helper_is_same_origin(self) -> None:
        text = source(COVER_URL)
        for token in (
            'PRODUCT_COVERS_BUCKET = "product-covers"',
            'COVER_DELIVERY_PREFIX = "/covers/"',
            "normalizeCoverStoragePath",
            "buildProductCoverUrl",
            "isLocalCoverAsset",
            "isSafeCoverUrl",
            "resolveCoverUrl",
            "COVER_PLACEHOLDER_SRC",
        ):
            self.assertIn(token, text)

        for forbidden in (
            "NEXT_PUBLIC_SUPABASE_URL",
            "/storage/v1/object/public",
            "supabase.co",
            "data:",
            "blob:",
            "images.unsplash.com",
            "coverSourceUrl",
        ):
            self.assertNotIn(forbidden, text)

    def test_cover_image_resolves_server_side_and_client_fallback(self) -> None:
        wrapper_text = source(COVER_IMAGE)
        client_text = source(COVER_IMAGE_CLIENT)

        self.assertNotIn('"use client"', wrapper_text)
        for token in (
            'import { CoverImageClient } from "./cover-image-client";',
            "resolveCoverUrl",
            "const resolvedSrc = resolveCoverUrl({",
            "coverUrl: src",
            "storagePath",
            "resolvedSrc={resolvedSrc}",
        ):
            self.assertIn(token, wrapper_text)

        for forbidden in (
            "NEXT_PUBLIC_SUPABASE_URL",
            "/storage/v1/object/public",
            "onError=",
            "COVER_PLACEHOLDER_SRC",
        ):
            self.assertNotIn(forbidden, wrapper_text)

        for token in (
            '"use client"',
            "COVER_PLACEHOLDER_SRC",
            "failedSrc === resolvedSrc",
            "setFailedSrc(resolvedSrc)",
            'data-cover-fallback={showPlaceholder ? "true" : "false"}',
        ):
            self.assertIn(token, client_text)

        self.assertNotIn("useEffect", client_text)
        self.assertNotIn("fallbackSrc", client_text)
        self.assertNotIn("coverSourceUrl", client_text)
        self.assertNotIn("NEXT_PUBLIC_SUPABASE_URL", client_text)

    def test_cover_delivery_route_is_strict_proxy(self) -> None:
        text = source(COVER_ROUTE)

        for token in (
            'export const runtime = "nodejs";',
            "PRODUCT_COVERS_BUCKET",
            "normalizeCoverStoragePath",
            "NEXT_PUBLIC_SUPABASE_URL",
            "/storage/v1/object/public/",
            'redirect: "error"',
            'cache: "no-store"',
            '"image/webp"',
            '"image/jpeg"',
            '"image/png"',
            '"public, max-age=3600, s-maxage=3600"',
            '"X-Content-Type-Options": "nosniff"',
            "errorResponse(404)",
            "errorResponse(502)",
            "errorResponse(503)",
        ):
            self.assertIn(token, text)

        for forbidden in (
            "SUPABASE_SECRET_KEY",
            "SUPABASE_SERVICE_ROLE_KEY",
            "Authorization",
            "Location",
            "redirect(",
        ):
            self.assertNotIn(forbidden, text)

    def test_rollback_does_not_delete_storage_by_inference(self) -> None:
        text = source(ROLLBACK).lower()
        for forbidden in (
            "delete from storage.objects",
            "storage.objects",
            "like 'ean/%'",
            "similar to",
        ):
            self.assertNotIn(forbidden, text)


if __name__ == "__main__":
    unittest.main()
