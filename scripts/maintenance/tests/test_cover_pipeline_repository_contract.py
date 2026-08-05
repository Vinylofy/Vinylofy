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

COMMON = MAINTENANCE / "cover_common.py"
REFRESH = MAINTENANCE / "cover_candidate_refresh.py"
WORKER = MAINTENANCE / "cover_worker.py"
MB_WORKER = MAINTENANCE / "cover_mb_worker.py"
COVER_URL = ROOT / "lib" / "cover-url.ts"
COVER_IMAGE = ROOT / "components" / "cover-image.tsx"


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
            MIGRATION,
            ROLLBACK,
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

    def test_frontend_cover_url_helper_is_strict(self) -> None:
        text = source(COVER_URL)
        for token in (
            'PRODUCT_COVERS_BUCKET = "product-covers"',
            "NEXT_PUBLIC_SUPABASE_URL",
            "normalizeCoverStoragePath",
            "buildProductCoverUrl",
            "isSafeCoverUrl",
            "resolveCoverUrl",
            "parsed.origin !== origin",
            "parsed.pathname === expectedPathname",
            "COVER_PLACEHOLDER_SRC",
        ):
            self.assertIn(token, text)
        for forbidden in (
            "data:",
            "blob:",
            "images.unsplash.com",
            "coverSourceUrl",
        ):
            self.assertNotIn(forbidden, text)

    def test_cover_image_has_single_local_fallback(self) -> None:
        text = source(COVER_IMAGE)
        for token in (
            '"use client"',
            "resolveCoverUrl",
            "COVER_PLACEHOLDER_SRC",
            "failedSrc === resolvedSrc",
            "setFailedSrc(resolvedSrc)",
            'data-cover-fallback={showPlaceholder ? "true" : "false"}',
        ):
            self.assertIn(token, text)
        self.assertNotIn("useEffect", text)
        self.assertNotIn("fallbackSrc", text)
        self.assertNotIn("coverSourceUrl", text)

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
