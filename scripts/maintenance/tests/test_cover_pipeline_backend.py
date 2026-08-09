from __future__ import annotations

import io
from pathlib import Path
import sys
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
MAINTENANCE = ROOT / "scripts" / "maintenance"
if str(MAINTENANCE) not in sys.path:
    sys.path.insert(0, str(MAINTENANCE))

import cover_common as common
import cover_candidate_refresh as candidate_refresh
import cover_mb_worker as mb_worker
import cover_worker as worker


class FakeResponse:
    def __init__(
        self,
        *,
        url: str,
        headers: dict[str, str],
        chunks: list[bytes],
    ) -> None:
        self.url = url
        self.headers = headers
        self._chunks = chunks
        self.status_code = 200

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        del chunk_size
        yield from self._chunks

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls = 0

    def get(self, url: str, **kwargs):
        del url, kwargs
        self.calls += 1
        return self._responses.pop(0)


class CoverCommonBackendTests(unittest.TestCase):
    def tearDown(self) -> None:
        common.clear_download_cache()

    def test_source_aliases_and_deterministic_paths(self) -> None:
        self.assertEqual(
            common.normalize_source_type("shop_listing_image"),
            "listing",
        )
        self.assertEqual(
            common.normalize_source_type("listing_img_src"),
            "listing",
        )
        self.assertEqual(
            common.normalize_source_type("shop_detail_image"),
            "detail",
        )
        self.assertEqual(
            common.build_product_storage_path("8712345678901"),
            "ean/871/8712345678901.webp",
        )
        self.assertEqual(
            common.build_release_storage_path(
                "123e4567-e89b-12d3-a456-426614174000"
            ),
            (
                "covers/releases/"
                "123e4567-e89b-12d3-a456-426614174000.webp"
            ),
        )

    def test_download_cache_streams_once_and_requires_image_mime(self) -> None:
        session = FakeSession(
            [
                FakeResponse(
                    url="https://cdn.test/final.webp",
                    headers={
                        "content-type": "image/webp",
                        "content-length": "6",
                    },
                    chunks=[b"abc", b"def"],
                )
            ]
        )
        with mock.patch.object(
            common,
            "throttle_domain",
            return_value=None,
        ):
            first = common.download_binary(
                session,
                "https://cdn.test/cover.webp",
            )
            second = common.download_binary(
                session,
                "https://cdn.test/cover.webp",
            )

        self.assertEqual(session.calls, 1)
        self.assertEqual(first.content, b"abcdef")
        self.assertFalse(first.reused)
        self.assertTrue(second.reused)

        bad_session = FakeSession(
            [
                FakeResponse(
                    url="https://cdn.test/file.txt",
                    headers={"content-type": "text/plain"},
                    chunks=[b"not-an-image"],
                )
            ]
        )
        with mock.patch.object(
            common,
            "throttle_domain",
            return_value=None,
        ):
            with self.assertRaises(common.CoverPipelineError):
                common.download_binary(
                    bad_session,
                    "https://cdn.test/file.txt",
                )

    def test_image_processing_transposes_and_does_not_upscale(self) -> None:
        from PIL import Image

        source = Image.new("RGB", (300, 500), (10, 20, 30))
        exif = Image.Exif()
        exif[274] = 6
        buffer = io.BytesIO()
        source.save(buffer, format="JPEG", exif=exif)

        prepared = common.prepare_image_for_storage(
            buffer.getvalue(),
            "image/jpeg",
        )
        self.assertEqual((prepared.width, prepared.height), (500, 300))
        self.assertEqual(prepared.mime_type, "image/webp")

        output = Image.open(io.BytesIO(prepared.output_bytes))
        self.assertEqual(output.size, (500, 300))
        self.assertEqual(output.mode, "RGB")


class CandidateRefreshBackendTests(unittest.TestCase):
    def make_product(
        self,
        *,
        status: str = "missing",
        path: str = "",
        needs_refresh: bool = False,
        source_url: str = "",
    ):
        return candidate_refresh.ProductSelection(
            product_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            ean="8712345678901",
            cover_priority=10,
            cover_status=status,
            cover_storage_path=path,
            cover_source_url=source_url,
            cover_needs_refresh=needs_refresh,
        )

    def test_storage_authority_blocked_and_repair_eligibility(self) -> None:
        self.assertEqual(
            candidate_refresh.product_is_eligible(
                self.make_product(status="blocked"),
                include_covered=True,
            ),
            (False, "blocked"),
        )
        self.assertEqual(
            candidate_refresh.product_is_eligible(
                self.make_product(path="ean/871/8712345678901.webp"),
                include_covered=False,
            ),
            (False, "local_cover"),
        )
        self.assertEqual(
            candidate_refresh.product_is_eligible(
                self.make_product(
                    path="ean/871/8712345678901.webp",
                    needs_refresh=True,
                ),
                include_covered=True,
            ),
            (True, "repair"),
        )

    def test_external_product_source_is_candidate_metadata(self) -> None:
        product = self.make_product(
            source_url="https://cdn.test/cover.jpg"
        )
        candidate = (
            candidate_refresh.candidate_from_product_source(product)
        )
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate.source_type, "meta")
        self.assertEqual(
            candidate.image_url,
            "https://cdn.test/cover.jpg",
        )

    def test_candidate_dedup_keeps_highest_rank_and_primary(self) -> None:
        first = common.CandidateRecord(
            product_id="p",
            ean="8712345678901",
            shop_id=None,
            shop_domain="shop.test",
            shop_name=None,
            product_url="https://shop.test/p",
            image_url="https://shop.test/a.webp",
            source_type="listing",
            source_rank=10,
            is_primary=False,
        )
        second = common.CandidateRecord(
            product_id="p",
            ean="8712345678901",
            shop_id=None,
            shop_domain="shop.test",
            shop_name=None,
            product_url="https://shop.test/p",
            image_url="https://shop.test/a.webp",
            source_type="listing",
            source_rank=20,
            is_primary=True,
        )
        unique, duplicate_count = (
            candidate_refresh.deduplicate_candidates(
                [first, second]
            )
        )
        self.assertEqual(duplicate_count, 1)
        self.assertEqual(len(unique), 1)
        self.assertEqual(unique[0].source_rank, 20)
        self.assertTrue(unique[0].is_primary)


class CentralWorkerBackendTests(unittest.TestCase):
    def test_local_preflight_accepts_exact_valid_object(self) -> None:
        state = common.StorageObjectState(
            remote_path="ean/871/8712345678901.webp",
            exists=True,
            metadata={
                "mimetype": "image/webp",
                "size": "1234",
            },
        )
        with (
            mock.patch.object(
                worker,
                "get_storage_bucket_api",
                return_value=(
                    "https://project.supabase.co",
                    "product-covers",
                    object(),
                ),
            ),
            mock.patch.object(
                worker,
                "inspect_storage_object",
                return_value=state,
            ),
        ):
            local = worker.preflight_local_object(
                "ean/871/8712345678901.webp"
            )

        self.assertIsNotNone(local)
        assert local is not None
        self.assertEqual(
            local.remote_path,
            "ean/871/8712345678901.webp",
        )
        self.assertEqual(local.mime_type, "image/webp")
        self.assertEqual(local.byte_size, 1234)

    def test_local_preflight_rejects_missing_or_non_image_object(self) -> None:
        missing = common.StorageObjectState(
            remote_path="ean/871/8712345678901.webp",
            exists=False,
            metadata={},
        )
        with (
            mock.patch.object(
                worker,
                "get_storage_bucket_api",
                return_value=(
                    "https://project.supabase.co",
                    "product-covers",
                    object(),
                ),
            ),
            mock.patch.object(
                worker,
                "inspect_storage_object",
                return_value=missing,
            ),
        ):
            self.assertIsNone(
                worker.preflight_local_object(
                    "ean/871/8712345678901.webp"
                )
            )

        invalid = common.StorageObjectState(
            remote_path="ean/871/8712345678901.webp",
            exists=True,
            metadata={"mimetype": "text/plain"},
        )
        with (
            mock.patch.object(
                worker,
                "get_storage_bucket_api",
                return_value=(
                    "https://project.supabase.co",
                    "product-covers",
                    object(),
                ),
            ),
            mock.patch.object(
                worker,
                "inspect_storage_object",
                return_value=invalid,
            ),
        ):
            self.assertIsNone(
                worker.preflight_local_object(
                    "ean/871/8712345678901.webp"
                )
            )


class MusicBrainzBackendTests(unittest.TestCase):
    def test_front_image_prefers_1200_thumbnail(self) -> None:
        payload = {
            "images": [
                {
                    "front": True,
                    "image": "https://archive.test/original.jpg",
                    "thumbnails": {
                        "250": "https://archive.test/250.jpg",
                        "1200": "https://archive.test/1200.jpg",
                    },
                }
            ]
        }
        self.assertEqual(
            mb_worker.extract_front_url(payload),
            "https://archive.test/1200.jpg",
        )

    def test_exact_barcode_match_becomes_candidate_quality_match(self) -> None:
        payload = {
            "releases": [
                {
                    "id": (
                        "123e4567-e89b-12d3-a456-426614174000"
                    ),
                    "barcode": "8712345678901",
                    "title": "Album",
                    "artist-credit": [{"name": "Artist"}],
                    "score": 100,
                    "status": "Official",
                }
            ]
        }
        decision, candidate, score, basis = (
            mb_worker.choose_candidate(
                payload,
                {
                    "ean": "8712345678901",
                    "title": "Album",
                    "artist": "Artist",
                },
            )
        )
        self.assertEqual(decision, "matched")
        self.assertIsNotNone(candidate)
        self.assertGreaterEqual(score, 115)
        self.assertIn("barcode_exact", basis)

    def test_candidate_rank_is_bounded(self) -> None:
        self.assertEqual(
            mb_worker.musicbrainz_candidate_rank(100.0),
            20,
        )
        self.assertEqual(
            mb_worker.musicbrainz_candidate_rank(125.0),
            45,
        )
        self.assertEqual(
            mb_worker.musicbrainz_candidate_rank(200.0),
            45,
        )


if __name__ == "__main__":
    unittest.main()
